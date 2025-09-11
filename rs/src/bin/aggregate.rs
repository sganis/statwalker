// Cargo.toml:
// [dependencies]
// clap = { version = "4", features = ["derive"] }
// csv = "1"
// memchr = "2"
// chrono = { version = "0.4", features = ["clock"] }
// libc = "0.2"

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use clap::Parser;
use csv::{ReaderBuilder, Trim, WriterBuilder};
use memchr::memchr_iter;
use chrono::Utc;

#[cfg(unix)]
use std::ffi::CStr;

#[derive(Parser, Debug)]
#[command(about = "Aggregate statwalker CSV into per-(folder, user, age) rows")]
struct Args {
    /// Input CSV file path
    input: PathBuf,
    /// Output CSV file path (defaults to <input_stem>.agg.csv)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Default, Clone, Debug, PartialEq)]
struct UserStats {
    file_count: u64,
    disk_usage: u64,    // integer bytes
    latest_mtime: i64,  // seconds since Unix epoch
}

impl UserStats {
    fn update(&mut self, disk: u64, mtime_secs: i64) {
        self.file_count = self.file_count.saturating_add(1);
        self.disk_usage = self.disk_usage.saturating_add(disk);
        if mtime_secs > self.latest_mtime {
            self.latest_mtime = mtime_secs;
        }
    }
}

/// Convert path bytes into a list of ancestor folder paths:
///  "/a/b/file.txt" -> ["/", "/a", "/a/b"]
fn get_folder_ancestors(path: &[u8]) -> Vec<Vec<u8>> {
    // Normalize path separators
    let normalized: Vec<u8> = path
        .iter()
        .map(|&b| if b == b'\\' { b'/' } else { b })
        .collect();

    // Find the last directory separator
    let parent_end = normalized.iter().rposition(|&b| b == b'/');

    let folder = match parent_end {
        Some(0) | None => return vec![b"/".to_vec()], // Root or no separator
        Some(pos) => &normalized[..pos],
    };

    // Remove trailing slashes
    let mut folder = folder.to_vec();
    while folder.len() > 1 && folder.last() == Some(&b'/') {
        folder.pop();
    }

    // Build ancestor list starting with root
    let mut ancestors = vec![b"/".to_vec()];

    // Skip leading '/' and split by '/'
    let trimmed = if folder.starts_with(&[b'/']) { &folder[1..] } else { &folder[..] };
    if trimmed.is_empty() {
        return ancestors;
    }

    // Build paths incrementally: /a, /a/b, /a/b/c
    let mut current_path = Vec::new();
    current_path.push(b'/');

    for segment in trimmed.split(|&b| b == b'/').filter(|s| !s.is_empty()) {
        if current_path.len() > 1 {
            current_path.push(b'/');
        }
        current_path.extend_from_slice(segment);
        ancestors.push(current_path.clone());
    }

    ancestors
}

/// Safely convert bytes to UTF-8 string, replacing invalid sequences
fn bytes_to_safe_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn parse_field_as_u32(field: Option<&[u8]>) -> u32 {
    field
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.trim())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0)
}

fn parse_field_as_i64(field: Option<&[u8]>) -> i64 {
    field
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.trim())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

fn parse_field_as_u64(field: Option<&[u8]>) -> u64 {
    field
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.trim())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Sanitize mtime: if it's more than 1 day in the future, set to 0
fn sanitize_mtime(now_ts: i64, mtime_ts: i64) -> i64 {
    const ONE_DAY_SECS: i64 = 86_400;
    
    if mtime_ts > now_ts + ONE_DAY_SECS {
        0  // Set to epoch if more than 1 day in the future
    } else {
        mtime_ts
    }
}

/// Bucket age in days into:
/// 0: <= 60 days
/// 1: 61..=600 days
/// 2: > 600 days OR unknown/invalid mtime
fn age_bucket(now_ts: i64, mtime_ts: i64) -> u8 {
    if mtime_ts <= 0 {
        return 2;
    }
    let age_secs = now_ts.saturating_sub(mtime_ts);
    let days = age_secs / 86_400;
    if days <= 60 {
        0
    } else if days <= 600 {
        1
    } else {
        2
    }
}

pub fn count_lines(path: &Path) -> std::io::Result<usize> {
    let mut file = File::open(path)?;
    let mut buf = [0u8; 128 * 1024]; // 128 KiB is plenty; adjust if you like
    let mut count = 0usize;
    let mut last: Option<u8> = None;

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        count += memchr_iter(b'\n', &buf[..n]).count();
        last = Some(buf[n - 1]);
    }

    if let Some(b) = last {
        if b != b'\n' {
            count += 1; // account for final line without trailing newline
        }
    }
    Ok(count)
}
fn write_results(
    output_path: &Path,
    aggregated_data: &HashMap<(Vec<u8>, String, u8), UserStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut sorted_entries: Vec<_> = aggregated_data.iter().collect();
    sorted_entries.sort_by(|a, b| {
        let (path_a, user_a, age_a) = &a.0;
        let (path_b, user_b, age_b) = &b.0;
        path_a
            .cmp(path_b)
            .then_with(|| user_a.cmp(user_b))
            .then_with(|| age_a.cmp(age_b))
    });

    let mut writer = WriterBuilder::new().has_headers(true).from_path(output_path)?;

    writer.write_record(&[
        "path",
        "user",
        "age",
        "files",
        "disk",
        "modified",
    ])?;

    for ((path_bytes, user, age), stats) in sorted_entries {
        // Convert path bytes to safe UTF-8 string
        let path_str = bytes_to_safe_string(path_bytes);
        
        writer.write_record(&[
            &path_str,
            user,
            &age.to_string(),
            &stats.file_count.to_string(),
            &stats.disk_usage.to_string(),
            &stats.latest_mtime.to_string(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

fn write_unknown_uids(unk_path: &Path, unk_uids: &HashSet<u32>) -> Result<(), Box<dyn std::error::Error>> {
    // deterministic order
    let mut list: Vec<u32> = unk_uids.iter().copied().collect();
    list.sort_unstable();

    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(unk_path)?;

    for uid in list {
        wtr.write_record(&[uid.to_string()])?;
    }
    wtr.flush()?;
    Ok(())
}

fn resolve_user(uid: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(u) = cache.get(&uid) {
        return u.clone();
    }
    let name = get_username_from_uid(uid);
    cache.insert(uid, name.clone());
    name
}

#[cfg(unix)]
fn get_username_from_uid(uid: u32) -> String {
    unsafe {
        let passwd = libc::getpwuid(uid);
        if passwd.is_null() {
            return "UNK".to_string();
        }
        let name_ptr = (*passwd).pw_name;
        if name_ptr.is_null() {
            return "UNK".to_string();
        }
        match CStr::from_ptr(name_ptr).to_str() {
            Ok(name) => name.to_string(),
            Err(_) => "UNK".to_string(),
        }
    }
}

#[cfg(not(unix))]
fn get_username_from_uid(uid: u32) -> String {
    // On non-Unix just echo the uid as a "name"
    uid.to_string()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();
    let args = Args::parse();

    // Determine output path
    let output_path = args.output.clone().unwrap_or_else(|| {
        let stem = args
            .input
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        PathBuf::from(format!("{}.agg.csv", stem))
    });

    // Unknown UID output path is always derived from INPUT stem (per request)
    let unk_path = {
        let stem = args
            .input
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        PathBuf::from(format!("{}.unk.csv", stem))
    };

    let mut user_cache: HashMap<u32, String> = HashMap::new(); // UID -> username cache
    let mut unk_uids: HashSet<u32> = HashSet::new(); // collect UIDs that resolve to UNK

    // Count total lines for progress tracking
    println!("Counting lines in {}", args.input.display());
    let total_lines = count_lines(&args.input)?;
    let data_lines = total_lines.saturating_sub(1);
    println!("Total lines: {} (data: {})", total_lines, data_lines);

    // Set up CSV reader
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(Trim::All)
        .from_path(&args.input)?;

    println!("Processing {}", args.input.display());

    // path, user, age -> stats
    let mut aggregated_data: HashMap<(Vec<u8>, String, u8), UserStats> = HashMap::new();
    let progress_interval = if data_lines >= 10 { data_lines / 10 } else { 0 };

    // Use Local::now() instead of deprecated Utc::now()
    let now_ts = Utc::now().timestamp();


    // fn epoch_secs_to_iso_date(secs: i64) -> String {
    //     chrono::DateTime::<Utc>::from_timestamp(secs, 0)
    //         .map(|dt| dt.date_naive().to_string()) // "YYYY-MM-DD"
    //         .unwrap_or_else(|| "1970-01-01".to_string())
    // }

    // Process each record
    for (index, record_result) in reader.byte_records().enumerate() {
        let record = match record_result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Warning: Skipping malformed row {}: {}", index + 1, e);
                continue;
            }
        };

        // Parse required fields
        // Columns (as used here): INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
        let raw_mtime = parse_field_as_i64(record.get(2));
        let sanitized_mtime = sanitize_mtime(now_ts, raw_mtime);
        
        let uid = parse_field_as_u32(record.get(3));
        let user = resolve_user(uid, &mut user_cache);

        // track unknowns
        if user == "UNK" {
            unk_uids.insert(uid);
        }

        let disk_usage = parse_field_as_u64(record.get(7)); // integer bytes
        let path_bytes = record.get(8).unwrap_or(b"");

        if user.is_empty() || path_bytes.is_empty() {
            continue;
        }

        let bucket = age_bucket(now_ts, sanitized_mtime);

        // Update statistics for each ancestor folder
        for folder_path in get_folder_ancestors(path_bytes) {
            let key = (folder_path, user.clone(), bucket);
            aggregated_data
                .entry(key)
                .or_default()
                .update(disk_usage, sanitized_mtime);
        }

        // Show progress (approx 10% steps)
        if progress_interval > 0 && (index + 1) % progress_interval == 0 {
            let percent = ((index + 1) as f64 * 100.0 / data_lines.max(1) as f64).ceil() as u32;
            println!("{}%", percent.min(100));
        }
    }

    // Write results
    write_results(&output_path, &aggregated_data)?;
    // Write unknown UIDs list
    write_unknown_uids(&unk_path, &unk_uids)?;

    let duration = start_time.elapsed();
    println!("✓ Aggregation complete!");
    println!("  Output: {}", output_path.display());
    println!("  Unknown UIDs: {} -> {}", unk_uids.len(), unk_path.display());
    let percent_unique = if data_lines > 0 { ((aggregated_data.len() as f64 / data_lines as f64) * 100.0) as i32 } else { 0 };
    println!(
        "  Unique (folder, user, age) triples: {} - {}%",
        aggregated_data.len(), percent_unique
    );
    println!("  Time: {:.2} seconds", duration.as_secs_f64());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_folder_ancestors_basic() {
        let result = get_folder_ancestors(b"/a/b/file.txt");
        assert_eq!(
            result,
            vec![b"/".to_vec(), b"/a".to_vec(), b"/a/b".to_vec()]
        );
    }

    #[test]
    fn test_folder_ancestors_root_file() {
        let result = get_folder_ancestors(b"/file.txt");
        assert_eq!(result, vec![b"/".to_vec()]);
    }

    #[test]
    fn test_folder_ancestors_no_leading_slash() {
        let result = get_folder_ancestors(b"file.txt");
        assert_eq!(result, vec![b"/".to_vec()]);
    }

    #[test]
    fn test_folder_ancestors_windows_separators() {
        let result = get_folder_ancestors(b"C:\\Users\\test\\file.txt");
        assert_eq!(
            result,
            vec![
                b"/".to_vec(),
                b"/C:".to_vec(),
                b"/C:/Users".to_vec(),
                b"/C:/Users/test".to_vec()
            ]
        );
    }

    #[test]
    fn test_folder_ancestors_deep_path() {
        let result = get_folder_ancestors(b"/a/b/c/d/e/file.txt");
        assert_eq!(
            result,
            vec![
                b"/".to_vec(),
                b"/a".to_vec(),
                b"/a/b".to_vec(),
                b"/a/b/c".to_vec(),
                b"/a/b/c/d".to_vec(),
                b"/a/b/c/d/e".to_vec()
            ]
        );
    }

    // #[test]
    // fn test_folder_ancestors_trailing_slash() {
    //     let result = get_folder_ancestors(b"/a/b/");
    //     assert_eq!(result, vec![b"/".to_vec(), b"/a".to_vec()]);
    // }

    #[test]
    fn test_folder_ancestors_empty_segments() {
        let result = get_folder_ancestors(b"/a//b/file.txt");
        assert_eq!(
            result,
            vec![b"/".to_vec(), b"/a".to_vec(), b"/a/b".to_vec()]
        );
    }

    #[test]
    fn test_parse_field_as_u64() {
        assert_eq!(parse_field_as_u64(Some(b"12345")), 12345u64);
        assert_eq!(parse_field_as_u64(Some(b"0")), 0u64);
        assert_eq!(parse_field_as_u64(Some(b"  42  ")), 42u64);
        assert_eq!(parse_field_as_u64(Some(b"invalid")), 0u64);
        assert_eq!(parse_field_as_u64(None), 0u64);
        assert_eq!(parse_field_as_u64(Some(b"")), 0u64);
    }

    #[test]
    fn test_sanitize_mtime() {
        let now = 2_000_000_000i64; // ~2033
        let one_day = 86_400i64;
        
        // Normal case: mtime in the past
        assert_eq!(sanitize_mtime(now, now - 1000), now - 1000);
        
        // Normal case: mtime slightly in future (< 1 day)
        assert_eq!(sanitize_mtime(now, now + 3600), now + 3600); // 1 hour future
        
        // Edge case: exactly 1 day in future
        assert_eq!(sanitize_mtime(now, now + one_day), now + one_day);
        
        // Problem case: more than 1 day in future - should be sanitized to 0
        assert_eq!(sanitize_mtime(now, now + one_day + 1), 0);
        assert_eq!(sanitize_mtime(now, now + 365 * one_day), 0); // 1 year future
        
        // Zero/negative timestamps should pass through
        assert_eq!(sanitize_mtime(now, 0), 0);
        assert_eq!(sanitize_mtime(now, -1), -1);
    }

    #[test]
    fn test_bytes_to_safe_string() {
        // Valid UTF-8
        assert_eq!(bytes_to_safe_string(b"hello"), "hello");
        assert_eq!(bytes_to_safe_string(b"/path/to/file"), "/path/to/file");
        
        // Invalid UTF-8 - should be replaced with replacement character
        let invalid_utf8 = &[0x80, 0x81, 0x82];
        let result = bytes_to_safe_string(invalid_utf8);
        assert!(result.contains('\u{FFFD}')); // replacement character
    }

    #[test]
    fn test_age_bucket() {
        let now = 2_000_000_000i64; // ~2033
        assert_eq!(age_bucket(now, now), 0); // 0 days
        assert_eq!(age_bucket(now, now - 60 * 86_400), 0); // 60d
        assert_eq!(age_bucket(now, now - 61 * 86_400), 1); // 61d
        assert_eq!(age_bucket(now, now - 600 * 86_400), 1); // 600d
        assert_eq!(age_bucket(now, now - 601 * 86_400), 2); // 601d
        assert_eq!(age_bucket(now, 0), 2); // sanitized timestamp
    }

    #[test]
    fn test_user_stats_update() {
        let mut stats = UserStats::default();

        stats.update(50, 10);
        assert_eq!(stats.file_count, 1);
        assert_eq!(stats.disk_usage, 50);
        assert_eq!(stats.latest_mtime, 10);

        stats.update(75, 20);
        assert_eq!(stats.file_count, 2);
        assert_eq!(stats.disk_usage, 125);
        assert_eq!(stats.latest_mtime, 20);

        // Older timestamp shouldn't update latest_mtime
        stats.update(25, 5);
        assert_eq!(stats.file_count, 3);
        assert_eq!(stats.disk_usage, 150);
        assert_eq!(stats.latest_mtime, 20);
    }

    #[test]
    fn test_user_stats_empty_mtime() {
        let mut stats = UserStats::default();

        stats.update(50, 0);
        assert_eq!(stats.latest_mtime, 0);

        stats.update(50, 123);
        assert_eq!(stats.latest_mtime, 123);
    }

    #[test]
    fn test_edge_cases() {
        // Test empty path
        let result = get_folder_ancestors(b"");
        assert_eq!(result, vec![b"/".to_vec()]);

        // Test just root
        let result = get_folder_ancestors(b"/");
        assert_eq!(result, vec![b"/".to_vec()]);

        // Test single character paths
        let result = get_folder_ancestors(b"/a");
        assert_eq!(result, vec![b"/".to_vec()]);

        let result = get_folder_ancestors(b"a");
        assert_eq!(result, vec![b"/".to_vec()]);
    }
}
