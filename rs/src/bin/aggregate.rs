use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use clap::{Parser, ColorChoice};
use csv::{ReaderBuilder, Trim, WriterBuilder};
use memchr::memchr_iter;
use chrono::Utc;

#[cfg(unix)]
use std::ffi::CStr;

// POSIX-style type masks as encoded by Statwalker in MODE
#[cfg(unix)]
const S_IFMT:  u32 = 0o170000;
#[cfg(unix)]
const S_IFDIR: u32 = 0o040000;

#[cfg(not(unix))]
const S_IFMT:  u32 = 0o170000;
#[cfg(not(unix))]
const S_IFDIR: u32 = 0o040000;


#[derive(Parser, Debug)]
#[command(author, version, color = ColorChoice::Always, 
    about = "Aggregate statwalker CSV into per-(folder, user, age) rows")]
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
    latest_atime: i64,  // seconds since Unix epoch
    latest_mtime: i64,  // seconds since Unix epoch
}

impl UserStats {
    fn update(&mut self, disk: u64, atime_secs: i64, mtime_secs: i64) {
        self.file_count = self.file_count.saturating_add(1);
        self.disk_usage = self.disk_usage.saturating_add(disk);
        if atime_secs > self.latest_atime {
            self.latest_atime = atime_secs;
        }
        if mtime_secs > self.latest_mtime {
            self.latest_mtime = mtime_secs;
        }
    }
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

    // Unknown UID output path is always derived from INPUT stem
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
    println!("Total lines: {}", total_lines);

    // Set up CSV reader
    // IMPORTANT: Trim::None so we never alter raw PATH bytes.
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(Trim::None)
        .from_path(&args.input)?;

    println!("Aggregating {}...", args.input.display());

    // path, user, age -> stats
    let mut aggregated_data: HashMap<(Vec<u8>, String, u8), UserStats> = HashMap::new();
    let progress_interval = if data_lines >= 10 { data_lines / 10 } else { 0 };

    let now_ts = Utc::now().timestamp();

    // Process each record
    for (index, record_result) in reader.byte_records().enumerate() {
        let record = match record_result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Warning: Skipping malformed row {}: {}", index + 1, e);
                continue;
            }
        };

        // Columns: INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
        let mode      = parse_field_as_u32(record.get(5));
        let is_dir    = (mode & S_IFMT) == S_IFDIR;
        let raw_atime = parse_field_as_i64(record.get(1));
        let raw_mtime = parse_field_as_i64(record.get(2));
        let mut sanitized_atime = sanitize_mtime(now_ts, raw_atime);
        let mut sanitized_mtime = sanitize_mtime(now_ts, raw_mtime);

        if is_dir {
            sanitized_atime = 0; // ignore folder accessed times
        }

        let uid = parse_field_as_u32(record.get(3));
        let user = resolve_user(uid, &mut user_cache);
        if user == "UNK" {
            unk_uids.insert(uid);
        }

        let disk_usage = parse_field_as_u64(record.get(7));
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
                .update(disk_usage, sanitized_atime, sanitized_mtime);
        }

        // Show progress (approx 10% steps)
        if progress_interval > 0 && (index + 1) % progress_interval == 0 {
            let percent = ((index + 1) as f64 * 100.0 / data_lines.max(1) as f64).ceil() as u32;
            println!("{}%", percent.min(100));
        }
    }

    // Write results (PATH converted to UTF-8 with lossless-as-possible, lossy-where-needed)
    write_results(&output_path, &aggregated_data)?;
    // Write unknown UIDs list
    write_unknown_uids(&unk_path, &unk_uids)?;

    let duration = start_time.elapsed();
    println!("Output       : {}", output_path.display());
    println!("Unknown UIDs : {} (total: {})", unk_path.display(), unk_uids.len());
    let percent_unique = if data_lines > 0 { ((aggregated_data.len() as f64 / data_lines as f64) * 100.0) as i32 } else { 0 };
    println!("Total lines  : {} ({}% of input)",aggregated_data.len(), percent_unique);
    println!("Elapsed time : {:.2} seconds", duration.as_secs_f64());
    Ok(())
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

/// Safely convert bytes to UTF-8 String (invalid sequences -> U+FFFD)
fn bytes_to_safe_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

// Fast, no-alloc numeric parsing from bytes (defaults to 0 on any error)
#[inline]
fn trim_ascii(mut s: &[u8]) -> &[u8] {
    while !s.is_empty() && s[0].is_ascii_whitespace() { s = &s[1..]; }
    while !s.is_empty() && s[s.len() - 1].is_ascii_whitespace() { s = &s[..s.len() - 1]; }
    s
}
#[inline]
fn parse_field_as_u32(field: Option<&[u8]>) -> u32 {
    let s = trim_ascii(field.unwrap_or(b"0"));
    atoi::atoi::<u32>(s).unwrap_or(0)
}
#[inline]
fn parse_field_as_i64(field: Option<&[u8]>) -> i64 {
    let s = trim_ascii(field.unwrap_or(b"0"));
    atoi::atoi::<i64>(s).unwrap_or(0)
}
#[inline]
fn parse_field_as_u64(field: Option<&[u8]>) -> u64 {
    let s = trim_ascii(field.unwrap_or(b"0"));
    atoi::atoi::<u64>(s).unwrap_or(0)
}

/// Sanitize mtime: if it's more than 1 day in the future, set to 0
fn sanitize_mtime(now_ts: i64, mtime_ts: i64) -> i64 {
    const ONE_DAY_SECS: i64 = 86_400;
    if mtime_ts > now_ts + ONE_DAY_SECS {
        0
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
    let mut buf = [0u8; 128 * 1024];
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
            count += 1;
        }
    }
    Ok(count)
}

fn write_results(
    output_path: &Path,
    aggregated_data: &HashMap<(Vec<u8>, String, u8), UserStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Sort deterministically (bytewise path, then user, then age)
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
        "accessed",
        "modified",
    ])?;

    for ((path_bytes, user, age), stats) in sorted_entries {
        // Convert path bytes to safe UTF-8 string (ONLY here)
        let path_str = bytes_to_safe_string(path_bytes);
        writer.write_record(&[
            &path_str,
            user,
            &age.to_string(),
            &stats.file_count.to_string(),
            &stats.disk_usage.to_string(),
            &stats.latest_atime.to_string(),
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
            Ok(name) => name.to_string(),   // ensure UTF-8 names; else "UNK"
            Err(_) => "UNK".to_string(),
        }
    }
}

#[cfg(not(unix))]
fn get_username_from_uid(uid: u32) -> String {
    // On non-Unix just echo the uid as a "name" (UTF-8)
    uid.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::NamedTempFile;


    #[test]
    fn bytes_to_safe_string_handles_invalid_utf8() {
        let bad = [0xFFu8, b'a', 0xFE, b'b'];
        let s = bytes_to_safe_string(&bad);
        assert!(s.contains('�'));
        assert!(s.contains('a') && s.contains('b'));
    }

    #[test]
    fn parser_trims_and_defaults() {
        assert_eq!(parse_field_as_u32(Some(b" 42 ")), 42);
        assert_eq!(parse_field_as_u32(Some(b"-1")), 0);
        assert_eq!(parse_field_as_u32(Some(b"4294967296")), 0); // overflow -> 0
        assert_eq!(parse_field_as_u64(Some(b"  100 ")), 100);
        assert_eq!(parse_field_as_i64(Some(b" +7 ")), 7);
        assert_eq!(parse_field_as_i64(Some(b" foo ")), 0);
        assert_eq!(parse_field_as_u32(None), 0);
    }

    #[test]
    fn ancestors_from_non_utf8_bytes() {
        // Include invalid UTF-8 byte 0xFF and nested segments
        let raw = [b'/', 0xFFu8, b'a', b'/', b'b', b'/', b'c', b'/', b'f', b'.', b't', b'x', b't'];
        let ancestors = get_folder_ancestors(&raw);
        // still builds byte paths; no panics
        assert_eq!(ancestors[0], b"/".to_vec());
        assert!(ancestors.contains(&vec![b'/', 0xFFu8, b'a']));
        assert!(ancestors.contains(&vec![b'/', 0xFFu8, b'a', b'/', b'b']));
    }

    #[test]
    fn write_results_emits_utf8_paths() {
        let mut map: HashMap<(Vec<u8>, String, u8), UserStats> = HashMap::new();
        let key = (vec![b'/', 0xFFu8, b'a'], "user".to_string(), 0u8);
        let mut s = UserStats::default();
        s.update(512, 1_700_000_000, 1_700_000_000);
        map.insert(key, s);

        let tmp = std::env::temp_dir().join(format!("agg_out_{}.csv", std::process::id()));
        let _ = fs::remove_file(&tmp);
        write_results(&tmp, &map).unwrap();

        let contents = fs::read_to_string(&tmp).unwrap(); // must be valid UTF-8
        fs::remove_file(&tmp).ok();

        // Should contain replacement char for 0xFF and the rest intact
        assert!(contents.contains('�'));
        assert!(contents.contains("/a") || contents.contains("/�a"));
        assert!(contents.lines().next().unwrap().contains("path,user,age,files,disk,accessed,modified"));
    }

    #[test]
    fn count_lines_empty() {
        let f = NamedTempFile::new().unwrap();
        assert_eq!(count_lines(f.path()).unwrap(), 0);
    }

    #[test]
    fn count_lines_no_trailing_newline() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "a\nb\nc").unwrap();
        assert_eq!(count_lines(f.path()).unwrap(), 3);
    }

    #[test]
    fn count_lines_with_trailing_newline() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "a\nb\nc\n").unwrap();
        assert_eq!(count_lines(f.path()).unwrap(), 3);
    }

    // ---------- write_results ordering ----------

    #[test]
    fn write_results_is_sorted_by_path_user_age() {
        let mut map: HashMap<(Vec<u8>, String, u8), UserStats> = HashMap::new();

        // Insert in scrambled order
        map.insert((b"/a/b".to_vec(), "user2".to_string(), 1), UserStats { file_count: 2, disk_usage: 200, latest_atime: 20, latest_mtime: 20 });
        map.insert((b"/a".to_vec(),   "user1".to_string(), 0), UserStats { file_count: 1, disk_usage: 100, latest_atime: 20, latest_mtime: 10 });
        map.insert((b"/a".to_vec(),   "user0".to_string(), 2), UserStats { file_count: 3, disk_usage: 300, latest_atime: 20, latest_mtime: 30 });

        let tmp = NamedTempFile::new().unwrap();
        write_results(tmp.path(), &map).unwrap();

        let contents = fs::read_to_string(tmp.path()).unwrap();
        let mut lines = contents.lines();
        // header
        assert_eq!(lines.next().unwrap(), "path,user,age,files,disk,accessed,modified");

        // Expected order: path (/a, /a, /a/b), then user (user0, user1), then age
        let row1 = lines.next().unwrap().to_string();
        let row2 = lines.next().unwrap().to_string();
        let row3 = lines.next().unwrap().to_string();

        assert!(row1.starts_with("/a,user0,2,"), "got: {row1}");
        assert!(row2.starts_with("/a,user1,0,"), "got: {row2}");
        assert!(row3.starts_with("/a/b,user2,1,"), "got: {row3}");

        // nothing else
        assert!(lines.next().is_none());
    }

    // ---------- write_unknown_uids ordering ----------

    #[test]
    fn write_unknown_uids_is_sorted() {
        let tmp = NamedTempFile::new().unwrap();
        let mut set = std::collections::HashSet::new();
        set.insert(42);
        set.insert(7);
        set.insert(1000);

        write_unknown_uids(tmp.path(), &set).unwrap();
        let s = fs::read_to_string(tmp.path()).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines, vec!["7", "42", "1000"]);
    }

    // ---------- get_folder_ancestors edge cases ----------

    #[test]
    fn ancestors_trailing_slashes_and_multi_seps() {
        // Trailing slashes + multiple slashes, with filename
        let res = get_folder_ancestors(b"/a//b///c//file.txt");
        assert_eq!(
            res,
            vec![
                b"/".to_vec(),
                b"/a".to_vec(),
                b"/a/b".to_vec(),
                b"/a/b/c".to_vec()
            ]
        );
    }

    #[test]
    fn ancestors_windows_backslashes_normalized() {
        let res = get_folder_ancestors(b"C:\\a\\b\\file.txt");
        assert_eq!(
            res,
            vec![
                b"/".to_vec(),
                b"/C:".to_vec(),
                b"/C:/a".to_vec(),
                b"/C:/a/b".to_vec()
            ]
        );
    }

    // ---------- PATH lossy UTF-8 conversion stays UTF-8 ----------

    #[test]
    fn bytes_to_safe_string_always_utf8() {
        // invalid bytes should produce replacement chars but valid UTF-8 string overall
        let raw = [b'/', 0xFFu8, b'a', b'/', 0xFE, b'b'];
        let s = bytes_to_safe_string(&raw);
        assert!(s.is_char_boundary(s.len())); // well-formed UTF-8
        assert!(s.contains('�'));
        assert!(s.contains("/"));
    }
}
