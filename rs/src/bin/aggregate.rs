// Cargo.toml:
// [dependencies]
// clap = { version = "4", features = ["derive"] }
// csv = "1"
// memchr = "2"

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use clap::Parser;
use csv::{ByteRecord, ReaderBuilder, Trim, WriterBuilder};
use memchr::memchr_iter;

#[derive(Parser, Debug)]
#[command(about = "Aggregate statwalker CSV into per-(folder, user) rows")]
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
    file_size: u64,     // CHANGED: integer bytes
    disk_usage: u64,    // CHANGED: integer bytes
    latest_mtime: String,
}

impl UserStats {
    fn update(&mut self, size: u64, disk: u64, mtime: &str) {
        self.file_count = self.file_count.saturating_add(1);
        self.file_size = self.file_size.saturating_add(size);
        self.disk_usage = self.disk_usage.saturating_add(disk);
        if self.latest_mtime.is_empty() || self.latest_mtime.as_str() < mtime {
            self.latest_mtime = mtime.to_string();
        }
    }
}

fn get_folder_ancestors(path: &[u8]) -> Vec<Vec<u8>> {
    // Normalize path separators
    let normalized: Vec<u8> = path.iter()
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

fn parse_field_as_u64(field: Option<&[u8]>) -> u64 {
    field
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.trim())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

fn parse_field_as_string(field: Option<&[u8]>) -> String {
    field
        .and_then(|b| std::str::from_utf8(b).ok())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn count_lines_fast(path: &Path) -> std::io::Result<usize> {
    let mut file = File::open(path)?;
    let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
    let mut line_count = 0;
    let mut has_content = false;
    let mut last_byte = b'\n';

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        has_content = true;
        line_count += memchr_iter(b'\n', &buffer[..bytes_read]).count();
        last_byte = buffer[bytes_read - 1];
    }

    // Count final line if file doesn't end with newline
    if has_content && last_byte != b'\n' {
        line_count += 1;
    }

    Ok(line_count)
}

fn write_results(
    output_path: &Path,
    aggregated_data: &HashMap<(Vec<u8>, String), UserStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut sorted_entries: Vec<_> = aggregated_data.iter().collect();
    sorted_entries.sort_by(|a, b| {
        let (path_a, user_a) = &a.0;
        let (path_b, user_b) = &b.0;
        path_a.cmp(path_b).then_with(|| user_a.cmp(user_b))
    });

    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_path)?;

    writer.write_record(&[
        "path", "user", "file_count", "file_size", "disk_usage", "latest_modified"
    ])?;

    for ((path, user), stats) in sorted_entries {
        let mut record = ByteRecord::new();
        record.push_field(path);
        record.push_field(user.as_bytes());
        record.push_field(stats.file_count.to_string().as_bytes());
        record.push_field(stats.file_size.to_string().as_bytes());   // integers
        record.push_field(stats.disk_usage.to_string().as_bytes());  // integers
        record.push_field(stats.latest_mtime.as_bytes());
        writer.write_byte_record(&record)?;
    }

    writer.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();
    let args = Args::parse();

    // Determine output path
    let output_path = args.output.unwrap_or_else(|| {
        let stem = args.input.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        PathBuf::from(format!("{}.agg.csv", stem))
    });

    // Count total lines for progress tracking
    println!("Counting lines in {}", args.input.display());
    let total_lines = count_lines_fast(&args.input)?;
    let data_lines = total_lines.saturating_sub(1);
    println!("Total lines: {} (data: {})", total_lines, data_lines);

    // Set up CSV reader
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(Trim::All)
        .from_path(&args.input)?;

    println!("Processing {}", args.input.display());

    let mut aggregated_data: HashMap<(Vec<u8>, String), UserStats> = HashMap::new();
    let progress_interval = if data_lines >= 10 { data_lines / 10 } else { 0 };

    // Process each record
    for (index, record_result) in reader.byte_records().enumerate() {
        let record = match record_result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Warning: Skipping malformed row {}: {}", index + 1, e);
                continue;
            }
        };

        // Parse required fields (columns: INODE,ACCESSED,MODIFIED,USER,GROUP,TYPE,PERM,SIZE,DISK,PATH,CATEGORY,HASH)
        let modified_time = parse_field_as_string(record.get(2));
        let user = parse_field_as_string(record.get(3));
        let file_size = parse_field_as_u64(record.get(7)); // integer bytes
        let disk_usage = parse_field_as_u64(record.get(8)); // integer bytes
        let path_bytes = record.get(9).unwrap_or(b"");

        if user.is_empty() || path_bytes.is_empty() {
            continue;
        }

        // Update statistics for each ancestor folder
        for folder_path in get_folder_ancestors(path_bytes) {
            let key = (folder_path, user.clone());
            aggregated_data.entry(key)
                .or_default()
                .update(file_size, disk_usage, &modified_time);
        }

        // Show progress (approx 10% steps)
        if progress_interval > 0 && (index + 1) % progress_interval == 0 {
            let percent = ((index + 1) as f64 * 100.0 / data_lines.max(1) as f64).ceil() as u32;
            println!("{}% - Processed {} rows", percent.min(100), index + 1);
        }
    }

    // Write results
    write_results(&output_path, &aggregated_data)?;

    let duration = start_time.elapsed();
    println!("✓ Aggregation complete!");
    println!("  Output: {}", output_path.display());
    println!("  Unique (folder, user) pairs: {}", aggregated_data.len());
    println!("  Time: {:.2} seconds", duration.as_secs_f64());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_folder_ancestors_basic() {
        let result = get_folder_ancestors(b"/a/b/file.txt");
        assert_eq!(result, vec![
            b"/".to_vec(),
            b"/a".to_vec(),
            b"/a/b".to_vec()
        ]);
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
        assert_eq!(result, vec![
            b"/".to_vec(),
            b"/C:".to_vec(),
            b"/C:/Users".to_vec(),
            b"/C:/Users/test".to_vec()
        ]);
    }

    #[test]
    fn test_folder_ancestors_deep_path() {
        let result = get_folder_ancestors(b"/a/b/c/d/e/file.txt");
        assert_eq!(result, vec![
            b"/".to_vec(),
            b"/a".to_vec(),
            b"/a/b".to_vec(),
            b"/a/b/c".to_vec(),
            b"/a/b/c/d".to_vec(),
            b"/a/b/c/d/e".to_vec()
        ]);
    }

    #[test]
    fn test_folder_ancestors_trailing_slash() {
        let result = get_folder_ancestors(b"/a/b/");
        assert_eq!(result, vec![
            b"/".to_vec(),
            b"/a".to_vec()
        ]);
    }

    #[test]
    fn test_folder_ancestors_empty_segments() {
        let result = get_folder_ancestors(b"/a//b/file.txt");
        assert_eq!(result, vec![
            b"/".to_vec(),
            b"/a".to_vec(),
            b"/a/b".to_vec()
        ]);
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
    fn test_parse_field_as_string() {
        assert_eq!(parse_field_as_string(Some(b"hello")), "hello");
        assert_eq!(parse_field_as_string(Some(b"  trimmed  ")), "trimmed");
        assert_eq!(parse_field_as_string(Some(b"")), "");
        assert_eq!(parse_field_as_string(None), "");
    }

    #[test]
    fn test_user_stats_update() {
        let mut stats = UserStats::default();
        
        stats.update(100, 50, "2023-01-01T00:00:00Z");
        assert_eq!(stats.file_count, 1);
        assert_eq!(stats.file_size, 100);
        assert_eq!(stats.disk_usage, 50);
        assert_eq!(stats.latest_mtime, "2023-01-01T00:00:00Z");

        stats.update(200, 75, "2023-01-02T00:00:00Z");
        assert_eq!(stats.file_count, 2);
        assert_eq!(stats.file_size, 300);
        assert_eq!(stats.disk_usage, 125);
        assert_eq!(stats.latest_mtime, "2023-01-02T00:00:00Z");

        // Test older timestamp doesn't update latest_mtime
        stats.update(50, 25, "2022-12-31T00:00:00Z");
        assert_eq!(stats.file_count, 3);
        assert_eq!(stats.file_size, 350);
        assert_eq!(stats.disk_usage, 150);
        assert_eq!(stats.latest_mtime, "2023-01-02T00:00:00Z");
    }

    #[test]
    fn test_user_stats_empty_mtime() {
        let mut stats = UserStats::default();
        
        stats.update(100, 50, "");
        assert_eq!(stats.latest_mtime, "");

        stats.update(100, 50, "2023-01-01T00:00:00Z");
        assert_eq!(stats.latest_mtime, "2023-01-01T00:00:00Z");
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
