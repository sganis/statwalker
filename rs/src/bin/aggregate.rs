use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::io::{BufRead, BufReader};
use std::fs::File;
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};

#[derive(Parser, Debug)]
#[command(about = "Aggregate statwalker CSV into per-(folder, uid) rows")]
struct Args {
    /// Input CSV produced by statwalker.py
    input: PathBuf,
    /// Output CSV (defaults to <stem>.agg.csv in the current directory)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Default, Clone, Copy, Debug)]
struct UserStats {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
}

/// Build folder ancestors like: /a, /a/b, /a/b/c
fn get_folder_ancestors(file_path: &str) -> Vec<String> {
    let folder = Path::new(file_path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string());

    // Normalize path separators and split into parts
    let normalized = folder.replace('\\', "/");
    let parts: Vec<&str> = normalized
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();

    if parts.is_empty() {
        return vec!["/".to_string()];
    }

    // Build cumulative paths: /a, /a/b, /a/b/c
    let mut ancestors = Vec::with_capacity(parts.len());
    let mut current_path = String::new();
    
    for part in parts {
        if current_path.is_empty() {
            current_path = format!("/{}", part);
        } else {
            current_path = format!("{}/{}", current_path, part);
        }
        ancestors.push(current_path.clone());
    }
    
    ancestors
}

fn update_aggregation(
    file_path: &str,
    file_size: u128,
    disk_usage: u128,
    mtime: i64,
    uid: u32,
    aggregation: &mut HashMap<(String, u32), UserStats>,
) {
    for folder in get_folder_ancestors(file_path) {
        let stats = aggregation.entry((folder, uid)).or_default();
        stats.file_count += 1;
        stats.file_size += file_size;
        stats.disk_usage += disk_usage;
        stats.latest_mtime = stats.latest_mtime.max(mtime);
    }
}

fn write_output_csv(
    output_path: &Path,
    aggregation: &HashMap<(String, u32), UserStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)?;

    // Write header comment or actual header if desired
    // writer.write_record(&["path", "uid", "file_count", "file_size", "disk_usage", "latest_mtime"])?;

    for ((path, uid), stats) in aggregation {
        writer.write_record(&[
            path,
            &uid.to_string(),
            &stats.file_count.to_string(),
            &stats.file_size.to_string(),
            &stats.disk_usage.to_string(),
            &stats.latest_mtime.to_string(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

fn count_input_lines(file_path: &Path) -> Result<usize, std::io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().count())
}

fn parse_csv_field<T>(field: Option<&[u8]>, default: T) -> T 
where
    T: std::str::FromStr + Default,
{
    field
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();
    let args = Args::parse();

    // Determine output path
    let output_path = match args.output {
        Some(path) => path,
        None => {
            let stem = args.input
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("output");
            PathBuf::from(format!("{}.agg.csv", stem))
        }
    };

    // Count total lines for progress tracking
    println!("Counting lines in {}", args.input.display());
    let total_lines = count_input_lines(&args.input)?;
    println!("Total lines: {}", total_lines);

    println!("Aggregating data from {}", args.input.display());

    // Main aggregation data structure: (folder_path, uid) -> UserStats
    let mut aggregation: HashMap<(String, u32), UserStats> = HashMap::new();

    // Process CSV file
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true) // Handle varying column counts gracefully
        .from_path(&args.input)?;

    for (line_num, record_result) in csv_reader.byte_records().enumerate() {
        match record_result {
            Ok(record) => {
                // Expected CSV format: "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
                let mtime = parse_csv_field(record.get(2), 0i64);
                let uid = parse_csv_field(record.get(3), 0u32);
                let file_size = parse_csv_field(record.get(6), 0u128);
                let disk_usage = parse_csv_field(record.get(7), 0u128);
                let file_path = record.get(8)
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                    .unwrap_or("");

                update_aggregation(file_path, file_size, disk_usage, mtime, uid, &mut aggregation);
            }
            Err(error) => {
                eprintln!("Warning: Error reading record at line {}: {}", line_num + 1, error);
            }
        }

        // Progress reporting
        if line_num > 0 && line_num % 1_000_000 == 0 && total_lines > 0 {
            let progress_percent = ((line_num as f64 / total_lines as f64) * 100.0).round() as u32;
            println!("{}% - Processed {} lines", progress_percent, line_num);
        }
    }

    // Write results
    write_output_csv(&output_path, &aggregation)?;
    
    let duration = start_time.elapsed();
    println!("✓ Aggregation complete!");
    println!("  Output: {}", output_path.display());
    println!("  Unique (folder, uid) combinations: {}", aggregation.len());
    println!("  Processing time: {:.2} seconds", duration.as_secs_f64());
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_folder_ancestors() {
        assert_eq!(
            get_folder_ancestors("/home/user/docs/file.txt"),
            vec!["/home", "/home/user", "/home/user/docs"]
        );
        
        assert_eq!(
            get_folder_ancestors("file.txt"),
            vec!["/"]
        );
        
        assert_eq!(
            get_folder_ancestors("/file.txt"),
            vec!["/"]
        );
    }

    #[test]
    fn test_parse_csv_field() {
        assert_eq!(parse_csv_field(Some(b"123"), 0i64), 123);
        assert_eq!(parse_csv_field(Some(b"invalid"), 0i64), 0);
        assert_eq!(parse_csv_field(None, 42i64), 42);
    }
}