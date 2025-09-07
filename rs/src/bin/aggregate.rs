use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::io::{BufRead, BufReader};
use std::fs::File;
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};

use chrono::{DateTime, NaiveDateTime, Utc};

fn parse_to_unix_time(date_str: &str) -> i64 {
    let naive_date = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S%.6f")
        .expect("Failed to parse date string");
    let datetime: DateTime<Utc> = DateTime::from_utc(naive_date, Utc);
    datetime.timestamp()
}

#[derive(Parser, Debug)]
#[command(about = "Aggregate statwalker CSV into per-(folder, uid) rows")]
struct Args {
    /// Input CSV produced by statwalker.py
    input: PathBuf,
    /// Output CSV (defaults to <stem>.usernorm.csv in the current directory)
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
fn ancestors_for(path: &str) -> Vec<String> {
    let folder = std::path::Path::new(path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string());

    // Normalize Windows backslashes to slashes, then split
    let folder = folder.replace('\\', "/");
    let parts: Vec<&str> = folder.split('/').filter(|p| !p.is_empty()).collect();

    if parts.is_empty() {
        return vec!["/".to_string()];
    }

    let mut out = Vec::with_capacity(parts.len());
    let mut key = String::new();
    for part in parts {
        key = if key.is_empty() {
            format!("/{}", part)
        } else {
            format!("{}/{}", key, part)
        };
        out.push(key.clone());
    }
    out
}

fn aggregate_per_user_folder(
    path: &str,
    size: u128,
    disk: u128,
    mtime: i64,
    uid: u32,
    agg: &mut HashMap<(String, u32), UserStats>,
) {
    for folder in ancestors_for(path) {
        let entry = agg.entry((folder, uid)).or_default();
        entry.file_count += 1;
        entry.file_size += size;
        entry.disk_usage += disk;
        if mtime > entry.latest_mtime {
            entry.latest_mtime = mtime;
        }
    }
}

fn write_usernorm_file(
    filename: &str,
    agg: &HashMap<(String, u32), UserStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(filename)?;

    // path,uid,file_count,file_size,disk_usage,latest_mtime
    for ((path, uid), stats) in agg {
        wtr.write_record(&[
            path,
            &uid.to_string(),
            &stats.file_count.to_string(),
            &stats.file_size.to_string(),
            &stats.disk_usage.to_string(),
            &stats.latest_mtime.to_string(),
        ])?;
    }

    wtr.flush()?;
    Ok(())
}

fn count_lines<P: AsRef<Path>>(path: P) -> Result<usize, std::io::Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let count = reader.lines().count();
    Ok(count)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let args = Args::parse();
    let input = args.input;

    let stem = input
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let output = args
        .output
        .unwrap_or_else(|| PathBuf::from(format!("{}.agg.csv", stem)));

    println!("Counting lines {}", input.display());
    let total = count_lines(Path::new(&input))?;
    println!("total lines: {}", total);

    println!("Aggregating (per folder, per user) {}", input.display());

    // (folder_path, uid) -> stats
    let mut agg: HashMap<(String, u32), UserStats> = HashMap::new();

    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(&input)?;

    for (idx, rec) in rdr.byte_records().enumerate() {
        match rec {
            Ok(r) => {
                // "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
                // let mtime = parse_to_unix_time(str_mtime); // if you need datetime format
                let mtime = std::str::from_utf8(r.get(2).unwrap_or(&[])).unwrap_or("0").parse::<i64>().unwrap_or(0);
                let uid = std::str::from_utf8(r.get(3).unwrap_or(&[])).unwrap_or("0").parse::<u32>().unwrap_or(0);
                let size = std::str::from_utf8(r.get(6).unwrap_or(&[])).unwrap_or("0").parse::<u128>().unwrap_or(0);
                let disk = std::str::from_utf8(r.get(7).unwrap_or(&[])).unwrap_or("0").parse::<u128>().unwrap_or(0);
                let path = std::str::from_utf8(r.get(8).unwrap_or(&[])).unwrap_or("");

                aggregate_per_user_folder(path, size, disk, mtime, uid, &mut agg);
            }
            Err(ref e) => {
                println!("Error at record {}: {:?}, {:?}", idx, e, rec);
            }
        }

        if idx % 1_000_000 == 0 && idx > 0 && total > 0 {
            let percent = (idx as f32 / total as f32 * 100.0).round() as i32;
            println!("{}% Read {} lines, total: {}", percent, idx, total);
        }
    }

    write_usernorm_file(&output.to_string_lossy(), &agg)?;
    println!("Aggregated (per-user normalized) -> {}", output.display());
    println!("Total aggregation time: {:.3} sec.", start.elapsed().as_secs_f64());
    Ok(())
}
