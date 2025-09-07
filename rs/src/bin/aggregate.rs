use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::io::{BufRead, BufReader};
use std::fs::File;
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::HashSet;


use chrono::{DateTime, NaiveDateTime, Utc};

fn parse_to_unix_time(date_str: &str) -> i64 {
    let naive_date = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S%.6f")
        .expect("Failed to parse date string");
    let datetime: DateTime<Utc> = DateTime::from_utc(naive_date, Utc);
    datetime.timestamp()
}


#[derive(Parser, Debug)]
#[command(about = "Aggregate statwalker CSV into human fields")]
struct Args {
    /// Input CSV produced by statwalker.py
    input: PathBuf,
    /// Output CSV (defaults to <stem>.agg.csv in the current directory)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct FolderStats {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
    users: HashSet<u32>,
}

impl FolderStats {
    fn new() -> Self {
        Self {
            file_count: 0,
            file_size: 0,
            disk_usage: 0,
            latest_mtime: 0,
            users: HashSet::new(),
        }
    }
}

fn aggregate_folder_stats(
    path: &str,
    size: u128,
    disk: u128,
    mtime: i64,
    user: u32,
    agg_data: &mut HashMap<String, FolderStats>,
) {
    let folder = std::path::Path::new(path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string());

    let parts: Vec<&str> = folder.split('/').filter(|p| !p.is_empty()).collect();
    let mut key = String::new();

    //println!("{:?}", key);

    for part in parts {
        key = if key.is_empty() {
            format!("/{}", part)
        } else {
            format!("{}/{}", key, part)
        };

        let stats = agg_data.entry(key.clone()).or_insert_with(FolderStats::new);
        stats.file_count += 1;
        stats.file_size += size;
        stats.disk_usage += disk;
        stats.latest_mtime = stats.latest_mtime.max(mtime);
        stats.users.insert(user);
    }
}

fn write_aggregation_file(
    filename: &str,
    agg_data: &HashMap<String, FolderStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(filename)?;

    for (path, stats) in agg_data {

        let users_str = stats.users.iter().map(|u| u.to_string()).collect::<Vec<_>>().join("|");

        wtr.write_record(&[
            &path,
            &stats.file_count.to_string(),
            &stats.file_size.to_string(),
            &stats.disk_usage.to_string(),
            &stats.latest_mtime.to_string(),
            &users_str,
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

    println!("Aggregating file {}", input.display());

    let mut agg_data: HashMap<String, FolderStats> = HashMap::new();

    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(&input)?;

    for (idx, rec) in rdr.byte_records().enumerate() {
        match rec {
            Ok(r) => {
                //print!("{:?}", r);

                // "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
                //let mtime = parse_to_unix_time(std::str::from_utf8(r.get(2).unwrap_or(&[])).unwrap_or("0"));
                let mtime = std::str::from_utf8(r.get(2).unwrap_or(&[])).unwrap_or("0").parse::<i64>().unwrap_or(0);
                let user = std::str::from_utf8(r.get(3).unwrap_or(&[])).unwrap_or("0").parse::<u32>().unwrap_or(0);
                let size = std::str::from_utf8(r.get(6).unwrap_or(&[])).unwrap_or("0").parse::<u128>().unwrap_or(0);
                let disk = std::str::from_utf8(r.get(7).unwrap_or(&[])).unwrap_or("0").parse::<u128>().unwrap_or(0);
                let path = std::str::from_utf8(r.get(8).unwrap_or(&[])).unwrap_or("");
                
                aggregate_folder_stats(path, size, disk, mtime, user, &mut agg_data);

            }
            Err(ref e) => {
                println!("Error at record {}: {:?}, {:?}", idx, e, rec);
            }
        }
        if idx % 1_000_000 == 0 {
            let percent = (idx as f32 / total as f32 * 100.0).round() as i32;
            println!("{}% Read {} lines, total: {}", percent, idx, total);
        }
    }

    write_aggregation_file(&output.to_string_lossy(), &agg_data)?;

    println!("Aggregated -> {}", output.display());

    println!("Total aggregation time: {:.3} sec.", start.elapsed().as_secs_f64());
    Ok(())
}
