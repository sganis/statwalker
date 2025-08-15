use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use time::{OffsetDateTime, UtcOffset};

#[derive(Parser, Debug)]
#[command(about = "Resolve statwalker CSV into human fields with aggregation")]
struct Args {
    /// Input CSV produced by statwalker.py
    input: PathBuf,
    /// Output CSV (defaults to <input>.res.csv)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

const OUT_HEADER: &[&str] = &[
    "INODE","ACCESSED","MODIFIED","USER","GROUP","TYPE","PERM","SIZE","DISK","PATH"
];

#[derive(Debug, Clone)]
struct FolderStats {
    file_count: u64,
    disk_usage: u128,
    latest_mtime: i64,
    users: HashSet<String>,
}

impl FolderStats {
    fn new() -> Self {
        Self {
            file_count: 0,
            disk_usage: 0,
            latest_mtime: 0,
            users: HashSet::new(),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let args = Args::parse();
    let input = args.input;
    let output = args
        .output
        .unwrap_or_else(|| {
            let mut path = input.clone();
            let stem = path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("output");
            path.set_file_name(format!("{}.res.csv", stem));
            path
        });

    println!("Resolving file {}", input.display());

    // CSV reader/writer
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(&input)?;
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(&output)?;

    wtr.write_record(OUT_HEADER)?;

    // Caches - matching Python implementation
    let mut time_cache: HashMap<i64, String> = HashMap::new(); // TIMES
    let mut user_cache: HashMap<u32, String> = HashMap::new(); // USERS  
    let mut group_cache: HashMap<u32, String> = HashMap::new(); // GROUPS
    let mut type_cache: HashMap<u32, String> = HashMap::new(); // TYPES
    let mut perm_cache: HashMap<u32, String> = HashMap::new(); // PERMS
    
    // Inode tracking for disk usage deduplication
    let mut inodes_seen: HashSet<String> = HashSet::new(); // INODES
    
    // Aggregation data
    let mut agg_data: HashMap<String, FolderStats> = HashMap::new(); // AGG

    // Try to get local timezone; if it fails, fall back to UTC.
    let local_off = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);

    for rec in rdr.records() {
        let r = rec?;

        // IN: "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
        let inode = r.get(0).unwrap_or("").to_string();
        let atime = parse_i64(r.get(1))?;
        let mtime = parse_i64(r.get(2))?;
        let uid = parse_u32(r.get(3))?;
        let gid = parse_u32(r.get(4))?;
        let mode_raw = parse_u32(r.get(5))?;
        let size_b = parse_u128(r.get(6))?;
        let mut disk_b = parse_u128(r.get(7))?;
        let path = r.get(8).unwrap_or("");

        let accessed = fmt_day(atime, &mut time_cache, local_off);
        let modified = fmt_day(mtime, &mut time_cache, local_off);

        let user = resolve_user(uid, &mut user_cache);
        let group = resolve_group(gid, &mut group_cache);

        let ftype = filetype_from_mode(mode_raw, &mut type_cache);
        let perm = octal_perm(mode_raw, &mut perm_cache);

        // Inode deduplication - only count disk usage once per inode
        if inodes_seen.contains(&inode) {
            disk_b = 0;
        } else {
            inodes_seen.insert(inode.clone());
        }

        // Aggregation - build folder hierarchy statistics
        aggregate_folder_stats(path, disk_b, mtime, &user, &mut agg_data);

        let size_gb = bytes_to_gb(size_b);
        let disk_gb = bytes_to_gb(disk_b);

        // Write with proper CSV quoting for paths
        wtr.write_record(&[
            &inode,
            &accessed,
            &modified,
            &user,
            &group,
            &ftype,
            &perm,
            &size_gb,
            &disk_gb,
            path, // csv crate handles quoting automatically
        ])?;
    }

    wtr.flush()?;
    println!("Resolved -> {}", output.display());

    // Write aggregation file
    let agg_output = format!("{}.agg.csv", input.to_string_lossy());
    write_aggregation_file(&agg_output, &agg_data, &mut time_cache, local_off)?;

    println!("Total resolve time: {:.3} sec.", start.elapsed().as_secs_f64());
    Ok(())
}

fn aggregate_folder_stats(
    path: &str,
    disk: u128,
    mtime: i64,
    user: &str,
    agg_data: &mut HashMap<String, FolderStats>,
) {
    let folder = Path::new(path).parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string());
    
    let parts: Vec<&str> = folder.split('/').filter(|p| !p.is_empty()).collect();
    let mut key = String::new();

    for part in parts {
        key = if key.is_empty() {
            format!("/{}", part)
        } else {
            format!("{}/{}", key, part)
        };

        let stats = agg_data.entry(key.clone()).or_insert_with(FolderStats::new);
        
        stats.file_count += 1;
        stats.disk_usage += disk;
        stats.latest_mtime = stats.latest_mtime.max(mtime);
        stats.users.insert(user.to_string());
    }
}

fn write_aggregation_file(
    filename: &str,
    agg_data: &HashMap<String, FolderStats>,
    time_cache: &mut HashMap<i64, String>,
    local_off: UtcOffset,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(filename)?;

    for (path, stats) in agg_data {
        let users_str = stats.users.iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("|");
        
        let latest_time = fmt_day(stats.latest_mtime, time_cache, local_off);
        
        wtr.write_record(&[
            path,
            &stats.file_count.to_string(),
            &stats.disk_usage.to_string(),
            &latest_time,
            &users_str,
        ])?;
    }

    wtr.flush()?;
    println!("Aggregation -> {}", filename);
    Ok(())
}

// ---------- helpers ----------

fn parse_i64(s: Option<&str>) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(s.unwrap_or("0").trim().parse::<i64>()?)
}

fn parse_u32(s: Option<&str>) -> Result<u32, Box<dyn std::error::Error>> {
    Ok(s.unwrap_or("0").trim().parse::<u32>()?)
}

fn parse_u128(s: Option<&str>) -> Result<u128, Box<dyn std::error::Error>> {
    Ok(s.unwrap_or("0").trim().parse::<u128>()?)
}

fn fmt_day(ts: i64, cache: &mut HashMap<i64, String>, off: time::UtcOffset) -> String {
    if let Some(s) = cache.get(&ts) {
        return s.clone();
    }
    let s = if ts <= 0 {
        "0001-01-01".to_string()
    } else {
        match OffsetDateTime::from_unix_timestamp(ts) {
            Ok(t) => t.to_offset(off).date().to_string(), // YYYY-MM-DD
            Err(_) => "0001-01-01".to_string(),
        }
    };
    cache.insert(ts, s.clone());
    s
}

fn bytes_to_gb(b: u128) -> String {
    // 1 GiB = 1073741824 bytes
    let gb = (b as f64) / 1073741824.0;
    format!("{:.6}", gb)
}

fn resolve_user(uid: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(u) = cache.get(&uid) {
        return u.clone();
    }
    
    // On Windows/cross-platform, we just use numeric IDs
    // This matches the Python fallback behavior
    let name = uid.to_string();
    cache.insert(uid, name.clone());
    name
}

fn resolve_group(gid: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(g) = cache.get(&gid) {
        return g.clone();
    }
    
    // On Windows/cross-platform, we just use numeric IDs
    let name = gid.to_string();
    cache.insert(gid, name.clone());
    name
}

fn filetype_from_mode(mode: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(t) = cache.get(&mode) {
        return t.clone();
    }

    let typ = detect_file_type(mode);
    cache.insert(mode, typ.clone());
    typ
}

fn detect_file_type(mode: u32) -> String {
    // Cross-platform file type detection
    #[cfg(unix)]
    {
        const S_IFMT: u32 = 0o170000;   // File type mask
        const S_IFSOCK: u32 = 0o140000; // Socket
        const S_IFLNK: u32 = 0o120000;  // Symbolic link
        const S_IFREG: u32 = 0o100000;  // Regular file
        const S_IFBLK: u32 = 0o060000;  // Block device
        const S_IFDIR: u32 = 0o040000;  // Directory
        const S_IFCHR: u32 = 0o020000;  // Character device
        const S_IFIFO: u32 = 0o010000;  // FIFO (named pipe)

        let m = mode & S_IFMT;
        match m {
            S_IFREG => "FILE".to_string(),
            S_IFDIR => "DIR".to_string(),
            S_IFLNK => "LINK".to_string(),
            S_IFSOCK => "SOCK".to_string(),
            S_IFIFO => "PIPE".to_string(),
            S_IFBLK => "BDEV".to_string(),
            S_IFCHR => "CDEV".to_string(),
            _ => "UNKNOWN".to_string(),
        }
    }
    
    #[cfg(not(unix))]
    {
        let file_type = mode & 0o170000;
        match file_type {
            0o100000 => "FILE".to_string(),
            0o040000 => "DIR".to_string(),
            0o120000 => "LINK".to_string(),
            0o140000 => "SOCK".to_string(),
            0o010000 => "PIPE".to_string(),
            0o060000 => "BDEV".to_string(),
            0o020000 => "CDEV".to_string(),
            _ => "UNKNOWN".to_string(),
        }
    }
}

fn octal_perm(mode: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(p) = cache.get(&mode) {
        return p.clone();
    }
    
    // Extract permission bits (lower 12 bits including suid/sgid/sticky)
    let imode = mode & 0o7777;
    let perm = format!("{}", imode); // Convert to decimal like Python's int()
    cache.insert(mode, perm.clone());
    perm
}

