use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::HashMap;
use std::path::PathBuf;
use time::{OffsetDateTime, UtcOffset};

#[cfg(unix)]
use std::ffi::CStr;

#[derive(Parser, Debug)]
#[command(about = "Resolve statwalker CSV into human fields with aggregation + redb index")]
struct Args {
    /// Input CSV produced by statwalker.py
    input: PathBuf,
    /// Output CSV (defaults to <stem>.res.csv in the current directory)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

const OUT_HEADER: &[&str] = &[
    "INODE","ACCESSED","MODIFIED","USER","GROUP","TYPE","PERM","SIZE","DISK","PATH","CATEGORY","HASH"
];

fn main() -> Result<()> {
    let start = std::time::Instant::now();
    let args = Args::parse();
    let input = args.input;

    // --- outputs in the current working directory ---
    let stem = input
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let output = args
        .output
        .unwrap_or_else(|| PathBuf::from(format!("{}.res.csv", stem)));

    println!("Resolving file {}", input.display());

    // CSV reader/writer
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(&input)
        .with_context(|| format!("opening input csv {}", input.display()))?;
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(&output)
        .with_context(|| format!("creating output csv {}", output.display()))?;

    wtr.write_record(OUT_HEADER)
        .context("writing output csv header")?;

    // Caches
    let mut time_cache: HashMap<i64, String> = HashMap::new(); // TIMES
    let mut user_cache: HashMap<u32, String> = HashMap::new(); // USERS
    let mut group_cache: HashMap<u32, String> = HashMap::new(); // GROUPS
    let mut type_cache: HashMap<u32, String> = HashMap::new(); // TYPES
    let mut perm_cache: HashMap<u32, String> = HashMap::new(); // PERMS

    // Inode dedup for disk usage
    //let mut inodes_seen: HashSet<String> = HashSet::new();

    // local tz or UTC fallback
    let local_off = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);

    for (idx, rec) in rdr.records().enumerate() {
        let line_no = idx + 2; // header + 1-based
        let r = rec.with_context(|| format!("reading csv record at line {}", line_no))?;

        // IN: "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
        let inode = r.get(0).unwrap_or("").to_string();
        let atime = parse_i64(r.get(1), "ATIME", line_no)?;
        let mtime = parse_i64(r.get(2), "MTIME", line_no)?;
        let uid = parse_u32(r.get(3), "UID", line_no)?;
        let gid = parse_u32(r.get(4), "GID", line_no)?;
        let mode_raw = parse_u32(r.get(5), "MODE", line_no)?;
        let size_b = parse_u64(r.get(6), "SIZE", line_no)?;
        let disk_b = parse_u64(r.get(7), "DISK", line_no)?;
        let path_raw = r.get(8).unwrap_or("");
        
        // Unquote the path if it's already quoted (from input CSV)
        let path = unquote_csv_field(path_raw);

        let category = r.get(9).unwrap_or("").to_string();
        let hash = r.get(10).unwrap_or("").to_string();

        let accessed = fmt_day(atime, &mut time_cache, local_off);
        let modified = fmt_day(mtime, &mut time_cache, local_off);

        let user = resolve_user(uid, &mut user_cache);
        let group = resolve_group(gid, &mut group_cache);

        let ftype = filetype_from_mode(mode_raw, &mut type_cache);
        let perm = octal_perm(mode_raw, &mut perm_cache);

        // dedup disk by inode
        // if inodes_seen.contains(&inode) {
        //     disk_b = 0;
        // } else {
        //     inodes_seen.insert(inode.clone());
        // }

        // aggregation - use unquoted path
        //aggregate_folder_stats(&path, size_b, disk_b, mtime, uid, &mut agg_data);

        // let size_gb = bytes_to_gb(size_b);
        // let disk_gb = bytes_to_gb(disk_b);

        // Smart quoting - only quote if needed
        //let quoted_path = csv_quote_if_needed(&path);

        wtr.write_record(&[
            &inode,
            &accessed,
            &modified,
            &user,
            &group,
            &ftype,
            &perm,
            &size_b.to_string(),
            &disk_b.to_string(),
            &path,
            &category,
            &hash,
        ])
        .with_context(|| format!("writing output csv line {} (path: {})", line_no, path))?;
    }

    wtr.flush().context("flushing output csv")?;
    println!("Resolved          -> {}", output.display());

    println!("Total resolve time: {:.3} sec.", start.elapsed().as_secs_f64());
    Ok(())
}

// Unquote CSV field if it's wrapped in quotes
fn unquote_csv_field(field: &str) -> String {
    let trimmed = field.trim();
    
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        // Remove outer quotes and unescape inner quotes
        let inner = &trimmed[1..trimmed.len()-1];
        inner.replace("\"\"", "\"")
    } else {
        // Not quoted, return as-is
        trimmed.to_string()
    }
}

// ---------- helpers ----------
fn parse_i64(s: Option<&str>, field: &str, line: usize) -> Result<i64> {
    let raw = s.unwrap_or("0").trim();
    raw.parse::<i64>()
        .with_context(|| format!("parsing field `{}`='{}' at line {}", field, raw, line))
}

fn parse_u32(s: Option<&str>, field: &str, line: usize) -> Result<u32> {
    let raw = s.unwrap_or("0").trim();
    raw.parse::<u32>()
        .with_context(|| format!("parsing field `{}`='{}' at line {}", field, raw, line))
}

fn parse_u64(s: Option<&str>, field: &str, line: usize) -> Result<u64> {
    let raw = s.unwrap_or("0").trim();
    raw.parse::<u64>()
        .with_context(|| format!("parsing field `{}`='{}' at line {}", field, raw, line))
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

fn _bytes_to_gb(b: u128) -> String {
    // 1 GiB = 1073741824 bytes
    let gb = (b as f64) / 1073741824.0;
    format!("{:.6}", gb)
}

fn resolve_user(uid: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(u) = cache.get(&uid) {
        return u.clone();
    }
    let name = get_username_from_uid(uid);
    cache.insert(uid, name.clone());
    name
}

fn resolve_group(gid: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(g) = cache.get(&gid) {
        return g.clone();
    }
    let name = get_groupname_from_gid(gid);
    cache.insert(gid, name.clone());
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

#[cfg(unix)]
fn get_groupname_from_gid(gid: u32) -> String {
    unsafe {
        let group = libc::getgrgid(gid);
        if group.is_null() {
            return "UNK".to_string();
        }
        let name_ptr = (*group).gr_name;
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
    uid.to_string() 
}

#[cfg(not(unix))]
fn get_groupname_from_gid(gid: u32) -> String { 
    gid.to_string() 
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
    #[cfg(unix)]
    {
        const S_IFMT: u32 = 0o170000;
        const S_IFSOCK: u32 = 0o140000;
        const S_IFLNK: u32 = 0o120000;
        const S_IFREG: u32 = 0o100000;
        const S_IFBLK: u32 = 0o060000;
        const S_IFDIR: u32 = 0o040000;
        const S_IFCHR: u32 = 0o020000;
        const S_IFIFO: u32 = 0o010000;

        match mode & S_IFMT {
            S_IFREG => "FILE".to_string(),
            S_IFDIR => "DIR".to_string(),
            S_IFLNK => "LINK".to_string(),
            S_IFSOCK => "SOCK".to_string(),
            S_IFIFO => "PIPE".to_string(),
            S_IFBLK => "BDEV".to_string(),
            S_IFCHR => "CDEV".to_string(),
            _ => "UNK".to_string(),
        }
    }
    #[cfg(not(unix))]
    {
        match mode & 0o170000 {
            0o100000 => "FILE".to_string(),
            0o040000 => "DIR".to_string(),
            0o120000 => "LINK".to_string(),
            0o140000 => "SOCK".to_string(),
            0o010000 => "PIPE".to_string(),
            0o060000 => "BDEV".to_string(),
            0o020000 => "CDEV".to_string(),
            _ => "UNK".to_string(),
        }
    }
}

fn octal_perm(mode: u32, cache: &mut HashMap<u32, String>) -> String {
    if let Some(p) = cache.get(&mode) {
        return p.clone();
    }
    let imode = mode & 0o7777;
    let perm = format!("{:o}", imode);  // <-- octal formatting
    cache.insert(mode, perm.clone());
    perm
}
