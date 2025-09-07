use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf, MAIN_SEPARATOR};
use time::{OffsetDateTime, UtcOffset};
use redb::{Database, TableDefinition, ReadableDatabase};
use bincode::{config, decode_from_slice, encode_to_vec, Decode, Encode};
use serde::Serialize;


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
    // Read a single aggregation row by folder PATH from <stem>.rdb and exit
    #[arg(long, value_name = "DIR")]
    read_agg: Option<String>,
    // List the immediate children of DIR from <stem>.rdb and exit
    #[arg(long, value_name = "DIR")]
    ls: Option<String>,
}

const OUT_HEADER: &[&str] = &[
    "INODE","ACCESSED","MODIFIED","USER","GROUP","TYPE","PERM","SIZE","DISK","PATH","CATEGORY","HASH"
];

#[derive(Debug, Clone)]
struct FolderStats {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
    users: HashSet<String>,
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

#[derive(Encode, Decode, Serialize, Debug)]
struct AggRowBin {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
    users: Vec<String>,
}

const AGG: TableDefinition<&str, &[u8]> = TableDefinition::new("agg");

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
    let agg_csv = PathBuf::from(format!("{}.agg.csv", stem));
    let agg_rdb = PathBuf::from(format!("{}.agg.rdb", stem));

    // Fast path: --ls => scan agg.redb and list children
    if let Some(dir) = args.ls.as_deref() {
        list_children(&agg_rdb, dir)?;
        return Ok(());
    }

    // Fast path: --read-agg => open DB, print row, exit
    if let Some(key) = args.read_agg.as_deref() {
        match read_one(&agg_rdb, key) {
            Ok(Some(row)) => {
                println!("{}", serde_json::to_string_pretty(&row)?);
            }
            Ok(None) => {
                eprintln!("Key not found: {}", key);
            }
            Err(e) => {
                eprintln!("Error reading key {}: {:#}", key, e);
            }
        }
        return Ok(());
    }

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
    let mut inodes_seen: HashSet<String> = HashSet::new();

    // Aggregation
    let mut agg_data: HashMap<String, FolderStats> = HashMap::new();

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
        let size_b = parse_u128(r.get(6), "SIZE", line_no)?;
        let mut disk_b = parse_u128(r.get(7), "DISK", line_no)?;
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
        aggregate_folder_stats(&path, size_b, disk_b, mtime, uid, &mut agg_data);

        let size_gb = bytes_to_gb(size_b);
        let disk_gb = bytes_to_gb(disk_b);

        // Smart quoting - only quote if needed
        let quoted_path = csv_quote_if_needed(&path);

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
            &quoted_path,
            &category,
            &hash,
        ])
        .with_context(|| format!("writing output csv line {} (path: {})", line_no, path))?;
    }

    wtr.flush().context("flushing output csv")?;
    println!("Resolved          -> {}", output.display());

    // write agg CSV (current dir)
    write_aggregation_file(&agg_csv.to_string_lossy(), &agg_data, &mut time_cache, local_off)?;
    println!("Aggregation (csv) -> {}", agg_csv.display());

    // write agg redb (current dir, redb v3)
    save_aggregation_to_redb(&agg_rdb, &agg_data)?;
    println!("Aggregation (rdb) -> {}", agg_rdb.display());

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

// Smart CSV quoting function - only quote when necessary
fn csv_quote_if_needed(s: &str) -> String {
    // Check if we need quoting at all
    let needs_quoting = s.chars().any(|c| c == '"' || c == ',' || c == '\n' || c == '\r');
    
    if !needs_quoting {
        // Fast path - no quoting needed at all
        s.to_string()
    } else {
        // println!("needs quotes: {}", s);
        // assert!(false);

        // Need quoting - check if we also need escaping
        if !s.contains('"') {
            // Quotes needed but no escaping required
            format!("{}", s)
        } else {
            // Need both quoting and escaping
            format!("{}", s.replace('"', "\"\""))
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
    let folder_path = Path::new(path).parent().unwrap_or_else(|| Path::new(""));

    // let folder_str = if folder_path.as_os_str().is_empty() {
    //     ".".to_string()
    // } else {
    //     folder_path.to_string_lossy().to_string()
    // };

    //println!("Aggregating path: {} -> folder: {}", path, folder_str);

    // Build a progressive key path, preserving drive/UNC + root when present.
    let mut comps = folder_path.components();
    let mut key_path = PathBuf::new();

    // Handle optional prefix (e.g. "C:" or "\\server\share") and root ("\")
    if let Some(first) = comps.clone().next() {
        match first {
            Component::Prefix(p) => {
                key_path.push(p.as_os_str());
                comps.next(); // consume prefix
                if let Some(Component::RootDir) = comps.clone().next() {
                    key_path.push(MAIN_SEPARATOR.to_string());
                    comps.next(); // consume root
                }
            }
            Component::RootDir => {
                key_path.push(MAIN_SEPARATOR.to_string());
                comps.next(); // consume root
            }
            _ => {}
        }
    }

    // For each normal component, extend the path and update stats at that level.
    for comp in comps {
        if let Component::Normal(seg) = comp {
            key_path.push(seg);

            let key = key_path.to_string_lossy().to_string();
            let stats = agg_data.entry(key).or_insert_with(FolderStats::new);
            stats.file_count += 1;
            stats.file_size += size;
            stats.disk_usage += disk;
            stats.latest_mtime = stats.latest_mtime.max(mtime);
            stats.users.insert(user);
        }
        // We intentionally skip CurDir ("." ) and ParentDir ("..") here.
    }
}

fn write_aggregation_file(
    filename: &str,
    agg_data: &HashMap<String, FolderStats>,
    _time_cache: &mut HashMap<i64, String>,
    _local_off: UtcOffset,
) -> Result<()> {
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(filename)
        .with_context(|| format!("creating aggregation CSV {}", filename))?;

    for (path, stats) in agg_data {
        let users_str = stats.users.iter().cloned().collect::<Vec<_>>().join("|");
        // let latest_time = fmt_day(stats.latest_mtime, time_cache, local_off);
        
        // Smart quoting for aggregation paths too
        let quoted_path = csv_quote_if_needed(path);

        wtr.write_record(&[
            &quoted_path,
            &stats.file_count.to_string(),
            &stats.file_size.to_string(),
            &stats.disk_usage.to_string(),
            &stats.latest_mtime.to_string(),
            &users_str,
        ])
        .with_context(|| format!("writing aggregation row for {}", path))?;
    }

    wtr.flush().context("flushing aggregation CSV")?;
    Ok(())
}

// -------- redb v3 persistence --------
fn save_aggregation_to_redb(db_path: &Path, agg_data: &HashMap<String, FolderStats>) -> Result<()> {
    let db = Database::create(db_path)
        .with_context(|| format!("opening/creating redb at {}", db_path.display()))?;
    let write = db.begin_write().context("begin redb write txn")?;
    {
        let mut table = write.open_table(AGG).context("open redb table")?;
        let cfg = config::standard().with_fixed_int_encoding(); // v2: choose fixed int encoding

        for (path, stats) in agg_data {
            let row = AggRowBin {
                file_count: stats.file_count,
                file_size: stats.file_size,
                disk_usage: stats.disk_usage,
                latest_mtime: stats.latest_mtime,
                users: stats.users.iter().cloned().collect(),
            };
            let bytes = encode_to_vec(&row, cfg).context("bincode encode")?;
            table
                .insert(path.as_str(), bytes.as_slice())
                .with_context(|| format!("redb insert key {}", path))?;
        }
    }
    write.commit().context("commit redb txn")?;
    Ok(())
}

fn normalize_seps(s: &str) -> String {
    if MAIN_SEPARATOR == '/' { s.replace('\\', "/") } else { s.replace('/', "\\") }
}

fn ensure_trailing_sep(mut s: String) -> String {
    if !s.ends_with(MAIN_SEPARATOR) { s.push(MAIN_SEPARATOR); }
    s
}

fn is_root_dir(p: &str) -> bool {
    use std::path::Component;
    let mut comps = Path::new(p).components();
    match comps.next() {
        Some(Component::Prefix(_)) => matches!(comps.next(), Some(Component::RootDir)) && comps.next().is_none(),
        Some(Component::RootDir) => comps.next().is_none(),
        _ => false,
    }
}

fn first_segment(s: &str) -> &str {
    for (i, ch) in s.char_indices() {
        if ch == '/' || ch == '\\' {
            return &s[..i];
        }
    }
    s
}

fn list_children(db_path: &Path, dir: &str) -> Result<()> {
    let db = Database::open(db_path)?;
    let read = db.begin_read()?;
    let table = read.open_table(AGG)?;

    let mut children = std::collections::BTreeSet::new();

    // Normalize the incoming dir to the platform’s separator.
    let mut dir_norm = normalize_seps(dir.trim());
    // Accept "C:" as root and turn it into "C:\".
    if cfg!(windows) && dir_norm.ends_with(':') {
        dir_norm.push(MAIN_SEPARATOR);
    }

    // Build the scan prefix (always ending with a separator).
    let prefix = if is_root_dir(&dir_norm) {
        ensure_trailing_sep(dir_norm)
    } else {
        let trimmed = dir_norm.trim_end_matches(|c| c == '/' || c == '\\').to_string();
        ensure_trailing_sep(trimmed)
    };

    for entry in table.range(prefix.as_str()..)? {
        let (key_guard, _value) = entry?;
        let key = key_guard.value();

        // Stop once we’ve passed the prefix range.
        if !key.starts_with(&prefix) {
            break;
        }

        let remainder = &key[prefix.len()..];
        let child = first_segment(remainder);
        if !child.is_empty() {
            children.insert(child.to_string());
        }
    }

    for c in &children {
        println!("{c}");
    }
    Ok(())
}


fn read_one(db_path: &std::path::Path, key: &str) -> Result<Option<AggRowBin>> {
    let db = Database::open(db_path)?;
    let read = db.begin_read()?;                      // needs ReadableDatabase in scope
    let table = read.open_table(AGG)?;
    if let Some(val) = table.get(key)? {
        let cfg = bincode::config::standard().with_fixed_int_encoding();
        // supply the 2nd generic, or let it be inferred with `_`
        let (row, _consumed): (AggRowBin, usize) = decode_from_slice::<AggRowBin, _>(val.value(), cfg)?;
        Ok(Some(row))
    } else {
        Ok(None)
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

fn parse_u128(s: Option<&str>, field: &str, line: usize) -> Result<u128> {
    let raw = s.unwrap_or("0").trim();
    raw.parse::<u128>()
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

fn bytes_to_gb(b: u128) -> String {
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
