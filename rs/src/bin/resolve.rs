use anyhow::{Context, Result};
use clap::{Parser, ColorChoice};
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::HashMap;
use std::path::PathBuf;
use chrono::{Local, TimeZone, Utc};

#[cfg(unix)]
use std::ffi::CStr;

#[derive(Parser, Debug)]
#[command(author, version, color = ColorChoice::Always, 
    about = "Resolve statwalker CSV into human fields")]
struct Args {
    /// Input CSV produced by statwalker.py
    input: PathBuf,
    /// Output CSV (defaults to <stem>.res.csv in the current directory)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

const OUT_HEADER: &[&str] = &[
    "INODE","ACCESSED","MODIFIED","USER","GROUP","TYPE","PERM","SIZE","DISK","PATH"
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

    let mut out_rec = csv::ByteRecord::new();

    for (idx, rec) in rdr.byte_records().enumerate() {
        let line_no = idx + 2; // header + 1-based
        let r = rec.with_context(|| format!("reading csv record at line {}", line_no))?;

        // IN: b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
        let inode_b = r.get(0).unwrap_or(b"");
        let atime   = parse_i64_bytes(r.get(1));
        let mtime   = parse_i64_bytes(r.get(2));
        let uid     = parse_u32_bytes(r.get(3));
        let gid     = parse_u32_bytes(r.get(4));
        let mode    = parse_u32_bytes(r.get(5));
        let size    = r.get(6).unwrap_or(b"0");
        let disk    = r.get(7).unwrap_or(b"0");
        let path_b  = r.get(8).unwrap_or(b""); // raw bytes (may be non-UTF-8)

        // Humanized fields (Strings)
        let accessed = fmt_day(atime, &mut time_cache);
        let modified = fmt_day(mtime, &mut time_cache);
        let user     = resolve_user(uid,  &mut user_cache);
        let group    = resolve_group(gid, &mut group_cache);
        let ftype    = filetype_from_mode(mode, &mut type_cache);
        let perm     = octal_perm(mode, &mut perm_cache);

        // Convert PATH to UTF-8, ignoring errors (invalid bytes -> U+FFFD)
        let path_utf8 = std::string::String::from_utf8_lossy(path_b); // Cow<str>

        // Build output record (mix of original bytes + our UTF-8 strings)
        out_rec.clear();
        out_rec.push_field(inode_b);
        out_rec.push_field(accessed.as_bytes());
        out_rec.push_field(modified.as_bytes());
        out_rec.push_field(user.as_bytes());
        out_rec.push_field(group.as_bytes());
        out_rec.push_field(ftype.as_bytes());
        out_rec.push_field(perm.as_bytes());
        out_rec.push_field(size);
        out_rec.push_field(disk);
        out_rec.push_field(path_utf8.as_bytes()); // UTF-8 path only

        wtr.write_byte_record(&out_rec)
            .with_context(|| format!("writing output csv line {} (path utf8)", line_no))?;
    }

    wtr.flush().context("flushing output csv")?;
    println!("Output       : {}", output.display());
    println!("Elapsed time : {:.3} sec.", start.elapsed().as_secs_f64());
    Ok(())
}

#[inline]
fn parse_i64_bytes(b: Option<&[u8]>) -> i64 {
    let s = trim_ascii(b.unwrap_or(b"0"));
    atoi::atoi::<i64>(s).unwrap_or(0)
}

#[inline]
fn parse_u32_bytes(b: Option<&[u8]>) -> u32 {
    let s = trim_ascii(b.unwrap_or(b"0"));
    atoi::atoi::<u32>(s).unwrap_or(0)
}

#[inline]
fn trim_ascii(mut s: &[u8]) -> &[u8] {
    while !s.is_empty() && s[0].is_ascii_whitespace() { s = &s[1..]; }
    while !s.is_empty() && s[s.len() - 1].is_ascii_whitespace() { s = &s[..s.len() - 1]; }
    s
}


fn fmt_day(ts: i64, cache: &mut HashMap<i64, String>) -> String {
    if let Some(s) = cache.get(&ts) {
        return s.clone();
    }
    let s = if ts <= 0 {
        "0001-01-01".to_string()
    } else {
        // Build UTC from the UNIX seconds, then convert to local and format.
        match Utc.timestamp_opt(ts, 0).single() {
            Some(t) => t.with_timezone(&Local).format("%Y-%m-%d").to_string(),
            None => "0001-01-01".to_string(),
        }
    };
    cache.insert(ts, s.clone());
    s
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


