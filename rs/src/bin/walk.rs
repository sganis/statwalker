// walk.rs
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io::{self, BufWriter, Write, BufRead, BufReader},
    path::{Path, PathBuf},
    time::Instant,
    thread,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering::Relaxed},
    Arc,
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use num_cpus;
use itoa::Buffer;
use clap::{Parser, ColorChoice};
use colored::Colorize;
use chrono::Local;
use zstd::stream::write::Encoder as ZstdEncoder;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

#[cfg(windows)]
use std::time::SystemTime;

// chunk sizes
const READ_BUF_SIZE: usize = 2 * 1024 * 1024; // 2MB for file reads
const FILE_CHUNK: usize = 16384;     // 16k entries per batch (was 8192)
const FLUSH_BYTES: usize = 8 * 1024 * 1024; // 8MB buffer (was 4MB)


#[derive(Parser, Debug)]
#[command(version, color = ColorChoice::Auto)]
struct Args {
    /// Root folder to scan (positional, required)
    root: String,
    /// Output path (default: "<root>.csv" or ".bin" if --bin)
    #[arg(short, long, value_name = "FILE")]
    output: Option<PathBuf>,
    /// Number of worker threads (default: 2×CPU, capped 48)
    #[arg(short = 't', long)]
    threads: Option<usize>,
    /// Skip any folder whose full path contains this substring
    #[arg(long, value_name = "SUBSTR")]
    skip: Option<String>,
    /// Write a binary .zst compressed file instead of .csv
    #[arg(long)]
    bin: bool,
    /// Zero the ATIME field in outputs (CSV & BIN) for testing
    #[arg(long = "skip-atime", aliases = ["no-atime", "zero-atime"])]
    skip_atime: bool,
}

#[derive(Debug)]
enum Task {
    Dir(PathBuf),
    Files { base: std::sync::Arc<PathBuf>, names: Vec<OsString> },
    Shutdown,
}

#[derive(Default)]
struct Stats {
    files: u64,
    errors: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputFormat { Csv, Bin }

#[derive(Clone)]
struct Config {
    skip: Option<String>,
    out_fmt: OutputFormat,
    skip_atime: bool,  // zero ATIME for reproducible outputs
}

struct Row<'a> {
    path: &'a Path,
    dev: u64,
    ino: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    blocks: u64,
    atime: i64,
    mtime: i64,
}

fn main() -> std::io::Result<()> {
    #[cfg(windows)]
    colored::control::set_virtual_terminal(true).unwrap_or(());

    println!("{}","------------------------------------------------".cyan().bold());
    println!("{}", "Statwaker: Super fast filesystem scanner".cyan().bold());
    println!("{}","------------------------------------------------".cyan().bold());

    let start_time = Instant::now();
    let args = Args::parse();
    let out_fmt = if args.bin { OutputFormat::Bin } else { OutputFormat::Csv };
    
    if args.skip_atime {
        eprintln!("{}", "Note: ATIME will be written as 0 for reproducible output (--skip-atime).".yellow());
    }

    // Canonicalize root
    let root = fs::canonicalize(&args.root)?;
    let root_normalized = strip_verbatim_prefix(&root);
    let root_str = root_normalized.display().to_string();

    // Decide default output by out_fmt
    let final_path: PathBuf = match args.output {
        Some(p) => if p.is_absolute() { p } else { std::env::current_dir()?.join(p) },
        None => {
            // choose extension based on mode 
            let ext = match out_fmt {
                OutputFormat::Csv => "csv",
                OutputFormat::Bin => "zst",
            };
            #[cfg(windows)]
            {
                let normalized = strip_verbatim_prefix(&root);
                let name = normalized.to_string_lossy().replace('\\', "-").replace(':', "");
                std::env::current_dir()?.join(format!("{name}.{ext}"))
            }
            #[cfg(not(windows))]
            {
                let name = root_normalized.to_string_lossy().trim_start_matches('/').replace('/', "-");
                std::env::current_dir()?.join(format!("{name}.{ext}"))
            }
        }
    };

    // Ensure the output directory exists and is writable
    let out_dir: PathBuf = final_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or(std::env::current_dir()?);

    if !out_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Output directory does not exist: {}", out_dir.display()),
        ));
    }

    if !out_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Output path is not a directory: {}", out_dir.display()),
        ));
    }

    // Check write access by trying to create a temp file
    let testfile = out_dir.join(".statwalker_write_test");
    match File::create(&testfile) {
        Ok(_) => {
            // Clean up the test file immediately
            let _ = fs::remove_file(&testfile);
        }
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("No write access to directory {}: {e}", out_dir.display()),
            ));
        }
    };

    let threads = args.threads.unwrap_or_else(|| (num_cpus::get()*2).max(4).min(48));
    let cmd: Vec<String> = std::env::args().collect();    
    let now = Local::now();
    let hostname = get_hostname();

    println!("Local time   : {}", now.format("%Y-%m-%d %H:%M:%S").to_string());
    println!("Host         : {}", hostname);
    println!("Command      : {}", cmd.join(" "));
    println!("Input        : {}", &root_str);
    println!("Output       : {}", &final_path.display());
    println!("Temp dir     : {}", out_dir.display());
    println!("Processes    : {}", threads);

    // ---- work queue + inflight counter ----
    let (tx, rx) = unbounded::<Task>();
    let inflight = Arc::new(AtomicUsize::new(0));

    // seed roots
    inflight.fetch_add(1, Relaxed);
    tx.send(Task::Dir(PathBuf::from(root))).expect("enqueue root");

    // shutdown notifier
    {
        let tx = tx.clone();
        let inflight = inflight.clone();
        thread::spawn(move || loop {
            if inflight.load(Relaxed) == 0 {
                for _ in 0..threads {
                    let _ = tx.send(Task::Shutdown);
                }
                break;
            }
            thread::sleep(std::time::Duration::from_millis(10));
        });
    }

    let cfg = Config { 
        skip: args.skip,
        out_fmt,
        skip_atime: args.skip_atime,
    };

    // ---- spawn workers ----
    let mut joins = Vec::with_capacity(threads);
    for tid in 0..threads {
        let rx = rx.clone();
        let tx = tx.clone();
        let inflight = inflight.clone();
        let out_dir = out_dir.clone();
        let cfg = cfg.clone();
        joins.push(thread::spawn(move || worker(tid, rx, tx, inflight, out_dir, cfg)));
    }
    drop(tx);

    // ---- gather stats ----
    let mut total = Stats::default();
    for j in joins {
        let s = j.join().expect("worker panicked");
        total.files += s.files;
        total.errors += s.errors;
    }
    // measure speed before merging
    let speed = (total.files as f64) / start_time.elapsed().as_secs_f64();
    
    // ---- merge shards ----
    merge_shards(&out_dir, &final_path, threads, out_fmt).expect("merge shards failed");

    println!("Total files  : {}", total.files);
    println!("Failed files : {}", total.errors);  
    println!("Elapsed time : {:.2} seconds", start_time.elapsed().as_secs_f64());
    println!("Files/sec.   : {:.2}", speed);
    println!("{}","------------------------------------------------".cyan().bold());
    println!("Done.");
    Ok(())
}

fn get_hostname() -> String {
    hostname::get().ok().and_then(|s| s.into_string().ok()).unwrap_or("noname".to_string())
}

#[cfg(windows)]
fn strip_verbatim_prefix(p: &std::path::Path) -> std::path::PathBuf {
    let s = match p.to_str() {
        Some(s) => s,
        None => return p.to_path_buf(),
    };

    if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{}", rest))
    } else if let Some(rest) = s.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        p.to_path_buf()
    }
}

#[cfg(not(windows))]
fn strip_verbatim_prefix(p: &std::path::Path) -> std::path::PathBuf {
    p.to_path_buf()
}

#[inline]
fn should_skip(path: &Path, skip: Option<&str>) -> bool {
    if let Some(s) = skip {
        path.as_os_str().to_string_lossy().contains(s)
    } else {
        false
    }
}

fn worker(
    tid: usize,
    rx: Receiver<Task>,
    tx: Sender<Task>,
    inflight: Arc<AtomicUsize>,
    out_dir: PathBuf,
    cfg: Config,
) -> Stats {
    let is_bin = cfg.out_fmt == OutputFormat::Bin;
    let hostname = get_hostname();
    let shard_path = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
    let file = File::create(&shard_path).expect("open shard");
    let mut base = BufWriter::with_capacity(32 * 1024 * 1024, file);

    // Header
    if !is_bin {
        base.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n").expect("write csv header"); 
    }

    // Choose writer: zstd encoder for binary; otherwise the base writer
    let mut writer: Box<dyn Write + Send> = if is_bin {
        let enc = ZstdEncoder::new(base, 1).expect("zstd encoder");
        Box::new(enc.auto_finish()) // finalize on drop
    } else {
        Box::new(base)
    };

    // Pre-allocate buffer for record batching
    let mut buf: Vec<u8> = Vec::with_capacity(32 * 1024 * 1024); 

    let mut stats = Stats { files: 0, errors: 0 };

    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,

            Task::Dir(dir) => {
                if should_skip(&dir, cfg.skip.as_deref()) {
                    let _ = inflight.fetch_sub(1, Relaxed);
                    continue;
                }
                if let Some(row) = stat_row(&dir) {
                    if is_bin { 
                        write_row_bin(&mut buf, row, cfg.skip_atime); 
                    } else { 
                        write_row_csv(&mut buf, &row, cfg.skip_atime);
                    }
                    stats.files += 1;
                } else {
                    stats.errors += 1;
                }

                if buf.len() >= FLUSH_BYTES {
                    let _ = writer.write_all(&buf);
                    buf.clear();
                }

                let error_count = enum_dir(&dir, &tx, &inflight, cfg.skip.as_deref());
                stats.errors += error_count;
                inflight.fetch_sub(1, Relaxed);
            }

            Task::Files { base, names } => {
                if should_skip(base.as_ref(), cfg.skip.as_deref()) {
                    inflight.fetch_sub(1, Relaxed);
                    continue;
                }
                for name in names {
                    let full = base.join(&name);
                    match stat_row(&full) {
                        Some(row) => {
                            if is_bin { 
                                write_row_bin(&mut buf, row, cfg.skip_atime); 
                            } else { 
                                write_row_csv(&mut buf, &row, cfg.skip_atime);
                            }
                            stats.files += 1;
                        }
                        None => stats.errors += 1,
                    }
                    if buf.len() >= FLUSH_BYTES {
                        let _ = writer.write_all(&buf);
                        buf.clear();
                    }
                }
                inflight.fetch_sub(1, Relaxed);
            }
        }
    }

    if !buf.is_empty() {
        let _ = writer.write_all(&buf);
    }
    let _ = writer.flush();

    stats
}

fn enum_dir(dir: &Path, tx: &Sender<Task>, inflight: &AtomicUsize, skip: Option<&str>) -> u64 {
    let rd = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(_) => return 1,
    };
    let mut error_count: u64 = 0;
    let mut page: Vec<OsString> = Vec::with_capacity(FILE_CHUNK);
    let base_arc = Arc::new(dir.to_path_buf());

    for dent in rd {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => { error_count += 1; continue; }
        };

        let name = dent.file_name();
        if name == OsStr::new(".") || name == OsStr::new("..") { continue; }

        let file_type = dent.file_type();
        let is_dir = match file_type {
            Ok(ft) => ft.is_dir(),
            Err(_) => {
                match dent.path().is_dir() {
                    true => true,
                    false => { error_count += 1; continue; }
                }
            }
        };

        if is_dir {
            if should_skip(&dent.path(), skip) { continue; }
            inflight.fetch_add(1, Relaxed);
            let _ = tx.send(Task::Dir(dent.path()));
        } else {
            page.push(name);
            if page.len() == FILE_CHUNK {
                inflight.fetch_add(1, Relaxed);
                let _ = tx.send(Task::Files {
                    base: base_arc.clone(),
                    names: std::mem::take(&mut page),
                });
            }
        }
    }

    if !page.is_empty() {
        inflight.fetch_add(1, Relaxed);
        let _ = tx.send(Task::Files { base: base_arc, names: page });
    }

    error_count
}

fn stat_row<'a>(path: &'a Path) -> Option<Row<'a>> {
    let md = fs::symlink_metadata(path).ok()?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(Row {
            path,
            dev: md.dev(),
            ino: md.ino(),
            mode: md.mode(),
            uid: md.uid(),
            gid: md.gid(),
            size: md.size(),
            blocks: md.blocks() as u64,
            atime: md.atime(),
            mtime: md.mtime(),
        })
    }
    #[cfg(windows)]
    {
        let is_file = md.is_file();

        let to_unix_timestamp = |time: SystemTime| -> i64 {
            time.duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        };

        let atime = md.accessed().ok().map(to_unix_timestamp).unwrap_or(0);
        let mtime = md.modified().ok().map(to_unix_timestamp).unwrap_or(0);
        let blocks = (md.len() + 511) / 512;

        let file_attributes = md.file_attributes();
        const FILE_ATTRIBUTE_READONLY: u32 = 0x1;
        
        let mut mode = if is_file { 0o100000 } else { 0o040000 };
        mode |= 0o400; // Owner read
        if (file_attributes & FILE_ATTRIBUTE_READONLY) == 0 { mode |= 0o200; }
        if is_file {
            if let Some(ext) = path.extension() {
                match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                    "exe" | "bat" | "cmd" | "com" | "scr" | "ps1" | "vbs" => mode |= 0o100,
                    _ => {}
                }
            }
        } else {
            mode |= 0o100;
        }
        let owner_perms = mode & 0o700;
        mode |= (owner_perms >> 3) | (owner_perms >> 6);

        Some(Row {
            path, dev: 0, ino: 0, mode, uid: 0, gid: 0,
            size: md.len(), blocks, atime, mtime,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        Some(Row {
            path, dev: 0, ino: 0, mode: 0, uid: 0, gid: 0,
            size: md.len(), blocks: 0, atime: 0, mtime: 0,
        })
    }
}

// Pre-allocate formatters to avoid repeated allocation
thread_local! {
    static U32_BUFFER: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
    static U64_BUFFER: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
    static I64_BUFFER: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
}

// ----- CSV writing -----
fn write_row_csv(buf: &mut Vec<u8>, r: &Row<'_>, skip_atime: bool) {
    buf.reserve(256);
    // INODE as dev-ino
    push_u64(buf, r.dev);
    buf.push(b'-');
    push_u64(buf, r.ino);
    push_comma(buf);

    // ATIME (zeroed if requested)
    if skip_atime { push_i64(buf, 0); } else { push_i64(buf, r.atime); }
    push_comma(buf);   

    // MTIME
    push_i64(buf, r.mtime); 
    push_comma(buf);

    // UID, GID, MODE
    push_u32(buf, r.uid);   
    push_comma(buf);
    push_u32(buf, r.gid);   
    push_comma(buf);
    push_u32(buf, r.mode);  
    push_comma(buf);

    // SIZE, DISK
    push_u64(buf, r.size);  
    push_comma(buf);
    let disk = r.blocks * 512;
    push_u64(buf, disk); 
    push_comma(buf);

    csv_push_path_smart_quoted(buf, r.path);
    buf.push(b'\n');
}

// ----- BIN writing -----
fn write_row_bin(buf: &mut Vec<u8>, r: Row<'_>, skip_atime: bool) {
    buf.reserve(64 + 2 * r.path.as_os_str().len());

    #[cfg(unix)]
    let path_bytes: Vec<u8> = {
        use std::os::unix::ffi::OsStrExt;
        r.path.as_os_str().as_bytes().to_vec()
    };
    #[cfg(not(unix))]
    let path_bytes: Vec<u8> = r.path.to_string_lossy().as_bytes().to_vec();

    let path_len = path_bytes.len() as u32;
    let atime = if skip_atime { 0i64 } else { r.atime };
    let disk = r.blocks * 512;
    
    buf.extend_from_slice(&path_len.to_le_bytes());
    buf.extend_from_slice(&path_bytes);
    buf.extend_from_slice(&r.dev.to_le_bytes());
    buf.extend_from_slice(&r.ino.to_le_bytes());
    buf.extend_from_slice(&atime.to_le_bytes());
    buf.extend_from_slice(&r.mtime.to_le_bytes());
    buf.extend_from_slice(&r.uid.to_le_bytes());
    buf.extend_from_slice(&r.gid.to_le_bytes());
    buf.extend_from_slice(&r.mode.to_le_bytes());
    buf.extend_from_slice(&r.size.to_le_bytes());
    buf.extend_from_slice(&disk.to_le_bytes());
}

fn csv_push_path_smart_quoted(buf: &mut Vec<u8>, p: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let bytes = p.as_os_str().as_bytes();
        csv_push_bytes_smart_quoted(buf, bytes);
    }
    #[cfg(not(unix))]
    {
        let s = p.to_string_lossy();
        csv_push_str_smart_quoted(buf, &s);
    }
}

#[cfg(unix)]
fn csv_push_bytes_smart_quoted(buf: &mut Vec<u8>, bytes: &[u8]) {
    let needs_quoting = bytes.iter().any(|&b| b == b'"' || b == b',' || b == b'\n' || b == b'\r');
    if !needs_quoting {
        buf.extend_from_slice(bytes);
    } else {
        buf.push(b'"');
        if !bytes.contains(&b'"') {
            buf.extend_from_slice(bytes);
        } else {
            buf.reserve(bytes.len() + bytes.iter().filter(|&&b| b == b'"').count());
            for &b in bytes {
                if b == b'"' {
                    buf.push(b'"');
                    buf.push(b'"');
                } else {
                    buf.push(b);
                }
            }
        }
        buf.push(b'"');
    }
}

#[cfg(windows)]
fn csv_push_str_smart_quoted(buf: &mut Vec<u8>, s: &str) {
    let normalized = if s.starts_with(r"\\?\") {
        if s.starts_with(r"\\?\UNC\") { format!(r"\\{}", &s[8..]) } else { s[4..].to_string() }
    } else { s.to_string() };
    let display_str = normalized.as_str();
    let needs_quoting = display_str.chars().any(|c| c == '"' || c == ',' || c == '\n' || c == '\r');
    if !needs_quoting {
        buf.extend_from_slice(display_str.as_bytes());
    } else {
        buf.push(b'"');
        if !display_str.contains('"') {
            buf.extend_from_slice(display_str.as_bytes());
        } else {
            let quote_count = display_str.matches('"').count();
            buf.reserve(display_str.len() + quote_count);
            for b in display_str.bytes() {
                if b == b'"' {
                    buf.push(b'"'); buf.push(b'"');
                } else {
                    buf.push(b);
                }
            }
        }
        buf.push(b'"');
    }
}

// ---- Merge shards (CSV or BIN) ----
fn merge_shards(
    out_dir: &Path, 
    final_path: &Path, 
    threads: usize, 
    out_fmt: OutputFormat,
) -> std::io::Result<()> {
    let mut out = BufWriter::with_capacity(16 * 1024 * 1024, File::create(&final_path)?);

    match out_fmt {
        OutputFormat::Csv => merge_shards_csv(out_dir, &mut out, threads),
        OutputFormat::Bin => merge_shards_bin(out_dir, &mut out, threads),
    }?;

    out.flush()?;
    Ok(())
}

fn merge_shards_csv(out_dir: &Path, out: &mut BufWriter<File>, threads: usize) -> std::io::Result<()> {
    out.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n")?;
    let hostname = get_hostname();
    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
        if !shard.exists() { continue; }
        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);

        // Skip shard header line
        let mut first_line = Vec::<u8>::with_capacity(128);
        reader.read_until(b'\n', &mut first_line)?; // discard header

        io::copy(&mut reader, out)?;
        let _ = fs::remove_file(shard);
    }
    Ok(())
}

fn merge_shards_bin(
    out_dir: &Path, 
    out: &mut BufWriter<File>, 
    threads: usize, 
) -> std::io::Result<()> {
    let hostname = get_hostname();
    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
        if !shard.exists() { continue; }
        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
        io::copy(&mut reader, out)?;
        let _ = fs::remove_file(shard);
    }

    Ok(())
}

#[inline] fn push_comma(buf: &mut Vec<u8>) { buf.push(b','); }
#[inline]
fn push_u32(out: &mut Vec<u8>, v: u32) {
    U32_BUFFER.with(|b| {
        let mut binding = b.borrow_mut();
        let formatted = binding.format(v);
        out.extend_from_slice(formatted.as_bytes());
    });
}
#[inline]
fn push_u64(out: &mut Vec<u8>, v: u64) {
    U64_BUFFER.with(|b| {
        let mut binding = b.borrow_mut();
        let formatted = binding.format(v);
        out.extend_from_slice(formatted.as_bytes());
    });
}
#[inline]
fn push_i64(out: &mut Vec<u8>, v: i64) {
    I64_BUFFER.with(|b| {
        let mut binding = b.borrow_mut();
        let formatted = binding.format(v);
        out.extend_from_slice(formatted.as_bytes());
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::tempdir;

    #[test]
    fn test_should_skip() {
        let p = PathBuf::from("/a/b/c/d");
        assert!(super::should_skip(&p, Some("b/c")));
        assert!(!super::should_skip(&p, Some("x")));
        assert!(!super::should_skip(&p, None));
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_fast_path() {
        let mut buf = Vec::new();
        super::csv_push_bytes_smart_quoted(&mut buf, b"abc_def");
        assert_eq!(&buf, b"abc_def");
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_comma() {
        let mut buf = Vec::new();
        super::csv_push_bytes_smart_quoted(&mut buf, b"a,b");
        assert_eq!(&buf, b"\"a,b\"");
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_quote() {
        let mut buf = Vec::new();
        super::csv_push_bytes_smart_quoted(&mut buf, b"a\"b");
        assert_eq!(&buf, b"\"a\"\"b\"");
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_newline() {
        let mut buf = Vec::new();
        super::csv_push_bytes_smart_quoted(&mut buf, b"a\nb");
        assert_eq!(&buf, b"\"a\nb\"");
    }

    #[cfg(windows)]
    #[test]
    fn test_csv_push_str_smart_quoted_normalize_verbatim() {
        let mut buf = Vec::new();
        super::csv_push_str_smart_quoted(&mut buf, r"\\?\C:\foo\bar");
        assert_eq!(std::str::from_utf8(&buf).unwrap(), r"C:\foo\bar");

        let mut buf2 = Vec::new();
        super::csv_push_str_smart_quoted(&mut buf2, r"\\?\UNC\server\share\foo");
        assert_eq!(std::str::from_utf8(&buf2).unwrap(), r"\\server\share\foo");
    }

    #[test]
    fn test_merge_shards_csv_unsorted_only() -> std::io::Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path = out_dir.join("out_unsorted.csv");

        // Create 2 CSV shard files with headers
        let shard0 = out_dir.join(format!("shard_{}_0.tmp", get_hostname()));
        let shard1 = out_dir.join(format!("shard_{}_1.tmp", get_hostname()));

        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\nb\n")?;
        }
        {
            let mut w = File::create(&shard1)?;
            w.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\na\n")?;
        }

        // Call with correct signature
        super::merge_shards(&out_dir, &final_path, 2, OutputFormat::Csv)?;

        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let mut lines: Vec<&str> = s.lines().collect();

        assert_eq!(lines.remove(0), "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        // Order is just concatenation of shards
        assert_eq!(lines, vec!["b", "a"]);
        Ok(())
    }

    // Test for skip_atime functionality
    #[test]
    fn test_skip_atime_in_csv() {
        let mut buf = Vec::new();
        let dummy_path = Path::new("/test/path");
        let row = Row {
            path: dummy_path,
            dev: 1,
            ino: 2,
            mode: 33188,
            uid: 1000,
            gid: 1000,
            size: 1024,
            blocks: 2,
            atime: 1609459200, // 2021-01-01 00:00:00 UTC
            mtime: 1609545600, // 2021-01-02 00:00:00 UTC
        };

        // With ATIME included
        write_row_csv(&mut buf, &row, false);
        let output_with_atime = String::from_utf8(buf.clone()).unwrap();
        assert!(output_with_atime.contains("1609459200"));
        assert!(output_with_atime.contains("1609545600"));

        // With ATIME skipped (zeroed)
        buf.clear();
        write_row_csv(&mut buf, &row, true);
        let output_without_atime = String::from_utf8(buf).unwrap();
        assert!(output_without_atime.contains(",0,1609545600")); // ATIME zeroed, MTIME present
    }
}
