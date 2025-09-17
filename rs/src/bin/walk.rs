// walk.rs - Performance Optimized Version
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io::{self, BufWriter, Write, BufRead, BufReader, Read},
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

// Optimized chunk sizes based on testing
const READ_BUF_SIZE: usize = 4 * 1024 * 1024; // 4MB for file reads (increased)
const FILE_CHUNK: usize = 32 * 1024;     // 32k entries per batch (doubled)
const FLUSH_BYTES: usize = 16 * 1024 * 1024; // 16MB buffer (doubled)
const BUF_CAPACITY: usize = 64 * 1024 * 1024; // 64MB initial buffer

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
    Files { base: Arc<PathBuf>, names: Vec<OsString> },
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
    out_fmt: OutputFormat,
    skip_atime: bool,
}

// Pre-computed skip pattern for faster matching
#[derive(Clone)]
struct SkipMatcher {
    pattern: Option<Vec<u8>>,
}

impl SkipMatcher {
    fn new(skip: Option<String>) -> Self {
        Self {
            pattern: skip.map(|s| s.into_bytes()),
        }
    }

    #[inline]
    fn should_skip(&self, path: &Path) -> bool {
        if let Some(pattern) = &self.pattern {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt;
                let path_bytes = path.as_os_str().as_bytes();
                // Use Boyer-Moore-like approach for larger patterns
                if pattern.len() > 4 {
                    boyer_moore_search(path_bytes, pattern)
                } else {
                    memchr_search(path_bytes, pattern)
                }
            }
            #[cfg(not(unix))]
            {
                let path_str = path.to_string_lossy();
                let pattern_str = std::str::from_utf8(pattern).unwrap_or("");
                path_str.contains(pattern_str)
            }
        } else {
            false
        }
    }
}

// Fast pattern matching for small patterns
#[cfg(unix)]
fn memchr_search(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() { return true; }
    if needle.len() > haystack.len() { return false; }
    
    let first = needle[0];
    let mut pos = 0;
    
    while pos + needle.len() <= haystack.len() {
        if let Some(found) = haystack[pos..].iter().position(|&b| b == first) {
            pos += found;
            if haystack[pos..].starts_with(needle) {
                return true;
            }
            pos += 1;
        } else {
            break;
        }
    }
    false
}

// Simple Boyer-Moore for larger patterns
#[cfg(unix)]
fn boyer_moore_search(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() { return true; }
    if needle.len() > haystack.len() { return false; }
    
    // Build bad character table
    let mut bad_char = [needle.len(); 256];
    for (i, &b) in needle.iter().enumerate().take(needle.len() - 1) {
        bad_char[b as usize] = needle.len() - 1 - i;
    }
    
    let mut pos = 0;
    while pos + needle.len() <= haystack.len() {
        let mut j = needle.len();
        while j > 0 && needle[j - 1] == haystack[pos + j - 1] {
            j -= 1;
        }
        
        if j == 0 {
            return true;
        }
        
        let bad_char_shift = bad_char[haystack[pos + j - 1] as usize];
        pos += bad_char_shift.max(1);
    }
    false
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
        out_fmt,
        skip_atime: args.skip_atime,
    };

    let skip_matcher = SkipMatcher::new(args.skip);

    // ---- spawn workers ----
    let mut joins = Vec::with_capacity(threads);
    for tid in 0..threads {
        let rx = rx.clone();
        let tx = tx.clone();
        let inflight = inflight.clone();
        let out_dir = out_dir.clone();
        let cfg = cfg.clone();
        let skip_matcher = skip_matcher.clone();
        joins.push(thread::spawn(move || worker(tid, rx, tx, inflight, out_dir, cfg, skip_matcher)));
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

fn worker(
    tid: usize,
    rx: Receiver<Task>,
    tx: Sender<Task>,
    inflight: Arc<AtomicUsize>,
    out_dir: PathBuf,
    cfg: Config,
    skip_matcher: SkipMatcher,
) -> Stats {
    let is_bin = cfg.out_fmt == OutputFormat::Bin;
    let hostname = get_hostname();
    let shard_path = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
    let file = File::create(&shard_path).expect("open shard");
    let mut base = BufWriter::with_capacity(BUF_CAPACITY, file); // Larger buffer

    // Header
    if !is_bin {
        base.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n").expect("write csv header"); 
    }

    // Choose writer: zstd encoder for binary; otherwise the base writer
    let mut writer: Box<dyn Write + Send> = if is_bin {
        let enc = ZstdEncoder::new(base, 1).expect("zstd encoder");
        Box::new(enc.auto_finish())
    } else {
        Box::new(base)
    };

    // Pre-allocate larger buffer for record batching
    let mut buf: Vec<u8> = Vec::with_capacity(BUF_CAPACITY); 
    let mut stats = Stats { files: 0, errors: 0 };

    // Pre-allocated buffers for path processing
    let mut path_buf = Vec::<u8>::with_capacity(4096);

    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,

            Task::Dir(dir) => {
                if skip_matcher.should_skip(&dir) {
                    inflight.fetch_sub(1, Relaxed);
                    continue;
                }
                if let Some(row) = stat_row(&dir) {
                    if is_bin { 
                        write_row_bin(&mut buf, &row, cfg.skip_atime, &mut path_buf); 
                    } else { 
                        write_row_csv(&mut buf, &row, cfg.skip_atime, &mut path_buf);
                    }
                    stats.files += 1;
                } else {
                    stats.errors += 1;
                }

                if buf.len() >= FLUSH_BYTES {
                    let _ = writer.write_all(&buf);
                    buf.clear();
                }

                let error_count = enum_dir(&dir, &tx, &inflight, &skip_matcher);
                stats.errors += error_count;
                inflight.fetch_sub(1, Relaxed);
            }

            Task::Files { base, names } => {
                if skip_matcher.should_skip(base.as_ref()) {
                    inflight.fetch_sub(1, Relaxed);
                    continue;
                }
                
                // Process files in batch with reduced syscalls
                for name in names {
                    let full = base.join(&name);
                    match stat_row(&full) {
                        Some(row) => {
                            if is_bin { 
                                write_row_bin(&mut buf, &row, cfg.skip_atime, &mut path_buf); 
                            } else { 
                                write_row_csv(&mut buf, &row, cfg.skip_atime, &mut path_buf);
                            }
                            stats.files += 1;
                        }
                        None => stats.errors += 1,
                    }
                    
                    // Batch flush to reduce syscalls
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

fn enum_dir(
    dir: &Path, 
    tx: &Sender<Task>, 
    inflight: &AtomicUsize, 
    skip_matcher: &SkipMatcher
) -> u64 {
    let rd = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(_) => return 1,
    };
    let mut error_count: u64 = 0;
    let mut page: Vec<OsString> = Vec::with_capacity(FILE_CHUNK);
    let base_arc = Arc::new(dir.to_path_buf());

    // Pre-allocate space for directory entries
    let mut dirs_to_process = Vec::<PathBuf>::with_capacity(256);

    for dent in rd {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => { error_count += 1; continue; }
        };

        let name = dent.file_name();
        if name == OsStr::new(".") || name == OsStr::new("..") { continue; }

        // Try to get file type without extra stat call
        let file_type = dent.file_type();
        let is_dir = match file_type {
            Ok(ft) => ft.is_dir(),
            Err(_) => {
                // Fallback to slower method
                match dent.path().is_dir() {
                    true => true,
                    false => { error_count += 1; continue; }
                }
            }
        };

        if is_dir {
            let dir_path = dent.path();
            if !skip_matcher.should_skip(&dir_path) {
                dirs_to_process.push(dir_path);
            }
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

    // Send remaining files
    if !page.is_empty() {
        inflight.fetch_add(1, Relaxed);
        let _ = tx.send(Task::Files { base: base_arc, names: page });
    }

    // Send directories after files to improve cache locality
    for dir_path in dirs_to_process {
        inflight.fetch_add(1, Relaxed);
        let _ = tx.send(Task::Dir(dir_path));
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
        mode |= 0o400;
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

// ----- Optimized CSV writing -----
fn write_row_csv(buf: &mut Vec<u8>, r: &Row<'_>, skip_atime: bool, path_buf: &mut Vec<u8>) {
    buf.reserve(512); // Reserve more space upfront
    
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

    csv_push_path_smart_quoted(buf, r.path, path_buf);
    buf.push(b'\n');
}

// ----- Optimized BIN writing -----
fn write_row_bin(buf: &mut Vec<u8>, r: &Row<'_>, skip_atime: bool, path_buf: &mut Vec<u8>) {
    path_buf.clear();
    
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path_buf.extend_from_slice(r.path.as_os_str().as_bytes());
    }
    #[cfg(not(unix))]
    {
        path_buf.extend_from_slice(r.path.to_string_lossy().as_bytes());
    }

    buf.reserve(64 + path_buf.len());

    let path_len = path_buf.len() as u32;
    let atime = if skip_atime { 0i64 } else { r.atime };
    let disk = r.blocks * 512;
    
    buf.extend_from_slice(&path_len.to_le_bytes());
    buf.extend_from_slice(path_buf);
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

fn csv_push_path_smart_quoted(buf: &mut Vec<u8>, p: &Path, _path_buf: &mut Vec<u8>) {
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
    // Fast path: scan for special characters
    let needs_quoting = bytes.iter().any(|&b| matches!(b, b'"' | b',' | b'\n' | b'\r'));
    
    if !needs_quoting {
        buf.extend_from_slice(bytes);
    } else {
        buf.push(b'"');
        
        // Fast path: no quotes to escape
        if !bytes.contains(&b'"') {
            buf.extend_from_slice(bytes);
        } else {
            // Reserve space for worst case (all quotes)
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
        if s.starts_with(r"\\?\UNC\") { 
            format!(r"\\{}", &s[8..]) 
        } else { 
            s[4..].to_string() 
        }
    } else { 
        s.to_string() 
    };
    
    let display_str = normalized.as_str();
    let needs_quoting = display_str.chars().any(|c| matches!(c, '"' | ',' | '\n' | '\r'));
    
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

// ---- Merge shards (CSV or BIN) ----
fn merge_shards(
    out_dir: &Path, 
    final_path: &Path, 
    threads: usize, 
    out_fmt: OutputFormat,
) -> std::io::Result<()> {
    let mut out = BufWriter::with_capacity(BUF_CAPACITY, File::create(&final_path)?); // Larger buffer

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
    
    // Use larger buffer for copying
    let mut copy_buf = vec![0u8; READ_BUF_SIZE];
    
    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
        if !shard.exists() { continue; }
        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);

        // Skip shard header line
        let mut first_line = Vec::<u8>::with_capacity(128);
        reader.read_until(b'\n', &mut first_line)?; // discard header

        // Use manual buffered copy for better performance
        loop {
            let bytes_read = reader.read(&mut copy_buf)?;
            if bytes_read == 0 { break; }
            out.write_all(&copy_buf[..bytes_read])?;
        }
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
    let mut copy_buf = vec![0u8; READ_BUF_SIZE];
    
    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
        if !shard.exists() { continue; }
        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
        
        // Manual buffered copy
        loop {
            let bytes_read = reader.read(&mut copy_buf)?;
            if bytes_read == 0 { break; }
            out.write_all(&copy_buf[..bytes_read])?;
        }
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
    fn test_skip_matcher() {
        let matcher = SkipMatcher::new(Some("target".to_string()));
        let p = PathBuf::from("/path/to/target/file");
        assert!(matcher.should_skip(&p));
        
        let matcher2 = SkipMatcher::new(None);
        assert!(!matcher2.should_skip(&p));
    }

    #[cfg(unix)]
    #[test]
    fn test_pattern_matching() {
        assert!(memchr_search(b"hello world", b"wor"));
        assert!(!memchr_search(b"hello world", b"xyz"));
        assert!(boyer_moore_search(b"this is a long string for testing", b"long string"));
        assert!(!boyer_moore_search(b"this is a long string for testing", b"not found"));
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_fast_path() {
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"abc_def");
        assert_eq!(&buf, b"abc_def");
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_comma() {
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"a,b");
        assert_eq!(&buf, b"\"a,b\"");
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_quote() {
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"a\"b");
        assert_eq!(&buf, b"\"a\"\"b\"");
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_newline() {
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"a\nb");
        assert_eq!(&buf, b"\"a\nb\"");
    }

    #[cfg(windows)]
    #[test]
    fn test_csv_push_str_smart_quoted_normalize_verbatim() {
        let mut buf = Vec::new();
        csv_push_str_smart_quoted(&mut buf, r"\\?\C:\foo\bar");
        assert_eq!(std::str::from_utf8(&buf).unwrap(), r"C:\foo\bar");

        let mut buf2 = Vec::new();
        csv_push_str_smart_quoted(&mut buf2, r"\\?\UNC\server\share\foo");
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

        merge_shards(&out_dir, &final_path, 2, OutputFormat::Csv)?;

        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let mut lines: Vec<&str> = s.lines().collect();

        assert_eq!(lines.remove(0), "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        assert_eq!(lines, vec!["b", "a"]);
        Ok(())
    }

    #[test]
    fn test_skip_atime_in_csv() {
        let mut buf = Vec::new();
        let mut path_buf = Vec::new();
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
            atime: 1609459200,
            mtime: 1609545600,
        };

        // With ATIME included
        write_row_csv(&mut buf, &row, false, &mut path_buf);
        let output_with_atime = String::from_utf8(buf.clone()).unwrap();
        assert!(output_with_atime.contains("1609459200"));
        assert!(output_with_atime.contains("1609545600"));

        // With ATIME skipped (zeroed)
        buf.clear();
        write_row_csv(&mut buf, &row, true, &mut path_buf);
        let output_without_atime = String::from_utf8(buf).unwrap();
        assert!(output_without_atime.contains(",0,1609545600"));
    }

    #[test]
    fn test_optimized_binary_write() {
        let mut buf = Vec::new();
        let mut path_buf = Vec::new();
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
            atime: 1609459200,
            mtime: 1609545600,
        };

        write_row_bin(&mut buf, &row, false, &mut path_buf);
        assert!(!buf.is_empty());
        
        // Test skip_atime functionality
        buf.clear();
        write_row_bin(&mut buf, &row, true, &mut path_buf);
        assert!(!buf.is_empty());
    }
}
