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

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

#[cfg(windows)]
use std::time::SystemTime;


// chunk sizes
const READ_BUF_SIZE: usize = 2 * 1024 * 1024; // 2MB for file reads
const FILE_CHUNK: usize = 16384;     // 16k entries per batch (was 8192)
const FLUSH_BYTES: usize = 8 * 1024 * 1024; // 8MB buffer (was 4MB)

#[derive(Parser, Debug)]
#[command(author, version, color = ColorChoice::Always,
     about = "Statwalker: Super Fast FS Scanner")]
struct Args {
    /// Root folder to scan
    #[arg(default_value = ".")]
    root: String,
    /// Output CSV path (default: "<canonical-root>.csv")
    #[arg(short, long, value_name = "FILE")]
    output: Option<PathBuf>,
    /// Number of worker threads (default: 2×CPU, capped 32)
    #[arg(short = 't', long)]
    threads: Option<usize>,
    /// Sort output lines (for easy diff/testing). Uses memory; avoid for huge scans.
    #[arg(long)]
    sort: bool,
    /// Skip any folder whose full path contains this substring
    #[arg(long, value_name = "SUBSTR")]
    skip: Option<String>,
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

#[derive(Clone)]
struct Config {
    skip: Option<String>,
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
    // Enable colors on Windows (this is automatic on Unix-like systems)
    #[cfg(windows)]
    colored::control::set_virtual_terminal(true).unwrap_or(());

    println!("{}","------------------------------------------------".cyan().bold());
    println!("{}", "Statlaker: Super fast filesystem scanner".cyan().bold());
    println!("{}","------------------------------------------------".cyan().bold());
    let start_time = Instant::now();
    let args = Args::parse();
    // Canonicalize the provided root so relative inputs like "../some/folder" become absolute.
    // Use the canonical path everywhere (for scanning and for path strings in CSV).
    let root = fs::canonicalize(&args.root)?;
    let root_normalized = strip_verbatim_prefix(&root);
    let root_str = root_normalized.display().to_string();

    let final_path: PathBuf = match args.output {
        Some(p) => if p.is_absolute() { p } else { std::env::current_dir()?.join(p) },
        None => {
            #[cfg(windows)]
            {
                let normalized = strip_verbatim_prefix(&root);
                let name = normalized.to_string_lossy().replace('\\', "-");
                // Remove drive colon if present (C: -> C)
                let name = name.replace(':', "");
                std::env::current_dir()?.join(format!("{name}.csv"))
            }
            #[cfg(not(windows))]
            {
                let name = root_normalized.to_string_lossy().trim_start_matches('/').replace('/', "-");
                std::env::current_dir()?.join(format!("{name}.csv"))
            }
        }
    };

    let cmd: Vec<String> = std::env::args().collect();    
    println!("Command      : {}", cmd.join(" "));
    println!("Input        : {}", &root_str);
    println!("Output       : {}", &final_path.display());

    // Use more threads for I/O bound work
    let threads = args.threads.unwrap_or_else(|| (num_cpus::get()*2).max(4).min(48));
    println!("Processes    : {}", threads);

    let out_dir = PathBuf::from(".");

    // ---- work queue + inflight counter ----
    let (tx, rx) = unbounded::<Task>();
    let inflight = Arc::new(AtomicUsize::new(0));

    // seed roots
    inflight.fetch_add(1, Relaxed);
    tx.send(Task::Dir(PathBuf::from(root))).expect("enqueue root");

    // shutdown notifier: when inflight hits 0, broadcast Shutdown
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
            thread::sleep(std::time::Duration::from_millis(10)); // Reduced sleep
        });
    }

    let cfg = Config { 
        skip: args.skip,
    };

    // ---- spawn workers (each returns its local Stats) ----
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

    // ---- gather stats from workers ----
    let mut total = Stats::default();
    for j in joins {
        let s = j.join().expect("worker panicked");
        total.files += s.files;
        total.errors += s.errors;
    }

    // ---- merge shards and print summary ----
    merge_shards(&out_dir, &final_path, threads, args.sort).expect("merge shards failed");

    let elapsed = start_time.elapsed();
    let secs = elapsed.as_secs_f64();

    println!("Total files  : {}", total.files);
    println!("Failed files : {}", total.errors);  
    println!("Elapsed time : {:.2} seconds", secs);
    println!("Files/sec.   : {:.2}", (total.files as f64) / secs);
    println!("{}","------------------------------------------------".cyan().bold());
    println!("Done.");
    Ok(())
}


#[cfg(windows)]
fn strip_verbatim_prefix(p: &std::path::Path) -> std::path::PathBuf {
    // Best-effort: only if the path is UTF-8. For non-UTF8, just return as-is.
    let s = match p.to_str() {
        Some(s) => s,
        None => return p.to_path_buf(),
    };

    if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
        // \\?\UNC\server\share\foo -> \\server\share\foo
        PathBuf::from(format!(r"\\{}", rest))
    } else if let Some(rest) = s.strip_prefix(r"\\?\") {
        // \\?\C:\foo -> C:\foo
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
    let shard_path = out_dir.join(format!("shard_{tid}.csv.tmp"));
    let mut shard = BufWriter::with_capacity(
        32 * 1024 * 1024, // 32MB
        File::create(&shard_path).expect("open shard")
    );    
    // Pre-allocate larger buffer
    let mut buf: Vec<u8> = Vec::with_capacity(32 * 1024 * 1024); 

    let mut stats = Stats {
        files: 0,
        errors: 0,
    };

    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,

            Task::Dir(dir) => {
                if should_skip(&dir, cfg.skip.as_deref()) {
                    let _ = inflight.fetch_sub(1, Relaxed);
                    continue;
                }
                // emit one row for the directory itself
                match stat_row(&dir) {
                    Some(row) => {
                        write_row(&mut buf, row);
                        stats.files += 1;
                    }
                    None => {
                        stats.errors += 1;  // Count directory stat failures
                    }
                }

                if buf.len() >= FLUSH_BYTES {
                    let _ = shard.write_all(&buf);
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
                // Process files in batch to reduce overhead
                for name in names {
                    let full = base.join(&name);
                    match stat_row(&full) {
                        Some(row) => {
                            write_row(&mut buf, row);
                            stats.files += 1;
                        }
                        None => {
                            stats.errors += 1;  // Count file stat failures
                        }
                    }

                    if buf.len() >= FLUSH_BYTES {
                        let _ = shard.write_all(&buf);
                        buf.clear();
                    }
                }
                inflight.fetch_sub(1, Relaxed);
            }
        }
    }

    if !buf.is_empty() {
        let _ = shard.write_all(&buf);
    }
    let _ = shard.flush();

    stats
}

fn enum_dir(dir: &Path, tx: &Sender<Task>, inflight: &AtomicUsize, skip: Option<&str>) -> u64 {
    // return number of errors
    let rd = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(_) => return 1,
    };
    let mut error_count: u64 = 0;
    let mut page: Vec<OsString> = Vec::with_capacity(FILE_CHUNK);
    let base_arc = Arc::new(dir.to_path_buf());
    
    // Collect entries in larger batches to reduce channel overhead
    //let entries: Vec<_> = rd.collect();

    for dent in rd {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => { error_count += 1; continue; }
        };

        let name = dent.file_name();
        if name == OsStr::new(".") || name == OsStr::new("..") {
            continue;
        }


        // Use cached file_type() result to avoid extra syscalls
        let file_type = dent.file_type();
        let is_dir = match file_type {
            Ok(ft) => ft.is_dir(),
            Err(_) => {
                // If file_type() fails, try fallback path.is_dir()
                match dent.path().is_dir() {
                    true => true,
                    false => {
                        // If both fail, we can't determine type - count as failed
                        error_count += 1;
                        continue;
                    }
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
        let _ = tx.send(Task::Files {
            base: base_arc,
            names: page,
        });
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

        // Convert SystemTime to Unix timestamp
        let to_unix_timestamp = |time: SystemTime| -> i64 {
            time.duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        };

        // Get Windows-specific metadata
        let atime = md.accessed().ok()
            .map(to_unix_timestamp)
            .unwrap_or(0);
        
        let mtime = md.modified().ok()
            .map(to_unix_timestamp)
            .unwrap_or(0);

        // Calculate blocks (approximate, based on 512-byte blocks)
        let blocks = (md.len() + 511) / 512;

        // Simple mode calculation for Windows
        let file_attributes = md.file_attributes();
        const FILE_ATTRIBUTE_READONLY: u32 = 0x1;
        
        let mut mode = if is_file { 0o100000 } else { 0o040000 }; // S_IFREG or S_IFDIR
        
        // Owner permissions (always readable)
        mode |= 0o400; // Owner read
        
        // Check if file is read-only
        if (file_attributes & FILE_ATTRIBUTE_READONLY) == 0 {
            mode |= 0o200; // Owner write
        }
        
        // Executable bit for files
        if is_file {
            if let Some(ext) = path.extension() {
                match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                    "exe" | "bat" | "cmd" | "com" | "scr" | "ps1" | "vbs" => mode |= 0o100,
                    _ => {}
                }
            }
        } else {
            mode |= 0o100; // Directories are executable
        }
        
        // Copy owner permissions to group and other
        let owner_perms = mode & 0o700;
        mode |= (owner_perms >> 3) | (owner_perms >> 6);

        Some(Row {
            path,
            dev: 0, // Windows doesn't have a direct equivalent
            ino: 0, // Windows file index would require additional API calls
            mode,
            uid: 0, // Windows uses SIDs instead of UIDs
            gid: 0, // Windows uses SIDs instead of GIDs
            size: md.len(),
            blocks,
            atime,
            mtime,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        Some(Row {
            path,
            dev: 0,
            ino: 0,
            mode: 0,
            uid: 0,
            gid: 0,
            size: md.len(),
            blocks: 0,
            atime: 0,
            mtime: 0,
        })
    }
}


// Pre-allocate formatters to avoid repeated allocation
thread_local! {
    static U32_BUFFER: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
    static U64_BUFFER: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
    static I64_BUFFER: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
}

fn write_row(buf: &mut Vec<u8>, r: Row<'_>) {
    // Reserve space to reduce reallocations
    buf.reserve(256);
    
    // INODE
    push_u64(buf, r.dev);
    buf.push(b'-');
    push_u64(buf, r.ino);
    push_comma(buf);

    // ATIME, MTIME
    push_i64(buf, r.atime); 
    push_comma(buf);
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

    // PATH (quote only if needed)
    csv_push_path_smart_quoted(buf, r.path);
    buf.push(b'\n');
}

fn csv_push_path_smart_quoted(buf: &mut Vec<u8>, p: &Path) {
    // Try to get bytes directly without UTF-8 validation when possible
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
    // Check if we need quoting at all
    let needs_quoting = bytes.iter().any(|&b| b == b'"' || b == b',' || b == b'\n' || b == b'\r');
    
    if !needs_quoting {
        // Fast path - no quoting needed at all
        buf.extend_from_slice(bytes);
    } else {
        // Need quoting - check if we also need escaping
        buf.push(b'"');
        if !bytes.contains(&b'"') {
            // Quotes needed but no escaping required
            buf.extend_from_slice(bytes);
        } else {
            // Need both quoting and escaping
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
    // For paths, normalize them first
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
    
    // Check if we need quoting at all
    let needs_quoting = display_str.chars().any(|c| c == '"' || c == ',' || c == '\n' || c == '\r');
    
    if !needs_quoting {
        // Fast path - no quoting needed at all
        buf.extend_from_slice(display_str.as_bytes());
    } else {
        // Need quoting - check if we also need escaping
        buf.push(b'"');
        if !display_str.contains('"') {
            // Quotes needed but no escaping required
            buf.extend_from_slice(display_str.as_bytes());
        } else {
            // Need both quoting and escaping
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


fn merge_shards(out_dir: &Path, 
    final_path: &Path, 
    threads: usize, 
    sort_lines: bool
) -> std::io::Result<()> {
    let mut out = BufWriter::with_capacity(
        16 * 1024 * 1024, // Larger output buffer
        File::create(&final_path)?
    );
    // build the header dynamically (replace the fixed write_all line)
    out.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n")?;

    if !sort_lines {
        // Stream shards directly to output (low memory)
        for tid in 0..threads {
            let shard = out_dir.join(format!("shard_{tid}.csv.tmp"));
            if !shard.exists() {
                continue;
            }
            let f = File::open(&shard)?;
            let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
            io::copy(&mut reader, &mut out)?;
            let _ = fs::remove_file(shard);
        }
        out.flush()?;
        return Ok(());
    }

    // Sorting requested: load all lines into memory, sort, and write.
    // Note: suitable for testing/small runs; avoid for huge datasets.
    let mut lines: Vec<Vec<u8>> = Vec::new();

    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{tid}.csv.tmp"));
        if !shard.exists() { continue; }

        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
        let mut line = Vec::<u8>::with_capacity(256);
        loop {
            line.clear();
            let n = reader.read_until(b'\n', &mut line)?;
            if n == 0 { break; }
            // Keep newline to preserve line boundaries
            lines.push(line.clone());
        }
        let _ = fs::remove_file(shard);
    }

    lines.sort_unstable(); // lexicographic byte-wise

    for l in lines {
        out.write_all(&l)?;
    }
    out.flush()?;
    Ok(())
}


#[inline] 
fn push_comma(buf: &mut Vec<u8>) { buf.push(b','); }

// Optimized number formatting with thread-local buffers
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

    // ---------- Quoting / PATH encoding ----------

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_fast_path() {
        let mut buf = Vec::new();
        super::csv_push_bytes_smart_quoted(&mut buf, b"abc_def");
        assert_eq!(&buf, b"abc_def"); // no quotes, no changes
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_comma() {
        let mut buf = Vec::new();
        super::csv_push_bytes_smart_quoted(&mut buf, b"a,b");
        assert_eq!(&buf, b"\"a,b\""); // quoted because of comma
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_quote() {
        let mut buf = Vec::new();
        super::csv_push_bytes_smart_quoted(&mut buf, b"a\"b");
        // inner quote doubled, whole field quoted
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
        // \\?\C:\foo -> C:\foo
        let mut buf = Vec::new();
        super::csv_push_str_smart_quoted(&mut buf, r"\\?\C:\foo\bar");
        assert_eq!(std::str::from_utf8(&buf).unwrap(), r"C:\foo\bar");

        // \\?\UNC\server\share\foo -> \\server\share\foo
        let mut buf2 = Vec::new();
        super::csv_push_str_smart_quoted(&mut buf2, r"\\?\UNC\server\share\foo");
        assert_eq!(std::str::from_utf8(&buf2).unwrap(), r"\\server\share\foo");
    }

    // ---------- merge_shards ----------

    #[test]
    fn test_merge_shards_unsorted_and_sorted() -> std::io::Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path_unsorted = out_dir.join("out_unsorted.csv");
        let final_path_sorted = out_dir.join("out_sorted.csv");

        // Create 2 shard files
        let shard0 = out_dir.join("shard_0.csv.tmp");
        let shard1 = out_dir.join("shard_1.csv.tmp");

        // shard0: "b\n"
        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"b\n")?;
        }
        // shard1: "a\n"
        {
            let mut w = File::create(&shard1)?;
            w.write_all(b"a\n")?;
        }

        // Unsorted merge
        super::merge_shards(&out_dir, &final_path_unsorted, 2, false)?;
        let mut s = String::new();
        File::open(&final_path_unsorted)?.read_to_string(&mut s)?;
        let mut lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.remove(0), "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        // Since we stream shards in 0..threads, order is shard0 then shard1
        assert_eq!(lines, vec!["b", "a"]);

        // Recreate shards for sorted test (they were removed)
        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"b\n")?;
        }
        {
            let mut w = File::create(&shard1)?;
            w.write_all(b"a\n")?;
        }

        // Sorted merge
        super::merge_shards(&out_dir, &final_path_sorted, 2, true)?;
        let mut s2 = String::new();
        File::open(&final_path_sorted)?.read_to_string(&mut s2)?;
        let mut lines2: Vec<&str> = s2.lines().collect();
        assert_eq!(lines2.remove(0), "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        assert_eq!(lines2, vec!["a", "b"]);

        Ok(())
    }

    // ---------- numeric formatters ----------

    #[test]
    fn test_push_numeric_helpers() {
        let mut buf = Vec::new();
        super::push_u32(&mut buf, 755);
        buf.push(b' ');
        super::push_u64(&mut buf, 1234567890123456789);
        buf.push(b' ');
        super::push_i64(&mut buf, -42);
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "755 1234567890123456789 -42");
    }

    // ---------- stat_row basics ----------

    #[test]
    fn test_stat_row_file_and_dir() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("d");
        let file = dir.join("f.txt");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&file, b"hello").unwrap();

        let dr = super::stat_row(&dir).expect("dir row");
        let fr = super::stat_row(&file).expect("file row");

        //assert!(dr.size >= 0);
        assert!(fr.size >= 5);
        assert!(dr.mtime >= 0);
        assert!(fr.mtime >= 0);
    }

    // ---------- enum_dir minimal behavior ----------

    #[test]
    fn test_enum_dir_enqueues_tasks() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let sub = root.join("sub");
        let f1 = root.join("a.txt");
        let f2 = root.join("b.txt");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(&f1, b"x").unwrap();
        std::fs::write(&f2, b"y").unwrap();

        let (tx, rx) = crossbeam::channel::unbounded::<super::Task>();
        let inflight = AtomicUsize::new(0);

        let errors = super::enum_dir(root, &tx, &inflight, None);
        assert_eq!(errors, 0);

        // We should receive one Dir task (for sub) and one Files batch for root files
        let mut saw_dir = false;
        let mut files_batch = 0usize;

        // Drain without blocking
        while let Ok(task) = rx.try_recv() {
            match task {
                super::Task::Dir(p) => {
                    assert!(p.ends_with("sub"));
                    saw_dir = true;
                }
                super::Task::Files { base, names } => {
                    assert_eq!(base.as_ref(), root);
                    files_batch += names.len();
                }
                super::Task::Shutdown => {}
            }
        }

        assert!(saw_dir, "expected subdirectory Task::Dir");
        assert_eq!(files_batch, 2, "expected 2 file names batched");
    }

    // ---------- Windows verbatim prefix helper ----------

    #[cfg(windows)]
    #[test]
    fn test_strip_verbatim_prefix_windows() {
        // \\?\C:\foo -> C:\foo
        let p = PathBuf::from(r"\\?\C:\foo\bar");
        let got = super::strip_verbatim_prefix(&p);
        assert_eq!(got.to_string_lossy(), r"C:\foo\bar");

        // \\?\UNC\server\share\foo -> \\server\share\foo
        let p2 = PathBuf::from(r"\\?\UNC\server\share\foo");
        let got2 = super::strip_verbatim_prefix(&p2);
        assert_eq!(got2.to_string_lossy(), r"\\server\share\foo");
    }

    #[cfg(not(windows))]
    #[test]
    fn test_strip_verbatim_prefix_noop_non_windows() {
        let p = PathBuf::from("/a/b");
        let got = super::strip_verbatim_prefix(&p);
        assert_eq!(got, p);
    }
}
