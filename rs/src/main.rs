use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io::{BufWriter, Write, BufReader},
    path::{Path, PathBuf},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering::Relaxed},
    Arc,
};
use std::{thread, time::Instant, io::Read};
use crossbeam::channel::{unbounded, Receiver, Sender};
use num_cpus;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use itoa::Buffer;
use clap::Parser;
use blake3::Hasher;

// Increased chunk sizes for better batching
const FILE_CHUNK: usize = 8192;      // entries per work unit (doubled)
const FLUSH_BYTES: usize = 4 * 1024 * 1024; // 4MB buffer (doubled)
const READ_BUF_SIZE: usize = 2 * 1024 * 1024; // 2MB for file reads

#[derive(Parser, Debug)]
#[command(author, version, about = "Super Fast FS Scanner")]
struct Args {
    /// Root folder to scan
    #[arg(default_value = ".")]
    root: String,
    /// Detect file category (text/binary/image/video/etc), 50% decrease in performance
    #[arg(long)]
    category: bool,
    /// Hash files (slow!)
    #[arg(long)]
    hash: bool,
}

#[derive(Debug)]
enum Task {
    Dir(PathBuf),
    Files { base: PathBuf, names: Vec<OsString> },
    Shutdown,
}

#[derive(Default)]
struct Stats {
    files: u64,
    largest_file_count: u64,
    largest_file_count_dir: PathBuf,
}

// Packed category enum for better cache efficiency
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
enum Category {
    // images
    Jpeg = 1, Png, Gif, Bmp, Tiff, Webp,
    // documents
    Pdf,
    // archives/compressed
    Zip, Gzip, Bzip2, Xz, Rar, SevenZip, Tar,
    // audio/video
    Mp3, Mp4, Mkv, Ogg, Mpeg,
    // executables
    Elf, PE,
    // fallback
    Text, Binary, Unknown = 0,
}

#[derive(Clone)]
struct Config {
    want_category: bool,
    want_hash: bool,
}

struct Row<'a> {
    path: &'a Path,
    category: Option<Category>,
    hash: Option<String>,
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
    let start_time = Instant::now();

    let args = Args::parse();
    
    let folder = PathBuf::from(args.root.clone());
    let fullpath = fs::canonicalize(&folder)?
        .into_os_string()
        .into_string()
        .unwrap();

    #[cfg(unix)]
    let name = fullpath[1..].replace("/", "-");
    #[cfg(windows)]
    let name = fullpath[7..].replace('\\', "-");

    let final_path = PathBuf::from(&format!("{name}.csv"));
    println!("output: {}", &final_path.display());

    // Use more threads for I/O bound work
    let threads = (num_cpus::get() * 2).max(4).min(32);
    let out_dir = PathBuf::from(".");

    // ---- work queue + inflight counter ----
    let (tx, rx) = unbounded::<Task>();
    let inflight = Arc::new(AtomicUsize::new(0));

    // seed roots
    inflight.fetch_add(1, Relaxed);
    tx.send(Task::Dir(PathBuf::from(folder))).expect("enqueue root");

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
        want_category: args.category,
        want_hash: args.hash,
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

        if s.largest_file_count > total.largest_file_count {
            total.largest_file_count = s.largest_file_count;
            total.largest_file_count_dir = s.largest_file_count_dir;
        }
    }

    // ---- merge shards and print summary ----
    merge_shards(&out_dir, &final_path, threads)?;

    println!("Total entries (files + dirs): {}", total.files);

    if total.largest_file_count > 0 {
        println!(
            "Largest folder by entry count: {} ({} entries)",
            total.largest_file_count_dir.display(),
            total.largest_file_count
        );
    }

    let elapsed = start_time.elapsed();
    let secs = elapsed.as_secs_f64();
    println!("Elapsed time: {:.3} seconds", secs);
    println!("Files per second: {:.2}", (total.files as f64) / secs);

    Ok(())
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
        16 * 1024 * 1024, // Increased buffer size
        File::create(&shard_path).expect("open shard")
    );
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024 * 1024); // Pre-allocate larger buffer

    let mut stats = Stats {
        files: 0,
        largest_file_count: 0,
        largest_file_count_dir: PathBuf::new(),
    };

    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,

            Task::Dir(dir) => {
                // emit one row for the directory itself
                if let Some(row) = stat_row(&dir, &cfg) {
                    write_row(&mut buf, row);
                    stats.files += 1;

                    if buf.len() >= FLUSH_BYTES {
                        let _ = shard.write_all(&buf);
                        buf.clear();
                    }
                }

                let child_entries = enum_dir(&dir, &tx, &inflight);

                if child_entries > stats.largest_file_count {
                    stats.largest_file_count = child_entries;
                    stats.largest_file_count_dir = dir.clone();
                }

                inflight.fetch_sub(1, Relaxed);
            }

            Task::Files { base, names } => {
                // Process files in batch to reduce overhead
                for name in names {
                    let full = base.join(&name);
                    if let Some(row) = stat_row(&full, &cfg) {
                        write_row(&mut buf, row);
                        stats.files += 1;

                        if buf.len() >= FLUSH_BYTES {
                            let _ = shard.write_all(&buf);
                            buf.clear();
                        }
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

fn enum_dir(dir: &Path, tx: &Sender<Task>, inflight: &AtomicUsize) -> u64 {
    let rd = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(_) => return 0,
    };

    let mut page: Vec<OsString> = Vec::with_capacity(FILE_CHUNK);
    let mut entry_count: u64 = 0;

    for dent in rd {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };

        let name = dent.file_name();
        if name == OsStr::new(".") || name == OsStr::new("..") {
            continue;
        }
        entry_count += 1;

        // Use cached file_type() result to avoid extra syscalls
        let file_type = dent.file_type();
        let is_dir = file_type
            .map(|t| t.is_dir())
            .unwrap_or_else(|_| dent.path().is_dir());

        if is_dir {
            inflight.fetch_add(1, Relaxed);
            let _ = tx.send(Task::Dir(dent.path()));
        } else {
            page.push(name);
            if page.len() == FILE_CHUNK {
                inflight.fetch_add(1, Relaxed);
                let _ = tx.send(Task::Files {
                    base: dir.to_path_buf(),
                    names: std::mem::take(&mut page),
                });
            }
        }
    }

    if !page.is_empty() {
        inflight.fetch_add(1, Relaxed);
        let _ = tx.send(Task::Files {
            base: dir.to_path_buf(),
            names: page,
        });
    }

    entry_count
}

// Optimized category detection with lookup table
fn detect_category(path: &Path) -> Category {
    use Category::*;
    
    // Fast extension-based detection first
    if let Some(ext) = path.extension() {
        let ext_lower = ext.to_ascii_lowercase();
        match ext_lower.to_str() {
            Some("jpg") | Some("jpeg") => return Jpeg,
            Some("png") => return Png,
            Some("gif") => return Gif,
            Some("bmp") => return Bmp,
            Some("tiff") | Some("tif") => return Tiff,
            Some("webp") => return Webp,
            Some("pdf") => return Pdf,
            Some("zip") => return Zip,
            Some("gz") => return Gzip,
            Some("bz2") => return Bzip2,
            Some("xz") => return Xz,
            Some("rar") => return Rar,
            Some("7z") => return SevenZip,
            Some("tar") => return Tar,
            Some("mp3") => return Mp3,
            Some("mp4") => return Mp4,
            Some("mkv") => return Mkv,
            Some("ogg") => return Ogg,
            Some("mpeg") | Some("mpg") => return Mpeg,
            Some("txt") | Some("md") | Some("rs") | Some("c") | Some("cpp") | Some("py") => return Text,
            _ => {} // Fall through to magic number detection
        }
    }
    
    // Magic number detection for files without extensions or unknown extensions
    let mut f = match File::open(path) { Ok(f) => f, Err(_) => return Unknown };
    let mut hdr = [0u8; 32]; // Reduced header size for faster reads
    let n = match f.read(&mut hdr) { Ok(n) => n, Err(_) => 0 };
    
    if n == 0 { return Unknown; }
    let h = &hdr[..n];

    // Optimized magic number checks - most common first
    if n >= 4 && h[0] == 0xFF && h[1] == 0xD8 && h[2] == 0xFF { return Jpeg }
    if n >= 8 && &h[0..4] == &[0x89,b'P',b'N',b'G'] { return Png }
    if n >= 4 && &h[0..4] == b"GIF8" { return Gif }
    if n >= 4 && &h[0..2] == b"BM" { return Bmp }
    if n >= 4 && &h[0..4] == b"%PDF" { return Pdf }
    if n >= 4 && &h[0..4] == &[0x50,0x4B,0x03,0x04] { return Zip }
    if n >= 3 && &h[0..2] == &[0x1F,0x8B] { return Gzip }
    if n >= 3 && &h[0..3] == &[0x42,0x5A,0x68] { return Bzip2 }
    if n >= 4 && &h[0..4] == &[0x7F,b'E',b'L',b'F'] { return Elf }
    if n >= 2 && &h[0..2] == &[0x4D,0x5A] { return PE }

    // Quick text/binary heuristic - sample fewer bytes
    let sample_size = n.min(32);
    let mut printable = 0;
    let mut has_null = false;
    
    for &b in &h[..sample_size] {
        match b {
            0x20..=0x7E | b'\t' | b'\n' | b'\r' => printable += 1,
            0 => { has_null = true; break; }
            _ => {}
        }
    }
    
    if has_null || printable < sample_size / 2 { Binary } else { Text }
}

fn stat_row<'a>(path: &'a Path, cfg: &'a Config) -> Option<Row<'a>> {
    let md = fs::symlink_metadata(path).ok()?;
    let mut file_hash = None;

    if cfg.want_hash && md.is_file() && md.len() > 0 { // Skip empty files
        file_hash = Some(hash_file(path).unwrap_or_default());
    }

    #[cfg(unix)]
    {
        Some(Row {
            path,
            category: if cfg.want_category { Some(detect_category(path)) } else { None },
            hash: file_hash,
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
    #[cfg(not(unix))]
    {
        Some(Row {
            path,
            category: if cfg.want_category { Some(detect_category(path)) } else { None },
            hash: file_hash,
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
    push_u64_fast(buf, r.dev);
    buf.push(b'-');
    push_u64_fast(buf, r.ino);
    push_comma(buf);

    // ATIME, MTIME
    push_i64_fast(buf, r.atime); 
    push_comma(buf);
    push_i64_fast(buf, r.mtime); 
    push_comma(buf);

    // UID, GID, MODE
    push_u32_fast(buf, r.uid);   
    push_comma(buf);
    push_u32_fast(buf, r.gid);   
    push_comma(buf);
    push_u32_fast(buf, r.mode);  
    push_comma(buf);

    // SIZE, DISK
    push_u64_fast(buf, r.size);  
    push_comma(buf);
    let disk = r.blocks * 512;
    push_u64_fast(buf, disk); 
    push_comma(buf);

    // PATH (quote only if needed)
    csv_push_path_smart_quoted(buf, r.path);
    push_comma(buf);
    
    // CAT
    if let Some(cat) = r.category {
        let s = match cat {
            Category::Jpeg|Category::Png|Category::Gif|Category::Bmp|Category::Tiff|Category::Webp => "image",
            Category::Zip|Category::Gzip|Category::Bzip2|Category::Xz|Category::SevenZip|Category::Rar|Category::Tar => "zip",
            Category::Pdf => "pdf",
            Category::Mp3 => "audio",
            Category::Mp4|Category::Mkv|Category::Ogg|Category::Mpeg => "video",
            Category::Elf|Category::PE|Category::Binary => "binary",
            Category::Text => "text",
            Category::Unknown => "",
        };
        buf.extend_from_slice(s.as_bytes());
    }
    push_comma(buf);

    // HASH
    if let Some(h) = r.hash {
        buf.extend_from_slice(h.as_bytes());
    }
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

#[cfg(not(unix))]
fn csv_push_str_smart_quoted(buf: &mut Vec<u8>, s: &str) {
    // Check if we need quoting at all
    let needs_quoting = s.chars().any(|c| c == '"' || c == ',' || c == '\n' || c == '\r');
    
    if !needs_quoting {
        // Fast path - no quoting needed at all
        buf.extend_from_slice(s.as_bytes());
    } else {
        // Need quoting - check if we also need escaping
        buf.push(b'"');
        if !s.contains('"') {
            // Quotes needed but no escaping required
            buf.extend_from_slice(s.as_bytes());
        } else {
            // Need both quoting and escaping
            let quote_count = s.matches('"').count();
            buf.reserve(s.len() + quote_count);
            
            for b in s.bytes() {
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

fn merge_shards(out_dir: &Path, final_path: &Path, threads: usize) -> std::io::Result<()> {
    let mut out = BufWriter::with_capacity(
        16 * 1024 * 1024, // Larger output buffer
        File::create(&final_path)?
    );
    out.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH,CAT,HASH\n")?;

    let mut buf = Vec::with_capacity(READ_BUF_SIZE);
    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{tid}.csv.tmp"));
        if !shard.exists() {
            continue;
        }
        
        // Use BufReader for better I/O performance
        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
        
        buf.clear();
        reader.read_to_end(&mut buf)?;
        out.write_all(&buf)?;
        
        let _ = fs::remove_file(shard);
    }
    out.flush()?;
    Ok(())
}

#[inline]
fn hash_file(path: &Path) -> Option<String> {
    let f = File::open(path).ok()?;
    let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
    let mut hasher = Hasher::new();
    let mut buf = vec![0u8; READ_BUF_SIZE]; // Larger buffer for hashing
    
    loop {
        let n = reader.read(&mut buf).ok()?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    Some(hasher.finalize().to_hex().to_string())
}

#[inline] 
fn push_comma(buf: &mut Vec<u8>) { buf.push(b','); }

// Optimized number formatting with thread-local buffers
#[inline]
fn push_u32_fast(out: &mut Vec<u8>, v: u32) {
    U32_BUFFER.with(|b| {
        let mut binding = b.borrow_mut();
        let formatted = binding.format(v);
        out.extend_from_slice(formatted.as_bytes());
    });
}

#[inline]
fn push_u64_fast(out: &mut Vec<u8>, v: u64) {
    U64_BUFFER.with(|b| {
        let mut binding = b.borrow_mut();
        let formatted = binding.format(v);
        out.extend_from_slice(formatted.as_bytes());
    });
}

#[inline]
fn push_i64_fast(out: &mut Vec<u8>, v: i64) {
    I64_BUFFER.with(|b| {
        let mut binding = b.borrow_mut();
        let formatted = binding.format(v);
        out.extend_from_slice(formatted.as_bytes());
    });
}
