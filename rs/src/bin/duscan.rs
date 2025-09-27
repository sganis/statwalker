// duscan.rs
use anyhow::{Context, Result};
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io::{self, BufWriter, Write, BufReader, Read},
    path::{Path, PathBuf},
    time::{Duration, Instant},
    thread::{self, JoinHandle},
};
use std::sync::{
    atomic::{AtomicUsize, AtomicU64, AtomicBool, Ordering::Relaxed},
    Arc,
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use num_cpus;
use clap::{Parser, ColorChoice};
use colored::Colorize;
use chrono::Local;
use zstd::stream::write::Encoder as ZstdEncoder;
use dutopia::util::{
    Row, should_skip, 
    format_duration, get_hostname, strip_verbatim_prefix,
    row_from_metadata, stat_row,
    human_count, human_bytes, progress_bar, parse_file_hint, print_about,
};

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

// chunk sizes
const READ_BUF_SIZE: usize = 2 * 1024 * 1024; 
const FILE_CHUNK: usize = 2048;     
const FLUSH_BYTES: usize = 4 * 1024 * 1024; 

#[derive(Parser, Debug)]
#[command(
    version, author, color = ColorChoice::Auto,
    about = "Scan filesystem and gather file metadata into CSV or binary output"
)]
struct Args {
    /// Folders to scan (required, one or more)
    folders: Vec<String>,
    /// Output path (default: folder.csv or folder.zst if --bin)
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,
    /// Number of worker (default: 2xCPU, capped to 48)
    #[arg(short, long, value_name = "N")]
    workers: Option<usize>,
    /// Skip any folder whose full path contains this substring
    #[arg(short, long, value_name = "SUBSTR")]
    skip: Option<String>,
    /// Write a binary .zst compressed file instead of .csv
    #[arg(short, long)]
    bin: bool,
    /// Zero the ATIME field in outputs (CSV & BIN) for testing
    #[arg(long = "no-atime")]
    no_atime: bool,
    /// Total files hint (e.g. 750m, 1.2b). Used for % progress
    #[arg(long = "files-hint", value_name = "N")]
    files_hint: Option<String>,
    /// Do not report progress
    #[arg(short, long)]
    quiet: bool,
}

#[derive(Default)]
struct Progress {
    files:  AtomicU64,
}

#[derive(Debug)]
struct FileItem {
    name: OsString,
    md: fs::Metadata,
}

#[derive(Debug)]
enum Task {
    Dir(PathBuf),
    Files { base: std::sync::Arc<PathBuf>, items: Vec<FileItem> },
    Shutdown,
}

#[derive(Default)]
struct Stats {
    files: u64,
    errors: u64,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputFormat { Csv, Bin }

#[derive(Clone)]
struct Config {
    skip: Option<String>,
    out_fmt: OutputFormat,
    no_atime: bool,  
    progress: Option<Arc<Progress>>,
    pid: u32,
}

/// Reusable buffers for worker threads - eliminates allocations in hot paths
struct WorkerBuffers {
    /// Main output buffer for CSV/binary data (grows but never shrinks)
    output_buf: Vec<u8>,
    /// Buffer for number formatting  
    num_buf: Vec<u8>,
    /// Buffer for path processing
    path_buf: Vec<u8>,
    /// String buffer for path conversions (Windows mainly)
    string_buf: String,
}

impl WorkerBuffers {
    fn new() -> Self {
        Self {
            // Pre-allocate generous sizes to avoid early reallocations
            output_buf: Vec::with_capacity(4 * 1024 * 1024), // 4MB
            num_buf: Vec::with_capacity(32),
            path_buf: Vec::with_capacity(4096), // Max path length
            string_buf: String::with_capacity(4096),
        }
    }
    
    /// Clear buffers but keep capacity - this is the key optimization
    fn reset(&mut self) {
        self.output_buf.clear();
        self.num_buf.clear();
        self.path_buf.clear();
        self.string_buf.clear();
        // Capacity is preserved!
    }

    /// Write a number directly to output buffer - avoids borrowing conflicts
    fn write_u64(&mut self, value: u64) {
        // Fast path for common small values
        if value < 10 {
            self.output_buf.push(b'0' + value as u8);
        } else {
            // Use our temporary buffer for formatting
            self.num_buf.clear();
            use std::fmt::Write;
            self.string_buf.clear();
            write!(&mut self.string_buf, "{}", value).unwrap();
            self.num_buf.extend_from_slice(self.string_buf.as_bytes());
            self.output_buf.extend_from_slice(&self.num_buf);
        }
    }
    
    fn write_i64(&mut self, value: i64) {
        self.num_buf.clear();
        use std::fmt::Write;
        self.string_buf.clear();
        write!(&mut self.string_buf, "{}", value).unwrap();
        self.num_buf.extend_from_slice(self.string_buf.as_bytes());
        self.output_buf.extend_from_slice(&self.num_buf);
    }
    
    fn write_u32(&mut self, value: u32) {
        if value < 10 {
            self.output_buf.push(b'0' + value as u8);
        } else {
            self.num_buf.clear();
            use std::fmt::Write;
            self.string_buf.clear();
            write!(&mut self.string_buf, "{}", value).unwrap();
            self.num_buf.extend_from_slice(self.string_buf.as_bytes());
            self.output_buf.extend_from_slice(&self.num_buf);
        }
    }
}

fn main() -> Result<()> {

    print_about();

    let args = Args::parse();
    
    if args.folders.is_empty() {
        anyhow::bail!("At least one folder must be specified");
    }
    
    let out_fmt = if args.bin { OutputFormat::Bin } else { OutputFormat::Csv };
    
    if args.no_atime {
        eprintln!("{}", "ATIME will be written as 0 and lines sorted for reproducible output.".yellow());
    }

    // Canonicalize all root folders
    let mut roots = Vec::new();
    for folder in &args.folders {
        let root = fs::canonicalize(folder)
            .with_context(|| format!("Failed to canonicalize folder: {}", folder))?;
        roots.push(root);
    }

    // Create a combined name for default output
    let combined_name = if roots.len() == 1 {
        let root_normalized = strip_verbatim_prefix(&roots[0]);
        #[cfg(windows)]
        {
            root_normalized.to_string_lossy().replace('\\', "-").replace(':', "")
        }
        #[cfg(not(windows))]
        {
            root_normalized.to_string_lossy().trim_start_matches('/').replace('/', "-")
        }
    } else {
        format!("stats_{}", roots.len())
    };

    // Decide default output by out_fmt
    let final_path: PathBuf = match args.output {
        Some(p) => if p.is_absolute() { p } else { std::env::current_dir()?.join(p) },
        None => {
            // choose extension based on mode 
            let ext = match out_fmt {
                OutputFormat::Csv => "csv",
                OutputFormat::Bin => "zst",
            };
            std::env::current_dir()?.join(format!("{combined_name}.{ext}"))
        }
    };

    // Ensure the output directory exists and is writable
    let out_dir: PathBuf = final_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or(std::env::current_dir()?);
 
   if !out_dir.exists() {
        anyhow::bail!("Output directory does not exist: {}", out_dir.display());
    }

    if !out_dir.is_dir() {
        anyhow::bail!("Output path is not a directory: {}", out_dir.display());
    }

    // Check write access by trying to create a temp file
    let testfile = out_dir.join(".dutopia_write_test");
    File::create(&testfile)
        .with_context(|| format!("No write access to directory {}", out_dir.display()))?;
    let _ = fs::remove_file(&testfile);

    let workers = args.workers.unwrap_or_else(|| (num_cpus::get()*2).max(4).min(48));
    let cmd: Vec<String> = std::env::args().collect();    
    let now = Local::now();
    let hostname = get_hostname();
    let pid = std::process::id();

    println!("Local time   : {}", now.format("%Y-%m-%d %H:%M:%S").to_string());
    println!("Host         : {}", hostname);
    println!("Process ID   : {}", pid);
    println!("Command      : {}", cmd.join(" "));
    
    for (i, root) in roots.iter().enumerate() {
        let root_normalized = strip_verbatim_prefix(root);
        println!("Input {}      : {}", i + 1, root_normalized.display());
    }

    println!("Output       : {}", &final_path.display());
    println!("Temp dir     : {}", out_dir.display());
    println!("Workers      : {}", workers);

    // ---- work queue + inflight counter ----
    let (tx, rx) = unbounded::<Task>();
    let inflight = Arc::new(AtomicUsize::new(0));


    let progress = Arc::new(Progress::default());
    let reporting_done = Arc::new(AtomicBool::new(false));
    let mut reporter_join: Option<JoinHandle<()>> = None;

    if !args.quiet {
        // args.files_hint is Option<String>
        let hinted_files = args
            .files_hint
            .as_deref()
            .and_then(|s| parse_file_hint(s));

        if let Some(total_files) = hinted_files {
            println!("Files hint   : {} (from --files-hint)", human_count(total_files));
        }
              
        let progress_for_reporter = progress.clone();
        let reporting_done = reporting_done.clone();
        let start_for_reporter = Instant::now();

        reporter_join = Some(thread::spawn(move || {
            let mut last_pct = 0.0;     
            loop {
                if reporting_done.load(Relaxed) { break; }
                let f = progress_for_reporter.files.load(Relaxed);
                let elapsed = start_for_reporter.elapsed().as_secs_f64().max(0.001);
                let rate_f = human_count((f as f64 / elapsed) as u64);                

                if let Some(total) = hinted_files {
                    let mut pct = ((f as f64 / total as f64) * 100.0).min(100.0);
                    if pct < last_pct { pct = last_pct; }
                    last_pct = pct;
                    let bar = progress_bar(pct.into(), 25);
                    eprint!(
                        "\r    {} {} {:>3}% | {} files [{} f/s]        \r",
                        "Progress".bright_cyan(), bar, pct as u32, human_count(f), rate_f
                    );
                } else {
                    eprint!(
                        "\r    {} : {} files [{} f/s]        \r",
                        "Progress".bright_cyan(), human_count(f), rate_f
                    );
                }
                thread::sleep(Duration::from_millis(1000));
            }
            eprint!("\r{}"," ".repeat(120));
        }));
    }

    let start_time = Instant::now();
    
    // seed all root folders
    for root in roots {
        inflight.fetch_add(1, Relaxed);
        tx.send(Task::Dir(root)).expect("enqueue root");
    }

    // shutdown notifier
    {
        let tx = tx.clone();
        let inflight = inflight.clone();
        thread::spawn(move || loop {
            if inflight.load(Relaxed) == 0 {
                for _ in 0..workers {
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
        no_atime: args.no_atime,
        progress: (!args.quiet).then(|| progress.clone()),
        pid,
    };

    // ---- spawn workers ----
    let mut joins = Vec::with_capacity(workers);
    for tid in 0..workers {
        let rx = rx.clone();
        let tx = tx.clone();
        let inflight = inflight.clone();
        let out_dir = out_dir.clone();
        let cfg = cfg.clone();
        joins.push(thread::spawn(move || worker_optimized(
            tid, rx, tx, inflight, out_dir, cfg, 
        )));
    }
    drop(tx);

    // ---- gather stats ----
    let mut total = Stats::default();
    for j in joins {
        let s = j.join().expect("worker panicked");
        total.files += s.files;
        total.errors += s.errors;
        total.bytes += s.bytes;
    }
    // measure speed before merging
    let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
    let speed = ((total.files as f64) / elapsed) as u32;
    
    // ---- merge shards ----
    let sort_csv = args.no_atime && matches!(out_fmt, OutputFormat::Csv);
    merge_shards(&out_dir, &final_path, workers, out_fmt, sort_csv, pid).expect("merge shards failed");

    if let Some(h) = reporter_join.take() {
        reporting_done.store(true, Relaxed);
        let _ = h.join();
    }

    let elapsed_str = format_duration(start_time.elapsed());
    
    println!("\rTotal files  : {}", total.files);
    println!("Total errors : {}", total.errors);  
    println!("Total disk   : {}", human_bytes(total.bytes));  
    println!("Elapsed time : {}", elapsed_str);
    println!("Files/s      : {:.2}", speed);
    println!("{}","-".repeat(44).bright_cyan());
    println!("Done.");
    Ok(())
}

/// Optimized worker function with buffer reuse
fn worker_optimized(
    tid: usize,
    rx: Receiver<Task>,
    tx: Sender<Task>,
    inflight: Arc<AtomicUsize>,
    out_dir: PathBuf,
    cfg: Config,
) -> Stats {
    let is_bin = cfg.out_fmt == OutputFormat::Bin;
    let hostname = get_hostname();
    let shard_path = out_dir.join(format!("shard_{}_{}_{}.tmp", hostname, cfg.pid, tid));
    let file = File::create(&shard_path).expect("open shard");
    let base = BufWriter::with_capacity(32 * 1024 * 1024, file);
    let has_progress = cfg.progress.is_some();  
    let progress = cfg.progress.unwrap_or_default();
    let batch_size = 1000;
    let mut local_files = 0u64;

    // Choose writer: zstd encoder for binary; otherwise the base writer
    let mut writer: Box<dyn Write + Send> = if is_bin {
        let enc = ZstdEncoder::new(base, 1).expect("zstd encoder");
        Box::new(enc.auto_finish()) // finalize on drop
    } else {
        Box::new(base)
    };

    // Our reusable buffers - this is the key optimization!
    let mut buffers = WorkerBuffers::new();
    let mut stats = Stats { files: 0, errors: 0, bytes: 0 };

    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,

            Task::Dir(dir) => {
                let mut error_count = 0u64;
                if should_skip(&dir, cfg.skip.as_deref()) {
                    let _ = inflight.fetch_sub(1, Relaxed);
                    continue;
                }
                if let Some(row) = stat_row(&dir) {
                    if is_bin { 
                        write_row_bin_optimized(&mut buffers, &dir, &row, cfg.no_atime); 
                    } else { 
                        write_row_csv_optimized(&mut buffers, &dir, &row, cfg.no_atime);
                    }
                    stats.files += 1;                          
                } else {
                    stats.errors += 1; 
                    error_count += 1;                   
                }

                // Write buffer when it gets large enough, then reuse it
                if buffers.output_buf.len() >= FLUSH_BYTES {
                    let _ = writer.write_all(&buffers.output_buf);
                    buffers.reset(); // Clear but keep capacity!
                }

                error_count += enum_dir(&dir, &tx, &inflight, cfg.skip.as_deref());
                stats.errors += error_count;
                inflight.fetch_sub(1, Relaxed);   

                local_files += 1;
                if has_progress && local_files >= batch_size {
                    progress.files.fetch_add(local_files, Relaxed);
                    local_files = 0;
                }
            }

            Task::Files { base, items } => {
                if should_skip(base.as_ref(), cfg.skip.as_deref()) {
                    inflight.fetch_sub(1, Relaxed);
                    continue;
                }

                for FileItem { name, md } in &items {
                    let full = base.join(&name);
                    let row = row_from_metadata(&md); // <-- no syscall here
                    if is_bin { 
                        write_row_bin_optimized(&mut buffers, &full, &row, cfg.no_atime);
                    } else { 
                        write_row_csv_optimized(&mut buffers, &full, &row, cfg.no_atime);
                    }
                    stats.files += 1;
                    stats.bytes += &row.blocks * 512;                    
                    local_files += 1;

                    // Flush when buffer gets large
                    if buffers.output_buf.len() >= FLUSH_BYTES {
                        let _ = writer.write_all(&buffers.output_buf);
                        buffers.reset(); // The magic happens here!
                    }                    
                }
                inflight.fetch_sub(1, Relaxed);

                if has_progress && local_files >= batch_size {
                    progress.files.fetch_add(local_files, Relaxed);
                    local_files = 0;
                }
            }
        }
    }
    
    if has_progress && local_files > 0 {
        progress.files.fetch_add(local_files, Relaxed);
    }

    // Final flush
    if !buffers.output_buf.is_empty() {
        let _ = writer.write_all(&buffers.output_buf);
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
    let mut page: Vec<FileItem> = Vec::with_capacity(FILE_CHUNK);
    let base_arc = Arc::new(dir.to_path_buf());

    for dent in rd {
        let dent = match dent { Ok(d) => d, Err(_) => { error_count += 1; continue; } };
        let name = dent.file_name();
        if name == OsStr::new(".") || name == OsStr::new("..") { continue; }

        // One file_type() call
        let ft = match dent.file_type() {
            Ok(ft) => ft,
            Err(_) => { error_count += 1; continue; }
        };

        if ft.is_dir() {
            let p = dent.path();
            if should_skip(&p, skip) { continue; }
            inflight.fetch_add(1, Relaxed);
            let _ = tx.send(Task::Dir(p));
        } else {
            // Preserve your symlink semantics:
            // - symlink -> use lstat (symlink_metadata)
            // - otherwise -> metadata() (follows, faster/cached)
            let md = if ft.is_symlink() {
                match fs::symlink_metadata(dent.path()) {
                    Ok(m) => m,
                    Err(_) => { error_count += 1; continue; }
                }
            } else {
                match dent.metadata() {
                    Ok(m) => m,
                    Err(_) => { error_count += 1; continue; }
                }
            };
            
            page.push(FileItem { name, md });
            if page.len() == FILE_CHUNK {
                inflight.fetch_add(1, Relaxed);
                let _ = tx.send(Task::Files {
                    base: base_arc.clone(),
                    items: std::mem::take(&mut page),
                });
            }
        }
    }

    if !page.is_empty() {
        inflight.fetch_add(1, Relaxed);
        let _ = tx.send(Task::Files { base: base_arc, items: page });
    }

    error_count
}

/// Optimized CSV row writer using buffer reuse
fn write_row_csv_optimized(buffers: &mut WorkerBuffers, path: &Path, r: &Row, no_atime: bool) {
    // INODE as dev-ino
    buffers.write_u64(r.dev);
    buffers.output_buf.push(b'-');
    buffers.write_u64(r.ino);
    buffers.output_buf.push(b',');

    // ATIME (zeroed if requested)
    let atime = if no_atime { 0 } else { r.atime };
    buffers.write_i64(atime);
    buffers.output_buf.push(b',');

    // MTIME
    buffers.write_i64(r.mtime);
    buffers.output_buf.push(b',');

    // UID, GID, MODE
    buffers.write_u32(r.uid);
    buffers.output_buf.push(b',');
    buffers.write_u32(r.gid);
    buffers.output_buf.push(b',');
    buffers.write_u32(r.mode);
    buffers.output_buf.push(b',');

    // SIZE
    buffers.write_u64(r.size);
    buffers.output_buf.push(b',');
    
    // DISK
    let disk = r.blocks * 512;
    buffers.write_u64(disk);
    buffers.output_buf.push(b',');

    // PATH (reuse path buffer for processing)
    csv_push_path_optimized(&mut buffers.output_buf, &mut buffers.path_buf, &mut buffers.string_buf, path);
    buffers.output_buf.push(b'\n');
}

/// Optimized binary row writer using buffer reuse
fn write_row_bin_optimized(buffers: &mut WorkerBuffers, path: &Path, r: &Row, no_atime: bool) {
    // Get path bytes into our reusable buffer
    buffers.path_buf.clear();
    
    #[cfg(unix)]
    {
        buffers.path_buf.extend_from_slice(path.as_os_str().as_bytes());
    }
    
    #[cfg(not(unix))]
    {
        buffers.string_buf.clear();
        buffers.string_buf.push_str(&path.to_string_lossy());
        buffers.path_buf.extend_from_slice(buffers.string_buf.as_bytes());
    }

    let path_len = buffers.path_buf.len() as u32;
    let atime = if no_atime { 0i64 } else { r.atime };
    let disk = r.blocks * 512;
    
    // Pre-allocate space for this record
    let record_size = 4 + buffers.path_buf.len() + 64; // Conservative estimate
    buffers.output_buf.reserve(record_size);
    
    // Write binary data - we can borrow output_buf mutably here since path processing is done
    buffers.output_buf.extend_from_slice(&path_len.to_le_bytes());
    buffers.output_buf.extend_from_slice(&buffers.path_buf);
    buffers.output_buf.extend_from_slice(&r.dev.to_le_bytes());
    buffers.output_buf.extend_from_slice(&r.ino.to_le_bytes());
    buffers.output_buf.extend_from_slice(&atime.to_le_bytes());
    buffers.output_buf.extend_from_slice(&r.mtime.to_le_bytes());
    buffers.output_buf.extend_from_slice(&r.uid.to_le_bytes());
    buffers.output_buf.extend_from_slice(&r.gid.to_le_bytes());
    buffers.output_buf.extend_from_slice(&r.mode.to_le_bytes());
    buffers.output_buf.extend_from_slice(&r.size.to_le_bytes());
    buffers.output_buf.extend_from_slice(&disk.to_le_bytes());
}

/// Optimized path quoting with buffer reuse
fn csv_push_path_optimized(
    output: &mut Vec<u8>, 
    path_buf: &mut Vec<u8>, 
    string_buf: &mut String,
    path: &Path
) {
    path_buf.clear();
    string_buf.clear();

    #[cfg(unix)]
    {
        path_buf.extend_from_slice(path.as_os_str().as_bytes());
    }
    
    #[cfg(not(unix))]
    {
        // Convert to string using our reusable buffer
        string_buf.push_str(&path.to_string_lossy());
        path_buf.extend_from_slice(string_buf.as_bytes());
    }

    // Fast check if quoting is needed (most paths don't need it)
    let needs_quoting = path_buf.iter().any(|&b| b == b',' || b == b'"' || b == b'\n' || b == b'\r');
    
    if !needs_quoting {
        // Fast path: just copy the bytes
        output.extend_from_slice(path_buf);
        return;
    }
    
    // Slow path: need to quote and escape
    output.push(b'"');
    for &byte in path_buf.iter() {
        if byte == b'"' {
            output.push(b'"');
            output.push(b'"');
        } else {
            output.push(byte);
        }
    }
    output.push(b'"');
}

// ---- Merge shards (CSV or BIN) ----
fn merge_shards(
    out_dir: &Path, 
    final_path: &Path, 
    workers: usize, 
    out_fmt: OutputFormat,
    sort_csv: bool,
    pid: u32,
) -> Result<()> {
    let mut out = BufWriter::with_capacity(16 * 1024 * 1024, File::create(&final_path)?);

    match out_fmt {
        OutputFormat::Csv => merge_shards_csv(out_dir, &mut out, workers, sort_csv, pid),
        OutputFormat::Bin => merge_shards_bin(out_dir, &mut out, workers, pid),
    }?;

    out.flush()?;
    Ok(())
}

fn merge_shards_csv(
    out_dir: &Path, 
    out: &mut BufWriter<File>,
    workers: usize, 
    sort_csv: bool, 
    pid: u32
) -> Result<()> {
    out.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n")?;
    let hostname = get_hostname();

    if !sort_csv {
        // Old behavior: stream in shard order
        for tid in 0..workers {
            // Find all shard files for this worker thread (may have different thread IDs)
            let pattern = format!("shard_{}_{}_{}", hostname, pid, tid);
            let shard_files: Vec<_> = fs::read_dir(out_dir)?
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.file_name()
                        .to_string_lossy()
                        .starts_with(&pattern)
                })
                .collect();

            for entry in shard_files {
                let shard_path = entry.path();
                let f = File::open(&shard_path)?;
                let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);

                // Skip shard header line
                //let mut first_line = Vec::<u8>::with_capacity(128);
                //reader.read_until(b'\n', &mut first_line)?; // discard header

                io::copy(&mut reader, out)?;
                let _ = fs::remove_file(shard_path);
            }
        }
        return Ok(());
    }

    // Sorted mode (only used when --skip-atime and CSV)
    let mut lines: Vec<String> = Vec::new();

    for tid in 0..workers {
        // Find all shard files for this worker thread
        let pattern = format!("shard_{}_{}_{}", hostname, pid, tid);
        let shard_files: Vec<_> = fs::read_dir(out_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_name()
                    .to_string_lossy()
                    .starts_with(&pattern)
            })
            .collect();

        for entry in shard_files {
            let shard_path = entry.path();
            let f = File::open(&shard_path)?;
            let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);

            // Skip shard header
            //let mut throwaway = Vec::<u8>::with_capacity(128);
            //reader.read_until(b'\n', &mut throwaway)?;

            // Read remainder into buffer and split into lines
            let mut buf = String::new();
            reader.read_to_string(&mut buf)?;
            for line in buf.split_inclusive('\n') {
                // retain only non-empty rows
                if line.trim().is_empty() { continue; }
                // store without trailing newline; we'll add our own
                let ln = line.strip_suffix('\n').unwrap_or(line).to_string();
                if !ln.is_empty() {
                    lines.push(ln);
                }
            }

            let _ = fs::remove_file(shard_path);
        }
    }

    // Full-line lexicographic sort (deterministic; ATIME is zeroed in the rows)
    lines.sort_unstable();

    // Write back
    for ln in lines {
        out.write_all(ln.as_bytes())?;
        out.write_all(b"\n")?;
    }

    Ok(())
}

fn merge_shards_bin(
    out_dir: &Path, 
    out: &mut BufWriter<File>, 
    workers: usize,
    pid: u32,
) -> Result<()> {
    let hostname = get_hostname();
    for tid in 0..workers {
        // Find all shard files for this worker thread
        let pattern = format!("shard_{}_{}_{}", hostname, pid, tid);
        let shard_files: Vec<_> = fs::read_dir(out_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_name()
                    .to_string_lossy()
                    .starts_with(&pattern)
            })
            .collect();

        for entry in shard_files {
            let shard_path = entry.path();
            let f = File::open(&shard_path)?;
            let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
            io::copy(&mut reader, out)?;
            let _ = fs::remove_file(shard_path);
        }
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::tempdir;
    
    #[cfg(windows)]
    use dutopia::util::csv_push_str_smart_quoted;

    #[cfg(unix)]
    use dutopia::util::csv_push_bytes_smart_quoted;
    
    #[test]
    fn test_should_skip() {
        let p = PathBuf::from("/a/b/c/d");
        assert!(should_skip(&p, Some("b/c")));
        assert!(!should_skip(&p, Some("x")));
        assert!(!should_skip(&p, None));
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
    fn test_merge_shards_csv_unsorted_only() -> Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path = out_dir.join("out_unsorted.csv");
        let pid = 123;

        // Create 2 CSV shard files without headers
        let shard0 = out_dir.join(format!("shard_{}_{}_0.tmp", get_hostname(), pid));
        let shard1 = out_dir.join(format!("shard_{}_{}_1.tmp", get_hostname(), pid));

        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"b\n")?;
        }
        {
            let mut w = File::create(&shard1)?;
            w.write_all(b"a\n")?;
        }

        // sort_csv = false → just concatenates
        merge_shards(&out_dir, &final_path, 2, OutputFormat::Csv, false, pid)?;

        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let mut lines: Vec<&str> = s.lines().collect();

        assert_eq!(lines.remove(0), "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        // Order is just concatenation of shards
        assert_eq!(lines, vec!["b", "a"]);
        Ok(())
    }

    #[test]
    fn test_merge_shards_csv_sorted_with_no_atime() -> Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path = out_dir.join("out_sorted.csv");
        let pid = 123;

        // Create 2 CSV shards (out of order) without headers
        let shard0 = out_dir.join(format!("shard_{}_{}_0.tmp", get_hostname(), pid));
        let shard1 = out_dir.join(format!("shard_{}_{}_1.tmp", get_hostname(), pid));
        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"b\n")?;
        }
        {
            let mut w = File::create(&shard1)?;
            w.write_all(b"a\n")?;
        }

        // sort_csv = true → sorted result
        merge_shards(&out_dir, &final_path, 2, OutputFormat::Csv, true, pid)?;

        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let mut lines: Vec<&str> = s.lines().collect();

        assert_eq!(lines.remove(0), "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        assert_eq!(lines, vec!["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_buffer_reuse() {
        let mut buffers = WorkerBuffers::new();
        
        // Fill buffers
        buffers.output_buf.extend_from_slice(b"test data");
        buffers.path_buf.extend_from_slice(b"/test/path");
        
        // Verify capacity before reset
        let output_cap = buffers.output_buf.capacity();
        let path_cap = buffers.path_buf.capacity();
        
        // Reset should clear but preserve capacity
        buffers.reset();
        
        assert_eq!(buffers.output_buf.len(), 0);
        assert_eq!(buffers.path_buf.len(), 0);
        assert_eq!(buffers.output_buf.capacity(), output_cap);
        assert_eq!(buffers.path_buf.capacity(), path_cap);
    }
    
    #[test]
    fn test_number_formatting() {
        let mut buffers = WorkerBuffers::new();
        
        // Test the write functions instead
        buffers.write_u64(12345);
        let result = String::from_utf8(buffers.output_buf.clone()).unwrap();
        assert!(result.contains("12345"));
        
        buffers.reset();
        buffers.write_i64(-6789);
        let result2 = String::from_utf8(buffers.output_buf.clone()).unwrap();
        assert!(result2.contains("-6789"));
        
        // Test single digit optimization
        buffers.reset();
        buffers.write_u32(7);
        let result3 = String::from_utf8(buffers.output_buf.clone()).unwrap();
        assert_eq!(result3, "7");
    }
}

