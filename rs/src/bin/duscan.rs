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
    Row, should_skip, push_u32, push_u64, push_i64,
    format_duration, get_hostname, strip_verbatim_prefix,
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
        joins.push(thread::spawn(move || worker(
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
    let pid = cfg.pid;
    let shard_path = out_dir.join(format!("shard_{hostname}_{pid}_{tid}.tmp"));
    let file = File::create(&shard_path).expect("open shard");
    let base = BufWriter::with_capacity(32 * 1024 * 1024, file);
    let has_progress = cfg.progress.is_some();  
    let progress = cfg.progress.unwrap_or_default();
    

    // Choose writer: zstd encoder for binary; otherwise the base writer
    let mut writer: Box<dyn Write + Send> = if is_bin {
        let enc = ZstdEncoder::new(base, 1).expect("zstd encoder");
        Box::new(enc.auto_finish()) // finalize on drop
    } else {
        Box::new(base)
    };

    // Pre-allocate buffer for record batching
    let mut buf: Vec<u8> = Vec::with_capacity(32 * 1024 * 1024); 

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
                        write_row_bin(&mut buf, &dir, &row, cfg.no_atime); 
                    } else { 
                        write_row_csv(&mut buf, &dir, &row, cfg.no_atime);
                    }
                    stats.files += 1;                    
                } else {
                    stats.errors += 1; 
                    error_count += 1;                   
                }

                if buf.len() >= FLUSH_BYTES {
                    let _ = writer.write_all(&buf);
                    buf.clear();
                }

                error_count += enum_dir(&dir, &tx, &inflight, cfg.skip.as_deref());
                stats.errors += error_count;
                inflight.fetch_sub(1, Relaxed);   
                if has_progress {
                    progress.files.fetch_add(1, Relaxed);
                }
            }

            Task::Files { base, items } => {
                if should_skip(base.as_ref(), cfg.skip.as_deref()) {
                    inflight.fetch_sub(1, Relaxed);
                    continue;
                }
                let mut files = 0u64;

                for FileItem { name, md } in &items {
                    let full = base.join(&name);
                    let row = row_from_metadata(&md);
                    if is_bin { 
                        write_row_bin(&mut buf, &full, &row, cfg.no_atime);
                    } else { 
                        write_row_csv(&mut buf, &full, &row, cfg.no_atime);
                    }
                    stats.files += 1;
                    stats.bytes += &row.size;
                    files += 1;
                    if buf.len() >= FLUSH_BYTES {
                        let _ = writer.write_all(&buf);
                        buf.clear();
                    }
                }
                inflight.fetch_sub(1, Relaxed);
                if has_progress {
                    progress.files.fetch_add(files, Relaxed);                 
                }
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


// ----- CSV writing -----
fn write_row_csv(buf: &mut Vec<u8>, path: &Path, r: &Row, no_atime: bool) {
    buf.reserve(256);
    // INODE as dev-ino
    push_u64(buf, r.dev);
    buf.push(b'-');
    push_u64(buf, r.ino);
    buf.push(b',');

    // ATIME (zeroed if requested)
    if no_atime { push_i64(buf, 0); } else { push_i64(buf, r.atime); }
    buf.push(b',');   

    // MTIME
    push_i64(buf, r.mtime); 
    buf.push(b',');

    // UID, GID, MODE
    push_u32(buf, r.uid);       
    buf.push(b',');
    push_u32(buf, r.gid);   
    buf.push(b',');
    push_u32(buf, r.mode);  
    buf.push(b',');

    // SIZE, DISK
    push_u64(buf, r.size);  
    buf.push(b',');
    let disk = r.blocks * 512;
    push_u64(buf, disk); 
    buf.push(b',');

    csv_push_path_smart_quoted(buf, path);
    buf.push(b'\n');
}

// ----- BIN writing -----
fn write_row_bin(buf: &mut Vec<u8>, path: &Path, r: &Row, no_atime: bool) {
    
    #[cfg(unix)]
    let path_bytes: &[u8] = path.as_os_str().as_bytes();

    #[cfg(not(unix))]
    let path_lossy = path.to_string_lossy();     // keep the Cow alive
    #[cfg(not(unix))]
    let path_bytes: &[u8] = path_lossy.as_bytes(); // borrow from it safely

    let path_len = path_bytes.len() as u32;
    let atime = if no_atime { 0i64 } else { r.atime };
    let disk = r.blocks * 512;
    
    //buf.reserve(64 + 2 * r.path.as_os_str().len());
    buf.reserve(80 + path_bytes.len()); // cheap pre-reserve
    buf.extend_from_slice(&path_len.to_le_bytes());
    buf.extend_from_slice(path_bytes);
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

// ---- Merge shards (CSV or BIN) ----
fn merge_shards(
    out_dir: &Path, 
    final_path: &Path, 
    threads: usize, 
    out_fmt: OutputFormat,
    sort_csv: bool,
    pid: u32,
) -> std::io::Result<()> {
    let mut out = BufWriter::with_capacity(16 * 1024 * 1024, File::create(&final_path)?);

    match out_fmt {
        OutputFormat::Csv => merge_shards_csv(out_dir, &mut out, threads, sort_csv, pid),
        OutputFormat::Bin => merge_shards_bin(out_dir, &mut out, threads, pid),
    }?;

    out.flush()?;
    Ok(())
}

fn merge_shards_csv(out_dir: &Path, out: &mut BufWriter<File>, threads: usize, sort_csv: bool, pid: u32) -> std::io::Result<()> {
    out.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n")?;
    let hostname = get_hostname();

    if !sort_csv {
        // Old behavior: stream in shard order
        for tid in 0..threads {
            let shard = out_dir.join(format!("shard_{hostname}_{pid}_{tid}.tmp"));
            if !shard.exists() { continue; }
            let f = File::open(&shard)?;
            let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);

            // // Skip shard header line
            // let mut first_line = Vec::<u8>::with_capacity(128);
            // reader.read_until(b'\n', &mut first_line)?; // discard header

            io::copy(&mut reader, out)?;
            let _ = fs::remove_file(shard);
        }
        return Ok(());
    }

    // Sorted mode (only used when --skip-atime and CSV)
    let mut lines: Vec<String> = Vec::new();

    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{hostname}_{pid}_{tid}.tmp"));
        if !shard.exists() { continue; }

        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);

        // // Skip shard header
        // let mut throwaway = Vec::<u8>::with_capacity(128);
        // reader.read_until(b'\n', &mut throwaway)?;

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

        let _ = fs::remove_file(shard);
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
    threads: usize, 
    pid: u32,
) -> std::io::Result<()> {
    let hostname = get_hostname();
    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{hostname}_{pid}_{tid}.tmp"));
        if !shard.exists() { continue; }
        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
        io::copy(&mut reader, out)?;
        let _ = fs::remove_file(shard);
    }

    Ok(())
}


pub fn csv_push_path_smart_quoted(buf: &mut Vec<u8>, p: &Path) {
    #[cfg(unix)]
    {
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
pub fn csv_push_bytes_smart_quoted(buf: &mut Vec<u8>, bytes: &[u8]) {
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
pub fn csv_push_str_smart_quoted(buf: &mut Vec<u8>, s: &str) {
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

pub fn row_from_metadata(md: &fs::Metadata) -> Row {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Row {
            //path,
            dev: md.dev(),
            ino: md.ino(),
            mode: md.mode(),
            uid: md.uid(),
            gid: md.gid(),
            size: md.size(),
            blocks: md.blocks() as u64,
            atime: md.atime(),
            mtime: md.mtime(),
        }
    }
    #[cfg(windows)]
    {
        //use std::os::windows::fs::MetadataExt;
        use std::time::SystemTime;

        let to_unix = |t: SystemTime| -> i64 {
            t.duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        };
        let atime = md.accessed().ok().map(to_unix).unwrap_or(0);
        let mtime = md.modified().ok().map(to_unix).unwrap_or(0);
        let blocks = (md.len() + 511) / 512;

        //let file_attributes = md.file_attributes();
        //const FILE_ATTRIBUTE_READONLY: u32 = 0x1;

        // let is_file = md.is_file();
        // let mut mode = if is_file { 0o100000 } else { 0o040000 };
        // mode |= 0o400; // Owner read
        // if (file_attributes & FILE_ATTRIBUTE_READONLY) == 0 { 
        //     mode |= 0o200; 
        // }
        // if is_file {
        //     if let Some(ext) = path.extension() {
        //         match ext.to_str().unwrap_or("").to_lowercase().as_str() {
        //             "exe" | "bat" | "cmd" | "com" | "scr" | "ps1" | "vbs" => mode |= 0o100,
        //             _ => {}
        //         }
        //     }
        // } else {
        //     mode |= 0o100;
        // }
        // let owner = mode & 0o700;
        // mode |= (owner >> 3) | (owner >> 6);
        
        // very expensive and problematic
        //let uid = get_rid(path).unwrap_or(0);
        
        Row {
            //path, 
            dev: 0, 
            ino: 0, 
            mode: 0, 
            uid: 0, 
            gid: 0,
            size: md.len(), 
            blocks, 
            atime, 
            mtime,
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        Row { 
            //path, 
            dev: 0, 
            ino: 0, 
            mode: 0, 
            uid: 0, 
            gid: 0, 
            size: md.len(), 
            blocks: 0, 
            atime: 0, 
            mtime: 0 
        }
    }
}

pub fn stat_row(path: &Path) -> Option<Row> {
    let md = fs::symlink_metadata(path).ok()?;
    Some(row_from_metadata(&md))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use tempfile::tempdir;
    use std::fs::{self, File};
    use std::path::PathBuf;
    use crossbeam::channel::unbounded;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    // Existing tests
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

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_with_carriage_return() {
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"a\rb");
        assert_eq!(&buf, b"\"a\rb\"");
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted_multiple_quotes() {
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"a\"b\"c");
        assert_eq!(&buf, b"\"a\"\"b\"\"c\"");
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

    #[cfg(windows)]
    #[test]
    fn test_csv_push_str_smart_quoted_no_normalization_needed() {
        let mut buf = Vec::new();
        csv_push_str_smart_quoted(&mut buf, r"C:\regular\path");
        assert_eq!(std::str::from_utf8(&buf).unwrap(), r"C:\regular\path");
    }

    #[cfg(windows)]
    #[test]
    fn test_csv_push_str_smart_quoted_with_quotes_and_commas() {
        let mut buf = Vec::new();
        csv_push_str_smart_quoted(&mut buf, r#"path with "quotes" and, commas"#);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), r#""path with ""quotes"" and, commas""#);
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

    // New comprehensive tests

    #[test]
    fn test_merge_shards_bin() -> Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path = out_dir.join("out.bin");
        let pid = 123;

        // Create 2 binary shard files
        let shard0 = out_dir.join(format!("shard_{}_{}_0.tmp", get_hostname(), pid));
        let shard1 = out_dir.join(format!("shard_{}_{}_1.tmp", get_hostname(), pid));

        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"binary_data_0")?;
        }
        {
            let mut w = File::create(&shard1)?;
            w.write_all(b"binary_data_1")?;
        }

        merge_shards(&out_dir, &final_path, 2, OutputFormat::Bin, false, pid)?;

        let mut s = Vec::new();
        File::open(&final_path)?.read_to_end(&mut s)?;
        assert_eq!(s, b"binary_data_0binary_data_1");
        Ok(())
    }

    #[test]
    fn test_merge_shards_with_missing_shards() -> Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path = out_dir.join("out.csv");
        let pid = 123;

        // Only create shard 1, shard 0 is missing
        let shard1 = out_dir.join(format!("shard_{}_{}_1.tmp", get_hostname(), pid));
        {
            let mut w = File::create(&shard1)?;
            w.write_all(b"data\n")?;
        }

        merge_shards(&out_dir, &final_path, 2, OutputFormat::Csv, false, pid)?;

        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let lines: Vec<&str> = s.lines().collect();
        
        assert_eq!(lines[0], "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        assert_eq!(lines[1], "data");
        Ok(())
    }

    #[test]
    fn test_merge_shards_csv_with_empty_lines() -> Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path = out_dir.join("out_sorted.csv");
        let pid = 123;

        let shard0 = out_dir.join(format!("shard_{}_{}_0.tmp", get_hostname(), pid));
        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"valid_line\n\n   \n")?;  // Empty and whitespace-only lines
        }

        merge_shards(&out_dir, &final_path, 1, OutputFormat::Csv, true, pid)?;

        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let lines: Vec<&str> = s.lines().collect();

        assert_eq!(lines[0], "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        assert_eq!(lines[1], "valid_line");
        assert_eq!(lines.len(), 2); // Only header and one valid line
        Ok(())
    }

    #[test]
    fn test_csv_push_path_smart_quoted() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("simple/path");
        csv_push_path_smart_quoted(&mut buf, path);
        
        #[cfg(unix)]
        assert_eq!(&buf, b"simple/path");
        #[cfg(windows)]
        assert_eq!(std::str::from_utf8(&buf).unwrap(), "simple/path");
    }

    #[test]
    fn test_write_row_csv_with_atime() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("test/path");
        let row = Row {
            dev: 1,
            ino: 2,
            mode: 755,
            uid: 1000,
            gid: 1000,
            size: 1024,
            blocks: 2,
            atime: 1234567890,
            mtime: 1234567891,
        };

        write_row_csv(&mut buf, path, &row, false);
        let result = String::from_utf8(buf).unwrap();
        
        // Should start with inode (dev-ino), then atime, mtime, uid, gid, mode, size, disk, path
        assert!(result.starts_with("1-2,1234567890,1234567891,1000,1000,755,1024,1024,"));
        assert!(result.ends_with("\n"));
    }

    #[test]
    fn test_write_row_csv_no_atime() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("test/path");
        let row = Row {
            dev: 1,
            ino: 2,
            mode: 755,
            uid: 1000,
            gid: 1000,
            size: 1024,
            blocks: 2,
            atime: 1234567890,
            mtime: 1234567891,
        };

        write_row_csv(&mut buf, path, &row, true);
        let result = String::from_utf8(buf).unwrap();
        
        // ATIME should be 0 when no_atime is true
        assert!(result.starts_with("1-2,0,1234567891,1000,1000,755,1024,1024,"));
    }

    #[test]
    fn test_write_row_bin_with_atime() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("test");
        let row = Row {
            dev: 1,
            ino: 2,
            mode: 755,
            uid: 1000,
            gid: 1000,
            size: 1024,
            blocks: 2,
            atime: 1234567890,
            mtime: 1234567891,
        };

        write_row_bin(&mut buf, path, &row, false);
        
        // Binary format: path_len(4) + path + dev(8) + ino(8) + atime(8) + mtime(8) + uid(4) + gid(4) + mode(4) + size(8) + disk(8)
        // Expected minimum: 4 + 4 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 8 + 8 = 68 bytes
        assert!(buf.len() >= 68, "Buffer length {} is less than expected 68", buf.len());
        
        // Check path length is encoded correctly
        let path_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(path_len, 4); // "test" = 4 bytes
    }

    #[test]
    fn test_write_row_bin_no_atime() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("test");
        let row = Row {
            dev: 1,
            ino: 2,
            mode: 755,
            uid: 1000,
            gid: 1000,
            size: 1024,
            blocks: 2,
            atime: 1234567890,
            mtime: 1234567891,
        };

        write_row_bin(&mut buf, path, &row, true);
        
        // Extract atime from binary data (starts at offset 4 + path_len + 8 + 8)
        let path_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let atime_offset = 4 + path_len + 8 + 8;
        let atime_bytes = &buf[atime_offset..atime_offset + 8];
        let atime = i64::from_le_bytes([
            atime_bytes[0], atime_bytes[1], atime_bytes[2], atime_bytes[3],
            atime_bytes[4], atime_bytes[5], atime_bytes[6], atime_bytes[7],
        ]);
        assert_eq!(atime, 0);
    }

    #[test]
    fn test_row_from_metadata() {
        let tmp = tempdir().unwrap();
        let test_file = tmp.path().join("test.txt");
        fs::write(&test_file, "test content").unwrap();
        
        let metadata = fs::metadata(&test_file).unwrap();
        let row = row_from_metadata(&metadata);
        
        assert_eq!(row.size, 12); // "test content" = 12 bytes
        assert!(row.mtime > 0); // Should have a modification time
        
        #[cfg(unix)]
        {
            assert!(row.dev > 0);
            assert!(row.ino > 0);
            // uid and gid are u32, so they're always >= 0
            assert!(row.mode > 0);
        }
        
        #[cfg(windows)]
        {
            assert_eq!(row.dev, 0);
            assert_eq!(row.ino, 0);
            assert_eq!(row.uid, 0);
            assert_eq!(row.gid, 0);
            assert_eq!(row.mode, 0);
        }
    }

    #[test]
    fn test_stat_row_success() {
        let tmp = tempdir().unwrap();
        let test_file = tmp.path().join("test.txt");
        fs::write(&test_file, "test content").unwrap();
        
        let row = stat_row(&test_file);
        assert!(row.is_some());
        
        let row = row.unwrap();
        assert_eq!(row.size, 12);
    }

    #[test]
    fn test_stat_row_failure() {
        let nonexistent = std::path::Path::new("/nonexistent/path/that/does/not/exist");
        let row = stat_row(nonexistent);
        assert!(row.is_none());
    }

    #[test]
    fn test_enum_dir_with_files_and_dirs() {
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path();
        
        // Create some test files and directories
        fs::write(test_dir.join("file1.txt"), "content1").unwrap();
        fs::write(test_dir.join("file2.txt"), "content2").unwrap();
        fs::create_dir(test_dir.join("subdir")).unwrap();
        fs::write(test_dir.join("subdir").join("file3.txt"), "content3").unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let error_count = enum_dir(test_dir, &tx, &inflight, None);
        
        // Should have no errors for valid directory
        assert_eq!(error_count, 0);
        
        // Check that tasks were queued
        let mut dir_tasks = 0;
        let mut file_tasks = 0;
        
        // Drain the channel to count tasks
        drop(tx); // Close sender so receiver will eventually get disconnected
        while let Ok(task) = rx.recv() {
            match task {
                Task::Dir(_) => dir_tasks += 1,
                Task::Files { items, .. } => file_tasks += items.len(),
                Task::Shutdown => break,
            }
        }
        
        assert!(dir_tasks >= 1); // At least the subdir
        assert!(file_tasks >= 2); // At least file1.txt and file2.txt
    }

    #[test]
    fn test_enum_dir_with_skip() {
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path();
        
        // Create a subdirectory that should be skipped
        fs::create_dir(test_dir.join("skip_me")).unwrap();
        fs::create_dir(test_dir.join("keep_me")).unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let error_count = enum_dir(test_dir, &tx, &inflight, Some("skip_me"));
        assert_eq!(error_count, 0);
        
        // Check tasks - should only have keep_me directory
        drop(tx);
        let mut found_skip = false;
        let mut found_keep = false;
        
        while let Ok(task) = rx.recv() {
            if let Task::Dir(path) = task {
                if path.file_name().unwrap() == "skip_me" {
                    found_skip = true;
                }
                if path.file_name().unwrap() == "keep_me" {
                    found_keep = true;
                }
            }
        }
        
        assert!(!found_skip);
        assert!(found_keep);
    }

    #[test]
    fn test_enum_dir_nonexistent() {
        let nonexistent = std::path::Path::new("/nonexistent/directory");
        let (tx, _rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let error_count = enum_dir(nonexistent, &tx, &inflight, None);
        assert_eq!(error_count, 1); // Should return 1 error for failed read_dir
    }

    #[test]
    fn test_enum_dir_with_chunking() {
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path();
        
        // Create more files than FILE_CHUNK to test chunking behavior
        for i in 0..(FILE_CHUNK + 10) {
            fs::write(test_dir.join(format!("file{}.txt", i)), "content").unwrap();
        }
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let error_count = enum_dir(test_dir, &tx, &inflight, None);
        assert_eq!(error_count, 0);
        
        drop(tx);
        
        let mut total_files = 0;
        let mut task_count = 0;
        
        while let Ok(task) = rx.recv() {
            if let Task::Files { items, .. } = task {
                total_files += items.len();
                task_count += 1;
            }
        }
        
        assert_eq!(total_files, FILE_CHUNK + 10);
        assert!(task_count > 1); // Should be split into multiple tasks
    }

    #[test]
    fn test_progress_default() {
        let progress = Progress::default();
        assert_eq!(progress.files.load(Relaxed), 0);
    }

    #[test]
    fn test_config_clone() {
        let progress = Arc::new(Progress::default());
        let config = Config {
            skip: Some("test".to_string()),
            out_fmt: OutputFormat::Csv,
            no_atime: true,
            progress: Some(progress.clone()),
            pid: 123,
        };
        
        let cloned = config.clone();
        assert_eq!(cloned.skip, Some("test".to_string()));
        assert_eq!(cloned.out_fmt, OutputFormat::Csv);
        assert_eq!(cloned.no_atime, true);
        assert_eq!(cloned.pid, 123);
        assert!(cloned.progress.is_some());
    }

    #[test]
    fn test_stats_default() {
        let stats = Stats::default();
        assert_eq!(stats.files, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.bytes, 0);
    }

    #[test]
    fn test_output_format_equality() {
        assert_eq!(OutputFormat::Csv, OutputFormat::Csv);
        assert_eq!(OutputFormat::Bin, OutputFormat::Bin);
        assert_ne!(OutputFormat::Csv, OutputFormat::Bin);
    }

    #[test]
    fn test_file_item_debug() {
        let tmp = tempdir().unwrap();
        let test_file = tmp.path().join("test.txt");
        fs::write(&test_file, "content").unwrap();
        let metadata = fs::metadata(&test_file).unwrap();
        
        let item = FileItem {
            name: "test.txt".into(),
            md: metadata,
        };
        
        let debug_str = format!("{:?}", item);
        assert!(debug_str.contains("test.txt"));
    }

    #[test]
    fn test_task_debug() {
        let dir_task = Task::Dir("/test/path".into());
        let debug_str = format!("{:?}", dir_task);
        assert!(debug_str.contains("Dir"));
        assert!(debug_str.contains("test/path"));

        let shutdown_task = Task::Shutdown;
        let debug_str = format!("{:?}", shutdown_task);
        assert!(debug_str.contains("Shutdown"));

        let files_task = Task::Files {
            base: Arc::new("/base".into()),
            items: vec![],
        };
        let debug_str = format!("{:?}", files_task);
        assert!(debug_str.contains("Files"));
    }

    // Test symlink handling (Unix only)
    #[cfg(unix)]
    #[test]
    fn test_enum_dir_with_symlinks() {
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path();
        
        // Create a regular file and a symlink to it
        let target_file = test_dir.join("target.txt");
        fs::write(&target_file, "target content").unwrap();
        
        let symlink_path = test_dir.join("link.txt");
        std::os::unix::fs::symlink(&target_file, &symlink_path).unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let error_count = enum_dir(test_dir, &tx, &inflight, None);
        assert_eq!(error_count, 0);
        
        drop(tx);
        
        // Should find both the regular file and the symlink
        let mut found_files = 0;
        while let Ok(task) = rx.recv() {
            if let Task::Files { items, .. } = task {
                found_files += items.len();
            }
        }
        
        assert_eq!(found_files, 2); // target.txt and link.txt
    }

    // Test large buffer handling
    #[test]
    fn test_write_row_csv_large_buffer() {
        let mut buf = Vec::new();
        
        // Fill buffer to near FLUSH_BYTES to test buffer management
        for _ in 0..1000 {
            buf.extend_from_slice(&vec![b'x'; 1000]);
        }
        
        let initial_len = buf.len();
        let path = std::path::Path::new("test/path");
        let row = Row {
            dev: 1, ino: 2, mode: 755, uid: 1000, gid: 1000,
            size: 1024, blocks: 2, atime: 1234567890, mtime: 1234567891,
        };
        
        write_row_csv(&mut buf, path, &row, false);
        assert!(buf.len() > initial_len);
    }

    #[test]
    fn test_write_row_bin_large_buffer() {
        let mut buf = Vec::new();
        
        // Fill buffer initially
        for _ in 0..1000 {
            buf.extend_from_slice(&vec![b'x'; 1000]);
        }
        
        let initial_len = buf.len();
        let path = std::path::Path::new("test/path/with/long/name");
        let row = Row {
            dev: 1, ino: 2, mode: 755, uid: 1000, gid: 1000,
            size: 1024, blocks: 2, atime: 1234567890, mtime: 1234567891,
        };
        
        write_row_bin(&mut buf, path, &row, false);
        assert!(buf.len() > initial_len);
    }

    // Test various path types for CSV quoting
    #[test]
    fn test_csv_push_path_with_special_chars() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("path with spaces,commas\"quotes\nand\rnewlines");
        csv_push_path_smart_quoted(&mut buf, path);
        
        let result = String::from_utf8(buf).unwrap();
        assert!(result.starts_with('"'));
        assert!(result.ends_with('"'));
        assert!(result.contains(r#""""#)); // Doubled quotes
    }

    // Test Args parsing would require integration with clap, which is complex
    // Instead, test the struct and its debug impl
    #[test]
    fn test_args_debug() {
        let args = Args {
            folders: vec!["folder1".to_string(), "folder2".to_string()],
            output: Some("output.csv".into()),
            workers: Some(8),
            skip: Some("skip_pattern".to_string()),
            bin: false,
            no_atime: true,
            files_hint: Some("1000".to_string()),
            quiet: false,
        };
        
        let debug_str = format!("{:?}", args);
        assert!(debug_str.contains("folder1"));
        assert!(debug_str.contains("output.csv"));
        assert!(debug_str.contains("skip_pattern"));
    }

    // Test edge cases for binary format
    #[test]
    fn test_write_row_bin_empty_path() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("");
        let row = Row {
            dev: 0, ino: 0, mode: 0, uid: 0, gid: 0,
            size: 0, blocks: 0, atime: 0, mtime: 0,
        };
        
        write_row_bin(&mut buf, path, &row, false);
        
        // Should encode empty path length as 0
        let path_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(path_len, 0);
    }

    #[cfg(unix)]
    #[test]  
    fn test_csv_push_bytes_edge_cases() {
        // Test with only quote character
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"\"");
        assert_eq!(&buf, b"\"\"\"\""); // Wrapping quotes + doubled inner quote = """"
        
        // Test empty bytes
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"");
        assert_eq!(&buf, b"");
        
        // Test bytes with all special characters: quote, comma, newline, carriage return
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"\",\n\r");
        // Should be: wrapping quote + doubled quote + comma + newline + carriage return + wrapping quote
        assert_eq!(&buf, b"\"\"\",\n\r\"");
    }

    #[cfg(windows)]
    #[test]
    fn test_csv_push_str_edge_cases() {
        // Test empty string
        let mut buf = Vec::new();
        csv_push_str_smart_quoted(&mut buf, "");
        assert_eq!(&buf, b"");
        
        // Test string with only quote
        let mut buf = Vec::new();
        csv_push_str_smart_quoted(&mut buf, "\"");
        assert_eq!(&buf, b"\"\"\"\"");
        
        // Test UNC path that doesn't start with \\?\UNC\
        let mut buf = Vec::new();
        csv_push_str_smart_quoted(&mut buf, r"\\?\some\other\path");
        assert_eq!(std::str::from_utf8(&buf).unwrap(), r"some\other\path");
    }

    // Test worker function with simple task
    #[test] 
    fn test_worker_simple() {
        let tmp = tempdir().unwrap();
        let test_file = tmp.path().join("test.txt");
        fs::write(&test_file, "content").unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        let progress = Arc::new(Progress::default());
        
        let cfg = Config {
            skip: None,
            out_fmt: OutputFormat::Csv,
            no_atime: false,
            progress: Some(progress.clone()),
            pid: 12345,
        };
        
        // Send a simple files task
        let metadata = fs::metadata(&test_file).unwrap();
        let file_item = FileItem {
            name: "test.txt".into(),
            md: metadata,
        };
        
        tx.send(Task::Files {
            base: Arc::new(tmp.path().to_path_buf()),
            items: vec![file_item],
        }).unwrap();
        
        tx.send(Task::Shutdown).unwrap();
        drop(tx);
        
        // Create a dummy sender for the worker (it won't use it for Files tasks)
        let (dummy_tx, _) = unbounded();
        
        let out_dir = tmp.path().to_path_buf();
        let stats = worker(0, rx, dummy_tx, inflight, out_dir, cfg);
        
        assert_eq!(stats.files, 1);
        assert_eq!(stats.errors, 0);
        assert!(progress.files.load(Relaxed) >= 1);
    }

    #[test]
    fn test_worker_with_binary_output() {
        let tmp = tempdir().unwrap();
        let test_file = tmp.path().join("test.txt");
        fs::write(&test_file, "test content").unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let cfg = Config {
            skip: None,
            out_fmt: OutputFormat::Bin,
            no_atime: true,
            progress: None,
            pid: 12345,
        };
        
        // Send a files task
        let metadata = fs::metadata(&test_file).unwrap();
        let file_item = FileItem {
            name: "test.txt".into(),
            md: metadata,
        };
        
        tx.send(Task::Files {
            base: Arc::new(tmp.path().to_path_buf()),
            items: vec![file_item],
        }).unwrap();
        
        tx.send(Task::Shutdown).unwrap();
        drop(tx);
        
        let (dummy_tx, _) = unbounded();
        let out_dir = tmp.path().to_path_buf();
        let stats = worker(0, rx, dummy_tx, inflight, out_dir, cfg);
        
        assert_eq!(stats.files, 1);
        assert_eq!(stats.errors, 0);
        assert!(stats.bytes > 0);
    }

    #[test]
    fn test_worker_with_skip_pattern() {
        let tmp = tempdir().unwrap();
        let skip_dir = tmp.path().join("skip_this");
        fs::create_dir(&skip_dir).unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let cfg = Config {
            skip: Some("skip_this".to_string()),
            out_fmt: OutputFormat::Csv,
            no_atime: false,
            progress: None,
            pid: 12345,
        };
        
        tx.send(Task::Dir(skip_dir)).unwrap();
        tx.send(Task::Shutdown).unwrap();
        drop(tx);
        
        let (dummy_tx, _) = unbounded();
        let out_dir = tmp.path().to_path_buf();
        let stats = worker(0, rx, dummy_tx, inflight, out_dir, cfg);
        
        // Should skip the directory and not process it
        assert_eq!(stats.files, 0);
    }

    #[test]
    fn test_worker_with_files_task_skip() {
        let tmp = tempdir().unwrap();
        let skip_base = tmp.path().join("skip_this");
        fs::create_dir(&skip_base).unwrap();
        let test_file = skip_base.join("test.txt");
        fs::write(&test_file, "content").unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let cfg = Config {
            skip: Some("skip_this".to_string()),
            out_fmt: OutputFormat::Csv,
            no_atime: false,
            progress: None,
            pid: 12345,
        };
        
        let metadata = fs::metadata(&test_file).unwrap();
        let file_item = FileItem {
            name: "test.txt".into(),
            md: metadata,
        };
        
        tx.send(Task::Files {
            base: Arc::new(skip_base),
            items: vec![file_item],
        }).unwrap();
        
        tx.send(Task::Shutdown).unwrap();
        drop(tx);
        
        let (dummy_tx, _) = unbounded();
        let out_dir = tmp.path().to_path_buf();
        let stats = worker(0, rx, dummy_tx, inflight, out_dir, cfg);
        
        // Should skip files in skipped base directory
        assert_eq!(stats.files, 0);
    }

    #[test]
    fn test_large_buffer_flush() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("test");
        let row = Row {
            dev: 1, ino: 2, mode: 755, uid: 1000, gid: 1000,
            size: 1024, blocks: 2, atime: 1234567890, mtime: 1234567891,
        };
        
        // Fill buffer to exceed FLUSH_BYTES
        while buf.len() < FLUSH_BYTES + 1000 {
            write_row_csv(&mut buf, path, &row, false);
        }
        
        // Buffer should be quite large now
        assert!(buf.len() > FLUSH_BYTES);
    }

    // Test constants
    #[test]
    fn test_constants() {
        assert_eq!(READ_BUF_SIZE, 2 * 1024 * 1024);
        assert_eq!(FILE_CHUNK, 2048);
        assert_eq!(FLUSH_BYTES, 4 * 1024 * 1024);
    }

    // Test Row struct can be created and accessed
    #[test]
    fn test_row_creation() {
        let row = Row {
            dev: 1,
            ino: 2,
            mode: 755,
            uid: 1000,
            gid: 1000,
            size: 1024,
            blocks: 2,
            atime: 1234567890,
            mtime: 1234567891,
        };
        
        assert_eq!(row.dev, 1);
        assert_eq!(row.ino, 2);
        assert_eq!(row.mode, 755);
        assert_eq!(row.uid, 1000);
        assert_eq!(row.gid, 1000);
        assert_eq!(row.size, 1024);
        assert_eq!(row.blocks, 2);
        assert_eq!(row.atime, 1234567890);
        assert_eq!(row.mtime, 1234567891);
    }

    // Test disk calculation in CSV output
    #[test]
    fn test_csv_disk_calculation() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("test");
        let row = Row {
            dev: 1, ino: 2, mode: 755, uid: 1000, gid: 1000,
            size: 1024, blocks: 3, atime: 1234567890, mtime: 1234567891,
        };
        
        write_row_csv(&mut buf, path, &row, false);
        let result = String::from_utf8(buf).unwrap();
        
        // Disk should be blocks * 512 = 3 * 512 = 1536
        assert!(result.contains(",1536,"));
    }

    // Test disk calculation in binary output
    #[test]
    fn test_bin_disk_calculation() {
        let mut buf = Vec::new();
        let path = std::path::Path::new("test");
        let row = Row {
            dev: 1, ino: 2, mode: 755, uid: 1000, gid: 1000,
            size: 1024, blocks: 3, atime: 1234567890, mtime: 1234567891,
        };
        
        write_row_bin(&mut buf, path, &row, false);
        
        // Extract disk value from the end of the binary data
        let disk_offset = buf.len() - 8;
        let disk_bytes = &buf[disk_offset..];
        let disk = u64::from_le_bytes([
            disk_bytes[0], disk_bytes[1], disk_bytes[2], disk_bytes[3],
            disk_bytes[4], disk_bytes[5], disk_bytes[6], disk_bytes[7],
        ]);
        
        assert_eq!(disk, 1536); // 3 * 512
    }

    // Test enum_dir with dot files (should be skipped)
    #[test]
    fn test_enum_dir_skips_dot_files() {
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path();
        
        // Create regular file and dot files
        fs::write(test_dir.join("regular.txt"), "content").unwrap();
        fs::write(test_dir.join(".hidden"), "hidden").unwrap();
        
        // Note: "." and ".." are handled specially, but other dot files are not skipped
        // in the current implementation, so this test verifies the current behavior
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let error_count = enum_dir(test_dir, &tx, &inflight, None);
        assert_eq!(error_count, 0);
        
        drop(tx);
        
        let mut found_files = Vec::new();
        while let Ok(task) = rx.recv() {
            if let Task::Files { items, .. } = task {
                for item in items {
                    found_files.push(item.name.to_string_lossy().to_string());
                }
            }
        }
        
        // Should find both regular.txt and .hidden
        assert!(found_files.contains(&"regular.txt".to_string()));
        assert!(found_files.contains(&".hidden".to_string()));
    }

    // Test merge with very large number of threads
    #[test]
    fn test_merge_shards_many_threads() -> Result<()> {
        let tmp = tempdir()?;
        let out_dir = tmp.path().to_path_buf();
        let final_path = out_dir.join("out.csv");
        let pid = 123;
        let num_threads = 100; // More threads than we have shards
        
        // Create only 2 shards
        let shard0 = out_dir.join(format!("shard_{}_{}_0.tmp", get_hostname(), pid));
        let shard50 = out_dir.join(format!("shard_{}_{}_50.tmp", get_hostname(), pid));
        
        {
            let mut w = File::create(&shard0)?;
            w.write_all(b"data0\n")?;
        }
        {
            let mut w = File::create(&shard50)?;
            w.write_all(b"data50\n")?;
        }
        
        merge_shards(&out_dir, &final_path, num_threads, OutputFormat::Csv, false, pid)?;
        
        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let lines: Vec<&str> = s.lines().collect();
        
        assert_eq!(lines[0], "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        assert!(lines.contains(&"data0"));
        assert!(lines.contains(&"data50"));
        Ok(())
    }

    // Test Config with different combinations
    #[test]
    fn test_config_variations() {
        let cfg1 = Config {
            skip: None,
            out_fmt: OutputFormat::Bin,
            no_atime: false,
            progress: None,
            pid: 1,
        };
        
        let cfg2 = cfg1.clone();
        assert_eq!(cfg1.out_fmt, cfg2.out_fmt);
        assert_eq!(cfg1.no_atime, cfg2.no_atime);
        assert_eq!(cfg1.pid, cfg2.pid);
    }

    // Test error handling in enum_dir with permission issues
    #[cfg(unix)]
    #[test]
    fn test_enum_dir_permission_errors() {
        use std::os::unix::fs::PermissionsExt;
        
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path().join("no_read");
        fs::create_dir(&test_dir).unwrap();
        
        // Create a file inside
        fs::write(test_dir.join("file.txt"), "content").unwrap();
        
        // Remove read permissions from directory
        let mut perms = fs::metadata(&test_dir).unwrap().permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&test_dir, perms).unwrap();
        
        let (tx, _rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let error_count = enum_dir(&test_dir, &tx, &inflight, None);
        
        // Restore permissions for cleanup
        let mut perms = fs::metadata(&test_dir).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&test_dir, perms).unwrap();
        
        // Should have error due to permission denial
        assert_eq!(error_count, 1);
    }

    // Test different metadata scenarios
    #[test] 
    fn test_stat_row_directory() {
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path().join("testdir");
        fs::create_dir(&test_dir).unwrap();
        
        let row = stat_row(&test_dir);
        assert!(row.is_some());
        
        #[cfg(unix)]
        {
            let row = row.unwrap();
            // Directory should have different mode bits
            assert!(row.mode > 0);
        }
    }

    // Test worker handles stat_row failure
    #[test]
    fn test_worker_stat_row_failure() {
        let tmp = tempdir().unwrap();
        
        let (tx, rx) = unbounded();
        let inflight = Arc::new(AtomicUsize::new(0));
        
        let cfg = Config {
            skip: None,
            out_fmt: OutputFormat::Csv,
            no_atime: false,
            progress: None,
            pid: 12345,
        };
        
        // Send a directory task for a path that will cause stat_row to fail
        let nonexistent = tmp.path().join("nonexistent");
        tx.send(Task::Dir(nonexistent)).unwrap();
        tx.send(Task::Shutdown).unwrap();
        drop(tx);
        
        let (dummy_tx, _) = unbounded();
        let out_dir = tmp.path().to_path_buf();
        let stats = worker(0, rx, dummy_tx, inflight, out_dir, cfg);
        
        // Should have errors from the failed stat_row and enum_dir
        assert_eq!(stats.files, 0);
        assert!(stats.errors >= 1, "Expected at least 1 error, got {}", stats.errors);
    }

    // Test that buffers are properly reserved
    #[test]
    fn test_buffer_reservation() {
        let mut buf = Vec::new();
        let initial_capacity = buf.capacity();
        
        let path = std::path::Path::new("some/test/path");
        let row = Row {
            dev: 1, ino: 2, mode: 755, uid: 1000, gid: 1000,
            size: 1024, blocks: 2, atime: 1234567890, mtime: 1234567891,
        };
        
        write_row_csv(&mut buf, path, &row, false);
        
        // Capacity should have increased due to reserve calls
        assert!(buf.capacity() >= initial_capacity + 256);
    }

    // Test binary buffer reservation
    #[test] 
    fn test_binary_buffer_reservation() {
        let mut buf = Vec::new();
        let initial_capacity = buf.capacity();
        
        let path = std::path::Path::new("some/test/path/that/is/longer");
        let row = Row {
            dev: 1, ino: 2, mode: 755, uid: 1000, gid: 1000,
            size: 1024, blocks: 2, atime: 1234567890, mtime: 1234567891,
        };
        
        write_row_bin(&mut buf, path, &row, false);
        
        // Capacity should have increased
        assert!(buf.capacity() >= initial_capacity + 80 + path.as_os_str().len());
    }

    // Comprehensive test that covers nearly all code paths
    #[test]
    fn test_integration_csv_and_binary() {
        let tmp = tempdir().unwrap();
        let test_dir = tmp.path().join("integration_test");
        fs::create_dir(&test_dir).unwrap();
        
        // Create various file types with safe names for Windows
        fs::write(test_dir.join("file1.txt"), "content1").unwrap();
        fs::write(test_dir.join("file_with_spaces.txt"), "content with spaces").unwrap();
        fs::write(test_dir.join("file_with_underscores.txt"), "quoted content").unwrap();
        
        let subdir = test_dir.join("subdir");
        fs::create_dir(&subdir).unwrap();
        fs::write(subdir.join("nested.txt"), "nested content").unwrap();
        
        // Test both CSV and Binary output
        for &(output_format, no_atime) in &[(OutputFormat::Csv, false), 
                                             (OutputFormat::Csv, true), 
                                             (OutputFormat::Bin, false),
                                             (OutputFormat::Bin, true)] {
            let (tx, rx) = unbounded();
            let inflight = Arc::new(AtomicUsize::new(0));
            let progress = Arc::new(Progress::default());
            
            let cfg = Config {
                skip: None,
                out_fmt: output_format,
                no_atime,
                progress: Some(progress.clone()),
                pid: 98765,
            };
            
            // Send files tasks instead of directory tasks to avoid hanging
            let files = ["file1.txt", "file_with_spaces.txt", "file_with_underscores.txt"];
            for file_name in &files {
                let file_path = test_dir.join(file_name);
                if let Ok(metadata) = fs::metadata(&file_path) {
                    let file_item = FileItem {
                        name: (*file_name).into(),
                        md: metadata,
                    };
                    
                    tx.send(Task::Files {
                        base: Arc::new(test_dir.clone()),
                        items: vec![file_item],
                    }).unwrap();
                }
            }
            
            tx.send(Task::Shutdown).unwrap();
            drop(tx);
            
            let (dummy_tx, _) = unbounded();
            let out_dir = tmp.path().to_path_buf();
            let stats = worker(0, rx, dummy_tx, inflight, out_dir.clone(), cfg);
            
            // Should process all files
            assert!(stats.files >= 3, "Expected at least 3 files, got {}", stats.files);
            assert_eq!(stats.errors, 0);
            assert!(stats.bytes > 0);
            assert!(progress.files.load(Relaxed) >= 3);
            
            // Check that shard file was created
            let shard_path = out_dir.join(format!("shard_{}_{}_0.tmp", get_hostname(), 98765));
            if shard_path.exists() {
                let _ = fs::remove_file(shard_path);
            }
        }
    }
}