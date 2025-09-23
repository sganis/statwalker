// duscan.rs
use anyhow::{Context, Result};
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io::{self, BufWriter, Write, BufRead, BufReader, Read},
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
    csv_push_path_smart_quoted, format_duration, get_hostname, strip_verbatim_prefix,
    push_u32, push_u64, push_i64, push_comma, row_from_metadata, stat_row,
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
    /// Folder to scan (required)
    folder: String,
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
    errors: AtomicU64,
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
}

fn main() -> Result<()> {

    print_about();

    let args = Args::parse();
    let out_fmt = if args.bin { OutputFormat::Bin } else { OutputFormat::Csv };
    
    if args.no_atime {
        eprintln!("{}", "ATIME will be written as 0 and lines sorted for reproducible output.".yellow());
    }

    // Canonicalize root
    let root = fs::canonicalize(&args.folder)?;
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

    println!("Local time   : {}", now.format("%Y-%m-%d %H:%M:%S").to_string());
    println!("Host         : {}", hostname);
    println!("Command      : {}", cmd.join(" "));
    println!("Input        : {}", &root_str);
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
                //let e = progress_for_reporter.errors.load(Relaxed);
                let elapsed = start_for_reporter.elapsed().as_secs_f64().max(0.001);
                let rate_f = human_count((f as f64 / elapsed) as u64);                

                if let Some(total) = hinted_files {
                    let mut pct = ((f as f64 / total as f64) * 100.0).min(100.0);
                    if pct < last_pct { pct = last_pct; }
                    last_pct = pct;
                    let bar = progress_bar(pct.into(), 25);
                    eprint!(
                        "\r    {} {} {:>3}% | Files: {} [{} files/s]        \r",
                        "Progress".cyan().bold(), bar, pct as u32, human_count(f), rate_f
                    );
                } else {
                    eprint!(
                        "\r    {} : Files: {} [{} files/s]        \r",
                        "Progress".cyan().bold(), human_count(f), rate_f
                    );
                }
                thread::sleep(Duration::from_millis(1000));
            }
            eprint!("\r{}"," ".repeat(120));
        }));
    }

    let start_time = Instant::now();
    
    // seed roots
    inflight.fetch_add(1, Relaxed);
    tx.send(Task::Dir(PathBuf::from(root))).expect("enqueue root");

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
    merge_shards(&out_dir, &final_path, workers, out_fmt, sort_csv).expect("merge shards failed");

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
    println!("{}","-".repeat(40).cyan().bold());
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
    let shard_path = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
    let file = File::create(&shard_path).expect("open shard");
    let mut base = BufWriter::with_capacity(32 * 1024 * 1024, file);
    let has_progress = cfg.progress.is_some();  
    let progress = cfg.progress.unwrap_or_default();

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
                        write_row_bin(&mut buf, &row, cfg.no_atime); 
                    } else { 
                        write_row_csv(&mut buf, &row, cfg.no_atime);
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
                    progress.errors.fetch_add(error_count, Relaxed);
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
                    let row = row_from_metadata(&full, &md); // <-- no syscall here
                    if is_bin { 
                        write_row_bin(&mut buf, &row, cfg.no_atime);
                    } else { 
                        write_row_csv(&mut buf, &row, cfg.no_atime);
                    }
                    stats.files += 1;
                    stats.bytes += &row.blocks * 512;
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
fn write_row_csv(buf: &mut Vec<u8>, r: &Row<'_>, no_atime: bool) {
    buf.reserve(256);
    // INODE as dev-ino
    push_u64(buf, r.dev);
    buf.push(b'-');
    push_u64(buf, r.ino);
    push_comma(buf);

    // ATIME (zeroed if requested)
    if no_atime { push_i64(buf, 0); } else { push_i64(buf, r.atime); }
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
fn write_row_bin(buf: &mut Vec<u8>, r: &Row<'_>, no_atime: bool) {
    
    #[cfg(unix)]
    let path_bytes: &[u8] = {
        r.path.as_os_str().as_bytes()
    };

    #[cfg(not(unix))]
    let path_lossy = r.path.to_string_lossy();     // keep the Cow alive
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
    workers: usize, 
    out_fmt: OutputFormat,
    sort_csv: bool,
) -> Result<()> {
    let mut out = BufWriter::with_capacity(16 * 1024 * 1024, File::create(&final_path)?);

    match out_fmt {
        OutputFormat::Csv => merge_shards_csv(out_dir, &mut out, workers, sort_csv),
        OutputFormat::Bin => merge_shards_bin(out_dir, &mut out, workers),
    }?;

    out.flush()?;
    Ok(())
}

fn merge_shards_csv(out_dir: &Path, out: &mut BufWriter<File>, workers: usize, sort_csv: bool) -> Result<()> {
    out.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n")?;
    let hostname = get_hostname();

    if !sort_csv {
        // Old behavior: stream in shard order
        for tid in 0..workers {
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
        return Ok(());
    }

    // Sorted mode (only used when --skip-atime and CSV)
    let mut lines: Vec<String> = Vec::new();

    for tid in 0..workers {
        let shard = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
        if !shard.exists() { continue; }

        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);

        // Skip shard header
        let mut throwaway = Vec::<u8>::with_capacity(128);
        reader.read_until(b'\n', &mut throwaway)?;

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
    workers: usize, 
) -> Result<()> {
    let hostname = get_hostname();
    for tid in 0..workers {
        let shard = out_dir.join(format!("shard_{hostname}_{tid}.tmp"));
        if !shard.exists() { continue; }
        let f = File::open(&shard)?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, f);
        io::copy(&mut reader, out)?;
        let _ = fs::remove_file(shard);
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
        assert!(super::should_skip(&p, Some("b/c")));
        assert!(!super::should_skip(&p, Some("x")));
        assert!(!super::should_skip(&p, None));
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

        // sort_csv = false → just concatenates
        super::merge_shards(&out_dir, &final_path, 2, OutputFormat::Csv, false)?;

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

        // Create 2 CSV shards (out of order)
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

        // sort_csv = true → sorted result
        super::merge_shards(&out_dir, &final_path, 2, OutputFormat::Csv, true)?;

        let mut s = String::new();
        File::open(&final_path)?.read_to_string(&mut s)?;
        let mut lines: Vec<&str> = s.lines().collect();

        assert_eq!(lines.remove(0), "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH");
        assert_eq!(lines, vec!["a", "b"]);
        Ok(())
    }

    // Test for no_atime functionality (Unix-only due to uid type)
    #[cfg(unix)]
    #[test]
    fn test_no_atime_in_csv() {
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
