use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::{thread, time::Instant, io::Read};
use crossbeam::channel::{unbounded, Receiver, Sender};
use num_cpus;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use itoa::Buffer;
use clap::Parser;

const FILE_CHUNK: usize = 4096;      // entries per work unit
const FLUSH_BYTES: usize = 2 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(author, version, about = "Super Fast FS Scanner")]
struct Args {
    /// Root folder to scan
    #[arg(default_value = ".")]
    root: String,
    /// Detect file category (text/binary/image/video/etc)
    #[arg(long)]
    category: bool,
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

#[derive(Debug)]
enum Category { Jpeg, Png, Gif, Pdf, Zip, Gzip, Mp3, Elf, PE, Tar, Text, Binary, Unknown }

#[derive(Clone)]
struct Config {
    want_category: bool,
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
    //println!("fullpath: {}", &fullpath);
    println!("output: {}", &final_path.display());

    let threads = num_cpus::get().max(1);
    let out_dir = PathBuf::from(".");
    //fs::create_dir_all(&out_dir)?;

    // ---- work queue + inflight counter ----
    let (tx, rx) = unbounded::<Task>();
    let inflight = Arc::new(AtomicUsize::new(0));

    // seed roots
    inflight.fetch_add(1, SeqCst);
    tx.send(Task::Dir(PathBuf::from(folder))).expect("enqueue root");

    // shutdown notifier: when inflight hits 0, broadcast Shutdown
    {
        let tx = tx.clone();
        let inflight = inflight.clone();
        thread::spawn(move || loop {
            if inflight.load(SeqCst) == 0 {
                for _ in 0..threads {
                    let _ = tx.send(Task::Shutdown);
                }
                break;
            }
            thread::sleep(std::time::Duration::from_millis(20));
        });
    }

    let cfg = Config { want_category: args.category };

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
    drop(tx); // main thread no longer sends tasks

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

    // This uses your current counter (files + dirs)
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
    let mut shard =
        BufWriter::with_capacity(8 * 1024 * 1024, File::create(&shard_path).expect("open shard"));
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024 * 1024);

    let mut stats = Stats {
        files: 0,
        largest_file_count: 0,
        largest_file_count_dir: PathBuf::new(),
    };

    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,

            Task::Dir(dir) => {
                // emit one row for the directory itself (counts as an entry)
                if let Some(row) = stat_row(&dir, &cfg) {
                    write_row(&mut buf, row);
                    stats.files += 1;

                    if buf.len() >= FLUSH_BYTES {
                        let _ = shard.write_all(&buf);
                        buf.clear();
                    }
                }

                // enumerate children and push work; get child entry count
                let child_entries = enum_dir(&dir, &tx, &inflight);

                // update "largest by entry count" (children only, like ls count)
                if child_entries > stats.largest_file_count {
                    stats.largest_file_count = child_entries;
                    stats.largest_file_count_dir = dir.clone();
                }

                inflight.fetch_sub(1, SeqCst);
            }

            Task::Files { base, names } => {
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
                inflight.fetch_sub(1, SeqCst);
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

       let is_dir = dent
            .file_type()
            .map(|t| t.is_dir())
            .unwrap_or_else(|_| dent.path().is_dir());

        if is_dir {
            inflight.fetch_add(1, SeqCst);
            let _ = tx.send(Task::Dir(dent.path()));
        } else {
            page.push(name);
            if page.len() == FILE_CHUNK {
                inflight.fetch_add(1, SeqCst);
                let _ = tx.send(Task::Files {
                    base: dir.to_path_buf(),
                    names: std::mem::take(&mut page),
                });
            }
        }
    }

    if !page.is_empty() {
        inflight.fetch_add(1, SeqCst);
        let _ = tx.send(Task::Files {
            base: dir.to_path_buf(),
            names: page,
        });
    }

    entry_count
}

fn detect_category(path: &Path) -> Category {
    let mut f = match File::open(path) { Ok(f) => f, Err(_) => return Category::Unknown };
    let mut hdr = [0u8; 32];
    let n = match f.read(&mut hdr) { Ok(n) => n, Err(_) => 0 };
    let h = &hdr[..n];

    if h.starts_with(&[0xFF,0xD8,0xFF]) { return Category::Jpeg }
    if h.starts_with(&[0x89, b'P', b'N', b'G']) { return Category::Png }
    if h.starts_with(b"GIF8") { return Category::Gif }
    if h.starts_with(b"%PDF") { return Category::Pdf }
    if h.starts_with(&[0x50,0x4B,0x03,0x04]) { return Category::Zip }
    if h.starts_with(&[0x1F,0x8B]) { return Category::Gzip }
    if h.starts_with(&[0x49,0x44,0x33]) { return Category::Mp3 }
    if h.starts_with(&[0x7F,b'E',b'L',b'F']) { return Category::Elf }
    if h.starts_with(&[0x4D,0x5A]) { return Category::PE }
    if n>265 && &hdr[257..262]==b"ustar" { return Category::Tar }

    // fallback
    let mut printable=0usize; let mut ctrl=0usize;
    for &b in h {
        match b {
            0x20..=0x7E | b'\t'|b'\n'|b'\r' => printable+=1,
            0 => { ctrl+=1; break; }
            _ => ctrl+=1
        }
    }
    if ctrl == 0 || printable > ctrl { Category::Text } else { Category::Binary }
}

struct Row<'a> {
    path: &'a Path,
    category: Option<Category>,
    dev: u64,
    ino: u64,
    mode: u32,
    //nlink: u64,
    uid: u32,
    gid: u32,
    size: u64,
    //blksize: u64,
    blocks: u64,
    atime: i64,
    mtime: i64,
    //ctime: i64,
}

fn stat_row<'a>(path: &'a Path, cfg: &'a Config) -> Option<Row<'a>> {
    let md = fs::symlink_metadata(path).ok()?;
    #[cfg(unix)]
    {
        Some(Row {
            path,
            category: if cfg.want_category { Some(detect_category(path)) } else { None },
            dev: md.dev(),
            ino: md.ino(),
            mode: md.mode(),
            //nlink: md.nlink() as u64,
            uid: md.uid(),
            gid: md.gid(),
            size: md.size(),
            //blksize: md.blksize() as u64,
            blocks: md.blocks() as u64,
            atime: md.atime(),
            mtime: md.mtime(),
            //ctime: md.ctime(),
        })
    }
    #[cfg(not(unix))]
    {
        Some(Row {
            path,
            category: if cfg.want_category { Some(detect_category(path)) } else { None },
            dev: 0,
            ino: 0,
            mode: 0,
            //nlink: 0,
            uid: 0,
            gid: 0,
            size: md.len(),
            //blksize: 0,
            blocks: 0,
            atime: 0,
            mtime: 0,
            //ctime: 0,
        })
    }
}

fn write_row(buf: &mut Vec<u8>, r: Row<'_>) {
    // INODE: "<dev>-<ino>"
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

    // SIZE (logical), DISK (allocated bytes = blocks * 512)
    push_u64(buf, r.size);  
    push_comma(buf);
    //let disk = if (r.mode & 0o170000) == 0o040000 { 0 } else { r.blocks * 512 };
    let disk = r.blocks * 512;
    push_u64(buf, disk); 
    push_comma(buf);
    
    // CAT
    if let Some(cat) = r.category {
        let s = match cat {
            Category::Jpeg=>"jpeg", Category::Png=>"png", Category::Gif=>"gif",
            Category::Pdf=>"pdf", Category::Zip=>"zip", Category::Gzip=>"gzip",
            Category::Mp3=>"mp3", Category::Elf=>"elf", Category::PE=>"pe",
            Category::Tar=>"tar", Category::Text=>"text", Category::Binary=>"binary",
            Category::Unknown=>"",
        };
        buf.extend_from_slice(s.as_bytes());
    }
    push_comma(buf);

    // PATH (always quoted)
    csv_push_path_always_quoted(buf, r.path);
    buf.push(b'\n');
}

fn csv_push_path_always_quoted(buf: &mut Vec<u8>, p: &Path) {
    let s = p.to_string_lossy();
    buf.push(b'"');
    for b in s.bytes() {
        if b == b'"' {
            buf.extend_from_slice(br#""""#); // escape quotes
        } else {
            buf.push(b);
        }
    }
    buf.push(b'"');
}

fn merge_shards(out_dir: &Path, final_path: &Path, threads: usize) -> std::io::Result<()> {
    //let final_path = out_dir.join("final.csv");
    // if final_path.exists() {
    //     fs::remove_file(final_path)?;
    // }
    let mut out = BufWriter::new(File::create(&final_path)?);
    out.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,CAT,PATH\n")?;

    let mut buf = Vec::with_capacity(FLUSH_BYTES);
    for tid in 0..threads {
        let shard = out_dir.join(format!("shard_{tid}.csv.tmp"));
        if !shard.exists() {
            continue;
        }
        let mut f = File::open(&shard)?;
        use std::io::Read;
        buf.clear();
        f.read_to_end(&mut buf)?;
        out.write_all(&buf)?; // shards have no header
        let _ = fs::remove_file(shard);
    }
    out.flush()?;
    Ok(())
}

#[inline] fn push_comma(buf: &mut Vec<u8>) { buf.push(b','); }
#[inline]
fn push_u32(out: &mut Vec<u8>, v: u32) { let mut b = Buffer::new(); out.extend_from_slice(b.format(v).as_bytes()); }
#[inline]
fn push_u64(out: &mut Vec<u8>, v: u64) { let mut b = Buffer::new(); out.extend_from_slice(b.format(v).as_bytes()); }
#[inline]
fn push_i64(out: &mut Vec<u8>, v: i64) { let mut b = Buffer::new(); out.extend_from_slice(b.format(v).as_bytes()); }
