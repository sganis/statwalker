#[allow(unused)]
use anyhow::{Error, Result};
use chrono::offset::Utc;
use chrono::DateTime;
use std::env;
use std::fs;
use std::io::{BufWriter, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use walkdir::WalkDir;

//fn work(index: i32, chunk: &[String]) -> Result<()> {
fn work<T: Write>(
    index: i32,
    chunk: Vec<String>,
    global_writer: Arc<Mutex<BufWriter<T>>>,
) -> Result<()> {
    let local_path = format!("file-{index}.csv");
    let f = fs::File::create(&local_path).expect("Unable to open file");
    let mut local_writer = BufWriter::new(f);

    for path in chunk.iter() {
        let metadata = fs::symlink_metadata(path)?;
        let size = metadata.len();
        //let accessed = metadata.accessed().unwrap();
        //let modified = metadata.modified().unwrap();
        let accessed: DateTime<Utc> = metadata.modified()?.into();
        let modified: DateTime<Utc> = metadata.modified()?.into();
        #[cfg(unix)]
        let mode = metadata.permissions().mode();
        #[cfg(windows)]
        let mode = 0;
        //let filetype = if metadata.is_dir() { "DIR" } else { "REG" };
        writeln!(
            local_writer,
            "{},{},{},{},\"{}\"",
            accessed.format("%Y-%m-%d"),
            modified.format("%Y-%m-%d"),
            //accessed.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            //modified.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            //filetype,
            mode,
            size,
            path
        )?;
    }
    let _ = local_writer.flush();

    // append local file to global file
    let mut input = fs::File::open(&local_path)?;
    let mut s = String::new();
    input.read_to_string(&mut s)?;
    write!(global_writer.lock().unwrap(), "{}", s)?;
    fs::remove_file(&local_path).expect("could not remove file");

    Ok(())
}

fn main() -> Result<()> {
    let now = Instant::now();
    let args: Vec<String> = env::args().collect();
    let fullpath = fs::canonicalize(&args[1])?
        .into_os_string()
        .into_string()
        .unwrap();

    #[cfg(unix)]
    let name = fullpath[1..].replace("/", "-");
    #[cfg(windows)]
    let name = fullpath[3..].replace('\\', "-");

    let path = format!("{name}.csv");
    let final_path = Path::new(&path);

    if final_path.exists() {
        fs::remove_file(final_path)?;
    }

    let file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .append(true)
        .open(final_path)?;
    let writer = Arc::new(Mutex::new(BufWriter::new(file)));
    let header = "ACCESSED,MODIFIED,MODE,SIZE,PATH\n";
    write!(writer.lock().unwrap(), "{header}").expect("cannot create final file");

    let mut files = Vec::<String>::new();
    for entry in WalkDir::new(&fullpath).into_iter().filter_map(|e| e.ok()) {
        let path = String::from(entry.path().to_string_lossy());
        files.push(path);
    }
    let total_files = files.len();
    let nprocs = num_cpus::get();
    let chunk_size = (total_files + nprocs - 1) / nprocs;

    //let chunks: Vec<&[String]> = files.chunks(n).collect();
    let chunks: Vec<Vec<String>> = files.chunks(chunk_size).map(|s| s.into()).collect();
    println!(
        "files: {}, nprocs: {}, chunk size: {}, rest: {}",
        total_files,
        nprocs,
        chunk_size,
        chunks[chunks.len() - 1].len()
    );

    // todo: parallelize this
    // for (index, chunk) in chunks.iter().enumerate() {
    //     work(index as i32, chunk)?;
    // }

    let mut handles = Vec::new();

    for (index, chunk) in chunks.into_iter().enumerate() {
        let writer = Arc::clone(&writer);
        let handle = thread::spawn(move || work(index as i32, chunk, writer));
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.join().expect("Could not join thread");
    }

    // for index in 0..nprocs {
    //     let path = format!("file-{index}.csv");
    //     let mut input = fs::File::open(&path)?;
    //     let mut s = String::new();
    //     input.read_to_string(&mut s)?;
    //     write!(writer.lock().unwrap(), "{}", s)?;
    //     fs::remove_file(&path).expect("could not remove file");
    // }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    Ok(())
}
