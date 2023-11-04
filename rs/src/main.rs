#[allow(unused)]
use anyhow::{Error, Result};
use chrono::offset::Utc;
use chrono::DateTime;
use std::env;
use std::fs;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
//use std::process::Command;
use std::thread;
use std::time::Instant;
//use std::time::SystemTime;
//use std::time::UNIX_EPOCH;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use walkdir::WalkDir;

//fn work(index: i32, chunk: &[String]) -> Result<()> {
fn work(index: i32, chunk: Vec<String>) -> Result<()> {
    let f = fs::File::create(format!("file-{index}.csv")).expect("Unable to open file");
    let mut writer = BufWriter::new(f);

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
            writer,
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
    Ok(())
}

fn main() -> Result<()> {
    let now = Instant::now();
    let args: Vec<String> = env::args().collect();
    let folder = &args[1];

    // println!("folder: {folder}");
    // let metadata = fs::metadata(folder).unwrap();
    // println!("{:?}", metadata.file_type());

    //let mut count = 0;
    let mut files = Vec::<String>::new();

    for entry in WalkDir::new(folder).into_iter().filter_map(|e| e.ok()) {
        let path = String::from(entry.path().to_string_lossy());
        files.push(path);
    }
    let total_files = files.len();
    let nprocs = num_cpus::get();
    let n = (total_files + nprocs - 1) / nprocs;
    //let chunks: Vec<&[String]> = files.chunks(n).collect();
    let chunks: Vec<Vec<String>> = files.chunks(n).map(|s| s.into()).collect();
    println!(
        "files: {}, nprocs: {}, chunk size: {}, rest: {}",
        total_files,
        nprocs,
        n,
        chunks[chunks.len() - 1].len()
    );

    // todo: parallelize this
    // for (index, chunk) in chunks.iter().enumerate() {
    //     work(index as i32, chunk)?;
    // }

    let mut handles = Vec::new();
    for (index, chunk) in chunks.into_iter().enumerate() {
        let handle = thread::spawn(move || work(index as i32, chunk));
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.join().expect("Could not join thread");
    }

    // collect files
    let final_path = Path::new("file.csv");
    let header = "ACCESSED,MODIFIED,MODE,SIZE,PATH\n";
    if final_path.exists() {
        fs::remove_file(final_path)?;
    }
    let file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .append(true)
        .open(final_path)?;
    let mut buf = BufWriter::new(file);
    write!(buf, "{header}").expect("cannot create final file");

    //fs::write(final_path, header).expect("Unable to write final file");
    for index in 0..nprocs {
        let path = format!("file-{index}.csv");
        let mut input = fs::File::open(&path)?;
        let mut s = String::new();
        input.read_to_string(&mut s)?;
        write!(buf, "{}", s)?;
        fs::remove_file(&path).expect("could not remove file");
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    Ok(())
}
