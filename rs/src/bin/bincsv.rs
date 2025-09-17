// src/bin/bincsv.rs
//
// Convert Statwalker binary stream (.bin or .zst) to CSV.
// Autodetects compression via zstd header bytes: magic == 0xFD2FB528
//
// Record (repeated):
//   u32 path_len
//   [path_len] path bytes (Unix: raw bytes; Windows: UTF-8 string written by writer)
//   u64 dev
//   u64 ino
//   i64 atime
//   i64 mtime
//   u32 uid
//   u32 gid
//   u32 mode
//   u64 size
//   u64 disk
//
// Output CSV header:
// INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH

use clap::{Parser, ColorChoice};
use colored::Colorize;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write, Seek};
use std::path::PathBuf;

const READ_BUF_SIZE: usize = 2 * 1024 * 1024; // 2 MiB
const WRITE_BUF_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

#[derive(Parser, Debug)]
#[command(version, color = ColorChoice::Auto)]
struct Args {
    /// Input STWK file (.bin or .zst). Compression is auto-detected from header.
    input: PathBuf,

    /// Output CSV path (default: same as input but with .csv)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

fn main() -> io::Result<()> {
    #[cfg(windows)]
    colored::control::set_virtual_terminal(true).unwrap_or(());

    println!("{}","------------------------------------------------".cyan().bold());
    println!("{}", "Statwaker bincsv: convert binary to csv".cyan().bold());
    println!("{}", format!("Version    : {}", env!("CARGO_PKG_VERSION")).cyan().bold());
    println!("{}", format!("Build date : {}", env!("BUILD_DATE")).cyan().bold());
    println!("{}","------------------------------------------------".cyan().bold());

    let args = Args::parse();

    // Open input and read STWK header from the raw File (not BufReader) so we can
    // hand off the same file handle to zstd Decoder positioned after the header.
    let f = File::open(&args.input)?;
    let mut f = f; // mutable for Read

    // Peek first 4 bytes
    let mut magic_buf = [0u8; 4];
    f.read_exact(&mut magic_buf)?;
    f.rewind()?; // reset to start
    let magic = u32::from_le_bytes(magic_buf);

    // Detect format
    let reader: Box<dyn Read> = if magic == 0xFD2FB528 {
        // Standard Zstd compressed stream
        let dec = zstd::stream::read::Decoder::new(f)?;
        Box::new(dec)
    } else {
        // Raw uncompressed Statwalker binary
        Box::new(f)
    };

    let mut r = BufReader::with_capacity(READ_BUF_SIZE, reader);

    // Decide output path
    let out_path = args
        .output
        .unwrap_or_else(|| args.input.with_extension("csv"));
    
    if out_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("Output file already exists: {}", out_path.display()),
        ));
    }

    let out_file = File::create(&out_path)?;
    let mut w = BufWriter::with_capacity(WRITE_BUF_SIZE, out_file);

    // CSV header
    w.write_all(b"INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH\n")?;

    // Reusable buffers
    let mut line = Vec::<u8>::with_capacity(256);
    let mut path_buf = Vec::<u8>::with_capacity(512);

    loop {
        // Read path_len; if we are exactly at EOF, stop.
        let path_len = match read_u32_le_opt(&mut r)? {
            None => break, // clean EOF at record boundary
            Some(v) => v as usize,
        };

        // Read path bytes
        path_buf.resize(path_len, 0);
        read_exact_fully(&mut r, &mut path_buf)?;

        // Read the fixed fields
        let dev = read_u64_le_exact(&mut r)?;
        let ino = read_u64_le_exact(&mut r)?;
        let atime = read_i64_le_exact(&mut r)?;
        let mtime = read_i64_le_exact(&mut r)?;
        let uid = read_u32_le_exact(&mut r)?;
        let gid = read_u32_le_exact(&mut r)?;
        let mode = read_u32_le_exact(&mut r)?;
        let size = read_u64_le_exact(&mut r)?;
        let disk = read_u64_le_exact(&mut r)?;

        // Compose CSV line: INODE (dev-ino), ATIME, MTIME, UID, GID, MODE, SIZE, DISK, PATH
        line.clear();

        // dev-ino
        push_u64(&mut line, dev);
        line.push(b'-');
        push_u64(&mut line, ino);
        line.push(b',');
        // atime, mtime
        push_i64(&mut line, atime);
        line.push(b',');
        push_i64(&mut line, mtime);
        line.push(b',');
        // uid, gid, mode
        push_u32(&mut line, uid);
        line.push(b',');
        push_u32(&mut line, gid);
        line.push(b',');
        push_u32(&mut line, mode);
        line.push(b',');
        // size, disk
        push_u64(&mut line, size);
        line.push(b',');
        push_u64(&mut line, disk);
        line.push(b',');
        // PATH (CSV-quoted as needed)
        csv_push_path(&mut line, &path_buf);
        line.push(b'\n');

        w.write_all(&line)?;
    }

    w.flush()?;
    eprintln!(
        "Wrote CSV: {}",
        out_path.as_path().to_string_lossy()
    );
    Ok(())
}

// ---------- CSV path quoting (Unix vs Windows) ----------

// On Unix we should preserve raw path bytes as written by walk (can be non-UTF8).
#[cfg(unix)]
fn csv_push_path(out: &mut Vec<u8>, path_bytes: &[u8]) {
    let needs_quoting = path_bytes
        .iter()
        .any(|&b| b == b'"' || b == b',' || b == b'\n' || b == b'\r');

    if !needs_quoting {
        out.extend_from_slice(path_bytes);
    } else {
        out.push(b'"');
        for &b in path_bytes {
            if b == b'"' {
                out.push(b'"');
                out.push(b'"');
            } else {
                out.push(b);
            }
        }
        out.push(b'"');
    }
}

// On Windows the writer already emitted a normalized UTF-8 path string.
// We can safely treat it as UTF-8 (lossy if needed) and quote for CSV.
#[cfg(windows)]
fn csv_push_path(out: &mut Vec<u8>, path_bytes: &[u8]) {
    let s = String::from_utf8_lossy(path_bytes);
    let needs_quoting = s.chars().any(|c| c == '"' || c == ',' || c == '\n' || c == '\r');
    if !needs_quoting {
        out.extend_from_slice(s.as_bytes());
    } else {
        out.push(b'"');
        for b in s.bytes() {
            if b == b'"' {
                out.push(b'"');
                out.push(b'"');
            } else {
                out.push(b);
            }
        }
        out.push(b'"');
    }
}

// ---------- Fast integer-to-string (itoa) ----------
use itoa::Buffer as ItoaBuf;

thread_local! {
    static U32BUF: std::cell::RefCell<ItoaBuf> = std::cell::RefCell::new(ItoaBuf::new());
    static U64BUF: std::cell::RefCell<ItoaBuf> = std::cell::RefCell::new(ItoaBuf::new());
    static I64BUF: std::cell::RefCell<itoa::Buffer> = std::cell::RefCell::new(ItoaBuf::new());
}

#[inline]
fn push_u32(out: &mut Vec<u8>, v: u32) {
    U32BUF.with(|b| {
        let mut b = b.borrow_mut();
        out.extend_from_slice(b.format(v).as_bytes());
    });
}
#[inline]
fn push_u64(out: &mut Vec<u8>, v: u64) {
    U64BUF.with(|b| {
        let mut b = b.borrow_mut();
        out.extend_from_slice(b.format(v).as_bytes());
    });
}
#[inline]
fn push_i64(out: &mut Vec<u8>, v: i64) {
    I64BUF.with(|b| {
        let mut b = b.borrow_mut();
        out.extend_from_slice(b.format(v).as_bytes());
    });
}

// ---------- Little-endian readers with EOF handling ----------

fn read_exact_fully<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<()> {
    let mut read = 0;
    while read < buf.len() {
        let n = r.read(&mut buf[read..])?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated input"));
        }
        read += n;
    }
    Ok(())
}

fn _read_u8_exact<R: Read>(r: &mut R) -> io::Result<u8> {
    let mut b = [0u8; 1];
    read_exact_fully(r, &mut b)?;
    Ok(b[0])
}

fn _read_u16_le_exact<R: Read>(r: &mut R) -> io::Result<u16> {
    let mut b = [0u8; 2];
    read_exact_fully(r, &mut b)?;
    Ok(u16::from_le_bytes(b))
}

fn read_u32_le_exact<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    read_exact_fully(r, &mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64_le_exact<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    read_exact_fully(r, &mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_i64_le_exact<R: Read>(r: &mut R) -> io::Result<i64> {
    let mut b = [0u8; 8];
    read_exact_fully(r, &mut b)?;
    Ok(i64::from_le_bytes(b))
}

// Read u32 that can be EOF at record boundary.
// Returns Ok(None) if we are exactly at EOF before reading any byte.
// Returns Err if we hit EOF mid-number (truncated).
fn read_u32_le_opt<R: Read>(r: &mut R) -> io::Result<Option<u32>> {
    let mut b = [0u8; 4];
    let mut off = 0usize;
    loop {
        let n = r.read(&mut b[off..])?;
        if n == 0 {
            if off == 0 {
                return Ok(None); // clean EOF
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated record (path_len)",
                ));
            }
        }
        off += n;
        if off == 4 {
            return Ok(Some(u32::from_le_bytes(b)));
        }
    }
}
