// duzip.rs
use anyhow::Result;
use clap::{ColorChoice, Parser};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, Write};
use std::path::PathBuf;
use dutopia::util::{print_about, push_i64, push_u32, push_u64};

const READ_BUF_SIZE: usize = 2 * 1024 * 1024; // 2 MiB
const WRITE_BUF_SIZE: usize = 8 * 1024 * 1024; // 8 MiB


#[derive(Parser, Debug)]
#[command(version, color = ColorChoice::Auto,
    about="Convert between CSV and compressed binary (.zst) formats")]
struct Args {
    /// Input file (.zst or .csv)
    input: PathBuf,

    /// Output file path (default: auto-determined based on operation)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq)]
struct BinaryRecord {
    path: Vec<u8>,
    dev: u64,
    ino: u64,
    atime: i64,
    mtime: i64,
    uid: u32,
    gid: u32,
    mode: u32,
    size: u64,
    disk: u64,
}

fn main() -> Result<()> {
    print_about();

    let args = Args::parse();

    let ext = args
        .input
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    match ext.as_str() {
        "csv" => csv_to_zst(&args),
        "zst" => zst_to_csv(&args),
        other => anyhow::bail!(
            "Unsupported input extension: '{}' (expected .csv, .bin, or .zst)",
             other            
        ),
    }
}

fn csv_to_zst(args: &Args) -> Result<()> {
    let start = std::time::Instant::now();
    let input_file = File::open(&args.input)?;
    let mut reader = BufReader::with_capacity(READ_BUF_SIZE, input_file);

    // Determine output path
    let out_path = args
        .output
        .clone()
        .unwrap_or_else(|| args.input.with_extension("zst"));

    if out_path.exists() {
        anyhow::bail!("Output file already exists: {}", out_path.display());
    }

    // Create ZST encoder
    let out_file = File::create(&out_path)?;
    let encoder = zstd::stream::write::Encoder::new(out_file, 1)?;
    let mut writer = BufWriter::with_capacity(WRITE_BUF_SIZE, encoder);

    // Read and validate CSV header
    let mut header_line = String::new();
    reader.read_line(&mut header_line)?;
    let header = header_line.trim();

    if header != "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH" {
        anyhow::bail!(
                "Invalid CSV header. Expected: INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH, Got: {}",
                header
        );
    }

    println!("Creating .zst file...");

    let mut line = String::new();

    // Process each CSV line
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break; // EOF
        }

        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue; // Skip empty lines
        }

        // Parse CSV line
        let record = parse_csv_record(trimmed)?;

        // Write binary record
        write_binary_record(&mut writer, &record)?;
    }

    // Finish compression
    let encoder = writer.into_inner().map_err(|_| {
        anyhow::anyhow!("failed to flush buffered zstd encoder")
    })?;
    encoder.finish()?;

    println!("Output       : {}", out_path.display());
    println!("Elapsed time : {:.3} sec.", start.elapsed().as_secs_f64());

    Ok(())
}

fn zst_to_csv(args: &Args) -> Result<()> {
    let start = std::time::Instant::now();
    let f = File::open(&args.input)?;
    let mut f = f; // mutable for Read + Seek

    // Peek first 4 bytes
    let mut magic_buf = [0u8; 4];
    f.read_exact(&mut magic_buf)?;
    f.rewind()?; // reset to start
    let magic = u32::from_le_bytes(magic_buf);

    // Detect format
    if magic != 0xFD2FB528 {
        eprintln!("Invalid format.");
        std::process::exit(1);
    } 

    let reader: Box<dyn Read> = Box::new(zstd::stream::read::Decoder::new(f)?);
    let mut r = BufReader::with_capacity(READ_BUF_SIZE, reader);

    // Decide output path
    let out_path = args
        .output
        .clone()
        .unwrap_or_else(|| args.input.with_extension("csv"));

    if out_path.exists() {
        anyhow::bail!(format!("Output file already exists: {}", out_path.display()));
    }

    let out_file = File::create(&out_path)?;
    let mut w = BufWriter::with_capacity(WRITE_BUF_SIZE, out_file);

    println!("Creating .csv file...");

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
    println!("Output       : {}", out_path.display());
    println!("Elapsed time : {:.3} sec.", start.elapsed().as_secs_f64());

    Ok(())
}

fn parse_csv_record(line: &str) -> Result<BinaryRecord> {
    let fields = parse_csv_line(line);

    if fields.len() != 9 {
        anyhow::bail!(format!("CSV record must have 9 fields, got {}: {}", fields.len(), line));
    }

    // Parse INODE field (dev-ino)
    let inode_parts: Vec<&str> = fields[0].split('-').collect();
    if inode_parts.len() != 2 {
       anyhow::bail!(format!("Invalid INODE format, expected dev-ino: {}", fields[0]));
    }

    let dev = inode_parts[0]
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("Invalid dev: {}", e))?;

    let ino = inode_parts[1]
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("Invalid ino: {}", e))?;

    let atime = fields[1]
        .parse::<i64>()
        .map_err(|e| anyhow::anyhow!("Invalid atime: {}", e))?;

    let mtime = fields[2]
        .parse::<i64>()
        .map_err(|e| anyhow::anyhow!("Invalid mtime: {}", e))?;

    let uid = fields[3]
        .parse::<u32>()
        .map_err(|e| anyhow::anyhow!("Invalid uid: {}", e))?;

    let gid = fields[4]
        .parse::<u32>()
        .map_err(|e| anyhow::anyhow!("Invalid gid: {}", e))?;

    let mode = fields[5]
        .parse::<u32>()
        .map_err(|e| anyhow::anyhow!("Invalid mode: {}", e))?;

    let size = fields[6]
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("Invalid size: {}", e))?;

    let disk = fields[7]
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("Invalid disk: {}", e))?;

    let path = fields[8].as_bytes().to_vec();

    Ok(BinaryRecord {
        path,
        dev,
        ino,
        atime,
        mtime,
        uid,
        gid,
        mode,
        size,
        disk,
    })
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' if !in_quotes => {
                in_quotes = true;
            }
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    // Escaped quote
                    chars.next(); // consume the second quote
                    current_field.push('"');
                } else {
                    in_quotes = false;
                }
            }
            ',' if !in_quotes => {
                fields.push(current_field);
                current_field = String::new();
            }
            _ => {
                current_field.push(ch);
            }
        }
    }

    fields.push(current_field);
    fields
}

fn write_binary_record<W: Write>(writer: &mut W, record: &BinaryRecord) -> Result<()> {
    // Write path_len
    let path_len = record.path.len() as u32;
    writer.write_all(&path_len.to_le_bytes())?;

    // Write path bytes
    writer.write_all(&record.path)?;

    // Write fixed fields
    writer.write_all(&record.dev.to_le_bytes())?;
    writer.write_all(&record.ino.to_le_bytes())?;
    writer.write_all(&record.atime.to_le_bytes())?;
    writer.write_all(&record.mtime.to_le_bytes())?;
    writer.write_all(&record.uid.to_le_bytes())?;
    writer.write_all(&record.gid.to_le_bytes())?;
    writer.write_all(&record.mode.to_le_bytes())?;
    writer.write_all(&record.size.to_le_bytes())?;
    writer.write_all(&record.disk.to_le_bytes())?;

    Ok(())
}

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

fn read_exact_fully<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<()> {
    let mut read = 0;
    while read < buf.len() {
        let n = r.read(&mut buf[read..])?;
        if n == 0 {
            anyhow::bail!("truncated input: expected {} bytes, got {}", buf.len(), read);
        }
        read += n;
    }
    Ok(())
}

fn read_u32_le_exact<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    read_exact_fully(r, &mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64_le_exact<R: Read>(r: &mut R) -> Result<u64> {
    let mut b = [0u8; 8];
    read_exact_fully(r, &mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_i64_le_exact<R: Read>(r: &mut R) -> Result<i64> {
    let mut b = [0u8; 8];
    read_exact_fully(r, &mut b)?;
    Ok(i64::from_le_bytes(b))
}

// Read u32 that can be EOF at record boundary.
// Returns Ok(None) if we are exactly at EOF before reading any byte.
// Returns Err if we hit EOF mid-number (truncated).
fn read_u32_le_opt<R: Read>(r: &mut R) -> Result<Option<u32>> {
    let mut b = [0u8; 4];
    let mut off = 0usize;
    loop {
        let n = r.read(&mut b[off..])?;
        if n == 0 {
            if off == 0 {
                return Ok(None); // clean EOF
            } else {
                anyhow::bail!("truncated record (path_len)");
            }
        }
        off += n;
        if off == 4 {
            return Ok(Some(u32::from_le_bytes(b)));
        }
    }
}

#[cfg(test)]
fn read_binary_record<R: Read>(r: &mut R) -> Result<Option<BinaryRecord>> {
    // Read path_len with proper EOF semantics:
    // - Ok(None) if we are exactly at EOF before any byte (clean EOF)
    // - Err(UnexpectedEof, "truncated record (path_len)") if partial
    let path_len = match read_u32_le_opt(r)? {
        None => return Ok(None),
        Some(v) => v as usize,
    };

    // Read path bytes
    let mut path = vec![0u8; path_len];
    read_exact_fully(r, &mut path)?; // will produce UnexpectedEof if truncated

    // Fixed-width fields (use your exact helpers to keep semantics consistent)
    let dev   = read_u64_le_exact(r)?;
    let ino   = read_u64_le_exact(r)?;
    let atime = read_i64_le_exact(r)?;
    let mtime = read_i64_le_exact(r)?;
    let uid   = read_u32_le_exact(r)?;
    let gid   = read_u32_le_exact(r)?;
    let mode  = read_u32_le_exact(r)?;
    let size  = read_u64_le_exact(r)?;
    let disk  = read_u64_le_exact(r)?;

    Ok(Some(BinaryRecord {
        path,
        dev,
        ino,
        atime,
        mtime,
        uid,
        gid,
        mode,
        size,
        disk,
    }))
}

#[cfg(test)]
fn format_csv_record(rec: &BinaryRecord) -> String {
    // PATH as lossy UTF-8, with smart CSV quoting (quote if contains [" , \n \r])
    fn needs_quote(s: &str) -> bool {
        s.as_bytes().iter().any(|&b| matches!(b, b',' | b'"' | b'\n' | b'\r'))
    }
    fn quote_csv(s: &str) -> String {
        if !needs_quote(s) {
            return s.to_string();
        }
        let mut out = String::with_capacity(s.len() + 2);
        out.push('"');
        for ch in s.chars() {
            if ch == '"' {
                out.push('"');
                out.push('"');
            } else {
                out.push(ch);
            }
        }
        out.push('"');
        out
    }

    let inode = format!("{}-{}", rec.dev, rec.ino);
    let path_str = String::from_utf8_lossy(&rec.path);
    let path_csv = quote_csv(&path_str);

    // INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
    // Use decimal for all integers to match your existing CSV.
    format!(
        "{inode},{atime},{mtime},{uid},{gid},{mode},{size},{disk},{path}",
        inode = inode,
        atime = rec.atime,
        mtime = rec.mtime,
        uid   = rec.uid,
        gid   = rec.gid,
        mode  = rec.mode,
        size  = rec.size,
        disk  = rec.disk,
        path  = path_csv
    )
}

// ========== TESTS ==========

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Test data
    fn sample_record() -> BinaryRecord {
        BinaryRecord {
            path: b"/home/user/test.txt".to_vec(),
            dev: 2049,
            ino: 12345,
            atime: 1672531200, // 2023-01-01 00:00:00 UTC
            mtime: 1672617600, // 2023-01-02 00:00:00 UTC
            uid: 1000,
            gid: 1000,
            mode: 33188, // regular file, 644 permissions
            size: 1024,
            disk: 42,
        }
    }

    fn sample_record_with_quotes() -> BinaryRecord {
        BinaryRecord {
            path: b"path with \"quotes\".txt".to_vec(),
            dev: 2050,
            ino: 67890,
            atime: -1,
            mtime: 0,
            uid: 0,
            gid: 0,
            mode: 16877, // directory, 755 permissions
            size: 4096,
            disk: 1,
        }
    }

    #[test]
    fn test_parse_csv_line_simple() {
        let line = "a,b,c,d";
        let fields = parse_csv_line(line);
        assert_eq!(fields, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_parse_csv_line_quoted() {
        let line = r#"a,"b,c",d"#;
        let fields = parse_csv_line(line);
        assert_eq!(fields, vec!["a", "b,c", "d"]);
    }

    #[test]
    fn test_parse_csv_line_escaped_quotes() {
        let line = r#"a,"b""c",d"#;
        let fields = parse_csv_line(line);
        assert_eq!(fields, vec!["a", r#"b"c"#, "d"]);
    }

    #[test]
    fn test_parse_csv_line_empty_fields() {
        let line = "a,,c";
        let fields = parse_csv_line(line);
        assert_eq!(fields, vec!["a", "", "c"]);
    }

    #[test]
    fn test_parse_csv_line_trailing_comma() {
        let line = "a,b,c,";
        let fields = parse_csv_line(line);
        assert_eq!(fields, vec!["a", "b", "c", ""]);
    }

    #[test]
    fn test_parse_csv_record_valid() {
        let csv_line = "2049-12345,1672531200,1672617600,1000,1000,33188,1024,42,/home/user/test.txt";
        let record = parse_csv_record(csv_line).unwrap();
        let expected = sample_record();
        assert_eq!(record, expected);
    }

    #[test]
    fn test_parse_csv_record_with_quoted_path() {
        let csv_line =
            r#"2050-67890,-1,0,0,0,16877,4096,1,"path with ""quotes"".txt""#;
        let record = parse_csv_record(csv_line).unwrap();
        let expected = sample_record_with_quotes();
        assert_eq!(record, expected);
    }

    #[test]
    fn test_parse_csv_record_invalid_fields_count() {
        let csv_line = "2049-12345,1672531200,1672617600";
        let result = parse_csv_record(csv_line);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must have 9 fields"));
    }

    #[test]
    fn test_parse_csv_record_invalid_inode_format() {
        // first token cannot be parsed as dev-ino pair
        let csv_line = "invalid-inode,1672531200,1672617600,1000,1000,33188,1024,42,/path";
        let result = parse_csv_record(csv_line);
        // the split yields ["invalid", "inode"]; "invalid" isn't u64
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid dev"));
    }

    #[test]
    fn test_parse_csv_record_missing_dev_ino_separator() {
        // No '-' in the first field, so split yields single part -> explicit format error
        let csv_line =
            "204912345,1672531200,1672617600,1000,1000,33188,1024,42,/path";
        let result = parse_csv_record(csv_line);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid INODE format"));
    }

    #[test]
    fn test_format_csv_record() {
        let record = sample_record();
        let csv_line = format_csv_record(&record);
        assert_eq!(
            csv_line,
            "2049-12345,1672531200,1672617600,1000,1000,33188,1024,42,/home/user/test.txt"
        );
    }

    #[test]
    fn test_format_csv_record_with_quotes() {
        let record = sample_record_with_quotes();
        let csv_line = format_csv_record(&record);
        assert_eq!(
            csv_line,
            r#"2050-67890,-1,0,0,0,16877,4096,1,"path with ""quotes"".txt""#
        );
    }

    #[test]
    fn test_write_and_read_binary_record() {
        let record = sample_record();
        let mut buffer = Vec::new();

        // Write record
        write_binary_record(&mut buffer, &record).unwrap();

        // Read it back
        let mut cursor = Cursor::new(&buffer);
        let read_record = read_binary_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record, read_record);
    }

    #[test]
    fn test_write_and_read_binary_record_with_quotes() {
        let record = sample_record_with_quotes();
        let mut buffer = Vec::new();

        // Write record
        write_binary_record(&mut buffer, &record).unwrap();

        // Read it back
        let mut cursor = Cursor::new(&buffer);
        let read_record = read_binary_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record, read_record);
    }

    #[test]
    fn test_read_binary_record_empty() {
        let buffer = Vec::new();
        let mut cursor = Cursor::new(&buffer);
        let result = read_binary_record(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_binary_record_truncated_path_len() {
        // Only 2 bytes of path_len provided (should be 4) -> UnexpectedEof
        let buffer = vec![0x05, 0x00]; // truncated u32
        let mut cursor = Cursor::new(&buffer);
        let err = read_binary_record(&mut cursor).unwrap_err();
        assert!(format!("{}", err).contains("path_len"));
    }

    #[test]
    fn test_csv_roundtrip_format_then_parse() {
        let rec = sample_record_with_quotes();
        let csv = format_csv_record(&rec);
        let parsed = parse_csv_record(&csv).unwrap();
        assert_eq!(rec, parsed);
    }
}
