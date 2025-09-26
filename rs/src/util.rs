// util.rs - Utility functions for the filesystem scanner
use std::{
    //ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use itoa::Buffer;
use colored::Colorize;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

pub struct Row<'a> {
    pub path: &'a Path,
    pub dev: u64,
    pub ino: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub blocks: u64,
    pub atime: i64,
    pub mtime: i64,
}

// ============================================================================
// Utilities
// ============================================================================


static SPINNER: [&str; 4] = ["/", "-", "\\", "|"];
static FRAME: AtomicUsize = AtomicUsize::new(0);

pub fn spinner() -> &'static str {
    let i = FRAME.fetch_add(1, Ordering::Relaxed) % SPINNER.len();
    SPINNER[i]
}

pub fn print_about() {
    #[cfg(windows)]
    colored::control::set_virtual_terminal(true).unwrap_or(());

    println!("{}","-".repeat(44).bright_cyan());
    println!("{}", format!("Dutopia      : Superfast filesystem analyzer").bright_cyan());
    println!("{}", format!("Version      : {}", env!("CARGO_PKG_VERSION")).bright_cyan());
    println!("{}", format!("Built        : {}", env!("BUILD_DATE")).bright_cyan());
    println!("{}","-".repeat(44).bright_cyan());
}

pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{:.1}s", duration.as_secs_f64())
    } else if secs < 3600 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h {:02}m {:02}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    }
}
pub fn human_count(n: u64) -> String {
    const UNITS: [&str; 5] = ["", "K", "M", "B", "T"];
    let mut val = n as f64;
    let mut unit = 0;

    while val >= 1000.0 && unit < UNITS.len() - 1 {
        val /= 1000.0;
        unit += 1;
    }

    if unit == 0 {
        // No suffix → show as integer
        format!("{}", n)
    } else {
        // One decimal for large numbers
        format!("{:.1}{}", val, UNITS[unit])
    }
}


pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;

    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        // Show integer for plain bytes
        format!("{}{}", size as u64, UNITS[unit])
    } else {
        // Show one decimal for larger units
        format!("{:.1}{}", size, UNITS[unit])
    }
}

pub fn get_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "noname".to_string())
}

// ============================================================================
// Path Utilities
// ============================================================================

#[cfg(windows)]
pub fn strip_verbatim_prefix(p: &Path) -> PathBuf {
    let s = match p.to_str() {
        Some(s) => s,
        None => return p.to_path_buf(),
    };

    if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{}", rest))
    } else if let Some(rest) = s.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        p.to_path_buf()
    }
}

#[cfg(not(windows))]
pub fn strip_verbatim_prefix(p: &Path) -> PathBuf {
    p.to_path_buf()
}

#[inline]
pub fn should_skip(path: &Path, skip: Option<&str>) -> bool {
    if let Some(s) = skip {
        path.as_os_str().to_string_lossy().contains(s)
    } else {
        false
    }
}

// ============================================================================
// CSV Writing Utilities
// ============================================================================

// Pre-allocate formatters to avoid repeated allocation
thread_local! {
    static U32BUF: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
    static U64BUF: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
    static I64BUF: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
}

#[inline] 
pub fn push_comma(buf: &mut Vec<u8>) { 
    buf.push(b','); 
}

#[inline]
pub fn push_u32(out: &mut Vec<u8>, v: u32) {
    U32BUF.with(|b| {
        let mut b = b.borrow_mut();
        out.extend_from_slice(b.format(v).as_bytes());
    });
}

#[inline]
pub fn push_u64(out: &mut Vec<u8>, v: u64) {
    U64BUF.with(|b| {
        let mut b = b.borrow_mut();
        out.extend_from_slice(b.format(v).as_bytes());
    });
}
#[inline]
pub fn push_i64(out: &mut Vec<u8>, v: i64) {
    I64BUF.with(|b| {
        let mut b = b.borrow_mut();
        out.extend_from_slice(b.format(v).as_bytes());
    });
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

// ============================================================================
// Metadata and File Stats
// ============================================================================

pub fn row_from_metadata<'a>(path: &'a Path, md: &fs::Metadata) -> Row<'a> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Row {
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
            path, 
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
            path, 
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

pub fn stat_row<'a>(path: &'a Path) -> Option<Row<'a>> {
    let md = fs::symlink_metadata(path).ok()?;
    Some(row_from_metadata(path, &md))
}

// ============================================================================
// Windows Security Functions
// ============================================================================

#[cfg(windows)]
pub fn get_rid(path: &Path) -> std::io::Result<u32> {
    use std::{io, iter, os::windows::ffi::OsStrExt, ptr};
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::{
        GetSidSubAuthority, GetSidSubAuthorityCount, IsValidSid, OWNER_SECURITY_INFORMATION
    };
    use windows_sys::Win32::Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT};

    // Check path length before processing to avoid stack overflow
    let path_len = path.as_os_str().len();
    if path_len > 32767 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path too long"
        ));
    }

    // Pre-allocate with capacity to avoid multiple allocations
    let mut wide = Vec::with_capacity(path_len + 1);
    
    // Try to collect the wide string, handling potential allocation failure
    match std::panic::catch_unwind(|| {
        wide.extend(path.as_os_str().encode_wide().chain(iter::once(0)));
        wide
    }) {
        Ok(wide_path) => wide = wide_path,
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Failed to allocate memory for path conversion"
            ));
        }
    }

    let mut p_owner_sid: *mut core::ffi::c_void = ptr::null_mut();
    let mut p_sd: *mut core::ffi::c_void = ptr::null_mut();

    let err = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut p_owner_sid as *mut _ as *mut _,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut p_sd,
        )
    };
    
    if err != 0 {
        return Err(io::Error::from_raw_os_error(err as i32));
    }

    let rid = unsafe {
        if IsValidSid(p_owner_sid) == 0 {
            LocalFree(p_sd as *mut _);
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid SID"));
        }
        let count = *GetSidSubAuthorityCount(p_owner_sid) as u32;
        if count == 0 {
            LocalFree(p_sd as *mut _);
            return Err(io::Error::new(io::ErrorKind::Other, "SID has no subauthorities"));
        }
        let p_last = GetSidSubAuthority(p_owner_sid, count - 1);
        let val = *p_last as u32;
        LocalFree(p_sd as *mut _);
        val
    };

    Ok(rid)
}

pub fn fs_used_bytes(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use libc::{statvfs, statvfs as statvfs_t};

        // SAFETY: statvfs needs a C string; use an existing path on that FS.
        let p = CString::new(path.as_os_str().as_bytes()).ok()?;
        let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { statvfs(p.as_ptr(), &mut s as *mut statvfs_t) };
        if rc != 0 { return None; }

        // Prefer fragment size if available; fall back to f_bsize.
        let bsize = if s.f_frsize != 0 { s.f_frsize } else { s.f_bsize } as u64;
        // df-style "used" = blocks - free
        let used_blocks = s.f_blocks.saturating_sub(s.f_bfree) as u64;
        return Some(used_blocks.saturating_mul(bsize));
    }

    #[cfg(windows)]
    {
        //use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

        // Any path on the volume works; use the root you’re scanning.
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(std::iter::once(0)).collect();

        let mut free_avail: u64 = 0;
        let mut total: u64 = 0;
        let mut free_total: u64 = 0;

        let ok = unsafe {
            GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut free_avail as *mut u64,
                &mut total as *mut u64,
                &mut free_total as *mut u64,
            )
        };
        if ok == 0 { return None; }

        // used = total - free_total (matches what users expect from df / explorer)
        return Some(total.saturating_sub(free_total));
    }

    #[allow(unreachable_code)]
    None
}

pub fn is_volume_root(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        // `/` is a mount root
        if path.parent().is_none() { return true; }

        // A mount point has a different device than its parent
        // (Beware: needs a valid parent that exists)
        if let (Ok(meta), Some(parent)) = (fs::metadata(path), path.parent()) {
            if let Ok(pmeta) = fs::metadata(parent) {
                return meta.dev() != pmeta.dev();
            }
        }
        false
    }

    #[cfg(windows)]
    {
        // use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::GetVolumePathNameW;

        // Ask Windows for the volume root of this path and compare.
        // If path *is* the volume root (e.g. "C:\" or "\\server\share\"),
        // the returned volume path equals the input’s own root.
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(std::iter::once(0)).collect();
        let mut buf = [0u16; 260]; // MAX_PATH-ish; good enough for the root path query
        let ok = unsafe { GetVolumePathNameW(wide.as_ptr(), buf.as_mut_ptr(), buf.len() as u32) };
        if ok == 0 { return false; }

        let vol = {
            let nul = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
            String::from_utf16_lossy(&buf[..nul])
        };

        // Normalize the input to a string to compare against `vol`
        let p = strip_verbatim_prefix(path);
        let p = p.to_string_lossy();

        // Ensure trailing backslash for comparison when needed
        let mut p_norm = p.to_string();
        if !p_norm.ends_with('\\') && p_norm.chars().nth(1) == Some(':') && p_norm.len() == 2 {
            p_norm.push('\\');
        }
        // UNC roots often have the trailing slash
        if !p_norm.ends_with('\\') && vol.ends_with('\\') {
            p_norm.push('\\');
        }

        // If this path resolves to that exact volume root, it’s a drive root.
        p_norm.eq_ignore_ascii_case(&vol)
    }
}

/// Print a colorized progress bar like: [====>-----] 42%
/// - Filled: cyan
/// - Head: bright cyan
/// - Empty: gray (bright black)
pub fn progress_bar(pct: f64, width: usize) -> String {
    let pct = pct.clamp(0.0, 100.0);
    let filled = ((pct / 100.0) * width as f64).round() as usize;
    let body_len = filled.saturating_sub(1); // '=' repeated
    let has_head = (filled > 0) as usize;    // '>' if any filled
    let tail_len = width.saturating_sub(body_len + has_head); // '-' repeated
    let mut bar = String::with_capacity(width + 8);
    bar.push('[');

    if body_len > 0 {
        bar.push_str(&"=".repeat(body_len).bright_cyan().to_string());
    }
    if has_head == 1 {
        bar.push_str(&">".bright_cyan().to_string());
    }
    if tail_len > 0 {
        bar.push_str(&"-".repeat(tail_len).bright_black().to_string());
    }
    bar.push(']');

    bar
}

#[inline]
pub fn trim_ascii(mut s: &[u8]) -> &[u8] {
    while !s.is_empty() && s[0].is_ascii_whitespace() { s = &s[1..]; }
    while !s.is_empty() && s[s.len() - 1].is_ascii_whitespace() { s = &s[..s.len() - 1]; }
    s
}

#[inline]
pub fn parse_int<T>(b: Option<&[u8]>) -> T 
where
    T: atoi::FromRadix10SignedChecked + Default,
{
    let s = trim_ascii(b.unwrap_or(b"0"));
    atoi::atoi::<T>(s).unwrap_or_default()
}


pub fn parse_file_hint(s: &str) -> Option<u64> {
    // Accept forms like: 10k, 2m, 1.5g, or plain 12345
    let s = s.trim().to_ascii_lowercase();
    let mut num = String::new();
    let mut unit = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            num.push(ch);
        } else if !ch.is_whitespace() {
            unit.push(ch);
        }
    }

    let val: f64 = num.parse().ok()?;
    let mul: f64 = match unit.as_str() {
        ""        => 1.0,
        "k"       => 1_000.0,
        "m"       => 1_000_000.0,
        "g"       => 1_000_000_000.0,
        _ => return None,
    };

    Some((val * mul) as u64)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_should_skip() {
        let p = PathBuf::from("/a/b/c/d");
        assert!(should_skip(&p, Some("b/c")));
        assert!(!should_skip(&p, Some("x")));
        assert!(!should_skip(&p, None));
    }

    #[cfg(unix)]
    #[test]
    fn test_csv_push_bytes_smart_quoted() {
        let mut buf = Vec::new();
        csv_push_bytes_smart_quoted(&mut buf, b"abc_def");
        assert_eq!(&buf, b"abc_def");
        
        buf.clear();
        csv_push_bytes_smart_quoted(&mut buf, b"a,b");
        assert_eq!(&buf, b"\"a,b\"");
        
        buf.clear();
        csv_push_bytes_smart_quoted(&mut buf, b"a\"b");
        assert_eq!(&buf, b"\"a\"\"b\"");
    }
}
