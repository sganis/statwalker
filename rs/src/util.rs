// util.rs - Utility functions for the filesystem scanner
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use itoa::Buffer;
use colored::Colorize;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

pub struct Row {
    //pub path: &'a Path,
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
        if let (Ok(meta), Some(parent)) = (std::fs::metadata(path), path.parent()) {
            if let Ok(pmeta) = std::fs::metadata(parent) {
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
    use std::time::Duration;
    use std::sync::atomic::Ordering;

    // ============================================================================
    // Row struct tests
    // ============================================================================

    #[test]
    fn test_row_creation() {
        let row = Row {
            dev: 123,
            ino: 456,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            size: 1024,
            blocks: 2,
            atime: 1640995200,
            mtime: 1640995200,
        };
        
        assert_eq!(row.dev, 123);
        assert_eq!(row.ino, 456);
        assert_eq!(row.mode, 0o644);
        assert_eq!(row.uid, 1000);
        assert_eq!(row.gid, 1000);
        assert_eq!(row.size, 1024);
        assert_eq!(row.blocks, 2);
        assert_eq!(row.atime, 1640995200);
        assert_eq!(row.mtime, 1640995200);
    }

    // ============================================================================
    // Spinner tests
    // ============================================================================

    #[test]
    fn test_spinner() {
        // Reset frame counter for predictable testing
        FRAME.store(0, Ordering::Relaxed);
        
        assert_eq!(spinner(), "/");
        assert_eq!(spinner(), "-");
        assert_eq!(spinner(), "\\");
        assert_eq!(spinner(), "|");
        assert_eq!(spinner(), "/"); // Should cycle back
    }

    #[test]
    fn test_spinner_thread_safety() {
        use std::thread;
        
        // Test that spinner works correctly with multiple threads
        let handles: Vec<_> = (0..10)
            .map(|_| {
                thread::spawn(|| {
                    for _ in 0..100 {
                        let s = spinner();
                        assert!(SPINNER.contains(&s));
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
    }

    // ============================================================================
    // Duration formatting tests
    // ============================================================================

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(Duration::from_secs(0)), "0.0s");
        assert_eq!(format_duration(Duration::from_secs(1)), "1.0s");
        assert_eq!(format_duration(Duration::from_secs(30)), "30.0s");
        assert_eq!(format_duration(Duration::from_secs(59)), "59.0s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(Duration::from_secs(60)), "1m 00s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_duration(Duration::from_secs(3599)), "59m 59s");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(Duration::from_secs(3600)), "1h 00m 00s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 01m 01s");
        assert_eq!(format_duration(Duration::from_secs(7200)), "2h 00m 00s");
    }

    #[test]
    fn test_format_duration_fractional() {
        assert_eq!(format_duration(Duration::from_millis(1500)), "1.5s");
        assert_eq!(format_duration(Duration::from_millis(500)), "0.5s");
        assert_eq!(format_duration(Duration::from_micros(100000)), "0.1s");
    }

    // ============================================================================
    // Human count formatting tests
    // ============================================================================

    #[test]
    fn test_human_count_basic() {
        assert_eq!(human_count(0), "0");
        assert_eq!(human_count(1), "1");
        assert_eq!(human_count(999), "999");
    }

    #[test]
    fn test_human_count_thousands() {
        assert_eq!(human_count(1000), "1.0K");
        assert_eq!(human_count(1500), "1.5K");
        assert_eq!(human_count(999999), "1000.0K"); // 999999 / 1000 = 999.999, rounds to 1000.0
    }

    #[test]
    fn test_human_count_millions() {
        assert_eq!(human_count(1000000), "1.0M");
        assert_eq!(human_count(1500000), "1.5M");
        assert_eq!(human_count(999999999), "1000.0M"); // 999999999 / 1000000 = 999.999999, rounds to 1000.0
    }

    #[test]
    fn test_human_count_billions() {
        assert_eq!(human_count(1000000000), "1.0B");
        assert_eq!(human_count(1500000000), "1.5B");
    }

    #[test]
    fn test_human_count_trillions() {
        assert_eq!(human_count(1000000000000), "1.0T");
        assert_eq!(human_count(u64::MAX), format!("{:.1}T", u64::MAX as f64 / 1e12));
    }

    // ============================================================================
    // Human bytes formatting tests
    // ============================================================================

    #[test]
    fn test_human_bytes_basic() {
        assert_eq!(human_bytes(0), "0B");
        assert_eq!(human_bytes(1), "1B");
        assert_eq!(human_bytes(1023), "1023B");
    }

    #[test]
    fn test_human_bytes_kilobytes() {
        assert_eq!(human_bytes(1024), "1.0KB");
        assert_eq!(human_bytes(1536), "1.5KB");
        assert_eq!(human_bytes(1048575), "1024.0KB"); // 1048575 / 1024 = 1023.999..., rounds to 1024.0
    }

    #[test]
    fn test_human_bytes_megabytes() {
        assert_eq!(human_bytes(1048576), "1.0MB");
        assert_eq!(human_bytes(1572864), "1.5MB");
    }

    #[test]
    fn test_human_bytes_gigabytes() {
        assert_eq!(human_bytes(1073741824), "1.0GB");
        assert_eq!(human_bytes(1610612736), "1.5GB");
    }

    #[test]
    fn test_human_bytes_terabytes() {
        assert_eq!(human_bytes(1099511627776), "1.0TB");
        assert_eq!(human_bytes(u64::MAX), format!("{:.1}TB", u64::MAX as f64 / (1024.0_f64.powi(4))));
    }

    // ============================================================================
    // Hostname tests
    // ============================================================================

    #[test]
    fn test_get_hostname() {
        let hostname = get_hostname();
        // Should return either a real hostname or "noname"
        assert!(!hostname.is_empty());
        assert!(hostname.len() <= 255); // Reasonable hostname length
    }

    // ============================================================================
    // Path utilities tests
    // ============================================================================

    #[test]
    fn test_should_skip() {
        let p = PathBuf::from("/a/b/c/d");
        assert!(should_skip(&p, Some("b/c")));
        assert!(!should_skip(&p, Some("x")));
        assert!(!should_skip(&p, None));
        
        let p2 = PathBuf::from("C:\\Users\\test");
        assert!(should_skip(&p2, Some("Users")));
        assert!(!should_skip(&p2, Some("Documents")));
    }

    #[test]
    fn test_should_skip_edge_cases() {
        let p = PathBuf::from("");
        assert!(!should_skip(&p, Some("test")));
        assert!(!should_skip(&p, None));
        
        let p2 = PathBuf::from("test");
        assert!(should_skip(&p2, Some(""))); // Empty skip string matches everything
        assert!(should_skip(&p2, Some("test")));
        assert!(should_skip(&p2, Some("te")));
    }

    #[cfg(not(windows))]
    #[test]
    fn test_strip_verbatim_prefix_unix() {
        let p = PathBuf::from("/some/path");
        assert_eq!(strip_verbatim_prefix(&p), p);
        
        let p2 = PathBuf::from("relative/path");
        assert_eq!(strip_verbatim_prefix(&p2), p2);
    }

    #[cfg(windows)]
    #[test]
    fn test_strip_verbatim_prefix_windows() {
        let p = PathBuf::from(r"\\?\C:\test");
        assert_eq!(strip_verbatim_prefix(&p), PathBuf::from(r"C:\test"));
        
        let p2 = PathBuf::from(r"\\?\UNC\server\share");
        assert_eq!(strip_verbatim_prefix(&p2), PathBuf::from(r"\\server\share"));
        
        let p3 = PathBuf::from(r"C:\normal\path");
        assert_eq!(strip_verbatim_prefix(&p3), p3);
    }

    #[cfg(windows)]
    #[test]
    fn test_strip_verbatim_prefix_invalid_unicode() {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStringExt;
        
        // Create an invalid UTF-8 path
        let invalid_utf16 = vec![0xD800, 0x41]; // Invalid surrogate pair
        let os_string = OsString::from_wide(&invalid_utf16);
        let p = PathBuf::from(os_string);
        
        // Should return the original path when conversion fails
        assert_eq!(strip_verbatim_prefix(&p), p);
    }

    // ============================================================================
    // CSV writing utilities tests
    // ============================================================================

    #[test]
    fn test_push_u32() {
        let mut out = Vec::new();
        push_u32(&mut out, 0);
        assert_eq!(out, b"0");
        
        out.clear();
        push_u32(&mut out, 42);
        assert_eq!(out, b"42");
        
        out.clear();
        push_u32(&mut out, u32::MAX);
        assert_eq!(out, b"4294967295");
    }

    #[test]
    fn test_push_u64() {
        let mut out = Vec::new();
        push_u64(&mut out, 0);
        assert_eq!(out, b"0");
        
        out.clear();
        push_u64(&mut out, 42);
        assert_eq!(out, b"42");
        
        out.clear();
        push_u64(&mut out, u64::MAX);
        assert_eq!(out, b"18446744073709551615");
    }

    #[test]
    fn test_push_i64() {
        let mut out = Vec::new();
        push_i64(&mut out, 0);
        assert_eq!(out, b"0");
        
        out.clear();
        push_i64(&mut out, 42);
        assert_eq!(out, b"42");
        
        out.clear();
        push_i64(&mut out, -42);
        assert_eq!(out, b"-42");
        
        out.clear();
        push_i64(&mut out, i64::MAX);
        assert_eq!(out, b"9223372036854775807");
        
        out.clear();
        push_i64(&mut out, i64::MIN);
        assert_eq!(out, b"-9223372036854775808");
    }

    #[test]
    fn test_csv_formatters_multiple_calls() {
        let mut out = Vec::new();
        push_u32(&mut out, 1);
        out.push(b',');
        push_u64(&mut out, 2);
        out.push(b',');
        push_i64(&mut out, -3);
        assert_eq!(out, b"1,2,-3");
    }

    // ============================================================================
    // Progress bar tests
    // ============================================================================

    #[test]
    fn test_progress_bar_basic() {
        // Note: These tests check structure without ANSI colors
        let bar = progress_bar(0.0, 10);
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
        // The function returns colored output, so we can't check exact content
        // Just verify it has reasonable structure
        assert!(bar.len() > 12); // Should have brackets plus content plus ANSI codes
        
        let bar = progress_bar(100.0, 10);
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
        assert!(bar.len() > 12); // Should have brackets plus content plus ANSI codes
    }

    #[test]
    fn test_progress_bar_percentages() {
        let bar = progress_bar(50.0, 10);
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
        // Should have roughly half filled
        
        let bar = progress_bar(25.0, 4);
        assert!(bar.len() > 6); // [XXX] plus ANSI codes
    }

    #[test]
    fn test_progress_bar_edge_cases() {
        let bar = progress_bar(-10.0, 5); // Negative percentage
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
        
        let bar = progress_bar(150.0, 5); // Over 100%
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
        
        let bar = progress_bar(50.0, 0); // Zero width
        assert_eq!(bar, "[]");
        
        let bar = progress_bar(50.0, 1); // Width of 1
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
    }

    // ============================================================================
    // ASCII trimming tests
    // ============================================================================

    #[test]
    fn test_trim_ascii() {
        assert_eq!(trim_ascii(b"hello"), b"hello");
        assert_eq!(trim_ascii(b"  hello  "), b"hello");
        assert_eq!(trim_ascii(b"\t\ntest\r\n"), b"test");
        assert_eq!(trim_ascii(b""), b"");
        assert_eq!(trim_ascii(b"   "), b"");
        assert_eq!(trim_ascii(b"\x00test\x00"), b"\x00test\x00"); // Only ASCII whitespace
    }

    #[test]
    fn test_trim_ascii_edge_cases() {
        assert_eq!(trim_ascii(b" "), b"");
        assert_eq!(trim_ascii(b"\t"), b"");
        assert_eq!(trim_ascii(b"\n"), b"");
        assert_eq!(trim_ascii(b"\r"), b"");
        assert_eq!(trim_ascii(b"a"), b"a");
        assert_eq!(trim_ascii(b" a "), b"a");
    }

    // ============================================================================
    // Integer parsing tests
    // ============================================================================

    #[test]
    fn test_parse_int_u32() {
        assert_eq!(parse_int::<u32>(Some(b"42")), 42u32);
        assert_eq!(parse_int::<u32>(Some(b"  42  ")), 42u32);
        assert_eq!(parse_int::<u32>(Some(b"0")), 0u32);
        assert_eq!(parse_int::<u32>(None), 0u32);
        assert_eq!(parse_int::<u32>(Some(b"")), 0u32);
        assert_eq!(parse_int::<u32>(Some(b"invalid")), 0u32);
    }

    #[test]
    fn test_parse_int_i32() {
        assert_eq!(parse_int::<i32>(Some(b"42")), 42i32);
        assert_eq!(parse_int::<i32>(Some(b"-42")), -42i32);
        assert_eq!(parse_int::<i32>(Some(b"  -42  ")), -42i32);
        assert_eq!(parse_int::<i32>(Some(b"0")), 0i32);
        assert_eq!(parse_int::<i32>(None), 0i32);
    }

    #[test]
    fn test_parse_int_u64() {
        assert_eq!(parse_int::<u64>(Some(b"1844674407370955161")), 1844674407370955161u64);
        assert_eq!(parse_int::<u64>(Some(b"0")), 0u64);
    }

    #[test]
    fn test_parse_int_overflow() {
        // Test overflow behavior - should return default (0) for invalid values
        assert_eq!(parse_int::<u8>(Some(b"256")), 0u8); // Overflow for u8
        assert_eq!(parse_int::<i8>(Some(b"128")), 0i8); // Overflow for i8
    }

    // ============================================================================
    // File hint parsing tests
    // ============================================================================

    #[test]
    fn test_parse_file_hint_basic() {
        assert_eq!(parse_file_hint("100"), Some(100));
        assert_eq!(parse_file_hint("0"), Some(0));
        assert_eq!(parse_file_hint("1234567"), Some(1234567));
    }

    #[test]
    fn test_parse_file_hint_units() {
        assert_eq!(parse_file_hint("10k"), Some(10_000));
        assert_eq!(parse_file_hint("10K"), Some(10_000));
        assert_eq!(parse_file_hint("2m"), Some(2_000_000));
        assert_eq!(parse_file_hint("2M"), Some(2_000_000));
        assert_eq!(parse_file_hint("1g"), Some(1_000_000_000));
        assert_eq!(parse_file_hint("1G"), Some(1_000_000_000));
    }

    #[test]
    fn test_parse_file_hint_decimals() {
        assert_eq!(parse_file_hint("1.5k"), Some(1_500));
        assert_eq!(parse_file_hint("2.5m"), Some(2_500_000));
        assert_eq!(parse_file_hint("0.5g"), Some(500_000_000));
    }

    #[test]
    fn test_parse_file_hint_whitespace() {
        assert_eq!(parse_file_hint("  10k  "), Some(10_000));
        assert_eq!(parse_file_hint("10 k"), Some(10_000));
        assert_eq!(parse_file_hint("\t2m\n"), Some(2_000_000));
    }

    #[test]
    fn test_parse_file_hint_invalid() {
        assert_eq!(parse_file_hint("invalid"), None);
        assert_eq!(parse_file_hint("10x"), None);
        assert_eq!(parse_file_hint(""), None);
        assert_eq!(parse_file_hint("k"), None);
        assert_eq!(parse_file_hint("10t"), None); // t not supported
    }

    // ============================================================================
    // Filesystem utilities tests (Unix)
    // ============================================================================

    #[cfg(unix)]
    #[test]
    fn test_fs_used_bytes_unix() {
        use std::path::Path;
        
        // Test with root filesystem (should always exist)
        let result = fs_used_bytes(Path::new("/"));
        assert!(result.is_some());
        
        // Test with non-existent path
        let result = fs_used_bytes(Path::new("/non/existent/path"));
        assert!(result.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn test_is_volume_root_unix() {
        use std::path::Path;
        
        // Root should be a volume root
        assert!(is_volume_root(Path::new("/")));
        
        // Most other paths shouldn't be (unless you have unusual mount points)
        // This is environment-dependent, so we'll just test the logic works
        let result = is_volume_root(Path::new("/usr"));
        // Don't assert the result as it depends on system configuration
        let _ = result;
        
        // Test with non-existent paths
        let result = is_volume_root(Path::new("/non/existent/path"));
        // Should be false for non-existent paths
        assert!(!result);
    }

    // ============================================================================
    // Filesystem utilities tests (Windows)
    // ============================================================================

    #[cfg(windows)]
    #[test]
    fn test_fs_used_bytes_windows() {
        use std::path::Path;
        
        // Test with C:\ (should usually exist)
        let result = fs_used_bytes(Path::new("C:\\"));
        // Don't assert result as it might not exist in all test environments
        let _ = result;
        
        // Test current directory
        let result = fs_used_bytes(Path::new("."));
        assert!(result.is_some());
    }

    #[cfg(windows)]
    #[test]
    fn test_is_volume_root_windows() {
        use std::path::Path;
        
        // These tests are environment-dependent
        let result = is_volume_root(Path::new("C:\\"));
        let _ = result; // Don't assert as C:\ might not exist in test env
        
        let result = is_volume_root(Path::new("C:\\Windows"));
        let _ = result; // Should typically be false
    }

    #[cfg(windows)]
    #[test]
    fn test_get_rid_windows() {
        use std::path::Path;
        
        // Test with current directory
        let result = get_rid(Path::new("."));
        match result {
            Ok(rid) => {
                // RID should be a reasonable value
                assert!(rid > 0);
            }
            Err(_) => {
                // Might fail in some test environments, that's ok
            }
        }
        
        // Test with non-existent path
        let result = get_rid(Path::new("C:\\non\\existent\\path"));
        assert!(result.is_err());
    }

    #[cfg(windows)]
    #[test]
    fn test_get_rid_path_too_long() {
        use std::path::Path;
        
        // Create a very long path
        let long_path = "C:\\".to_string() + &"a\\".repeat(10000);
        let result = get_rid(Path::new(&long_path));
        assert!(result.is_err());
    }

    // ============================================================================
    // Print about test (can't really test output, but ensure it doesn't panic)
    // ============================================================================

    #[test]
    fn test_print_about() {
        // This function prints to stdout, we just ensure it doesn't panic
        print_about();
    }

    // ============================================================================
    // Integration tests
    // ============================================================================

    #[test]
    fn test_csv_integration() {
        let mut out = Vec::new();
        
        // Simulate writing a CSV row
        push_u64(&mut out, 123);
        out.push(b',');
        push_u32(&mut out, 456);
        out.push(b',');
        push_i64(&mut out, -789);
        out.push(b'\n');
        
        let csv_line = String::from_utf8(out).unwrap();
        assert_eq!(csv_line, "123,456,-789\n");
    }

    #[test]
    fn test_path_processing_integration() {
        let test_paths = [
            "/usr/local/bin",
            "/home/user/documents", 
            "/var/log/system.log",
        ];
        
        for path_str in &test_paths {
            let path = Path::new(path_str);
            let stripped = strip_verbatim_prefix(&path);
            
            // Should not skip normal paths
            assert!(!should_skip(&stripped, Some("nonexistent")));
            
            // Should skip if pattern matches
            if path_str.contains("user") {
                assert!(should_skip(&stripped, Some("user")));
            }
        }
    }

    #[test]
    fn test_formatting_consistency() {
        // Test that our formatting functions are consistent
        let test_values = [0, 1, 1000, 1024, 1000000, 1048576];
        
        for &val in &test_values {
            let count_str = human_count(val);
            let bytes_str = human_bytes(val);
            
            // Both should be non-empty
            assert!(!count_str.is_empty());
            assert!(!bytes_str.is_empty());
            
            // Bytes should have a unit suffix
            if val > 0 {
                assert!(bytes_str.ends_with('B') || bytes_str.chars().last().unwrap().is_ascii_alphabetic());
            }
        }
    }
}