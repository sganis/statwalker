// storage.rs
use std::fmt;

#[derive(Debug, Clone)]
pub struct StorageInfo {
    pub device: String,
    pub filesystem: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub mount_points: Vec<String>,
}

impl StorageInfo {
    pub fn usage_percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.used_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }
}

impl fmt::Display for StorageInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let binding = "Unknown".to_string();
        let main_mount = self.mount_points.first().unwrap_or(&binding);
        let mount_info = if self.mount_points.len() > 1 {
            format!("{} (+{} others)", main_mount, self.mount_points.len() - 1)
        } else {
            main_mount.clone()
        };
        
        write!(
            f,
            "Device: {} | Mount: {} | Total: {:.2} GB | Used: {:.2} GB | Usage: {:.1}%",
            self.device,
            mount_info,
            //self.filesystem,
            self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            self.used_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            //self.available_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            self.usage_percentage()
        )
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::StorageInfo;
    use std::fs::File;
    use std::io::{BufRead, BufReader, Result};
    use std::collections::HashMap;

    unsafe extern "C" {
        fn statvfs(path: *const libc::c_char, buf: *mut libc::statvfs) -> libc::c_int;
    }

    pub fn get_storage_info() -> Result<Vec<StorageInfo>> {
        let mut device_map: HashMap<String, StorageInfo> = HashMap::new();
        
        // Read /proc/mounts to get mounted filesystems
        let file = File::open("/proc/mounts")?;
        let reader = BufReader::new(file);
        
        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            
            if parts.len() < 3 {
                continue;
            }
            
            let device = parts[0];
            let mount_point = parts[1];
            let filesystem = parts[2];
            
            // Skip virtual filesystems
            if should_skip_filesystem(filesystem, device) {
                continue;
            }
            
            // Get the base device name (remove partition numbers and snap paths)
            let base_device = get_base_device(device);
            
            // Get storage statistics
            if let Some(stats) = get_statvfs_info(mount_point, device, filesystem) {
                if let Some(existing) = device_map.get_mut(&base_device) {
                    // Add mount point to existing device
                    existing.mount_points.push(mount_point.to_string());
                } else {
                    // Create new device entry
                    device_map.insert(base_device.clone(), StorageInfo {
                        device: base_device,
                        filesystem: stats.filesystem,
                        total_bytes: stats.total_bytes,
                        used_bytes: stats.used_bytes,
                        available_bytes: stats.available_bytes,
                        mount_points: vec![mount_point.to_string()],
                    });
                }
            }
        }
        
        Ok(device_map.into_values().collect())
    }
    
    fn get_base_device(device: &str) -> String {
        // Remove partition numbers and snap paths to get base device
        if device.starts_with("/dev/") {
            // Remove partition numbers (e.g., /dev/sda1 -> /dev/sda)
            let base = device.trim_end_matches(|c: char| c.is_ascii_digit());
            // Handle nvme drives (e.g., /dev/nvme0n1p1 -> /dev/nvme0n1)
            if base.contains("nvme") && base.ends_with("n") {
                return format!("{}1", base);
            }
            base.to_string()
        } else if device.starts_with("/dev/mapper/") || device.contains("snap") {
            // Keep full path for mapped devices and snaps
            device.to_string()
        } else {
            device.to_string()
        }
    }
    
    fn should_skip_filesystem(filesystem: &str, device: &str) -> bool {
        let virtual_fs = [
            "proc", "sysfs", "devtmpfs", "devpts", "tmpfs", "securityfs",
            "cgroup", "cgroup2", "pstore", "bpf", "tracefs", "debugfs",
            "mqueue", "hugetlbfs", "systemd-1", "binfmt_misc", "autofs",
            "rpc_pipefs", "nfsd", "sunrpc", "fuse.gvfsd-fuse", "fusectl"
        ];
        
        virtual_fs.contains(&filesystem) || 
        device.starts_with("/sys") ||
        device.starts_with("/proc") ||
        device.starts_with("/dev/loop") ||
        !device.starts_with("/")
    }
    
    fn get_statvfs_info(mount_point: &str, device: &str, filesystem: &str) -> Option<StorageInfo> {
        let mount_cstring = std::ffi::CString::new(mount_point).ok()?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        
        let result = unsafe { statvfs(mount_cstring.as_ptr(), &mut stat) };
        
        if result == 0 {
            let block_size = stat.f_frsize as u64;
            let total_bytes = stat.f_blocks * block_size;
            let available_bytes = stat.f_bavail * block_size;
            let free_bytes = stat.f_bfree * block_size;
            let used_bytes = total_bytes - free_bytes;
            
            Some(StorageInfo {
                device: device.to_string(),
                filesystem: filesystem.to_string(),
                total_bytes,
                used_bytes,
                available_bytes,
                mount_points: vec![mount_point.to_string()],
            })
        } else {
            None
        }
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::StorageInfo;
    use std::io::Result;
    use std::collections::HashMap;

    unsafe extern "C" {
        fn getmntinfo(mntbufp: *mut *mut libc::statfs, flags: libc::c_int) -> libc::c_int;
    }

    pub fn get_storage_info() -> Result<Vec<StorageInfo>> {
        let mut device_map: HashMap<String, StorageInfo> = HashMap::new();
        
        let mut mounts_ptr: *mut libc::statfs = std::ptr::null_mut();
        let mount_count = unsafe { getmntinfo(&mut mounts_ptr, libc::MNT_WAIT) };
        
        if mount_count < 0 {
            return Ok(vec![]);
        }
        
        unsafe {
            let mounts = std::slice::from_raw_parts(mounts_ptr, mount_count as usize);
            
            for mount in mounts {
                let device = std::ffi::CStr::from_ptr(mount.f_mntfromname.as_ptr())
                    .to_string_lossy()
                    .to_string();
                let mount_point = std::ffi::CStr::from_ptr(mount.f_mntonname.as_ptr())
                    .to_string_lossy()
                    .to_string();
                let filesystem = std::ffi::CStr::from_ptr(mount.f_fstypename.as_ptr())
                    .to_string_lossy()
                    .to_string();
                
                // Skip virtual filesystems
                if should_skip_filesystem(&filesystem, &device) {
                    continue;
                }
                
                // Get base device (physical disk)
                let base_device = get_base_device(&device);
                
                let block_size = mount.f_bsize as u64;
                let total_bytes = mount.f_blocks * block_size;
                let available_bytes = mount.f_bavail * block_size;
                let free_bytes = mount.f_bfree * block_size;
                let used_bytes = total_bytes - free_bytes;
                
                if let Some(existing) = device_map.get_mut(&base_device) {
                    // Add mount point to existing device
                    existing.mount_points.push(mount_point);
                } else {
                    // Create new device entry
                    device_map.insert(base_device.clone(), StorageInfo {
                        device: base_device,
                        filesystem,
                        total_bytes,
                        used_bytes,
                        available_bytes,
                        mount_points: vec![mount_point],
                    });
                }
            }
        }
        
        Ok(device_map.into_values().collect())
    }
    
    fn get_base_device(device: &str) -> String {
        // For APFS, extract the base disk from device names like /dev/disk3s1s1
        if device.starts_with("/dev/disk") {
            // Extract base disk number (e.g., /dev/disk3s1s1 -> /dev/disk3)
            if let Some(pos) = device.find('s') {
                return device[..pos].to_string();
            }
        }
        device.to_string()
    }
    
    fn should_skip_filesystem(filesystem: &str, device: &str) -> bool {
        let virtual_fs = [
            "devfs", "map", "lofs", "fdesc", "union", "kernfs", "procfs",
            "ptyfs", "tmpfs", "nullfs", "overlay", "autofs"
        ];
        
        virtual_fs.contains(&filesystem) ||
        device.starts_with("map ") ||
        device == "devfs"
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use super::StorageInfo;
    use std::io::Result;
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStrExt;
    
    unsafe extern "system" {
        fn GetLogicalDrives() -> u32;
        fn GetDriveTypeW(lpRootPathName: *const u16) -> u32;
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
        fn GetVolumeInformationW(
            lpRootPathName: *const u16,
            lpVolumeNameBuffer: *mut u16,
            nVolumeNameSize: u32,
            lpVolumeSerialNumber: *mut u32,
            lpMaximumComponentLength: *mut u32,
            lpFileSystemFlags: *mut u32,
            lpFileSystemNameBuffer: *mut u16,
            nFileSystemNameSize: u32,
        ) -> i32;
    }

    const DRIVE_FIXED: u32 = 3;
    const DRIVE_REMOVABLE: u32 = 2;
    const DRIVE_REMOTE: u32 = 4;

    pub fn get_storage_info() -> Result<Vec<StorageInfo>> {
        let mut storages = Vec::new();
        
        let drive_mask = unsafe { GetLogicalDrives() };
        
        for i in 0..26 {
            if (drive_mask & (1 << i)) != 0 {
                let drive_letter = (b'A' + i) as char;
                let drive_path = format!("{}:\\", drive_letter);
                
                if let Some(storage_info) = get_drive_info(&drive_path) {
                    storages.push(storage_info);
                }
            }
        }
        
        Ok(storages)
    }
    
    fn get_drive_info(drive_path: &str) -> Option<StorageInfo> {
        let wide_path: Vec<u16> = OsString::from(drive_path)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        
        let drive_type = unsafe { GetDriveTypeW(wide_path.as_ptr()) };
        
        // Only include fixed drives, removable drives, and network drives
        if ![DRIVE_FIXED, DRIVE_REMOVABLE, DRIVE_REMOTE].contains(&drive_type) {
            return None;
        }
        
        let mut available_bytes: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut free_bytes: u64 = 0;
        
        let result = unsafe {
            GetDiskFreeSpaceExW(
                wide_path.as_ptr(),
                &mut available_bytes,
                &mut total_bytes,
                &mut free_bytes,
            )
        };
        
        if result == 0 {
            return None;
        }
        
        let used_bytes = total_bytes - free_bytes;
        
        // Get filesystem information
        let mut fs_name_buffer: [u16; 256] = [0; 256];
        let mut volume_name_buffer: [u16; 256] = [0; 256];
        let mut volume_serial: u32 = 0;
        let mut max_component_length: u32 = 0;
        let mut fs_flags: u32 = 0;
        
        let fs_result = unsafe {
            GetVolumeInformationW(
                wide_path.as_ptr(),
                volume_name_buffer.as_mut_ptr(),
                volume_name_buffer.len() as u32,
                &mut volume_serial,
                &mut max_component_length,
                &mut fs_flags,
                fs_name_buffer.as_mut_ptr(),
                fs_name_buffer.len() as u32,
            )
        };
        
        let filesystem = if fs_result != 0 {
            String::from_utf16_lossy(&fs_name_buffer)
                .trim_end_matches('\0')
                .to_string()
        } else {
            "Unknown".to_string()
        };
        
        let drive_type_str = match drive_type {
            DRIVE_FIXED => "Fixed Drive",
            DRIVE_REMOVABLE => "Removable Drive",
            DRIVE_REMOTE => "Network Drive",
            _ => "Unknown Drive",
        };
        
        Some(StorageInfo {
            device: format!("{} ({})", drive_path, drive_type_str),
            filesystem,
            total_bytes,
            used_bytes,
            available_bytes,
            mount_points: vec![drive_path.to_string()],
        })
    }
}

pub fn get_all_storage_info() -> std::io::Result<Vec<StorageInfo>> {
    #[cfg(target_os = "linux")]
    {
        linux::get_storage_info()
    }
    
    #[cfg(target_os = "macos")]
    {
        macos::get_storage_info()
    }
    
    #[cfg(target_os = "windows")]
    {
        windows::get_storage_info()
    }
    
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Unsupported operating system",
        ))
    }
}

// Example usage and test function
// fn main() -> std::io::Result<()> {
//     println!("Physical Storage Devices:");
//     println!("{}", "=".repeat(120));
    
//     match get_all_storage_info() {
//         Ok(storages) => {
//             if storages.is_empty() {
//                 println!("No storage devices found.");
//             } else {
//                 for storage in storages {
//                     println!("{}", storage);
//                     if storage.mount_points.len() > 1 {
//                         println!("  Mount points: {}", storage.mount_points.join(", "));
//                     }
//                 }
//             }
//         }
//         Err(e) => {
//             eprintln!("Error getting storage information: {}", e);
//         }
//     }
    
//     Ok(())
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_storage_info() {
        let result = get_all_storage_info();
        assert!(result.is_ok());
        
        let storages = result.unwrap();
        // Should have at least one storage device (root/system drive)
        assert!(!storages.is_empty());
        
        for storage in storages {
            // Basic validation
            assert!(!storage.device.is_empty());
            assert!(!storage.mount_points.is_empty());
            assert!(storage.total_bytes > 0);
            assert!(storage.used_bytes <= storage.total_bytes);
            assert!(storage.available_bytes <= storage.total_bytes);
        }
    }

    #[test]
    fn test_storage_info_display() {
        let storage = StorageInfo {
            device: "/dev/sda".to_string(),
            filesystem: "ext4".to_string(),
            total_bytes: 1_000_000_000_000, // 1 TB
            used_bytes: 500_000_000_000,    // 500 GB
            available_bytes: 500_000_000_000, // 500 GB
            mount_points: vec!["/".to_string()],
        };
        
        let display_str = format!("{}", storage);
        assert!(display_str.contains("/dev/sda"));
        //assert!(display_str.contains("ext4"));
        assert!(display_str.contains("50.0%"));
    }
}