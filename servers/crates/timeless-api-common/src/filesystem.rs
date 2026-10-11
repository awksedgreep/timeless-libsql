//! What the data directory's filesystem means for a signal database.
//!
//! The servers rewrite 4 KB pages all over a multi-gigabyte file every few
//! seconds. On a copy-on-write filesystem each rewritten page becomes its
//! own extent, and under maintenance load the kernel throttles and stalls
//! the writer for seconds (#139, #142). Nothing in the server's own
//! behaviour points at the filesystem, so a server says so when it starts.

use std::path::Path;

/// A warning to print at startup if `database` is, or will be created, on
/// btrfs without the no-copy-on-write attribute; `None` otherwise, including
/// wherever that cannot be determined (other platforms, other filesystems,
/// a path that cannot be opened).
pub fn copy_on_write_warning(database: &Path) -> Option<String> {
    // The file's own flag once it exists; before that, the directory's,
    // which new files inherit.
    let target = if database.exists() {
        database.to_path_buf()
    } else {
        match database.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => Path::new(".").to_path_buf(),
        }
    };
    if !imp::btrfs_copy_on_write(&target)? {
        return None;
    }
    Some(format!(
        "{} is on btrfs with copy-on-write enabled. Database pages rewritten in \
         place each become their own extent there, and under maintenance load the \
         kernel can stall the writer for seconds at a time. Keep the data directory \
         NOCOW (`chattr +C` on an empty directory, then create or copy the database \
         into it) or on ext4/xfs. See \"Where to put the database\" in the servers \
         README.",
        target.display()
    ))
}

#[cfg(target_os = "linux")]
mod imp {
    use std::ffi::CString;
    use std::fs::File;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    const BTRFS_SUPER_MAGIC: i64 = 0x9123_683E;
    const FS_NOCOW_FL: libc::c_long = 0x0080_0000;
    // _IOR('f', 1, long): the size of `long` is part of the request number.
    #[cfg(target_pointer_width = "64")]
    const FS_IOC_GETFLAGS: libc::c_ulong = 0x8008_6601;
    #[cfg(target_pointer_width = "32")]
    const FS_IOC_GETFLAGS: libc::c_ulong = 0x8004_6601;

    /// `Some(true)` for a path on btrfs without NOCOW, `Some(false)` for
    /// one with it, `None` when it is not btrfs or cannot be examined.
    pub(super) fn btrfs_copy_on_write(path: &Path) -> Option<bool> {
        let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: `c_path` is a valid C string and `stat` a valid buffer.
        let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c_path.as_ptr(), &mut stat) } != 0 {
            return None;
        }
        if stat.f_type as i64 != BTRFS_SUPER_MAGIC {
            return None;
        }
        let file = File::open(path).ok()?;
        let mut flags: libc::c_long = 0;
        // SAFETY: the descriptor is open and `flags` is the `long` the
        // request writes.
        if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_GETFLAGS as _, &mut flags) } != 0 {
            return None;
        }
        Some(flags & FS_NOCOW_FL == 0)
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::path::Path;

    pub(super) fn btrfs_copy_on_write(_path: &Path) -> Option<bool> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_that_cannot_be_examined_is_not_warned_about() {
        assert_eq!(
            copy_on_write_warning(Path::new("/nonexistent-timeless-dir/metrics.db")),
            None
        );
    }

    #[test]
    fn a_filesystem_that_is_not_btrfs_is_not_warned_about() {
        // procfs is never btrfs, wherever the tests run.
        if Path::new("/proc/self").exists() {
            assert_eq!(copy_on_write_warning(Path::new("/proc/self/status")), None);
            assert_eq!(copy_on_write_warning(Path::new("/proc/self/new.db")), None);
        }
    }
}
