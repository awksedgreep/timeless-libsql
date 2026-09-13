use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::suffix_path;

/// Cooperative ownership of one database by the signal servers. SQLite
/// connections must use `database_path()` so their WAL and lease agree.
#[derive(Debug)]
pub struct DatabaseLease {
    database_path: PathBuf,
    locks: Vec<File>,
}

impl DatabaseLease {
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn unlock(self) -> io::Result<()> {
        for file in &self.locks {
            FileExt::unlock(file)?;
        }
        Ok(())
    }
}

fn resolve_database_path(path: &Path) -> Result<PathBuf, String> {
    match path.canonicalize() {
        Ok(path) => {
            let metadata = path
                .metadata()
                .map_err(|error| format!("inspect database {}: {error}", path.display()))?;
            if !metadata.is_file() {
                return Err(format!(
                    "database {} must be a regular file",
                    path.display()
                ));
            }
            // Hard links give SQLite different WAL names for the same inode.
            // A pathname-based lease cannot make that topology safe.
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(format!(
                        "database {} has hard links; use a single database file with symlink aliases instead",
                        path.display()
                    ));
                }
            }
            Ok(path)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if path.symlink_metadata().is_ok() {
                return Err(format!(
                    "database {} is a dangling symlink; create its target before starting the server",
                    path.display()
                ));
            }
            let filename = path
                .file_name()
                .ok_or_else(|| format!("database {} must name a file", path.display()))?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .canonicalize()
                .map_err(|error| {
                    format!("resolve database directory {}: {error}", path.display())
                })?;
            Ok(parent.join(filename))
        }
        Err(error) => Err(format!("resolve database {}: {error}", path.display())),
    }
}

pub fn acquire_database_lease(path: &Path, signal: &str) -> Result<DatabaseLease, String> {
    let database_path = resolve_database_path(path)?;
    let mut locks = Vec::new();
    // One owner across signals. Retain the previous per-signal locks too,
    // so an older server using the canonical path cannot enter alongside us.
    for suffix in [
        ".timeless-api.lock",
        ".timeless-metrics-api.lock",
        ".timeless-logs-api.lock",
        ".timeless-traces-api.lock",
    ] {
        let lock_path = suffix_path(&database_path, suffix);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| {
                format!("open database owner lease {}: {error}", lock_path.display())
            })?;
        file.try_lock_exclusive().map_err(|error| {
            format!(
                "database {} is already owned by another signal server; cannot start timeless-{signal}-api: {error}",
                database_path.display()
            )
        })?;
        locks.push(file);
    }
    Ok(DatabaseLease {
        database_path,
        locks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_paths_share_one_lease_across_signals_and_recover() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("signal.db");
        let first = acquire_database_lease(&path, "metrics").unwrap();
        assert_eq!(
            first.database_path(),
            directory.path().canonicalize().unwrap().join("signal.db")
        );
        for signal in ["metrics", "logs", "traces"] {
            let error =
                acquire_database_lease(&directory.path().join("./signal.db"), signal).unwrap_err();
            assert!(error.contains("already owned"), "{error}");
        }
        drop(first);
        acquire_database_lease(&path, "logs")
            .unwrap()
            .unlock()
            .unwrap();
        acquire_database_lease(&path, "traces").unwrap();
    }

    #[test]
    fn legacy_lease_is_respected_and_failed_acquisition_releases_all_locks() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("signal.db");
        for signal in ["metrics", "logs", "traces"] {
            let file =
                File::create(suffix_path(&path, &format!(".timeless-{signal}-api.lock"))).unwrap();
            file.try_lock_exclusive().unwrap();
            assert!(acquire_database_lease(&path, "metrics")
                .unwrap_err()
                .contains("already owned"));
            drop(file);
            acquire_database_lease(&path, "metrics").unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlink_aliases_resolve_before_leasing_existing_and_new_databases() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("signal.db");
        let dir_alias = directory.path().join("alias");
        symlink(directory.path(), &dir_alias).unwrap();
        let first = acquire_database_lease(&dir_alias.join("signal.db"), "metrics").unwrap();
        assert!(acquire_database_lease(&path, "logs").is_err());
        File::create(&path).unwrap();
        let file_alias = directory.path().join("alias.db");
        symlink(&path, &file_alias).unwrap();
        assert!(acquire_database_lease(&file_alias, "metrics").is_err());
        drop(first);
        let alias_owner = acquire_database_lease(&file_alias, "logs").unwrap();
        assert_eq!(alias_owner.database_path(), path.canonicalize().unwrap());
        assert!(acquire_database_lease(&path, "traces").is_err());
        drop(alias_owner);
        acquire_database_lease(&path, "traces").unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn hard_links_and_dangling_symlinks_fail_before_sqlite_opens() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("signal.db");
        File::create(&path).unwrap();
        let alias = directory.path().join("hard.db");
        std::fs::hard_link(&path, &alias).unwrap();
        for path in [&path, &alias] {
            assert!(acquire_database_lease(path, "metrics")
                .unwrap_err()
                .contains("hard links"));
        }
        let dangling = directory.path().join("dangling.db");
        std::os::unix::fs::symlink(directory.path().join("absent.db"), &dangling).unwrap();
        assert!(acquire_database_lease(&dangling, "metrics")
            .unwrap_err()
            .contains("dangling symlink"));
        assert!(!directory.path().join("absent.db").exists());
    }
}
