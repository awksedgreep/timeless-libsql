//! Neutral release lifecycle shared by the three signal-specific servers.
//!
//! Signal routes, wire formats, query semantics, and storage commands do not
//! belong here. This crate is limited to extension/schema negotiation, owner
//! fencing, loopback policy, shutdown signals, and identical maintenance-task
//! lifecycle.

mod auth;

pub use auth::{protect_router, AuthConfig, ClaimLimits, VerifiedClaims, RESULT_ROWS_HEADER};

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use rusqlite::backup::Backup;
use rusqlite::{params, Connection, OptionalExtension};
use semver::Version;
use serde::{Deserialize, Serialize};

pub const DATA_SCHEMA_VERSION: i64 = 1;
pub const REQUIRED_EXTENSION_DATA_ABI: u64 = 1;
pub const MINIMUM_EXTENSION_VERSION: &str = "0.3.0";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupRequest {
    pub destination: PathBuf,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct BackupReport {
    pub signal: String,
    pub destination: String,
    pub bytes: u64,
    pub pages: i32,
    pub schema_version: i64,
    pub checkpoint_log_frames: i64,
    pub checkpointed_frames: i64,
    pub completed_unix_seconds: u64,
    pub elapsed_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointReport {
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

/// Checkpoint every committed WAL frame or fail. A busy result is not a
/// successful maintenance boundary: callers must never label a partially
/// checkpointed database as a release backup.
pub fn checkpoint_wal(conn: &Connection, signal: &str) -> Result<CheckpointReport, String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|error| format!("checkpoint {signal} WAL: {error}"))?;
        if busy == 0 && log_frames == checkpointed_frames {
            return Ok(CheckpointReport {
                log_frames,
                checkpointed_frames,
            });
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "checkpoint {signal} WAL remained busy after 5s: busy={busy}, log_frames={log_frames}, checkpointed_frames={checkpointed_frames}"
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Copy a database with SQLite's public online-backup API while the caller's
/// established writer connection is the sole storage owner. The final path is
/// published without overwrite only after page-copy, integrity, schema, and
/// fsync checks succeed.
pub fn create_verified_backup(
    conn: &Connection,
    destination: &Path,
    signal: &str,
    checkpoint: CheckpointReport,
) -> Result<BackupReport, String> {
    let started = Instant::now();
    if !destination.is_absolute() {
        return Err("backup destination must be an absolute path".into());
    }
    if destination.file_name().is_none() {
        return Err("backup destination must name a file".into());
    }
    if destination.exists() {
        return Err(format!(
            "backup destination already exists; refusing to overwrite {}",
            destination.display()
        ));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| "backup destination has no parent directory".to_string())?;
    let canonical_parent = parent.canonicalize().map_err(|error| {
        format!(
            "backup parent directory must already exist and be accessible {}: {error}",
            parent.display()
        )
    })?;
    if !canonical_parent.is_dir() {
        return Err(format!(
            "backup parent is not a directory: {}",
            parent.display()
        ));
    }
    let name = destination
        .file_name()
        .expect("checked above")
        .to_string_lossy();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system time precedes Unix epoch: {error}"))?
        .as_nanos();
    let partial = canonical_parent.join(format!(
        ".{name}.timeless-backup-{}-{nonce}.partial",
        std::process::id()
    ));
    let final_path = canonical_parent.join(destination.file_name().expect("checked above"));

    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)
        .map_err(|error| format!("create backup staging file {}: {error}", partial.display()))?;

    let result = (|| {
        let mut target = Connection::open(&partial)
            .map_err(|error| format!("open backup staging database: {error}"))?;
        let pages = {
            let backup = Backup::new(conn, &mut target)
                .map_err(|error| format!("initialize {signal} SQLite backup: {error}"))?;
            backup
                .run_to_completion(256, Duration::from_millis(2), None)
                .map_err(|error| format!("copy {signal} SQLite backup pages: {error}"))?;
            backup.progress().pagecount
        };
        let integrity: String = target
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .map_err(|error| format!("verify {signal} backup integrity: {error}"))?;
        if integrity != "ok" {
            return Err(format!(
                "verify {signal} backup integrity: PRAGMA quick_check returned {integrity:?}"
            ));
        }
        let schema_version = preflight_database(&target, signal)?;
        if schema_version != DATA_SCHEMA_VERSION {
            return Err(format!(
                "verify {signal} backup schema: expected version {DATA_SCHEMA_VERSION}, found {schema_version}"
            ));
        }
        target
            .close()
            .map_err(|(_, error)| format!("close {signal} backup database: {error}"))?;
        let staging = OpenOptions::new()
            .read(true)
            .open(&partial)
            .map_err(|error| format!("reopen backup staging file for fsync: {error}"))?;
        staging
            .sync_all()
            .map_err(|error| format!("fsync {signal} backup staging file: {error}"))?;

        // hard_link is the portable no-clobber publication primitive. Both
        // names are in the same canonical directory, so this cannot cross a
        // filesystem boundary.
        std::fs::hard_link(&partial, &final_path).map_err(|error| {
            format!(
                "publish {signal} backup without overwrite {}: {error}",
                final_path.display()
            )
        })?;
        std::fs::remove_file(&partial)
            .map_err(|error| format!("remove published backup staging name: {error}"))?;
        #[cfg(unix)]
        File::open(&canonical_parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("fsync backup directory: {error}"))?;

        let bytes = std::fs::metadata(&final_path)
            .map_err(|error| format!("inspect completed backup: {error}"))?
            .len();
        let completed_unix_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time precedes Unix epoch: {error}"))?
            .as_secs();
        Ok(BackupReport {
            signal: signal.to_string(),
            destination: final_path.to_string_lossy().into_owned(),
            bytes,
            pages,
            schema_version,
            checkpoint_log_frames: checkpoint.log_frames,
            checkpointed_frames: checkpoint.checkpointed_frames,
            completed_unix_seconds,
            elapsed_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&partial);
    }
    result
}

pub fn server_build_identity(signal: &str) -> serde_json::Value {
    serde_json::json!({
        "name": format!("timeless-{signal}-api"),
        "version": env!("CARGO_PKG_VERSION"),
        "commit": env!("TIMELESS_BUILD_COMMIT_RESOLVED"),
        "target": env!("TIMELESS_BUILD_TARGET"),
        "profile": env!("TIMELESS_BUILD_PROFILE")
    })
}

#[derive(Clone, Copy, Debug)]
pub struct DataPlaneSpec {
    pub signal: &'static str,
    pub required_batch: &'static str,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ExtensionCapabilities {
    pub extension_version: String,
    pub data_abi: u64,
    pub minimum_server_version: String,
    pub build: BuildIdentity,
    pub signals: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct BuildIdentity {
    pub commit: String,
    pub target: String,
    pub profile: String,
}

/// Read and validate the extension handshake before a server creates a vtab,
/// changes a PRAGMA, or binds its listener.
pub fn preflight_extension(
    conn: &Connection,
    spec: DataPlaneSpec,
) -> Result<ExtensionCapabilities, String> {
    let encoded: String = conn
        .query_row("SELECT timeless_capabilities()", [], |row| row.get(0))
        .map_err(|error| {
            format!("incompatible timeless extension: missing timeless_capabilities(): {error}")
        })?;
    let capabilities: ExtensionCapabilities = serde_json::from_str(&encoded)
        .map_err(|error| format!("invalid timeless extension capability document: {error}"))?;
    if capabilities.data_abi != REQUIRED_EXTENSION_DATA_ABI {
        return Err(format!(
            "incompatible timeless extension data ABI: server requires {}, extension provides {}",
            REQUIRED_EXTENSION_DATA_ABI, capabilities.data_abi
        ));
    }
    let actual = Version::parse(&capabilities.extension_version).map_err(|error| {
        format!(
            "invalid timeless extension version {:?}: {error}",
            capabilities.extension_version
        )
    })?;
    let minimum = Version::parse(MINIMUM_EXTENSION_VERSION).expect("constant is valid semver");
    if actual < minimum {
        return Err(format!(
            "incompatible timeless extension version: server requires >= {minimum}, extension provides {actual}"
        ));
    }
    let server = Version::parse(env!("CARGO_PKG_VERSION")).expect("package version is semver");
    let minimum_server = Version::parse(&capabilities.minimum_server_version).map_err(|error| {
        format!(
            "invalid minimum server version {:?} in extension capability document: {error}",
            capabilities.minimum_server_version
        )
    })?;
    if server < minimum_server {
        return Err(format!(
            "incompatible timeless server version: extension requires >= {minimum_server}, server provides {server}"
        ));
    }
    let signal = capabilities.signals.get(spec.signal).ok_or_else(|| {
        format!(
            "incompatible timeless extension: missing {} capability",
            spec.signal
        )
    })?;
    let batches = signal
        .get("batches")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            format!(
                "incompatible timeless extension: {} capability has no batch list",
                spec.signal
            )
        })?;
    if !batches
        .iter()
        .any(|batch| batch.as_str() == Some(spec.required_batch))
    {
        return Err(format!(
            "incompatible timeless extension: {} server requires batch capability {:?}",
            spec.signal, spec.required_batch
        ));
    }
    Ok(capabilities)
}

/// Refuse a database written by a newer server before initialization mutates
/// it. A pre-ledger database is schema 0 and is eligible for the additive v1
/// ledger migration on the writer connection.
pub fn preflight_database(conn: &Connection, signal: &str) -> Result<i64, String> {
    let ledger_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_timeless_schema_migrations')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("inspect data schema ledger: {error}"))?;
    if !ledger_exists {
        return Ok(0);
    }
    let row: Option<(i64, i64)> = conn
        .query_row(
            "SELECT version,extension_data_abi
             FROM _timeless_schema_migrations
             WHERE signal = ?1 ORDER BY version DESC LIMIT 1",
            [signal],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| format!("read {signal} data schema ledger: {error}"))?;
    let Some((version, data_abi)) = row else {
        return Ok(0);
    };
    if version > DATA_SCHEMA_VERSION {
        return Err(format!(
            "incompatible {signal} data schema version {version}: this server supports at most {DATA_SCHEMA_VERSION}; use a compatible newer server"
        ));
    }
    if data_abi != REQUIRED_EXTENSION_DATA_ABI as i64 {
        return Err(format!(
            "incompatible {signal} database data ABI {data_abi}: this server requires {REQUIRED_EXTENSION_DATA_ABI}"
        ));
    }
    Ok(version)
}

/// Apply the additive schema ledger after the signal vtab has initialized.
/// The transaction is idempotent and records the exact extension build.
pub fn apply_schema_ledger(
    conn: &Connection,
    spec: DataPlaneSpec,
    capabilities: &ExtensionCapabilities,
) -> Result<(), String> {
    let prior = preflight_database(conn, spec.signal)?;
    if prior == DATA_SCHEMA_VERSION {
        return Ok(());
    }
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS _timeless_schema_migrations(
           signal TEXT NOT NULL,
           version INTEGER NOT NULL CHECK(version > 0),
           applied_at_unix INTEGER NOT NULL,
           server_version TEXT NOT NULL,
           extension_version TEXT NOT NULL,
           extension_data_abi INTEGER NOT NULL,
           PRIMARY KEY(signal, version)
         ) STRICT;",
    )
    .map_err(|error| {
        format!(
            "begin additive {signal} schema migration: {error}",
            signal = spec.signal
        )
    })?;
    let result = conn.execute(
        "INSERT OR IGNORE INTO _timeless_schema_migrations(
           signal,version,applied_at_unix,server_version,extension_version,extension_data_abi
         ) VALUES (?1,?2,unixepoch(),?3,?4,?5)",
        params![
            spec.signal,
            DATA_SCHEMA_VERSION,
            env!("CARGO_PKG_VERSION"),
            capabilities.extension_version,
            i64::try_from(capabilities.data_abi)
                .map_err(|_| "extension data ABI exceeds SQLite INTEGER".to_string())?,
        ],
    );
    match result {
        Ok(_) => conn
            .execute_batch("COMMIT;")
            .map_err(|error| format!("commit {} schema migration: {error}", spec.signal)),
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK;");
            Err(format!("apply {} schema migration: {error}", spec.signal))
        }
    }
}

pub fn require_current_schema(conn: &Connection, signal: &str) -> Result<(), String> {
    let version = preflight_database(conn, signal)?;
    if version != DATA_SCHEMA_VERSION {
        return Err(format!(
            "incompatible {signal} data schema version {version}: writer must apply additive version {DATA_SCHEMA_VERSION} before readers start"
        ));
    }
    Ok(())
}

pub fn validate_loopback(address: SocketAddr) -> Result<(), String> {
    let allow_non_loopback = matches!(
        std::env::var("TIMELESS_ALLOW_NON_LOOPBACK").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    );

    if address.ip().is_loopback() || allow_non_loopback {
        Ok(())
    } else {
        Err(format!(
            "non-loopback listen address {address} is disabled; use loopback or the release Unix-socket transport"
        ))
    }
}

pub fn acquire_database_lease(database_path: &Path, signal: &str) -> Result<File, String> {
    let lock_path = suffix_path(database_path, &format!(".timeless-{signal}-api.lock"));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| format!("open database owner lease {}: {error}", lock_path.display()))?;
    file.try_lock_exclusive().map_err(|error| {
        format!(
            "database {} is already owned by another timeless-{signal}-api process: {error}",
            database_path.display()
        )
    })?;
    Ok(file)
}

pub fn suffix_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub fn maintenance_task<S, F, Fut>(
    interval: Duration,
    state: S,
    operation: F,
) -> tokio::task::JoinHandle<()>
where
    S: Clone + Send + 'static,
    F: Fn(S) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send,
{
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.tick().await;
        loop {
            timer.tick().await;
            if let Err(error) = operation(state.clone()).await {
                eprintln!("timeless data-plane maintenance error: {error}");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn future_database_schema_fails_before_migration() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE _timeless_schema_migrations(
               signal TEXT NOT NULL, version INTEGER NOT NULL,
               applied_at_unix INTEGER NOT NULL, server_version TEXT NOT NULL,
               extension_version TEXT NOT NULL, extension_data_abi INTEGER NOT NULL,
               PRIMARY KEY(signal, version));
             INSERT INTO _timeless_schema_migrations VALUES
               ('logs', 999, 0, 'future', 'future', 999);",
        )
        .unwrap();
        let error = preflight_database(&conn, "logs").unwrap_err();
        assert!(error.contains("supports at most 1"), "{error}");
    }

    #[test]
    fn additive_ledger_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        let capabilities = ExtensionCapabilities {
            extension_version: "0.3.0".into(),
            data_abi: 1,
            minimum_server_version: "0.3.0".into(),
            build: BuildIdentity {
                commit: "test".into(),
                target: "test".into(),
                profile: "test".into(),
            },
            signals: serde_json::Map::new(),
        };
        let spec = DataPlaneSpec {
            signal: "metrics",
            required_batch: "named-v0",
        };
        apply_schema_ledger(&conn, spec, &capabilities).unwrap();
        apply_schema_ledger(&conn, spec, &capabilities).unwrap();
        assert_eq!(preflight_database(&conn, "metrics").unwrap(), 1);
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _timeless_schema_migrations",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn loopback_is_the_only_tcp_default() {
        unsafe { std::env::remove_var("TIMELESS_ALLOW_NON_LOOPBACK") };
        assert!(validate_loopback("127.0.0.1:1".parse().unwrap()).is_ok());
        assert!(validate_loopback("[::1]:1".parse().unwrap()).is_ok());
        assert!(validate_loopback("0.0.0.0:1".parse().unwrap()).is_err());
    }

    #[test]
    fn explicit_container_override_allows_non_loopback() {
        unsafe { std::env::set_var("TIMELESS_ALLOW_NON_LOOPBACK", "1") };
        assert!(validate_loopback("0.0.0.0:1".parse().unwrap()).is_ok());
        unsafe { std::env::remove_var("TIMELESS_ALLOW_NON_LOOPBACK") };
    }
}
