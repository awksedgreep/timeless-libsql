//! Neutral release lifecycle shared by the three signal-specific servers.
//!
//! Signal routes, wire formats, query semantics, and storage commands do not
//! belong here. This crate is limited to extension/schema negotiation, owner
//! fencing, loopback policy, shutdown signals, and identical maintenance-task
//! lifecycle.

mod admission;
mod auth;
mod prometheus;

pub use admission::{BytesGate, GatePermit};
pub use auth::{protect_router, AuthConfig, ClaimLimits, VerifiedClaims, RESULT_ROWS_HEADER};
pub use prometheus::{build_info, Exposition, PROMETHEUS_CONTENT_TYPE};

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
pub const MINIMUM_EXTENSION_VERSION: &str = "0.8.0";

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

/// One best-effort `wal_checkpoint(TRUNCATE)` for periodic maintenance,
/// bounded only by the connection's busy timeout. Unlike [`checkpoint_wal`]
/// a busy or partial result is not retried here — the caller's next interval
/// tries again — but it is surfaced as the error string so the maintenance
/// loop's logging reports it. A complete checkpoint is silent.
pub fn periodic_wal_checkpoint(conn: &Connection, signal: &str) -> Result<(), String> {
    let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| format!("periodic {signal} WAL checkpoint: {error}"))?;
    if busy != 0 {
        return Err(format!(
            "periodic {signal} WAL checkpoint could not complete: busy={busy}, log_frames={log_frames}, checkpointed_frames={checkpointed_frames}"
        ));
    }
    Ok(())
}

/// Copy a database with SQLite's public online-backup API while the caller's
/// established writer connection is the sole storage owner. The final path is
/// published without overwrite only after page-copy, integrity, schema, and
/// fsync checks succeed.
/// The directory backups are confined to: `TIMELESS_BACKUP_DIR` when set,
/// otherwise `backups/` beside the database file. Confinement is what keeps
/// `POST /api/v1/backup` from being a write-anywhere primitive: without it a
/// caller can repeatedly stage full database copies under chosen names
/// anywhere the server process can write. Correct for authenticated callers
/// too, so it applies regardless of auth mode.
pub fn backup_root(conn: &Connection) -> Result<PathBuf, String> {
    if let Some(dir) = std::env::var_os("TIMELESS_BACKUP_DIR") {
        if dir.is_empty() {
            return Err("TIMELESS_BACKUP_DIR must not be empty".into());
        }
        return Ok(PathBuf::from(dir));
    }
    let main_db: String = conn
        .query_row(
            "SELECT file FROM pragma_database_list WHERE name = 'main'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("resolve main database path: {error}"))?;
    if main_db.is_empty() {
        return Err("backup requires a file-backed database".into());
    }
    let db_path = PathBuf::from(main_db);
    let parent = db_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| "database path has no parent directory".to_string())?;
    Ok(parent.join("backups"))
}

pub fn create_verified_backup(
    conn: &Connection,
    destination: &Path,
    signal: &str,
    checkpoint: CheckpointReport,
) -> Result<BackupReport, String> {
    let started = Instant::now();
    if destination.file_name().is_none() {
        return Err("backup destination must name a file".into());
    }
    let root = backup_root(conn)?;
    std::fs::create_dir_all(&root)
        .map_err(|error| format!("create backup directory {}: {error}", root.display()))?;
    let canonical_root = root.canonicalize().map_err(|error| {
        format!(
            "backup directory must be accessible {}: {error}",
            root.display()
        )
    })?;
    // Relative destinations resolve inside the root; absolute destinations
    // must already canonicalize inside it.
    let destination: PathBuf = if destination.is_absolute() {
        destination.to_path_buf()
    } else {
        canonical_root.join(destination)
    };
    if destination.exists() {
        return Err(format!(
            "backup destination already exists; refusing to overwrite {}",
            destination.display()
        ));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| "backup destination has no parent directory".to_string())?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("create backup directory {}: {error}", parent.display()))?;
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
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(format!(
            "backup destination {} is outside the backup directory {} \
             (set TIMELESS_BACKUP_DIR to relocate it)",
            destination.display(),
            canonical_root.display()
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
    #[serde(default)]
    pub query_surfaces: serde_json::Map<String, serde_json::Value>,
}

/// Require one additive query-surface flag after the base extension handshake.
/// A server calls this before opening its virtual table when correct bounded
/// execution depends on a particular public storage guard.
pub fn require_query_surface(
    capabilities: &ExtensionCapabilities,
    surface: &str,
    capability: &str,
) -> Result<(), String> {
    if capabilities
        .query_surfaces
        .get(surface)
        .and_then(|surface| surface.get(capability))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return Ok(());
    }
    Err(format!(
        "incompatible timeless extension: query surface {surface:?} requires capability {capability:?}"
    ))
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
            "non-loopback listen address {address} is disabled; use loopback or set TIMELESS_ALLOW_NON_LOOPBACK=1 for an explicitly secured deployment"
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

    #[test]
    fn backups_are_confined_to_the_backup_root() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("signal.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE _timeless_schema_migrations(
               signal TEXT NOT NULL, version INTEGER NOT NULL,
               applied_at_unix INTEGER NOT NULL, server_version TEXT NOT NULL,
               extension_version TEXT NOT NULL, extension_data_abi INTEGER NOT NULL,
               PRIMARY KEY(signal, version));
             INSERT INTO _timeless_schema_migrations VALUES
               ('test', 1, 0, 'test', 'test', 1);
             CREATE TABLE t(x); INSERT INTO t VALUES (1);",
        )
        .unwrap();
        let checkpoint = checkpoint_wal(&conn, "test").unwrap();

        // Outside the root: rejected, nothing written.
        let outside = dir.path().join("stolen.db");
        let error = create_verified_backup(&conn, &outside, "test", checkpoint).unwrap_err();
        assert!(error.contains("outside the backup directory"), "{error}");
        assert!(!outside.exists());

        // Absolute path inside the default root: accepted.
        let inside = dir.path().join("backups").join("good.db");
        create_verified_backup(&conn, &inside, "test", checkpoint).unwrap();
        assert!(inside.exists());

        // Relative destination resolves inside the root.
        create_verified_backup(&conn, Path::new("relative.db"), "test", checkpoint).unwrap();
        assert!(dir.path().join("backups").join("relative.db").exists());

        // Traversal out of the root: rejected.
        let sneaky = dir.path().join("backups").join("../escape.db");
        let error = create_verified_backup(&conn, &sneaky, "test", checkpoint).unwrap_err();
        assert!(error.contains("outside the backup directory"), "{error}");
        assert!(!dir.path().join("escape.db").exists());
    }

    use super::*;
    use std::sync::Mutex;

    static NON_LOOPBACK_ENV: Mutex<()> = Mutex::new(());

    #[test]
    fn release_compatibility_floor_matches_the_server_release_line() {
        assert_ne!(
            env!("CARGO_PKG_VERSION"),
            "0.3.0",
            "the tagged v0.3.0 artifact predates the release server handshake"
        );
        assert!(
            env!("CARGO_PKG_VERSION").starts_with("0.8."),
            "the current servers must remain on the documented 0.8 compatibility line"
        );
        assert_eq!(
            MINIMUM_EXTENSION_VERSION, "0.8.0",
            "the 0.8 line moved the extension floor with the workspace bump"
        );
    }

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
            extension_version: "0.4.0".into(),
            data_abi: 1,
            minimum_server_version: "0.4.0".into(),
            build: BuildIdentity {
                commit: "test".into(),
                target: "test".into(),
                profile: "test".into(),
            },
            signals: serde_json::Map::new(),
            query_surfaces: serde_json::Map::new(),
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
    fn additive_query_surface_capabilities_fail_closed() {
        let mut capabilities = ExtensionCapabilities {
            extension_version: "0.4.0".into(),
            data_abi: 1,
            minimum_server_version: "0.4.0".into(),
            build: BuildIdentity {
                commit: "test".into(),
                target: "test".into(),
                profile: "test".into(),
            },
            signals: serde_json::Map::new(),
            query_surfaces: serde_json::Map::new(),
        };
        let error =
            require_query_surface(&capabilities, "timeless_logs", "max_work_entries").unwrap_err();
        assert!(error.contains("timeless_logs"), "{error}");
        assert!(error.contains("max_work_entries"), "{error}");

        capabilities.query_surfaces.insert(
            "timeless_logs".into(),
            serde_json::json!({"max_work_entries": true}),
        );
        require_query_surface(&capabilities, "timeless_logs", "max_work_entries").unwrap();
    }

    #[test]
    fn loopback_is_the_only_tcp_default() {
        let _guard = NON_LOOPBACK_ENV.lock().unwrap();
        unsafe { std::env::remove_var("TIMELESS_ALLOW_NON_LOOPBACK") };
        assert!(validate_loopback("127.0.0.1:1".parse().unwrap()).is_ok());
        assert!(validate_loopback("[::1]:1".parse().unwrap()).is_ok());
        assert!(validate_loopback("0.0.0.0:1".parse().unwrap()).is_err());
    }

    #[test]
    fn explicit_container_override_allows_non_loopback() {
        let _guard = NON_LOOPBACK_ENV.lock().unwrap();
        unsafe { std::env::set_var("TIMELESS_ALLOW_NON_LOOPBACK", "1") };
        assert!(validate_loopback("0.0.0.0:1".parse().unwrap()).is_ok());
        unsafe { std::env::remove_var("TIMELESS_ALLOW_NON_LOOPBACK") };
    }

    #[test]
    fn non_loopback_error_names_the_transport_that_is_actually_supported() {
        let _guard = NON_LOOPBACK_ENV.lock().unwrap();
        unsafe { std::env::remove_var("TIMELESS_ALLOW_NON_LOOPBACK") };
        let error = validate_loopback("0.0.0.0:19439".parse().unwrap()).unwrap_err();
        assert!(error.contains("TIMELESS_ALLOW_NON_LOOPBACK=1"), "{error}");
        assert!(!error.contains("Unix-socket"), "{error}");
    }
}

/// Ceiling on the whole post-signal drain: serve-loop stop, maintenance
/// task teardown, storage flush/checkpoint/join. Orchestrators send
/// SIGTERM and then SIGKILL after their grace period; without a ceiling,
/// one open live-tail stream or one wedged checkpoint consumes that
/// grace and the process dies anyway — mid-drain and unlogged. On
/// expiry the drain future is dropped (aborting the in-flight drain) and
/// the returned errors explain why. Buffered-but-unflushed data beyond
/// the last durable barrier is lost exactly as on SIGKILL, which the
/// durability contract already accepts.
/// Stable reason a backup was refused because one is already running.
/// Handlers compare against this exact value to answer 409 instead of
/// 500; it is a shared sentinel, not a substring match on prose.
pub const BACKUP_IN_PROGRESS: &str = "a backup is already running";

/// At-most-one-at-a-time guard for work that occupies the single writer.
///
/// A backup flushes, drains the whole optimize backlog, checkpoints the
/// WAL and copies the database — all on the writer thread, so every
/// queued ingest waits behind it. Without this, N concurrent requests
/// queue N full backups and the writer does nothing else until they
/// finish: an authorized caller can stall ingest indefinitely just by
/// repeating the call. Refusing the overlap keeps the cost of a repeated
/// request O(1) instead of O(backup).
///
/// Deliberately per-process, not per-destination: the scarce resource is
/// the writer, not the output path.
#[derive(Debug, Default)]
pub struct SingleFlight {
    busy: std::sync::atomic::AtomicBool,
}

impl SingleFlight {
    pub const fn new() -> Self {
        Self {
            busy: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Claim the slot, or return `None` if it is already held. The guard
    /// releases on drop, so an early `?`, a cancelled request future, or
    /// a panic cannot strand the slot.
    pub fn try_enter(&self) -> Option<SingleFlightGuard<'_>> {
        self.busy
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
            .then_some(SingleFlightGuard { flag: &self.busy })
    }
}

#[derive(Debug)]
pub struct SingleFlightGuard<'a> {
    flag: &'a std::sync::atomic::AtomicBool,
}

impl Drop for SingleFlightGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, std::sync::atomic::Ordering::Release);
    }
}

pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

/// Run `drain` under [`SHUTDOWN_DEADLINE`]. `drain` yields the serve
/// result and the storage-shutdown result; on expiry both error slots
/// are populated and the process should proceed to exit.
pub async fn shutdown_with_deadline(
    deadline: Duration,
    signal: &'static str,
    drain: impl Future<Output = (Result<(), String>, Result<(), String>)>,
) -> (Result<(), String>, Result<(), String>) {
    match tokio::time::timeout(deadline, drain).await {
        Ok(pair) => pair,
        Err(_) => {
            eprintln!(
                "{signal}: shutdown deadline of {}s exceeded; exiting without \
                 a full drain — buffered tail data beyond the last durable \
                 barrier is lost, exactly as on SIGKILL",
                deadline.as_secs()
            );
            let expired = Err(format!(
                "{signal}: drain abandoned after the {}s shutdown deadline",
                SHUTDOWN_DEADLINE.as_secs()
            ));
            (expired.clone(), expired)
        }
    }
}

#[cfg(test)]
mod shutdown_deadline_tests {
    use super::*;
    use std::future::pending;

    #[tokio::test]
    async fn completed_drain_passes_both_results_through() {
        let (served, storage) = shutdown_with_deadline(Duration::from_secs(5), "t", async {
            (Ok(()), Err("storage failed".into()))
        })
        .await;
        assert_eq!(served, Ok(()));
        assert_eq!(storage, Err("storage failed".into()));
    }

    #[tokio::test]
    async fn hung_drain_hits_the_deadline_with_errors() {
        let started = Instant::now();
        let (served, storage) = shutdown_with_deadline(Duration::from_millis(50), "t", async {
            pending::<()>().await;
            (Ok(()), Ok(()))
        })
        .await;
        assert!(served.is_err() && storage.is_err());
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "deadline must actually bound the wait"
        );
    }
}

#[cfg(test)]
mod single_flight_tests {
    use super::SingleFlight;

    #[test]
    fn only_one_holder_at_a_time_and_the_slot_is_reusable() {
        let flight = SingleFlight::new();

        let first = flight.try_enter().expect("uncontended slot is available");
        assert!(
            flight.try_enter().is_none(),
            "a second caller must be refused while the slot is held"
        );

        drop(first);
        let second = flight
            .try_enter()
            .expect("dropping the guard releases the slot");
        assert!(flight.try_enter().is_none());
        drop(second);
        assert!(flight.try_enter().is_some());
    }

    #[test]
    fn an_unwinding_holder_still_releases_the_slot() {
        // The guard is what makes an early `?`, a cancelled request future
        // and a panic all release the slot; if release were manual, one
        // failed backup would wedge the endpoint until restart.
        let flight = SingleFlight::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = flight.try_enter().expect("slot available");
            panic!("holder blew up");
        }));
        assert!(result.is_err());
        assert!(
            flight.try_enter().is_some(),
            "the slot must not stay claimed after the holder unwound"
        );
    }
}
