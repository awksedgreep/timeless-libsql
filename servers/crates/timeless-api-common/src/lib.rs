//! Neutral release lifecycle shared by the three signal-specific servers.
//!
//! Signal routes, wire formats, query semantics, and storage commands do not
//! belong here. This crate is limited to extension/schema negotiation, owner
//! fencing, loopback policy, shutdown signals, and identical maintenance-task
//! lifecycle.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};
use semver::Version;
use serde::Deserialize;

pub const DATA_SCHEMA_VERSION: i64 = 1;
pub const REQUIRED_EXTENSION_DATA_ABI: u64 = 1;
pub const MINIMUM_EXTENSION_VERSION: &str = "0.3.0";

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
    if address.ip().is_loopback() {
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
        assert!(validate_loopback("127.0.0.1:1".parse().unwrap()).is_ok());
        assert!(validate_loopback("[::1]:1".parse().unwrap()).is_ok());
        assert!(validate_loopback("0.0.0.0:1".parse().unwrap()).is_err());
    }
}
