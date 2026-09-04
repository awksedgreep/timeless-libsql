//! Release server for the Timeless traces data plane.
//!
//! This crate owns HTTP scheduling and SQLite connections. Span buffering,
//! the authoritative 8,192-span automatic flush, block layout, compression,
//! indexing, retention, and recovery remain in the public `timeless_traces`
//! extension.

mod api;
mod otlp;
mod query;
mod storage;
mod tail;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use timeless_api_common::{
    maintenance_task, protect_router, shutdown_signal, validate_loopback, AuthConfig,
};
use tokio::net::TcpListener;

pub use api::{router, router_with_limits, MAX_BODY_BYTES};
pub use storage::{
    FlushReport, IngestTimings, RuntimeWatermarks, Storage, StorageStats, TRACE_CAPABILITY,
};
pub use timeless_api_common::BackupReport;

pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Cadence of the writer's periodic `wal_checkpoint(TRUNCATE)`. It keeps the
/// WAL file near its configured bound instead of its high-water size; a busy
/// pass is only reported and the next interval tries again.
const WAL_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(300);

/// Hard read-path execution limits, mirroring the logs/metrics servers'
/// posture: bounded evaluation even when authentication is disabled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TracesQueryLimits {
    pub max_response_bytes: usize,
    pub deadline: Duration,
}

impl Default for TracesQueryLimits {
    fn default() -> Self {
        Self {
            // Bulk-search contract, not the logs/metrics 16 MiB: the
            // suite reads 131072 spans in one search (tens of MB of
            // envelope), so the cap only kills GB-scale egress while
            // the search limit and deadline bound compute and memory.
            max_response_bytes: 256 * 1024 * 1024,
            deadline: Duration::from_secs(30),
        }
    }
}

impl TracesQueryLimits {
    pub fn validate(self) -> Result<(), String> {
        if self.max_response_bytes == 0 {
            return Err("max_response_bytes must be positive".into());
        }
        if self.deadline.as_millis() == 0 {
            return Err("traces query deadline must be at least 1ms".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub extension_path: PathBuf,
    pub database_path: PathBuf,
    pub listen: SocketAddr,
    pub reader_connections: usize,
    pub command_queue_batches: usize,
    pub retention: Option<Duration>,
    /// `false` inherits the retention persisted by the migration-created
    /// virtual table. `true` requires an exact configured match.
    pub enforce_retention: bool,
    pub flush_interval: Duration,
    pub optimize_interval: Duration,
    pub query_limits: TracesQueryLimits,
    pub queue_bytes: usize,
    pub auth: AuthConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            extension_path: PathBuf::new(),
            database_path: PathBuf::new(),
            listen: "127.0.0.1:19449".parse().unwrap(),
            // Session 7 measured one/two/four/eight on the fixed 800k-span
            // query matrix. Two retained the best useful tails while holding
            // process HWM to 66,732 KiB (four/eight used 110/198 MiB).
            reader_connections: 2,
            command_queue_batches: 256,
            retention: None,
            enforce_retention: false,
            flush_interval: Duration::from_secs(1),
            optimize_interval: Duration::from_secs(30),
            query_limits: TracesQueryLimits::default(),
            queue_bytes: Storage::DEFAULT_QUEUE_BYTES,
            auth: AuthConfig::disabled(),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.extension_path.as_os_str().is_empty() {
            return Err("extension path is required".into());
        }
        if self.database_path.as_os_str().is_empty() {
            return Err("database path is required".into());
        }
        validate_loopback(self.listen)?;
        if self.reader_connections == 0 {
            return Err("reader_connections must be positive".into());
        }
        if self.command_queue_batches == 0 {
            return Err("command_queue_batches must be positive".into());
        }
        if self.queue_bytes == 0 {
            return Err("queue_bytes must be positive".into());
        }
        if self.retention.is_some_and(|duration| duration.is_zero()) {
            return Err("retention must be positive when enabled".into());
        }
        if !self.enforce_retention && self.retention.is_some() {
            return Err("inherited retention cannot also specify a duration".into());
        }
        if self.flush_interval.is_zero() || self.optimize_interval.is_zero() {
            return Err("maintenance intervals must be positive".into());
        }
        self.auth.preflight()?;
        Ok(())
    }
}

pub async fn run(config: Config) -> Result<(), String> {
    config.validate()?;
    let storage = Storage::start_with_queue_bytes_and_retention_policy(
        config.database_path.clone(),
        config.extension_path.clone(),
        config.reader_connections,
        config.command_queue_batches,
        config.retention,
        config.enforce_retention,
        config.queue_bytes,
    )?;
    let app = protect_router(
        router_with_limits(storage.clone(), config.query_limits),
        config.auth.clone(),
    );
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|error| format!("bind {}: {error}", config.listen))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("read listening address: {error}"))?;
    println!("timeless-traces-api listening on {address}");

    let flush_task = maintenance_task(
        config.flush_interval,
        storage.clone(),
        |storage| async move { storage.schedule_flush().await },
    );
    let optimize_task = maintenance_task(
        config.optimize_interval,
        storage.clone(),
        |storage| async move { storage.schedule_optimize().await },
    );
    let wal_checkpoint_task = maintenance_task(
        WAL_CHECKPOINT_INTERVAL,
        storage.clone(),
        |storage| async move { storage.schedule_wal_checkpoint().await },
    );

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|error| format!("serve API: {error}"));

    // Admission has stopped because axum has drained all accepted requests.
    // Stop maintenance before the final ordered flush/checkpoint barrier.
    flush_task.abort();
    optimize_task.abort();
    wal_checkpoint_task.abort();
    let _ = flush_task.await;
    let _ = optimize_task.await;
    let _ = wal_checkpoint_task.await;
    let shutdown = storage.shutdown().await;
    served.and(shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measured_reader_default_is_pinned() {
        assert_eq!(Config::default().reader_connections, 2);
    }

    #[test]
    fn invalid_configuration_fails_before_storage_is_opened() {
        let mut config = Config::default();
        assert_eq!(config.validate().unwrap_err(), "extension path is required");
        config.extension_path = "extension.so".into();
        assert_eq!(config.validate().unwrap_err(), "database path is required");
        config.database_path = "traces.db".into();
        config.reader_connections = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "reader_connections must be positive"
        );
        config.reader_connections = 1;
        config.command_queue_batches = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "command_queue_batches must be positive"
        );
    }
}
