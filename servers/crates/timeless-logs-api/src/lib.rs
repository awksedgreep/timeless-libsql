//! Release server for the Timeless logs data plane.
//!
//! Storage policy is intentionally not implemented here. The API feeds the
//! existing `timeless_logs` batch-blob path and leaves its 8,192-entry buffer,
//! automatic raw flush, block layout, and compression behavior unchanged.

mod api;
mod logsql;
mod pipeline;
mod storage;
mod syslog;
mod tail;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use timeless_api_common::{
    maintenance_task, protect_router, shutdown_signal, validate_loopback, AuthConfig,
};
use tokio::net::TcpListener;

pub use api::{router, router_with_limits};
pub use logsql::{
    parse as parse_logsql, parse_at as parse_logsql_at, LogsqlError, LogsqlErrorKind, LogsqlOutput,
    LogsqlPlan,
};
pub use storage::{
    FieldCompareOp, LogEntry, LogField, LogPredicate, MetadataExact, NumericOp, PatternMatchMode,
    PatternMatcher, QuerySpec, Storage, StorageStats, TimestampUnit, ValueTypeKind,
};
pub use timeless_api_common::BackupReport;

/// Hard LogsQL execution limits applied even when authentication is disabled.
/// Claim-derived policy may lower these values but cannot raise the storage
/// owner's bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogsQueryLimits {
    pub max_result_rows: usize,
    pub max_work_rows: usize,
    pub max_response_bytes: usize,
    pub deadline: Duration,
}

impl Default for LogsQueryLimits {
    fn default() -> Self {
        Self {
            max_result_rows: 100_000,
            max_work_rows: 100_000,
            max_response_bytes: 16 * 1024 * 1024,
            deadline: Duration::from_secs(30),
        }
    }
}

impl LogsQueryLimits {
    pub fn validate(self) -> Result<(), String> {
        if !(1..=100_000).contains(&self.max_result_rows) {
            return Err("max_result_rows must be in 1..=100000".into());
        }
        if self.max_work_rows == 0 {
            return Err("max_work_rows must be positive".into());
        }
        if self.max_response_bytes == 0 {
            return Err("max_response_bytes must be positive".into());
        }
        if self.deadline.as_millis() == 0 {
            return Err("LogsQL deadline must be at least 1ms".into());
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
    pub flush_interval: Duration,
    pub optimize_interval: Duration,
    pub timestamp_unit: TimestampUnit,
    pub logs_query_limits: LogsQueryLimits,
    pub auth: AuthConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            extension_path: PathBuf::new(),
            database_path: PathBuf::new(),
            listen: "127.0.0.1:19429".parse().unwrap(),
            // Two readers gave the best latency/memory balance in the pinned
            // mixed workload. Larger pools did not improve completed writes
            // and duplicate SQLite/extension working sets unnecessarily.
            reader_connections: 2,
            command_queue_batches: 256,
            // Host orchestration only: the extension remains passive at low
            // volume, just as it does for every direct SQLite user.
            flush_interval: Duration::from_secs(1),
            optimize_interval: Duration::from_secs(30),
            // The released Elixir product's canonical timestamp is epoch
            // microseconds. Direct SQL callers can still create the legacy
            // default millisecond table explicitly.
            timestamp_unit: TimestampUnit::Microseconds,
            logs_query_limits: LogsQueryLimits::default(),
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
        if self.flush_interval.is_zero() || self.optimize_interval.is_zero() {
            return Err("maintenance intervals must be positive".into());
        }
        self.logs_query_limits.validate()?;
        self.auth.preflight()?;
        Ok(())
    }
}

pub async fn run(config: Config) -> Result<(), String> {
    config.validate()?;

    let storage = Storage::start_with_timestamp_unit(
        config.database_path.clone(),
        config.extension_path.clone(),
        config.reader_connections,
        config.command_queue_batches,
        config.timestamp_unit,
    )?;
    let app = protect_router(
        router_with_limits(storage.clone(), config.logs_query_limits),
        config.auth.clone(),
    );
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|e| format!("bind {}: {e}", config.listen))?;
    println!("timeless-logs-api listening on {}", config.listen);

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

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| format!("serve API: {e}"));

    flush_task.abort();
    optimize_task.abort();
    let shutdown = storage.shutdown().await;
    served.and(shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_reader_pool_is_the_measured_embedded_balance() {
        assert_eq!(Config::default().reader_connections, 2);
    }

    #[test]
    fn invalid_configuration_fails_before_storage_is_opened() {
        let mut config = Config::default();
        assert_eq!(config.validate().unwrap_err(), "extension path is required");
        config.extension_path = "extension.so".into();
        assert_eq!(config.validate().unwrap_err(), "database path is required");
        config.database_path = "logs.db".into();
        config.reader_connections = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "reader_connections must be positive"
        );
        config.reader_connections = 1;
        config.logs_query_limits.max_response_bytes = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "max_response_bytes must be positive"
        );
    }
}
