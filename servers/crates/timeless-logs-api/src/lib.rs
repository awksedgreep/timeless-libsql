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
use std::sync::Arc;
use std::time::Duration;

use timeless_api_common::{
    maintenance_task, protect_router, shutdown_after_signal_with_deadline, shutdown_signal,
    validate_loopback, AuthConfig,
};
use tokio::net::TcpListener;
use tokio::sync::Notify;

pub use api::{router, router_with_limits};
pub use logsql::{
    parse as parse_logsql, parse_at as parse_logsql_at, LogsqlError, LogsqlErrorKind, LogsqlOutput,
    LogsqlPlan,
};
pub use storage::{
    FieldCompareOp, LogEntry, LogField, LogPredicate, MetadataExact, NumericOp, PatternMatchMode,
    PatternMatcher, QuerySpec, Storage, StorageStats, StorePolicy, TimestampUnit, ValueTypeKind,
};
pub use timeless_api_common::otel::{
    MaintenanceTelemetry, OtelExportState, OtelHeaders, OtelTelemetry, OtelTracesConfig,
    OtelTracesStats,
};
pub use timeless_api_common::BackupReport;

/// Cadence of the writer's periodic `wal_checkpoint(TRUNCATE)`. It keeps the
/// WAL file near its configured bound instead of its high-water size; a busy
/// pass is only reported and the next interval tries again.
const WAL_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(300);

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
    pub queue_bytes: usize,
    pub flush_interval: Duration,
    pub optimize_interval: Duration,
    /// Optional one-span-per-optimize-sweep OpenTelemetry export. An unset
    /// endpoint disables it entirely.
    pub otel_traces: OtelTracesConfig,
    pub timestamp_unit: TimestampUnit,
    pub logs_query_limits: LogsQueryLimits,
    pub auth: AuthConfig,
    pub store_policy: StorePolicy,
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
            queue_bytes: Storage::DEFAULT_QUEUE_BYTES,
            // Host orchestration only: the extension remains passive at low
            // volume, just as it does for every direct SQLite user.
            flush_interval: Duration::from_secs(1),
            optimize_interval: Duration::from_secs(30),
            otel_traces: OtelTracesConfig::default(),
            // The released Elixir product's canonical timestamp is epoch
            // microseconds. Direct SQL callers can still create the legacy
            // default millisecond table explicitly.
            timestamp_unit: TimestampUnit::Microseconds,
            logs_query_limits: LogsQueryLimits::default(),
            auth: AuthConfig::disabled(),
            store_policy: StorePolicy::default(),
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
        if self.flush_interval.is_zero() || self.optimize_interval.is_zero() {
            return Err("maintenance intervals must be positive".into());
        }
        self.otel_traces.validate()?;
        self.logs_query_limits.validate()?;
        self.auth.preflight()?;
        Ok(())
    }
}

pub async fn run(config: Config) -> Result<(), String> {
    config.validate()?;
    let telemetry = OtelTelemetry::initialize(&config.otel_traces, "logs")?;
    if telemetry.is_some() {
        // Endpoint and header names only: header values are secrets.
        println!(
            "timeless-logs-api OpenTelemetry optimize tracing enabled: endpoint {} \
             sample_ratio {} queue {} spans, batch {} spans, delay {} ms, timeout {} ms, \
             headers {:?}",
            config.otel_traces.endpoint.as_deref().unwrap_or_default(),
            config.otel_traces.sample_ratio,
            config.otel_traces.queue_spans,
            config.otel_traces.batch_spans,
            config.otel_traces.export_delay.as_millis(),
            config.otel_traces.export_timeout.as_millis(),
            config.otel_traces.headers.names(),
        );
    }

    let storage = Storage::start_with_policy_full(
        config.database_path.clone(),
        config.extension_path.clone(),
        config.reader_connections,
        config.command_queue_batches,
        config.timestamp_unit,
        config.queue_bytes,
        config.store_policy.clone(),
    )?;
    if let Some(telemetry) = &telemetry {
        storage.attach_otel_health(telemetry.health());
    }
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
    let maintenance_telemetry = telemetry
        .as_ref()
        .map(|telemetry| telemetry.maintenance("logs"));
    let optimize_task = maintenance_task(
        config.optimize_interval,
        (storage.clone(), maintenance_telemetry),
        |(storage, telemetry)| async move {
            storage
                .schedule_optimize_with_telemetry(telemetry.as_ref())
                .await
        },
    );
    let wal_checkpoint_task = maintenance_task(
        WAL_CHECKPOINT_INTERVAL,
        storage.clone(),
        |storage| async move { storage.schedule_wal_checkpoint().await },
    );

    let shutdown_started = Arc::new(Notify::new());
    let shutdown_notice = Arc::clone(&shutdown_started);
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        shutdown_signal().await;
        shutdown_notice.notify_one();
    });
    let drain = async {
        let served = server.await.map_err(|e| format!("serve API: {e}"));
        flush_task.abort();
        optimize_task.abort();
        wal_checkpoint_task.abort();
        let shutdown = storage.shutdown().await;
        (served, shutdown)
    };
    let (served, shutdown) = shutdown_after_signal_with_deadline(
        timeless_api_common::SHUTDOWN_DEADLINE,
        "timeless-logs-api",
        shutdown_started.notified(),
        drain,
    )
    .await;
    if let Some(telemetry) = telemetry {
        // The exporter flush blocks on HTTP; keep it off the async workers.
        match tokio::task::spawn_blocking(move || telemetry.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("timeless-logs-api: {error}"),
            Err(error) => eprintln!("timeless-logs-api: flush OpenTelemetry traces: {error}"),
        }
    }
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
