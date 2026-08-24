//! Release server for the Timeless metrics data plane.
//!
//! This crate owns HTTP scheduling and SQLite connections. Metrics buffering,
//! the 4,096-point automatic flush, compression, chunks, series identity,
//! rollups, and retention remain in the existing `timeless_metrics` extension.

mod api;
mod promql;
mod query;
mod scrape;
mod storage;
mod victoria;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use timeless_api_common::{
    maintenance_task, protect_router, shutdown_signal, validate_loopback, AuthConfig,
};
use tokio::net::TcpListener;

pub use api::{router, router_with_limits};
pub use scrape::{
    ScrapeAuth, ScrapeTarget, ScrapeTargetReport, ScrapeTargetSet, ScrapeTargetSetReport,
};
pub use storage::{FlushReport, Storage, StorageStats};
pub use timeless_api_common::BackupReport;

pub const DEFAULT_RAW_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Cadence of the writer's periodic `wal_checkpoint(TRUNCATE)`. It keeps the
/// WAL file near its configured bound instead of its high-water size; a busy
/// pass is only reported and the next interval tries again.
const WAL_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(300);

/// Hard PromQL execution limits that apply even when API authentication is
/// disabled. Authentication claims may impose tighter request-specific
/// limits, but can never raise these storage-owner bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PromQueryLimits {
    pub max_points_per_series: usize,
    pub max_result_points: usize,
    pub max_work_points: usize,
    pub max_response_bytes: usize,
    /// Resolution used by PromQL subqueries that omit their step. This is a
    /// query-engine setting (Prometheus calls it the global evaluation
    /// interval), not an extension or storage-table property.
    pub default_subquery_step: Duration,
    pub deadline: Duration,
}

impl Default for PromQueryLimits {
    fn default() -> Self {
        Self {
            max_points_per_series: 11_000,
            max_result_points: 100_000,
            max_work_points: 100_000,
            max_response_bytes: 16 * 1024 * 1024,
            default_subquery_step: Duration::from_secs(15),
            deadline: Duration::from_secs(30),
        }
    }
}

impl PromQueryLimits {
    pub fn validate(self) -> Result<(), String> {
        if self.max_points_per_series == 0 || self.max_points_per_series > 11_000 {
            return Err("max_points_per_series must be in 1..=11000".into());
        }
        if self.max_result_points == 0 {
            return Err("max_result_points must be positive".into());
        }
        if self.max_work_points == 0 {
            return Err("max_work_points must be positive".into());
        }
        if self.max_response_bytes == 0 {
            return Err("max_response_bytes must be positive".into());
        }
        if self.default_subquery_step.is_zero()
            || self.default_subquery_step.as_millis() > i64::MAX as u128
        {
            return Err("default_subquery_step must be in 1ms..=i64::MAXms".into());
        }
        if self.deadline.is_zero() {
            return Err("PromQL deadline must be positive".into());
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
    pub compact_interval: Duration,
    pub retention_interval: Duration,
    pub raw_retention: Duration,
    pub prom_query_limits: PromQueryLimits,
    pub auth: AuthConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            extension_path: PathBuf::new(),
            database_path: PathBuf::new(),
            listen: "127.0.0.1:19439".parse().unwrap(),
            // Session 6 measured 1/2/4/8 on the pinned mixed workload. Two
            // readers retained the best throughput/memory/tail balance.
            reader_connections: 2,
            command_queue_batches: 256,
            // These match TimelessMetrics.LibsqlEngine's current orchestration.
            flush_interval: Duration::from_secs(10),
            compact_interval: Duration::from_secs(5 * 60),
            retention_interval: Duration::from_secs(60 * 60),
            raw_retention: DEFAULT_RAW_RETENTION,
            prom_query_limits: PromQueryLimits::default(),
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
        if self.flush_interval.is_zero()
            || self.compact_interval.is_zero()
            || self.retention_interval.is_zero()
            || self.raw_retention.is_zero()
        {
            return Err("maintenance and retention intervals must be positive".into());
        }
        self.prom_query_limits.validate()?;
        self.auth.preflight()?;
        Ok(())
    }
}

pub async fn run(config: Config) -> Result<(), String> {
    config.validate()?;
    let storage = Storage::start(
        config.database_path.clone(),
        config.extension_path.clone(),
        config.reader_connections,
        config.command_queue_batches,
        config.raw_retention,
    )?;
    let app = protect_router(
        router_with_limits(storage.clone(), config.prom_query_limits),
        config.auth.clone(),
    );
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|error| format!("bind {}: {error}", config.listen))?;
    println!("timeless-metrics-api listening on {}", config.listen);

    let flush_task = maintenance_task(
        config.flush_interval,
        storage.clone(),
        |storage| async move { storage.schedule_flush().await },
    );
    let compact_task = maintenance_task(
        config.compact_interval,
        storage.clone(),
        |storage| async move { storage.schedule_compact().await },
    );
    let retention_task = maintenance_task(
        config.retention_interval,
        storage.clone(),
        |storage| async move { storage.schedule_retention().await },
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

    flush_task.abort();
    compact_task.abort();
    retention_task.abort();
    wal_checkpoint_task.abort();
    let _ = flush_task.await;
    let _ = compact_task.await;
    let _ = retention_task.await;
    let _ = wal_checkpoint_task.await;
    let shutdown = storage.shutdown().await;
    served.and(shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_reader_topology_matches_the_measured_operating_point() {
        assert_eq!(Config::default().reader_connections, 2);
    }

    #[test]
    fn invalid_configuration_fails_before_opening_storage() {
        let mut config = Config::default();
        assert_eq!(config.validate().unwrap_err(), "extension path is required");

        config.extension_path = "extension.so".into();
        assert_eq!(config.validate().unwrap_err(), "database path is required");

        config.database_path = "metrics.db".into();
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

        config.command_queue_batches = 1;
        config.flush_interval = Duration::ZERO;
        assert_eq!(
            config.validate().unwrap_err(),
            "maintenance and retention intervals must be positive"
        );

        config.flush_interval = Duration::from_secs(1);
        config.prom_query_limits.max_points_per_series = 11_001;
        assert_eq!(
            config.validate().unwrap_err(),
            "max_points_per_series must be in 1..=11000"
        );
        config.prom_query_limits.max_points_per_series = 11_000;
        config.prom_query_limits.max_response_bytes = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "max_response_bytes must be positive"
        );
    }

    #[test]
    fn default_subquery_resolution_is_positive_and_bounded() {
        assert_eq!(
            PromQueryLimits::default().default_subquery_step,
            Duration::from_secs(15)
        );
        let limits = PromQueryLimits {
            default_subquery_step: Duration::ZERO,
            ..PromQueryLimits::default()
        };
        assert_eq!(
            limits.validate().unwrap_err(),
            "default_subquery_step must be in 1ms..=i64::MAXms"
        );
    }
}
