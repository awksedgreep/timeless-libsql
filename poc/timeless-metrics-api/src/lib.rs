//! Standalone API-boundary proof of concept for the Timeless metrics data plane.
//!
//! This crate owns HTTP scheduling and SQLite connections. Metrics buffering,
//! the 4,096-point automatic flush, compression, chunks, series identity,
//! rollups, and retention remain in the existing `timeless_metrics` extension.

mod api;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::net::TcpListener;

pub use api::router;
pub use storage::{FlushReport, Storage, StorageStats};

pub const DEFAULT_RAW_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            extension_path: PathBuf::new(),
            database_path: PathBuf::new(),
            listen: "127.0.0.1:19439".parse().unwrap(),
            // Provisional correctness default. Session 6 will sweep 1/2/4/8;
            // this is not inherited as the final answer from the logs POC.
            reader_connections: 2,
            command_queue_batches: 256,
            // These match TimelessMetrics.LibsqlEngine's current orchestration.
            flush_interval: Duration::from_secs(10),
            compact_interval: Duration::from_secs(5 * 60),
            retention_interval: Duration::from_secs(60 * 60),
            raw_retention: DEFAULT_RAW_RETENTION,
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
    let app = router(storage.clone());
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

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|error| format!("serve API: {error}"));

    flush_task.abort();
    compact_task.abort();
    retention_task.abort();
    let _ = flush_task.await;
    let _ = compact_task.await;
    let _ = retention_task.await;
    let shutdown = storage.shutdown().await;
    served.and(shutdown)
}

fn maintenance_task<F, Fut>(
    interval: Duration,
    storage: Storage,
    operation: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(Storage) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send,
{
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.tick().await;
        loop {
            timer.tick().await;
            if let Err(error) = operation(storage.clone()).await {
                eprintln!("timeless-metrics-api maintenance error: {error}");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
