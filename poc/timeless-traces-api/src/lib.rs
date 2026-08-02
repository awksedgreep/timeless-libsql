//! Traces-specific Rust data-plane proof of concept.
//!
//! This crate owns HTTP scheduling and SQLite connections. Span buffering,
//! the authoritative 8,192-span automatic flush, block layout, compression,
//! indexing, retention, and recovery remain in the public `timeless_traces`
//! extension.

mod api;
mod otlp;
mod query;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::net::TcpListener;

pub use api::{router, MAX_BODY_BYTES};
pub use storage::{
    FlushReport, IngestTimings, RuntimeWatermarks, Storage, StorageStats, TRACE_CAPABILITY,
};

pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct Config {
    pub extension_path: PathBuf,
    pub database_path: PathBuf,
    pub listen: SocketAddr,
    pub reader_connections: usize,
    pub command_queue_batches: usize,
    pub retention: Option<Duration>,
    pub flush_interval: Duration,
    pub optimize_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            extension_path: PathBuf::new(),
            database_path: PathBuf::new(),
            listen: "127.0.0.1:19449".parse().unwrap(),
            // Provisional correctness default. Session 7 must measure
            // one/two/four/eight rather than inheriting this as an answer.
            reader_connections: 2,
            command_queue_batches: 256,
            retention: Some(DEFAULT_RETENTION),
            flush_interval: Duration::from_secs(1),
            optimize_interval: Duration::from_secs(30),
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
        if self.retention.is_some_and(|duration| duration.is_zero()) {
            return Err("retention must be positive when enabled".into());
        }
        if self.flush_interval.is_zero() || self.optimize_interval.is_zero() {
            return Err("maintenance intervals must be positive".into());
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
        config.retention,
    )?;
    let app = router(storage.clone());
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

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|error| format!("serve API: {error}"));

    // Admission has stopped because axum has drained all accepted requests.
    // Stop maintenance before the final ordered flush/checkpoint barrier.
    flush_task.abort();
    optimize_task.abort();
    let _ = flush_task.await;
    let _ = optimize_task.await;
    let shutdown = storage.shutdown().await;
    served.and(shutdown)
}

async fn shutdown_signal() {
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
                eprintln!("timeless-traces-api maintenance error: {error}");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_reader_count_is_explicitly_provisional() {
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
