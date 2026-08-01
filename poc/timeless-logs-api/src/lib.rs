//! API-only proof of concept for the Timeless logs data plane.
//!
//! Storage policy is intentionally not implemented here. The API feeds the
//! existing `timeless_logs` batch-blob path and leaves its 8,192-entry buffer,
//! automatic raw flush, block layout, and compression behavior unchanged.

mod api;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::net::TcpListener;

pub use api::router;
pub use storage::{LogEntry, QuerySpec, Storage, StorageStats};

#[derive(Clone, Debug)]
pub struct Config {
    pub extension_path: PathBuf,
    pub database_path: PathBuf,
    pub listen: SocketAddr,
    pub reader_connections: usize,
    pub command_queue_batches: usize,
    pub flush_interval: Duration,
    pub optimize_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            extension_path: PathBuf::new(),
            database_path: PathBuf::new(),
            listen: "127.0.0.1:19429".parse().unwrap(),
            reader_connections: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(4)
                .clamp(1, 8),
            command_queue_batches: 256,
            // Host orchestration only: the extension remains passive at low
            // volume, just as it does for every direct SQLite user.
            flush_interval: Duration::from_secs(1),
            optimize_interval: Duration::from_secs(30),
        }
    }
}

pub async fn run(config: Config) -> Result<(), String> {
    if config.extension_path.as_os_str().is_empty() {
        return Err("extension path is required".into());
    }
    if config.database_path.as_os_str().is_empty() {
        return Err("database path is required".into());
    }
    if config.reader_connections == 0 || config.command_queue_batches == 0 {
        return Err("reader_connections and command_queue_batches must be positive".into());
    }

    let storage = Storage::start(
        config.database_path.clone(),
        config.extension_path.clone(),
        config.reader_connections,
        config.command_queue_batches,
    )?;
    let app = router(storage.clone());
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|e| format!("bind {}: {e}", config.listen))?;
    println!("timeless-logs-api listening on {}", config.listen);

    let flush_storage = storage.clone();
    let flush_interval = config.flush_interval;
    let flush_task = tokio::spawn(async move {
        let mut timer = tokio::time::interval(flush_interval);
        timer.tick().await;
        loop {
            timer.tick().await;
            if flush_storage.schedule_flush().await.is_err() {
                break;
            }
        }
    });

    let optimize_storage = storage.clone();
    let optimize_interval = config.optimize_interval;
    let optimize_task = tokio::spawn(async move {
        let mut timer = tokio::time::interval(optimize_interval);
        timer.tick().await;
        loop {
            timer.tick().await;
            if optimize_storage.schedule_optimize().await.is_err() {
                break;
            }
        }
    });

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| format!("serve API: {e}"));

    flush_task.abort();
    optimize_task.abort();
    let shutdown = storage.shutdown().await;
    served.and(shutdown)
}
