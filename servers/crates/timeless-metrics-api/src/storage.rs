use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fs2::FileExt;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection};
use serde::Serialize;
use timeless_api_common::{
    acquire_database_lease, apply_schema_ledger, preflight_database, preflight_extension,
    require_current_schema, DataPlaneSpec,
};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::query::{self, QueryFeatures, ReadKind, ReadOutput, ReadRequest};

/// The same ladder currently created by `TimelessMetrics.LibsqlEngine`.
pub const DEFAULT_ROLLUPS: &str = "3600s@2592000s,86400s@31536000s,2592000s@0";

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageStats {
    pub module: String,
    pub raw_retention_seconds: u64,
    pub rollup_tiers: Option<String>,
    pub series: i64,
    pub chunks: i64,
    pub raw_tier_chunks: i64,
    pub rollup_chunks: i64,
    pub disk_points: i64,
    pub buffered_points: i64,
    pub total_points: i64,
    pub bytes_on_disk: i64,
    pub bytes_per_point: f64,
    pub buffer_memory_bytes: i64,
    pub oldest_timestamp_seconds: Option<i64>,
    pub newest_timestamp_seconds: Option<i64>,

    // These are deliberately named by unit. Row counts are not byte sizes.
    pub series_index_entries: i64,
    pub raw_chunk_index_entries: i64,
    pub rollup_index_entries: i64,
    pub sqlite_index_bytes: i64,
    pub database_file_bytes: u64,
    pub database_wal_bytes: u64,
    pub database_shm_bytes: u64,
    pub physical_database_bytes: u64,
    pub sqlite_page_bytes: i64,
    pub freelist_pages: i64,
    pub freelist_bytes: i64,

    pub writer_connections: usize,
    pub reader_connections: usize,
    pub command_queue_capacity_batches: usize,
    pub admitted_batches: u64,
    pub admitted_points: u64,
    pub completed_batches: u64,
    pub completed_points: u64,
    pub failed_batches: u64,
    pub failed_points: u64,
    pub queued_batches: u64,
    pub queued_points: u64,
    pub in_flight_batches: u64,
    pub in_flight_points: u64,
    pub oldest_queued_ms: u64,
    pub queued_unknown_point_batches: u64,
    pub admitted_body_bytes: u64,
    pub completed_body_bytes: u64,
    pub queued_body_bytes: u64,
    pub in_flight_body_bytes: u64,
    pub import_errors: u64,

    pub api_ingest_requests: u64,
    pub api_victoria_requests: u64,
    pub api_prometheus_requests: u64,
    pub api_parse_ns: u64,
    pub api_batch_encode_ns: u64,
    pub api_admission_wait_ns: u64,
    pub api_queue_wait_ns: u64,
    pub api_queue_wait_max_ns: u64,
    pub api_sqlite_insert_ns: u64,
    pub api_stats_count: u64,
    pub api_stats_total_ns: u64,
    pub api_stats_sqlite_ns: u64,
    pub api_stats_retries: u64,
    pub api_read_requests: u64,
    pub api_latest_requests: u64,
    pub api_export_requests: u64,
    pub api_range_requests: u64,
    pub api_discovery_requests: u64,
    pub api_promql_requests: u64,
    pub api_read_in_flight: u64,
    pub api_read_cancelled: u64,
    pub api_read_total_ns: u64,
    pub api_read_errors: u64,
    pub api_read_frame_bytes: u64,
    pub api_read_response_bytes: u64,
    pub api_read_result_series: u64,
    pub api_read_result_points: u64,
    pub api_flush_count: u64,
    pub api_flush_total_ns: u64,
    pub api_flush_sqlite_ns: u64,
    pub api_flush_errors: u64,
    pub scheduled_flush_count: u64,
    pub scheduled_flush_total_ns: u64,
    pub scheduled_flush_errors: u64,
    pub compact_count: u64,
    pub compact_total_ns: u64,
    pub compact_errors: u64,
    pub prune_count: u64,
    pub prune_total_ns: u64,
    pub prune_errors: u64,
    pub extension_prometheus_ingest_batches: i64,
    pub extension_prometheus_ingest_points: i64,
    pub extension_prometheus_ingest_errors: i64,
    pub extension_prometheus_ingest_total_ns: i64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct FlushReport {
    pub status: String,
    pub through_batches: u64,
    pub through_points: u64,
    pub completed_batches: u64,
    pub completed_points: u64,
    pub failed_batches: u64,
    pub failed_points: u64,
    pub queued_batches: u64,
    pub queued_points: u64,
    pub in_flight_batches: u64,
    pub in_flight_points: u64,
    pub flush_sqlite_ns: u64,
    pub api_request_ns: u64,
}

#[derive(Default)]
struct ApiProfile {
    pending: VecDeque<PendingBatch>,
    in_flight_batches: u64,
    in_flight_points: u64,
    in_flight_body_bytes: u64,
    admitted_batches: u64,
    admitted_points: u64,
    admitted_body_bytes: u64,
    completed_batches: u64,
    completed_points: u64,
    completed_body_bytes: u64,
    failed_batches: u64,
    failed_points: u64,
    import_errors: u64,
    ingest_requests: u64,
    victoria_requests: u64,
    prometheus_requests: u64,
    parse_ns: u64,
    batch_encode_ns: u64,
    admission_wait_ns: u64,
    queue_wait_ns: u64,
    queue_wait_max_ns: u64,
    sqlite_insert_ns: u64,
    stats_count: u64,
    stats_total_ns: u64,
    stats_sqlite_ns: u64,
    stats_retries: u64,
    read_requests: u64,
    latest_requests: u64,
    export_requests: u64,
    range_requests: u64,
    discovery_requests: u64,
    promql_requests: u64,
    read_in_flight: u64,
    read_cancelled: u64,
    read_total_ns: u64,
    read_errors: u64,
    read_frame_bytes: u64,
    read_response_bytes: u64,
    read_result_series: u64,
    read_result_points: u64,
    explicit_flush_count: u64,
    explicit_flush_total_ns: u64,
    explicit_flush_sqlite_ns: u64,
    explicit_flush_errors: u64,
    scheduled_flush_count: u64,
    scheduled_flush_total_ns: u64,
    scheduled_flush_errors: u64,
    compact_count: u64,
    compact_total_ns: u64,
    compact_errors: u64,
    prune_count: u64,
    prune_total_ns: u64,
    prune_errors: u64,
    last_error: Option<String>,
}

struct PendingBatch {
    queued_at: Instant,
    points: Option<usize>,
    body_bytes: usize,
}

#[derive(Clone, Copy)]
enum IngestFormat {
    Internal,
    Victoria,
    Prometheus,
}

struct Admission {
    points: Option<usize>,
    body_bytes: usize,
    import_errors: usize,
    parse_duration: Duration,
    encode_duration: Duration,
    format: IngestFormat,
}

#[derive(Clone, Copy)]
struct IngestOutcome {
    points: usize,
    import_errors: usize,
}

enum WriteCommand {
    IngestNamed {
        blob: Vec<u8>,
        points: usize,
        body_bytes: usize,
    },
    IngestPrometheus {
        body: Bytes,
        body_bytes: usize,
    },
    Barrier(oneshot::Sender<Result<(), String>>),
    Flush {
        through_batches: u64,
        explicit: bool,
        reply: oneshot::Sender<Result<FlushReport, String>>,
    },
    Compact(oneshot::Sender<Result<(), String>>),
    Prune {
        cutoff_seconds: i64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Shutdown(oneshot::Sender<Result<(), String>>),
}

enum ReadCommand {
    Stats(oneshot::Sender<Result<(StorageStats, u64, u64), String>>),
    Query {
        request: ReadRequest,
        cancelled: Arc<AtomicBool>,
        reply: oneshot::Sender<Result<ReadOutput, String>>,
    },
    Shutdown,
}

struct StorageInner {
    writer: mpsc::Sender<WriteCommand>,
    readers: Vec<mpsc::Sender<ReadCommand>>,
    next_reader: AtomicUsize,
    profile: Arc<StdMutex<ApiProfile>>,
    admission: Mutex<()>,
    joins: Mutex<Vec<JoinHandle<Result<(), String>>>>,
    lease: StdMutex<Option<File>>,
    database_path: PathBuf,
    raw_retention: Duration,
    queue_capacity: usize,
    shutting_down: AtomicBool,
}

#[derive(Clone)]
pub struct Storage(Arc<StorageInner>);

impl Storage {
    pub fn is_ready(&self) -> bool {
        !self.0.shutting_down.load(Ordering::Acquire)
    }

    pub fn start(
        database_path: PathBuf,
        extension_path: PathBuf,
        reader_connections: usize,
        queue_batches: usize,
        raw_retention: Duration,
    ) -> Result<Self, String> {
        if reader_connections == 0 {
            return Err("reader_connections must be positive".into());
        }
        if queue_batches == 0 {
            return Err("command_queue_batches must be positive".into());
        }
        if raw_retention.is_zero() {
            return Err("raw_retention must be positive".into());
        }
        if let Some(parent) = database_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!("create database directory {}: {error}", parent.display())
            })?;
        }
        let lease = acquire_database_lease(&database_path, "metrics")?;
        let profile = Arc::new(StdMutex::new(ApiProfile::default()));

        let (writer_tx, writer_rx) = mpsc::channel(queue_batches);
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let writer_db = database_path.clone();
        let writer_ext = extension_path.clone();
        let writer_profile = Arc::clone(&profile);
        let writer_join = thread::Builder::new()
            .name("timeless-metrics-writer".into())
            .spawn(move || writer_main(writer_db, writer_ext, writer_rx, ready_tx, writer_profile))
            .map_err(|error| format!("spawn SQLite writer: {error}"))?;
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(writer_tx);
                let _ = writer_join.join();
                return Err(error);
            }
            Err(_) => {
                drop(writer_tx);
                let _ = writer_join.join();
                return Err("SQLite writer exited during startup".into());
            }
        }

        let mut readers = Vec::with_capacity(reader_connections);
        let mut joins = vec![writer_join];
        for number in 0..reader_connections {
            let (reader_tx, reader_rx) = mpsc::channel(queue_batches);
            let (ready_tx, ready_rx) = std_mpsc::channel();
            let reader_db = database_path.clone();
            let reader_ext = extension_path.clone();
            let join = thread::Builder::new()
                .name(format!("timeless-metrics-reader-{number}"))
                .spawn(move || reader_main(reader_db, reader_ext, reader_rx, ready_tx))
                .map_err(|error| format!("spawn SQLite reader {number}: {error}"))?;
            match ready_rx.recv() {
                Ok(Ok(())) => {
                    readers.push(reader_tx);
                    joins.push(join);
                }
                Ok(Err(error)) => {
                    drop(reader_tx);
                    let _ = join.join();
                    drop(readers);
                    drop(writer_tx);
                    for join in joins {
                        let _ = join.join();
                    }
                    return Err(error);
                }
                Err(_) => {
                    drop(reader_tx);
                    let _ = join.join();
                    drop(readers);
                    drop(writer_tx);
                    for join in joins {
                        let _ = join.join();
                    }
                    return Err(format!("SQLite reader {number} exited during startup"));
                }
            }
        }

        Ok(Self(Arc::new(StorageInner {
            writer: writer_tx,
            readers,
            next_reader: AtomicUsize::new(0),
            profile,
            admission: Mutex::new(()),
            joins: Mutex::new(joins),
            lease: StdMutex::new(Some(lease)),
            database_path,
            raw_retention,
            queue_capacity: queue_batches,
            shutting_down: AtomicBool::new(false),
        })))
    }

    /// Internal contract seam used by extension-backed tests. Production HTTP
    /// named batches enter through submit_victoria_batch so request telemetry
    /// remains complete.
    pub async fn submit_named_batch(&self, blob: Vec<u8>, points: usize) -> Result<(), String> {
        let body_bytes = blob.len();
        self.admit_ingest(
            WriteCommand::IngestNamed {
                blob,
                points,
                body_bytes,
            },
            Admission {
                points: Some(points),
                body_bytes,
                import_errors: 0,
                parse_duration: Duration::ZERO,
                encode_duration: Duration::ZERO,
                format: IngestFormat::Internal,
            },
        )
        .await
    }

    pub async fn submit_victoria_batch(
        &self,
        blob: Vec<u8>,
        points: usize,
        import_errors: usize,
        body_bytes: usize,
        parse_duration: Duration,
        encode_duration: Duration,
    ) -> Result<(), String> {
        self.admit_ingest(
            WriteCommand::IngestNamed {
                blob,
                points,
                body_bytes,
            },
            Admission {
                points: Some(points),
                body_bytes,
                import_errors,
                parse_duration,
                encode_duration,
                format: IngestFormat::Victoria,
            },
        )
        .await
    }

    pub async fn submit_prometheus(&self, body: Bytes) -> Result<(), String> {
        let body_bytes = body.len();
        self.admit_ingest(
            WriteCommand::IngestPrometheus { body, body_bytes },
            Admission {
                points: None,
                body_bytes,
                import_errors: 0,
                parse_duration: Duration::ZERO,
                encode_duration: Duration::ZERO,
                format: IngestFormat::Prometheus,
            },
        )
        .await
    }

    async fn admit_ingest(
        &self,
        command: WriteCommand,
        admission: Admission,
    ) -> Result<(), String> {
        let admission_started = Instant::now();
        let _ordered = self.0.admission.lock().await;
        let permit = self
            .0
            .writer
            .reserve()
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        let admission_ns = elapsed_ns(admission_started);
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.pending.push_back(PendingBatch {
                queued_at: Instant::now(),
                points: admission.points,
                body_bytes: admission.body_bytes,
            });
            profile.admitted_batches = profile.admitted_batches.saturating_add(1);
            profile.admitted_points = profile
                .admitted_points
                .saturating_add(admission.points.unwrap_or(0) as u64);
            profile.admitted_body_bytes = profile
                .admitted_body_bytes
                .saturating_add(admission.body_bytes as u64);
            profile.import_errors = profile
                .import_errors
                .saturating_add(admission.import_errors as u64);
            profile.parse_ns = profile
                .parse_ns
                .saturating_add(duration_ns(admission.parse_duration));
            profile.batch_encode_ns = profile
                .batch_encode_ns
                .saturating_add(duration_ns(admission.encode_duration));
            profile.admission_wait_ns = profile.admission_wait_ns.saturating_add(admission_ns);
            match admission.format {
                IngestFormat::Internal => {}
                IngestFormat::Victoria => {
                    profile.ingest_requests = profile.ingest_requests.saturating_add(1);
                    profile.victoria_requests = profile.victoria_requests.saturating_add(1);
                }
                IngestFormat::Prometheus => {
                    profile.ingest_requests = profile.ingest_requests.saturating_add(1);
                    profile.prometheus_requests = profile.prometheus_requests.saturating_add(1);
                }
            }
        }
        permit.send(command);
        Ok(())
    }

    /// Proves that every previously admitted batch completed its SQLite
    /// statement. It does not flush extension buffers or alter durability.
    pub async fn barrier(&self) -> Result<(), String> {
        let _ordered = self.0.admission.lock().await;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Barrier(reply_tx))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        drop(_ordered);
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before barrier".to_string())?
    }

    pub async fn flush(&self) -> Result<FlushReport, String> {
        self.flush_ordered(true).await
    }

    pub async fn schedule_flush(&self) -> Result<(), String> {
        self.flush_ordered(false).await.map(|_| ())
    }

    async fn flush_ordered(&self, explicit: bool) -> Result<FlushReport, String> {
        let total_started = Instant::now();
        let _ordered = self.0.admission.lock().await;
        let through_batches = {
            let profile = profile_lock(&self.0.profile);
            profile.admitted_batches
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Flush {
                through_batches,
                explicit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        drop(_ordered);
        let result = reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before flush completed".to_string())?;
        if explicit {
            let mut profile = profile_lock(&self.0.profile);
            profile.explicit_flush_count = profile.explicit_flush_count.saturating_add(1);
            profile.explicit_flush_total_ns = profile
                .explicit_flush_total_ns
                .saturating_add(elapsed_ns(total_started));
            if result.is_err() {
                profile.explicit_flush_errors = profile.explicit_flush_errors.saturating_add(1);
            }
        }
        result
    }

    pub async fn schedule_compact(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Compact(reply_tx))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before compact completed".to_string())?
    }

    pub async fn schedule_retention(&self) -> Result<(), String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time precedes Unix epoch: {error}"))?;
        let cutoff = now.as_secs().saturating_sub(self.0.raw_retention.as_secs());
        let cutoff_seconds = i64::try_from(cutoff).unwrap_or(i64::MAX);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Prune {
                cutoff_seconds,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before prune completed".to_string())?
    }

    pub async fn stats(&self) -> Result<StorageStats, String> {
        let total_started = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader()
            .send(ReadCommand::Stats(reply_tx))
            .await
            .map_err(|_| "SQLite reader is not running".to_string())?;
        let (mut stats, sqlite_ns, retries) = reply_rx
            .await
            .map_err(|_| "SQLite reader stopped before stats completed".to_string())??;
        let total_ns = elapsed_ns(total_started);
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.stats_count = profile.stats_count.saturating_add(1);
            profile.stats_total_ns = profile.stats_total_ns.saturating_add(total_ns);
            profile.stats_sqlite_ns = profile.stats_sqlite_ns.saturating_add(sqlite_ns);
            profile.stats_retries = profile.stats_retries.saturating_add(retries);
            apply_profile(&mut stats, &profile);
        }
        stats.raw_retention_seconds = self.0.raw_retention.as_secs();
        stats.writer_connections = 1;
        stats.reader_connections = self.0.readers.len();
        stats.command_queue_capacity_batches = self.0.queue_capacity;
        let (file, wal, shm) = database_file_sizes(&self.0.database_path);
        stats.database_file_bytes = file;
        stats.database_wal_bytes = wal;
        stats.database_shm_bytes = shm;
        stats.physical_database_bytes = file.saturating_add(wal).saturating_add(shm);
        Ok(stats)
    }

    pub(crate) async fn read(&self, request: ReadRequest) -> Result<ReadOutput, String> {
        let started = Instant::now();
        let kind = request.kind();
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.read_requests = profile.read_requests.saturating_add(1);
            profile.read_in_flight = profile.read_in_flight.saturating_add(1);
            match kind {
                ReadKind::Latest => {
                    profile.latest_requests = profile.latest_requests.saturating_add(1)
                }
                ReadKind::Export => {
                    profile.export_requests = profile.export_requests.saturating_add(1)
                }
                ReadKind::Range => {
                    profile.range_requests = profile.range_requests.saturating_add(1)
                }
                ReadKind::Discovery => {
                    profile.discovery_requests = profile.discovery_requests.saturating_add(1)
                }
                ReadKind::Promql => {
                    profile.promql_requests = profile.promql_requests.saturating_add(1)
                }
            }
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut cancellation = ReadCancellation {
            cancelled: Arc::clone(&cancelled),
            profile: Arc::clone(&self.0.profile),
            started,
            armed: true,
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .reader()
            .send(ReadCommand::Query {
                request,
                cancelled,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            cancellation.disarm();
            let error = "SQLite reader is not running".to_string();
            record_read_completion(&self.0.profile, started, &Err(error.clone()));
            return Err(error);
        }
        let result = reply_rx
            .await
            .unwrap_or_else(|_| Err("SQLite reader stopped before query completed".to_string()));
        cancellation.disarm();
        record_read_completion(&self.0.profile, started, &result);
        result
    }

    fn reader(&self) -> &mpsc::Sender<ReadCommand> {
        let number = self.0.next_reader.fetch_add(1, Ordering::Relaxed);
        &self.0.readers[number % self.0.readers.len()]
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        if self.0.shutting_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        for reader in &self.0.readers {
            let _ = reader.send(ReadCommand::Shutdown).await;
        }
        let _ordered = self.0.admission.lock().await;
        let (reply_tx, reply_rx) = oneshot::channel();
        let writer_result = match self.0.writer.send(WriteCommand::Shutdown(reply_tx)).await {
            Ok(()) => reply_rx
                .await
                .map_err(|_| "SQLite writer stopped during shutdown".to_string())?,
            Err(_) => Err("SQLite writer is not running".into()),
        };
        drop(_ordered);
        let joins = {
            let mut guard = self.0.joins.lock().await;
            std::mem::take(&mut *guard)
        };
        for join in joins {
            join.join()
                .map_err(|_| "SQLite API worker panicked".to_string())??;
        }
        if let Some(file) = profile_lock(&self.0.lease).take() {
            FileExt::unlock(&file)
                .map_err(|error| format!("release database owner lease: {error}"))?;
        }
        writer_result
    }
}

struct ReadCancellation {
    cancelled: Arc<AtomicBool>,
    profile: Arc<StdMutex<ApiProfile>>,
    started: Instant,
    armed: bool,
}

impl ReadCancellation {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReadCancellation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
        let mut profile = profile_lock(&self.profile);
        profile.read_in_flight = profile.read_in_flight.saturating_sub(1);
        profile.read_cancelled = profile.read_cancelled.saturating_add(1);
        profile.read_total_ns = profile
            .read_total_ns
            .saturating_add(elapsed_ns(self.started));
    }
}

fn record_read_completion(
    profile: &StdMutex<ApiProfile>,
    started: Instant,
    result: &Result<ReadOutput, String>,
) {
    let mut profile = profile_lock(profile);
    profile.read_in_flight = profile.read_in_flight.saturating_sub(1);
    profile.read_total_ns = profile.read_total_ns.saturating_add(elapsed_ns(started));
    match result {
        Ok(output) => {
            profile.read_frame_bytes = profile
                .read_frame_bytes
                .saturating_add(output.frame_bytes as u64);
            profile.read_response_bytes = profile
                .read_response_bytes
                .saturating_add(output.body.len() as u64);
            profile.read_result_series = profile.read_result_series.saturating_add(output.series);
            profile.read_result_points = profile.read_result_points.saturating_add(output.points);
        }
        Err(error) => {
            profile.read_errors = profile.read_errors.saturating_add(1);
            profile.last_error = Some(error.clone());
        }
    }
}

fn writer_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<WriteCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
    profile: Arc<StdMutex<ApiProfile>>,
) -> Result<(), String> {
    let conn = match open_connection(&database_path, &extension_path, true) {
        Ok(conn) => {
            let _ = ready.send(Ok(()));
            conn
        }
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let mut unreported_error: Option<String> = None;
    while let Some(command) = commands.blocking_recv() {
        match command {
            WriteCommand::IngestNamed {
                blob,
                points,
                body_bytes,
            } => {
                record_queue_start(&profile, Some(points), body_bytes);
                let started = Instant::now();
                let result = insert_named_batch(&conn, &blob, points).map(|()| IngestOutcome {
                    points,
                    import_errors: 0,
                });
                let insert_ns = elapsed_ns(started);
                record_queue_completion(&profile, Some(points), body_bytes, insert_ns, &result);
                if let Err(error) = result {
                    unreported_error = Some(error);
                }
            }
            WriteCommand::IngestPrometheus { body, body_bytes } => {
                record_queue_start(&profile, None, body_bytes);
                let started = Instant::now();
                let result = insert_prometheus_body(&conn, body.as_ref());
                let insert_ns = elapsed_ns(started);
                record_queue_completion(&profile, None, body_bytes, insert_ns, &result);
                if let Err(error) = result {
                    unreported_error = Some(error);
                }
            }
            WriteCommand::Barrier(reply) => {
                let result = unreported_error.take().map_or(Ok(()), Err);
                let _ = reply.send(result);
            }
            WriteCommand::Flush {
                through_batches,
                explicit,
                reply,
            } => {
                let started = Instant::now();
                let flush_result = run_command(&conn, "flush", "flush metrics");
                let flush_ns = elapsed_ns(started);
                {
                    let mut api = profile_lock(&profile);
                    if explicit {
                        api.explicit_flush_sqlite_ns =
                            api.explicit_flush_sqlite_ns.saturating_add(flush_ns);
                    } else {
                        api.scheduled_flush_count = api.scheduled_flush_count.saturating_add(1);
                        api.scheduled_flush_total_ns =
                            api.scheduled_flush_total_ns.saturating_add(flush_ns);
                        if flush_result.is_err() {
                            api.scheduled_flush_errors =
                                api.scheduled_flush_errors.saturating_add(1);
                        }
                    }
                    if let Err(error) = &flush_result {
                        api.last_error = Some(error.clone());
                    }
                }
                let prior_error = unreported_error.take();
                let result = match (prior_error, flush_result) {
                    (Some(error), _) | (None, Err(error)) => Err(error),
                    (None, Ok(())) => {
                        let api = profile_lock(&profile);
                        Ok(flush_report(&api, through_batches, flush_ns))
                    }
                };
                let _ = reply.send(result);
            }
            WriteCommand::Compact(reply) => {
                let started = Instant::now();
                let result = run_command(&conn, "compact", "compact and roll up metrics");
                record_maintenance(&profile, Maintenance::Compact, started.elapsed(), &result);
                let _ = reply.send(result);
            }
            WriteCommand::Prune {
                cutoff_seconds,
                reply,
            } => {
                let started = Instant::now();
                let result = run_command(
                    &conn,
                    &format!("prune:{cutoff_seconds}"),
                    "prune raw metrics",
                );
                record_maintenance(&profile, Maintenance::Prune, started.elapsed(), &result);
                let _ = reply.send(result);
            }
            WriteCommand::Shutdown(reply) => {
                let result = run_command(&conn, "flush", "graceful metrics flush");
                let _ = reply.send(result.clone());
                return result;
            }
        }
    }
    // A dropped API still makes its admitted buffered points durable.
    run_command(
        &conn,
        "flush",
        "final metrics flush after writer disconnect",
    )
}

fn reader_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<ReadCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
) -> Result<(), String> {
    let conn = match open_connection(&database_path, &extension_path, false) {
        Ok(conn) => conn,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let features = match QueryFeatures::discover(&conn) {
        Ok(features) => {
            let _ = ready.send(Ok(()));
            features
        }
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    while let Some(command) = commands.blocking_recv() {
        match command {
            ReadCommand::Stats(reply) => {
                let started = Instant::now();
                let mut retries = 0_u64;
                let result = retry_read(
                    || storage_stats(&conn),
                    || retries = retries.saturating_add(1),
                )
                .map(|stats| (stats, elapsed_ns(started), retries));
                let _ = reply.send(result);
            }
            ReadCommand::Query {
                request,
                cancelled,
                reply,
            } => {
                let progress_cancelled = Arc::clone(&cancelled);
                let result = conn
                    .progress_handler(
                        1_000,
                        Some(move || progress_cancelled.load(Ordering::Acquire)),
                    )
                    .map_err(|error| format!("install query cancellation handler: {error}"))
                    .and_then(|()| {
                        retry_read(
                            || query::execute(&conn, features, request.clone(), &cancelled),
                            || {},
                        )
                    });
                let cleared = conn.progress_handler(0, None::<fn() -> bool>);
                let result = match (result, cleared) {
                    (Ok(output), Ok(())) => Ok(output),
                    (Err(error), _) => Err(error),
                    (Ok(_), Err(error)) => {
                        Err(format!("clear query cancellation handler: {error}"))
                    }
                };
                let _ = reply.send(result);
            }
            ReadCommand::Shutdown => return Ok(()),
        }
    }
    Ok(())
}

fn open_connection(path: &Path, extension: &Path, initialize: bool) -> Result<Connection, String> {
    let conn =
        Connection::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
    unsafe {
        conn.load_extension_enable()
            .map_err(|error| format!("enable extension loading: {error}"))?;
        conn.load_extension(extension, None::<&str>)
            .map_err(|error| format!("load {}: {error}", extension.display()))?;
    }
    conn.load_extension_disable()
        .map_err(|error| format!("disable extension loading: {error}"))?;
    let spec = DataPlaneSpec {
        signal: "metrics",
        required_batch: "named-v0",
    };
    let capabilities = preflight_extension(&conn, spec)?;
    preflight_database(&conn, spec.signal)?;
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(|error| format!("set busy timeout: {error}"))?;
    if initialize {
        conn.execute_batch(&format!(
            "PRAGMA page_size = 16384;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -128000;
             PRAGMA auto_vacuum = INCREMENTAL;
             PRAGMA mmap_size = 2147483648;
             PRAGMA wal_autocheckpoint = 10000;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;
             CREATE VIRTUAL TABLE IF NOT EXISTS metrics USING timeless_metrics(
               rollups='{DEFAULT_ROLLUPS}');"
        ))
        .map_err(|error| format!("initialize metrics database: {error}"))?;
        apply_schema_ledger(&conn, spec, &capabilities)?;
    } else {
        require_current_schema(&conn, spec.signal)?;
        conn.execute_batch(
            "PRAGMA cache_size = -8000;
             PRAGMA mmap_size = 2147483648;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(|error| format!("configure metrics reader: {error}"))?;
        // Force xConnect during startup rather than returning a superficially
        // ready reader whose first production request discovers incompatibility.
        conn.prepare("SELECT name FROM metrics LIMIT 0")
            .map_err(|error| format!("connect metrics virtual table: {error}"))?;
    }
    Ok(conn)
}

fn insert_named_batch(conn: &Connection, blob: &[u8], points: usize) -> Result<(), String> {
    let expected = i64::try_from(points)
        .map_err(|_| "metrics named batch point count exceeds i64::MAX".to_string())?;
    conn.execute("INSERT INTO metrics(metrics) VALUES (?1)", params![blob])
        .map_err(|error| format!("insert metrics named batch: {error}"))?;
    let inserted = conn.last_insert_rowid();
    if inserted != expected {
        return Err(format!(
            "metrics extension accepted {inserted} points; API batch declared {points}"
        ));
    }
    Ok(())
}

fn insert_prometheus_body(conn: &Connection, body: &[u8]) -> Result<IngestOutcome, String> {
    if body.is_empty() {
        return Ok(IngestOutcome {
            points: 0,
            import_errors: 0,
        });
    }
    // The hidden-column public protocol reserves these bytes for batch
    // versions. A Prometheus HTTP route must not let an arbitrary request
    // switch protocols; valid exposition can never start with them.
    if matches!(body.first(), Some(0x00..=0x08)) {
        return Ok(IngestOutcome {
            points: 0,
            import_errors: 1,
        });
    }

    let execution = conn
        .execute("INSERT INTO metrics(metrics) VALUES (?1)", params![body])
        .map_err(|error| format!("insert Prometheus exposition: {error}"));

    match execution {
        Ok(_) => {
            let inserted = conn.last_insert_rowid();
            let points = usize::try_from(inserted).map_err(|_| {
                format!("Prometheus extension returned invalid point count {inserted}")
            })?;
            Ok(IngestOutcome {
                points,
                // The extension owns Prometheus parsing and exposes its
                // cumulative rejected-line count through timeless_stats.
                // Reading that TVF around every write is observability work
                // on the hot path, so StorageStats combines it later.
                import_errors: 0,
            })
        }
        Err(error) if error.contains("prometheus body: 0 samples ingested") => {
            // Preserve the established asynchronous HTTP contract: an
            // all-malformed request is completed with rejected-line telemetry,
            // not converted into a synchronous HTTP/storage failure.
            Ok(IngestOutcome {
                points: 0,
                import_errors: 0,
            })
        }
        Err(error) => Err(error),
    }
}

fn run_command(conn: &Connection, command: &str, context: &str) -> Result<(), String> {
    conn.execute("INSERT INTO metrics(metrics) VALUES (?1)", [command])
        .map(|_| ())
        .map_err(|error| format!("{context}: {error}"))
}

fn storage_stats(conn: &Connection) -> Result<StorageStats, String> {
    let values = stat_values(conn)?;
    let integer = |key: &str| match values.get(key) {
        Some(SqlValue::Integer(value)) => *value,
        Some(SqlValue::Real(value)) => *value as i64,
        _ => 0,
    };
    let real = |key: &str| match values.get(key) {
        Some(SqlValue::Real(value)) => *value,
        Some(SqlValue::Integer(value)) => *value as f64,
        _ => 0.0,
    };
    let text = |key: &str| match values.get(key) {
        Some(SqlValue::Text(value)) => Some(value.clone()),
        _ => None,
    };
    let (raw_tier_chunks, raw_points, rollup_entries): (i64, i64, i64) = conn
        .query_row(
            "SELECT COALESCE(SUM(resolution = 0), 0),
                    COALESCE(SUM(CASE WHEN resolution = 0 THEN point_count ELSE 0 END), 0),
                    COALESCE(SUM(resolution > 0), 0)
               FROM metrics_chunks",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| format!("read metrics chunk units: {error}"))?;
    let series_index_entries: i64 = conn
        .query_row("SELECT COUNT(*) FROM metrics_series", [], |row| row.get(0))
        .map_err(|error| format!("read metrics series index entries: {error}"))?;
    let (page_count, page_size, freelist_pages): (i64, i64, i64) = conn
        .query_row(
            "SELECT (SELECT page_count FROM pragma_page_count),
                    (SELECT page_size FROM pragma_page_size),
                    (SELECT freelist_count FROM pragma_freelist_count)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| format!("read SQLite page accounting: {error}"))?;
    let sqlite_index_bytes: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(pgsize), 0)
               FROM dbstat
              WHERE name IN (
                'metrics_chunks_series_ts',
                'sqlite_autoindex_metrics_series_1',
                'sqlite_autoindex_metrics_meta_1'
              )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("read metrics SQLite index bytes: {error}"))?;
    let disk_points = integer("disk_points").max(raw_points);
    let buffered_points = integer("buffered_points");
    Ok(StorageStats {
        module: text("module").unwrap_or_else(|| "metrics".into()),
        rollup_tiers: text("rollup_tiers"),
        series: integer("series"),
        chunks: raw_tier_chunks.saturating_add(rollup_entries),
        raw_tier_chunks,
        rollup_chunks: integer("rollup_chunks").max(rollup_entries),
        disk_points,
        buffered_points,
        total_points: disk_points.saturating_add(buffered_points),
        bytes_on_disk: integer("bytes_on_disk"),
        bytes_per_point: real("bytes_per_point"),
        buffer_memory_bytes: integer("buffer_memory"),
        oldest_timestamp_seconds: optional_integer(values.get("ts_min")),
        newest_timestamp_seconds: optional_integer(values.get("ts_max")),
        series_index_entries,
        raw_chunk_index_entries: raw_tier_chunks,
        rollup_index_entries: rollup_entries,
        sqlite_index_bytes,
        sqlite_page_bytes: page_count.saturating_mul(page_size),
        freelist_pages,
        freelist_bytes: freelist_pages.saturating_mul(page_size),
        extension_prometheus_ingest_batches: integer("prometheus_ingest_batches"),
        extension_prometheus_ingest_points: integer("prometheus_ingest_points"),
        extension_prometheus_ingest_errors: integer("prometheus_ingest_errors"),
        extension_prometheus_ingest_total_ns: integer("prometheus_ingest_total_ns"),
        ..StorageStats::default()
    })
}

fn stat_values(conn: &Connection) -> Result<HashMap<String, SqlValue>, String> {
    let mut stmt = conn
        .prepare("SELECT key, value FROM timeless_stats('metrics')")
        .map_err(|error| format!("prepare timeless_stats: {error}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, SqlValue>(1)?))
        })
        .map_err(|error| format!("read timeless_stats: {error}"))?;
    let mut values = HashMap::new();
    for row in rows {
        let (key, value) = row.map_err(|error| format!("collect timeless_stats: {error}"))?;
        values.insert(key, value);
    }
    Ok(values)
}

fn optional_integer(value: Option<&SqlValue>) -> Option<i64> {
    match value {
        Some(SqlValue::Integer(value)) => Some(*value),
        Some(SqlValue::Real(value)) => Some(*value as i64),
        _ => None,
    }
}

fn apply_profile(stats: &mut StorageStats, profile: &ApiProfile) {
    stats.admitted_batches = profile.admitted_batches;
    stats.admitted_points = profile.admitted_points;
    stats.completed_batches = profile.completed_batches;
    stats.completed_points = profile.completed_points;
    stats.failed_batches = profile.failed_batches;
    stats.failed_points = profile.failed_points;
    stats.queued_batches = profile.pending.len() as u64;
    stats.queued_points = profile
        .pending
        .iter()
        .filter_map(|pending| pending.points)
        .map(|points| points as u64)
        .sum();
    stats.queued_unknown_point_batches = profile
        .pending
        .iter()
        .filter(|pending| pending.points.is_none())
        .count() as u64;
    stats.in_flight_batches = profile.in_flight_batches;
    stats.in_flight_points = profile.in_flight_points;
    stats.admitted_body_bytes = profile.admitted_body_bytes;
    stats.completed_body_bytes = profile.completed_body_bytes;
    stats.queued_body_bytes = profile
        .pending
        .iter()
        .map(|pending| pending.body_bytes as u64)
        .sum();
    stats.in_flight_body_bytes = profile.in_flight_body_bytes;
    stats.import_errors = profile
        .import_errors
        .saturating_add(stats.extension_prometheus_ingest_errors.max(0) as u64);
    stats.oldest_queued_ms = profile
        .pending
        .front()
        .map(|pending| duration_ms(pending.queued_at.elapsed()))
        .unwrap_or(0);
    stats.api_ingest_requests = profile.ingest_requests;
    stats.api_victoria_requests = profile.victoria_requests;
    stats.api_prometheus_requests = profile.prometheus_requests;
    stats.api_parse_ns = profile.parse_ns;
    stats.api_batch_encode_ns = profile.batch_encode_ns;
    stats.api_admission_wait_ns = profile.admission_wait_ns;
    stats.api_queue_wait_ns = profile.queue_wait_ns;
    stats.api_queue_wait_max_ns = profile.queue_wait_max_ns;
    stats.api_sqlite_insert_ns = profile.sqlite_insert_ns;
    stats.api_stats_count = profile.stats_count;
    stats.api_stats_total_ns = profile.stats_total_ns;
    stats.api_stats_sqlite_ns = profile.stats_sqlite_ns;
    stats.api_stats_retries = profile.stats_retries;
    stats.api_read_requests = profile.read_requests;
    stats.api_latest_requests = profile.latest_requests;
    stats.api_export_requests = profile.export_requests;
    stats.api_range_requests = profile.range_requests;
    stats.api_discovery_requests = profile.discovery_requests;
    stats.api_promql_requests = profile.promql_requests;
    stats.api_read_in_flight = profile.read_in_flight;
    stats.api_read_cancelled = profile.read_cancelled;
    stats.api_read_total_ns = profile.read_total_ns;
    stats.api_read_errors = profile.read_errors;
    stats.api_read_frame_bytes = profile.read_frame_bytes;
    stats.api_read_response_bytes = profile.read_response_bytes;
    stats.api_read_result_series = profile.read_result_series;
    stats.api_read_result_points = profile.read_result_points;
    stats.api_flush_count = profile.explicit_flush_count;
    stats.api_flush_total_ns = profile.explicit_flush_total_ns;
    stats.api_flush_sqlite_ns = profile.explicit_flush_sqlite_ns;
    stats.api_flush_errors = profile.explicit_flush_errors;
    stats.scheduled_flush_count = profile.scheduled_flush_count;
    stats.scheduled_flush_total_ns = profile.scheduled_flush_total_ns;
    stats.scheduled_flush_errors = profile.scheduled_flush_errors;
    stats.compact_count = profile.compact_count;
    stats.compact_total_ns = profile.compact_total_ns;
    stats.compact_errors = profile.compact_errors;
    stats.prune_count = profile.prune_count;
    stats.prune_total_ns = profile.prune_total_ns;
    stats.prune_errors = profile.prune_errors;
    stats.last_error = profile.last_error.clone();
}

fn flush_report(profile: &ApiProfile, through_batches: u64, flush_sqlite_ns: u64) -> FlushReport {
    FlushReport {
        status: "ok".into(),
        through_batches,
        through_points: profile.completed_points,
        completed_batches: profile.completed_batches,
        completed_points: profile.completed_points,
        failed_batches: profile.failed_batches,
        failed_points: profile.failed_points,
        queued_batches: profile.pending.len() as u64,
        queued_points: profile
            .pending
            .iter()
            .filter_map(|pending| pending.points)
            .map(|points| points as u64)
            .sum(),
        in_flight_batches: profile.in_flight_batches,
        in_flight_points: profile.in_flight_points,
        flush_sqlite_ns,
        api_request_ns: 0,
    }
}

fn record_queue_start(profile: &StdMutex<ApiProfile>, points: Option<usize>, body_bytes: usize) {
    let mut profile = profile_lock(profile);
    if let Some(pending) = profile.pending.pop_front() {
        debug_assert_eq!(pending.points, points);
        debug_assert_eq!(pending.body_bytes, body_bytes);
        let wait_ns = elapsed_ns(pending.queued_at);
        profile.queue_wait_ns = profile.queue_wait_ns.saturating_add(wait_ns);
        profile.queue_wait_max_ns = profile.queue_wait_max_ns.max(wait_ns);
        profile.in_flight_batches = profile.in_flight_batches.saturating_add(1);
        profile.in_flight_points = profile
            .in_flight_points
            .saturating_add(pending.points.unwrap_or(0) as u64);
        profile.in_flight_body_bytes = profile
            .in_flight_body_bytes
            .saturating_add(body_bytes as u64);
    }
}

fn record_queue_completion(
    profile: &StdMutex<ApiProfile>,
    declared_points: Option<usize>,
    body_bytes: usize,
    insert_ns: u64,
    result: &Result<IngestOutcome, String>,
) {
    let mut profile = profile_lock(profile);
    profile.in_flight_batches = profile.in_flight_batches.saturating_sub(1);
    profile.in_flight_points = profile
        .in_flight_points
        .saturating_sub(declared_points.unwrap_or(0) as u64);
    profile.in_flight_body_bytes = profile
        .in_flight_body_bytes
        .saturating_sub(body_bytes as u64);
    profile.sqlite_insert_ns = profile.sqlite_insert_ns.saturating_add(insert_ns);
    match result {
        Err(error) => {
            profile.failed_batches = profile.failed_batches.saturating_add(1);
            profile.failed_points = profile
                .failed_points
                .saturating_add(declared_points.unwrap_or(0) as u64);
            profile.last_error = Some(error.clone());
        }
        Ok(outcome) => {
            if declared_points.is_none() {
                profile.admitted_points = profile
                    .admitted_points
                    .saturating_add(outcome.points as u64);
            }
            profile.completed_batches = profile.completed_batches.saturating_add(1);
            profile.completed_points = profile
                .completed_points
                .saturating_add(outcome.points as u64);
            profile.completed_body_bytes = profile
                .completed_body_bytes
                .saturating_add(body_bytes as u64);
            profile.import_errors = profile
                .import_errors
                .saturating_add(outcome.import_errors as u64);
        }
    }
}

enum Maintenance {
    Compact,
    Prune,
}

fn record_maintenance(
    profile: &StdMutex<ApiProfile>,
    operation: Maintenance,
    duration: Duration,
    result: &Result<(), String>,
) {
    let mut profile = profile_lock(profile);
    let ns = duration_ns(duration);
    match operation {
        Maintenance::Compact => {
            profile.compact_count = profile.compact_count.saturating_add(1);
            profile.compact_total_ns = profile.compact_total_ns.saturating_add(ns);
            if result.is_err() {
                profile.compact_errors = profile.compact_errors.saturating_add(1);
            }
        }
        Maintenance::Prune => {
            profile.prune_count = profile.prune_count.saturating_add(1);
            profile.prune_total_ns = profile.prune_total_ns.saturating_add(ns);
            if result.is_err() {
                profile.prune_errors = profile.prune_errors.saturating_add(1);
            }
        }
    }
    if let Err(error) = result {
        profile.last_error = Some(error.clone());
    }
}

fn database_file_sizes(database_path: &Path) -> (u64, u64, u64) {
    (
        file_size(database_path),
        file_size(&suffix_path(database_path, "-wal")),
        file_size(&suffix_path(database_path, "-shm")),
    )
}

fn suffix_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn retry_read<T>(
    mut operation: impl FnMut() -> Result<T, String>,
    mut retried: impl FnMut(),
) -> Result<T, String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if Instant::now() < deadline && is_retryable_read(&error) => {
                retried();
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_retryable_read(error: &str) -> bool {
    error.contains("active write transaction")
        || error.contains("pending writer transaction")
        || error.contains("database is locked")
        || error.contains("database is busy")
}

fn profile_lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn elapsed_ns(started: Instant) -> u64 {
    duration_ns(started.elapsed())
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn owner_lease_rejects_a_second_server_for_the_same_database() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("metrics.db");
        let first = acquire_database_lease(&database, "metrics").unwrap();
        let error = acquire_database_lease(&database, "metrics").unwrap_err();
        assert!(error.contains("already owned"), "{error}");
        FileExt::unlock(&first).unwrap();
        acquire_database_lease(&database, "metrics").unwrap();
    }

    #[test]
    fn pending_writer_conflicts_are_retried() {
        let attempts = Cell::new(0_u64);
        let retries = Cell::new(0_u64);
        let value = retry_read(
            || {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    Err("metrics read is blocked by a pending writer transaction".into())
                } else {
                    Ok(42)
                }
            },
            || retries.set(retries.get() + 1),
        )
        .unwrap();
        assert_eq!(value, 42);
        assert_eq!(attempts.get(), 2);
        assert_eq!(retries.get(), 1);
    }

    #[test]
    fn suffixes_do_not_replace_the_database_extension() {
        let database = Path::new("metrics.db");
        assert_eq!(
            suffix_path(database, "-wal"),
            PathBuf::from("metrics.db-wal")
        );
        assert_eq!(
            suffix_path(database, ".timeless-metrics-api.lock"),
            PathBuf::from("metrics.db.timeless-metrics-api.lock")
        );
    }
}
