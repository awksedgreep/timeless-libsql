use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use fs2::FileExt;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection};
use serde::Serialize;
use timeless_api_common::{
    acquire_database_lease, apply_schema_ledger, checkpoint_wal, create_verified_backup,
    preflight_database, preflight_extension, require_current_schema, BackupReport, DataPlaneSpec,
};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::query::{self, ReadKind, ReadOutput, ReadRequest};

pub const TRACE_CAPABILITY: &str = "timeless_traces/rich-span-batch-v1";

const EXPECTED_COLUMNS: &[(&str, &str, i64)] = &[
    ("trace_id", "BLOB", 0),
    ("span_id", "BLOB", 0),
    ("parent_span_id", "BLOB", 0),
    ("name", "TEXT", 0),
    ("service", "TEXT", 0),
    ("kind", "TEXT", 0),
    ("status", "TEXT", 0),
    ("start_ts", "INTEGER", 0),
    ("duration_ns", "INTEGER", 0),
    ("attributes", "TEXT", 0),
    ("status_description", "TEXT", 0),
    ("events", "TEXT", 0),
    ("resource", "TEXT", 0),
    ("instrumentation_scope", "TEXT", 0),
    ("traces", "", 1),
];

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageStats {
    pub capability: String,
    pub module: String,
    pub retention_nanoseconds: Option<i64>,
    pub blocks: i64,
    pub raw_blocks: i64,
    pub compressed_blocks: i64,
    pub buffered_spans: i64,
    pub disk_spans: i64,
    pub total_spans: i64,
    pub bytes_on_disk: i64,
    pub terms: i64,
    pub trace_index_rows: i64,
    pub oldest_timestamp_nanoseconds: Option<i64>,
    pub newest_timestamp_nanoseconds: Option<i64>,
    pub extension_query_count: i64,
    pub extension_query_cancelled: i64,
    pub extension_query_total_ns: i64,
    pub extension_query_candidate_blocks: i64,
    pub extension_query_payload_blocks_read: i64,
    pub extension_query_payload_bytes_read: i64,
    pub extension_query_decoded_spans: i64,
    pub extension_query_buffered_spans_examined: i64,
    pub extension_query_matched_spans: i64,
    pub extension_query_returned_spans: i64,
    pub extension_query_snapshot_ns: i64,
    pub extension_query_snapshot_payload_bytes: i64,
    pub extension_query_snapshot_payload_max_bytes: i64,
    pub extension_query_stable_location_snapshots: i64,
    pub extension_query_bounded_count: i64,
    pub extension_query_bounded_requested_spans: i64,
    pub extension_query_bounded_max_spans: i64,
    pub extension_query_blocks_skipped_by_bound: i64,
    pub extension_discovery_count: i64,
    pub extension_discovery_total_ns: i64,
    pub extension_discovery_payload_bytes_read: i64,
    pub extension_discovery_decoded_spans: i64,
    pub extension_optimize_count: i64,
    pub extension_optimize_total_ns: i64,
    pub extension_optimize_blocks_removed: i64,
    pub extension_optimize_blocks_written: i64,
    pub extension_optimize_budgeted_count: i64,
    pub extension_optimize_budget_entries: i64,
    pub extension_optimize_budget_limited_count: i64,
    pub extension_optimize_raw_groups: i64,
    pub extension_optimize_raw_blocks: i64,
    pub extension_optimize_raw_entries: i64,
    pub extension_optimize_raw_input_bytes: i64,
    pub extension_optimize_raw_output_bytes: i64,
    pub extension_optimize_raw_total_ns: i64,
    pub extension_optimize_merge_groups: i64,
    pub extension_optimize_merge_blocks: i64,
    pub extension_optimize_merge_entries: i64,
    pub extension_optimize_merge_input_bytes: i64,
    pub extension_optimize_merge_output_bytes: i64,
    pub extension_optimize_merge_total_ns: i64,
    pub extension_optimize_pending_raw_blocks: i64,
    pub extension_optimize_pending_raw_entries: i64,
    pub extension_optimize_merge_ready_groups: i64,
    pub extension_optimize_merge_ready_blocks: i64,
    pub extension_optimize_merge_ready_entries: i64,
    pub extension_optimize_merge_deferred_blocks: i64,
    pub extension_optimize_merge_deferred_entries: i64,
    pub extension_read_permit_count: i64,
    pub extension_read_permit_hold_ns: i64,
    pub extension_read_conflicts: i64,
    pub extension_read_barge_rejections: i64,
    pub extension_waiting_writers: i64,
    pub extension_writer_wait_count: i64,
    pub extension_writer_wait_ns: i64,
    pub extension_writer_timeouts: i64,

    pub database_file_bytes: u64,
    pub database_wal_bytes: u64,
    pub database_shm_bytes: u64,
    pub physical_database_bytes: u64,
    pub sqlite_page_bytes: i64,
    pub sqlite_index_bytes: i64,
    pub freelist_pages: i64,
    pub freelist_bytes: i64,

    pub writer_connections: usize,
    pub reader_connections: usize,
    pub command_queue_capacity_requests: usize,
    pub admitted_requests: u64,
    pub admitted_spans: u64,
    pub admitted_body_bytes: u64,
    pub completed_requests: u64,
    pub completed_spans: u64,
    pub completed_body_bytes: u64,
    pub failed_requests: u64,
    pub failed_spans: u64,
    pub failed_body_bytes: u64,
    pub queued_requests: u64,
    pub queued_spans: u64,
    pub queued_body_bytes: u64,
    pub in_flight_requests: u64,
    pub in_flight_spans: u64,
    pub in_flight_body_bytes: u64,
    pub oldest_queued_ms: u64,

    pub api_admission_wait_ns: u64,
    pub api_queue_wait_ns: u64,
    pub api_queue_wait_max_ns: u64,
    pub api_ingest_requests: u64,
    pub api_rejected_requests: u64,
    pub api_rejected_spans: u64,
    pub api_rejected_body_bytes: u64,
    pub api_parse_ns: u64,
    pub api_wire_decode_ns: u64,
    pub api_batch_encode_ns: u64,
    pub api_decompressed_body_bytes: u64,
    pub api_sqlite_insert_ns: u64,
    pub api_sqlite_transaction_ns: u64,
    pub api_stats_count: u64,
    pub api_stats_total_ns: u64,
    pub api_stats_sqlite_ns: u64,
    pub api_stats_retries: u64,
    pub api_read_requests: u64,
    pub api_services_requests: u64,
    pub api_operations_requests: u64,
    pub api_trace_requests: u64,
    pub api_search_requests: u64,
    pub api_read_in_flight: u64,
    pub api_read_cancelled: u64,
    pub api_read_total_ns: u64,
    pub api_read_sqlite_ns: u64,
    pub api_read_errors: u64,
    pub api_read_retries: u64,
    pub api_read_response_bytes: u64,
    pub api_read_result_traces: u64,
    pub api_read_result_spans: u64,
    pub api_flush_count: u64,
    pub api_flush_total_ns: u64,
    pub api_flush_sqlite_ns: u64,
    pub api_flush_errors: u64,
    pub scheduled_flush_count: u64,
    pub scheduled_flush_total_ns: u64,
    pub scheduled_flush_errors: u64,
    pub optimize_count: u64,
    pub optimize_total_ns: u64,
    pub optimize_errors: u64,
    pub checkpoint_count: u64,
    pub checkpoint_total_ns: u64,
    pub checkpoint_errors: u64,
    pub backup_count: u64,
    pub backup_total_ns: u64,
    pub backup_errors: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct FlushReport {
    pub status: String,
    pub through_requests: u64,
    pub through_spans: u64,
    pub through_body_bytes: u64,
    pub completed_requests: u64,
    pub completed_spans: u64,
    pub completed_body_bytes: u64,
    pub failed_requests: u64,
    pub failed_spans: u64,
    pub queued_requests: u64,
    pub queued_spans: u64,
    pub queued_body_bytes: u64,
    pub in_flight_requests: u64,
    pub in_flight_spans: u64,
    pub in_flight_body_bytes: u64,
    pub flush_sqlite_ns: u64,
    pub api_request_ns: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RuntimeWatermarks {
    pub admitted_requests: u64,
    pub admitted_spans: u64,
    pub admitted_body_bytes: u64,
    pub completed_requests: u64,
    pub completed_spans: u64,
    pub completed_body_bytes: u64,
    pub failed_requests: u64,
    pub failed_spans: u64,
    pub failed_body_bytes: u64,
    pub queued_requests: u64,
    pub queued_spans: u64,
    pub queued_body_bytes: u64,
    pub in_flight_requests: u64,
    pub in_flight_spans: u64,
    pub in_flight_body_bytes: u64,
    pub oldest_queued_ms: u64,
}

#[derive(Default)]
struct ApiProfile {
    pending: VecDeque<PendingRequest>,
    in_flight_requests: u64,
    in_flight_spans: u64,
    in_flight_body_bytes: u64,
    admitted_requests: u64,
    admitted_spans: u64,
    admitted_body_bytes: u64,
    completed_requests: u64,
    completed_spans: u64,
    completed_body_bytes: u64,
    failed_requests: u64,
    failed_spans: u64,
    failed_body_bytes: u64,
    admission_wait_ns: u64,
    queue_wait_ns: u64,
    queue_wait_max_ns: u64,
    ingest_requests: u64,
    rejected_requests: u64,
    rejected_spans: u64,
    rejected_body_bytes: u64,
    parse_ns: u64,
    wire_decode_ns: u64,
    batch_encode_ns: u64,
    decompressed_body_bytes: u64,
    sqlite_insert_ns: u64,
    stats_count: u64,
    stats_total_ns: u64,
    stats_sqlite_ns: u64,
    stats_retries: u64,
    read_requests: u64,
    services_requests: u64,
    operations_requests: u64,
    trace_requests: u64,
    search_requests: u64,
    read_in_flight: u64,
    read_cancelled: u64,
    read_total_ns: u64,
    read_sqlite_ns: u64,
    read_errors: u64,
    read_retries: u64,
    read_response_bytes: u64,
    read_result_traces: u64,
    read_result_spans: u64,
    explicit_flush_count: u64,
    explicit_flush_total_ns: u64,
    explicit_flush_sqlite_ns: u64,
    explicit_flush_errors: u64,
    scheduled_flush_count: u64,
    scheduled_flush_total_ns: u64,
    scheduled_flush_errors: u64,
    optimize_count: u64,
    optimize_total_ns: u64,
    optimize_errors: u64,
    checkpoint_count: u64,
    checkpoint_total_ns: u64,
    checkpoint_errors: u64,
    backup_count: u64,
    backup_total_ns: u64,
    backup_errors: u64,
    last_error: Option<String>,
}

struct PendingRequest {
    queued_at: Instant,
    spans: usize,
    body_bytes: usize,
}

enum WriteCommand {
    Ingest {
        blob: Vec<u8>,
        spans: usize,
        body_bytes: usize,
        reply: Option<oneshot::Sender<Result<(), String>>>,
    },
    Barrier(oneshot::Sender<Result<(), String>>),
    Flush {
        through_requests: u64,
        through_spans: u64,
        through_body_bytes: u64,
        explicit: bool,
        reply: oneshot::Sender<Result<FlushReport, String>>,
    },
    Optimize(oneshot::Sender<Result<(), String>>),
    Backup {
        destination: PathBuf,
        reply: oneshot::Sender<Result<BackupReport, String>>,
    },
    Shutdown(oneshot::Sender<Result<(), String>>),
}

enum ReadCommand {
    Stats(oneshot::Sender<Result<(StorageStats, u64, u64), String>>),
    Query {
        request: ReadRequest,
        cancelled: Arc<AtomicBool>,
        interrupt: Arc<StdMutex<Option<Arc<rusqlite::InterruptHandle>>>>,
        reply: oneshot::Sender<Result<(ReadOutput, u64, u64), String>>,
    },
    Shutdown,
}

// Maintenance planning remains extension-owned. The API timer is only a
// wake-up: it asks timeless_stats for the exact actionable backlog, samples
// those blocks' bytes, and converts the 32 MiB work target into a span budget.
// 8,192 is the extension's public flush/merge target and only guarantees that
// one complete planner group can make progress; the API never creates blocks.
const OPTIMIZE_SOURCE_BYTE_BUDGET: u64 = 32 * 1024 * 1024;
const OPTIMIZE_TARGET_SPANS: usize = 8192;

struct StorageInner {
    writer: mpsc::Sender<WriteCommand>,
    readers: Vec<mpsc::Sender<ReadCommand>>,
    next_reader: AtomicUsize,
    profile: Arc<StdMutex<ApiProfile>>,
    admission: Mutex<()>,
    joins: Mutex<Vec<JoinHandle<Result<(), String>>>>,
    lease: StdMutex<Option<File>>,
    database_path: PathBuf,
    retention: Option<Duration>,
    queue_capacity: usize,
    shutting_down: AtomicBool,
}

#[derive(Clone)]
pub struct Storage(Arc<StorageInner>);

#[derive(Clone, Copy, Debug, Default)]
pub struct IngestTimings {
    pub parse: Duration,
    pub wire_decode: Duration,
    pub batch_encode: Duration,
    pub decompressed_body_bytes: usize,
}

impl Storage {
    pub fn start(
        database_path: PathBuf,
        extension_path: PathBuf,
        reader_connections: usize,
        queue_batches: usize,
        retention: Option<Duration>,
    ) -> Result<Self, String> {
        Self::start_with_retention_policy(
            database_path,
            extension_path,
            reader_connections,
            queue_batches,
            retention,
            true,
        )
    }

    pub fn start_with_retention_policy(
        database_path: PathBuf,
        extension_path: PathBuf,
        reader_connections: usize,
        queue_batches: usize,
        retention: Option<Duration>,
        enforce_retention: bool,
    ) -> Result<Self, String> {
        if reader_connections == 0 {
            return Err("reader_connections must be positive".into());
        }
        if queue_batches == 0 {
            return Err("command_queue_batches must be positive".into());
        }
        if retention.is_some_and(|duration| duration.is_zero()) {
            return Err("retention must be positive when enabled".into());
        }
        if let Some(parent) = database_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!("create database directory {}: {error}", parent.display())
            })?;
        }

        // This is deliberately acquired before SQLite is opened. A second
        // process cannot initialize or recover the same extension state.
        let lease = acquire_database_lease(&database_path, "traces")?;
        let profile = Arc::new(StdMutex::new(ApiProfile::default()));
        let (writer_tx, writer_rx) = mpsc::channel(queue_batches);
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let writer_db = database_path.clone();
        let writer_ext = extension_path.clone();
        let writer_profile = Arc::clone(&profile);
        let writer_join = thread::Builder::new()
            .name("timeless-traces-writer".into())
            .spawn(move || {
                writer_main(
                    writer_db,
                    writer_ext,
                    retention,
                    enforce_retention,
                    writer_rx,
                    ready_tx,
                    writer_profile,
                )
            })
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
                .name(format!("timeless-traces-reader-{number}"))
                .spawn(move || {
                    reader_main(
                        reader_db,
                        reader_ext,
                        retention,
                        enforce_retention,
                        reader_rx,
                        ready_tx,
                    )
                })
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
            retention,
            queue_capacity: queue_batches,
            shutting_down: AtomicBool::new(false),
        })))
    }

    /// The Session 3 OTLP handler uses this seam after parsing one request
    /// and encoding one public rich-span v1 batch. It never inserts spans one
    /// at a time and never owns a second buffer or block policy.
    pub async fn submit_batch(
        &self,
        blob: Vec<u8>,
        spans: usize,
        body_bytes: usize,
    ) -> Result<(), String> {
        self.admit_batch(
            blob,
            spans,
            body_bytes,
            IngestTimings::default(),
            false,
            None,
        )
        .await
    }

    /// Production OTLP admission waits for the single SQLite batch statement
    /// to finish. A successful HTTP response cannot conceal a writer failure.
    pub async fn submit_otlp_batch(
        &self,
        blob: Vec<u8>,
        spans: usize,
        body_bytes: usize,
        timings: IngestTimings,
    ) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.admit_batch(blob, spans, body_bytes, timings, true, Some(reply_tx))
            .await?;
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before OTLP request completed".to_string())?
    }

    async fn admit_batch(
        &self,
        blob: Vec<u8>,
        spans: usize,
        body_bytes: usize,
        timings: IngestTimings,
        otlp: bool,
        reply: Option<oneshot::Sender<Result<(), String>>>,
    ) -> Result<(), String> {
        let admission_started = Instant::now();
        let _ordered = self.0.admission.lock().await;
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("traces API is shutting down; admission is closed".into());
        }
        let permit = self
            .0
            .writer
            .reserve()
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        let admission_ns = elapsed_ns(admission_started);
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.pending.push_back(PendingRequest {
                queued_at: Instant::now(),
                spans,
                body_bytes,
            });
            profile.admitted_requests = profile.admitted_requests.saturating_add(1);
            profile.admitted_spans = profile.admitted_spans.saturating_add(spans as u64);
            profile.admitted_body_bytes = profile
                .admitted_body_bytes
                .saturating_add(body_bytes as u64);
            profile.admission_wait_ns = profile.admission_wait_ns.saturating_add(admission_ns);
            if otlp {
                profile.ingest_requests = profile.ingest_requests.saturating_add(1);
            }
            profile.parse_ns = profile.parse_ns.saturating_add(duration_ns(timings.parse));
            profile.wire_decode_ns = profile
                .wire_decode_ns
                .saturating_add(duration_ns(timings.wire_decode));
            profile.batch_encode_ns = profile
                .batch_encode_ns
                .saturating_add(duration_ns(timings.batch_encode));
            profile.decompressed_body_bytes = profile
                .decompressed_body_bytes
                .saturating_add(timings.decompressed_body_bytes as u64);
        }
        permit.send(WriteCommand::Ingest {
            blob,
            spans,
            body_bytes,
            reply,
        });
        Ok(())
    }

    pub fn record_ingest_rejection(&self, spans: usize, body_bytes: usize) {
        let mut profile = profile_lock(&self.0.profile);
        profile.ingest_requests = profile.ingest_requests.saturating_add(1);
        profile.rejected_requests = profile.rejected_requests.saturating_add(1);
        profile.rejected_spans = profile.rejected_spans.saturating_add(spans as u64);
        profile.rejected_body_bytes = profile
            .rejected_body_bytes
            .saturating_add(body_bytes as u64);
    }

    /// Proves that every earlier admitted request completed its one SQLite
    /// statement. It deliberately does not flush the extension buffer.
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
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("traces API is shutting down; flush is closed".into());
        }
        let (through_requests, through_spans, through_body_bytes) = {
            let profile = profile_lock(&self.0.profile);
            (
                profile.admitted_requests,
                profile.admitted_spans,
                profile.admitted_body_bytes,
            )
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Flush {
                through_requests,
                through_spans,
                through_body_bytes,
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

    pub async fn schedule_optimize(&self) -> Result<(), String> {
        let _ordered = self.0.admission.lock().await;
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("traces API is shutting down; optimize is closed".into());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Optimize(reply_tx))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        drop(_ordered);
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before optimize completed".to_string())?
    }

    pub async fn backup(&self, destination: PathBuf) -> Result<BackupReport, String> {
        let _ordered = self.0.admission.lock().await;
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("traces API is shutting down; backup is closed".into());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Backup {
                destination,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        drop(_ordered);
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before backup completed".to_string())?
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
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.stats_count = profile.stats_count.saturating_add(1);
            profile.stats_total_ns = profile
                .stats_total_ns
                .saturating_add(elapsed_ns(total_started));
            profile.stats_sqlite_ns = profile.stats_sqlite_ns.saturating_add(sqlite_ns);
            profile.stats_retries = profile.stats_retries.saturating_add(retries);
            apply_profile(&mut stats, &profile);
        }
        stats.writer_connections = 1;
        stats.reader_connections = self.0.readers.len();
        stats.command_queue_capacity_requests = self.0.queue_capacity;
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
                ReadKind::Services => {
                    profile.services_requests = profile.services_requests.saturating_add(1)
                }
                ReadKind::Operations => {
                    profile.operations_requests = profile.operations_requests.saturating_add(1)
                }
                ReadKind::Trace => {
                    profile.trace_requests = profile.trace_requests.saturating_add(1)
                }
                ReadKind::Search => {
                    profile.search_requests = profile.search_requests.saturating_add(1)
                }
            }
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let interrupt = Arc::new(StdMutex::new(None));
        let mut cancellation = ReadCancellation {
            cancelled: Arc::clone(&cancelled),
            interrupt: Arc::clone(&interrupt),
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
                interrupt,
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
        result.map(|(output, _, _)| output)
    }

    pub fn is_ready(&self) -> bool {
        !self.0.shutting_down.load(Ordering::Acquire)
    }

    /// Returns host admission/completion state without waiting on SQLite.
    /// This remains responsive while a writer is blocked and lets operators
    /// distinguish database work from API queue saturation.
    pub fn runtime_watermarks(&self) -> RuntimeWatermarks {
        let profile = profile_lock(&self.0.profile);
        RuntimeWatermarks {
            admitted_requests: profile.admitted_requests,
            admitted_spans: profile.admitted_spans,
            admitted_body_bytes: profile.admitted_body_bytes,
            completed_requests: profile.completed_requests,
            completed_spans: profile.completed_spans,
            completed_body_bytes: profile.completed_body_bytes,
            failed_requests: profile.failed_requests,
            failed_spans: profile.failed_spans,
            failed_body_bytes: profile.failed_body_bytes,
            queued_requests: profile.pending.len() as u64,
            queued_spans: profile
                .pending
                .iter()
                .map(|pending| pending.spans as u64)
                .sum(),
            queued_body_bytes: profile
                .pending
                .iter()
                .map(|pending| pending.body_bytes as u64)
                .sum(),
            in_flight_requests: profile.in_flight_requests,
            in_flight_spans: profile.in_flight_spans,
            in_flight_body_bytes: profile.in_flight_body_bytes,
            oldest_queued_ms: profile
                .pending
                .front()
                .map(|pending| duration_ms(pending.queued_at.elapsed()))
                .unwrap_or(0),
        }
    }

    fn reader(&self) -> &mpsc::Sender<ReadCommand> {
        let number = self.0.next_reader.fetch_add(1, Ordering::Relaxed);
        &self.0.readers[number % self.0.readers.len()]
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        if self.0.shutting_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        // Serialize with admission so no accepted request can land behind the
        // shutdown marker. All prior writer commands drain in FIFO order.
        let _ordered = self.0.admission.lock().await;
        for reader in &self.0.readers {
            let _ = reader.send(ReadCommand::Shutdown).await;
        }
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
                .map_err(|_| "SQLite traces API worker panicked".to_string())??;
        }
        if let Some(file) = profile_lock(&self.0.lease).take() {
            FileExt::unlock(&file)
                .map_err(|error| format!("release database owner lease: {error}"))?;
        }
        writer_result
    }

    pub fn retention(&self) -> Option<Duration> {
        self.0.retention
    }
}

fn writer_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    retention: Option<Duration>,
    enforce_retention: bool,
    mut commands: mpsc::Receiver<WriteCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
    profile: Arc<StdMutex<ApiProfile>>,
) -> Result<(), String> {
    let conn = match open_connection(
        &database_path,
        &extension_path,
        retention,
        enforce_retention,
        true,
    ) {
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
            WriteCommand::Ingest {
                blob,
                spans,
                body_bytes,
                reply,
            } => {
                record_queue_start(&profile, spans, body_bytes);
                let started = Instant::now();
                let result = insert_rich_batch(&conn, &blob, spans);
                record_queue_completion(&profile, spans, body_bytes, elapsed_ns(started), &result);
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                } else if let Err(error) = result {
                    unreported_error = Some(error);
                }
            }
            WriteCommand::Barrier(reply) => {
                let result = unreported_error.take().map_or(Ok(()), Err);
                let _ = reply.send(result);
            }
            WriteCommand::Flush {
                through_requests,
                through_spans,
                through_body_bytes,
                explicit,
                reply,
            } => {
                let started = Instant::now();
                let flush_result = run_command(&conn, "flush", "flush traces");
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
                        Ok(flush_report(
                            &api,
                            through_requests,
                            through_spans,
                            through_body_bytes,
                            flush_ns,
                        ))
                    }
                };
                let _ = reply.send(result);
            }
            WriteCommand::Optimize(reply) => {
                let started = Instant::now();
                let result = optimize_backlog(&conn);
                let mut api = profile_lock(&profile);
                api.optimize_count = api.optimize_count.saturating_add(1);
                api.optimize_total_ns = api.optimize_total_ns.saturating_add(elapsed_ns(started));
                if let Err(error) = &result {
                    api.optimize_errors = api.optimize_errors.saturating_add(1);
                    api.last_error = Some(error.clone());
                }
                drop(api);
                let _ = reply.send(result);
            }
            WriteCommand::Backup { destination, reply } => {
                let started = Instant::now();
                let result = (|| {
                    if let Some(error) = unreported_error.take() {
                        return Err(format!(
                            "refusing traces backup after an unreported write failure: {error}"
                        ));
                    }
                    run_command(&conn, "flush", "flush traces for backup")?;
                    optimize_all_backlog(&conn)?;
                    let checkpoint_started = Instant::now();
                    let checkpoint = checkpoint_wal(&conn, "traces");
                    record_checkpoint(&profile, checkpoint_started.elapsed(), &checkpoint);
                    let checkpoint = checkpoint?;
                    create_verified_backup(&conn, &destination, "traces", checkpoint)
                })();
                record_backup(&profile, started.elapsed(), &result);
                let _ = reply.send(result);
            }
            WriteCommand::Shutdown(reply) => {
                let flush = run_command(&conn, "flush", "graceful traces flush");
                let checkpoint_started = Instant::now();
                // Attempt the checkpoint even when flush reports an error so
                // shutdown telemetry preserves both independent operations.
                let checkpoint = checkpoint_wal(&conn, "traces").map(|_| ());
                record_checkpoint(&profile, checkpoint_started.elapsed(), &checkpoint);
                let result = match (unreported_error.take(), flush, checkpoint) {
                    (Some(error), _, _) => Err(error),
                    (None, Err(error), _) => Err(error),
                    (None, Ok(()), Err(error)) => Err(error),
                    (None, Ok(()), Ok(())) => Ok(()),
                };
                let _ = reply.send(result.clone());
                return result;
            }
        }
    }
    // A dropped API still flushes its accepted tail. SIGKILL cannot run this;
    // only previously flushed/committed blocks are promised after kill -9.
    run_command(&conn, "flush", "final traces flush after writer disconnect")?;
    checkpoint_wal(&conn, "traces").map(|_| ())
}

fn optimize_backlog(conn: &Connection) -> Result<(), String> {
    let stats = stat_values(conn)?;
    let integer = |key: &str| match stats.get(key) {
        Some(SqlValue::Integer(value)) => *value,
        _ => 0,
    };
    let actionable_spans = integer("optimize_pending_raw_entries")
        .saturating_add(integer("optimize_merge_ready_entries"))
        .max(0) as u64;
    if actionable_spans == 0 {
        return Ok(());
    }
    let (sample_spans, sample_bytes): (i64, i64) = conn
        .query_row(
            "SELECT COALESCE(SUM(entry_count), 0),
                    COALESCE(SUM(length(data)), 0)
               FROM traces_blocks
              WHERE codec = 1 OR entry_count < 8192",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| format!("inspect traces optimize source bytes: {error}"))?;
    let budget = optimize_span_budget(
        actionable_spans,
        sample_spans.max(0) as u64,
        sample_bytes.max(0) as u64,
    );
    run_command(
        conn,
        &format!("optimize:{budget}"),
        &format!("optimize traces with {budget}-span budget"),
    )
}

fn optimize_all_backlog(conn: &Connection) -> Result<(), String> {
    let mut previous = u64::MAX;
    loop {
        let stats = stat_values(conn)?;
        let integer = |key: &str| match stats.get(key) {
            Some(SqlValue::Integer(value)) => *value,
            _ => 0,
        };
        let actionable = integer("optimize_pending_raw_entries")
            .saturating_add(integer("optimize_merge_ready_entries"))
            .max(0) as u64;
        if actionable == 0 {
            return Ok(());
        }
        if actionable >= previous {
            return Err(format!(
                "traces optimize backlog made no progress: previous={previous}, current={actionable}"
            ));
        }
        previous = actionable;
        optimize_backlog(conn)?;
    }
}

fn optimize_span_budget(actionable_spans: u64, sample_spans: u64, sample_bytes: u64) -> usize {
    if actionable_spans == 0 {
        return 0;
    }
    let target_spans = OPTIMIZE_TARGET_SPANS as u64;
    if sample_spans == 0 || sample_bytes == 0 {
        return usize::try_from(actionable_spans.min(target_spans)).unwrap_or(usize::MAX);
    }
    let estimated = (u128::from(OPTIMIZE_SOURCE_BYTE_BUDGET)
        .saturating_mul(u128::from(sample_spans))
        .saturating_add(u128::from(sample_bytes - 1))
        / u128::from(sample_bytes))
    .min(u128::from(u64::MAX)) as u64;
    usize::try_from(actionable_spans.min(estimated.max(target_spans))).unwrap_or(usize::MAX)
}

fn reader_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    retention: Option<Duration>,
    enforce_retention: bool,
    mut commands: mpsc::Receiver<ReadCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
) -> Result<(), String> {
    let conn = match open_connection(
        &database_path,
        &extension_path,
        retention,
        enforce_retention,
        false,
    ) {
        Ok(conn) => {
            let _ = ready.send(Ok(()));
            conn
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
                interrupt,
                reply,
            } => {
                let handle = Arc::new(conn.get_interrupt_handle());
                *profile_lock(&interrupt) = Some(Arc::clone(&handle));
                if cancelled.load(Ordering::Acquire) {
                    handle.interrupt();
                }
                let progress_cancelled = Arc::clone(&cancelled);
                let started = Instant::now();
                let mut retries = 0_u64;
                let result = conn
                    .progress_handler(
                        1_000,
                        Some(move || progress_cancelled.load(Ordering::Acquire)),
                    )
                    .map_err(|error| format!("install query cancellation handler: {error}"))
                    .and_then(|()| {
                        retry_read(
                            || query::execute(&conn, request.clone(), &cancelled),
                            || retries = retries.saturating_add(1),
                        )
                    });
                let sqlite_ns = elapsed_ns(started);
                let cleared = conn.progress_handler(0, None::<fn() -> bool>);
                profile_lock(&interrupt).take();
                let result = match (result, cleared) {
                    (Ok(output), Ok(())) => Ok((output, sqlite_ns, retries)),
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

fn open_connection(
    path: &Path,
    extension: &Path,
    retention: Option<Duration>,
    enforce_retention: bool,
    initialize: bool,
) -> Result<Connection, String> {
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
        signal: "traces",
        required_batch: "rich-span-v1",
    };
    let capabilities = preflight_extension(&conn, spec)?;
    preflight_database(&conn, spec.signal)?;
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(|error| format!("set busy timeout: {error}"))?;
    if initialize {
        let create = match retention {
            Some(duration) => format!(
                "CREATE VIRTUAL TABLE IF NOT EXISTS traces USING timeless_traces(retention='{}s');",
                duration.as_secs()
            ),
            None => "CREATE VIRTUAL TABLE IF NOT EXISTS traces USING timeless_traces;".to_owned(),
        };
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
             {create}"
        ))
        .map_err(|error| format!("initialize traces database: {error}"))?;
        apply_schema_ledger(&conn, spec, &capabilities)?;
    } else {
        require_current_schema(&conn, spec.signal)?;
        conn.execute_batch(
            "PRAGMA cache_size = -8000;
             PRAGMA mmap_size = 2147483648;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(|error| format!("configure traces reader: {error}"))?;
    }
    verify_capability(&conn, retention, enforce_retention, initialize)?;
    Ok(conn)
}

fn verify_capability(
    conn: &Connection,
    retention: Option<Duration>,
    enforce_retention: bool,
    probe_batch: bool,
) -> Result<(), String> {
    let mut statement = conn
        .prepare("PRAGMA table_xinfo('traces')")
        .map_err(|error| format!("inspect traces capability schema: {error}"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .map_err(|error| format!("read traces capability schema: {error}"))?;
    let actual = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("collect traces capability schema: {error}"))?;
    let expected = EXPECTED_COLUMNS
        .iter()
        .map(|(name, kind, hidden)| ((*name).to_owned(), (*kind).to_owned(), *hidden))
        .collect::<Vec<_>>();
    if actual != expected {
        return Err(format!(
            "incompatible timeless_traces extension: server requires {TRACE_CAPABILITY}; expected columns {expected:?}, got {actual:?}"
        ));
    }

    let values = stat_values(conn)?;
    match values.get("module") {
        Some(SqlValue::Text(module)) if module == "timeless_traces" => {}
        value => {
            return Err(format!(
                "incompatible timeless_traces extension: expected module timeless_traces, got {value:?}"
            ))
        }
    }
    let expected_retention = retention
        .map(|duration| duration.as_secs())
        .map(|seconds| seconds.saturating_mul(1_000_000_000))
        .map(|native| i64::try_from(native).unwrap_or(i64::MAX));
    let actual_retention = optional_integer(values.get("retention"));
    if enforce_retention && actual_retention != expected_retention {
        return Err(format!(
            "traces retention mismatch: server requested {expected_retention:?} ns but database stores {actual_retention:?} ns"
        ));
    }

    if probe_batch {
        // A zero-span v1 batch is a public, non-data capability probe. The
        // schema alone cannot distinguish a rich-schema build that lacks the
        // matching versioned batch decoder.
        let empty_v1 = [0x02_u8, 0, 0, 0, 0, 0, 0, 0];
        conn.execute("INSERT INTO traces(traces) VALUES (?1)", params![empty_v1])
            .map_err(|error| {
                format!(
                    "incompatible timeless_traces extension: {TRACE_CAPABILITY} batch probe failed: {error}"
                )
            })?;
    } else {
        conn.prepare(
            "SELECT status_description,events,resource,instrumentation_scope FROM traces LIMIT 0",
        )
        .map_err(|error| format!("connect rich traces virtual table: {error}"))?;
    }
    Ok(())
}

fn insert_rich_batch(conn: &Connection, blob: &[u8], spans: usize) -> Result<(), String> {
    if blob.first() != Some(&0x02) {
        return Err("traces API writer accepts only public rich-span batch v1 (0x02)".into());
    }
    let expected =
        i64::try_from(spans).map_err(|_| "traces batch span count exceeds i64::MAX".to_string())?;
    conn.execute("INSERT INTO traces(traces) VALUES (?1)", params![blob])
        .map_err(|error| format!("insert traces rich batch: {error}"))?;
    let inserted = conn.last_insert_rowid();
    if inserted != expected {
        return Err(format!(
            "timeless_traces accepted {inserted} spans; API batch declared {spans}"
        ));
    }
    Ok(())
}

fn run_command(conn: &Connection, command: &str, context: &str) -> Result<(), String> {
    conn.execute("INSERT INTO traces(traces) VALUES (?1)", [command])
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
    let text = |key: &str| match values.get(key) {
        Some(SqlValue::Text(value)) => Some(value.clone()),
        _ => None,
    };
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
            "SELECT COALESCE(SUM(pgsize), 0) FROM dbstat
              WHERE name IN ('traces_blocks_ts','traces_terms','traces_trace_blocks',
                             'sqlite_autoindex_traces_meta_1')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("read traces SQLite index bytes: {error}"))?;
    let blocks = integer("blocks");
    let raw_blocks = integer("raw_blocks");
    Ok(StorageStats {
        capability: TRACE_CAPABILITY.to_owned(),
        module: text("module").unwrap_or_default(),
        retention_nanoseconds: optional_integer(values.get("retention")),
        blocks,
        raw_blocks,
        compressed_blocks: blocks.saturating_sub(raw_blocks),
        buffered_spans: integer("buffered_spans"),
        disk_spans: integer("disk_spans"),
        total_spans: integer("total_spans"),
        bytes_on_disk: integer("bytes_on_disk"),
        terms: integer("terms"),
        trace_index_rows: integer("trace_index_rows"),
        oldest_timestamp_nanoseconds: optional_integer(values.get("ts_min")),
        newest_timestamp_nanoseconds: optional_integer(values.get("ts_max")),
        extension_query_count: integer("query_count"),
        extension_query_cancelled: integer("query_cancelled"),
        extension_query_total_ns: integer("query_total_ns"),
        extension_query_candidate_blocks: integer("query_candidate_blocks"),
        extension_query_payload_blocks_read: integer("query_payload_blocks_read"),
        extension_query_payload_bytes_read: integer("query_payload_bytes_read"),
        extension_query_decoded_spans: integer("query_decoded_spans"),
        extension_query_buffered_spans_examined: integer("query_buffered_spans_examined"),
        extension_query_matched_spans: integer("query_matched_spans"),
        extension_query_returned_spans: integer("query_returned_spans"),
        extension_query_snapshot_ns: integer("query_snapshot_ns"),
        extension_query_snapshot_payload_bytes: integer("query_snapshot_payload_bytes"),
        extension_query_snapshot_payload_max_bytes: integer("query_snapshot_payload_max_bytes"),
        extension_query_stable_location_snapshots: integer("query_stable_location_snapshots"),
        extension_query_bounded_count: integer("query_bounded_count"),
        extension_query_bounded_requested_spans: integer("query_bounded_requested_spans"),
        extension_query_bounded_max_spans: integer("query_bounded_max_spans"),
        extension_query_blocks_skipped_by_bound: integer("query_blocks_skipped_by_bound"),
        extension_discovery_count: integer("discovery_count"),
        extension_discovery_total_ns: integer("discovery_total_ns"),
        extension_discovery_payload_bytes_read: integer("discovery_payload_bytes_read"),
        extension_discovery_decoded_spans: integer("discovery_decoded_spans"),
        extension_optimize_count: integer("optimize_count"),
        extension_optimize_total_ns: integer("optimize_total_ns"),
        extension_optimize_blocks_removed: integer("optimize_blocks_removed"),
        extension_optimize_blocks_written: integer("optimize_blocks_written"),
        extension_optimize_budgeted_count: integer("optimize_budgeted_count"),
        extension_optimize_budget_entries: integer("optimize_budget_entries"),
        extension_optimize_budget_limited_count: integer("optimize_budget_limited_count"),
        extension_optimize_raw_groups: integer("optimize_raw_groups"),
        extension_optimize_raw_blocks: integer("optimize_raw_blocks"),
        extension_optimize_raw_entries: integer("optimize_raw_entries"),
        extension_optimize_raw_input_bytes: integer("optimize_raw_input_bytes"),
        extension_optimize_raw_output_bytes: integer("optimize_raw_output_bytes"),
        extension_optimize_raw_total_ns: integer("optimize_raw_total_ns"),
        extension_optimize_merge_groups: integer("optimize_merge_groups"),
        extension_optimize_merge_blocks: integer("optimize_merge_blocks"),
        extension_optimize_merge_entries: integer("optimize_merge_entries"),
        extension_optimize_merge_input_bytes: integer("optimize_merge_input_bytes"),
        extension_optimize_merge_output_bytes: integer("optimize_merge_output_bytes"),
        extension_optimize_merge_total_ns: integer("optimize_merge_total_ns"),
        extension_optimize_pending_raw_blocks: integer("optimize_pending_raw_blocks"),
        extension_optimize_pending_raw_entries: integer("optimize_pending_raw_entries"),
        extension_optimize_merge_ready_groups: integer("optimize_merge_ready_groups"),
        extension_optimize_merge_ready_blocks: integer("optimize_merge_ready_blocks"),
        extension_optimize_merge_ready_entries: integer("optimize_merge_ready_entries"),
        extension_optimize_merge_deferred_blocks: integer("optimize_merge_deferred_blocks"),
        extension_optimize_merge_deferred_entries: integer("optimize_merge_deferred_entries"),
        extension_read_permit_count: integer("read_permit_count"),
        extension_read_permit_hold_ns: integer("read_permit_hold_ns"),
        extension_read_conflicts: integer("read_conflicts"),
        extension_read_barge_rejections: integer("read_barge_rejections"),
        extension_waiting_writers: integer("waiting_writers"),
        extension_writer_wait_count: integer("writer_wait_count"),
        extension_writer_wait_ns: integer("writer_wait_ns"),
        extension_writer_timeouts: integer("writer_timeouts"),
        sqlite_page_bytes: page_count.saturating_mul(page_size),
        sqlite_index_bytes,
        freelist_pages,
        freelist_bytes: freelist_pages.saturating_mul(page_size),
        ..StorageStats::default()
    })
}

fn stat_values(conn: &Connection) -> Result<HashMap<String, SqlValue>, String> {
    let mut statement = conn
        .prepare("SELECT key,value FROM timeless_stats('traces')")
        .map_err(|error| format!("prepare timeless_stats for traces: {error}"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, SqlValue>(1)?))
        })
        .map_err(|error| format!("read timeless_stats for traces: {error}"))?;
    let mut values = HashMap::new();
    for row in rows {
        let (key, value) =
            row.map_err(|error| format!("collect timeless_stats for traces: {error}"))?;
        values.insert(key, value);
    }
    Ok(values)
}

fn apply_profile(stats: &mut StorageStats, profile: &ApiProfile) {
    stats.admitted_requests = profile.admitted_requests;
    stats.admitted_spans = profile.admitted_spans;
    stats.admitted_body_bytes = profile.admitted_body_bytes;
    stats.completed_requests = profile.completed_requests;
    stats.completed_spans = profile.completed_spans;
    stats.completed_body_bytes = profile.completed_body_bytes;
    stats.failed_requests = profile.failed_requests;
    stats.failed_spans = profile.failed_spans;
    stats.failed_body_bytes = profile.failed_body_bytes;
    stats.queued_requests = profile.pending.len() as u64;
    stats.queued_spans = profile
        .pending
        .iter()
        .map(|pending| pending.spans as u64)
        .sum();
    stats.queued_body_bytes = profile
        .pending
        .iter()
        .map(|pending| pending.body_bytes as u64)
        .sum();
    stats.in_flight_requests = profile.in_flight_requests;
    stats.in_flight_spans = profile.in_flight_spans;
    stats.in_flight_body_bytes = profile.in_flight_body_bytes;
    stats.oldest_queued_ms = profile
        .pending
        .front()
        .map(|pending| duration_ms(pending.queued_at.elapsed()))
        .unwrap_or(0);
    stats.api_admission_wait_ns = profile.admission_wait_ns;
    stats.api_queue_wait_ns = profile.queue_wait_ns;
    stats.api_queue_wait_max_ns = profile.queue_wait_max_ns;
    stats.api_ingest_requests = profile.ingest_requests;
    stats.api_rejected_requests = profile.rejected_requests;
    stats.api_rejected_spans = profile.rejected_spans;
    stats.api_rejected_body_bytes = profile.rejected_body_bytes;
    stats.api_parse_ns = profile.parse_ns;
    stats.api_wire_decode_ns = profile.wire_decode_ns;
    stats.api_batch_encode_ns = profile.batch_encode_ns;
    stats.api_decompressed_body_bytes = profile.decompressed_body_bytes;
    stats.api_sqlite_insert_ns = profile.sqlite_insert_ns;
    // One public batch insertion is one autocommit SQLite transaction. Keep
    // both names so phase accounting is explicit without double measurement.
    stats.api_sqlite_transaction_ns = profile.sqlite_insert_ns;
    stats.api_stats_count = profile.stats_count;
    stats.api_stats_total_ns = profile.stats_total_ns;
    stats.api_stats_sqlite_ns = profile.stats_sqlite_ns;
    stats.api_stats_retries = profile.stats_retries;
    stats.api_read_requests = profile.read_requests;
    stats.api_services_requests = profile.services_requests;
    stats.api_operations_requests = profile.operations_requests;
    stats.api_trace_requests = profile.trace_requests;
    stats.api_search_requests = profile.search_requests;
    stats.api_read_in_flight = profile.read_in_flight;
    stats.api_read_cancelled = profile.read_cancelled;
    stats.api_read_total_ns = profile.read_total_ns;
    stats.api_read_sqlite_ns = profile.read_sqlite_ns;
    stats.api_read_errors = profile.read_errors;
    stats.api_read_retries = profile.read_retries;
    stats.api_read_response_bytes = profile.read_response_bytes;
    stats.api_read_result_traces = profile.read_result_traces;
    stats.api_read_result_spans = profile.read_result_spans;
    stats.api_flush_count = profile.explicit_flush_count;
    stats.api_flush_total_ns = profile.explicit_flush_total_ns;
    stats.api_flush_sqlite_ns = profile.explicit_flush_sqlite_ns;
    stats.api_flush_errors = profile.explicit_flush_errors;
    stats.scheduled_flush_count = profile.scheduled_flush_count;
    stats.scheduled_flush_total_ns = profile.scheduled_flush_total_ns;
    stats.scheduled_flush_errors = profile.scheduled_flush_errors;
    stats.optimize_count = profile.optimize_count;
    stats.optimize_total_ns = profile.optimize_total_ns;
    stats.optimize_errors = profile.optimize_errors;
    stats.checkpoint_count = profile.checkpoint_count;
    stats.checkpoint_total_ns = profile.checkpoint_total_ns;
    stats.checkpoint_errors = profile.checkpoint_errors;
    stats.backup_count = profile.backup_count;
    stats.backup_total_ns = profile.backup_total_ns;
    stats.backup_errors = profile.backup_errors;
    stats.last_error.clone_from(&profile.last_error);
}

fn record_checkpoint<T>(
    profile: &StdMutex<ApiProfile>,
    duration: Duration,
    result: &Result<T, String>,
) {
    let mut profile = profile_lock(profile);
    profile.checkpoint_count = profile.checkpoint_count.saturating_add(1);
    profile.checkpoint_total_ns = profile
        .checkpoint_total_ns
        .saturating_add(duration_ns(duration));
    if let Err(error) = result {
        profile.checkpoint_errors = profile.checkpoint_errors.saturating_add(1);
        profile.last_error = Some(error.clone());
    }
}

fn record_backup(
    profile: &StdMutex<ApiProfile>,
    duration: Duration,
    result: &Result<BackupReport, String>,
) {
    let mut profile = profile_lock(profile);
    profile.backup_count = profile.backup_count.saturating_add(1);
    profile.backup_total_ns = profile
        .backup_total_ns
        .saturating_add(duration_ns(duration));
    if let Err(error) = result {
        profile.backup_errors = profile.backup_errors.saturating_add(1);
        profile.last_error = Some(error.clone());
    }
}

fn flush_report(
    profile: &ApiProfile,
    through_requests: u64,
    through_spans: u64,
    through_body_bytes: u64,
    flush_sqlite_ns: u64,
) -> FlushReport {
    FlushReport {
        status: "ok".into(),
        through_requests,
        through_spans,
        through_body_bytes,
        completed_requests: profile.completed_requests,
        completed_spans: profile.completed_spans,
        completed_body_bytes: profile.completed_body_bytes,
        failed_requests: profile.failed_requests,
        failed_spans: profile.failed_spans,
        queued_requests: profile.pending.len() as u64,
        queued_spans: profile
            .pending
            .iter()
            .map(|pending| pending.spans as u64)
            .sum(),
        queued_body_bytes: profile
            .pending
            .iter()
            .map(|pending| pending.body_bytes as u64)
            .sum(),
        in_flight_requests: profile.in_flight_requests,
        in_flight_spans: profile.in_flight_spans,
        in_flight_body_bytes: profile.in_flight_body_bytes,
        flush_sqlite_ns,
        api_request_ns: 0,
    }
}

fn record_queue_start(profile: &StdMutex<ApiProfile>, spans: usize, body_bytes: usize) {
    let mut profile = profile_lock(profile);
    if let Some(pending) = profile.pending.pop_front() {
        debug_assert_eq!(pending.spans, spans);
        debug_assert_eq!(pending.body_bytes, body_bytes);
        let wait_ns = elapsed_ns(pending.queued_at);
        profile.queue_wait_ns = profile.queue_wait_ns.saturating_add(wait_ns);
        profile.queue_wait_max_ns = profile.queue_wait_max_ns.max(wait_ns);
        profile.in_flight_requests = profile.in_flight_requests.saturating_add(1);
        profile.in_flight_spans = profile.in_flight_spans.saturating_add(spans as u64);
        profile.in_flight_body_bytes = profile
            .in_flight_body_bytes
            .saturating_add(body_bytes as u64);
    }
}

fn record_queue_completion(
    profile: &StdMutex<ApiProfile>,
    spans: usize,
    body_bytes: usize,
    insert_ns: u64,
    result: &Result<(), String>,
) {
    let mut profile = profile_lock(profile);
    profile.in_flight_requests = profile.in_flight_requests.saturating_sub(1);
    profile.in_flight_spans = profile.in_flight_spans.saturating_sub(spans as u64);
    profile.in_flight_body_bytes = profile
        .in_flight_body_bytes
        .saturating_sub(body_bytes as u64);
    profile.sqlite_insert_ns = profile.sqlite_insert_ns.saturating_add(insert_ns);
    match result {
        Ok(()) => {
            profile.completed_requests = profile.completed_requests.saturating_add(1);
            profile.completed_spans = profile.completed_spans.saturating_add(spans as u64);
            profile.completed_body_bytes = profile
                .completed_body_bytes
                .saturating_add(body_bytes as u64);
        }
        Err(error) => {
            profile.failed_requests = profile.failed_requests.saturating_add(1);
            profile.failed_spans = profile.failed_spans.saturating_add(spans as u64);
            profile.failed_body_bytes = profile.failed_body_bytes.saturating_add(body_bytes as u64);
            profile.last_error = Some(error.clone());
        }
    }
}

struct ReadCancellation {
    cancelled: Arc<AtomicBool>,
    interrupt: Arc<StdMutex<Option<Arc<rusqlite::InterruptHandle>>>>,
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
        if let Some(interrupt) = profile_lock(&self.interrupt).as_ref() {
            interrupt.interrupt();
        }
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
    result: &Result<(ReadOutput, u64, u64), String>,
) {
    let mut profile = profile_lock(profile);
    profile.read_in_flight = profile.read_in_flight.saturating_sub(1);
    profile.read_total_ns = profile.read_total_ns.saturating_add(elapsed_ns(started));
    match result {
        Ok((output, sqlite_ns, retries)) => {
            profile.read_sqlite_ns = profile.read_sqlite_ns.saturating_add(*sqlite_ns);
            profile.read_retries = profile.read_retries.saturating_add(*retries);
            profile.read_response_bytes = profile
                .read_response_bytes
                .saturating_add(output.body.len() as u64);
            profile.read_result_traces = profile.read_result_traces.saturating_add(output.traces);
            profile.read_result_spans = profile.read_result_spans.saturating_add(output.spans);
        }
        Err(error) if error == "query cancelled" || error.contains("interrupted") => {}
        Err(error) => {
            profile.read_errors = profile.read_errors.saturating_add(1);
            profile.last_error = Some(error.clone());
        }
    }
}

fn optional_integer(value: Option<&SqlValue>) -> Option<i64> {
    match value {
        Some(SqlValue::Integer(value)) => Some(*value),
        Some(SqlValue::Real(value)) => Some(*value as i64),
        _ => None,
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
    fn owner_lease_is_exclusive_and_recoverable() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("traces.db");
        let first = acquire_database_lease(&database, "traces").unwrap();
        let error = acquire_database_lease(&database, "traces").unwrap_err();
        assert!(error.contains("already owned"), "{error}");
        FileExt::unlock(&first).unwrap();
        acquire_database_lease(&database, "traces").unwrap();
    }

    #[test]
    fn transient_reader_conflicts_are_retried() {
        let attempts = Cell::new(0_u64);
        let retries = Cell::new(0_u64);
        let value = retry_read(
            || {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    Err("traces read is blocked by a pending writer transaction".into())
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
    fn optimize_budget_tracks_source_bytes_and_one_complete_extension_group() {
        assert_eq!(optimize_span_budget(0, 0, 0), 0);
        assert_eq!(optimize_span_budget(4_000, 4_000, 1024), 4_000);
        assert_eq!(optimize_span_budget(100_000, 100_000, 64 << 20), 50_000);
        assert_eq!(
            optimize_span_budget(100_000, 100_000, 1024 << 20),
            OPTIMIZE_TARGET_SPANS
        );
        assert_eq!(optimize_span_budget(100_000, 0, 0), OPTIMIZE_TARGET_SPANS);
    }
}
