use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use fs2::FileExt;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::Serialize;
use timeless_api_common::{
    acquire_database_lease, apply_schema_ledger, checkpoint_wal, create_verified_backup,
    preflight_database, preflight_extension, require_current_schema, BackupReport, DataPlaneSpec,
};
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampUnit {
    Milliseconds,
    Microseconds,
}

impl TimestampUnit {
    fn sql_name(self) -> &'static str {
        match self {
            Self::Milliseconds => "ms",
            Self::Microseconds => "us",
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub ts: i64,
    pub level: u8,
    pub severity: String,
    pub message: String,
    pub metadata_json: String,
}

#[derive(Clone, Debug, Default)]
pub struct QuerySpec {
    pub level: Option<String>,
    pub service: Option<String>,
    pub metadata_eq: BTreeMap<String, String>,
    pub message: Option<String>,
    pub ts_min: Option<i64>,
    pub ts_max: Option<i64>,
    pub limit: usize,
    pub offset: usize,
    pub descending: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryRow {
    pub ts: i64,
    pub level: String,
    pub message: String,
    pub metadata_json: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageStats {
    pub total_blocks: i64,
    pub total_entries: i64,
    pub total_bytes: i64,
    pub disk_size: i64,
    pub index_size: i64,
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
    pub term_postings: i64,
    pub oldest_timestamp: Option<i64>,
    pub newest_timestamp: Option<i64>,
    pub raw_blocks: i64,
    pub raw_bytes: i64,
    pub compressed_blocks: i64,
    pub compressed_bytes: i64,
    pub buffered_entries: i64,
    pub queued_batches: i64,
    pub queued_entries: i64,
    pub oldest_queued_ms: i64,
    pub admitted_batches: i64,
    pub admitted_entries: i64,
    pub completed_batches: i64,
    pub completed_entries: i64,
    pub api_parse_ns: i64,
    pub api_batch_encode_ns: i64,
    pub api_sqlite_insert_ns: i64,
    pub api_queue_wait_ns: i64,
    pub api_queue_wait_max_ns: i64,
    pub api_query_count: i64,
    pub api_query_ns: i64,
    pub ingest_batch_count: i64,
    pub ingest_batch_entries: i64,
    pub ingest_wire_decode_ns: i64,
    pub ingest_normalize_ns: i64,
    pub ingest_buffer_append_ns: i64,
    pub flush_count: i64,
    pub flush_entries: i64,
    pub flush_total_ns: i64,
    pub flush_partition_ns: i64,
    pub flush_encode_terms_ns: i64,
    pub flush_store_ns: i64,
    pub query_count: i64,
    pub query_total_ns: i64,
    pub query_snapshot_ns: i64,
    pub query_materialize_ns: i64,
    pub query_snapshot_payload_bytes: i64,
    pub query_snapshot_payload_max_bytes: i64,
    pub query_snapshot_buffered_entries: i64,
    pub query_stable_location_snapshots: i64,
    pub query_payload_bytes_read: i64,
    pub query_candidate_blocks: i64,
    pub query_decoded_entries: i64,
    pub query_matched_entries: i64,
    pub query_returned_entries: i64,
    pub query_bounded_count: i64,
    pub query_bounded_requested_entries: i64,
    pub query_bounded_max_entries: i64,
    pub query_blocks_skipped_by_bound: i64,
    pub native_count_count: i64,
    pub native_count_total_ns: i64,
    pub native_count_snapshot_ns: i64,
    pub native_count_payload_bytes_read: i64,
    pub native_count_metadata_blocks: i64,
    pub native_count_metadata_entries: i64,
    pub native_count_decoded_blocks: i64,
    pub native_count_decoded_entries: i64,
    pub optimize_count: i64,
    pub optimize_total_ns: i64,
    pub optimize_blocks_removed: i64,
    pub optimize_blocks_written: i64,
    pub optimize_budgeted_count: i64,
    pub optimize_budget_entries: i64,
    pub optimize_budget_limited_count: i64,
    pub optimize_raw_groups: i64,
    pub optimize_raw_blocks: i64,
    pub optimize_raw_entries: i64,
    pub optimize_raw_input_bytes: i64,
    pub optimize_raw_output_bytes: i64,
    pub optimize_raw_total_ns: i64,
    pub optimize_merge_groups: i64,
    pub optimize_merge_blocks: i64,
    pub optimize_merge_entries: i64,
    pub optimize_merge_input_bytes: i64,
    pub optimize_merge_output_bytes: i64,
    pub optimize_merge_total_ns: i64,
    pub optimize_pending_raw_blocks: i64,
    pub optimize_pending_raw_entries: i64,
    pub optimize_merge_ready_groups: i64,
    pub optimize_merge_ready_blocks: i64,
    pub optimize_merge_ready_entries: i64,
    pub optimize_merge_deferred_blocks: i64,
    pub optimize_merge_deferred_entries: i64,
    pub read_permit_count: i64,
    pub read_permit_hold_ns: i64,
    pub read_conflicts: i64,
    pub read_barge_rejections: i64,
    pub waiting_writers: i64,
    pub writer_wait_count: i64,
    pub writer_wait_ns: i64,
    pub writer_timeouts: i64,
    pub checkpoint_count: i64,
    pub checkpoint_total_ns: i64,
    pub checkpoint_errors: i64,
    pub backup_count: i64,
    pub backup_total_ns: i64,
    pub backup_errors: i64,
    pub last_error: Option<String>,
}

#[derive(Default)]
struct QueueProfile {
    pending: VecDeque<(Instant, usize)>,
    admitted_batches: u64,
    admitted_entries: u64,
    completed_batches: u64,
    completed_entries: u64,
    parse_ns: u64,
    batch_encode_ns: u64,
    sqlite_insert_ns: u64,
    queue_wait_ns: u64,
    queue_wait_max_ns: u64,
    query_count: u64,
    query_ns: u64,
    checkpoint_count: u64,
    checkpoint_total_ns: u64,
    checkpoint_errors: u64,
    backup_count: u64,
    backup_total_ns: u64,
    backup_errors: u64,
    last_error: Option<String>,
}

enum WriteCommand {
    Ingest(Vec<LogEntry>),
    Flush(Option<oneshot::Sender<Result<(), String>>>),
    Optimize,
    Barrier(oneshot::Sender<()>),
    Backup {
        destination: PathBuf,
        reply: oneshot::Sender<Result<BackupReport, String>>,
    },
    Shutdown(oneshot::Sender<Result<(), String>>),
}

enum ReadCommand {
    Query(QuerySpec, oneshot::Sender<Result<Vec<QueryRow>, String>>),
    Count(QuerySpec, oneshot::Sender<Result<i64, String>>),
    FieldValues(
        QuerySpec,
        String,
        usize,
        oneshot::Sender<Result<Vec<String>, String>>,
    ),
    Stats(oneshot::Sender<Result<StorageStats, String>>),
    Shutdown,
}

// Optimize remains extension-owned. The API timer is only a maintenance
// wake-up; this byte target turns the extension's current actionable backlog
// into a bounded entry budget without adding a host-side flush/block policy.
const OPTIMIZE_SOURCE_BYTE_BUDGET: u64 = 32 * 1024 * 1024;
const OPTIMIZE_TARGET_ENTRIES: usize = 8192;
const MAX_BACKUP_OPTIMIZE_STEPS: usize = 1_000_000;

struct StorageInner {
    writer: mpsc::Sender<WriteCommand>,
    readers: Vec<mpsc::Sender<ReadCommand>>,
    next_reader: AtomicUsize,
    profile: Arc<StdMutex<QueueProfile>>,
    joins: Mutex<Vec<JoinHandle<Result<(), String>>>>,
    admission: Mutex<()>,
    lease: StdMutex<Option<File>>,
    shutting_down: AtomicBool,
    timestamp_unit: TimestampUnit,
    database_path: PathBuf,
    queue_capacity: usize,
}

#[derive(Clone)]
pub struct Storage(Arc<StorageInner>);

impl Storage {
    pub fn start(
        database_path: PathBuf,
        extension_path: PathBuf,
        reader_connections: usize,
        queue_batches: usize,
    ) -> Result<Self, String> {
        Self::start_with_timestamp_unit(
            database_path,
            extension_path,
            reader_connections,
            queue_batches,
            TimestampUnit::Milliseconds,
        )
    }

    pub fn start_with_timestamp_unit(
        database_path: PathBuf,
        extension_path: PathBuf,
        reader_connections: usize,
        queue_batches: usize,
        timestamp_unit: TimestampUnit,
    ) -> Result<Self, String> {
        if reader_connections == 0 {
            return Err("reader_connections must be positive".into());
        }
        if queue_batches == 0 {
            return Err("command_queue_batches must be positive".into());
        }
        if let Some(parent) = database_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!("create database directory {}: {error}", parent.display())
            })?;
        }
        let lease = acquire_database_lease(&database_path, "logs")?;
        let (writer_tx, writer_rx) = mpsc::channel(queue_batches);
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let profile = Arc::new(StdMutex::new(QueueProfile::default()));
        let writer_profile = Arc::clone(&profile);
        let writer_db = database_path.clone();
        let writer_ext = extension_path.clone();
        let writer_join = thread::Builder::new()
            .name("timeless-logs-writer".into())
            .spawn(move || {
                writer_main(
                    writer_db,
                    writer_ext,
                    writer_rx,
                    ready_tx,
                    writer_profile,
                    timestamp_unit,
                )
            })
            .map_err(|e| format!("spawn SQLite writer: {e}"))?;
        ready_rx
            .recv()
            .map_err(|_| "SQLite writer exited during startup".to_string())??;

        let mut readers = Vec::with_capacity(reader_connections);
        let mut joins = vec![writer_join];
        for number in 0..reader_connections {
            let (reader_tx, reader_rx) = mpsc::channel(queue_batches);
            let (ready_tx, ready_rx) = std_mpsc::channel();
            let reader_db = database_path.clone();
            let reader_ext = extension_path.clone();
            let reader_profile = Arc::clone(&profile);
            let join = thread::Builder::new()
                .name(format!("timeless-logs-reader-{number}"))
                .spawn(move || {
                    reader_main(reader_db, reader_ext, reader_rx, ready_tx, reader_profile)
                })
                .map_err(|e| format!("spawn SQLite reader {number}: {e}"))?;
            ready_rx
                .recv()
                .map_err(|_| format!("SQLite reader {number} exited during startup"))??;
            readers.push(reader_tx);
            joins.push(join);
        }

        Ok(Storage(Arc::new(StorageInner {
            writer: writer_tx,
            readers,
            next_reader: AtomicUsize::new(0),
            profile,
            joins: Mutex::new(joins),
            admission: Mutex::new(()),
            lease: StdMutex::new(Some(lease)),
            shutting_down: AtomicBool::new(false),
            timestamp_unit,
            database_path,
            queue_capacity: queue_batches,
        })))
    }

    pub fn timestamp_unit(&self) -> TimestampUnit {
        self.0.timestamp_unit
    }

    pub fn is_ready(&self) -> bool {
        !self.0.shutting_down.load(Ordering::Acquire)
    }

    pub async fn ingest(&self, entries: Vec<LogEntry>) -> Result<usize, String> {
        let _admission = self.0.admission.lock().await;
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("logs data plane is shutting down".into());
        }
        let count = entries.len();
        let permit = self
            .0
            .writer
            .reserve()
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.pending.push_back((Instant::now(), count));
            profile.admitted_batches = profile.admitted_batches.saturating_add(1);
            profile.admitted_entries = profile.admitted_entries.saturating_add(count as u64);
        }
        permit.send(WriteCommand::Ingest(entries));
        Ok(count)
    }

    pub fn record_parse(&self, duration: Duration) {
        let mut profile = profile_lock(&self.0.profile);
        profile.parse_ns = profile.parse_ns.saturating_add(duration_ns(duration));
    }

    pub async fn schedule_flush(&self) -> Result<(), String> {
        self.0
            .writer
            .send(WriteCommand::Flush(None))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())
    }

    pub async fn flush(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Flush(Some(reply_tx)))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before flush completed".to_string())?
    }

    pub async fn schedule_optimize(&self) -> Result<(), String> {
        self.0
            .writer
            .send(WriteCommand::Optimize)
            .await
            .map_err(|_| "SQLite writer is not running".to_string())
    }

    /// Ordered API test/administration barrier. It changes no storage state;
    /// the reply only proves all previously admitted batches reached the
    /// established extension ingest path.
    pub async fn barrier(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Barrier(reply_tx))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before barrier".to_string())
    }

    pub async fn backup(&self, destination: PathBuf) -> Result<BackupReport, String> {
        let _ordered = self.0.admission.lock().await;
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("logs API is shutting down; backup is closed".into());
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

    pub async fn query(&self, spec: QuerySpec) -> Result<Vec<QueryRow>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader()
            .send(ReadCommand::Query(spec, reply_tx))
            .await
            .map_err(|_| "SQLite reader is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite reader stopped before query completed".to_string())?
    }

    pub async fn count(&self, spec: QuerySpec) -> Result<i64, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader()
            .send(ReadCommand::Count(spec, reply_tx))
            .await
            .map_err(|_| "SQLite reader is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite reader stopped before count completed".to_string())?
    }

    pub async fn field_values(
        &self,
        spec: QuerySpec,
        key: String,
        limit: usize,
    ) -> Result<Vec<String>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader()
            .send(ReadCommand::FieldValues(spec, key, limit, reply_tx))
            .await
            .map_err(|_| "SQLite reader is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite reader stopped before field discovery completed".to_string())?
    }

    pub async fn stats(&self) -> Result<StorageStats, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader()
            .send(ReadCommand::Stats(reply_tx))
            .await
            .map_err(|_| "SQLite reader is not running".to_string())?;
        let mut stats = reply_rx
            .await
            .map_err(|_| "SQLite reader stopped before stats completed".to_string())??;
        let profile = profile_lock(&self.0.profile);
        stats.queued_batches = profile.pending.len() as i64;
        stats.queued_entries = profile.pending.iter().map(|(_, count)| *count as i64).sum();
        stats.oldest_queued_ms = profile
            .pending
            .front()
            .map(|(queued_at, _)| queued_at.elapsed().as_millis() as i64)
            .unwrap_or(0);
        stats.admitted_batches = profile.admitted_batches as i64;
        stats.admitted_entries = profile.admitted_entries as i64;
        stats.completed_batches = profile.completed_batches as i64;
        stats.completed_entries = profile.completed_entries as i64;
        stats.api_parse_ns = profile.parse_ns as i64;
        stats.api_batch_encode_ns = profile.batch_encode_ns as i64;
        stats.api_sqlite_insert_ns = profile.sqlite_insert_ns as i64;
        stats.api_queue_wait_ns = profile.queue_wait_ns as i64;
        stats.api_queue_wait_max_ns = profile.queue_wait_max_ns as i64;
        stats.api_query_count = profile.query_count as i64;
        stats.api_query_ns = profile.query_ns as i64;
        stats.checkpoint_count = profile.checkpoint_count as i64;
        stats.checkpoint_total_ns = profile.checkpoint_total_ns as i64;
        stats.checkpoint_errors = profile.checkpoint_errors as i64;
        stats.backup_count = profile.backup_count as i64;
        stats.backup_total_ns = profile.backup_total_ns as i64;
        stats.backup_errors = profile.backup_errors as i64;
        stats.last_error.clone_from(&profile.last_error);
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

    fn reader(&self) -> &mpsc::Sender<ReadCommand> {
        let number = self.0.next_reader.fetch_add(1, Ordering::Relaxed);
        &self.0.readers[number % self.0.readers.len()]
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        if self.0.shutting_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let _admission = self.0.admission.lock().await;
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
        let joins = {
            let mut guard = self.0.joins.lock().await;
            std::mem::take(&mut *guard)
        };
        for join in joins {
            join.join()
                .map_err(|_| "SQLite API worker panicked".to_string())??;
        }
        if let Some(file) = self
            .0
            .lease
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            FileExt::unlock(&file)
                .map_err(|error| format!("release database owner lease: {error}"))?;
        }
        writer_result
    }
}

fn writer_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<WriteCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
    profile: Arc<StdMutex<QueueProfile>>,
    timestamp_unit: TimestampUnit,
) -> Result<(), String> {
    let conn = match open_connection(&database_path, &extension_path, Some(timestamp_unit)) {
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
            WriteCommand::Ingest(entries) => {
                let count = entries.len();
                record_queue_start(&profile);
                let result = insert_batch(&conn, &entries, &profile);
                record_queue_completion(&profile, count, result.is_ok());
                result?;
            }
            WriteCommand::Flush(reply) => {
                let result = conn
                    .execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                    .map(|_| ())
                    .map_err(|e| format!("flush logs: {e}"));
                if let Some(reply) = reply {
                    let _ = reply.send(result.clone());
                }
                result?;
            }
            WriteCommand::Optimize => {
                optimize_backlog(&conn)?;
            }
            WriteCommand::Barrier(reply) => {
                let _ = reply.send(());
            }
            WriteCommand::Backup { destination, reply } => {
                let started = Instant::now();
                let result = (|| {
                    conn.execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                        .map_err(|error| format!("flush logs for backup: {error}"))?;
                    optimize_all_backlog(&conn)?;
                    let checkpoint_started = Instant::now();
                    let checkpoint = checkpoint_wal(&conn, "logs");
                    record_checkpoint(&profile, checkpoint_started.elapsed(), &checkpoint);
                    let checkpoint = checkpoint?;
                    create_verified_backup(&conn, &destination, "logs", checkpoint)
                })();
                record_backup(&profile, started.elapsed(), &result);
                let _ = reply.send(result);
            }
            WriteCommand::Shutdown(reply) => {
                let flush = conn
                    .execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                    .map(|_| ())
                    .map_err(|e| format!("graceful logs flush: {e}"));
                let checkpoint_started = Instant::now();
                let checkpoint = checkpoint_wal(&conn, "logs").map(|_| ());
                record_checkpoint(&profile, checkpoint_started.elapsed(), &checkpoint);
                let result = match (flush, checkpoint) {
                    (Err(error), _) | (Ok(()), Err(error)) => Err(error),
                    (Ok(()), Ok(())) => Ok(()),
                };
                let _ = reply.send(result.clone());
                return result;
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OptimizeBacklog {
    pending_raw_blocks: u64,
    pending_raw_entries: u64,
    merge_ready_groups: u64,
    merge_ready_blocks: u64,
    merge_ready_entries: u64,
}

impl OptimizeBacklog {
    fn actionable_entries(self) -> u64 {
        self.pending_raw_entries
            .saturating_add(self.merge_ready_entries)
    }
}

fn optimize_made_progress(before: OptimizeBacklog, after: OptimizeBacklog) -> bool {
    after.actionable_entries() == 0 || after != before
}

fn optimize_backlog_state(conn: &Connection) -> Result<OptimizeBacklog, String> {
    let stats = stat_values(conn)?;
    let stat = |key: &str| stats.get(key).copied().unwrap_or(0).max(0) as u64;
    Ok(OptimizeBacklog {
        pending_raw_blocks: stat("optimize_pending_raw_blocks"),
        pending_raw_entries: stat("optimize_pending_raw_entries"),
        merge_ready_groups: stat("optimize_merge_ready_groups"),
        merge_ready_blocks: stat("optimize_merge_ready_blocks"),
        merge_ready_entries: stat("optimize_merge_ready_entries"),
    })
}

fn optimize_backlog(conn: &Connection) -> Result<(), String> {
    let actionable_entries = optimize_backlog_state(conn)?.actionable_entries();
    if actionable_entries == 0 {
        return Ok(());
    }
    optimize_backlog_with_actionable(conn, actionable_entries)
}

fn optimize_backlog_with_actionable(
    conn: &Connection,
    actionable_entries: u64,
) -> Result<(), String> {
    // The exact planner identifies actionable entries. Blob bytes are sampled
    // over raw/small candidates because payload length is intentionally not
    // part of the extension's in-memory metadata index.
    let (sample_entries, sample_bytes): (i64, i64) = conn
        .query_row(
            "SELECT COALESCE(SUM(entry_count), 0),
                    COALESCE(SUM(length(data)), 0)
             FROM logs_blocks
             WHERE codec IN (1, 6) OR entry_count < 8192",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| format!("inspect optimize source bytes: {error}"))?;
    let budget = optimize_entry_budget(
        actionable_entries,
        sample_entries.max(0) as u64,
        sample_bytes.max(0) as u64,
    );
    conn.execute(
        "INSERT INTO logs(logs) VALUES (?1)",
        [format!("optimize:{budget}")],
    )
    .map_err(|error| format!("optimize logs with {budget}-entry budget: {error}"))?;
    Ok(())
}

fn optimize_all_backlog(conn: &Connection) -> Result<(), String> {
    for step in 0..MAX_BACKUP_OPTIMIZE_STEPS {
        let before = optimize_backlog_state(conn)?;
        let actionable = before.actionable_entries();
        if actionable == 0 {
            return Ok(());
        }
        optimize_backlog_with_actionable(conn, actionable)?;
        let after = optimize_backlog_state(conn)?;
        if after.actionable_entries() == 0 {
            return Ok(());
        }
        if !optimize_made_progress(before, after) {
            return Err(format!(
                "logs optimize backlog made no progress at step {step}: {after:?}"
            ));
        }
    }
    Err(format!(
        "logs optimize backlog exceeded {MAX_BACKUP_OPTIMIZE_STEPS} steps"
    ))
}

fn optimize_entry_budget(actionable_entries: u64, sample_entries: u64, sample_bytes: u64) -> usize {
    if actionable_entries == 0 {
        return 0;
    }
    let target_entries = OPTIMIZE_TARGET_ENTRIES as u64;
    if sample_entries == 0 || sample_bytes == 0 {
        return usize::try_from(actionable_entries.min(target_entries)).unwrap_or(usize::MAX);
    }
    let estimated = (u128::from(OPTIMIZE_SOURCE_BYTE_BUDGET)
        .saturating_mul(u128::from(sample_entries))
        .saturating_add(u128::from(sample_bytes - 1))
        / u128::from(sample_bytes))
    .min(u128::from(u64::MAX)) as u64;
    usize::try_from(actionable_entries.min(estimated.max(target_entries))).unwrap_or(usize::MAX)
}

fn reader_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<ReadCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
    profile: Arc<StdMutex<QueueProfile>>,
) -> Result<(), String> {
    let conn = match open_connection(&database_path, &extension_path, None) {
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
            ReadCommand::Query(spec, reply) => {
                let started = Instant::now();
                let result = retry_read(|| query_rows(&conn, &spec));
                record_query(&profile, started.elapsed());
                let _ = reply.send(result);
            }
            ReadCommand::Count(spec, reply) => {
                let started = Instant::now();
                let result = retry_read(|| query_count(&conn, &spec));
                record_query(&profile, started.elapsed());
                let _ = reply.send(result);
            }
            ReadCommand::FieldValues(spec, key, limit, reply) => {
                let started = Instant::now();
                let result = retry_read(|| query_field_values(&conn, &spec, &key, limit));
                record_query(&profile, started.elapsed());
                let _ = reply.send(result);
            }
            ReadCommand::Stats(reply) => {
                let _ = reply.send(retry_read(|| storage_stats(&conn)));
            }
            ReadCommand::Shutdown => return Ok(()),
        }
    }
    Ok(())
}

fn open_connection(
    path: &Path,
    extension: &Path,
    initialize: Option<TimestampUnit>,
) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    unsafe {
        conn.load_extension_enable()
            .map_err(|e| format!("enable extension loading: {e}"))?;
        conn.load_extension(extension, None::<&str>)
            .map_err(|e| format!("load {}: {e}", extension.display()))?;
    }
    conn.load_extension_disable()
        .map_err(|e| format!("disable extension loading: {e}"))?;
    let spec = DataPlaneSpec {
        signal: "logs",
        required_batch: "rich-v1",
    };
    let capabilities = preflight_extension(&conn, spec)?;
    preflight_database(&conn, spec.signal)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| format!("set busy timeout: {e}"))?;
    if let Some(timestamp_unit) = initialize {
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA auto_vacuum = INCREMENTAL;
             CREATE VIRTUAL TABLE IF NOT EXISTS logs USING timeless_logs(
               index_keys='service,path,status,host', timestamp_unit='{}');",
            timestamp_unit.sql_name()
        ))
        .map_err(|e| format!("initialize logs database: {e}"))?;
        apply_schema_ledger(&conn, spec, &capabilities)?;
        let stored: Option<String> = conn
            .query_row(
                "SELECT CAST(v AS TEXT) FROM logs_meta WHERE k = 'timestamp_unit'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| format!("read logs timestamp capability: {e}"))?;
        if stored.as_deref() != Some(timestamp_unit.sql_name()) {
            return Err(format!(
                "logs timestamp capability mismatch: binary requested {}, database stores {}",
                timestamp_unit.sql_name(),
                stored.as_deref().unwrap_or("<missing>")
            ));
        }
    } else {
        require_current_schema(&conn, spec.signal)?;
    }
    Ok(conn)
}

/// The extension deliberately reports a retryable conflict when a reader
/// reaches a shared engine during the writer's short virtual-table
/// transaction. HTTP callers should wait behind that publication boundary,
/// not receive a spurious 500. This is API scheduling only; it does not alter
/// the engine, its buffer, or its transactions.
fn retry_read<T>(mut operation: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error)
                if std::time::Instant::now() < deadline
                    && (error.contains("active write transaction")
                        || error.contains("pending writer transaction")
                        || error.contains("database is locked")
                        || error.contains("database is busy")) =>
            {
                thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
}

fn profile_lock(profile: &StdMutex<QueueProfile>) -> std::sync::MutexGuard<'_, QueueProfile> {
    profile.lock().unwrap_or_else(|error| error.into_inner())
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn elapsed_ns(started: Instant) -> u64 {
    duration_ns(started.elapsed())
}

fn record_queue_start(profile: &StdMutex<QueueProfile>) {
    let mut profile = profile_lock(profile);
    if let Some((queued_at, _)) = profile.pending.front() {
        let wait_ns = elapsed_ns(*queued_at);
        profile.queue_wait_ns = profile.queue_wait_ns.saturating_add(wait_ns);
        profile.queue_wait_max_ns = profile.queue_wait_max_ns.max(wait_ns);
    }
}

fn record_queue_completion(profile: &StdMutex<QueueProfile>, count: usize, success: bool) {
    let mut profile = profile_lock(profile);
    let queued = profile.pending.pop_front();
    debug_assert_eq!(queued.map(|(_, queued_count)| queued_count), Some(count));
    if success {
        profile.completed_batches = profile.completed_batches.saturating_add(1);
        profile.completed_entries = profile.completed_entries.saturating_add(count as u64);
    }
}

fn record_query(profile: &StdMutex<QueueProfile>, duration: Duration) {
    let mut profile = profile_lock(profile);
    profile.query_count = profile.query_count.saturating_add(1);
    profile.query_ns = profile.query_ns.saturating_add(duration_ns(duration));
}

fn insert_batch(
    conn: &Connection,
    entries: &[LogEntry],
    profile: &StdMutex<QueueProfile>,
) -> Result<(), String> {
    if entries.is_empty() {
        return Ok(());
    }
    let encode_started = Instant::now();
    let blob = encode_batch(entries)?;
    let encode_ns = elapsed_ns(encode_started);
    let insert_started = Instant::now();
    let result = conn
        .execute("INSERT INTO logs(logs) VALUES (?1)", params![blob])
        .map(|_| ())
        .map_err(|e| format!("insert logs batch: {e}"));
    let insert_ns = elapsed_ns(insert_started);
    let mut profile = profile_lock(profile);
    profile.batch_encode_ns = profile.batch_encode_ns.saturating_add(encode_ns);
    profile.sqlite_insert_ns = profile.sqlite_insert_ns.saturating_add(insert_ns);
    result
}

fn encode_batch(entries: &[LogEntry]) -> Result<Vec<u8>, String> {
    let count = u32::try_from(entries.len()).map_err(|_| "log batch exceeds u32::MAX entries")?;
    let mut out = Vec::with_capacity(8 + entries.len() * 80);
    out.push(0x02);
    out.push(0x00);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.ts.to_le_bytes());
    }
    for entry in entries {
        if entry.level > 3 {
            return Err(format!("invalid log level {}", entry.level));
        }
        push_string(&mut out, &entry.severity)?;
    }
    for entry in entries {
        push_string(&mut out, &entry.message)?;
    }
    for entry in entries {
        push_string(&mut out, &entry.metadata_json)?;
    }
    Ok(out)
}

fn push_string(out: &mut Vec<u8>, value: &str) -> Result<(), String> {
    let len = u32::try_from(value.len()).map_err(|_| "log string exceeds u32::MAX bytes")?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn query_parts(spec: &QuerySpec) -> Result<(String, Vec<SqlValue>), String> {
    let mut clauses = Vec::new();
    let mut values = Vec::new();
    if let Some(level) = &spec.level {
        clauses.push("level = ?");
        values.push(SqlValue::Text(level.clone()));
    }
    if let Some(service) = &spec.service {
        clauses.push("service = ?");
        values.push(SqlValue::Text(service.clone()));
    }
    for (key, value) in &spec.metadata_eq {
        if !matches!(key.as_str(), "service" | "host" | "path" | "status") {
            return Err(format!("unsupported indexed log metadata field {key:?}"));
        }
        clauses.push(match key.as_str() {
            "service" => "service = ?",
            "host" => "host = ?",
            "path" => "path = ?",
            "status" => "status = ?",
            _ => unreachable!("metadata field validated above"),
        });
        values.push(SqlValue::Text(value.clone()));
    }
    if let Some(message) = &spec.message {
        clauses.push("message_contains = ?");
        values.push(SqlValue::Text(message.clone()));
    }
    if let Some(ts_min) = spec.ts_min {
        clauses.push("ts >= ?");
        values.push(SqlValue::Integer(ts_min));
    }
    if let Some(ts_max) = spec.ts_max {
        clauses.push("ts <= ?");
        values.push(SqlValue::Integer(ts_max));
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    Ok((where_sql, values))
}

fn query_rows(conn: &Connection, spec: &QuerySpec) -> Result<Vec<QueryRow>, String> {
    let (where_sql, mut values) = query_parts(spec)?;
    let order = if spec.descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT ts, level, message, metadata FROM logs{where_sql} \
         ORDER BY ts {order} LIMIT ? OFFSET ?"
    );
    values.push(SqlValue::Integer(spec.limit.clamp(1, 100_000) as i64));
    values.push(SqlValue::Integer(spec.offset as i64));
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("prepare query: {e}"))?;
    let rows = stmt
        .query_map(params_from_iter(values), |row| {
            Ok(QueryRow {
                ts: row.get(0)?,
                level: row.get(1)?,
                message: row.get(2)?,
                metadata_json: row.get(3)?,
            })
        })
        .map_err(|e| format!("query logs: {e}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read log row: {e}"))
}

fn query_count(conn: &Connection, spec: &QuerySpec) -> Result<i64, String> {
    let mut filter = BTreeMap::new();
    if let Some(level) = &spec.level {
        filter.insert("level", level);
    }
    if let Some(service) = &spec.service {
        filter.insert("service", service);
    }
    for (key, value) in &spec.metadata_eq {
        filter.insert(key, value);
    }
    let filter_json = if filter.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&filter)
                .map_err(|error| format!("encode native count filter: {error}"))?,
        )
    };
    conn.query_row(
        "SELECT n FROM timeless_log_count('logs', ?1, ?2, ?3, ?4)",
        params![
            filter_json,
            spec.message.as_deref(),
            spec.ts_min.unwrap_or(i64::MIN),
            spec.ts_max.unwrap_or(i64::MAX)
        ],
        |row| row.get(0),
    )
    .map_err(|e| format!("count logs: {e}"))
}

fn query_field_values(
    conn: &Connection,
    spec: &QuerySpec,
    key: &str,
    limit: usize,
) -> Result<Vec<String>, String> {
    if !matches!(key, "service" | "host" | "path" | "status") {
        return Err(format!("unsupported indexed log field {key:?}"));
    }
    let mut filter = BTreeMap::new();
    if let Some(level) = &spec.level {
        filter.insert("level", level);
    }
    if let Some(service) = &spec.service {
        filter.insert("service", service);
    }
    for (filter_key, value) in &spec.metadata_eq {
        filter.insert(filter_key, value);
    }
    let filter_json = if filter.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&filter)
                .map_err(|error| format!("encode field-values filter: {error}"))?,
        )
    };
    let mut statement = conn
        .prepare("SELECT value FROM timeless_log_values('logs', ?1, ?2, ?3, ?4, ?5, ?6)")
        .map_err(|error| format!("prepare log field-values query: {error}"))?;
    let values = statement
        .query_map(
            params![
                key,
                filter_json,
                spec.message.as_deref(),
                spec.ts_min,
                spec.ts_max,
                i64::try_from(limit).unwrap_or(i64::MAX)
            ],
            |row| row.get(0),
        )
        .map_err(|error| format!("query log field values: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read log field value: {error}"))?;
    Ok(values)
}

fn storage_stats(conn: &Connection) -> Result<StorageStats, String> {
    let engine = stat_values(conn)?;
    let stat = |key: &str| engine.get(key).copied().unwrap_or(0);
    let buffered = stat("buffered_entries");
    let (blocks, disk_entries, bytes, raw_blocks, raw_bytes, oldest, newest): (
        i64,
        i64,
        i64,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
    ) = conn
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(entry_count),0), COALESCE(SUM(length(data)),0),
                    COALESCE(SUM(codec IN (1, 6)),0),
                    COALESCE(SUM(CASE WHEN codec IN (1, 6) THEN length(data) ELSE 0 END),0),
                    MIN(ts_min), MAX(ts_max)
             FROM logs_blocks",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .map_err(|e| format!("read block stats: {e}"))?;
    let (page_count, page_size, freelist_pages): (i64, i64, i64) = conn
        .query_row(
            "SELECT (SELECT page_count FROM pragma_page_count),
                    (SELECT page_size FROM pragma_page_size),
                    (SELECT freelist_count FROM pragma_freelist_count)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|e| format!("read database size: {e}"))?;
    let page_bytes = page_count.saturating_mul(page_size);
    let index_bytes: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(pgsize), 0)
               FROM dbstat
              WHERE name IN (
                'logs_terms',
                'logs_blocks_ts',
                'logs_meta',
                'sqlite_autoindex_logs_meta_1'
              )",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("read database index size: {e}"))?;
    let term_postings: i64 = conn
        .query_row("SELECT COUNT(*) FROM logs_terms", [], |row| row.get(0))
        .map_err(|e| format!("read term count: {e}"))?;
    Ok(StorageStats {
        total_blocks: blocks,
        total_entries: disk_entries + buffered,
        total_bytes: bytes,
        disk_size: page_bytes,
        index_size: index_bytes,
        database_file_bytes: 0,
        database_wal_bytes: 0,
        database_shm_bytes: 0,
        physical_database_bytes: 0,
        sqlite_page_bytes: page_bytes,
        freelist_pages,
        freelist_bytes: freelist_pages.saturating_mul(page_size),
        writer_connections: 0,
        reader_connections: 0,
        command_queue_capacity_batches: 0,
        term_postings,
        oldest_timestamp: oldest,
        newest_timestamp: newest,
        raw_blocks,
        raw_bytes,
        compressed_blocks: blocks - raw_blocks,
        compressed_bytes: bytes - raw_bytes,
        buffered_entries: buffered,
        queued_batches: 0,
        queued_entries: 0,
        oldest_queued_ms: 0,
        admitted_batches: 0,
        admitted_entries: 0,
        completed_batches: 0,
        completed_entries: 0,
        api_parse_ns: 0,
        api_batch_encode_ns: 0,
        api_sqlite_insert_ns: 0,
        api_queue_wait_ns: 0,
        api_queue_wait_max_ns: 0,
        api_query_count: 0,
        api_query_ns: 0,
        ingest_batch_count: stat("ingest_batch_count"),
        ingest_batch_entries: stat("ingest_batch_entries"),
        ingest_wire_decode_ns: stat("ingest_wire_decode_ns"),
        ingest_normalize_ns: stat("ingest_normalize_ns"),
        ingest_buffer_append_ns: stat("ingest_buffer_append_ns"),
        flush_count: stat("flush_count"),
        flush_entries: stat("flush_entries"),
        flush_total_ns: stat("flush_total_ns"),
        flush_partition_ns: stat("flush_partition_ns"),
        flush_encode_terms_ns: stat("flush_encode_terms_ns"),
        flush_store_ns: stat("flush_store_ns"),
        query_count: stat("query_count"),
        query_total_ns: stat("query_total_ns"),
        query_snapshot_ns: stat("query_snapshot_ns"),
        query_materialize_ns: stat("query_materialize_ns"),
        query_snapshot_payload_bytes: stat("query_snapshot_payload_bytes"),
        query_snapshot_payload_max_bytes: stat("query_snapshot_payload_max_bytes"),
        query_snapshot_buffered_entries: stat("query_snapshot_buffered_entries"),
        query_stable_location_snapshots: stat("query_stable_location_snapshots"),
        query_payload_bytes_read: stat("query_payload_bytes_read"),
        query_candidate_blocks: stat("query_candidate_blocks"),
        query_decoded_entries: stat("query_decoded_entries"),
        query_matched_entries: stat("query_matched_entries"),
        query_returned_entries: stat("query_returned_entries"),
        query_bounded_count: stat("query_bounded_count"),
        query_bounded_requested_entries: stat("query_bounded_requested_entries"),
        query_bounded_max_entries: stat("query_bounded_max_entries"),
        query_blocks_skipped_by_bound: stat("query_blocks_skipped_by_bound"),
        native_count_count: stat("native_count_count"),
        native_count_total_ns: stat("native_count_total_ns"),
        native_count_snapshot_ns: stat("native_count_snapshot_ns"),
        native_count_payload_bytes_read: stat("native_count_payload_bytes_read"),
        native_count_metadata_blocks: stat("native_count_metadata_blocks"),
        native_count_metadata_entries: stat("native_count_metadata_entries"),
        native_count_decoded_blocks: stat("native_count_decoded_blocks"),
        native_count_decoded_entries: stat("native_count_decoded_entries"),
        optimize_count: stat("optimize_count"),
        optimize_total_ns: stat("optimize_total_ns"),
        optimize_blocks_removed: stat("optimize_blocks_removed"),
        optimize_blocks_written: stat("optimize_blocks_written"),
        optimize_budgeted_count: stat("optimize_budgeted_count"),
        optimize_budget_entries: stat("optimize_budget_entries"),
        optimize_budget_limited_count: stat("optimize_budget_limited_count"),
        optimize_raw_groups: stat("optimize_raw_groups"),
        optimize_raw_blocks: stat("optimize_raw_blocks"),
        optimize_raw_entries: stat("optimize_raw_entries"),
        optimize_raw_input_bytes: stat("optimize_raw_input_bytes"),
        optimize_raw_output_bytes: stat("optimize_raw_output_bytes"),
        optimize_raw_total_ns: stat("optimize_raw_total_ns"),
        optimize_merge_groups: stat("optimize_merge_groups"),
        optimize_merge_blocks: stat("optimize_merge_blocks"),
        optimize_merge_entries: stat("optimize_merge_entries"),
        optimize_merge_input_bytes: stat("optimize_merge_input_bytes"),
        optimize_merge_output_bytes: stat("optimize_merge_output_bytes"),
        optimize_merge_total_ns: stat("optimize_merge_total_ns"),
        optimize_pending_raw_blocks: stat("optimize_pending_raw_blocks"),
        optimize_pending_raw_entries: stat("optimize_pending_raw_entries"),
        optimize_merge_ready_groups: stat("optimize_merge_ready_groups"),
        optimize_merge_ready_blocks: stat("optimize_merge_ready_blocks"),
        optimize_merge_ready_entries: stat("optimize_merge_ready_entries"),
        optimize_merge_deferred_blocks: stat("optimize_merge_deferred_blocks"),
        optimize_merge_deferred_entries: stat("optimize_merge_deferred_entries"),
        read_permit_count: stat("read_permit_count"),
        read_permit_hold_ns: stat("read_permit_hold_ns"),
        read_conflicts: stat("read_conflicts"),
        read_barge_rejections: stat("read_barge_rejections"),
        waiting_writers: stat("waiting_writers"),
        writer_wait_count: stat("writer_wait_count"),
        writer_wait_ns: stat("writer_wait_ns"),
        writer_timeouts: stat("writer_timeouts"),
        checkpoint_count: 0,
        checkpoint_total_ns: 0,
        checkpoint_errors: 0,
        backup_count: 0,
        backup_total_ns: 0,
        backup_errors: 0,
        last_error: None,
    })
}

fn record_checkpoint<T>(
    profile: &StdMutex<QueueProfile>,
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
    profile: &StdMutex<QueueProfile>,
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
        .map(|value| value.len())
        .unwrap_or(0)
}

fn stat_values(conn: &Connection) -> Result<HashMap<String, i64>, String> {
    let mut stmt = conn
        .prepare("SELECT key, CAST(value AS INTEGER) FROM timeless_stats('logs')")
        .map_err(|e| format!("prepare timeless_stats: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .map_err(|e| format!("read timeless_stats: {e}"))?;
    let mut values = HashMap::new();
    for row in rows {
        let (key, value) = row.map_err(|e| format!("collect timeless_stats: {e}"))?;
        if let Some(value) = value {
            values.insert(key, value);
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn owner_lease_is_exclusive_and_recoverable() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("logs.db");
        let first = acquire_database_lease(&database, "logs").unwrap();
        let error = acquire_database_lease(&database, "logs").unwrap_err();
        assert!(error.contains("already owned"), "{error}");
        FileExt::unlock(&first).unwrap();
        acquire_database_lease(&database, "logs").unwrap();
    }

    #[test]
    fn batch_encoding_uses_rich_v1_header_and_exact_severity() {
        let entries = vec![LogEntry {
            ts: 42,
            level: 3,
            severity: "critical".into(),
            message: "boom".into(),
            metadata_json: "{\"service\":\"api\"}".into(),
        }];
        let blob = encode_batch(&entries).unwrap();
        assert_eq!(&blob[..4], &[2, 0, 0, 0]);
        assert_eq!(&blob[4..8], &1u32.to_le_bytes());
        assert_eq!(&blob[8..16], &42i64.to_le_bytes());
        assert_eq!(&blob[16..20], &8u32.to_le_bytes());
        assert_eq!(&blob[20..28], b"critical");
    }

    #[test]
    fn pending_writer_conflicts_are_retried_instead_of_becoming_http_errors() {
        let attempts = Cell::new(0);
        let value = retry_read(|| {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                Err("table \"logs\" read is blocked by a pending writer transaction — retry, as for SQLITE_BUSY".to_string())
            } else {
                Ok(42)
            }
        })
        .unwrap();

        assert_eq!(value, 42);
        assert_eq!(attempts.get(), 2);
    }

    #[test]
    fn optimize_budget_tracks_source_bytes_and_one_complete_group() {
        assert_eq!(optimize_entry_budget(0, 0, 0), 0);
        assert_eq!(optimize_entry_budget(4_000, 4_000, 1024), 4_000);
        assert_eq!(optimize_entry_budget(100_000, 100_000, 64 << 20), 50_000);
        assert_eq!(
            optimize_entry_budget(100_000, 100_000, 1024 << 20),
            OPTIMIZE_TARGET_ENTRIES
        );
        assert_eq!(
            optimize_entry_budget(100_000, 0, 0),
            OPTIMIZE_TARGET_ENTRIES
        );
    }

    #[test]
    fn optimize_progress_accepts_raw_to_merge_phase_expansion() {
        let raw = OptimizeBacklog {
            pending_raw_blocks: 1,
            pending_raw_entries: 1_280,
            merge_ready_groups: 0,
            merge_ready_blocks: 0,
            merge_ready_entries: 0,
        };
        let merge = OptimizeBacklog {
            pending_raw_blocks: 0,
            pending_raw_entries: 0,
            merge_ready_groups: 1,
            merge_ready_blocks: 4,
            merge_ready_entries: 4_698,
        };

        assert_ne!(raw, merge);
        assert!(merge.actionable_entries() > raw.actionable_entries());
        assert!(optimize_made_progress(raw, merge));
        assert!(!optimize_made_progress(raw, raw));
    }

    #[test]
    fn bundled_sqlite_exposes_page_accounting_for_compatible_index_bytes() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE postings(term TEXT PRIMARY KEY)", [])
            .unwrap();
        conn.execute("INSERT INTO postings VALUES ('level:error')", [])
            .unwrap();
        let bytes: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(pgsize), 0) FROM dbstat WHERE name = 'postings'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(bytes > 0);
    }
}
