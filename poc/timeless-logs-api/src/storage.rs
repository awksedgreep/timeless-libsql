use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub ts: i64,
    pub level: u8,
    pub message: String,
    pub metadata_json: String,
}

#[derive(Clone, Debug, Default)]
pub struct QuerySpec {
    pub level: Option<String>,
    pub service: Option<String>,
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

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct StorageStats {
    pub total_blocks: i64,
    pub total_entries: i64,
    pub total_bytes: i64,
    pub disk_size: i64,
    pub index_size: i64,
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
}

enum WriteCommand {
    Ingest(Vec<LogEntry>),
    Flush(Option<oneshot::Sender<Result<(), String>>>),
    Optimize,
    Barrier(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<Result<(), String>>),
}

enum ReadCommand {
    Query(QuerySpec, oneshot::Sender<Result<Vec<QueryRow>, String>>),
    Count(QuerySpec, oneshot::Sender<Result<i64, String>>),
    Stats(oneshot::Sender<Result<StorageStats, String>>),
    Shutdown,
}

// Optimize remains extension-owned. The API timer is only a maintenance
// wake-up; this byte target turns the extension's current actionable backlog
// into a bounded entry budget without adding a host-side flush/block policy.
const OPTIMIZE_SOURCE_BYTE_BUDGET: u64 = 32 * 1024 * 1024;
const OPTIMIZE_TARGET_ENTRIES: usize = 8192;

struct StorageInner {
    writer: mpsc::Sender<WriteCommand>,
    readers: Vec<mpsc::Sender<ReadCommand>>,
    next_reader: AtomicUsize,
    profile: Arc<StdMutex<QueueProfile>>,
    joins: Mutex<Vec<JoinHandle<Result<(), String>>>>,
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
        let (writer_tx, writer_rx) = mpsc::channel(queue_batches);
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let profile = Arc::new(StdMutex::new(QueueProfile::default()));
        let writer_profile = Arc::clone(&profile);
        let writer_db = database_path.clone();
        let writer_ext = extension_path.clone();
        let writer_join = thread::Builder::new()
            .name("timeless-logs-writer".into())
            .spawn(move || writer_main(writer_db, writer_ext, writer_rx, ready_tx, writer_profile))
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
        })))
    }

    pub async fn ingest(&self, entries: Vec<LogEntry>) -> Result<usize, String> {
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
        Ok(stats)
    }

    fn reader(&self) -> &mpsc::Sender<ReadCommand> {
        let number = self.0.next_reader.fetch_add(1, Ordering::Relaxed);
        &self.0.readers[number % self.0.readers.len()]
    }

    pub async fn shutdown(&self) -> Result<(), String> {
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
        writer_result
    }
}

fn writer_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<WriteCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
    profile: Arc<StdMutex<QueueProfile>>,
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
                let stats = stat_values(&conn)?;
                let actionable_entries = stats
                    .get("optimize_pending_raw_entries")
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(
                        stats
                            .get("optimize_merge_ready_entries")
                            .copied()
                            .unwrap_or(0),
                    )
                    .max(0) as u64;
                if actionable_entries > 0 {
                    // The exact planner identifies actionable entries. Blob
                    // bytes are sampled over all raw/small compressed
                    // candidates because block payload length intentionally
                    // is not part of the engine's in-memory metadata index.
                    let (sample_entries, sample_bytes): (i64, i64) = conn
                        .query_row(
                            "SELECT COALESCE(SUM(entry_count), 0),
                                    COALESCE(SUM(length(data)), 0)
                             FROM logs_blocks
                             WHERE codec = 1 OR entry_count < 8192",
                            [],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .map_err(|e| format!("inspect optimize source bytes: {e}"))?;
                    let budget = optimize_entry_budget(
                        actionable_entries,
                        sample_entries.max(0) as u64,
                        sample_bytes.max(0) as u64,
                    );
                    conn.execute(
                        "INSERT INTO logs(logs) VALUES (?1)",
                        [format!("optimize:{budget}")],
                    )
                    .map_err(|e| format!("optimize logs with {budget}-entry budget: {e}"))?;
                }
            }
            WriteCommand::Barrier(reply) => {
                let _ = reply.send(());
            }
            WriteCommand::Shutdown(reply) => {
                let result = conn
                    .execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                    .map(|_| ())
                    .map_err(|e| format!("graceful logs flush: {e}"));
                let _ = reply.send(result.clone());
                return result;
            }
        }
    }
    Ok(())
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
    let conn = match open_connection(&database_path, &extension_path, false) {
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
            ReadCommand::Stats(reply) => {
                let _ = reply.send(retry_read(|| storage_stats(&conn)));
            }
            ReadCommand::Shutdown => return Ok(()),
        }
    }
    Ok(())
}

fn open_connection(path: &Path, extension: &Path, initialize: bool) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    unsafe {
        conn.load_extension_enable()
            .map_err(|e| format!("enable extension loading: {e}"))?;
        conn.load_extension(extension, None::<&str>)
            .map_err(|e| format!("load {}: {e}", extension.display()))?;
    }
    conn.load_extension_disable()
        .map_err(|e| format!("disable extension loading: {e}"))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| format!("set busy timeout: {e}"))?;
    if initialize {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA auto_vacuum = INCREMENTAL;
             CREATE VIRTUAL TABLE IF NOT EXISTS logs USING timeless_logs(
               index_keys='service,path,status,host');",
        )
        .map_err(|e| format!("initialize logs database: {e}"))?;
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
    out.push(0x01);
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
        out.push(entry.level);
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

fn query_parts(spec: &QuerySpec) -> (String, Vec<SqlValue>) {
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
    (where_sql, values)
}

fn query_rows(conn: &Connection, spec: &QuerySpec) -> Result<Vec<QueryRow>, String> {
    let (where_sql, mut values) = query_parts(spec);
    let order = if spec.descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT ts, level, message, metadata FROM logs{where_sql} \
         ORDER BY ts {order} LIMIT ? OFFSET ?"
    );
    values.push(SqlValue::Integer(spec.limit.max(1).min(100_000) as i64));
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
                    COALESCE(SUM(codec=1),0),
                    COALESCE(SUM(CASE WHEN codec=1 THEN length(data) ELSE 0 END),0),
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
    let page_bytes: i64 = conn
        .query_row(
            "SELECT (SELECT page_count FROM pragma_page_count) *
                    (SELECT page_size FROM pragma_page_size)",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("read database size: {e}"))?;
    let terms: i64 = conn
        .query_row("SELECT COUNT(*) FROM logs_terms", [], |row| row.get(0))
        .map_err(|e| format!("read term count: {e}"))?;
    Ok(StorageStats {
        total_blocks: blocks,
        total_entries: disk_entries + buffered,
        total_bytes: bytes,
        disk_size: page_bytes,
        index_size: terms,
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
    })
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
    fn batch_encoding_preserves_the_established_v0_header_and_count() {
        let entries = vec![LogEntry {
            ts: 42,
            level: 3,
            message: "boom".into(),
            metadata_json: "{\"service\":\"api\"}".into(),
        }];
        let blob = encode_batch(&entries).unwrap();
        assert_eq!(&blob[..4], &[1, 0, 0, 0]);
        assert_eq!(&blob[4..8], &1u32.to_le_bytes());
        assert_eq!(&blob[8..16], &42i64.to_le_bytes());
        assert_eq!(blob[16], 3);
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
}
