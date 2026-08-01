use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

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

struct StorageInner {
    writer: mpsc::Sender<WriteCommand>,
    readers: Vec<mpsc::Sender<ReadCommand>>,
    next_reader: AtomicUsize,
    pending_entries: Arc<AtomicUsize>,
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
        let pending_entries = Arc::new(AtomicUsize::new(0));
        let writer_pending_entries = Arc::clone(&pending_entries);
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
                    writer_pending_entries,
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
            let join = thread::Builder::new()
                .name(format!("timeless-logs-reader-{number}"))
                .spawn(move || reader_main(reader_db, reader_ext, reader_rx, ready_tx))
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
            pending_entries,
            joins: Mutex::new(joins),
        })))
    }

    pub async fn ingest(&self, entries: Vec<LogEntry>) -> Result<usize, String> {
        let count = entries.len();
        self.0.pending_entries.fetch_add(count, Ordering::Relaxed);
        if self
            .0
            .writer
            .send(WriteCommand::Ingest(entries))
            .await
            .is_err()
        {
            self.0.pending_entries.fetch_sub(count, Ordering::Relaxed);
            return Err("SQLite writer is not running".into());
        }
        Ok(count)
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
        stats.queued_batches = (self.0.writer.max_capacity() - self.0.writer.capacity()) as i64;
        stats.queued_entries = self.0.pending_entries.load(Ordering::Relaxed) as i64;
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
    pending_entries: Arc<AtomicUsize>,
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
                let result = insert_batch(&conn, &entries);
                pending_entries.fetch_sub(count, Ordering::Relaxed);
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
                let raw: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM logs_blocks WHERE codec = 1",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|e| format!("inspect raw blocks: {e}"))?;
                if raw > 0 {
                    conn.execute("INSERT INTO logs(logs) VALUES ('optimize')", [])
                        .map_err(|e| format!("optimize logs: {e}"))?;
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

fn reader_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<ReadCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
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
                let _ = reply.send(retry_read(|| query_rows(&conn, &spec)));
            }
            ReadCommand::Count(spec, reply) => {
                let _ = reply.send(retry_read(|| query_count(&conn, &spec)));
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
                        || error.contains("database is locked")
                        || error.contains("database is busy")) =>
            {
                thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
}

fn insert_batch(conn: &Connection, entries: &[LogEntry]) -> Result<(), String> {
    if entries.is_empty() {
        return Ok(());
    }
    let blob = encode_batch(entries)?;
    conn.execute("INSERT INTO logs(logs) VALUES (?1)", params![blob])
        .map(|_| ())
        .map_err(|e| format!("insert logs batch: {e}"))
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
        clauses.push("message LIKE '%' || ? || '%'");
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
    let (where_sql, values) = query_parts(spec);
    let sql = format!("SELECT COUNT(*) FROM logs{where_sql}");
    conn.query_row(&sql, params_from_iter(values), |row| row.get(0))
        .map_err(|e| format!("count logs: {e}"))
}

fn storage_stats(conn: &Connection) -> Result<StorageStats, String> {
    let buffered = stat_value(conn, "buffered_entries")?;
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
    })
}

fn stat_value(conn: &Connection, key: &str) -> Result<i64, String> {
    conn.query_row(
        "SELECT CAST(value AS INTEGER) FROM timeless_stats('logs') WHERE key=?1",
        [key],
        |row| row.get(0),
    )
    .map_err(|e| format!("read timeless_stats {key}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
