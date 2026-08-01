use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, Transaction};
use timeless_api::{encode_logs_batch, now_ms, LogQuery, LogRecord, SortOrder};
use tokio::sync::{mpsc, oneshot};

const COMMIT_BATCH_ENTRIES: usize = 8_192;
const COMMIT_COALESCE: Duration = Duration::from_millis(2);
const READ_BUSY_RETRIES: usize = 100;
const READ_BUSY_BACKOFF: Duration = Duration::from_micros(250);

#[derive(Clone)]
pub struct Database {
    writer: mpsc::Sender<Command>,
    readers: Arc<Vec<mpsc::Sender<Command>>>,
    next_reader: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
pub enum Error {
    Overloaded,
    Stopped,
    Database(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overloaded => formatter.write_str("database queue is full"),
            Self::Stopped => formatter.write_str("database worker has stopped"),
            Self::Database(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error.to_string())
    }
}

#[derive(Debug)]
pub struct StoredLog {
    pub ts_ms: i64,
    pub level: String,
    pub message: String,
    pub metadata_json: String,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct DatabaseStats {
    pub blocks: i64,
    pub entries: i64,
    pub disk_size: u64,
}

enum Command {
    Ingest {
        entries: Vec<LogRecord>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Query {
        query: LogQuery,
        reply: oneshot::Sender<Result<Vec<StoredLog>, Error>>,
    },
    Count {
        query: LogQuery,
        reply: oneshot::Sender<Result<i64, Error>>,
    },
    Stats {
        reply: oneshot::Sender<Result<DatabaseStats, Error>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), Error>>,
    },
}

struct Worker {
    connection: Connection,
    path: PathBuf,
    index_keys: HashSet<String>,
}

impl Database {
    pub fn start(
        path: PathBuf,
        extension: PathBuf,
        index_keys: Vec<String>,
        queue_capacity: usize,
        read_workers: usize,
    ) -> Result<Self, String> {
        let worker =
            Worker::open(path.clone(), &extension, index_keys.clone()).map_err(|error| {
                format!(
                    "open database with extension {}: {error}",
                    extension.display()
                )
            })?;
        let (writer, mut receiver) = mpsc::channel(queue_capacity.max(1));
        thread::Builder::new()
            .name("timelessd-sqlite-writer".into())
            .spawn(move || {
                let mut worker = worker;
                worker.run(&mut receiver);
            })
            .map_err(|error| format!("start database worker: {error}"))?;

        let mut readers = Vec::with_capacity(read_workers.max(1));
        for number in 0..read_workers.max(1) {
            let worker = Worker::open(path.clone(), &extension, index_keys.clone())
                .map_err(|error| format!("open database read worker {number}: {error}"))?;
            let (sender, mut receiver) = mpsc::channel(queue_capacity.max(1));
            thread::Builder::new()
                .name(format!("timelessd-sqlite-reader-{number}"))
                .spawn(move || {
                    let worker = worker;
                    worker.run_reader(&mut receiver);
                })
                .map_err(|error| format!("start database read worker {number}: {error}"))?;
            readers.push(sender);
        }
        Ok(Self {
            writer,
            readers: Arc::new(readers),
            next_reader: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn ingest(&self, entries: Vec<LogRecord>) -> Result<(), Error> {
        let (reply, receive) = oneshot::channel();
        self.send_writer(Command::Ingest { entries, reply })?;
        receive.await.map_err(|_| Error::Stopped)?
    }

    pub async fn query(&self, query: LogQuery) -> Result<Vec<StoredLog>, Error> {
        let (reply, receive) = oneshot::channel();
        self.send_reader(Command::Query { query, reply })?;
        receive.await.map_err(|_| Error::Stopped)?
    }

    pub async fn count(&self, query: LogQuery) -> Result<i64, Error> {
        let (reply, receive) = oneshot::channel();
        self.send_reader(Command::Count { query, reply })?;
        receive.await.map_err(|_| Error::Stopped)?
    }

    pub async fn stats(&self) -> Result<DatabaseStats, Error> {
        let (reply, receive) = oneshot::channel();
        self.send_reader(Command::Stats { reply })?;
        receive.await.map_err(|_| Error::Stopped)?
    }

    pub async fn flush(&self) -> Result<(), Error> {
        let (reply, receive) = oneshot::channel();
        self.send_writer(Command::Flush { reply })?;
        receive.await.map_err(|_| Error::Stopped)?
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        shutdown_sender(&self.writer).await?;
        for reader in self.readers.iter() {
            shutdown_sender(reader).await?;
        }
        Ok(())
    }

    fn send_writer(&self, command: Command) -> Result<(), Error> {
        send_one(&self.writer, command)
    }

    fn send_reader(&self, mut command: Command) -> Result<(), Error> {
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed);
        let mut saw_full = false;
        for offset in 0..self.readers.len() {
            let reader = &self.readers[(start + offset) % self.readers.len()];
            match reader.try_send(command) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    saw_full = true;
                    command = returned;
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => command = returned,
            }
        }
        if saw_full {
            Err(Error::Overloaded)
        } else {
            Err(Error::Stopped)
        }
    }
}

fn send_one(sender: &mpsc::Sender<Command>, command: Command) -> Result<(), Error> {
    sender.try_send(command).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => Error::Overloaded,
        mpsc::error::TrySendError::Closed(_) => Error::Stopped,
    })
}

async fn shutdown_sender(sender: &mpsc::Sender<Command>) -> Result<(), String> {
    let (reply, receive) = oneshot::channel();
    match sender.try_send(Command::Shutdown { reply }) {
        Ok(()) => receive
            .await
            .map_err(|_| "database worker stopped before shutdown".to_owned())?
            .map_err(|error| error.to_string()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Graceful shutdown is allowed to wait for admitted work.
            let (reply, receive) = oneshot::channel();
            sender
                .send(Command::Shutdown { reply })
                .await
                .map_err(|_| "database worker already stopped".to_owned())?;
            receive
                .await
                .map_err(|_| "database worker stopped before shutdown".to_owned())?
                .map_err(|error| error.to_string())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Ok(()),
    }
}

impl Worker {
    fn open(path: PathBuf, extension: &Path, index_keys: Vec<String>) -> Result<Self, Error> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|error| {
                Error::Database(format!(
                    "create database directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let connection = Connection::open(&path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        // Exercise the exact distributable extension boundary used by SQLite
        // and libSQL hosts. A loadable extension cannot be linked as a normal
        // Rust library: its SQLite calls intentionally route through the API
        // table initialized by sqlite3_load_extension.
        unsafe {
            connection.load_extension_enable()?;
            let loaded = connection.load_extension(extension, None::<&str>);
            let disabled = connection.load_extension_disable();
            loaded?;
            disabled?;
        }
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = NORMAL;\n\
             PRAGMA foreign_keys = ON;",
        )?;
        let keys = index_keys.join(",").replace('\'', "''");
        connection.execute_batch(&format!(
            "CREATE VIRTUAL TABLE IF NOT EXISTS logs USING timeless_logs(\
             index_keys='{keys}')"
        ))?;
        Ok(Self {
            connection,
            path,
            index_keys: index_keys.into_iter().collect(),
        })
    }

    fn run(&mut self, receiver: &mut mpsc::Receiver<Command>) {
        let mut deferred = None;
        loop {
            let command = match deferred.take().or_else(|| receiver.blocking_recv()) {
                Some(command) => command,
                None => break,
            };
            match command {
                Command::Ingest { entries, reply } => {
                    self.ingest_group(entries, reply, receiver, &mut deferred);
                }
                Command::Shutdown { reply } => {
                    let _ = reply.send(self.flush());
                    break;
                }
                command => self.handle_non_ingest(command),
            }
        }
    }

    fn run_reader(self, receiver: &mut mpsc::Receiver<Command>) {
        while let Some(command) = receiver.blocking_recv() {
            match command {
                Command::Query { query, reply } => {
                    let _ = reply.send(self.retry_busy(|| self.query(&query)));
                }
                Command::Count { query, reply } => {
                    let _ = reply.send(self.retry_busy(|| self.count(&query)));
                }
                Command::Stats { reply } => {
                    let _ = reply.send(self.retry_busy(|| self.stats()));
                }
                Command::Shutdown { reply } => {
                    let _ = reply.send(Ok(()));
                    break;
                }
                Command::Ingest { reply, .. } | Command::Flush { reply } => {
                    let _ = reply.send(Err(Error::Database(
                        "write command routed to a read worker".into(),
                    )));
                }
            }
        }
    }

    /// Coalesce only ADJACENT writes, preserving the observable order around
    /// queries and control commands. The 2ms window turns concurrent small
    /// HTTP posts into extension-sized commits while every reply still waits
    /// for the shared transaction to become durable.
    fn ingest_group(
        &mut self,
        first_entries: Vec<LogRecord>,
        first_reply: oneshot::Sender<Result<(), Error>>,
        receiver: &mut mpsc::Receiver<Command>,
        deferred: &mut Option<Command>,
    ) {
        let mut entries = first_entries;
        let mut replies = vec![first_reply];
        let mut waited = false;
        loop {
            match receiver.try_recv() {
                Ok(Command::Ingest {
                    entries: next,
                    reply,
                }) if entries.len().saturating_add(next.len()) <= COMMIT_BATCH_ENTRIES => {
                    entries.extend(next);
                    replies.push(reply);
                }
                Ok(command) => {
                    *deferred = Some(command);
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty)
                    if !waited && entries.len() < COMMIT_BATCH_ENTRIES =>
                {
                    thread::sleep(COMMIT_COALESCE);
                    waited = true;
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
            if entries.len() >= COMMIT_BATCH_ENTRIES {
                break;
            }
        }
        let result = self.ingest(&entries);
        for reply in replies {
            let _ = reply.send(result.clone());
        }
    }

    fn handle_non_ingest(&mut self, command: Command) {
        match command {
            Command::Flush { reply } => {
                let _ = reply.send(self.flush());
            }
            Command::Ingest { .. }
            | Command::Query { .. }
            | Command::Count { .. }
            | Command::Stats { .. }
            | Command::Shutdown { .. } => unreachable!(),
        }
    }

    fn ingest(&mut self, entries: &[LogRecord]) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let blob = encode_logs_batch(entries);
        let transaction = self.connection.transaction()?;
        transaction.execute("INSERT INTO logs(logs) VALUES (?1)", params![blob])?;
        flush_transaction(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.connection
            .execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
        Ok(())
    }

    fn query(&self, query: &LogQuery) -> Result<Vec<StoredLog>, Error> {
        let (where_sql, mut values) = self.where_clause(query);
        values.push(Value::Integer(query.limit as i64));
        values.push(Value::Integer(query.offset as i64));
        let order = match query.order {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        };
        let sql = format!(
            "SELECT ts, level, message, metadata FROM logs {where_sql} \
             ORDER BY ts {order} LIMIT ? OFFSET ?"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), |row| {
            Ok(StoredLog {
                ts_ms: row.get(0)?,
                level: row.get(1)?,
                message: row.get(2)?,
                metadata_json: row.get(3)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Error::from)
    }

    fn count(&self, query: &LogQuery) -> Result<i64, Error> {
        // Reuse the extension's metadata-counting bucket kernel when its
        // exact fast-path applies. This avoids materializing every matching
        // error log for DDNet's common `_time:1h level:error | stats count`.
        if let (true, true, Some(start)) = (
            query.message.is_none(),
            query.metadata.is_empty(),
            query.since_ms,
        ) {
            let stop = query.until_ms.unwrap_or_else(now_ms);
            if stop < start {
                return Ok(0);
            }
            let step = (stop as i128 - start as i128 + 1).clamp(1, i64::MAX as i128) as i64;
            let filter = query
                .level
                .map(|level| format!("{{\"level\":\"{}\"}}", level.as_str()))
                .unwrap_or_else(|| "{}".to_owned());
            return self
                .connection
                .query_row(
                    "SELECT COALESCE(SUM(n), 0) \
                     FROM timeless_log_buckets('logs', 'level', ?1, ?2, ?3, ?4)",
                    params![filter, start, stop, step],
                    |row| row.get(0),
                )
                .map_err(Error::from);
        }
        let (where_sql, values) = self.where_clause(query);
        self.connection
            .query_row(
                &format!("SELECT COUNT(*) FROM logs {where_sql}"),
                params_from_iter(values),
                |row| row.get(0),
            )
            .map_err(Error::from)
    }

    fn where_clause(&self, query: &LogQuery) -> (String, Vec<Value>) {
        let mut clauses = Vec::new();
        let mut values = Vec::new();
        if let Some(since) = query.since_ms {
            clauses.push("ts >= ?".to_owned());
            values.push(Value::Integer(since));
        }
        if let Some(until) = query.until_ms {
            clauses.push("ts <= ?".to_owned());
            values.push(Value::Integer(until));
        }
        if let Some(level) = query.level {
            clauses.push("level = ?".to_owned());
            values.push(Value::Text(level.as_str().to_owned()));
        }
        if let Some(message) = &query.message {
            clauses.push("message LIKE ? ESCAPE '\\'".to_owned());
            values.push(Value::Text(format!("%{}%", escape_like(message))));
        }
        for (key, value) in &query.metadata {
            if self.index_keys.contains(key) {
                clauses.push(format!("{} = ?", quote_identifier(key)));
                values.push(Value::Text(value.clone()));
            } else {
                clauses.push("json_extract(metadata, ?) = ?".to_owned());
                values.push(Value::Text(json_path(key)));
                values.push(Value::Text(value.clone()));
            }
        }
        if clauses.is_empty() {
            (String::new(), values)
        } else {
            (format!("WHERE {}", clauses.join(" AND ")), values)
        }
    }

    fn stats(&self) -> Result<DatabaseStats, Error> {
        let mut stats = DatabaseStats::default();
        let mut statement = self
            .connection
            .prepare("SELECT key, value FROM timeless_stats('logs')")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Value>(1)?))
        })?;
        for row in rows {
            let (key, value) = row?;
            if let Value::Integer(value) = value {
                match key.as_str() {
                    "blocks" => stats.blocks = value,
                    "entries" => stats.entries = value,
                    _ => {}
                }
            }
        }
        stats.disk_size = database_size(&self.path);
        Ok(stats)
    }

    fn retry_busy<T>(&self, mut operation: impl FnMut() -> Result<T, Error>) -> Result<T, Error> {
        for attempt in 0..=READ_BUSY_RETRIES {
            match operation() {
                Err(error) if attempt < READ_BUSY_RETRIES && error.is_retryable_busy() => {
                    thread::sleep(READ_BUSY_BACKOFF);
                }
                result => return result,
            }
        }
        unreachable!("bounded retry loop always returns")
    }
}

impl Error {
    fn is_retryable_busy(&self) -> bool {
        let Self::Database(message) = self else {
            return false;
        };
        message.contains("SQLITE_BUSY")
            || message.contains("database is locked")
            || message.contains("database table is locked")
            || message.contains("database is busy")
            || message.contains("read is blocked by another connection's active write")
    }
}

fn flush_transaction(transaction: &Transaction<'_>) -> Result<(), Error> {
    transaction.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn json_path(key: &str) -> String {
    format!("$.\"{}\"", key.replace('"', "\\\""))
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn database_size(path: &Path) -> u64 {
    let base = path.to_string_lossy();
    [
        base.to_string(),
        format!("{base}-wal"),
        format!("{base}-shm"),
    ]
    .iter()
    .filter_map(|path| fs::metadata(path).ok())
    .map(|metadata| metadata.len())
    .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::tempdir;
    use timeless_api::Level;

    fn extension_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("target/debug/libtimeless_ext.so")
    }

    fn record(ts_ms: i64, level: Level, message: &str, service: &str) -> LogRecord {
        LogRecord {
            ts_ms,
            level,
            message: message.into(),
            metadata: BTreeMap::from([("service".into(), service.into())]),
        }
    }

    #[tokio::test]
    async fn immediate_query_and_restart_are_durable() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("logs.db");
        let database =
            Database::start(path.clone(), extension_path(), vec!["service".into()], 8, 2).unwrap();
        database
            .ingest(vec![
                record(1_700_000_000_000, Level::Info, "hello", "api"),
                record(1_700_000_001_000, Level::Error, "boom", "worker"),
            ])
            .await
            .unwrap();
        let query = LogQuery {
            level: Some(Level::Error),
            ..LogQuery::default()
        };
        let rows = database.query(query).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message, "boom");
        assert_eq!(database.stats().await.unwrap().entries, 2);
        database.shutdown().await.unwrap();

        let reopened =
            Database::start(path, extension_path(), vec!["service".into()], 8, 2).unwrap();
        let mut query = LogQuery::default();
        query.metadata.insert("service".into(), "api".into());
        let rows = reopened.query(query).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message, "hello");
        assert_eq!(reopened.stats().await.unwrap().entries, 2);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn count_and_literal_message_filter_work() {
        let directory = tempdir().unwrap();
        let database = Database::start(
            directory.path().join("logs.db"),
            extension_path(),
            vec!["service".into()],
            8,
            2,
        )
        .unwrap();
        database
            .ingest(vec![
                record(1, Level::Info, "100% ready", "api"),
                record(2, Level::Info, "1000 ready", "api"),
            ])
            .await
            .unwrap();
        let query = LogQuery {
            message: Some("100%".into()),
            ..LogQuery::default()
        };
        assert_eq!(database.count(query).await.unwrap(), 1);
        database.shutdown().await.unwrap();
    }
}
