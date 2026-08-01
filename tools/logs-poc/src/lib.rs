//! Storage-only logs data-plane proof.
//!
//! This crate intentionally contains no HTTP server, authentication, or
//! API protocol code. It exercises the boundary that has to be correct
//! first: bounded admission -> batch ingest -> queryable memory -> raw
//! threshold/timer flush -> bounded background compression.

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rusqlite::{params, Connection};

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub ts: i64,
    pub level: u8,
    pub message: String,
    pub metadata_json: String,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub database_path: PathBuf,
    pub extension_path: PathBuf,
    /// Bounded queue capacity, measured in accepted producer batches.
    pub queue_batches: usize,
    /// Hard admission capacity measured in entries. Credits remain in use
    /// until the corresponding extension buffer has been flushed as raw,
    /// so pulling a batch from the channel cannot hide storage backlog.
    pub queue_entries: usize,
    /// Entries accumulated in the extension buffer before the host asks
    /// SQLite to persist one raw batch.
    pub flush_entries: usize,
    /// Low-volume raw flush timer.
    pub flush_interval: Duration,
    /// How often raw debt is evaluated.
    pub optimize_interval: Duration,
    /// Entry debt that makes an optimize pass due regardless of age.
    pub optimize_raw_entries: usize,
    /// Oldest host-observed raw debt age that makes a pass due.
    pub optimize_max_raw_age: Duration,
    /// Maximum source entries decoded by one optimize command. A single
    /// persisted block may exceed it because blocks are never split.
    pub optimize_entry_budget: usize,
}

impl StorageConfig {
    fn validate(&self) -> Result<(), String> {
        if self.queue_batches == 0 {
            return Err("queue_batches must be greater than zero".into());
        }
        if self.queue_entries == 0 {
            return Err("queue_entries must be greater than zero".into());
        }
        if self.flush_entries == 0 {
            return Err("flush_entries must be greater than zero".into());
        }
        if self.flush_interval.is_zero() {
            return Err("flush_interval must be greater than zero".into());
        }
        if self.optimize_interval.is_zero() {
            return Err("optimize_interval must be greater than zero".into());
        }
        if self.optimize_raw_entries == 0 {
            return Err("optimize_raw_entries must be greater than zero".into());
        }
        if self.optimize_entry_budget == 0 {
            return Err("optimize_entry_budget must be greater than zero".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageSnapshot {
    pub admitted_entries: i64,
    pub blocks: i64,
    pub raw_blocks: i64,
    pub compressed_blocks: i64,
    pub buffered_entries: i64,
    pub disk_entries: i64,
    pub raw_entries: i64,
    pub compressed_entries: i64,
    pub raw_bytes: i64,
    pub compressed_bytes: i64,
    pub total_entries: i64,
    pub error_entries: i64,
    pub api_error_entries: i64,
    pub timeout_entries: i64,
    pub codec5_blocks: i64,
}

enum Command {
    Ingest(Vec<LogEntry>),
    Snapshot(mpsc::Sender<Result<StorageSnapshot, String>>),
    Flush(mpsc::Sender<Result<(), String>>),
    OptimizeOnce(mpsc::Sender<Result<StorageSnapshot, String>>),
    Shutdown(mpsc::Sender<Result<(), String>>),
}

/// A single SQLite writer with bounded producer admission.
///
/// `enqueue` acknowledges queue admission, not durability. The worker
/// inserts batches into the vtab's queryable in-memory buffer and only
/// creates raw blocks at the aggregate entry threshold or timer. It never
/// compresses an individual request and never runs optimize on ingest.
pub struct StorageWorker {
    tx: SyncSender<Command>,
    admission: Arc<Admission>,
    join: Option<JoinHandle<Result<(), String>>>,
}

impl StorageWorker {
    pub fn start(config: StorageConfig) -> Result<Self, String> {
        config.validate()?;
        let queue_batches = config.queue_batches;
        let (tx, rx) = mpsc::sync_channel(queue_batches);
        let (ready_tx, ready_rx) = mpsc::channel();
        let admission = Arc::new(Admission::new(config.queue_entries));
        let worker_admission = Arc::clone(&admission);
        let join = thread::Builder::new()
            .name("timeless-logs-storage".into())
            .spawn(move || {
                let opened = StorageLoop::open(config, worker_admission);
                match opened {
                    Ok(storage) => {
                        let _ = ready_tx.send(Ok(()));
                        storage.run(rx)
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.clone()));
                        Err(error)
                    }
                }
            })
            .map_err(|e| format!("spawn storage worker: {e}"))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(StorageWorker {
                tx,
                admission,
                join: Some(join),
            }),
            Ok(Err(error)) => {
                let _ = join.join();
                Err(error)
            }
            Err(error) => {
                let _ = join.join();
                Err(format!("storage worker exited during startup: {error}"))
            }
        }
    }

    /// Accept a producer batch into the bounded queue. Success does not
    /// imply that the batch has reached a raw block yet.
    pub fn enqueue(&self, entries: Vec<LogEntry>) -> Result<usize, String> {
        let count = entries.len();
        self.admission.reserve(count)?;
        if self.tx.send(Command::Ingest(entries)).is_err() {
            self.admission.release(count);
            return Err("storage worker is not running".into());
        }
        Ok(count)
    }

    /// An ordered barrier: all batches accepted before this call have
    /// reached the extension buffer (or an error is returned).
    pub fn snapshot(&self) -> Result<StorageSnapshot, String> {
        self.request(Command::Snapshot)
    }

    pub fn flush(&self) -> Result<(), String> {
        self.request(Command::Flush)
    }

    pub fn optimize_once(&self) -> Result<StorageSnapshot, String> {
        self.request(Command::OptimizeOnce)
    }

    fn request<T>(
        &self,
        make: impl FnOnce(mpsc::Sender<Result<T, String>>) -> Command,
    ) -> Result<T, String> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(make(reply_tx))
            .map_err(|_| "storage worker is not running".to_string())?;
        reply_rx
            .recv()
            .map_err(|_| "storage worker stopped before replying".to_string())?
    }

    pub fn shutdown(mut self) -> Result<(), String> {
        let result = self.request(Command::Shutdown);
        let joined = self
            .join
            .take()
            .expect("join handle present")
            .join()
            .map_err(|_| "storage worker panicked".to_string())?;
        result.and(joined)
    }
}

struct Admission {
    limit: usize,
    used: Mutex<usize>,
    available: Condvar,
}

impl Admission {
    fn new(limit: usize) -> Self {
        Admission {
            limit,
            used: Mutex::new(0),
            available: Condvar::new(),
        }
    }

    fn reserve(&self, count: usize) -> Result<(), String> {
        if count > self.limit {
            return Err(format!(
                "batch has {count} entries, exceeding admission capacity {}",
                self.limit
            ));
        }
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        while used.saturating_add(count) > self.limit {
            used = self.available.wait(used).unwrap_or_else(|e| e.into_inner());
        }
        *used += count;
        Ok(())
    }

    fn release(&self, count: usize) {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        *used = used.saturating_sub(count);
        self.available.notify_all();
    }

    fn used(&self) -> usize {
        *self.used.lock().unwrap_or_else(|e| e.into_inner())
    }
}

struct StorageLoop {
    conn: Connection,
    config: StorageConfig,
    admission: Arc<Admission>,
    entries_since_flush: usize,
    admitted_since_flush: usize,
    raw_since: Option<Instant>,
    next_flush: Instant,
    next_optimize: Instant,
}

impl StorageLoop {
    fn open(config: StorageConfig, admission: Arc<Admission>) -> Result<Self, String> {
        let conn = Connection::open(&config.database_path)
            .map_err(|e| format!("open {}: {e}", config.database_path.display()))?;
        unsafe {
            conn.load_extension_enable()
                .map_err(|e| format!("enable extension loading: {e}"))?;
            conn.load_extension(&config.extension_path, None::<&str>)
                .map_err(|e| format!("load extension {}: {e}", config.extension_path.display()))?;
        }
        conn.load_extension_disable()
            .map_err(|e| format!("disable extension loading: {e}"))?;
        conn.execute_batch(
            "PRAGMA auto_vacuum = INCREMENTAL;
             CREATE VIRTUAL TABLE IF NOT EXISTS logs USING \
               timeless_logs(index_keys='service,path,status');",
        )
        .map_err(|e| format!("initialize logs table: {e}"))?;

        let now = Instant::now();
        let raw_entries = stat_i64(&conn, "raw_entries")?;
        let flush_interval = config.flush_interval;
        let optimize_interval = config.optimize_interval;
        Ok(StorageLoop {
            conn,
            config,
            admission,
            entries_since_flush: 0,
            admitted_since_flush: 0,
            raw_since: (raw_entries > 0).then_some(now),
            next_flush: now + flush_interval,
            next_optimize: now + optimize_interval,
        })
    }

    fn run(mut self, rx: Receiver<Command>) -> Result<(), String> {
        loop {
            self.run_due_maintenance()?;
            let now = Instant::now();
            let deadline = self.next_flush.min(self.next_optimize);
            let timeout = deadline.saturating_duration_since(now);
            match rx.recv_timeout(timeout) {
                Ok(Command::Ingest(entries)) => {
                    let count = entries.len();
                    if let Err(error) = self.ingest(entries) {
                        self.admission.release(count);
                        return Err(error);
                    }
                }
                Ok(Command::Snapshot(reply)) => {
                    let _ = reply.send(self.snapshot());
                }
                Ok(Command::Flush(reply)) => {
                    let result = self.flush_raw();
                    let _ = reply.send(result);
                }
                Ok(Command::OptimizeOnce(reply)) => {
                    let result = self.optimize_once().and_then(|()| self.snapshot());
                    let _ = reply.send(result);
                }
                Ok(Command::Shutdown(reply)) => {
                    // Channel ordering means every batch accepted before
                    // shutdown has already been inserted. Graceful shutdown
                    // makes the remaining extension buffer durable as raw;
                    // compaction remains background maintenance.
                    let result = self.flush_raw();
                    let _ = reply.send(result.clone());
                    return result;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    // Losing the owning handle is treated as an ungraceful
                    // stop: accepted-but-buffered data may be lost, matching
                    // the documented admission rather than durability ack.
                    return Ok(());
                }
            }
        }
    }

    fn ingest(&mut self, entries: Vec<LogEntry>) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        let count = entries.len();
        let raw_before = stat_i64(&self.conn, "raw_entries")?;
        let blob = encode_batch(&entries)?;
        self.conn
            .execute("INSERT INTO logs(logs) VALUES (?1)", params![blob])
            .map_err(|e| format!("insert log batch: {e}"))?;
        self.entries_since_flush = self.entries_since_flush.saturating_add(count);
        self.admitted_since_flush = self.admitted_since_flush.saturating_add(count);

        // The extension has its own high-water safety flush. Observe it so
        // age-based maintenance remains correct even when a producer batch
        // crosses that internal threshold before the host timer runs.
        let raw_after = stat_i64(&self.conn, "raw_entries")?;
        if raw_after > raw_before && self.raw_since.is_none() {
            self.raw_since = Some(Instant::now());
        }
        if self.entries_since_flush >= self.config.flush_entries {
            self.flush_raw()?;
        }
        Ok(())
    }

    fn flush_raw(&mut self) -> Result<(), String> {
        let buffered = stat_i64(&self.conn, "buffered_entries")?;
        if buffered > 0 {
            self.conn
                .execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                .map_err(|e| format!("flush logs buffer: {e}"))?;
            if self.raw_since.is_none() {
                self.raw_since = Some(Instant::now());
            }
        }
        self.entries_since_flush = 0;
        self.admission.release(self.admitted_since_flush);
        self.admitted_since_flush = 0;
        Ok(())
    }

    fn optimize_once(&mut self) -> Result<(), String> {
        let command = format!("optimize:{}", self.config.optimize_entry_budget);
        self.conn
            .execute("INSERT INTO logs(logs) VALUES (?1)", params![command])
            .map_err(|e| format!("bounded logs optimize: {e}"))?;
        if stat_i64(&self.conn, "raw_entries")? == 0 {
            self.raw_since = None;
        }
        Ok(())
    }

    fn run_due_maintenance(&mut self) -> Result<(), String> {
        let now = Instant::now();
        if now >= self.next_flush {
            self.flush_raw()?;
            self.next_flush = now + self.config.flush_interval;
        }
        if now >= self.next_optimize {
            let raw_entries = stat_i64(&self.conn, "raw_entries")? as usize;
            if raw_entries == 0 {
                self.raw_since = None;
            } else {
                let age_due = self
                    .raw_since
                    .is_some_and(|since| since.elapsed() >= self.config.optimize_max_raw_age);
                if raw_entries >= self.config.optimize_raw_entries || age_due {
                    self.optimize_once()?;
                }
            }
            self.next_optimize = now + self.config.optimize_interval;
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StorageSnapshot, String> {
        Ok(StorageSnapshot {
            admitted_entries: self.admission.used() as i64,
            blocks: stat_i64(&self.conn, "blocks")?,
            raw_blocks: stat_i64(&self.conn, "raw_blocks")?,
            compressed_blocks: stat_i64(&self.conn, "compressed_blocks")?,
            buffered_entries: stat_i64(&self.conn, "buffered_entries")?,
            disk_entries: stat_i64(&self.conn, "disk_entries")?,
            raw_entries: stat_i64(&self.conn, "raw_entries")?,
            compressed_entries: stat_i64(&self.conn, "compressed_entries")?,
            raw_bytes: stat_i64(&self.conn, "raw_bytes")?,
            compressed_bytes: stat_i64(&self.conn, "compressed_bytes")?,
            total_entries: query_count(&self.conn, "SELECT COUNT(*) FROM logs")?,
            error_entries: query_count(
                &self.conn,
                "SELECT COUNT(*) FROM logs WHERE level = 'error'",
            )?,
            api_error_entries: query_count(
                &self.conn,
                "SELECT COUNT(*) FROM logs WHERE level = 'error' AND service = 'api'",
            )?,
            timeout_entries: query_count(
                &self.conn,
                "SELECT COUNT(*) FROM logs WHERE message LIKE '%timeout%'",
            )?,
            codec5_blocks: query_count(
                &self.conn,
                "SELECT COUNT(*) FROM logs_blocks WHERE codec = 5",
            )?,
        })
    }
}

fn query_count(conn: &Connection, sql: &str) -> Result<i64, String> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(|e| format!("query {sql:?}: {e}"))
}

fn stat_i64(conn: &Connection, key: &str) -> Result<i64, String> {
    conn.query_row(
        "SELECT CAST(value AS INTEGER) FROM timeless_stats('logs') WHERE key = ?1",
        [key],
        |row| row.get(0),
    )
    .map_err(|e| format!("read timeless_stats key {key:?}: {e}"))
}

fn encode_batch(entries: &[LogEntry]) -> Result<Vec<u8>, String> {
    let count = u32::try_from(entries.len()).map_err(|_| "batch has more than u32::MAX entries")?;
    let mut out = Vec::with_capacity(8 + entries.len() * 64);
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
    let len = u32::try_from(value.len()).map_err(|_| "batch string exceeds u32::MAX bytes")?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
