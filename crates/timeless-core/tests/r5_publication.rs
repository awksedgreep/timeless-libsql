use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

use timeless_core::{
    BlockEngine, BlockEngineConfig, BlockLoc, BlockMeta, BlockStore, ChunkBytes, ChunkLoc,
    ChunkStore, EncodedBlock, EncodedChunk, EncodedSpanBlock, Engine, FsStore, LogEntry, LogQuery,
    MemBlockStore, MemSpanStore, ResolvedSeries, SpanBlockEngine, SpanBlockStore, SpanEngineConfig,
    SpanEntry, SpanQuery, StoredChunk, StoredSeries,
};

const MAINTENANCE_WINDOW: Duration = Duration::from_millis(200);
static NEXT_TEMP: AtomicUsize = AtomicUsize::new(0);

struct ReadPause {
    armed: AtomicBool,
    entered: Barrier,
    release: Barrier,
}

impl ReadPause {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Barrier::new(2),
            release: Barrier::new(2),
        }
    }

    fn arm(&self) {
        assert!(
            !self.armed.swap(true, Ordering::SeqCst),
            "read pause already armed"
        );
    }

    fn before_read(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.wait();
            self.release.wait();
        }
    }

    fn wait_until_paused(&self) {
        self.entered.wait();
    }

    fn resume(&self) {
        self.release.wait();
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "timeless_r5_{name}_{}_{}",
        std::process::id(),
        sequence
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn wait_for_overtake(done: &mpsc::Receiver<()>) {
    let _ = done.recv_timeout(MAINTENANCE_WINDOW);
}

struct PausingChunkStore {
    inner: FsStore,
    pause: Arc<ReadPause>,
}

impl ChunkStore for PausingChunkStore {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        self.inner.put_chunks(chunks)
    }

    fn replace_chunks(
        &self,
        add: &[EncodedChunk],
        remove: &[ChunkLoc],
        on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        self.inner.replace_chunks(add, remove, on_committed)
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        self.pause.before_read();
        if let ChunkLoc::File { path, .. } = loc {
            if !path.exists() {
                return Err(format!(
                    "test reader observed deleted chunk {}",
                    path.display()
                ));
            }
        }
        self.inner.read_chunk(loc)
    }

    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String> {
        self.inner.delete_chunks(locs)
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        self.inner.scan()
    }

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String> {
        self.inner.save_registry(bytes)
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_registry()
    }

    fn load_series(&self) -> Result<Vec<StoredSeries>, String> {
        self.inner.load_series()
    }

    fn resolve_series(
        &self,
        name: &str,
        labels: &[(String, String)],
    ) -> Result<ResolvedSeries, String> {
        self.inner.resolve_series(name, labels)
    }

    fn migrate_series(&self, series: &[StoredSeries]) -> Result<(), String> {
        self.inner.migrate_series(series)
    }

    fn storage_stats(&self) -> (u64, usize) {
        self.inner.storage_stats()
    }

    fn sweep_cache(&self) {
        self.inner.sweep_cache();
    }
}

struct PausingBlockStore {
    inner: MemBlockStore,
    pause: Arc<ReadPause>,
}

impl BlockStore for PausingBlockStore {
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        self.inner.put_block(block)
    }

    fn put_blocks(&self, blocks: &[EncodedBlock]) -> Result<Vec<BlockLoc>, String> {
        self.inner.put_blocks(blocks)
    }

    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        self.inner.replace_blocks(add, remove, on_committed)
    }

    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.pause.before_read();
        self.inner.read_block(loc)
    }

    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        self.inner.delete_blocks(locs)
    }

    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        self.inner.scan()
    }

    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.inner.query_terms(terms, ts_min, ts_max)
    }

    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.inner.save_meta(key, value)
    }

    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_meta(key)
    }
}

struct PausingSpanStore {
    inner: MemSpanStore,
    pause: Arc<ReadPause>,
}

impl SpanBlockStore for PausingSpanStore {
    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String> {
        self.inner.put_blocks(blocks)
    }

    fn replace_blocks(
        &self,
        add: &[EncodedSpanBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        self.inner.replace_blocks(add, remove, on_committed)
    }

    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.pause.before_read();
        self.inner.read_block(loc)
    }

    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        self.inner.delete_blocks(locs)
    }

    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        self.inner.scan()
    }

    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.inner.query_terms(terms, ts_min, ts_max)
    }

    fn query_trace(&self, trace_id: &[u8; 16]) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.inner.query_trace(trace_id)
    }

    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.inner.save_meta(key, value)
    }

    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_meta(key)
    }
}

fn log_entry(ts: i64, message: &str) -> LogEntry {
    LogEntry {
        ts,
        level: 1,
        message: message.to_owned(),
        metadata: vec![("service".to_owned(), "api".to_owned())],
    }
}

fn log_query() -> LogQuery {
    LogQuery {
        ts_min: i64::MIN + 1,
        ts_max: i64::MAX - 1,
        level: None,
        metadata_eq: Vec::new(),
        message_contains: None,
    }
}

fn span_entry(ts: i64, span_id: u8) -> SpanEntry {
    SpanEntry {
        trace_id: [7; 16],
        span_id: [span_id; 8],
        parent_span_id: None,
        name: "GET /items".to_owned(),
        service: "api".to_owned(),
        kind: 1,
        status: 1,
        start_ts: ts,
        duration_ns: 10,
        attributes: Vec::new(),
    }
}

fn span_query() -> SpanQuery {
    SpanQuery {
        ts_min: i64::MIN + 1,
        ts_max: i64::MAX - 1,
        trace_id: None,
        service: None,
        kind: None,
        status: None,
        name: None,
    }
}

#[test]
fn metrics_query_cannot_miss_buffer_during_flush() {
    let dir = temp_dir("metrics_flush");
    let pause = Arc::new(ReadPause::new());
    let store = PausingChunkStore {
        inner: FsStore::new(dir.clone()).unwrap(),
        pause: pause.clone(),
    };
    let engine =
        Arc::new(Engine::with_store(Box::new(store), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap());
    let sid = engine.resolve_cached("cpu", &HashMap::new()).unwrap();
    engine.write_point(sid, 1, 1.0);
    engine.flush_all().unwrap();
    engine.write_point(sid, 2, 2.0);

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query_range_by_id(sid, 0, 10));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let flush_engine = engine.clone();
    let flush = thread::spawn(move || {
        let result = flush_engine.flush_all();
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    flush.join().unwrap().unwrap();
    assert_eq!(rows, vec![(1, 1.0), (2, 2.0)]);

    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn metrics_query_cannot_read_deleted_location_during_compaction() {
    let dir = temp_dir("metrics_compact");
    let pause = Arc::new(ReadPause::new());
    let store = PausingChunkStore {
        inner: FsStore::new(dir.clone()).unwrap(),
        pause: pause.clone(),
    };
    let engine =
        Arc::new(Engine::with_store(Box::new(store), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap());
    let sid = engine.resolve_cached("cpu", &HashMap::new()).unwrap();
    engine.write_point(sid, 1, 1.0);
    engine.flush_all().unwrap();
    engine.write_point(sid, 2, 2.0);
    engine.flush_all().unwrap();

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query_range_by_id(sid, 0, 10));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let compact_engine = engine.clone();
    let compact = thread::spawn(move || {
        let result = compact_engine.compact_partitions(i64::MAX);
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    compact.join().unwrap().unwrap();
    assert_eq!(rows, vec![(1, 1.0), (2, 2.0)]);

    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn logs_query_cannot_miss_buffer_during_flush() {
    let pause = Arc::new(ReadPause::new());
    let store = PausingBlockStore {
        inner: MemBlockStore::new(),
        pause: pause.clone(),
    };
    let engine = Arc::new(
        BlockEngine::new(
            Box::new(store),
            BlockEngineConfig {
                flush_threshold: 1000,
                ..BlockEngineConfig::default()
            },
        )
        .unwrap(),
    );
    engine.push(log_entry(1, "one")).unwrap();
    engine.flush().unwrap();
    engine.push(log_entry(2, "two")).unwrap();

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query(&log_query()));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let flush_engine = engine.clone();
    let flush = thread::spawn(move || {
        let result = flush_engine.flush();
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    flush.join().unwrap().unwrap();
    assert_eq!(rows, vec![log_entry(1, "one"), log_entry(2, "two")]);
}

#[test]
fn traces_query_cannot_miss_buffer_during_flush() {
    let pause = Arc::new(ReadPause::new());
    let store = PausingSpanStore {
        inner: MemSpanStore::new(),
        pause: pause.clone(),
    };
    let engine = Arc::new(
        SpanBlockEngine::new(
            Box::new(store),
            SpanEngineConfig {
                flush_threshold: 1000,
                ..SpanEngineConfig::default()
            },
        )
        .unwrap(),
    );
    engine.push(span_entry(1, 1)).unwrap();
    engine.flush().unwrap();
    engine.push(span_entry(2, 2)).unwrap();

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query(&span_query()));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let flush_engine = engine.clone();
    let flush = thread::spawn(move || {
        let result = flush_engine.flush();
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    flush.join().unwrap().unwrap();
    assert_eq!(rows, vec![span_entry(1, 1), span_entry(2, 2)]);
}

#[test]
fn logs_query_cannot_read_deleted_location_during_optimize() {
    let pause = Arc::new(ReadPause::new());
    let store = PausingBlockStore {
        inner: MemBlockStore::new(),
        pause: pause.clone(),
    };
    let engine = Arc::new(
        BlockEngine::new(
            Box::new(store),
            BlockEngineConfig {
                flush_threshold: 1000,
                ..BlockEngineConfig::default()
            },
        )
        .unwrap(),
    );
    engine.push(log_entry(1, "one")).unwrap();
    engine.flush().unwrap();
    engine.push(log_entry(2, "two")).unwrap();
    engine.flush().unwrap();

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query(&log_query()));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let optimize_engine = engine.clone();
    let optimize = thread::spawn(move || {
        let result = optimize_engine.optimize();
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    optimize.join().unwrap().unwrap();
    assert_eq!(rows, vec![log_entry(1, "one"), log_entry(2, "two")]);
}

#[test]
fn logs_query_cannot_read_deleted_location_during_prune() {
    let pause = Arc::new(ReadPause::new());
    let store = PausingBlockStore {
        inner: MemBlockStore::new(),
        pause: pause.clone(),
    };
    let engine = Arc::new(BlockEngine::new(Box::new(store), BlockEngineConfig::default()).unwrap());
    engine.push(log_entry(1, "one")).unwrap();
    engine.flush().unwrap();

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query(&log_query()));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let prune_engine = engine.clone();
    let prune = thread::spawn(move || {
        let result = prune_engine.prune(10);
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    prune.join().unwrap().unwrap();
    assert_eq!(rows, vec![log_entry(1, "one")]);
}

#[test]
fn traces_query_cannot_read_deleted_location_during_optimize() {
    let pause = Arc::new(ReadPause::new());
    let store = PausingSpanStore {
        inner: MemSpanStore::new(),
        pause: pause.clone(),
    };
    let engine = Arc::new(
        SpanBlockEngine::new(
            Box::new(store),
            SpanEngineConfig {
                flush_threshold: 1000,
                ..SpanEngineConfig::default()
            },
        )
        .unwrap(),
    );
    engine.push(span_entry(1, 1)).unwrap();
    engine.flush().unwrap();
    engine.push(span_entry(2, 2)).unwrap();
    engine.flush().unwrap();

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query(&span_query()));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let optimize_engine = engine.clone();
    let optimize = thread::spawn(move || {
        let result = optimize_engine.optimize();
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    optimize.join().unwrap().unwrap();
    assert_eq!(rows, vec![span_entry(1, 1), span_entry(2, 2)]);
}

#[test]
fn traces_query_cannot_read_deleted_location_during_prune() {
    let pause = Arc::new(ReadPause::new());
    let store = PausingSpanStore {
        inner: MemSpanStore::new(),
        pause: pause.clone(),
    };
    let engine =
        Arc::new(SpanBlockEngine::new(Box::new(store), SpanEngineConfig::default()).unwrap());
    engine.push(span_entry(1, 1)).unwrap();
    engine.flush().unwrap();

    pause.arm();
    let query_engine = engine.clone();
    let query = thread::spawn(move || query_engine.query(&span_query()));
    pause.wait_until_paused();

    let (done_tx, done_rx) = mpsc::channel();
    let prune_engine = engine.clone();
    let prune = thread::spawn(move || {
        let result = prune_engine.prune(10);
        done_tx.send(()).unwrap();
        result
    });
    wait_for_overtake(&done_rx);
    pause.resume();

    let rows = query.join().unwrap().unwrap();
    prune.join().unwrap().unwrap();
    assert_eq!(rows, vec![span_entry(1, 1)]);
}
