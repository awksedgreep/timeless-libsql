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
    SpanEntry, SpanQuery, SpanQueryOrder, StoredChunk, StoredSeries,
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

/// Test double for a store with MVCC-style stable row versions. Replaced
/// blocks stay physically readable after publication, matching the contract
/// ShadowBlockStore gets from the host SQLite read transaction.
struct StableLocationBlockStore {
    inner: MemBlockStore,
}

impl BlockStore for StableLocationBlockStore {
    fn query_snapshot_keeps_locations_readable(&self) -> bool {
        true
    }

    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        self.inner.put_block(block)
    }

    fn put_blocks(&self, blocks: &[EncodedBlock]) -> Result<Vec<BlockLoc>, String> {
        self.inner.put_blocks(blocks)
    }

    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
        _remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        let locs = self.inner.put_blocks(add)?;
        on_committed(&locs);
        Ok(locs)
    }

    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.inner.read_block(loc)
    }

    fn delete_blocks(&self, _locs: &[BlockLoc]) -> Vec<String> {
        Vec::new()
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

/// MVCC-style span store used to prove that traces retain locations rather
/// than every payload when the backend pins old row versions.
struct StableLocationSpanStore {
    inner: MemSpanStore,
}

impl SpanBlockStore for StableLocationSpanStore {
    fn query_snapshot_keeps_locations_readable(&self) -> bool {
        true
    }

    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String> {
        self.inner.put_blocks(blocks)
    }

    fn replace_blocks(
        &self,
        add: &[EncodedSpanBlock],
        _remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        let locations = self.inner.put_blocks(add)?;
        on_committed(&locations);
        Ok(locations)
    }

    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.inner.read_block(loc)
    }

    fn delete_blocks(&self, _locs: &[BlockLoc]) -> Vec<String> {
        Vec::new()
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

    fn query_term_values(&self, prefix: &str) -> Result<Option<Vec<String>>, String> {
        self.inner.query_term_values(prefix)
    }

    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.inner.save_meta(key, value)
    }

    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_meta(key)
    }
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
        severity: None,
        message: message.to_owned(),
        metadata: vec![("service".to_owned(), "api".to_owned())],
        metadata_json: None,
    }
}

fn log_query() -> LogQuery {
    LogQuery {
        ts_min: i64::MIN + 1,
        ts_max: i64::MAX - 1,
        level: None,
        severity: None,
        metadata_eq: Vec::new(),
        message_contains: None,
        message_like_prune: None,
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
        status_description: "".into(),
        start_ts: ts,
        duration_ns: 10,
        attributes: "{}".into(),
        events: "[]".into(),
        resource: "{}".into(),
        instrumentation_scope: "{}".into(),
        links: "[]".into(),
        trace_state: "".into(),
        trace_flags: 0,
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        resource_schema_url: "".into(),
        scope_schema_url: "".into(),
        resource_dropped_attributes_count: 0,
        scope_dropped_attributes_count: 0,
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
        attribute: None,
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
fn logs_flush_can_publish_after_query_snapshot_before_materialization() {
    let engine = Arc::new(
        BlockEngine::new(
            Box::new(MemBlockStore::new()),
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

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        query_engine.query_after_snapshot(&log_query(), move || {
            snapshot_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        })
    });
    snapshot_rx.recv().unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let flush_engine = Arc::clone(&engine);
    let flush = thread::spawn(move || {
        let result = flush_engine.flush();
        done_tx.send(()).unwrap();
        result
    });
    let flush_overtook_materialization = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();

    let rows = query.join().unwrap().unwrap();
    flush.join().unwrap().unwrap();
    assert!(
        flush_overtook_materialization,
        "flush remained blocked after the query owned its block and buffer generation"
    );
    assert_eq!(rows, vec![log_entry(1, "one"), log_entry(2, "two")]);
}

#[test]
fn logs_optimize_can_publish_after_query_snapshot_before_materialization() {
    let engine = Arc::new(
        BlockEngine::new(
            Box::new(MemBlockStore::new()),
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

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        query_engine.query_after_snapshot(&log_query(), move || {
            snapshot_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        })
    });
    snapshot_rx.recv().unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let optimize_engine = Arc::clone(&engine);
    let optimize = thread::spawn(move || {
        let result = optimize_engine.optimize();
        done_tx.send(()).unwrap();
        result
    });
    let optimize_overtook_materialization = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();

    let rows = query.join().unwrap().unwrap();
    optimize.join().unwrap().unwrap();
    assert!(
        optimize_overtook_materialization,
        "optimize remained blocked after the query owned its payload snapshot"
    );
    assert_eq!(rows, vec![log_entry(1, "one"), log_entry(2, "two")]);
}

#[test]
fn logs_stable_store_streams_locations_without_owning_all_payloads() {
    let engine = Arc::new(
        BlockEngine::new(
            Box::new(StableLocationBlockStore {
                inner: MemBlockStore::new(),
            }),
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

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        query_engine.query_after_snapshot(&log_query(), move || {
            snapshot_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        })
    });
    snapshot_rx.recv().unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let optimize_engine = Arc::clone(&engine);
    let optimize = thread::spawn(move || {
        let result = optimize_engine.optimize();
        done_tx.send(()).unwrap();
        result
    });
    let optimize_overtook_materialization = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();

    let rows = query.join().unwrap().unwrap();
    optimize.join().unwrap().unwrap();
    assert!(optimize_overtook_materialization);
    assert_eq!(rows, vec![log_entry(1, "one"), log_entry(2, "two")]);
    let profile = engine.profile();
    assert_eq!(profile.query_stable_location_snapshots, 1);
    assert_eq!(profile.query_snapshot_payload_bytes, 0);
    assert_eq!(profile.query_snapshot_payload_max_bytes, 0);
    assert!(profile.query_payload_bytes_read > 0);
}

#[test]
fn logs_prune_can_publish_after_query_snapshot_before_materialization() {
    let engine = Arc::new(
        BlockEngine::new(Box::new(MemBlockStore::new()), BlockEngineConfig::default()).unwrap(),
    );
    engine.push(log_entry(1, "one")).unwrap();
    engine.flush().unwrap();

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        query_engine.query_after_snapshot(&log_query(), move || {
            snapshot_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        })
    });
    snapshot_rx.recv().unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let prune_engine = Arc::clone(&engine);
    let prune = thread::spawn(move || {
        let result = prune_engine.prune(10);
        done_tx.send(()).unwrap();
        result
    });
    let prune_overtook_materialization = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();

    let rows = query.join().unwrap().unwrap();
    prune.join().unwrap().unwrap();
    assert!(
        prune_overtook_materialization,
        "prune remained blocked after the query owned its payload snapshot"
    );
    assert_eq!(rows, vec![log_entry(1, "one")]);
}

#[test]
fn traces_flush_can_publish_after_query_snapshot_before_materialization() {
    let engine = Arc::new(
        SpanBlockEngine::new(
            Box::new(MemSpanStore::new()),
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

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        query_engine.query_ordered_after_snapshot(
            &span_query(),
            SpanQueryOrder::Asc,
            None,
            move || {
                snapshot_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            },
        )
    });
    snapshot_rx.recv().unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let maintenance_engine = Arc::clone(&engine);
    let maintenance = thread::spawn(move || {
        let result = maintenance_engine.flush();
        done_tx.send(()).unwrap();
        result
    });
    let overtook = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();
    assert_eq!(
        query.join().unwrap().unwrap(),
        vec![span_entry(1, 1), span_entry(2, 2)]
    );
    maintenance.join().unwrap().unwrap();
    assert!(overtook, "flush stayed blocked after the trace snapshot");
}

#[test]
fn traces_optimize_can_publish_after_query_snapshot_before_materialization() {
    let engine = Arc::new(
        SpanBlockEngine::new(
            Box::new(MemSpanStore::new()),
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

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        query_engine.query_ordered_after_snapshot(
            &span_query(),
            SpanQueryOrder::Asc,
            None,
            move || {
                snapshot_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            },
        )
    });
    snapshot_rx.recv().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let maintenance_engine = Arc::clone(&engine);
    let maintenance = thread::spawn(move || {
        let result = maintenance_engine.optimize();
        done_tx.send(()).unwrap();
        result
    });
    let overtook = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();
    assert_eq!(
        query.join().unwrap().unwrap(),
        vec![span_entry(1, 1), span_entry(2, 2)]
    );
    maintenance.join().unwrap().unwrap();
    assert!(overtook, "optimize stayed blocked after the trace snapshot");
}

#[test]
fn traces_prune_can_publish_after_query_snapshot_before_materialization() {
    let engine = Arc::new(
        SpanBlockEngine::new(Box::new(MemSpanStore::new()), SpanEngineConfig::default()).unwrap(),
    );
    engine.push(span_entry(1, 1)).unwrap();
    engine.flush().unwrap();

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        query_engine.query_ordered_after_snapshot(
            &span_query(),
            SpanQueryOrder::Asc,
            None,
            move || {
                snapshot_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            },
        )
    });
    snapshot_rx.recv().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let maintenance_engine = Arc::clone(&engine);
    let maintenance = thread::spawn(move || {
        let result = maintenance_engine.prune(10);
        done_tx.send(()).unwrap();
        result
    });
    let overtook = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();
    assert_eq!(query.join().unwrap().unwrap(), vec![span_entry(1, 1)]);
    maintenance.join().unwrap().unwrap();
    assert!(overtook, "prune stayed blocked after the trace snapshot");
}

#[test]
fn traces_stable_store_streams_locations_with_bounded_payload_memory() {
    let engine = Arc::new(
        SpanBlockEngine::new(
            Box::new(StableLocationSpanStore {
                inner: MemSpanStore::new(),
            }),
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

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let query_engine = Arc::clone(&engine);
    let query = thread::spawn(move || {
        let mut stream = query_engine.query_stream_after_snapshot(&span_query(), move || {
            snapshot_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        })?;
        let mut rows = Vec::new();
        while let Some(row) = query_engine.query_stream_next(&mut stream)? {
            rows.push(row);
        }
        Ok::<_, String>(rows)
    });
    snapshot_rx.recv().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let maintenance_engine = Arc::clone(&engine);
    let maintenance = thread::spawn(move || {
        let result = maintenance_engine.optimize();
        done_tx.send(()).unwrap();
        result
    });
    let overtook = done_rx.recv_timeout(MAINTENANCE_WINDOW).is_ok();
    resume_tx.send(()).unwrap();
    assert_eq!(
        query.join().unwrap().unwrap(),
        vec![span_entry(1, 1), span_entry(2, 2)]
    );
    maintenance.join().unwrap().unwrap();
    assert!(overtook);
    let profile = engine.query_profile();
    assert_eq!(profile.query_stable_location_snapshots, 1);
    assert_eq!(profile.query_snapshot_payload_bytes, 0);
    assert_eq!(profile.query_snapshot_payload_max_bytes, 0);
    assert!(profile.query_payload_bytes_read > 0);
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
