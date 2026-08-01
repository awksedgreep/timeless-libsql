//! Storage-seam acceptance tests: data written through the default
//! filesystem engine must be recoverable both through an engine built
//! over an explicit FsStore (the seam carries recovery) and through the
//! bare FsStore scan (the store alone understands the on-disk layout).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use timeless_core::{ChunkBytes, ChunkLoc, ChunkStore, EncodedChunk, Engine, FsStore, StoredChunk};

struct FaultyBatchStore {
    inner: FsStore,
    mode: Arc<AtomicU8>,
}

impl ChunkStore for FaultyBatchStore {
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
        self.inner.read_chunk(loc)
    }

    fn read_chunks(&self, locs: &[ChunkLoc]) -> Result<Vec<ChunkBytes>, String> {
        match self.mode.load(Ordering::SeqCst) {
            1 => Err("injected batch read failure".into()),
            2 => {
                let mut payloads = self.inner.read_chunks(locs)?;
                payloads.pop();
                Ok(payloads)
            }
            _ => self.inner.read_chunks(locs),
        }
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

    fn storage_stats(&self) -> (u64, usize) {
        self.inner.storage_stats()
    }

    fn sweep_cache(&self) {
        self.inner.sweep_cache();
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("timeless_core_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn new_engine(dir: &std::path::Path) -> Engine {
    // (data_dir, flush_threshold, min_flush_size, compression_level,
    //  memory_budget, defer_compression)
    Engine::new(dir.to_path_buf(), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap()
}

#[test]
fn engine_open_fails_closed_on_malformed_compaction_manifest() {
    let dir = temp_dir("malformed_manifest");
    let manifest = dir.join("compaction.manifest");
    std::fs::write(&manifest, "P\tmissing-final-path\n").unwrap();

    let err = match Engine::new(dir.clone(), 1000, 0, 8, 64 * 1024 * 1024, false) {
        Ok(_) => panic!("engine opened with malformed recovery state"),
        Err(err) => err,
    };

    assert!(err.contains("line 1"), "unexpected error: {err}");
    assert!(
        manifest.exists(),
        "engine discarded malformed recovery state"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn with_store_recovers_fs_data() {
    let dir = temp_dir("store_seam");

    let labels: HashMap<String, String> = [("host".to_string(), "pvm1".to_string())]
        .into_iter()
        .collect();

    let n_points: i64 = 5_000;
    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();
        for ts in 0..n_points {
            engine.write_point(sid, ts, ts as f64 * 2.0);
        }
        engine.shutdown().unwrap();
    }

    // "Restart" through the seam: an engine built over an explicit
    // FsStore must recover the registry and chunk index identically.
    {
        let engine = Engine::with_store(
            Box::new(FsStore::new(dir.clone()).unwrap()),
            1000,
            0,
            8,
            64 * 1024 * 1024,
            false,
        )
        .unwrap();
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(
            rows.len(),
            n_points as usize,
            "seam recovery rebuilds index via store.scan()"
        );
        assert_eq!(rows[4_999], (4_999, 4_999.0 * 2.0));

        let info = engine.info();
        assert_eq!(info.series_count, 1);
        assert!(info.total_bytes > 0, "storage_stats sees the chunk files");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fs_store_scan_reads_engine_output() {
    let dir = temp_dir("store_compat");

    let labels: HashMap<String, String> = HashMap::new();
    let n_points: i64 = 3_000;
    let sid;
    {
        let engine = new_engine(&dir);
        sid = engine.resolve_cached("gauge", &labels).unwrap();
        for ts in 0..n_points {
            engine.write_point(sid, ts, 1.0);
        }
        engine.flush_all().unwrap();
    }

    // A bare FsStore over the same dir must enumerate the chunks the
    // engine persisted, with intact metadata.
    let store = FsStore::new(dir.clone()).unwrap();
    let chunks = store.scan().unwrap();
    assert!(!chunks.is_empty(), "scan finds persisted chunks");
    let total_points: u64 = chunks
        .iter()
        .filter(|c| c.series_id == sid)
        .map(|c| c.meta.point_count as u64)
        .sum();
    assert_eq!(total_points, n_points as u64);
    let min_ts = chunks.iter().map(|c| c.meta.min_ts).min().unwrap();
    let max_ts = chunks.iter().map(|c| c.meta.max_ts).max().unwrap();
    assert_eq!((min_ts, max_ts), (0, n_points - 1));

    // And each chunk's payload must be readable at its ChunkLoc.
    for chunk in &chunks {
        let bytes = store.read_chunk(&chunk.meta.loc).unwrap();
        assert!(!bytes.ts().is_empty());
        assert!(!bytes.val().is_empty());
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aggregate_and_latest_batches_propagate_store_contract_failures() {
    let dir = temp_dir("batch_store_failures");
    let mode = Arc::new(AtomicU8::new(0));
    let store = FaultyBatchStore {
        inner: FsStore::new(dir.clone()).unwrap(),
        mode: Arc::clone(&mode),
    };
    let engine = Engine::with_store(Box::new(store), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap();
    let sid = engine.resolve_cached("cpu", &HashMap::new()).unwrap();
    for timestamp in 0..10 {
        engine.write_point(sid, timestamp, timestamp as f64);
    }
    engine.flush_all().unwrap();

    // The strict subrange forces both primitives off their metadata-only fast
    // paths so the injected batch-store behavior is observed.
    mode.store(1, Ordering::SeqCst);
    for error in [
        engine
            .query_aggregate_summary_batch_by_id(&[sid], 1, 8)
            .unwrap_err(),
        engine.query_latest_batch_by_id(&[sid], 1, 8).unwrap_err(),
    ] {
        assert!(error.contains("injected batch read failure"), "{error}");
    }

    mode.store(2, Ordering::SeqCst);
    for error in [
        engine
            .query_aggregate_summary_batch_by_id(&[sid], 1, 8)
            .unwrap_err(),
        engine.query_latest_batch_by_id(&[sid], 1, 8).unwrap_err(),
    ] {
        assert!(
            error.contains("returned 0 payloads for 1 locations"),
            "{error}"
        );
    }

    mode.store(0, Ordering::SeqCst);
    engine.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
