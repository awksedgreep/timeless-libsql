//! Storage-seam acceptance tests: data written through the default
//! filesystem engine must be recoverable both through an engine built
//! over an explicit FsStore (the seam carries recovery) and through the
//! bare FsStore scan (the store alone understands the on-disk layout).

use std::collections::HashMap;
use timeless_core::{ChunkStore, Engine, FsStore};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("timeless_core_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn new_engine(dir: &std::path::Path) -> Engine {
    // (data_dir, flush_threshold, min_flush_size, compression_level,
    //  memory_budget, defer_compression)
    Engine::new(dir.to_path_buf(), 1000, 0, 8, 64 * 1024 * 1024, false)
}

#[test]
fn with_store_recovers_fs_data() {
    let dir = temp_dir("store_seam");

    let labels: HashMap<String, String> =
        [("host".to_string(), "pvm1".to_string())].into_iter().collect();

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
            Box::new(FsStore::new(dir.clone())),
            1000,
            0,
            8,
            64 * 1024 * 1024,
            false,
        );
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(rows.len(), n_points as usize, "seam recovery rebuilds index via store.scan()");
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
    let store = FsStore::new(dir.clone());
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
