//! F3 rollup lifecycle at the engine level: produce/query/watermark,
//! recovery via scan_rollups, per-tier retention, and journal rollback
//! of the rollup index (FEATURE_PLAN.md F3).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use timeless_core::{
    ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, EncodedRollupChunk, Engine,
    RollupTier, StoredChunk, StoredRollupChunk,
};

/// Minimal in-memory ChunkStore with rollup support. Not transactional —
/// journal tests assert ENGINE state only (the row side rides the host
/// transaction in the real store, which cli.sh §25 covers).
#[derive(Default)]
struct MemChunkStore {
    next_id: AtomicI64,
    // (id, series_id, resolution, meta-ish, payload)
    chunks: Mutex<Vec<(i64, i64, i64, ChunkMeta, Vec<u8>)>>,
    registry: Mutex<Option<Vec<u8>>>,
}

impl MemChunkStore {
    fn new() -> Self {
        Self {
            next_id: AtomicI64::new(1),
            chunks: Mutex::new(Vec::new()),
            registry: Mutex::new(None),
        }
    }
}

impl ChunkStore for MemChunkStore {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        let mut store = self.chunks.lock().unwrap();
        let mut locs = Vec::new();
        for cp in chunks {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let loc = ChunkLoc::Row { rowid: id };
            let mut payload = cp.ts_bytes.clone();
            let ts_len = payload.len();
            payload.extend_from_slice(&cp.val_bytes);
            let meta = ChunkMeta {
                min_ts: cp.min_ts,
                max_ts: cp.max_ts,
                max_ts_val: Some(cp.max_ts_val),
                point_count: cp.point_count,
                min_val: cp.min_val,
                max_val: cp.max_val,
                sum_val: cp.sum_val,
                loc: loc.clone(),
                encoding: cp.encoding,
            };
            // stash ts_len in resolution slot? No — store (ts_len) via
            // a parallel encoding: keep payload split point in meta-free
            // storage: prepend 8-byte ts_len.
            let mut framed = (ts_len as u64).to_le_bytes().to_vec();
            framed.extend_from_slice(&payload);
            store.push((id, cp.series_id, 0, meta, framed));
            locs.push(loc);
        }
        Ok(locs)
    }

    fn replace_chunks(
        &self,
        add: &[EncodedChunk],
        remove: &[ChunkLoc],
        on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        let locs = self.put_chunks(add)?;
        on_committed(&locs);
        self.delete_chunks(remove);
        Ok(locs)
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        let ChunkLoc::Row { rowid } = loc else {
            return Err("mem store uses Row locs".into());
        };
        let store = self.chunks.lock().unwrap();
        let (_, _, _, _, framed) = store
            .iter()
            .find(|(id, _, _, _, _)| id == rowid)
            .ok_or_else(|| format!("chunk {rowid} missing"))?;
        let ts_len = u64::from_le_bytes(framed[..8].try_into().unwrap()) as usize;
        let data = framed[8..].to_vec();
        let total = data.len();
        Ok(ChunkBytes {
            data: std::sync::Arc::new(data),
            ts_range: 0..ts_len,
            val_range: ts_len..total,
        })
    }

    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String> {
        let ids: Vec<i64> = locs
            .iter()
            .filter_map(|l| match l {
                ChunkLoc::Row { rowid } => Some(*rowid),
                _ => None,
            })
            .collect();
        self.chunks
            .lock()
            .unwrap()
            .retain(|(id, _, _, _, _)| !ids.contains(id));
        Vec::new()
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        Ok(self
            .chunks
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, res, _, _)| *res == 0)
            .map(|(_, sid, _, meta, _)| StoredChunk {
                series_id: *sid,
                meta: meta.clone(),
            })
            .collect())
    }

    fn scan_rollups(&self) -> Result<Vec<StoredRollupChunk>, String> {
        Ok(self
            .chunks
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, res, _, _)| *res > 0)
            .map(|(_, sid, res, meta, _)| StoredRollupChunk {
                series_id: *sid,
                resolution: *res,
                meta: meta.clone(),
            })
            .collect())
    }

    fn put_rollup_chunks(&self, chunks: &[EncodedRollupChunk]) -> Result<Vec<ChunkLoc>, String> {
        let mut store = self.chunks.lock().unwrap();
        let mut locs = Vec::new();
        for cp in chunks {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let loc = ChunkLoc::Row { rowid: id };
            let meta = ChunkMeta {
                min_ts: cp.min_ts,
                max_ts: cp.max_ts,
                max_ts_val: None,
                point_count: cp.bucket_count,
                min_val: 0.0,
                max_val: 0.0,
                sum_val: 0.0,
                loc: loc.clone(),
                encoding: timeless_core::ENC_ROLLUP_V1,
            };
            let mut framed = (cp.payload.len() as u64).to_le_bytes().to_vec();
            framed.extend_from_slice(&cp.payload);
            store.push((id, cp.series_id, cp.resolution, meta, framed));
            locs.push(loc);
        }
        Ok(locs)
    }

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String> {
        *self.registry.lock().unwrap() = Some(bytes.to_vec());
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        Ok(self.registry.lock().unwrap().clone())
    }

    fn storage_stats(&self) -> (u64, usize) {
        let store = self.chunks.lock().unwrap();
        (
            store.iter().map(|(_, _, _, _, p)| p.len() as u64).sum(),
            store.len(),
        )
    }

    fn sweep_cache(&self) {}
}

fn new_engine(store: Box<dyn ChunkStore>) -> Engine {
    Engine::with_store(store, 1_000_000, 0, 3, 64 << 20, false).unwrap()
}

fn labels() -> HashMap<String, String> {
    HashMap::new()
}

#[test]
fn rollup_produce_query_watermark_retention() {
    let store = std::sync::Arc::new(MemChunkStore::new());

    struct Shared(std::sync::Arc<MemChunkStore>);
    impl ChunkStore for Shared {
        fn put_chunks(&self, c: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
            self.0.put_chunks(c)
        }
        fn replace_chunks(
            &self,
            a: &[EncodedChunk],
            r: &[ChunkLoc],
            f: &mut dyn FnMut(&[ChunkLoc]),
        ) -> Result<Vec<ChunkLoc>, String> {
            self.0.replace_chunks(a, r, f)
        }
        fn read_chunk(&self, l: &ChunkLoc) -> Result<ChunkBytes, String> {
            self.0.read_chunk(l)
        }
        fn delete_chunks(&self, l: &[ChunkLoc]) -> Vec<String> {
            self.0.delete_chunks(l)
        }
        fn scan(&self) -> Result<Vec<StoredChunk>, String> {
            self.0.scan()
        }
        fn scan_rollups(&self) -> Result<Vec<StoredRollupChunk>, String> {
            self.0.scan_rollups()
        }
        fn put_rollup_chunks(&self, c: &[EncodedRollupChunk]) -> Result<Vec<ChunkLoc>, String> {
            self.0.put_rollup_chunks(c)
        }
        fn save_registry(&self, b: &[u8]) -> Result<(), String> {
            self.0.save_registry(b)
        }
        fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
            self.0.load_registry()
        }
        fn storage_stats(&self) -> (u64, usize) {
            self.0.storage_stats()
        }
        fn sweep_cache(&self) {}
    }

    let engine = new_engine(Box::new(Shared(store.clone())));
    engine.set_rollups(vec![
        RollupTier {
            resolution: 60,
            retention: 0,
        },
        RollupTier {
            resolution: 300,
            retention: 500,
        },
    ]);
    let sid = engine.resolve_cached("cpu", &labels()).unwrap();
    for i in 0..100 {
        engine.write_point(sid, 1000 + i * 10, i as f64);
    }
    engine.flush_all().unwrap();
    let (chunks, buckets) = engine.rollup().unwrap();
    assert!(chunks >= 2 && buckets > 0, "both tiers produced");

    // Query matches naive bucket math over the raw samples.
    let raw = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
    let rolled = engine
        .query_rollup_by_id(sid, 60, i64::MIN, i64::MAX)
        .unwrap();
    assert!(!rolled.is_empty());
    for b in &rolled {
        let members: Vec<f64> = raw
            .iter()
            .filter(|&&(ts, _)| ts.div_euclid(60) * 60 == b.bucket_ts)
            .map(|&(_, v)| v)
            .collect();
        assert_eq!(b.count as usize, members.len(), "bucket {}", b.bucket_ts);
        let sum = members.iter().fold(0.0f64, |a, &v| a + v);
        assert_eq!(b.sum.to_bits(), sum.to_bits(), "bucket {}", b.bucket_ts);
    }

    // Idempotent: nothing new on re-run.
    assert_eq!(engine.rollup().unwrap(), (0, 0));

    // Recovery: a fresh engine over the same store sees the same buckets.
    let engine2 = new_engine(Box::new(Shared(store.clone())));
    let rolled2 = engine2
        .query_rollup_by_id(sid, 60, i64::MIN, i64::MAX)
        .unwrap();
    assert_eq!(rolled.len(), rolled2.len(), "rollup index recovered");

    // The packed-TVF primitive retains requested series order and exactly
    // matches the established single-series read, including empty ids.
    let batch = engine2
        .query_rollup_batch_by_id(&[sid + 10_000, sid], 60, i64::MIN, i64::MAX)
        .unwrap();
    assert_eq!(batch[0], (sid + 10_000, Vec::new()));
    assert_eq!(batch[1], (sid, rolled2.clone()));

    // Per-tier retention: advance raw far enough that the 300s tier's
    // 500-unit retention prunes its old chunk, while the 60s tier
    // (retention 0 = forever) keeps everything.
    engine2.set_rollups(vec![
        RollupTier {
            resolution: 60,
            retention: 0,
        },
        RollupTier {
            resolution: 300,
            retention: 500,
        },
    ]);
    engine2.write_point(sid, 5000, 1.0);
    engine2.flush_all().unwrap();
    let r300 = engine2
        .query_rollup_by_id(sid, 300, i64::MIN, i64::MAX)
        .unwrap();
    assert!(
        r300.is_empty(),
        "300s tier pruned by its 500-unit retention (cutoff 4500)"
    );
    let r60 = engine2
        .query_rollup_by_id(sid, 60, i64::MIN, i64::MAX)
        .unwrap();
    assert_eq!(r60.len(), rolled.len(), "keep-forever tier untouched");
}

/// THE LADDER'S PURPOSE: raw ages out, coarse survives. Raw retention
/// prunes the old epoch; the keep-forever tier still answers for it.
#[test]
fn rollups_survive_raw_retention() {
    let engine = new_engine(Box::new(MemChunkStore::new()));
    engine.set_retention(Some(1_000));
    engine.set_rollups(vec![RollupTier {
        resolution: 60,
        retention: 0,
    }]);
    let sid = engine.resolve_cached("cpu", &labels()).unwrap();

    // Three raw windows, rolled as they settle.
    for epoch in 0..3i64 {
        for i in 0..100 {
            engine.write_point(sid, epoch * 2_000 + i * 10, i as f64);
        }
        engine.flush_all().unwrap();
        engine.rollup().unwrap();
    }
    // Advance far enough that ALL earlier raw is pruned.
    engine.write_point(sid, 10_000, 0.0);
    engine.flush_all().unwrap();

    let raw = engine.query_range_by_id(sid, 0, 5_000).unwrap();
    assert!(
        raw.is_empty(),
        "old raw pruned by retention ({} left)",
        raw.len()
    );

    let rolled = engine.query_rollup_by_id(sid, 60, 0, 5_000).unwrap();
    assert!(
        !rolled.is_empty(),
        "rollups still answer for the pruned raw window"
    );
    // Epoch 0's first bucket is fully intact in the rollup.
    let b0 = rolled.iter().find(|b| b.bucket_ts == 0).expect("bucket 0");
    assert_eq!(b0.count, 6, "ts 0..50 (6 samples) in bucket 0");
    assert_eq!(b0.sum, (0 + 1 + 2 + 3 + 4 + 5) as f64);
}

#[test]
fn rollup_rollback_restores_index() {
    let engine = new_engine(Box::new(MemChunkStore::new()));
    engine.set_rollups(vec![RollupTier {
        resolution: 60,
        retention: 0,
    }]);
    let sid = engine.resolve_cached("cpu", &labels()).unwrap();
    for i in 0..50 {
        engine.write_point(sid, 1000 + i * 10, i as f64);
    }
    engine.flush_all().unwrap();

    // Rollup inside a transaction, then roll back: the engine's rollup
    // index must forget the entries (the real store's rows ride the
    // host txn; cli.sh §25 covers that half).
    engine.txn_begin();
    let (chunks, _) = engine.rollup().unwrap();
    assert!(chunks > 0);
    assert!(!engine
        .query_rollup_by_id(sid, 60, i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
    engine.txn_rollback();
    assert!(
        engine
            .query_rollup_by_id(sid, 60, i64::MIN, i64::MAX)
            .unwrap()
            .is_empty(),
        "rollback removed rolled-up index entries"
    );
}
