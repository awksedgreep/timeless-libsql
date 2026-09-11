//! F3 rollup lifecycle at the engine level: produce/query/watermark,
//! recovery via scan_rollups, per-tier retention, and journal rollback
//! of the rollup index (FEATURE_PLAN.md F3).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use timeless_core::{
    ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, EncodedRollupChunk, Engine,
    RollupTier, StoredChunk, StoredRollupChunk,
};

type StoredTestChunk = (i64, i64, i64, ChunkMeta, Vec<u8>);

/// Minimal in-memory ChunkStore with rollup support. Not transactional —
/// journal tests assert ENGINE state only (the row side rides the host
/// transaction in the real store, which cli.sh §25 covers).
#[derive(Default)]
struct MemChunkStore {
    next_id: AtomicI64,
    // (id, series_id, resolution, meta-ish, payload)
    chunks: Mutex<Vec<StoredTestChunk>>,
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
                payload_bytes: (cp.ts_bytes.len() + cp.val_bytes.len()) as u64,
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
                payload_bytes: cp.payload.len() as u64,
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

#[derive(Clone)]
struct SharedMemChunkStore(Arc<MemChunkStore>);

impl ChunkStore for SharedMemChunkStore {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        self.0.put_chunks(chunks)
    }

    fn replace_chunks(
        &self,
        add: &[EncodedChunk],
        remove: &[ChunkLoc],
        on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        self.0.replace_chunks(add, remove, on_committed)
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        self.0.read_chunk(loc)
    }

    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String> {
        self.0.delete_chunks(locs)
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        self.0.scan()
    }

    fn scan_rollups(&self) -> Result<Vec<StoredRollupChunk>, String> {
        self.0.scan_rollups()
    }

    fn put_rollup_chunks(&self, chunks: &[EncodedRollupChunk]) -> Result<Vec<ChunkLoc>, String> {
        self.0.put_rollup_chunks(chunks)
    }

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String> {
        self.0.save_registry(bytes)
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        self.0.load_registry()
    }

    fn storage_stats(&self) -> (u64, usize) {
        self.0.storage_stats()
    }

    fn sweep_cache(&self) {}
}

fn new_engine(store: Box<dyn ChunkStore>) -> Engine {
    Engine::with_store(store, 1_000_000, 0, 3, 64 << 20, false).unwrap()
}

fn labels() -> HashMap<String, String> {
    HashMap::new()
}

fn compressed_chunk(series_id: i64, start_ts: i64, point_count: usize) -> EncodedChunk {
    let timestamps: Vec<i64> = (0..point_count)
        .map(|offset| start_ts + offset as i64)
        .collect();
    let values: Vec<f64> = timestamps
        .iter()
        .map(|timestamp| *timestamp as f64)
        .collect();
    let config = pco::ChunkConfig::default();
    EncodedChunk {
        series_id,
        min_ts: timestamps[0],
        max_ts: *timestamps.last().unwrap(),
        max_ts_val: *values.last().unwrap(),
        point_count: point_count as u32,
        min_val: values[0],
        max_val: *values.last().unwrap(),
        sum_val: values.iter().sum(),
        encoding: timeless_core::store::ENC_PCO,
        ts_bytes: pco::standalone::simple_compress(&timestamps, &config).unwrap(),
        val_bytes: pco::standalone::simple_compress(&values, &config).unwrap(),
    }
}

#[test]
fn rollup_produce_query_watermark_retention() {
    let store = Arc::new(MemChunkStore::new());
    let engine = new_engine(Box::new(SharedMemChunkStore(store.clone())));
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
    let engine2 = new_engine(Box::new(SharedMemChunkStore(store.clone())));
    assert_eq!(
        engine2.info().rollup_chunk_count,
        chunks,
        "all persisted rollup entries recovered"
    );
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

    let indexed = engine2.info().rollup_chunk_count;
    engine2.set_rollups(Vec::new());
    let mut deleted_total = 0;
    loop {
        let (deleted, more, errors) = engine2.clear_rollups_bounded(1);
        assert!(errors.is_empty(), "clear errors: {errors:?}");
        assert!(deleted <= 1, "bounded cleanup exceeded its chunk budget");
        deleted_total += deleted;
        if !more {
            break;
        }
    }
    assert_eq!(deleted_total, indexed);
    assert_eq!(engine2.info().rollup_chunk_count, 0);
}

#[test]
fn bounded_metrics_maintenance_drains_across_commit_sized_steps() {
    let engine = Engine::with_store(
        Box::new(MemChunkStore::new()),
        1_000_000,
        0,
        3,
        64 << 20,
        true,
    )
    .unwrap();
    let series_ids: Vec<i64> = (0..3)
        .map(|number| {
            engine
                .resolve_cached(&format!("bounded_{number}"), &labels())
                .unwrap()
        })
        .collect();

    // Two flushes leave two raw chunks per series. Raw conversion is always
    // actionable even though these tiny groups are intentionally too small
    // for a later compressed merge.
    for epoch in 0..2 {
        for &series_id in &series_ids {
            for offset in 0..10 {
                engine.write_point(series_id, epoch * 1_000 + offset * 10, offset as f64);
            }
        }
        engine.flush_all().unwrap();
    }

    let mut compacted = 0;
    let mut compact_steps = 0;
    loop {
        let (series, _chunks, more) = engine.compact_partitions_bounded(i64::MAX, 1).unwrap();
        assert!(series <= 1, "one step exceeded its series budget");
        compacted += series;
        compact_steps += 1;
        if !more {
            break;
        }
    }
    assert_eq!(compacted, 3);
    assert_eq!(compact_steps, 3);
    for &series_id in &series_ids {
        assert_eq!(
            engine
                .query_range_by_id(series_id, i64::MIN, i64::MAX)
                .unwrap()
                .len(),
            20,
            "bounded compaction preserves every raw point"
        );
    }

    let tier = RollupTier {
        resolution: 60,
        retention: 0,
    };
    engine.set_rollups(vec![tier]);
    let (first_chunks, _first_buckets, first_more) = engine.rollup_bounded(1).unwrap();
    assert_eq!(first_chunks, 1);
    assert!(first_more);

    // Opening another connection reapplies the same persisted ladder. That
    // idempotent setup must not rewind an in-progress shared-engine cycle.
    engine.set_rollups(vec![tier]);
    let mut rolled = first_chunks;
    let mut rollup_steps = 1;
    loop {
        let (chunks, _buckets, more) = engine.rollup_bounded(1).unwrap();
        assert!(chunks <= 1, "one step exceeded its group budget");
        rolled += chunks;
        rollup_steps += 1;
        if !more {
            break;
        }
    }
    assert_eq!(rolled, 3);
    assert_eq!(rollup_steps, 3);
    for &series_id in &series_ids {
        assert!(
            !engine
                .query_rollup_by_id(series_id, 60, i64::MIN, i64::MAX)
                .unwrap()
                .is_empty(),
            "each series is visited across the bounded rollup cycle"
        );
    }
}

#[test]
fn repeated_raw_arrivals_merge_by_size_tier_without_rewriting_the_growing_tail() {
    const ROUNDS: usize = 64;
    const POINTS_PER_ROUND: usize = 1024;

    let engine = Engine::with_store(
        Box::new(MemChunkStore::new()),
        1_000_000,
        0,
        3,
        64 << 20,
        true,
    )
    .unwrap();
    let series_id = engine.resolve_cached("tiered", &labels()).unwrap();
    let mut previous_merge_points = 0u64;

    for round in 0..ROUNDS {
        for offset in 0..POINTS_PER_ROUND {
            let ts = (round * POINTS_PER_ROUND + offset) as i64;
            engine.write_point(series_id, ts, ts as f64);
        }
        engine.flush_all().unwrap();

        loop {
            let (_, _, more) = engine.compact_partitions_bounded(i64::MAX, 64).unwrap();
            if !more {
                break;
            }
        }

        let expected = (round + 1) * POINTS_PER_ROUND;
        let points = engine
            .query_range_by_id(series_id, i64::MIN, i64::MAX)
            .unwrap();
        assert_eq!(points.len(), expected);
        assert_eq!(points.first().unwrap().0, 0);
        assert_eq!(points.last().unwrap().0, expected as i64 - 1);

        let info = engine.info();
        let merge_points = info
            .compaction_merge_points
            .saturating_sub(previous_merge_points);
        assert!(
            merge_points <= timeless_core::METRICS_COMPACTION_TARGET_POINTS as u64,
            "one fixed append rewrote {merge_points} compressed points"
        );
        previous_merge_points = info.compaction_merge_points;
    }

    let ingested = (ROUNDS * POINTS_PER_ROUND) as u64;
    let info = engine.info();
    assert_eq!(info.compaction_raw_points, ingested);
    assert_eq!(info.compaction_raw_input_bytes, ingested * 16);
    assert!(info.compaction_raw_output_bytes < info.compaction_raw_input_bytes);
    assert_eq!(info.compaction_merge_points, ingested + ingested / 2);
    assert!(info.compaction_merge_input_bytes > 0);
    assert!(info.compaction_merge_output_bytes > 0);
    assert_eq!(
        engine
            .query_range_by_id(series_id, i64::MIN, i64::MAX)
            .unwrap()
            .len(),
        ingested as usize
    );
}

#[test]
fn compressed_merge_reports_a_newly_unlocked_next_tier() {
    const CHUNKS: usize = 6;
    const POINTS_PER_CHUNK: usize = 8 * 1024;
    // Five equal peers fill the planner's 125%-of-target group. Its 40K
    // replacement splits into 32K + 8K; that remainder and the sixth 8K
    // source form a newly actionable tier only after the first swap commits.
    let store = Arc::new(MemChunkStore::new());
    let registry_engine = Engine::with_store(
        Box::new(SharedMemChunkStore(store.clone())),
        1_000_000,
        0,
        3,
        64 << 20,
        true,
    )
    .unwrap();
    let series_id = registry_engine
        .resolve_cached("cascade", &labels())
        .unwrap();
    registry_engine.flush_all().unwrap();
    drop(registry_engine);
    let chunks: Vec<_> = (0..CHUNKS)
        .map(|chunk| {
            compressed_chunk(
                series_id,
                (chunk * POINTS_PER_CHUNK) as i64,
                POINTS_PER_CHUNK,
            )
        })
        .collect();
    store.put_chunks(&chunks).unwrap();
    let engine = Engine::with_store(
        Box::new(SharedMemChunkStore(store)),
        1_000_000,
        0,
        3,
        64 << 20,
        true,
    )
    .unwrap();

    let (_, first_sources, more) = engine.compact_partitions_bounded(i64::MAX, 64).unwrap();
    assert_eq!(first_sources, 5);
    assert!(more, "the committed 8K remainder unlocks another tier");

    let (_, second_sources, more) = engine.compact_partitions_bounded(i64::MAX, 64).unwrap();
    assert_eq!(second_sources, 2);
    assert!(!more);
    let info = engine.info();
    assert_eq!(info.compaction_merge_steps, 2);
    assert_eq!(
        info.compaction_merge_points,
        (5 * POINTS_PER_CHUNK + 2 * POINTS_PER_CHUNK) as u64
    );
    assert_eq!(
        engine
            .query_range_by_id(series_id, i64::MIN, i64::MAX)
            .unwrap()
            .len(),
        CHUNKS * POINTS_PER_CHUNK
    );
}

#[test]
fn bounded_metrics_compaction_caps_each_transaction_by_input_points_and_bytes() {
    const SERIES: usize = 9;
    const POINTS: usize = timeless_core::METRICS_COMPACTION_TARGET_POINTS;

    let engine = Engine::with_store(
        Box::new(MemChunkStore::new()),
        1_000_000,
        0,
        3,
        128 << 20,
        true,
    )
    .unwrap();
    let series_ids: Vec<_> = (0..SERIES)
        .map(|number| {
            engine
                .resolve_cached(&format!("budget_{number}"), &labels())
                .unwrap()
        })
        .collect();
    for &series_id in &series_ids {
        for offset in 0..POINTS {
            engine.write_point(series_id, offset as i64, offset as f64);
        }
    }
    engine.flush_all().unwrap();

    let before = engine.info();
    let (series, _, more) = engine.compact_partitions_bounded(i64::MAX, 64).unwrap();
    let after = engine.info();
    assert_eq!(series, 8, "point budget should admit eight target groups");
    assert!(more);
    assert_eq!(
        after.compaction_raw_points - before.compaction_raw_points,
        timeless_core::METRICS_COMPACTION_STEP_INPUT_POINTS as u64
    );
    assert_eq!(
        after.compaction_raw_input_bytes - before.compaction_raw_input_bytes,
        timeless_core::METRICS_COMPACTION_STEP_INPUT_BYTES
    );

    let before = after;
    let (series, _, more) = engine.compact_partitions_bounded(i64::MAX, 64).unwrap();
    let after = engine.info();
    assert_eq!(series, 1);
    assert!(!more);
    assert_eq!(
        after.compaction_raw_points - before.compaction_raw_points,
        POINTS as u64
    );
    for series_id in series_ids {
        assert_eq!(
            engine
                .query_range_by_id(series_id, i64::MIN, i64::MAX)
                .unwrap()
                .len(),
            POINTS
        );
    }
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
    assert_eq!(b0.sum, 15.0);
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

#[test]
fn rollup_configuration_rollback_restores_the_previous_ladder() {
    let engine = new_engine(Box::new(MemChunkStore::new()));
    let original = vec![RollupTier {
        resolution: 60,
        retention: 3_600,
    }];
    engine.set_rollups(original.clone());

    engine.txn_begin();
    engine.set_rollups_transactional(Vec::new());
    assert!(engine.rollup_tiers().is_empty());
    engine.txn_rollback();

    assert_eq!(engine.rollup_tiers(), original);
}
