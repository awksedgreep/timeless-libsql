//! P2: pure-append delta refresh vs the full-reload fallback.
//!
//! A scripted authoritative store counts full scans vs delta scans and
//! lets the test mutate "the file" externally (as replication or a
//! second process would). Pinned properties:
//!   1. appended series+chunks refresh via ONE delta scan, zero full
//!      scans, and the new data is queryable;
//!   2. a shape change (delete) forces the full reload;
//!   3. delta application is idempotent by ChunkLoc — rows the engine
//!      already indexed (its own flushes, a stale watermark) are never
//!      double-counted.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use timeless_core::{
    ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, Engine, ResolvedSeries,
    StoredChunk, StoredRollupChunk, StoredSeries,
};

#[derive(Default)]
struct State {
    series: Vec<StoredSeries>,
    /// (rowid, series_id, meta, ts payload, val payload)
    chunks: Vec<(i64, i64, ChunkMeta, Vec<u8>, Vec<u8>)>,
    next_rowid: i64,
    chunk_gen: i64,
    shape_gen: i64,
    full_scans: AtomicUsize,
    delta_scans: AtomicUsize,
}

struct ScriptedStore(Arc<Mutex<State>>);

fn lock(s: &ScriptedStore) -> std::sync::MutexGuard<'_, State> {
    s.0.lock().unwrap()
}

impl ChunkStore for ScriptedStore {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        let mut st = lock(self);
        let mut locs = Vec::new();
        for c in chunks {
            st.next_rowid += 1;
            let rowid = st.next_rowid;
            let meta = ChunkMeta {
                min_ts: c.min_ts,
                max_ts: c.max_ts,
                max_ts_val: Some(c.max_ts_val),
                point_count: c.point_count,
                min_val: c.min_val,
                max_val: c.max_val,
                sum_val: c.sum_val,
                loc: ChunkLoc::Row { rowid },
                encoding: c.encoding,
            };
            st.chunks
                .push((rowid, c.series_id, meta, c.ts_bytes.clone(), c.val_bytes.clone()));
            locs.push(ChunkLoc::Row { rowid });
        }
        st.chunk_gen += 1;
        Ok(locs)
    }

    fn replace_chunks(
        &self,
        _add: &[EncodedChunk],
        _remove: &[ChunkLoc],
        _on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        Err("not scripted".into())
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        let st = lock(self);
        let ChunkLoc::Row { rowid } = loc else {
            return Err("bad loc".into());
        };
        let (_, _, _, ts, val) = st
            .chunks
            .iter()
            .find(|(r, ..)| r == rowid)
            .ok_or("missing chunk")?;
        let ts_len = ts.len();
        let mut buf = ts.clone();
        buf.extend_from_slice(val);
        Ok(ChunkBytes {
            data: Arc::new(buf),
            ts_range: 0..ts_len,
            val_range: ts_len..ts_len + val.len(),
        })
    }

    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String> {
        let mut st = lock(self);
        st.chunks.retain(|(r, ..)| {
            !locs.iter().any(|l| matches!(l, ChunkLoc::Row { rowid } if rowid == r))
        });
        st.chunk_gen += 1;
        st.shape_gen += 1;
        Vec::new()
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        let st = lock(self);
        st.full_scans.fetch_add(1, Ordering::SeqCst);
        Ok(st
            .chunks
            .iter()
            .map(|(_, sid, meta, ..)| StoredChunk {
                series_id: *sid,
                meta: meta.clone(),
            })
            .collect())
    }

    fn scan_since(
        &self,
        after_rowid: i64,
    ) -> Result<(Vec<StoredChunk>, Vec<StoredRollupChunk>), String> {
        let st = lock(self);
        st.delta_scans.fetch_add(1, Ordering::SeqCst);
        Ok((
            st.chunks
                .iter()
                .filter(|(r, ..)| *r > after_rowid)
                .map(|(_, sid, meta, ..)| StoredChunk {
                    series_id: *sid,
                    meta: meta.clone(),
                })
                .collect(),
            Vec::new(),
        ))
    }

    fn append_watermark(&self) -> Result<Option<(i64, i64)>, String> {
        let st = lock(self);
        Ok(Some((st.shape_gen, st.next_rowid)))
    }

    fn save_registry(&self, _bytes: &[u8]) -> Result<(), String> {
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    fn has_authoritative_series(&self) -> bool {
        true
    }

    fn load_series(&self) -> Result<Vec<StoredSeries>, String> {
        Ok(lock(self).series.clone())
    }

    fn load_series_since(&self, after_id: i64) -> Result<Vec<StoredSeries>, String> {
        Ok(lock(self)
            .series
            .iter()
            .filter(|s| s.id > after_id)
            .cloned()
            .collect())
    }

    fn resolve_series(
        &self,
        name: &str,
        labels: &[(String, String)],
    ) -> Result<ResolvedSeries, String> {
        let mut st = lock(self);
        if let Some(s) = st
            .series
            .iter()
            .find(|s| s.name == name && s.labels == labels)
        {
            return Ok(ResolvedSeries {
                id: s.id,
                created: false,
            });
        }
        let id = st.series.iter().map(|s| s.id).max().unwrap_or(0) + 1;
        st.series.push(StoredSeries {
            id,
            name: name.to_string(),
            labels: labels.to_vec(),
        });
        Ok(ResolvedSeries { id, created: true })
    }

    fn catalog_generation(&self) -> Result<Option<(i64, i64)>, String> {
        let st = lock(self);
        let max_id = st.series.iter().map(|s| s.id).max().unwrap_or(0);
        Ok(Some((max_id, st.chunk_gen)))
    }

    fn storage_stats(&self) -> (u64, usize) {
        (0, 0)
    }

    fn sweep_cache(&self) {}
}

fn engine_over(state: Arc<Mutex<State>>) -> Engine {
    Engine::with_store(Box::new(ScriptedStore(state)), 4, 0, 3, 64 << 20, false).unwrap()
}

fn counters(state: &Arc<Mutex<State>>) -> (usize, usize) {
    let st = state.lock().unwrap();
    (
        st.full_scans.load(Ordering::SeqCst),
        st.delta_scans.load(Ordering::SeqCst),
    )
}

/// Write points through a SECOND engine over the same state — the
/// closest core-level model of an external process committing to the
/// shared file.
fn external_append(state: &Arc<Mutex<State>>, metric: &str, ts0: i64) {
    let other = engine_over(Arc::clone(state));
    let labels: HashMap<String, String> = HashMap::new();
    let sid = other.resolve_cached(metric, &labels).unwrap();
    for i in 0..5 {
        other.write_point(sid, ts0 + i, i as f64);
    }
    other.flush_all().unwrap();
}

#[test]
fn appended_series_and_chunks_refresh_via_delta() {
    let state = Arc::new(Mutex::new(State::default()));
    let engine = engine_over(Arc::clone(&state));
    external_append(&state, "cpu", 100);

    let (full_before, _) = counters(&state);
    engine.refresh_authoritative_state().unwrap();
    let (full_after, delta_after) = counters(&state);
    assert_eq!(full_after, full_before, "delta path must not full-scan");
    assert_eq!(delta_after, 1, "exactly one delta scan");

    // The externally appended series is queryable through this engine.
    let labels: HashMap<String, String> = HashMap::new();
    let sid = engine.resolve_cached("cpu", &labels).unwrap();
    let pts = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
    assert_eq!(pts.len(), 5, "appended points visible after delta refresh");

    // Unchanged store: the very next refresh is a pure fast path.
    engine.refresh_authoritative_state().unwrap();
    assert_eq!(counters(&state), (full_after, delta_after));
}

#[test]
fn shape_change_forces_full_reload() {
    let state = Arc::new(Mutex::new(State::default()));
    let engine = engine_over(Arc::clone(&state));
    external_append(&state, "cpu", 100);
    engine.refresh_authoritative_state().unwrap();
    let (full0, delta0) = counters(&state);

    // External retention prune: delete one chunk row (shape bump).
    {
        let loc = {
            let st = state.lock().unwrap();
            st.chunks.first().map(|(r, ..)| ChunkLoc::Row { rowid: *r }).unwrap()
        };
        let store = ScriptedStore(Arc::clone(&state));
        store.delete_chunks(&[loc]);
    }
    engine.refresh_authoritative_state().unwrap();
    let (full1, delta1) = counters(&state);
    assert_eq!(full1, full0 + 1, "shape change must take the full reload");
    assert_eq!(delta1, delta0, "no delta scan across a shape change");

    let labels: HashMap<String, String> = HashMap::new();
    let sid = engine.resolve_cached("cpu", &labels).unwrap();
    let pts = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
    assert_eq!(pts.len(), 0, "pruned chunk gone after full reload");
}

#[test]
fn delta_apply_is_idempotent_for_already_indexed_rows() {
    let state = Arc::new(Mutex::new(State::default()));
    let engine = engine_over(Arc::clone(&state));

    // THIS engine flushes (its own index already has the chunk, and its
    // commit did not publish, so gen+watermark are both stale)...
    let labels: HashMap<String, String> = HashMap::new();
    let sid = engine.resolve_cached("cpu", &labels).unwrap();
    for i in 0..5 {
        engine.write_point(sid, 100 + i, i as f64);
    }
    engine.flush_all().unwrap();

    // ...then an external append lands and the engine refreshes: the
    // delta re-reads its own flushed row plus the new one.
    external_append(&state, "mem", 500);
    engine.refresh_authoritative_state().unwrap();

    let pts = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
    assert_eq!(
        pts.len(),
        5,
        "re-read own chunk must be skipped, not double-counted"
    );
    let sid2 = engine.resolve_cached("mem", &labels).unwrap();
    let pts2 = engine.query_range_by_id(sid2, i64::MIN, i64::MAX).unwrap();
    assert_eq!(pts2.len(), 5, "external append visible");
}
