//! MemSpanStore: a HashMap-backed SpanBlockStore for unit-testing the
//! SpanBlockEngine without SQLite — the traces twin of blocks/mem.rs
//! (same "no fs backend, SQLite transactions replaced all that
//! machinery" reasoning; read that header). The one addition is the
//! in-memory TRACE INDEX, maintained under exactly the contract the
//! SQL store honors: trace rows appear with their block and disappear
//! with it, never dangling.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use super::{BlockLoc, BlockMeta, EncodedSpanBlock, SpanBlockStore, SpanDurationBounds};

#[derive(Default)]
struct MemInner {
    next_id: i64,
    /// id → (meta, payload, optional duration extrema)
    blocks: HashMap<i64, (BlockMeta, Vec<u8>, Option<SpanDurationBounds>)>,
    /// term → posting list of block ids.
    terms: HashMap<String, BTreeSet<i64>>,
    /// packed trace id → block ids containing that trace's spans —
    /// the `_trace_blocks` shadow table's in-memory shape.
    traces: HashMap<[u8; 16], BTreeSet<i64>>,
    meta_kv: HashMap<String, Vec<u8>>,
}

#[derive(Default)]
pub struct MemSpanStore {
    inner: Mutex<MemInner>,
}

impl MemSpanStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn insert_one(
        inner: &mut MemInner,
        block: &EncodedSpanBlock,
        duration_bounds: Option<SpanDurationBounds>,
    ) -> BlockLoc {
        inner.next_id += 1;
        let id = inner.next_id;
        inner
            .blocks
            .insert(id, (block.meta, block.data.clone(), duration_bounds));
        for term in &block.terms {
            inner.terms.entry(term.clone()).or_default().insert(id);
        }
        for tid in &block.trace_ids {
            inner.traces.entry(*tid).or_default().insert(id);
        }
        BlockLoc { id }
    }

    /// Remove blocks and their posting-list AND trace-index entries
    /// together — the never-dangle contract the SQL store honors.
    fn remove_ids(inner: &mut MemInner, ids: &[i64]) {
        for id in ids {
            inner.blocks.remove(id);
        }
        inner.terms.retain(|_, set| {
            for id in ids {
                set.remove(id);
            }
            !set.is_empty()
        });
        inner.traces.retain(|_, set| {
            for id in ids {
                set.remove(id);
            }
            !set.is_empty()
        });
    }

    /// Test helper: number of live (trace_id, block_id) index rows —
    /// lets tests prove prune/optimize left nothing dangling.
    pub fn trace_index_rows(&self) -> usize {
        self.lock().traces.values().map(|s| s.len()).sum()
    }
}

impl SpanBlockStore for MemSpanStore {
    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String> {
        let mut inner = self.lock();
        Ok(blocks
            .iter()
            .map(|b| Self::insert_one(&mut inner, b, None))
            .collect())
    }

    fn put_blocks_with_duration_bounds(
        &self,
        blocks: &[EncodedSpanBlock],
        duration_bounds: &[SpanDurationBounds],
    ) -> Result<Vec<BlockLoc>, String> {
        if blocks.len() != duration_bounds.len() {
            return Err(format!(
                "span block/duration metadata length mismatch: {} blocks, {} bounds",
                blocks.len(),
                duration_bounds.len()
            ));
        }
        let mut inner = self.lock();
        Ok(blocks
            .iter()
            .zip(duration_bounds)
            .map(|(block, bounds)| Self::insert_one(&mut inner, block, Some(*bounds)))
            .collect())
    }

    fn replace_blocks(
        &self,
        add: &[EncodedSpanBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        let locs: Vec<BlockLoc> = {
            let mut inner = self.lock();
            add.iter()
                .map(|b| Self::insert_one(&mut inner, b, None))
                .collect()
            // lock released before on_committed — same deadlock-shyness
            // as MemBlockStore (the callback re-locks the engine index).
        };
        on_committed(&locs);
        let ids: Vec<i64> = remove.iter().map(|l| l.id).collect();
        Self::remove_ids(&mut self.lock(), &ids);
        Ok(locs)
    }

    fn replace_blocks_with_duration_bounds(
        &self,
        add: &[EncodedSpanBlock],
        duration_bounds: &[SpanDurationBounds],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        if add.len() != duration_bounds.len() {
            return Err(format!(
                "span block/duration metadata length mismatch: {} blocks, {} bounds",
                add.len(),
                duration_bounds.len()
            ));
        }
        let locs: Vec<BlockLoc> = {
            let mut inner = self.lock();
            add.iter()
                .zip(duration_bounds)
                .map(|(block, bounds)| Self::insert_one(&mut inner, block, Some(*bounds)))
                .collect()
        };
        on_committed(&locs);
        let ids: Vec<i64> = remove.iter().map(|location| location.id).collect();
        Self::remove_ids(&mut self.lock(), &ids);
        Ok(locs)
    }

    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.lock()
            .blocks
            .get(&loc.id)
            .map(|(_, data, _)| data.clone())
            .ok_or_else(|| format!("MemSpanStore: no block with id {}", loc.id))
    }

    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        let mut inner = self.lock();
        let mut errors = Vec::new();
        let mut ids = Vec::with_capacity(locs.len());
        for loc in locs {
            if inner.blocks.contains_key(&loc.id) {
                ids.push(loc.id);
            } else {
                errors.push(format!("MemSpanStore: no block with id {}", loc.id));
            }
        }
        Self::remove_ids(&mut inner, &ids);
        errors
    }

    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        let inner = self.lock();
        let mut out: Vec<(BlockMeta, BlockLoc)> = inner
            .blocks
            .iter()
            .map(|(id, (meta, _, _))| (*meta, BlockLoc { id: *id }))
            .collect();
        out.sort_by_key(|(_, loc)| loc.id); // deterministic for tests
        Ok(out)
    }

    fn blocks_missing_duration_bounds(&self) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let inner = self.lock();
        let mut blocks = inner
            .blocks
            .iter()
            .filter_map(|(id, (meta, _, bounds))| {
                bounds.is_none().then_some((BlockLoc { id: *id }, *meta))
            })
            .collect::<Vec<_>>();
        blocks.sort_by_key(|(location, meta)| (meta.ts_min, location.id));
        Ok(blocks)
    }

    fn update_duration_bounds(
        &self,
        updates: &[(BlockLoc, SpanDurationBounds)],
    ) -> Result<(), String> {
        let mut inner = self.lock();
        for (location, bounds) in updates {
            let Some((_, _, current)) = inner.blocks.get_mut(&location.id) else {
                return Err(format!(
                    "MemSpanStore: no block with id {} during duration-bound backfill",
                    location.id
                ));
            };
            *current = Some(*bounds);
        }
        Ok(())
    }

    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let inner = self.lock();
        let mut ids: Option<BTreeSet<i64>> = None;
        for term in terms {
            let set = inner.terms.get(term).cloned().unwrap_or_default();
            ids = Some(match ids {
                None => set,
                Some(prev) => prev.intersection(&set).copied().collect(),
            });
        }
        let mut out = Vec::new();
        match ids {
            Some(ids) => {
                for id in ids {
                    if let Some((meta, _, _)) = inner.blocks.get(&id) {
                        if meta.ts_min <= ts_max && meta.ts_max >= ts_min {
                            out.push((BlockLoc { id }, *meta));
                        }
                    }
                }
            }
            None => {
                let mut all: Vec<(i64, BlockMeta)> = inner
                    .blocks
                    .iter()
                    .filter(|(_, (m, _, _))| m.ts_min <= ts_max && m.ts_max >= ts_min)
                    .map(|(id, (m, _, _))| (*id, *m))
                    .collect();
                all.sort_unstable_by_key(|(id, _)| *id);
                out.extend(all.into_iter().map(|(id, m)| (BlockLoc { id }, m)));
            }
        }
        Ok(out)
    }

    fn query_terms_with_duration_bounds(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
        duration_min_ns: i64,
        duration_max_ns: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let mut candidates = self.query_terms(terms, ts_min, ts_max)?;
        let inner = self.lock();
        candidates.retain(|(location, _)| {
            inner
                .blocks
                .get(&location.id)
                .and_then(|(_, _, bounds)| *bounds)
                .is_none_or(|bounds| !bounds.excludes(duration_min_ns, duration_max_ns))
        });
        Ok(candidates)
    }

    fn query_trace(&self, trace_id: &[u8; 16]) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let inner = self.lock();
        let mut out = Vec::new();
        if let Some(ids) = inner.traces.get(trace_id) {
            for id in ids {
                // A trace row without its block would be a broken
                // never-dangle contract; surface it loudly.
                let (meta, _, _) = inner
                    .blocks
                    .get(id)
                    .ok_or_else(|| format!("MemSpanStore: dangling trace row → block {id}"))?;
                out.push((BlockLoc { id: *id }, *meta));
            }
        }
        Ok(out)
    }

    fn query_trace_with_duration_bounds(
        &self,
        trace_id: &[u8; 16],
        ts_min: i64,
        ts_max: i64,
        duration_min_ns: i64,
        duration_max_ns: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let mut candidates = self.query_trace(trace_id)?;
        let inner = self.lock();
        candidates.retain(|(location, meta)| {
            meta.ts_min <= ts_max
                && meta.ts_max >= ts_min
                && inner
                    .blocks
                    .get(&location.id)
                    .and_then(|(_, _, bounds)| *bounds)
                    .is_none_or(|bounds| !bounds.excludes(duration_min_ns, duration_max_ns))
        });
        Ok(candidates)
    }

    fn query_term_values(&self, prefix: &str) -> Result<Option<Vec<String>>, String> {
        let inner = self.lock();
        let values = inner
            .terms
            .keys()
            .filter_map(|term| term.strip_prefix(prefix).map(ToOwned::to_owned))
            .collect();
        Ok(Some(values))
    }

    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.lock().meta_kv.insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.lock().meta_kv.get(key).cloned())
    }
}
