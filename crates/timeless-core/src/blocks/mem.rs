//! MemBlockStore: a HashMap-backed BlockStore for unit-testing the
//! BlockEngine without SQLite.
//!
//! Why there is no fs backend here (unlike the metrics ChunkStore,
//! which kept FsStore): the metrics engine had a filesystem donor
//! implementation to preserve for timeless_metrics compatibility. The
//! block engine has no such heritage — its ONLY production target is
//! the extension's shadow-table store, and a second durable backend
//! would mean re-growing exactly the snapshot/manifest crash machinery
//! that moving to SQLite transactions let us delete. Tests that need a
//! store use this; everything durable goes through the extension.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use super::{BlockLoc, BlockMeta, BlockStore, EncodedBlock};

#[derive(Default)]
struct MemInner {
    next_id: i64,
    /// id → (meta, payload)
    blocks: HashMap<i64, (BlockMeta, Vec<u8>)>,
    /// term → posting list of block ids (BTreeSet: sorted, cheap
    /// intersection).
    terms: HashMap<String, BTreeSet<i64>>,
    meta_kv: HashMap<String, Vec<u8>>,
}

#[derive(Default)]
pub struct MemBlockStore {
    inner: Mutex<MemInner>,
}

impl MemBlockStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn insert_one(inner: &mut MemInner, block: &EncodedBlock) -> BlockLoc {
        inner.next_id += 1;
        let id = inner.next_id;
        inner.blocks.insert(id, (block.meta, block.data.clone()));
        for term in &block.terms {
            inner.terms.entry(term.clone()).or_default().insert(id);
        }
        BlockLoc { id }
    }

    /// Remove blocks and their posting-list entries together — the same
    /// "posting lists never dangle" contract the SQL store honors.
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
    }
}

impl BlockStore for MemBlockStore {
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        let mut inner = self.lock();
        Ok(Self::insert_one(&mut inner, block))
    }

    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        let locs: Vec<BlockLoc> = {
            let mut inner = self.lock();
            add.iter().map(|b| Self::insert_one(&mut inner, b)).collect()
            // lock released before on_committed: the callback re-locks
            // the ENGINE's index, and holding our lock across it would
            // invite ordering deadlocks in tests for no benefit.
        };
        on_committed(&locs);
        let ids: Vec<i64> = remove.iter().map(|l| l.id).collect();
        Self::remove_ids(&mut self.lock(), &ids);
        Ok(locs)
    }

    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.lock()
            .blocks
            .get(&loc.id)
            .map(|(_, data)| data.clone())
            .ok_or_else(|| format!("MemBlockStore: no block with id {}", loc.id))
    }

    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        let mut inner = self.lock();
        let mut errors = Vec::new();
        let mut ids = Vec::with_capacity(locs.len());
        for loc in locs {
            if inner.blocks.contains_key(&loc.id) {
                ids.push(loc.id);
            } else {
                errors.push(format!("MemBlockStore: no block with id {}", loc.id));
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
            .map(|(id, (meta, _))| (*meta, BlockLoc { id: *id }))
            .collect();
        out.sort_by_key(|(_, loc)| loc.id); // deterministic for tests
        Ok(out)
    }

    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<BlockLoc>, String> {
        let inner = self.lock();
        // Intersect posting lists (empty terms = no term constraint).
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
                    if let Some((meta, _)) = inner.blocks.get(&id) {
                        if meta.ts_min <= ts_max && meta.ts_max >= ts_min {
                            out.push(BlockLoc { id });
                        }
                    }
                }
            }
            None => {
                let mut all: Vec<i64> = inner
                    .blocks
                    .iter()
                    .filter(|(_, (m, _))| m.ts_min <= ts_max && m.ts_max >= ts_min)
                    .map(|(id, _)| *id)
                    .collect();
                all.sort_unstable();
                out.extend(all.into_iter().map(|id| BlockLoc { id }));
            }
        }
        Ok(out)
    }

    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.lock().meta_kv.insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.lock().meta_kv.get(key).cloned())
    }
}
