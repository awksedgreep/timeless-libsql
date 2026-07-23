use crate::store::{ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, FsStore, ENC_PCO, ENC_RAW};
use dashmap::DashMap;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Helpers that lived below the NIF boundary in the original tms_engine file.
fn partition_vec_memory(timestamps: &Vec<i64>, values: &Vec<f64>) -> usize {
    (timestamps.len() + values.len()) * 8
}

pub type EngineResult<T> = Result<T, String>;

const BATCH_CHUNK_SIZE: usize = 1000;

/// Chunks newer than this are never compacted: the recent window keeps
/// small chunks so narrow dashboard queries stay cheap.
const COMPACT_MIN_AGE_SECS: i64 = 3600;

// ═══════════════════════════════════════════════════════════════════════
// Core types
// ═══════════════════════════════════════════════════════════════════════

/// Sorted label set. BTreeMap gives deterministic ordering for hashing.
pub type Labels = BTreeMap<String, String>;

/// Partition key is just a series_id. The series registry maps
/// (metric_name, labels) → series_id.
#[derive(Hash, Eq, PartialEq, Clone, Debug, Ord, PartialOrd, Copy)]
struct PartitionKey {
    series_id: i64,
}

/// Full identity of a series for reverse lookups and label queries.
#[derive(Clone)]
pub struct SeriesInfo {
    pub metric_name: String,
    pub labels: Labels,
}

struct PartitionBuffer {
    timestamps: Vec<i64>,
    values: Vec<f64>,
    last_write: Instant,
    queued_for_flush: bool,
}

impl PartitionBuffer {
    fn new() -> Self {
        PartitionBuffer {
            timestamps: Vec::new(),
            values: Vec::new(),
            last_write: Instant::now(),
            queued_for_flush: false,
        }
    }
    fn memory_bytes(&self) -> usize {
        (self.timestamps.len() + self.values.len()) * 8
    }
}

// Payload encoding constants (ENC_PCO / ENC_RAW) and ChunkMeta moved to
// the store module — they are shared vocabulary across the seam.

// ═══════════════════════════════════════════════════════════════════════
// Series Registry — maps (metric_name, labels) → series_id
// ═══════════════════════════════════════════════════════════════════════

pub struct SeriesRegistry {
    /// Forward: (metric, labels) → series_id
    series_map: HashMap<(String, Labels), i64>,
    /// Reverse: series_id → SeriesInfo
    series_info: HashMap<i64, SeriesInfo>,
    /// Inverted label index: (label_key, label_value) → set of series_ids
    label_index: HashMap<(String, String), HashSet<i64>>,
    /// Metric name → set of series_ids
    metric_index: HashMap<String, HashSet<i64>>,
    /// Next ID
    next_id: AtomicI64,
    dirty: bool,
}

impl SeriesRegistry {
    fn new() -> Self {
        SeriesRegistry {
            series_map: HashMap::new(),
            series_info: HashMap::new(),
            label_index: HashMap::new(),
            metric_index: HashMap::new(),
            next_id: AtomicI64::new(1),
            dirty: false,
        }
    }

    /// Resolve (metric_name, labels) → series_id. Creates if new.
    fn get_or_create(&mut self, metric_name: &str, labels: &Labels) -> i64 {
        let key = (metric_name.to_string(), labels.clone());
        if let Some(&id) = self.series_map.get(&key) {
            return id;
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // Forward map
        self.series_map.insert(key, id);

        // Reverse map
        self.series_info.insert(
            id,
            SeriesInfo {
                metric_name: metric_name.to_string(),
                labels: labels.clone(),
            },
        );

        // Label index — index every label pair + __name__
        self.metric_index
            .entry(metric_name.to_string())
            .or_default()
            .insert(id);
        for (k, v) in labels {
            self.label_index
                .entry((k.clone(), v.clone()))
                .or_default()
                .insert(id);
        }

        self.dirty = true;
        id
    }

    pub fn info_for(&self, id: i64) -> Option<&SeriesInfo> {
        self.series_info.get(&id)
    }

    /// Find all series_ids matching a metric name and optional label filters.
    pub fn find_series(&self, metric_name: &str, label_filter: &Labels) -> Vec<i64> {
        let metric_ids = match self.metric_index.get(metric_name) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };

        if label_filter.is_empty() {
            return metric_ids.into_iter().collect();
        }

        let mut smallest = &metric_ids;

        for (k, v) in label_filter {
            let matching = match self.label_index.get(&(k.clone(), v.clone())) {
                Some(ids) => ids,
                None => return Vec::new(),
            };
            if matching.len() < smallest.len() {
                smallest = matching;
            }
        }

        smallest
            .iter()
            .copied()
            .filter(|id| {
                let Some(info) = self.series_info.get(id) else {
                    return false;
                };

                info.metric_name == metric_name
                    && label_filter
                        .iter()
                        .all(|(k, v)| info.labels.get(k).is_some_and(|actual| actual == v))
            })
            .collect()
    }

    pub fn list_metrics(&self) -> Vec<String> {
        let mut names: Vec<String> = self.metric_index.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn label_values(&self, metric_name: &str, label_key: &str) -> Vec<String> {
        let series_ids = match self.metric_index.get(metric_name) {
            Some(ids) => ids,
            None => return Vec::new(),
        };

        let mut values: HashSet<String> = HashSet::new();
        for &id in series_ids {
            if let Some(info) = self.series_info.get(&id) {
                if let Some(val) = info.labels.get(label_key) {
                    values.insert(val.clone());
                }
            }
        }

        let mut result: Vec<String> = values.into_iter().collect();
        result.sort();
        result
    }

    pub fn all_label_names(&self) -> Vec<String> {
        let mut names: HashSet<String> = HashSet::new();
        names.insert("__name__".to_string());
        for (k, _) in self.label_index.keys() {
            names.insert(k.clone());
        }
        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }

    fn series_count(&self) -> usize {
        self.series_map.len()
    }

    /// Serialize for persistence (the store decides where bytes land).
    /// Format: [count: u32] [id: i64, metric_len: u16, metric: bytes,
    ///   label_count: u16, [key_len: u16, key, val_len: u16, val]...]...
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let count = self.series_info.len() as u32;
        out.extend_from_slice(&count.to_be_bytes());

        let mut entries: Vec<(&i64, &SeriesInfo)> = self.series_info.iter().collect();
        entries.sort_by_key(|&(id, _)| *id);

        for (&id, info) in entries {
            out.extend_from_slice(&id.to_be_bytes());
            let mb = info.metric_name.as_bytes();
            out.extend_from_slice(&(mb.len() as u16).to_be_bytes());
            out.extend_from_slice(mb);
            out.extend_from_slice(&(info.labels.len() as u16).to_be_bytes());
            for (k, v) in &info.labels {
                let kb = k.as_bytes();
                let vb = v.as_bytes();
                out.extend_from_slice(&(kb.len() as u16).to_be_bytes());
                out.extend_from_slice(kb);
                out.extend_from_slice(&(vb.len() as u16).to_be_bytes());
                out.extend_from_slice(vb);
            }
        }

        out
    }

    fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 4 {
            return Err("series registry file too small".to_string());
        }

        let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut reg = SeriesRegistry::new();
        let mut max_id: i64 = 0;
        let mut pos = 4;

        for entry_idx in 0..count {
            if pos + 10 > data.len() {
                return Err(format!(
                    "series registry truncated at entry {} (pos {} of {})",
                    entry_idx,
                    pos,
                    data.len()
                ));
            }
            let id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let ml = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + ml > data.len() {
                return Err(format!(
                    "series registry truncated: metric name at entry {} (pos {} of {})",
                    entry_idx,
                    pos,
                    data.len()
                ));
            }
            let metric_name = String::from_utf8(data[pos..pos + ml].to_vec()).map_err(|e| {
                format!("invalid UTF-8 in metric name at entry {}: {}", entry_idx, e)
            })?;
            pos += ml;

            if pos + 2 > data.len() {
                return Err(format!(
                    "series registry truncated: label count at entry {} (pos {} of {})",
                    entry_idx,
                    pos,
                    data.len()
                ));
            }
            let lc = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let mut labels = BTreeMap::new();
            for label_idx in 0..lc {
                if pos + 2 > data.len() {
                    return Err(format!(
                        "series registry truncated: label key len at entry {} label {} (pos {} of {})",
                        entry_idx, label_idx, pos, data.len()
                    ));
                }
                let kl = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + kl > data.len() {
                    return Err(format!(
                        "series registry truncated: label key at entry {} label {} (pos {} of {})",
                        entry_idx,
                        label_idx,
                        pos,
                        data.len()
                    ));
                }
                let k = String::from_utf8(data[pos..pos + kl].to_vec()).map_err(|e| {
                    format!(
                        "invalid UTF-8 in label key at entry {} label {}: {}",
                        entry_idx, label_idx, e
                    )
                })?;
                pos += kl;
                if pos + 2 > data.len() {
                    return Err(format!(
                        "series registry truncated: label value len at entry {} label {} (pos {} of {})",
                        entry_idx, label_idx, pos, data.len()
                    ));
                }
                let vl = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + vl > data.len() {
                    return Err(format!(
                        "series registry truncated: label value at entry {} label {} (pos {} of {})",
                        entry_idx, label_idx, pos, data.len()
                    ));
                }
                let v = String::from_utf8(data[pos..pos + vl].to_vec()).map_err(|e| {
                    format!(
                        "invalid UTF-8 in label value at entry {} label {}: {}",
                        entry_idx, label_idx, e
                    )
                })?;
                pos += vl;
                labels.insert(k, v);
            }

            let key = (metric_name.clone(), labels.clone());
            reg.series_map.insert(key, id);
            reg.series_info.insert(
                id,
                SeriesInfo {
                    metric_name: metric_name.clone(),
                    labels: labels.clone(),
                },
            );
            reg.metric_index.entry(metric_name).or_default().insert(id);
            for (k, v) in &labels {
                reg.label_index
                    .entry((k.clone(), v.clone()))
                    .or_default()
                    .insert(id);
            }
            if id > max_id {
                max_id = id;
            }
        }

        reg.next_id = AtomicI64::new(max_id + 1);
        Ok(reg)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Engine
// ═══════════════════════════════════════════════════════════════════════

/// Fast hash of (metric, labels) for the resolution cache.
/// Uses std DefaultHasher which is SipHash — fast and collision-resistant.
fn fast_series_hash(metric: &str, labels: &HashMap<String, String>) -> u64 {
    let mut pairs: Vec<(&str, &str)> = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    pairs.sort_unstable_by_key(|&(k, _)| k);
    fast_series_hash_pairs(metric, &pairs)
}

/// Hash core shared by the HashMap path and the fused-ingest path.
/// Pairs MUST be sorted by key and deduplicated — both callers guarantee
/// it — so both paths produce identical hashes for the same series and
/// share the resolve cache.
fn fast_series_hash_pairs(metric: &str, sorted_pairs: &[(&str, &str)]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric.hash(&mut hasher);
    for &(k, v) in sorted_pairs {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    hasher.finish()
}

pub struct Engine {
    /// Chunk persistence backend (filesystem today, SQLite shadow
    /// tables later). All bytes-at-rest go through this seam.
    store: Box<dyn ChunkStore>,
    flush_threshold: usize,
    min_flush_size: usize,
    compression_level: usize,
    memory_budget: usize,
    /// Raw-first mode: flushes write raw (uncompressed) chunks; the
    /// periodic compactor later merges them into large pco chunks.
    defer_compression: bool,
    partitions: DashMap<PartitionKey, PartitionBuffer>,
    index: RwLock<BTreeMap<(PartitionKey, i64), ChunkMeta>>,
    series: RwLock<SeriesRegistry>,
    flush_queue: Mutex<Vec<PartitionKey>>,
    buffer_memory: AtomicUsize,
    cold_flush_running: AtomicBool,
    compaction_running: AtomicBool,
    /// Fast resolution cache: hash(metric, labels) → series_id.
    /// Persists across batches — steady-state scraping is pure cache hits.
    resolve_cache: DashMap<u64, i64>,
    /// True while a transaction journal is recording (between txn_begin
    /// and txn_commit/txn_rollback). An atomic so the hot paths can
    /// skip journal work with a single relaxed-ish load when no
    /// transaction is active (the overwhelmingly common case).
    txn_active: AtomicBool,
    /// The transaction journal itself (PLAN.md risk R5). See txn_begin
    /// for the full design story.
    txn: Mutex<TxnJournal>,
}

// ═══════════════════════════════════════════════════════════════════════
// Transaction journal (PLAN.md risk R5)
//
// THE PROBLEM: the engine buffers points in memory and persists chunks
// through the store. When the store is SQLite shadow tables, chunk ROWS
// ride the host transaction — ROLLBACK removes them — but the engine's
// in-memory state (partition buffers, chunk index, flush queue) knows
// nothing about SQL transactions. Without a journal, ROLLBACK leaves:
//   - buffered points that SQL says never happened, and worse
//   - index entries pointing at chunk rows that no longer exist
//     (dangling locs → read errors on the next query).
//
// THE FIX: while a journal is active, every mutation of engine memory
// records enough to undo itself. SQLite calls xBegin before the FIRST
// write of ANY transaction — including the implicit per-statement
// transaction wrapping a bare INSERT in autocommit mode (verified
// empirically, see metrics_vtab.rs) — so txn_begin must be CHEAP:
// O(active partitions) usize marks into reused (capacity-retaining)
// collections, zero steady-state allocations.
//
// WHAT IS JOURNALED:
//   - buffer_marks: each partition's buffered length at begin. Rollback
//     truncates back to the mark (points pushed during the txn vanish).
//   - saved: pre-txn buffered points that an intra-txn flush DRAINED
//     into chunks. Those chunk rows roll back with the host txn, so the
//     points must return to the buffer or they would be silently lost
//     (they were inserted by previously-COMMITTED statements!).
//   - added: index keys inserted during the txn (flush/compact). Their
//     rows vanish at rollback, so the entries must be removed or they
//     would dangle.
//   - removed: pre-txn index entries removed during the txn (compact /
//     prune), WITH their metas. SQLite's rollback restores the deleted
//     rows under their original rowids (page-level undo), so restoring
//     the entries verbatim — same ChunkLoc::Row ids — is exactly right.
//
// The added/removed pair follows one dedup rule to stay consistent when
// one txn both adds and removes the same entry (flush then compact):
// removing an entry that is in `added` cancels the add instead of
// journaling a removal — that chunk never existed as far as rollback is
// concerned. Restores therefore never resurrect intra-txn chunks.
//
// WHAT IS *NOT* JOURNALED (accepted + documented):
//   - Series registered during the txn stay registered in memory. They
//     are harmless empty series (their chunks rolled back); rollback
//     marks the registry dirty so the next save_series re-persists a
//     blob consistent with the in-memory state (the intra-txn blob
//     write rolled back with everything else).
//   - The resolve cache: ids stay valid because the registry keeps them.
//
// PRECONDITIONS:
//   - The store must be transactional (shadow tables riding the host
//     txn). Over FsStore, file writes/deletes cannot roll back — the
//     txn_* API must simply not be used there (the vtab is the only
//     caller and always uses ShadowTableStore).
//   - SQLite never nests xBegin (savepoints would use xSavepoint, which
//     this module does not implement), so txn_begin asserts no journal
//     is already active.
//
// LOCK ORDER (deadlock rule): txn journal → partitions/flush_queue →
// index → series. Every site that touches the journal acquires it
// FIRST, before any other engine lock.
// ═══════════════════════════════════════════════════════════════════════

#[derive(Default)]
struct TxnJournal {
    /// Partition buffer lengths at txn_begin (or 0 after an intra-txn
    /// flush drained a partition — its pre-txn points moved to `saved`).
    /// Partitions absent from the map were created during the txn:
    /// their mark is implicitly 0.
    buffer_marks: HashMap<PartitionKey, usize>,
    /// Pre-txn buffered points drained by intra-txn flushes; restored
    /// into the buffers on rollback.
    saved: Vec<(PartitionKey, Vec<i64>, Vec<f64>)>,
    /// Index keys inserted during the txn (their chunk rows roll back).
    added: HashSet<(PartitionKey, i64)>,
    /// Pre-txn index entries removed during the txn (their chunk rows
    /// are restored by the host rollback).
    removed: Vec<((PartitionKey, i64), ChunkMeta)>,
}

struct ColdFlushGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for ColdFlushGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

impl Engine {
    fn index_read(&self) -> RwLockReadGuard<'_, BTreeMap<(PartitionKey, i64), ChunkMeta>> {
        self.index.read().unwrap_or_else(|e| e.into_inner())
    }

    fn index_write(&self) -> RwLockWriteGuard<'_, BTreeMap<(PartitionKey, i64), ChunkMeta>> {
        self.index.write().unwrap_or_else(|e| e.into_inner())
    }

    pub fn series_read(&self) -> RwLockReadGuard<'_, SeriesRegistry> {
        self.series.read().unwrap_or_else(|e| e.into_inner())
    }

    fn series_write(&self) -> RwLockWriteGuard<'_, SeriesRegistry> {
        self.series.write().unwrap_or_else(|e| e.into_inner())
    }

    fn flush_queue_lock(&self) -> MutexGuard<'_, Vec<PartitionKey>> {
        self.flush_queue.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn txn_lock(&self) -> MutexGuard<'_, TxnJournal> {
        self.txn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Acquire the journal iff a transaction is active. Every mutation
    /// site calls this FIRST (lock order: txn → everything else); the
    /// atomic makes the no-txn fast path a single load.
    fn txn_guard(&self) -> Option<MutexGuard<'_, TxnJournal>> {
        if self.txn_active.load(Ordering::SeqCst) {
            Some(self.txn_lock())
        } else {
            None
        }
    }

    // ── Transaction journal API (PLAN.md R5; see TxnJournal docs) ────

    /// Start journaling. Called from the vtab's xBegin — which SQLite
    /// fires before the first write of EVERY transaction, including the
    /// implicit one wrapping each bare INSERT statement in autocommit
    /// mode, so this is on the per-statement path and must stay cheap:
    /// O(active partitions) marks into capacity-retaining collections.
    ///
    /// Nested begins are impossible from SQLite (savepoints would be
    /// xSavepoint, which is not implemented); debug builds assert it,
    /// release builds defensively restart the journal.
    pub fn txn_begin(&self) {
        let mut j = self.txn_lock();
        debug_assert!(
            !self.txn_active.load(Ordering::SeqCst),
            "txn_begin while a transaction journal is already active (nested xBegin?)"
        );
        j.buffer_marks.clear();
        j.saved.clear();
        j.added.clear();
        j.removed.clear();
        for e in self.partitions.iter() {
            j.buffer_marks.insert(*e.key(), e.value().timestamps.len());
        }
        self.txn_active.store(true, Ordering::SeqCst);
    }

    /// Commit: the host transaction made every journaled mutation
    /// permanent — drop the journal. Contents are cleared lazily by the
    /// next txn_begin; only the flag needs to flip here.
    pub fn txn_commit(&self) {
        let _j = self.txn_lock(); // serialize against in-flight recorders
        self.txn_active.store(false, Ordering::SeqCst);
    }

    /// Rollback: undo every journaled mutation, in an order that
    /// mirrors what the host rollback did to the shadow tables:
    ///   1. truncate partition buffers to their marks (points inserted
    ///      during the txn vanish, exactly like their SQL statements),
    ///   2. restore pre-txn points that intra-txn flushes drained (their
    ///      chunk rows just rolled back — the points move back home),
    ///   3. rebuild the flush queue from actual buffer sizes (intra-txn
    ///      flushes may have consumed pre-txn queue entries),
    ///   4. remove index entries added during the txn (their rows are
    ///      gone) and restore entries removed during it (their rows are
    ///      back, same rowids — SQLite rollback is page-level undo),
    ///   5. mark the series registry dirty: any intra-txn registry blob
    ///      write rolled back, so the next save_series must re-persist.
    pub fn txn_rollback(&self) {
        let mut j = self.txn_lock();
        if !self.txn_active.load(Ordering::SeqCst) {
            return; // xRollback without xBegin — nothing recorded
        }

        // 1. Truncate buffers. Partitions with no mark were created
        //    during the txn → truncate to 0 (the empty PartitionBuffer
        //    entry itself is harmless and stays).
        for mut e in self.partitions.iter_mut() {
            let mark = j.buffer_marks.get(e.key()).copied().unwrap_or(0);
            let buf = e.value_mut();
            if buf.timestamps.len() > mark {
                let before = buf.memory_bytes();
                buf.timestamps.truncate(mark);
                buf.values.truncate(mark);
                let freed = before - buf.memory_bytes();
                if freed > 0 {
                    self.buffer_memory.fetch_sub(freed, Ordering::Relaxed);
                }
            }
        }

        // 2. Restore drained pre-txn points. Order within the buffer
        //    does not matter: flush sorts before encoding and queries
        //    sort results.
        for (key, timestamps, values) in j.saved.drain(..) {
            let added = partition_vec_memory(&timestamps, &values);
            let mut entry = self
                .partitions
                .entry(key)
                .or_insert_with(PartitionBuffer::new);
            let buf = entry.value_mut();
            buf.timestamps.extend(timestamps);
            buf.values.extend(values);
            drop(entry);
            if added > 0 {
                self.buffer_memory.fetch_add(added, Ordering::Relaxed);
            }
        }

        // 3. Rebuild the flush queue from scratch. Cheaper than trying
        //    to reconcile marks with whatever intra-txn flushes did to
        //    it, and rollback is not a hot path.
        {
            let mut queue = self.flush_queue_lock();
            queue.clear();
            for mut e in self.partitions.iter_mut() {
                let key = *e.key();
                let buf = e.value_mut();
                let should_queue = buf.timestamps.len() >= self.flush_threshold;
                buf.queued_for_flush = should_queue;
                if should_queue {
                    queue.push(key);
                }
            }
        }

        // 4. Index: adds out, removals back in. The dedup rule at
        //    record time guarantees `removed` never contains an entry
        //    whose chunk row was created inside this txn.
        {
            let mut index = self.index_write();
            for key in j.added.drain() {
                index.remove(&key);
            }
            for (key, meta) in j.removed.drain(..) {
                index.insert(key, meta);
            }
        }

        // 5. Registry: force the next save_series to write the blob
        //    even if an intra-txn save cleared the dirty flag — that
        //    write rolled back with the host transaction.
        self.series_write().dirty = true;

        self.txn_active.store(false, Ordering::SeqCst);
    }

    /// Insert freshly-persisted chunk metas into the index, journaling
    /// the additions (and any silent overwrites of pre-existing keys)
    /// when a transaction is active. THE single index-insertion path
    /// for all flush routes — centralizing it here is what makes the
    /// journal complete by construction.
    fn index_insert_new(&self, items: Vec<(PartitionKey, ChunkMeta)>) {
        if items.is_empty() {
            return;
        }
        let mut j = self.txn_guard();
        let mut index = self.index_write();
        for (key, meta) in items {
            let k = (key, meta.min_ts);
            if let Some(j) = j.as_deref_mut() {
                // If this key already exists (two chunks of one series
                // sharing a min_ts — an index-shadowing edge that
                // predates the journal), journal the old meta so
                // rollback restores it rather than losing it.
                if let Some(old) = index.get(&k) {
                    if !j.added.contains(&k) {
                        j.removed.push((k, old.clone()));
                    }
                }
                j.added.insert(k);
            }
            index.insert(k, meta);
        }
    }

    /// Convenience constructor over the filesystem backend — the
    /// pre-seam signature and behavior, byte-for-byte on disk.
    pub fn new(
        data_dir: PathBuf,
        flush_threshold: usize,
        min_flush_size: usize,
        compression_level: usize,
        memory_budget: usize,
        defer_compression: bool,
    ) -> Self {
        Self::with_store(
            Box::new(FsStore::new(data_dir)),
            flush_threshold,
            min_flush_size,
            compression_level,
            memory_budget,
            defer_compression,
        )
    }

    /// Construct over an arbitrary chunk store. The store is expected to
    /// have completed any crash recovery of its own (FsStore finishes
    /// interrupted compactions in its constructor) before scan() runs.
    pub fn with_store(
        store: Box<dyn ChunkStore>,
        flush_threshold: usize,
        min_flush_size: usize,
        compression_level: usize,
        memory_budget: usize,
        defer_compression: bool,
    ) -> Self {
        let registry = match store.load_registry() {
            Ok(Some(bytes)) => SeriesRegistry::from_bytes(&bytes).unwrap_or_else(|e| {
                eprintln!("WARNING: corrupt series registry, starting fresh: {}", e);
                SeriesRegistry::new()
            }),
            Ok(None) => SeriesRegistry::new(),
            Err(e) => {
                eprintln!("WARNING: unreadable series registry, starting fresh: {}", e);
                SeriesRegistry::new()
            }
        };

        let engine = Engine {
            store,
            flush_threshold,
            min_flush_size,
            compression_level,
            memory_budget,
            defer_compression,
            partitions: DashMap::new(),
            index: RwLock::new(BTreeMap::new()),
            series: RwLock::new(registry),
            flush_queue: Mutex::new(Vec::new()),
            buffer_memory: AtomicUsize::new(0),
            cold_flush_running: AtomicBool::new(false),
            compaction_running: AtomicBool::new(false),
            resolve_cache: DashMap::new(),
            txn_active: AtomicBool::new(false),
            txn: Mutex::new(TxnJournal::default()),
        };
        engine.rebuild_index();
        engine
    }

    // ── Series resolution ────────────────────────────────────────────

    /// Resolve (metric, labels) → series_id. Fast read path, slow write path.
    fn resolve_series(&self, metric_name: &str, labels: &Labels) -> EngineResult<i64> {
        let key = (metric_name.to_string(), labels.clone());
        let mut reg = self.series_write();
        if let Some(&id) = reg.series_map.get(&key) {
            return Ok(id);
        }
        Ok(reg.get_or_create(metric_name, labels))
    }

    pub fn resolve_series_batch(&self, entries: &[(String, Labels)]) -> EngineResult<Vec<i64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(entries.len());
        let mut misses: Vec<(usize, &str, &Labels)> = Vec::new();

        {
            let reg = self.series_read();
            for (idx, (metric_name, labels)) in entries.iter().enumerate() {
                if let Some(&id) = reg.series_map.get(&(metric_name.clone(), labels.clone())) {
                    out.push(id);
                } else {
                    out.push(0);
                    misses.push((idx, metric_name.as_str(), labels));
                }
            }
        }

        if misses.is_empty() {
            return Ok(out);
        }

        let mut reg = self.series_write();
        for (idx, metric_name, labels) in misses {
            out[idx] = reg.get_or_create(metric_name, labels);
        }

        Ok(out)
    }

    fn save_series(&self) -> EngineResult<()> {
        let mut reg = self.series_write();
        if !reg.dirty {
            return Ok(());
        }
        let bytes = reg.to_bytes();
        self.store
            .save_registry(&bytes)
            .map_err(|err| format!("failed to persist series registry: {err}"))?;
        reg.dirty = false;
        Ok(())
    }

    // ── Write path ───────────────────────────────────────────────────

    #[inline]
    pub fn write_point(&self, series_id: i64, ts: i64, val: f64) {
        let key = PartitionKey { series_id };
        let should_queue_flush;
        let mem_delta: isize;

        {
            let mut entry = self
                .partitions
                .entry(key)
                .or_insert_with(PartitionBuffer::new);
            let buf = entry.value_mut();
            let old_cap = buf.memory_bytes();
            buf.timestamps.push(ts);
            buf.values.push(val);
            buf.last_write = Instant::now();
            let new_cap = buf.memory_bytes();
            mem_delta = (new_cap as isize) - (old_cap as isize);
            should_queue_flush =
                buf.timestamps.len() >= self.flush_threshold && !buf.queued_for_flush;
            if should_queue_flush {
                buf.queued_for_flush = true;
            }
        }

        if mem_delta > 0 {
            self.buffer_memory
                .fetch_add(mem_delta as usize, Ordering::Relaxed);
        } else if mem_delta < 0 {
            self.buffer_memory
                .fetch_sub((-mem_delta) as usize, Ordering::Relaxed);
        }

        if should_queue_flush {
            self.flush_queue_lock().push(key);
        }
    }

    /// Resolve series using the persistent hash cache.
    /// Fast path: DashMap hash lookup + verification (~100ns).
    /// Slow path: full registry resolve + cache insert.
    /// Verification prevents silent data corruption from hash collisions.
    #[inline]
    pub fn resolve_cached(&self, metric: &str, labels: &HashMap<String, String>) -> EngineResult<i64> {
        let hash = fast_series_hash(metric, labels);

        // Fast path: cache hit with verification
        if let Some(id) = self.resolve_cache.get(&hash) {
            let series_id = *id;
            if self.verify_series_identity(series_id, metric, labels) {
                return Ok(series_id);
            }
            // Hash collision detected — fall through to slow path
        }

        // Slow path: full resolve + cache
        let labels_bt: BTreeMap<String, String> =
            labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let id = self.resolve_series(metric, &labels_bt)?;
        self.resolve_cache.insert(hash, id);
        Ok(id)
    }

    /// Verify that a cached series_id still matches (metric, labels).
    /// Reads from series_info under read lock — single HashMap lookup.
    #[inline]
    fn verify_series_identity(
        &self,
        series_id: i64,
        metric: &str,
        labels: &HashMap<String, String>,
    ) -> bool {
        let reg = match self.series.read() {
            Ok(r) => r,
            Err(_) => return false,
        };
        match reg.info_for(series_id) {
            Some(info) => {
                if info.metric_name != metric {
                    return false;
                }
                if info.labels.len() != labels.len() {
                    return false;
                }
                for (k, v) in labels {
                    match info.labels.get(k) {
                        Some(iv) if iv == v => {}
                        _ => return false,
                    }
                }
                true
            }
            None => false,
        }
    }

    /// Write a batch of labeled entries. Resolves series internally.
    /// Uses persistent hash cache — steady-state scraping is pure cache hits.
    pub fn write_batch_labeled(
        &self,
        entries: Vec<(String, HashMap<String, String>, i64, f64)>,
    ) -> EngineResult<()> {
        for (metric, labels_hm, ts, val) in entries {
            let series_id = self.resolve_cached(&metric, &labels_hm)?;
            self.write_point(series_id, ts, val);
        }
        Ok(())
    }

    /// Binary batch: [series_id: i64, ts: i64, val: f64] = 24 bytes per entry.
    /// Use after pre-resolving series IDs.
    pub fn write_batch_raw(&self, data: &[u8]) -> EngineResult<()> {
        const ENTRY_SIZE: usize = 24;
        if data.len() % ENTRY_SIZE != 0 {
            return Err(format!(
                "raw batch length {} is not a multiple of {}",
                data.len(),
                ENTRY_SIZE
            ));
        }
        let count = data.len() / ENTRY_SIZE;
        for i in 0..count {
            let o = i * ENTRY_SIZE;
            let series_id = i64::from_ne_bytes(data[o..o + 8].try_into().unwrap());
            let ts = i64::from_ne_bytes(data[o + 8..o + 16].try_into().unwrap());
            let val = f64::from_ne_bytes(data[o + 16..o + 24].try_into().unwrap());
            self.write_point(series_id, ts, val);
        }
        Ok(())
    }

    /// Verify a cached series_id against borrowed (metric, sorted pairs).
    /// BTreeMap iterates sorted by key, so element-wise zip comparison works.
    #[inline]
    fn verify_series_identity_pairs(
        &self,
        series_id: i64,
        metric: &str,
        sorted_pairs: &[(&str, &str)],
    ) -> bool {
        let reg = self.series_read();
        match reg.info_for(series_id) {
            Some(info) => {
                info.metric_name == metric
                    && info.labels.len() == sorted_pairs.len()
                    && info
                        .labels
                        .iter()
                        .zip(sorted_pairs)
                        .all(|((ik, iv), &(k, v))| ik == k && iv == v)
            }
            None => false,
        }
    }

    /// Slow path for the fused ingest: materialize owned strings, resolve
    /// through the registry, and cache under the precomputed hash.
    fn resolve_pairs_slow(
        &self,
        hash: u64,
        metric: &str,
        sorted_pairs: &[(&str, &str)],
    ) -> EngineResult<i64> {
        let labels_bt: Labels = sorted_pairs
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let id = self.resolve_series(metric, &labels_bt)?;
        self.resolve_cache.insert(hash, id);
        Ok(id)
    }

    /// Fused ingest: Prometheus text → resolve → buffer in one pass.
    /// No BEAM terms are built per sample; on the steady-state cache-hit
    /// path no allocations happen per sample either. `default_ts` (epoch
    /// seconds) is used for samples without a timestamp; millisecond
    /// timestamps are normalized to seconds, matching the scraper.
    /// Returns (samples_written, parse_errors).
    pub fn ingest_prometheus(&self, body: &[u8], default_ts: i64) -> EngineResult<(usize, usize)> {
        let mut sorted: Vec<(&str, &str)> = Vec::with_capacity(16);
        let mut failure: EngineResult<()> = Ok(());

        let (count, errors) = parse_prom_body_visit(body, |name, labels, value, ts| {
            if failure.is_err() {
                return;
            }

            let ts = if ts == 0 {
                default_ts
            } else if ts > 1_000_000_000_000 {
                ts / 1000
            } else {
                ts
            };

            match self.resolve_entry(name, labels, &mut sorted) {
                Ok(series_id) => self.write_point(series_id, ts, value),
                Err(e) => failure = Err(e),
            }
        });

        failure?;
        Ok((count, errors))
    }

    /// Resolve one parsed sample to a series_id. Cache hits touch only
    /// borrowed data; UTF-8 validation (not conversion) keeps hashing
    /// identical to the String-based path so both share resolve_cache.
    fn resolve_entry<'a>(
        &self,
        name: &'a [u8],
        labels: &[(&'a [u8], &'a [u8])],
        sorted: &mut Vec<(&'a str, &'a str)>,
    ) -> EngineResult<i64> {
        let Some(metric) = std::str::from_utf8(name).ok() else {
            return self.resolve_lossy(name, labels);
        };

        sorted.clear();
        for &(k, v) in labels {
            match (std::str::from_utf8(k), std::str::from_utf8(v)) {
                (Ok(k), Ok(v)) => sorted.push((k, v)),
                _ => return self.resolve_lossy(name, labels),
            }
        }

        // Sort by key (stable) and keep the LAST occurrence of duplicate
        // keys, matching HashMap/BTreeMap insert semantics downstream.
        sorted.sort_by_key(|&(k, _)| k);
        let mut w = 0;
        for i in 0..sorted.len() {
            if i + 1 < sorted.len() && sorted[i + 1].0 == sorted[i].0 {
                continue;
            }
            sorted[w] = sorted[i];
            w += 1;
        }
        sorted.truncate(w);

        let hash = fast_series_hash_pairs(metric, sorted);

        if let Some(id) = self.resolve_cache.get(&hash) {
            let series_id = *id;
            if self.verify_series_identity_pairs(series_id, metric, sorted) {
                return Ok(series_id);
            }
            // Hash collision — fall through to the verified slow path
        }

        self.resolve_pairs_slow(hash, metric, sorted)
    }

    /// Rare fallback for invalid UTF-8 in names/labels: resolve through
    /// the registry with lossy conversion, bypassing the hash cache.
    fn resolve_lossy(&self, name: &[u8], labels: &[(&[u8], &[u8])]) -> EngineResult<i64> {
        let metric = String::from_utf8_lossy(name);
        let labels_bt: Labels = labels
            .iter()
            .map(|&(k, v)| {
                (
                    String::from_utf8_lossy(k).into_owned(),
                    String::from_utf8_lossy(v).into_owned(),
                )
            })
            .collect();
        self.resolve_series(&metric, &labels_bt)
    }

    // ── Flush ────────────────────────────────────────────────────────

    pub fn flush_pending(&self) -> EngineResult<usize> {
        let keys: Vec<PartitionKey> = {
            let mut queue = self.flush_queue_lock();
            std::mem::take(&mut *queue)
        };
        let mut count = 0;
        for key in keys {
            if let Some((timestamps, values)) =
                self.drain_partition_if(&key, |buf| buf.timestamps.len() >= self.min_flush_size)
            {
                let cp = self.compress_partition(&key, &timestamps, &values)?;
                let meta = self.put_single_chunk(&cp)?;
                self.index_insert_new(vec![(key, meta)]);
                count += 1;
            } else {
                self.clear_flush_queued(&key);
            }
        }
        self.save_series()?;
        Ok(count)
    }

    #[allow(dead_code)]
    fn flush_partition_individual(&self, key: &PartitionKey) -> EngineResult<()> {
        if let Some((timestamps, values)) =
            self.drain_partition_if(key, |buf| !buf.timestamps.is_empty())
        {
            let cp = self.compress_partition(key, &timestamps, &values)?;
            let meta = self.put_single_chunk(&cp)?;
            self.index_insert_new(vec![(*key, meta)]);
        }
        Ok(())
    }

    /// Backend cache maintenance (fs: drop expired file-cache entries —
    /// the read path only evicts entries it happens to touch after
    /// expiry, so a file read once and never again would stay resident
    /// forever without this periodic sweep).
    pub fn sweep_file_cache(&self) {
        self.store.sweep_cache();
    }

    pub fn flush_cold(&self, max_idle_secs: u64) -> EngineResult<(usize, usize, usize)> {
        if self
            .cold_flush_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok((0, 0, 0));
        }

        let _guard = ColdFlushGuard {
            flag: &self.cold_flush_running,
        };

        // Piggyback on the periodic cold-flush timer to bound cache memory.
        self.sweep_file_cache();

        // In raw-first mode, the same timer drives compaction of raw and
        // undersized chunks into large pco chunks. Recent chunks are
        // excluded: dashboards query recent windows, and small chunks
        // keep those narrow reads cheap (no whole-chunk decompression).
        if self.defer_compression {
            let cutoff = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
                - COMPACT_MIN_AGE_SECS;
            self.compact_partitions(cutoff)?;
        }

        let now = Instant::now();
        let cold_keys: Vec<PartitionKey> = self
            .partitions
            .iter()
            .filter(|e| now.duration_since(e.value().last_write).as_secs() >= max_idle_secs)
            .map(|e| *e.key())
            .collect();

        let mut compressed: Vec<EncodedChunk> = Vec::new();
        let mut evicted = 0;

        for key in &cold_keys {
            if let Some((timestamps, values)) = self.drain_partition_if(key, |buf| {
                now.duration_since(buf.last_write).as_secs() >= max_idle_secs
                    && !buf.timestamps.is_empty()
            }) {
                compressed.push(self.compress_partition(key, &timestamps, &values)?);
                evicted += 1;
            }
        }

        if compressed.is_empty() {
            return Ok((0, evicted, 0));
        }

        let flushed = compressed.len();
        let mut files_written = 0;
        for batch in compressed.chunks(1000) {
            let metas = self.put_chunk_batch(batch)?;
            self.index_insert_new(metas);
            files_written += 1;
        }

        self.save_series()?;
        Ok((flushed, evicted, files_written))
    }

    pub fn flush_by_memory(&self) -> EngineResult<usize> {
        let current = self.buffer_memory.load(Ordering::Relaxed);
        if current <= self.memory_budget {
            return Ok(0);
        }

        let mut sizes: Vec<(PartitionKey, usize)> = self
            .partitions
            .iter()
            .map(|e| (*e.key(), e.value().timestamps.len()))
            .collect();
        sizes.sort_by(|a, b| b.1.cmp(&a.1));

        let mut freed = 0usize;
        let overage = current - self.memory_budget;
        let mut compressed: Vec<EncodedChunk> = Vec::new();

        for (key, _) in sizes {
            if freed >= overage {
                break;
            }
            if let Some((timestamps, values)) =
                self.drain_partition_if(&key, |buf| !buf.timestamps.is_empty())
            {
                freed += partition_vec_memory(&timestamps, &values);
                compressed.push(self.compress_partition(&key, &timestamps, &values)?);
            }
        }

        let count = compressed.len();
        if !compressed.is_empty() {
            for batch in compressed.chunks(BATCH_CHUNK_SIZE) {
                let metas = self.put_chunk_batch(batch)?;
                self.index_insert_new(metas);
            }
        }
        self.save_series()?;
        Ok(count)
    }

    pub fn flush_all(&self) -> EngineResult<()> {
        let keys: Vec<(PartitionKey, usize)> = self
            .partitions
            .iter()
            .filter(|e| !e.value().timestamps.is_empty())
            .map(|e| (*e.key(), e.value().timestamps.len()))
            .collect();

        let mut small_compressed: Vec<EncodedChunk> = Vec::new();
        let mut new_individual: Vec<(PartitionKey, ChunkMeta)> = Vec::new();

        for (key, len) in keys {
            if let Some((timestamps, values)) =
                self.drain_partition_if(&key, |buf| !buf.timestamps.is_empty())
            {
                let cp = self.compress_partition(&key, &timestamps, &values)?;
                if len >= self.min_flush_size {
                    new_individual.push((key, self.put_single_chunk(&cp)?));
                } else {
                    small_compressed.push(cp);
                }
            }
        }

        let mut all_metas = new_individual;
        for batch in small_compressed.chunks(BATCH_CHUNK_SIZE) {
            all_metas.extend(self.put_chunk_batch(batch)?);
        }
        self.index_insert_new(all_metas);
        self.save_series()?;
        Ok(())
    }

    pub fn shutdown(&self) -> EngineResult<()> {
        self.flush_all()?;
        self.save_series()
    }

    // ── Compression ──────────────────────────────────────────────────

    fn compress_partition(
        &self,
        key: &PartitionKey,
        timestamps: &[i64],
        values: &[f64],
    ) -> EngineResult<EncodedChunk> {
        if self.defer_compression {
            self.encode_partition(key, timestamps, values, ENC_RAW, self.compression_level)
        } else {
            self.encode_partition(key, timestamps, values, ENC_PCO, self.compression_level)
        }
    }

    fn encode_partition(
        &self,
        key: &PartitionKey,
        timestamps: &[i64],
        values: &[f64],
        encoding: u8,
        level: usize,
    ) -> EngineResult<EncodedChunk> {
        if timestamps.is_empty() || timestamps.len() != values.len() {
            return Err(format!(
                "invalid partition payload for series {}: {} timestamps, {} values",
                key.series_id,
                timestamps.len(),
                values.len()
            ));
        }

        let needs_sort = timestamps.windows(2).any(|w| w[0] > w[1]);
        let sorted_points = if needs_sort {
            let mut points: Vec<(i64, f64)> = timestamps
                .iter()
                .copied()
                .zip(values.iter().copied())
                .collect();
            points.sort_unstable_by_key(|&(ts, _)| ts);
            Some(points.into_iter().unzip::<_, _, Vec<i64>, Vec<f64>>())
        } else {
            None
        };
        let (ts_slice, val_slice) = match &sorted_points {
            Some((ts, vals)) => (&ts[..], &vals[..]),
            None => (timestamps, values),
        };

        let (ts_compressed, val_compressed) = if encoding == ENC_RAW {
            let mut ts_raw = Vec::with_capacity(ts_slice.len() * 8);
            for ts in ts_slice {
                ts_raw.extend_from_slice(&ts.to_be_bytes());
            }
            let mut val_raw = Vec::with_capacity(val_slice.len() * 8);
            for v in val_slice {
                val_raw.extend_from_slice(&v.to_be_bytes());
            }
            (ts_raw, val_raw)
        } else {
            let config = pco::ChunkConfig::default().with_compression_level(level);
            let ts_compressed = pco::standalone::simple_compress(ts_slice, &config)
                .map_err(|err| {
                    format!(
                        "failed to compress timestamps for series {}: {err}",
                        key.series_id
                    )
                })?;
            let val_compressed = pco::standalone::simple_compress(val_slice, &config)
                .map_err(|err| {
                    format!(
                        "failed to compress values for series {}: {err}",
                        key.series_id
                    )
                })?;
            (ts_compressed, val_compressed)
        };

        let min_ts = ts_slice[0];
        let max_ts = ts_slice[ts_slice.len() - 1];
        let point_count = ts_slice.len() as u32;
        let (mut min_val, mut max_val, mut sum_val) = (val_slice[0], val_slice[0], 0.0f64);
        for &v in val_slice {
            if v < min_val {
                min_val = v;
            }
            if v > max_val {
                max_val = v;
            }
            sum_val += v;
        }

        Ok(EncodedChunk {
            series_id: key.series_id,
            min_ts,
            max_ts,
            point_count,
            min_val,
            max_val,
            sum_val,
            encoding,
            ts_bytes: ts_compressed,
            val_bytes: val_compressed,
        })
    }

    // ── Chunk persistence (through the store seam) ───────────────────

    /// Persist one chunk through the store and build its index metadata.
    fn put_single_chunk(&self, cp: &EncodedChunk) -> EngineResult<ChunkMeta> {
        let locs = self.store.put_chunks(std::slice::from_ref(cp))?;
        let loc = locs
            .into_iter()
            .next()
            .ok_or_else(|| "store returned no location for chunk".to_string())?;
        Ok(cp.meta(loc))
    }

    /// Persist a batch through the store (the backend may pack it into
    /// one file); returns (key, meta) pairs for the index, same order.
    fn put_chunk_batch(
        &self,
        batch: &[EncodedChunk],
    ) -> EngineResult<Vec<(PartitionKey, ChunkMeta)>> {
        let locs = self.store.put_chunks(batch)?;
        if locs.len() != batch.len() {
            return Err(format!(
                "store returned {} locations for {} chunks",
                locs.len(),
                batch.len()
            ));
        }
        Ok(batch
            .iter()
            .zip(locs)
            .map(|(cp, loc)| {
                (
                    PartitionKey {
                        series_id: cp.series_id,
                    },
                    cp.meta(loc),
                )
            })
            .collect())
    }

    // ── Compaction ───────────────────────────────────────────────────

    /// Merge each series' raw and undersized chunks into large pco chunks
    /// at maximum compression. Only chunks entirely older than `cutoff_ts`
    /// are eligible — the recent window stays in small/raw chunks so
    /// narrow dashboard queries never pay whole-chunk decompression.
    ///
    /// Crash safety lives in the store: `replace_chunks` persists the
    /// replacements and removes the old storage units such that a crash
    /// at any point either leaves the pre-compaction state or is
    /// completed by the store's recovery on the next start (fs backend:
    /// the pending/manifest/rename protocol). Old units are removed only
    /// when no surviving index entry references them (batch files are
    /// shared across series).
    pub fn compact_partitions(&self, cutoff_ts: i64) -> EngineResult<(usize, usize)> {
        const SMALL_CHUNK_POINTS: u32 = 16 * 1024;
        const MAX_OUTPUT_POINTS: usize = 32 * 1024;
        const COMPACTION_LEVEL: usize = 12;

        // Single-flight: the cold-flush timer and the explicit NIF may
        // both call in; one compaction at a time.
        if self
            .compaction_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok((0, 0));
        }
        let _guard = ColdFlushGuard {
            flag: &self.compaction_running,
        };

        // Group eligible chunks by series: all raw chunks, plus pco
        // chunks small enough that merging improves the ratio.
        let mut candidates: HashMap<PartitionKey, Vec<(i64, ChunkMeta)>> = HashMap::new();
        {
            let index = self.index_read();
            for ((key, min_ts), meta) in index.iter() {
                let eligible = meta.max_ts < cutoff_ts
                    && (meta.encoding == ENC_RAW || meta.point_count < SMALL_CHUNK_POINTS);
                if eligible {
                    candidates
                        .entry(*key)
                        .or_default()
                        .push((*min_ts, meta.clone()));
                }
            }
        }
        candidates.retain(|_, chunks| {
            chunks.len() >= 2 || chunks.iter().any(|(_, m)| m.encoding == ENC_RAW)
        });

        if candidates.is_empty() {
            return Ok((0, 0));
        }

        // Phase 1: re-encode every replacement chunk in memory — nothing
        // is persisted or visible to queries yet. `add` holds the chunks
        // in plan order; each plan records how many are its own.
        let mut plans: Vec<(PartitionKey, Vec<(i64, ChunkMeta)>, usize)> = Vec::new();
        let mut add: Vec<EncodedChunk> = Vec::new();

        for (key, chunks) in candidates {
            let mut points: Vec<(i64, f64)> = Vec::new();
            for (_, meta) in &chunks {
                points.extend(self.read_chunk_data(meta, i64::MIN, i64::MAX)?);
            }
            if points.is_empty() {
                continue;
            }
            points.sort_unstable_by_key(|&(ts, _)| ts);

            let mut new_count = 0;
            for slice in points.chunks(MAX_OUTPUT_POINTS) {
                let (ts, vals): (Vec<i64>, Vec<f64>) = slice.iter().copied().unzip();
                add.push(self.encode_partition(&key, &ts, &vals, ENC_PCO, COMPACTION_LEVEL)?);
                new_count += 1;
            }
            plans.push((key, chunks, new_count));
        }

        if plans.is_empty() {
            return Ok((0, 0));
        }

        // Old storage units are deletable only if no surviving
        // (non-replaced) index entry still references them.
        let removed: HashSet<(PartitionKey, i64)> = plans
            .iter()
            .flat_map(|(key, chunks, _)| chunks.iter().map(move |(ts, _)| (*key, *ts)))
            .collect();
        let deletable: Vec<ChunkLoc> = {
            let index = self.index_read();
            let survivors: HashSet<ChunkLoc> = index
                .iter()
                .filter(|(entry_key, _)| !removed.contains(entry_key))
                .map(|(_, m)| m.loc.unit())
                .collect();
            let mut seen: HashSet<ChunkLoc> = HashSet::new();
            plans
                .iter()
                .flat_map(|(_, chunks, _)| chunks.iter().map(|(_, m)| m.loc.unit()))
                .filter(|u| !survivors.contains(u) && seen.insert(u.clone()))
                .collect()
        };

        // Phase 2: the store makes the swap durable (fs: manifest +
        // renames). The commit callback swaps the index while the new
        // chunks are live but the old ones not yet removed, so queries
        // never see a deleted unit.
        //
        // Journal (R5): grabbed BEFORE replace_chunks so the lock order
        // inside the callback stays txn → index. Removals journal their
        // metas — the host rollback restores the deleted rows under
        // their original rowids, so restoring the entries verbatim is
        // correct — EXCEPT entries this same txn added (flush → compact
        // in one txn): removing those just cancels the add. Additions
        // journal their keys so rollback can drop them.
        let mut j = self.txn_guard();
        self.store.replace_chunks(&add, &deletable, &mut |locs| {
            let mut index = self.index_write();
            let mut next = 0;
            for (key, chunks, new_count) in &plans {
                for (min_ts, meta) in chunks {
                    let k = (*key, *min_ts);
                    if let Some(j) = j.as_deref_mut() {
                        if !j.added.remove(&k) {
                            j.removed.push((k, meta.clone()));
                        }
                    }
                    index.remove(&k);
                }
                for i in next..next + new_count {
                    let meta = add[i].meta(locs[i].clone());
                    let k = (*key, meta.min_ts);
                    if let Some(j) = j.as_deref_mut() {
                        if let Some(old) = index.get(&k) {
                            if !j.added.contains(&k) {
                                j.removed.push((k, old.clone()));
                            }
                        }
                        j.added.insert(k);
                    }
                    index.insert(k, meta);
                }
                next += new_count;
            }
        })?;
        drop(j);

        let series_compacted = plans.len();
        let chunks_replaced = plans.iter().map(|(_, chunks, _)| chunks.len()).sum();
        Ok((series_compacted, chunks_replaced))
    }

    fn drain_partition_if<F>(
        &self,
        key: &PartitionKey,
        should_drain: F,
    ) -> Option<(Vec<i64>, Vec<f64>)>
    where
        F: FnOnce(&PartitionBuffer) -> bool,
    {
        // Journal first (lock order: txn → partitions). Draining while
        // a transaction is active moves pre-txn points into chunks
        // whose rows would vanish on rollback — so the pre-txn prefix
        // (everything below this partition's mark) is SAVED before the
        // drain and the mark drops to 0: from here on, everything in
        // this buffer is txn-era and rollback simply truncates it.
        let mut j = self.txn_guard();
        let mut entry = self.partitions.get_mut(key)?;
        if !should_drain(&entry) {
            return None;
        }
        if let Some(j) = j.as_deref_mut() {
            let mark = j.buffer_marks.get(key).copied().unwrap_or(0);
            if mark > 0 {
                j.saved.push((
                    *key,
                    entry.timestamps[..mark].to_vec(),
                    entry.values[..mark].to_vec(),
                ));
                j.buffer_marks.insert(*key, 0);
            }
        }
        drop(j);

        let freed = entry.memory_bytes();
        let timestamps = std::mem::take(&mut entry.timestamps);
        let values = std::mem::take(&mut entry.values);
        entry.queued_for_flush = false;
        entry.last_write = Instant::now();
        drop(entry);

        if freed > 0 {
            self.buffer_memory.fetch_sub(freed, Ordering::Relaxed);
        }

        Some((timestamps, values))
    }

    fn clear_flush_queued(&self, key: &PartitionKey) {
        if let Some(mut entry) = self.partitions.get_mut(key) {
            entry.queued_for_flush = false;
        }
    }

    // ── Queries ──────────────────────────────────────────────────────

    /// Query by metric name + label filter. Returns data for all matching series.
    pub fn query_range_labeled(
        &self,
        metric_name: &str,
        label_filter: &Labels,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Vec<(Labels, Vec<(i64, f64)>)>> {
        let candidates: Vec<(i64, Labels)> = {
            let reg = self.series_read();
            reg.find_series(metric_name, label_filter)
                .into_iter()
                .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
                .collect()
        };

        candidates
            .into_par_iter()
            .map(|(sid, labels)| {
                let points = self.query_range_by_id(sid, t_start, t_end)?;
                Ok(if points.is_empty() {
                    None
                } else {
                    Some((labels, points))
                })
            })
            .filter_map(
                |result: EngineResult<Option<(Labels, Vec<(i64, f64)>)>>| match result {
                    Ok(Some(value)) => Some(Ok(value)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect()
    }

    /// Query a single series by ID. Repeated reads of a shared chunk
    /// file within one query hit the store's read cache.
    pub fn query_range_by_id(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Vec<(i64, f64)>> {
        let pk = PartitionKey { series_id };

        let matching: Vec<ChunkMeta> = {
            let index = self.index_read();
            index
                .range((pk, i64::MIN)..)
                .take_while(|((k, _), _)| k == &pk)
                .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                .map(|(_, meta)| meta.clone())
                .collect()
        };

        let mut results = Vec::new();
        for meta in &matching {
            results.extend(self.read_chunk_data(meta, t_start, t_end)?);
        }

        if let Some(buf) = self.partitions.get(&pk) {
            for i in 0..buf.timestamps.len() {
                let ts = buf.timestamps[i];
                if ts >= t_start && ts <= t_end {
                    results.push((ts, buf.values[i]));
                }
            }
        }

        results.sort_by_key(|&(ts, _)| ts);
        Ok(results)
    }

    /// Aggregate query by metric + labels. Returns per-series aggregates.
    pub fn query_aggregate_labeled(
        &self,
        metric_name: &str,
        label_filter: &Labels,
        t_start: i64,
        t_end: i64,
        agg: AggFn,
    ) -> EngineResult<Vec<(Labels, f64)>> {
        let candidates: Vec<(i64, Labels)> = {
            let reg = self.series_read();
            reg.find_series(metric_name, label_filter)
                .into_iter()
                .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
                .collect()
        };

        candidates
            .into_par_iter()
            .map(|(sid, labels)| {
                let value = self.query_aggregate_by_id(sid, t_start, t_end, agg)?;
                Ok(value.map(|val| (labels, val)))
            })
            .filter_map(|result: EngineResult<Option<(Labels, f64)>>| match result {
                Ok(Some(value)) => Some(Ok(value)),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            })
            .collect()
    }

    fn query_aggregate_by_id(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
        agg: AggFn,
    ) -> EngineResult<Option<f64>> {
        let pk = PartitionKey { series_id };

        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut global_min: Option<f64> = None;
        let mut global_max: Option<f64> = None;

        let chunks: Vec<ChunkMeta> = {
            let index = self.index_read();
            index
                .range((pk, i64::MIN)..)
                .take_while(|((k, _), _)| k == &pk)
                .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                .map(|(_, meta)| meta.clone())
                .collect()
        };

        for meta in &chunks {
            if meta.min_ts >= t_start && meta.max_ts <= t_end {
                total_count += meta.point_count as u64;
                total_sum += meta.sum_val;
                global_min = Some(match global_min {
                    Some(m) => m.min(meta.min_val),
                    None => meta.min_val,
                });
                global_max = Some(match global_max {
                    Some(m) => m.max(meta.max_val),
                    None => meta.max_val,
                });
            } else {
                let points = self.read_chunk_data(meta, t_start, t_end)?;
                for &(_, val) in &points {
                    total_count += 1;
                    total_sum += val;
                    global_min = Some(match global_min {
                        Some(m) => m.min(val),
                        None => val,
                    });
                    global_max = Some(match global_max {
                        Some(m) => m.max(val),
                        None => val,
                    });
                }
            }
        }

        if let Some(buf) = self.partitions.get(&pk) {
            for i in 0..buf.timestamps.len() {
                if buf.timestamps[i] >= t_start && buf.timestamps[i] <= t_end {
                    let val = buf.values[i];
                    total_count += 1;
                    total_sum += val;
                    global_min = Some(match global_min {
                        Some(m) => m.min(val),
                        None => val,
                    });
                    global_max = Some(match global_max {
                        Some(m) => m.max(val),
                        None => val,
                    });
                }
            }
        }

        if total_count == 0 {
            return Ok(None);
        }
        Ok(Some(match agg {
            AggFn::Avg => total_sum / total_count as f64,
            AggFn::Sum => total_sum,
            AggFn::Min => global_min.unwrap(),
            AggFn::Max => global_max.unwrap(),
            AggFn::Count => total_count as f64,
        }))
    }

    // ── Chunk reading ────────────────────────────────────────────────

    /// Read one chunk through the store and decode the points in
    /// [t_start, t_end]. The store handles file formats and caching;
    /// the engine handles payload decoding (pco vs raw).
    fn read_chunk_data(
        &self,
        meta: &ChunkMeta,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, f64)>, String> {
        let bytes = self.store.read_chunk(&meta.loc)?;
        let (ts_data, val_data) = (bytes.ts(), bytes.val());

        let (timestamps, values): (Vec<i64>, Vec<f64>) = if meta.encoding == ENC_RAW {
            if ts_data.len() % 8 != 0 || val_data.len() % 8 != 0 {
                return Err(format!("raw payload misaligned in {:?}", meta.loc));
            }
            (
                ts_data
                    .chunks_exact(8)
                    .map(|b| i64::from_be_bytes(b.try_into().unwrap()))
                    .collect(),
                val_data
                    .chunks_exact(8)
                    .map(|b| f64::from_be_bytes(b.try_into().unwrap()))
                    .collect(),
            )
        } else {
            (
                pco::standalone::simple_decompress(ts_data).map_err(|e| e.to_string())?,
                pco::standalone::simple_decompress(val_data).map_err(|e| e.to_string())?,
            )
        };
        if timestamps.len() != values.len() {
            return Err(format!(
                "timestamp/value length mismatch in {:?}: {} vs {}",
                meta.loc,
                timestamps.len(),
                values.len()
            ));
        }

        let mut results = Vec::new();
        for i in 0..timestamps.len() {
            if timestamps[i] >= t_start && timestamps[i] <= t_end {
                results.push((timestamps[i], values[i]));
            }
        }
        Ok(results)
    }

    // ── Retention ────────────────────────────────────────────────────

    pub fn delete_before(&self, before_ts: i64) -> (usize, usize, Vec<String>) {
        // Journal first (lock order: txn → index). Pruned rows are
        // DELETEd through the store inside the host transaction, so a
        // rollback restores them — the journal restores the matching
        // index entries. Entries added by this same txn cancel instead
        // (their rows will not come back).
        let mut j = self.txn_guard();
        let mut index = self.index_write();

        let to_remove: Vec<(PartitionKey, i64)> = index
            .iter()
            .filter(|(_, meta)| meta.max_ts < before_ts)
            .map(|(k, _)| k.clone())
            .collect();

        let entries_removed = to_remove.len();
        // Refcount storage units (a batch file is one unit shared by
        // many chunks) — a unit is deletable once nothing references it.
        let mut unit_refcount: HashMap<ChunkLoc, usize> = HashMap::new();
        for meta in index.values() {
            *unit_refcount.entry(meta.loc.unit()).or_insert(0) += 1;
        }

        let mut units_to_delete: HashSet<ChunkLoc> = HashSet::new();
        for key in &to_remove {
            if let Some(meta) = index.remove(key) {
                let unit = meta.loc.unit();
                if let Some(count) = unit_refcount.get_mut(&unit) {
                    *count -= 1;
                    if *count == 0 {
                        units_to_delete.insert(unit);
                    }
                }
                if let Some(j) = j.as_deref_mut() {
                    if !j.added.remove(key) {
                        j.removed.push((*key, meta));
                    }
                }
            }
        }

        drop(index);
        drop(j);
        let files_deleted = units_to_delete.len();
        let units: Vec<ChunkLoc> = units_to_delete.into_iter().collect();
        let errors = self.store.delete_chunks(&units);

        (entries_removed, files_deleted, errors)
    }

    // ── Index rebuild ────────────────────────────────────────────────

    fn rebuild_index(&self) {
        let stored = match self.store.scan() {
            Ok(chunks) => chunks,
            Err(e) => {
                eprintln!("WARNING: chunk scan failed during recovery: {}", e);
                return;
            }
        };
        let mut index = self.index_write();
        for chunk in stored {
            let key = PartitionKey {
                series_id: chunk.series_id,
            };
            index.insert((key, chunk.meta.min_ts), chunk.meta);
        }
    }


    pub fn info(&self) -> EngineInfo {
        let index = self.index_read();
        let series_reg = self.series_read();
        let chunk_count = index.len();
        let partition_count = self.partitions.len();
        let series_count = series_reg.series_count();
        let buffered_points: usize = self
            .partitions
            .iter()
            .map(|e| e.value().timestamps.len())
            .sum();
        let buffer_memory = self.buffer_memory.load(Ordering::Relaxed);

        let mut total_disk_points: u64 = 0;
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;
        for meta in index.values() {
            total_disk_points += meta.point_count as u64;
            oldest_ts = match oldest_ts {
                Some(existing) => Some(existing.min(meta.min_ts)),
                None => Some(meta.min_ts),
            };
            newest_ts = match newest_ts {
                Some(existing) => Some(existing.max(meta.max_ts)),
                None => Some(meta.max_ts),
            };
        }

        for entry in self.partitions.iter() {
            let buf = entry.value();
            if let Some(min_ts) = buf.timestamps.iter().min() {
                oldest_ts = match oldest_ts {
                    Some(existing) => Some(existing.min(*min_ts)),
                    None => Some(*min_ts),
                };
            }
            if let Some(max_ts) = buf.timestamps.iter().max() {
                newest_ts = match newest_ts {
                    Some(existing) => Some(existing.max(*max_ts)),
                    None => Some(*max_ts),
                };
            }
        }

        let (total_bytes, file_count) = self.store.storage_stats();
        let total_points = total_disk_points + buffered_points as u64;
        let bytes_per_point = if total_disk_points > 0 {
            total_bytes as f64 / total_disk_points as f64
        } else {
            0.0
        };

        EngineInfo {
            chunk_count,
            partition_count,
            series_count,
            disk_points: total_disk_points,
            buffered_points,
            total_points,
            total_bytes,
            bytes_per_point,
            buffer_memory,
            file_count,
            oldest_ts,
            newest_ts,
        }
    }
}

pub struct EngineInfo {
    pub chunk_count: usize,
    pub partition_count: usize,
    pub series_count: usize,
    pub disk_points: u64,
    pub buffered_points: usize,
    pub total_points: u64,
    pub total_bytes: u64,
    pub bytes_per_point: f64,
    pub buffer_memory: usize,
    pub file_count: usize,
    pub oldest_ts: Option<i64>,
    pub newest_ts: Option<i64>,
}

#[derive(Clone, Copy)]
pub enum AggFn {
    Avg,
    Sum,
    Min,
    Max,
    Count,
}

// ═══════════════════════════════════════════════════════════════════════
// Prometheus text-format parser (bench prototype)
//
// Mirrors c_src/prometheus_nif.cpp semantics: entries are
// (name, [(label_key, label_value)], value, timestamp), timestamp 0 when
// absent, NaN/Inf values rejected, malformed non-comment lines counted
// as errors. Exposed as two NIFs so parse cost and term-materialization
// cost can be measured separately.
// ═══════════════════════════════════════════════════════════════════════

/// Parse a Prometheus sample value. Rejects NaN/Inf — the BEAM cannot
/// represent non-finite floats.
fn parse_prom_value(bytes: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(bytes).ok()?;
    let v: f64 = s.parse().ok()?;
    v.is_finite().then_some(v)
}

/// Parse the inside of a `{key="val",key2="val2"}` label block into `out`.
/// Escaped characters in values are kept raw, as the C++ parser does.
fn parse_prom_labels_into<'a>(mut s: &'a [u8], out: &mut Vec<(&'a [u8], &'a [u8])>) {
    loop {
        while let Some((&b, rest)) = s.split_first() {
            if b == b' ' || b == b',' {
                s = rest;
            } else {
                break;
            }
        }
        if s.is_empty() {
            break;
        }

        let Some(eq) = s.iter().position(|&b| b == b'=') else {
            break;
        };
        let mut key = &s[..eq];
        while let [rest @ .., b' '] = key {
            key = rest;
        }
        s = &s[eq + 1..];

        let Some((&b'"', rest)) = s.split_first() else {
            break;
        };
        s = rest;

        let mut i = 0;
        while i < s.len() && s[i] != b'"' {
            if s[i] == b'\\' && i + 1 < s.len() {
                i += 2;
            } else {
                i += 1;
            }
        }
        out.push((key, &s[..i]));
        s = if i < s.len() { &s[i + 1..] } else { &s[i..] };
    }
}

/// Parse one exposition line. Labels land in the caller's scratch buffer;
/// returns (name, value, timestamp) on success. Returns None for comments,
/// blanks, and malformed lines — the caller decides which count as errors.
fn parse_prom_line_into<'a>(
    line: &'a [u8],
    labels: &mut Vec<(&'a [u8], &'a [u8])>,
) -> Option<(&'a [u8], f64, i64)> {
    let line = line.trim_ascii();
    if line.is_empty() || line[0] == b'#' {
        return None;
    }

    let name_end = line
        .iter()
        .position(|&b| b == b'{' || b == b' ' || b == b'\t')?;
    if name_end == 0 {
        return None;
    }
    let name = &line[..name_end];

    let rest = if line[name_end] == b'{' {
        let close = name_end
            + 1
            + line[name_end + 1..].iter().position(|&b| b == b'}')?;
        parse_prom_labels_into(&line[name_end + 1..close], labels);
        &line[close + 1..]
    } else {
        &line[name_end..]
    };

    let mut fields = rest
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|f| !f.is_empty());
    let value = parse_prom_value(fields.next()?)?;
    let timestamp = fields
        .next()
        .and_then(|f| std::str::from_utf8(f).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    Some((name, value, timestamp))
}

/// Streaming parse: invokes `sink` once per valid sample with borrowed
/// views into `data`. One scratch label buffer is reused across all lines,
/// so steady-state parsing performs zero heap allocations. Returns
/// (entry_count, error_count).
fn parse_prom_body_visit<'a, F>(data: &'a [u8], mut sink: F) -> (usize, usize)
where
    F: FnMut(&'a [u8], &[(&'a [u8], &'a [u8])], f64, i64),
{
    let mut labels: Vec<(&[u8], &[u8])> = Vec::with_capacity(16);
    let mut count = 0;
    let mut errors = 0;

    for line in data.split(|&b| b == b'\n') {
        labels.clear();
        match parse_prom_line_into(line, &mut labels) {
            Some((name, value, timestamp)) => {
                count += 1;
                sink(name, &labels, value, timestamp);
            }
            None => {
                let t = line.trim_ascii();
                if !t.is_empty() && t[0] != b'#' {
                    errors += 1;
                }
            }
        }
    }
    (count, errors)
}
