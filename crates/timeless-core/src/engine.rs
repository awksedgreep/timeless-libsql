use crate::store::{
    ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, FsStore, StoredChunk, StoredSeries,
    ENC_PCO, ENC_RAW,
};

use crate::rollup::{decode_rollup_payload, encode_rollup_payload, ENC_ROLLUP_V1};
use crate::rollup::{rollup_buckets, RollupBucket, RollupTier};
use crate::store::{EncodedRollupChunk, StoredRollupChunk};
use dashmap::DashMap;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Helpers that lived below the NIF boundary in the original tms_engine file.
fn partition_vec_memory(timestamps: &[i64], values: &[f64]) -> usize {
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

/// Ordered newest-point results keyed by durable series ID.
pub type LatestSeriesBatch = Vec<(i64, Option<(i64, f64)>)>;

/// Ordered scalar-summary results keyed by durable series ID.
pub type AggregateSummaryBatch = Vec<(i64, Option<AggregateSummary>)>;

/// Partition key is just a series_id. The series registry maps
/// (metric_name, labels) → series_id.
#[derive(Hash, Eq, PartialEq, Clone, Debug, Ord, PartialOrd, Copy)]
struct PartitionKey {
    series_id: i64,
}

/// Chunk index key. The trailing sequence number is a per-engine
/// monotonic id (not persisted): two chunks for the same series may
/// legitimately share a min_ts (backfill, duplicate timestamps across
/// flush boundaries, compaction output), and a two-field key would let
/// the second insert silently shadow the first.
/// (Ported from the donor fix in timeless_metrics — see
/// the chunk-index shadowing fix (2026-07-22, see git history).)
type ChunkKey = (PartitionKey, i64, u64);

/// F3: rollup index key — (partition, resolution, first bucket ts, seq).
type RollupKey = (PartitionKey, i64, i64, u64);

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

    fn insert_known(
        &mut self,
        id: i64,
        metric_name: &str,
        labels: &Labels,
        dirty: bool,
    ) -> Result<(), String> {
        if id <= 0 {
            return Err(format!("series id must be positive, got {id}"));
        }
        let key = (metric_name.to_string(), labels.clone());
        if let Some(existing_id) = self.series_map.get(&key) {
            return if *existing_id == id {
                Ok(())
            } else {
                Err(format!(
                    "series identity {metric_name:?} maps to both {existing_id} and {id}"
                ))
            };
        }
        if let Some(existing) = self.series_info.get(&id) {
            return Err(format!(
                "series id {id} maps to both {:?} and {metric_name:?}",
                existing.metric_name
            ));
        }

        self.series_map.insert(key, id);
        self.series_info.insert(
            id,
            SeriesInfo {
                metric_name: metric_name.to_string(),
                labels: labels.clone(),
            },
        );
        self.metric_index
            .entry(metric_name.to_string())
            .or_default()
            .insert(id);
        for (key, value) in labels {
            self.label_index
                .entry((key.clone(), value.clone()))
                .or_default()
                .insert(id);
        }
        let next_after_id = id
            .checked_add(1)
            .ok_or_else(|| format!("series id {id} cannot be incremented"))?;
        let next_id = self.next_id.load(Ordering::Relaxed).max(next_after_id);
        self.next_id.store(next_id, Ordering::Relaxed);
        self.dirty |= dirty;
        Ok(())
    }

    fn remove_id(&mut self, id: i64) {
        let Some(info) = self.series_info.remove(&id) else {
            return;
        };
        self.series_map
            .remove(&(info.metric_name.clone(), info.labels.clone()));
        if let Some(ids) = self.metric_index.get_mut(&info.metric_name) {
            ids.remove(&id);
            if ids.is_empty() {
                self.metric_index.remove(&info.metric_name);
            }
        }
        for (key, value) in info.labels {
            let index_key = (key, value);
            if let Some(ids) = self.label_index.get_mut(&index_key) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.label_index.remove(&index_key);
                }
            }
        }
    }

    fn from_stored(rows: &[StoredSeries]) -> Result<Self, String> {
        let mut registry = Self::new();
        for row in rows {
            let mut labels = Labels::new();
            let mut previous_key: Option<&str> = None;
            for (key, value) in &row.labels {
                if previous_key.is_some_and(|previous| previous >= key.as_str()) {
                    return Err(format!(
                        "series {} labels are not in strict canonical order",
                        row.id
                    ));
                }
                if labels.insert(key.clone(), value.clone()).is_some() {
                    return Err(format!("series {} has duplicate label key {key:?}", row.id));
                }
                previous_key = Some(key);
            }
            registry.insert_known(row.id, &row.name, &labels, false)?;
        }
        Ok(registry)
    }

    fn stored_rows(&self) -> Vec<StoredSeries> {
        let mut rows: Vec<StoredSeries> = self
            .series_info
            .iter()
            .map(|(&id, info)| StoredSeries {
                id,
                name: info.metric_name.clone(),
                labels: info
                    .labels
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            })
            .collect();
        rows.sort_by_key(|row| row.id);
        rows
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
                if labels.insert(k.clone(), v).is_some() {
                    return Err(format!(
                        "series registry entry {} has duplicate label key {k:?}",
                        entry_idx
                    ));
                }
            }

            reg.insert_known(id, &metric_name, &labels, false)?;
            if id > max_id {
                max_id = id;
            }
        }

        if pos != data.len() {
            return Err(format!(
                "series registry has {} trailing bytes",
                data.len() - pos
            ));
        }
        reg.next_id = AtomicI64::new(
            max_id
                .checked_add(1)
                .ok_or_else(|| "series registry maximum id cannot be incremented".to_string())?,
        );
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
    /// Pins one complete query-visible generation. Queries hold a shared
    /// guard from candidate lookup through payload and buffer reads;
    /// maintenance holds the exclusive guard while moving data between
    /// buffers, indexes, and the store.
    transition: RwLock<()>,
    authoritative_series: bool,
    flush_threshold: usize,
    min_flush_size: usize,
    compression_level: usize,
    memory_budget: usize,
    /// Raw-first mode: flushes write raw (uncompressed) chunks; the
    /// periodic compactor later merges them into large pco chunks.
    defer_compression: bool,
    partitions: DashMap<PartitionKey, PartitionBuffer>,
    index: RwLock<BTreeMap<ChunkKey, ChunkMeta>>,
    /// Source of the ChunkKey sequence field. In-memory only — restart
    /// recovery re-assigns fresh values while scanning.
    chunk_seq: AtomicU64,
    series: RwLock<SeriesRegistry>,
    flush_queue: Mutex<Vec<PartitionKey>>,
    buffer_memory: AtomicUsize,
    cold_flush_running: AtomicBool,
    compaction_running: AtomicBool,
    /// Fast resolution cache: hash(metric, labels) → series_id.
    /// Persists across batches — steady-state scraping is pure cache hits.
    resolve_cache: DashMap<u64, i64>,
    /// Fused Prometheus ingest telemetry. These cumulative counters are
    /// exposed through timeless_stats so embedding hosts can observe partial
    /// parse success without parsing the exposition body a second time.
    prometheus_ingest_batches: AtomicU64,
    prometheus_ingest_points: AtomicU64,
    prometheus_ingest_errors: AtomicU64,
    prometheus_ingest_total_ns: AtomicU64,
    /// Cumulative profile for the public packed/raw batch read waist. These
    /// counters make pruning and decode cost observable to every SQLite host,
    /// not only to the signal server wrapping the returned frame.
    raw_batch_query_count: AtomicU64,
    raw_batch_query_total_ns: AtomicU64,
    raw_batch_query_series_considered: AtomicU64,
    raw_batch_query_candidate_chunks: AtomicU64,
    raw_batch_query_payload_bytes_read: AtomicU64,
    raw_batch_query_decoded_points: AtomicU64,
    raw_batch_query_buffered_points_considered: AtomicU64,
    raw_batch_query_returned_points: AtomicU64,
    /// Cumulative profile for the public packed window batch waist. Kept
    /// separate from raw reads so direct SQL users can attribute reduction
    /// pruning, decode, and returned-grid work precisely.
    window_batch_query_count: AtomicU64,
    window_batch_query_total_ns: AtomicU64,
    window_batch_query_series_considered: AtomicU64,
    window_batch_query_candidate_chunks: AtomicU64,
    window_batch_query_payload_bytes_read: AtomicU64,
    window_batch_query_decoded_points: AtomicU64,
    window_batch_query_buffered_points_considered: AtomicU64,
    window_batch_query_returned_points: AtomicU64,
    /// True while a transaction journal is recording (between txn_begin
    /// and txn_commit/txn_rollback). An atomic so the hot paths can
    /// skip journal work with a single relaxed-ish load when no
    /// transaction is active (the overwhelmingly common case).
    txn_active: AtomicBool,
    /// The transaction journal itself (PLAN.md risk R5). See txn_begin
    /// for the full design story.
    txn: Mutex<TxnJournal>,
    /// Store catalog generation observed by the last completed
    /// refresh_authoritative_state reload (None = never refreshed, or the
    /// store cannot report one). Lets refresh skip its full catalog +
    /// chunk-index reload when nothing has been committed since — the
    /// per-query cost drops from O(series + chunks) SQL to one cached
    /// single-row SELECT. Own-process mutations do NOT update this, so
    /// SQL hosts may publish the exact transaction generation through
    /// txn_commit_published(), avoiding a redundant reload after local
    /// flush/compact/prune/new-series work. Other hosts leave it stale and
    /// retain the always-correct reload fallback.
    catalog_gen: Mutex<Option<(i64, i64)>>,
    /// P2: the append watermark `(chunk_shape_gen, max chunk rowid)`
    /// the last reload/delta observed. While the shape half is
    /// unchanged, a generation mismatch is pure appends and refresh
    /// applies only the rows past the rowid half instead of reloading
    /// O(total series + chunks).
    append_wm: Mutex<Option<(i64, i64)>>,
    /// F3 rollup index: (partition, resolution, min_ts, seq) → meta for
    /// every persisted rollup chunk. Separate from the raw index ON
    /// PURPOSE — every pre-F3 read path stays byte-identical. meta
    /// semantics per StoredRollupChunk (max_ts = coverage END).
    rollup_index: RwLock<BTreeMap<RollupKey, ChunkMeta>>,
    /// The declared ladder (ascending resolutions). Empty = no rollups.
    rollup_tiers: Mutex<Vec<RollupTier>>,
    /// F2 retention window in NATIVE ts units; 0 = disabled. Set from
    /// the persisted table argument after construction (idempotent —
    /// every connection loads the same _meta value).
    retention_native: AtomicI64,
    /// Advance guard: the last retention cutoff actually applied, so
    /// per-maintenance application skips until the cutoff has moved by
    /// a meaningful slice (retention/16). i64::MIN = never applied.
    retention_floor: AtomicI64,
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
// SERIES CATALOG:
//   - Filesystem stores retain the legacy registry blob behavior.
//   - Authoritative stores record IDs inserted by each frame. Rollback
//     removes those identities from memory and clears the resolve cache,
//     matching the host rollback of their `_series` rows.
//
// PRECONDITIONS:
//   - The store must be transactional (shadow tables riding the host
//     txn). Over FsStore, file writes/deletes cannot roll back — the
//     txn_* API must simply not be used there (the vtab is the only
//     caller and always uses ShadowTableStore).
//   - SQLite never nests xBegin. Savepoints create additional undo
//     frames inside the one outer transaction journal.
//
// LOCK ORDER (deadlock rule): transition → txn journal →
// partitions/flush_queue → store callbacks → index → series. Queries
// take only a shared transition guard; their candidate index guard is
// released before store reads, and store reads finish before buffer access.
// Store callbacks re-enter only the caller's SQLite connection and
// never call back into this engine, so holding the transition guard
// across them cannot form an engine/connection lock cycle.
// ═══════════════════════════════════════════════════════════════════════

#[derive(Default)]
struct TxnFrame {
    savepoint: Option<i32>,
    /// Partition buffer lengths at txn_begin (or 0 after an intra-txn
    /// flush drained a partition — its pre-txn points moved to `saved`).
    /// Partitions absent from the map were created during the txn:
    /// their mark is implicitly 0.
    buffer_marks: HashMap<PartitionKey, usize>,
    /// Pre-txn buffered points drained by intra-txn flushes; restored
    /// into the buffers on rollback.
    saved: Vec<(PartitionKey, Vec<i64>, Vec<f64>)>,
    /// Index keys inserted during the txn (their chunk rows roll back).
    added: HashSet<ChunkKey>,
    /// Pre-txn index entries removed during the txn (their chunk rows
    /// are restored by the host rollback). Keys carry their original
    /// chunk_seq, so rollback reinstates entries verbatim.
    removed: Vec<(ChunkKey, ChunkMeta)>,
    /// Authoritative series rows first inserted inside this frame.
    series_added: HashSet<i64>,
    /// F3: rollup index entries added / removed inside this frame (same
    /// cancel rule as added/removed on the raw index).
    rollup_added: HashSet<RollupKey>,
    rollup_removed: Vec<(RollupKey, ChunkMeta)>,
}

#[derive(Default)]
struct TxnJournal {
    /// Frame zero is the outer transaction. Later frames correspond to
    /// SQLite savepoint numbers and receive mutations exclusively until
    /// release or rollback-to.
    frames: Vec<TxnFrame>,
    /// Capacity-retaining frames recycled across autocommit statements
    /// and repeated savepoint use.
    spares: Vec<TxnFrame>,
}

impl Deref for TxnJournal {
    type Target = TxnFrame;

    fn deref(&self) -> &Self::Target {
        self.frames
            .last()
            .expect("active journal has an undo frame")
    }
}

impl DerefMut for TxnJournal {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.frames
            .last_mut()
            .expect("active journal has an undo frame")
    }
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
    fn transition_read(&self) -> RwLockReadGuard<'_, ()> {
        self.transition.read().unwrap_or_else(|e| e.into_inner())
    }

    fn transition_write(&self) -> RwLockWriteGuard<'_, ()> {
        self.transition.write().unwrap_or_else(|e| e.into_inner())
    }

    fn index_read(&self) -> RwLockReadGuard<'_, BTreeMap<ChunkKey, ChunkMeta>> {
        self.index.read().unwrap_or_else(|e| e.into_inner())
    }

    fn index_write(&self) -> RwLockWriteGuard<'_, BTreeMap<ChunkKey, ChunkMeta>> {
        self.index.write().unwrap_or_else(|e| e.into_inner())
    }

    fn rollup_read(&self) -> RwLockReadGuard<'_, BTreeMap<RollupKey, ChunkMeta>> {
        self.rollup_index.read().unwrap_or_else(|e| e.into_inner())
    }

    fn rollup_write(&self) -> RwLockWriteGuard<'_, BTreeMap<RollupKey, ChunkMeta>> {
        self.rollup_index.write().unwrap_or_else(|e| e.into_inner())
    }

    fn next_chunk_seq(&self) -> u64 {
        self.chunk_seq.fetch_add(1, Ordering::Relaxed)
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

    fn catalog_gen_lock(&self) -> MutexGuard<'_, Option<(i64, i64)>> {
        self.catalog_gen.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn append_wm_lock(&self) -> MutexGuard<'_, Option<(i64, i64)>> {
        self.append_wm.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Acquire the journal iff a transaction is active. Every mutation
    /// site calls this before buffer/index/series locks, after taking
    /// any required transition guard; the atomic makes the no-txn fast
    /// path a single load.
    fn txn_guard(&self) -> Option<MutexGuard<'_, TxnJournal>> {
        if !self.txn_active.load(Ordering::SeqCst) {
            return None;
        }
        let journal = self.txn_lock();
        self.txn_active.load(Ordering::SeqCst).then_some(journal)
    }

    // ── Transaction journal API (PLAN.md R5; see TxnJournal docs) ────

    /// Start journaling. Called from the vtab's xBegin — which SQLite
    /// fires before the first write of EVERY transaction, including the
    /// implicit one wrapping each bare INSERT statement in autocommit
    /// mode, so this is on the per-statement path and must stay cheap:
    /// O(active partitions) marks into capacity-retaining collections.
    ///
    /// Nested begins are impossible from SQLite; savepoints add frames
    /// through txn_savepoint instead.
    pub fn txn_begin(&self) {
        let mut j = self.txn_lock();
        debug_assert!(
            !self.txn_active.load(Ordering::SeqCst),
            "txn_begin while a transaction journal is already active (nested xBegin?)"
        );
        while let Some(frame) = j.frames.pop() {
            j.spares.push(frame);
        }
        let frame = j.spares.pop().unwrap_or_default();
        j.frames.push(self.reset_txn_frame(frame, None));
        self.txn_active.store(true, Ordering::SeqCst);
    }

    /// Commit: the host transaction made every journaled mutation
    /// permanent — drop the journal. Contents are cleared lazily by the
    /// next txn_begin; only the flag needs to flip here.
    pub fn txn_commit(&self) {
        self.txn_commit_published(None);
    }

    /// Commit and, when supplied by an authoritative transactional host,
    /// publish the exact store generation captured during that transaction's
    /// xSync phase. Engine memory already contains every committed mutation,
    /// so this token lets the next reader prove that a full catalog/index
    /// reload would be redundant.
    ///
    /// `generation` must have been read from the same transaction after its
    /// final mutation and while it still excluded other writers. Passing
    /// `None` preserves the conservative stale-token behavior. The token is
    /// installed before txn_active becomes false, so a racing refresher sees
    /// either the active journal or the published generation, never an
    /// inactive transaction paired with the old token.
    pub fn txn_commit_published(&self, generation: Option<(i64, i64)>) {
        let mut j = self.txn_lock(); // serialize against in-flight recorders
        while let Some(frame) = j.frames.pop() {
            j.spares.push(frame);
        }
        if let Some(generation) = generation {
            *self.catalog_gen_lock() = Some(generation);
        }
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
    ///   5. remove authoritative series inserted during the transaction,
    ///      or mark a legacy blob registry dirty for re-persistence.
    pub fn txn_rollback(&self) {
        let _transition = self.transition_write();
        let mut j = self.txn_lock();
        if !self.txn_active.load(Ordering::SeqCst) {
            return; // xRollback without xBegin — nothing recorded
        }
        while let Some(mut frame) = j.frames.pop() {
            self.rollback_txn_frame(&mut frame);
            j.spares.push(frame);
        }
        self.rebuild_flush_queue();
        if !self.authoritative_series {
            self.series_write().dirty = true;
        }
        self.txn_active.store(false, Ordering::SeqCst);
    }

    /// Start an undo frame for SQLite savepoint `id`.
    pub fn txn_savepoint(&self, id: i32) {
        let mut j = self.txn_lock();
        if !self.txn_active.load(Ordering::SeqCst) {
            return;
        }
        debug_assert!(
            j.frames.iter().all(|frame| frame.savepoint != Some(id)),
            "duplicate savepoint id {id}"
        );
        let frame = j.spares.pop().unwrap_or_default();
        j.frames.push(self.reset_txn_frame(frame, Some(id)));
    }

    /// Release `id` and every nested frame, merging their undo records
    /// into the parent so an outer rollback still restores its own start.
    pub fn txn_release(&self, id: i32) {
        let mut j = self.txn_lock();
        let Some(pos) = j
            .frames
            .iter()
            .position(|frame| frame.savepoint == Some(id))
        else {
            return;
        };
        if pos == 0 {
            return;
        }
        let released = j.frames.split_off(pos);
        for mut child in released {
            {
                let parent = j
                    .frames
                    .last_mut()
                    .expect("savepoint frame has an outer parent");
                Self::merge_txn_frame(parent, &mut child);
            }
            j.spares.push(child);
        }
    }

    /// Restore the state captured by `id`, discard nested frames, and
    /// leave `id` active so SQLite may roll back to it again.
    pub fn txn_rollback_to(&self, id: i32) {
        let _transition = self.transition_write();
        let mut j = self.txn_lock();
        let Some(pos) = j
            .frames
            .iter()
            .position(|frame| frame.savepoint == Some(id))
        else {
            return;
        };
        while j.frames.len() > pos {
            let mut frame = j.frames.pop().expect("frame length checked");
            self.rollback_txn_frame(&mut frame);
            j.spares.push(frame);
        }
        let frame = j.spares.pop().unwrap_or_default();
        j.frames.push(self.reset_txn_frame(frame, Some(id)));
        self.rebuild_flush_queue();
        if !self.authoritative_series {
            self.series_write().dirty = true;
        }
    }

    fn reset_txn_frame(&self, mut frame: TxnFrame, savepoint: Option<i32>) -> TxnFrame {
        frame.savepoint = savepoint;
        frame.buffer_marks.clear();
        frame.saved.clear();
        frame.added.clear();
        frame.removed.clear();
        frame.series_added.clear();
        frame.rollup_added.clear();
        frame.rollup_removed.clear();
        for entry in self.partitions.iter() {
            frame
                .buffer_marks
                .insert(*entry.key(), entry.value().timestamps.len());
        }
        frame
    }

    fn rollback_txn_frame(&self, frame: &mut TxnFrame) {
        // 1. Truncate buffers. Partitions with no mark were created
        //    during the frame → truncate to 0 (the empty PartitionBuffer
        //    entry itself is harmless and stays).
        for mut e in self.partitions.iter_mut() {
            let mark = frame.buffer_marks.get(e.key()).copied().unwrap_or(0);
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
        for (key, timestamps, values) in frame.saved.drain(..) {
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

        // 3. Index: adds out, removals back in. The dedup rule at
        //    record time guarantees `removed` never contains an entry
        //    whose chunk row was created inside this frame.
        {
            let mut index = self.index_write();
            for key in frame.added.drain() {
                index.remove(&key);
            }
            for (key, meta) in frame.removed.drain(..) {
                index.insert(key, meta);
            }
        }

        if !frame.rollup_added.is_empty() || !frame.rollup_removed.is_empty() {
            let mut rollups = self.rollup_write();
            for key in frame.rollup_added.drain() {
                rollups.remove(&key);
            }
            for (key, meta) in frame.rollup_removed.drain(..) {
                rollups.insert(key, meta);
            }
        }

        if !frame.series_added.is_empty() {
            let mut series = self.series_write();
            for id in frame.series_added.drain() {
                series.remove_id(id);
            }
            self.resolve_cache.clear();
        }
    }

    fn rebuild_flush_queue(&self) {
        let mut queue = self.flush_queue_lock();
        queue.clear();
        for mut entry in self.partitions.iter_mut() {
            let key = *entry.key();
            let buf = entry.value_mut();
            let should_queue = buf.timestamps.len() >= self.flush_threshold;
            buf.queued_for_flush = should_queue;
            if should_queue {
                queue.push(key);
            }
        }
    }

    fn merge_txn_frame(parent: &mut TxnFrame, child: &mut TxnFrame) {
        for (key, mut timestamps, mut values) in child.saved.drain(..) {
            let Some(mark) = parent.buffer_marks.get_mut(&key) else {
                continue;
            };
            if *mark == 0 {
                continue;
            }
            timestamps.truncate(*mark);
            values.truncate(*mark);
            parent.saved.push((key, timestamps, values));
            *mark = 0;
        }
        for (key, meta) in child.removed.drain(..) {
            if !parent.added.remove(&key) {
                parent.removed.push((key, meta));
            }
        }
        parent.added.extend(child.added.drain());
        for (key, meta) in child.rollup_removed.drain(..) {
            if !parent.rollup_added.remove(&key) {
                parent.rollup_removed.push((key, meta));
            }
        }
        parent.rollup_added.extend(child.rollup_added.drain());
        parent.series_added.extend(child.series_added.drain());
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
            // The fresh chunk_seq makes every insert key unique, so an
            // insert can never overwrite an existing entry (the min_ts
            // shadowing bug this key shape fixes) — the pre-fix
            // "journal the shadowed meta" branch is gone because
            // shadowing is now impossible by construction.
            let k = (key, meta.min_ts, self.next_chunk_seq());
            if let Some(j) = j.as_deref_mut() {
                j.added.insert(k);
            }
            index.insert(k, meta);
        }
    }

    /// Convenience constructor over the filesystem backend. Opening is
    /// fallible because interrupted compaction recovery must complete
    /// before the store can safely scan persisted chunks.
    pub fn new(
        data_dir: PathBuf,
        flush_threshold: usize,
        min_flush_size: usize,
        compression_level: usize,
        memory_budget: usize,
        defer_compression: bool,
    ) -> EngineResult<Self> {
        let store = FsStore::new(data_dir)?;
        Self::with_store(
            Box::new(store),
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
    ) -> EngineResult<Self> {
        let authoritative_series = store.has_authoritative_series();
        // P2: prime the refresh tokens BEFORE the recovery scans — read
        // after, a commit landing between scan and prime would be
        // skipped by the first refresh. Read before, the worst case is
        // one redundant reload (the same stale-token rule
        // refresh_authoritative_state documents).
        let primed_gen = if authoritative_series {
            store
                .catalog_generation()
                .map_err(|err| format!("failed to prime catalog generation: {err}"))?
        } else {
            None
        };
        let primed_wm = if authoritative_series {
            store
                .append_watermark()
                .map_err(|err| format!("failed to prime append watermark: {err}"))?
        } else {
            None
        };
        let stored_chunks = store
            .scan()
            .map_err(|err| format!("failed to recover chunk index: {err}"))?;

        let registry = if authoritative_series {
            let mut rows = store
                .load_series()
                .map_err(|err| format!("failed to load series catalog: {err}"))?;
            let mut registry = SeriesRegistry::from_stored(&rows)
                .map_err(|err| format!("series catalog is invalid: {err}"))?;
            let needs_legacy =
                rows.is_empty() || Self::validate_chunk_series(&registry, &stored_chunks).is_err();
            if needs_legacy {
                match store
                    .load_registry()
                    .map_err(|err| format!("failed to load legacy series registry: {err}"))?
                {
                    Some(bytes) => {
                        let legacy = SeriesRegistry::from_bytes(&bytes)
                            .map_err(|err| format!("legacy series registry is corrupt: {err}"))?;
                        let legacy_rows = legacy.stored_rows();
                        if !legacy_rows.is_empty() {
                            store.migrate_series(&legacy_rows).map_err(|err| {
                                format!("failed to migrate legacy series registry: {err}")
                            })?;
                            rows = store.load_series().map_err(|err| {
                                format!("failed to reload migrated series catalog: {err}")
                            })?;
                            registry = SeriesRegistry::from_stored(&rows).map_err(|err| {
                                format!("migrated series catalog is invalid: {err}")
                            })?;
                            for row in &legacy_rows {
                                let labels: Labels = row.labels.iter().cloned().collect();
                                if registry
                                    .series_map
                                    .get(&(row.name.clone(), labels))
                                    .copied()
                                    != Some(row.id)
                                {
                                    return Err(format!(
                                        "legacy series {} did not migrate with its original id",
                                        row.id
                                    ));
                                }
                            }
                        }
                    }
                    None if !stored_chunks.is_empty() || !rows.is_empty() => {
                        return Err(
                            "the authoritative series catalog does not identify every persisted \
                             chunk and no legacy registry is available"
                                .to_string(),
                        );
                    }
                    None => {}
                }
            }
            registry
        } else {
            match store
                .load_registry()
                .map_err(|err| format!("failed to load series registry: {err}"))?
            {
                Some(bytes) => SeriesRegistry::from_bytes(&bytes)
                    .map_err(|err| format!("series registry is corrupt: {err}"))?,
                None if !stored_chunks.is_empty() => {
                    return Err("persisted chunks exist without a series registry".to_string());
                }
                None => SeriesRegistry::new(),
            }
        };
        Self::validate_chunk_series(&registry, &stored_chunks)?;
        let stored_rollups = store
            .scan_rollups()
            .map_err(|err| format!("failed to scan rollup chunks: {err}"))?;

        let engine = Engine {
            store,
            transition: RwLock::new(()),
            authoritative_series,
            flush_threshold,
            min_flush_size,
            compression_level,
            memory_budget,
            defer_compression,
            partitions: DashMap::new(),
            index: RwLock::new(BTreeMap::new()),
            chunk_seq: AtomicU64::new(0),
            series: RwLock::new(registry),
            flush_queue: Mutex::new(Vec::new()),
            buffer_memory: AtomicUsize::new(0),
            cold_flush_running: AtomicBool::new(false),
            compaction_running: AtomicBool::new(false),
            resolve_cache: DashMap::new(),
            prometheus_ingest_batches: AtomicU64::new(0),
            prometheus_ingest_points: AtomicU64::new(0),
            prometheus_ingest_errors: AtomicU64::new(0),
            prometheus_ingest_total_ns: AtomicU64::new(0),
            raw_batch_query_count: AtomicU64::new(0),
            raw_batch_query_total_ns: AtomicU64::new(0),
            raw_batch_query_series_considered: AtomicU64::new(0),
            raw_batch_query_candidate_chunks: AtomicU64::new(0),
            raw_batch_query_payload_bytes_read: AtomicU64::new(0),
            raw_batch_query_decoded_points: AtomicU64::new(0),
            raw_batch_query_buffered_points_considered: AtomicU64::new(0),
            raw_batch_query_returned_points: AtomicU64::new(0),
            window_batch_query_count: AtomicU64::new(0),
            window_batch_query_total_ns: AtomicU64::new(0),
            window_batch_query_series_considered: AtomicU64::new(0),
            window_batch_query_candidate_chunks: AtomicU64::new(0),
            window_batch_query_payload_bytes_read: AtomicU64::new(0),
            window_batch_query_decoded_points: AtomicU64::new(0),
            window_batch_query_buffered_points_considered: AtomicU64::new(0),
            window_batch_query_returned_points: AtomicU64::new(0),
            txn_active: AtomicBool::new(false),
            txn: Mutex::new(TxnJournal::default()),
            catalog_gen: Mutex::new(primed_gen),
            append_wm: Mutex::new(primed_wm),
            rollup_index: RwLock::new(BTreeMap::new()),
            rollup_tiers: Mutex::new(Vec::new()),
            retention_native: AtomicI64::new(0),
            retention_floor: AtomicI64::new(i64::MIN),
        };
        engine.replace_index(stored_chunks);
        engine.replace_rollup_index(stored_rollups);
        Ok(engine)
    }

    // ── Series resolution ────────────────────────────────────────────

    /// Resolve (metric, labels) → series_id. Fast read path, slow write path.
    fn resolve_series(&self, metric_name: &str, labels: &Labels) -> EngineResult<i64> {
        let key = (metric_name.to_string(), labels.clone());
        let mut journal = self.txn_guard();
        let mut reg = self.series_write();
        if let Some(&id) = reg.series_map.get(&key) {
            return Ok(id);
        }
        if !self.authoritative_series {
            return Ok(reg.get_or_create(metric_name, labels));
        }

        let label_pairs: Vec<(String, String)> = labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let resolved = self
            .store
            .resolve_series(metric_name, &label_pairs)
            .map_err(|err| format!("failed to resolve series {metric_name:?}: {err}"))?;
        reg.insert_known(resolved.id, metric_name, labels, false)?;
        if resolved.created {
            if let Some(journal) = journal.as_deref_mut() {
                journal.series_added.insert(resolved.id);
            }
        }
        Ok(resolved.id)
    }

    /// Batch resolution for Tier 2 ingest: one registry pass for hits, and
    /// misses go to the store through ONE bulk call instead of a statement
    /// pair per series. Semantics per entry are identical to
    /// resolve_series — authoritative ids allocated in the caller's
    /// transaction, created ids journaled for rollback.
    ///
    /// LOCK HAZARD: the bulk store call runs multi-row SQL, and multi-row
    /// DML makes SQLite open a statement journal, which fires xSavepoint
    /// on every vtab in the transaction — re-entrantly, into THIS engine's
    /// txn_savepoint, which takes the journal mutex. So no engine lock may
    /// be held across the store call: misses are detected under a read
    /// lock that is dropped first, and results are recorded under locks
    /// taken after the call returns. The writer gate (held by every
    /// caller) is what makes the between-locks window safe: no other
    /// writer can touch the registry, and refresh skips while txn_active.
    pub fn resolve_series_batch(&self, entries: &[(String, Labels)]) -> EngineResult<Vec<i64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = vec![0i64; entries.len()];
        let mut misses: Vec<usize> = Vec::new();
        {
            let reg = self.series_read();
            for (idx, (metric_name, labels)) in entries.iter().enumerate() {
                if let Some(&id) = reg.series_map.get(&(metric_name.clone(), labels.clone())) {
                    out[idx] = id;
                } else {
                    misses.push(idx);
                }
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }
        if !self.authoritative_series {
            let mut reg = self.series_write();
            for &idx in &misses {
                out[idx] = reg.get_or_create(&entries[idx].0, &entries[idx].1);
            }
            return Ok(out);
        }

        // Dedupe repeated keys so the store sees each new series once and
        // the registry insert stays idempotent within the batch.
        let mut first_slot: HashMap<&(String, Labels), usize> = HashMap::new();
        let mut unique: Vec<usize> = Vec::new();
        for &idx in &misses {
            first_slot.entry(&entries[idx]).or_insert_with(|| {
                unique.push(idx);
                unique.len() - 1
            });
        }
        let requests: Vec<(&str, Vec<(String, String)>)> = unique
            .iter()
            .map(|&idx| {
                let (name, labels) = &entries[idx];
                (
                    name.as_str(),
                    labels
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                )
            })
            .collect();
        // NO engine locks held here — see the lock-hazard note above.
        let resolved = self
            .store
            .resolve_series_bulk(&requests)
            .map_err(|err| format!("failed to bulk-resolve series: {err}"))?;
        if resolved.len() != unique.len() {
            return Err(format!(
                "bulk series resolution returned {} of {} entries",
                resolved.len(),
                unique.len()
            ));
        }
        let mut journal = self.txn_guard();
        let mut reg = self.series_write();
        for (&idx, res) in unique.iter().zip(&resolved) {
            let (name, labels) = &entries[idx];
            reg.insert_known(res.id, name, labels, false)?;
            if res.created {
                if let Some(journal) = journal.as_deref_mut() {
                    journal.series_added.insert(res.id);
                }
            }
        }
        for &idx in &misses {
            out[idx] = resolved[first_slot[&entries[idx]]].id;
        }
        Ok(out)
    }

    fn save_series(&self) -> EngineResult<()> {
        if self.authoritative_series {
            return Ok(());
        }
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
        self.write_point_at(series_id, ts, val, Instant::now());
    }

    /// write_point with the wall-clock stamp hoisted out so batch ingest
    /// reads the clock once per statement instead of once per point.
    /// last_write feeds an IDLE-SECONDS heuristic, so statement
    /// granularity is far more precision than it needs. Measured effect
    /// is small (~2% of Tier 2 — the commpage clock read pipelines well;
    /// a sampling profiler wildly over-attributes it), but it is free.
    #[inline]
    fn write_point_at(&self, series_id: i64, ts: i64, val: f64, now: Instant) {
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
            buf.last_write = now;
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
    pub fn resolve_cached(
        &self,
        metric: &str,
        labels: &HashMap<String, String>,
    ) -> EngineResult<i64> {
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
        let now = Instant::now();
        for (metric, labels_hm, ts, val) in entries {
            let series_id = self.resolve_cached(&metric, &labels_hm)?;
            self.write_point_at(series_id, ts, val, now);
        }
        Ok(())
    }

    /// Binary batch: [series_id: i64, ts: i64, val: f64] = 24 bytes per entry.
    /// Use after pre-resolving series IDs.
    pub fn write_batch_raw(&self, data: &[u8]) -> EngineResult<()> {
        const ENTRY_SIZE: usize = 24;
        if !data.len().is_multiple_of(ENTRY_SIZE) {
            return Err(format!(
                "raw batch length {} is not a multiple of {}",
                data.len(),
                ENTRY_SIZE
            ));
        }
        let count = data.len() / ENTRY_SIZE;
        let now = Instant::now();
        for i in 0..count {
            let o = i * ENTRY_SIZE;
            let series_id = i64::from_ne_bytes(data[o..o + 8].try_into().unwrap());
            let ts = i64::from_ne_bytes(data[o + 8..o + 16].try_into().unwrap());
            let val = f64::from_ne_bytes(data[o + 16..o + 24].try_into().unwrap());
            self.write_point_at(series_id, ts, val, now);
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
        let started = Instant::now();
        let mut sorted: Vec<(&str, &str)> = Vec::with_capacity(16);
        let mut failure: EngineResult<()> = Ok(());
        let now = Instant::now();

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
                Ok(series_id) => self.write_point_at(series_id, ts, value, now),
                Err(e) => failure = Err(e),
            }
        });

        self.prometheus_ingest_batches
            .fetch_add(1, Ordering::Relaxed);
        self.prometheus_ingest_points
            .fetch_add(count as u64, Ordering::Relaxed);
        self.prometheus_ingest_errors
            .fetch_add(errors as u64, Ordering::Relaxed);
        self.prometheus_ingest_total_ns.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
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
        if name.contains(&b'\\')
            || labels
                .iter()
                .any(|(key, value)| key.contains(&b'\\') || value.contains(&b'\\'))
        {
            return self.resolve_escaped(name, labels);
        }
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

    /// Prometheus exposition escapes newline, quote, and backslash bytes in
    /// quoted metric names, label names, and label values. Keep the
    /// allocation-free borrowed fast path for overwhelmingly common
    /// unescaped identities, and materialize only samples that contain an
    /// escape.
    fn resolve_escaped(&self, name: &[u8], labels: &[(&[u8], &[u8])]) -> EngineResult<i64> {
        let metric_storage = name
            .contains(&b'\\')
            .then(|| unescape_prom_label_value(name));
        let metric = String::from_utf8_lossy(metric_storage.as_deref().unwrap_or(name));
        let labels: HashMap<String, String> = labels
            .iter()
            .map(|&(raw_key, raw_value)| {
                let key_storage = raw_key
                    .contains(&b'\\')
                    .then(|| unescape_prom_label_value(raw_key));
                let value_storage = raw_value
                    .contains(&b'\\')
                    .then(|| unescape_prom_label_value(raw_value));
                (
                    String::from_utf8_lossy(key_storage.as_deref().unwrap_or(raw_key)).into_owned(),
                    String::from_utf8_lossy(value_storage.as_deref().unwrap_or(raw_value))
                        .into_owned(),
                )
            })
            .collect();
        self.resolve_cached(&metric, &labels)
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
        let count = self.flush_pending_inner()?;
        if count > 0 {
            self.apply_retention()?;
        }
        Ok(count)
    }

    fn flush_pending_inner(&self) -> EngineResult<usize> {
        let _transition = self.transition_write();
        let keys: Vec<PartitionKey> = {
            let mut queue = self.flush_queue_lock();
            std::mem::take(&mut *queue)
        };
        // The virtual-table ingest paths call this after every statement so
        // the advertised threshold is self-driving for direct SQLite users.
        // Keep the overwhelmingly common below-threshold path free of store
        // writes (especially the legacy registry snapshot).
        if keys.is_empty() {
            return Ok(0);
        }
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
        let _transition = self.transition_write();
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

        let _transition = self.transition_write();
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
        let _transition = self.transition_write();
        let current = self.buffer_memory.load(Ordering::Relaxed);
        if current <= self.memory_budget {
            return Ok(0);
        }

        let mut sizes: Vec<(PartitionKey, usize)> = self
            .partitions
            .iter()
            .map(|e| (*e.key(), e.value().timestamps.len()))
            .collect();
        sizes.sort_by_key(|entry| std::cmp::Reverse(entry.1));

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
        self.flush_all_inner()?;
        self.apply_retention()?;
        Ok(())
    }

    fn flush_all_inner(&self) -> EngineResult<()> {
        let _transition = self.transition_write();
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
            let ts_compressed =
                pco::standalone::simple_compress(ts_slice, &config).map_err(|err| {
                    format!(
                        "failed to compress timestamps for series {}: {err}",
                        key.series_id
                    )
                })?;
            let val_compressed =
                pco::standalone::simple_compress(val_slice, &config).map_err(|err| {
                    format!(
                        "failed to compress values for series {}: {err}",
                        key.series_id
                    )
                })?;
            (ts_compressed, val_compressed)
        };

        let min_ts = ts_slice[0];
        let max_ts = ts_slice[ts_slice.len() - 1];
        let max_ts_index = ts_slice.partition_point(|ts| *ts < max_ts);
        let max_ts_val = val_slice[max_ts_index];
        let point_count = ts_slice.len() as u32;
        let (mut min_val, mut max_val, mut sum_val) = (f64::NAN, f64::NAN, 0.0f64);
        for &v in val_slice {
            if min_val.is_nan() || v < min_val {
                min_val = v;
            }
            if max_val.is_nan() || v > max_val {
                max_val = v;
            }
            sum_val += v;
        }

        Ok(EncodedChunk {
            series_id: key.series_id,
            min_ts,
            max_ts,
            max_ts_val,
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
        let out = self.compact_partitions_inner(cutoff_ts)?;
        self.apply_retention()?;
        Ok(out)
    }

    fn compact_partitions_inner(&self, cutoff_ts: i64) -> EngineResult<(usize, usize)> {
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
        let _transition = self.transition_write();

        // Group eligible chunks by series: all raw chunks, plus pco
        // chunks small enough that merging improves the ratio.
        let mut candidates: HashMap<PartitionKey, Vec<(ChunkKey, ChunkMeta)>> = HashMap::new();
        {
            let index = self.index_read();
            for (chunk_key, meta) in index.iter() {
                let eligible = meta.max_ts < cutoff_ts
                    && (meta.encoding == ENC_RAW || meta.point_count < SMALL_CHUNK_POINTS);
                if eligible {
                    candidates
                        .entry(chunk_key.0)
                        .or_default()
                        .push((*chunk_key, meta.clone()));
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
        let mut plans: Vec<(PartitionKey, Vec<(ChunkKey, ChunkMeta)>, usize)> = Vec::new();
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
        let removed: HashSet<ChunkKey> = plans
            .iter()
            .flat_map(|(_, chunks, _)| chunks.iter().map(|(k, _)| *k))
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
                for (chunk_key, meta) in chunks {
                    if let Some(j) = j.as_deref_mut() {
                        if !j.added.remove(chunk_key) {
                            j.removed.push((*chunk_key, meta.clone()));
                        }
                    }
                    index.remove(chunk_key);
                }
                for i in next..next + new_count {
                    let meta = add[i].meta(locs[i].clone());
                    // Fresh chunk_seq → the key cannot collide with any
                    // existing entry, so no shadowed-meta journaling is
                    // needed (see index_insert_new).
                    let k = (*key, meta.min_ts, self.next_chunk_seq());
                    if let Some(j) = j.as_deref_mut() {
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
        // The caller holds the transition guard. Journal before the
        // partition lock: draining while
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
        let _transition = self.transition_read();
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
                let points = self.query_range_by_id_inner(sid, t_start, t_end)?;
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
        let _transition = self.transition_read();
        self.query_range_by_id_inner(series_id, t_start, t_end)
    }

    /// Query an ordered batch of series without Rayon. This is the
    /// callback-safe packed-raw primitive: one transition guard and one store
    /// batch read replace a separate chunk lookup for every series. Results
    /// retain input series-id order, including empty vectors, and preserve the
    /// exact stable timestamp ordering of `query_range_by_id`.
    pub fn query_range_batch_by_id(
        &self,
        series_ids: &[i64],
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Vec<(i64, Vec<(i64, f64)>)>> {
        self.query_range_batch_by_id_inner(series_ids, t_start, t_end, None)
    }

    /// The bounded form of [`Self::query_range_batch_by_id`]. `max_work_points`
    /// caps the conservative number of stored chunk points plus buffered
    /// points that the query may inspect. The inclusive limit is checked
    /// before persisted payloads are read, so callers can bound decode and
    /// result-allocation work without relying on a post-query row limit.
    pub fn query_range_batch_by_id_limited(
        &self,
        series_ids: &[i64],
        t_start: i64,
        t_end: i64,
        max_work_points: u64,
    ) -> EngineResult<Vec<(i64, Vec<(i64, f64)>)>> {
        self.query_range_batch_by_id_inner(series_ids, t_start, t_end, Some(max_work_points))
    }

    fn query_range_batch_by_id_inner(
        &self,
        series_ids: &[i64],
        t_start: i64,
        t_end: i64,
        max_work_points: Option<u64>,
    ) -> EngineResult<Vec<(i64, Vec<(i64, f64)>)>> {
        let started = Instant::now();
        if t_start > t_end {
            self.record_raw_batch_query(started, series_ids.len(), 0, 0, 0, 0, 0);
            return Ok(series_ids.iter().map(|&sid| (sid, Vec::new())).collect());
        }

        let _transition = self.transition_read();
        let matching: Vec<Vec<ChunkMeta>> = {
            let index = self.index_read();
            series_ids
                .iter()
                .map(|&series_id| {
                    let pk = PartitionKey { series_id };
                    index
                        .range((pk, i64::MIN, u64::MIN)..)
                        .take_while(|((key, _, _), _)| key == &pk)
                        .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                        .map(|(_, meta)| meta.clone())
                        .collect()
                })
                .collect()
        };
        let locs: Vec<ChunkLoc> = matching
            .iter()
            .flat_map(|chunks| chunks.iter().map(|meta| meta.loc.clone()))
            .collect();
        let decoded_points = matching
            .iter()
            .flat_map(|chunks| chunks.iter())
            .map(|meta| u64::from(meta.point_count))
            .sum::<u64>();
        let preflight_buffered_points = series_ids.iter().fold(0_u64, |total, &series_id| {
            let pk = PartitionKey { series_id };
            total.saturating_add(
                self.partitions
                    .get(&pk)
                    .map_or(0, |buffer| buffer.timestamps.len() as u64),
            )
        });
        let preflight_work_points = decoded_points.saturating_add(preflight_buffered_points);
        if let Some(limit) = max_work_points.filter(|limit| preflight_work_points > *limit) {
            self.record_raw_batch_query(
                started,
                series_ids.len(),
                locs.len(),
                0,
                decoded_points,
                preflight_buffered_points,
                0,
            );
            return Err(format!(
                "raw batch work point limit {limit} exceeded (candidate points: {preflight_work_points})"
            ));
        }
        let chunk_bytes = self.store.read_chunks(&locs)?;
        if chunk_bytes.len() != locs.len() {
            return Err(format!(
                "batch chunk read returned {} payloads for {} locations",
                chunk_bytes.len(),
                locs.len()
            ));
        }

        let payload_bytes = chunk_bytes
            .iter()
            .map(|bytes| bytes.ts().len().saturating_add(bytes.val().len()))
            .sum::<usize>();
        let candidate_chunks = locs.len();
        let mut payloads = chunk_bytes.into_iter();
        let mut result = Vec::with_capacity(series_ids.len());
        let mut buffered_points_considered = 0_u64;
        let mut returned_points = 0_u64;
        for (&series_id, chunks) in series_ids.iter().zip(matching) {
            let mut points = Vec::new();
            for meta in chunks {
                let bytes = payloads
                    .next()
                    .ok_or_else(|| "batch chunk payload order underflow".to_string())?;
                points.extend(Self::decode_chunk_data(&meta, &bytes, t_start, t_end)?);
            }

            let pk = PartitionKey { series_id };
            if let Some(buffer) = self.partitions.get(&pk) {
                buffered_points_considered =
                    buffered_points_considered.saturating_add(buffer.timestamps.len() as u64);
                let observed_work_points =
                    decoded_points.saturating_add(buffered_points_considered);
                if let Some(limit) = max_work_points.filter(|limit| observed_work_points > *limit) {
                    self.record_raw_batch_query(
                        started,
                        series_ids.len(),
                        candidate_chunks,
                        payload_bytes,
                        decoded_points,
                        buffered_points_considered,
                        returned_points,
                    );
                    return Err(format!(
                        "raw batch work point limit {limit} exceeded (candidate points: {observed_work_points})"
                    ));
                }
                for index in 0..buffer.timestamps.len() {
                    let timestamp = buffer.timestamps[index];
                    if timestamp >= t_start && timestamp <= t_end {
                        points.push((timestamp, buffer.values[index]));
                    }
                }
            }
            points.sort_by_key(|&(timestamp, _)| timestamp);
            returned_points = returned_points.saturating_add(points.len() as u64);
            result.push((series_id, points));
        }
        debug_assert!(payloads.next().is_none());
        self.record_raw_batch_query(
            started,
            series_ids.len(),
            candidate_chunks,
            payload_bytes,
            decoded_points,
            buffered_points_considered,
            returned_points,
        );
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_raw_batch_query(
        &self,
        started: Instant,
        series_considered: usize,
        candidate_chunks: usize,
        payload_bytes: usize,
        decoded_points: u64,
        buffered_points_considered: u64,
        returned_points: u64,
    ) {
        let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.raw_batch_query_count.fetch_add(1, Ordering::Relaxed);
        self.raw_batch_query_total_ns
            .fetch_add(elapsed, Ordering::Relaxed);
        self.raw_batch_query_series_considered.fetch_add(
            u64::try_from(series_considered).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.raw_batch_query_candidate_chunks.fetch_add(
            u64::try_from(candidate_chunks).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.raw_batch_query_payload_bytes_read.fetch_add(
            u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.raw_batch_query_decoded_points
            .fetch_add(decoded_points, Ordering::Relaxed);
        self.raw_batch_query_buffered_points_considered
            .fetch_add(buffered_points_considered, Ordering::Relaxed);
        self.raw_batch_query_returned_points
            .fetch_add(returned_points, Ordering::Relaxed);
    }

    fn query_range_by_id_inner(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Vec<(i64, f64)>> {
        let pk = PartitionKey { series_id };

        let matching: Vec<ChunkMeta> = {
            let index = self.index_read();
            index
                .range((pk, i64::MIN, u64::MIN)..)
                .take_while(|((k, _, _), _)| k == &pk)
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

    /// Return the newest point in an inclusive range without materializing the
    /// series history.
    ///
    /// This is the callback-safe primitive used by SQLite extensions. Candidate
    /// chunks are visited by descending possible timestamp and the walk stops
    /// once an older chunk cannot change the result. Duplicate timestamps retain
    /// `query_range_by_id` semantics: the first point in its stable engine order
    /// wins (chunk-index order, then in-chunk order, then buffered insertion
    /// order).
    pub fn query_latest_by_id(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Option<(i64, f64)>> {
        if t_start > t_end {
            return Ok(None);
        }
        let _transition = self.transition_read();
        self.query_latest_by_id_inner(series_id, t_start, t_end)
    }

    /// Return the newest point for an ordered batch of durable series IDs.
    ///
    /// This is the callback-safe packed-latest primitive: one transition guard,
    /// one index snapshot, and one ordered store batch read replace a separate
    /// query transition and shadow-table statement per series. Results retain
    /// input ID order, including repeated and missing IDs.
    pub fn query_latest_batch_by_id(
        &self,
        series_ids: &[i64],
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<LatestSeriesBatch> {
        if t_start > t_end {
            return Ok(series_ids.iter().map(|&sid| (sid, None)).collect());
        }

        let _transition = self.transition_read();
        let matching: Vec<Vec<(usize, ChunkMeta)>> = {
            let index = self.index_read();
            series_ids
                .iter()
                .map(|&series_id| {
                    let pk = PartitionKey { series_id };
                    let mut chunks: Vec<_> = index
                        .range((pk, i64::MIN, u64::MIN)..)
                        .take_while(|((key, _, _), _)| key == &pk)
                        .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                        .enumerate()
                        .map(|(rank, (_, meta))| (rank, meta.clone()))
                        .collect();
                    chunks.sort_by(|(rank_a, a), (rank_b, b)| {
                        b.max_ts
                            .min(t_end)
                            .cmp(&a.max_ts.min(t_end))
                            .then_with(|| rank_a.cmp(rank_b))
                    });
                    chunks
                })
                .collect()
        };

        // Metadata answers an unbounded latest lookup without bytes. Collect
        // only chunks whose maximum lies beyond the upper bound or whose
        // persisted newest-value metadata is absent (legacy databases).
        let mut locs = Vec::new();
        let work: Vec<Vec<(usize, ChunkMeta, Option<usize>)>> = matching
            .into_iter()
            .map(|chunks| {
                chunks
                    .into_iter()
                    .map(|(rank, meta)| {
                        let decode_index = if meta.max_ts <= t_end && meta.max_ts_val.is_some() {
                            None
                        } else {
                            let index = locs.len();
                            locs.push(meta.loc.clone());
                            Some(index)
                        };
                        (rank, meta, decode_index)
                    })
                    .collect()
            })
            .collect();
        let payloads = self.store.read_chunks(&locs)?;
        if payloads.len() != locs.len() {
            return Err(format!(
                "batch chunk read returned {} payloads for {} locations",
                payloads.len(),
                locs.len()
            ));
        }

        let mut result = Vec::with_capacity(series_ids.len());
        for (&series_id, chunks) in series_ids.iter().zip(work) {
            let pk = PartitionKey { series_id };
            // Buffered points follow persisted chunks in stable raw order.
            let mut best: Option<(i64, f64, usize)> = None;
            if let Some(buffer) = self.partitions.get(&pk) {
                for index in 0..buffer.timestamps.len() {
                    let timestamp = buffer.timestamps[index];
                    if timestamp < t_start || timestamp > t_end {
                        continue;
                    }
                    if best
                        .as_ref()
                        .is_none_or(|(best_timestamp, _, _)| timestamp > *best_timestamp)
                    {
                        best = Some((timestamp, buffer.values[index], usize::MAX));
                    }
                }
            }

            for (rank, meta, decode_index) in chunks {
                let possible_ts = meta.max_ts.min(t_end);
                if best
                    .as_ref()
                    .is_some_and(|(best_timestamp, _, _)| possible_ts < *best_timestamp)
                {
                    break;
                }

                let chunk_best = match decode_index {
                    None => meta.max_ts_val.map(|value| (meta.max_ts, value)),
                    Some(index) => {
                        let points =
                            Self::decode_chunk_data(&meta, &payloads[index], t_start, t_end)?;
                        let mut decoded_best: Option<(i64, f64)> = None;
                        for (timestamp, value) in points {
                            if decoded_best
                                .as_ref()
                                .is_none_or(|(best_timestamp, _)| timestamp > *best_timestamp)
                            {
                                decoded_best = Some((timestamp, value));
                            }
                        }
                        decoded_best
                    }
                };

                let Some((timestamp, value)) = chunk_best else {
                    continue;
                };
                let replace = match best {
                    None => true,
                    Some((best_timestamp, _, best_rank)) => {
                        timestamp > best_timestamp
                            || (timestamp == best_timestamp && rank < best_rank)
                    }
                };
                if replace {
                    best = Some((timestamp, value, rank));
                }
            }
            result.push((
                series_id,
                best.map(|(timestamp, value, _)| (timestamp, value)),
            ));
        }
        Ok(result)
    }

    fn query_latest_by_id_inner(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Option<(i64, f64)>> {
        let pk = PartitionKey { series_id };

        // Preserve each chunk's position in query_range_by_id's pre-sort
        // stream. We may read chunks newest-first, but this rank resolves an
        // equal-timestamp tie exactly as the stable range sort does.
        let mut chunks: Vec<(usize, ChunkMeta)> = {
            let index = self.index_read();
            index
                .range((pk, i64::MIN, u64::MIN)..)
                .take_while(|((k, _, _), _)| k == &pk)
                .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                .enumerate()
                .map(|(rank, (_, meta))| (rank, meta.clone()))
                .collect()
        };

        // Highest possible in-range timestamp first. The stable secondary rank
        // is not required for correctness, but makes the read order deterministic.
        chunks.sort_by(|(rank_a, a), (rank_b, b)| {
            b.max_ts
                .min(t_end)
                .cmp(&a.max_ts.min(t_end))
                .then_with(|| rank_a.cmp(rank_b))
        });

        // (timestamp, value, source rank). Buffered points follow every chunk
        // in query_range_by_id, so usize::MAX is their source rank.
        let mut best: Option<(i64, f64, usize)> = None;
        if let Some(buf) = self.partitions.get(&pk) {
            for i in 0..buf.timestamps.len() {
                let ts = buf.timestamps[i];
                if ts < t_start || ts > t_end {
                    continue;
                }
                if best.as_ref().is_none_or(|(best_ts, _, _)| ts > *best_ts) {
                    best = Some((ts, buf.values[i], usize::MAX));
                }
            }
        }

        for (rank, meta) in chunks {
            let possible_ts = meta.max_ts.min(t_end);
            if best
                .as_ref()
                .is_some_and(|(best_ts, _, _)| possible_ts < *best_ts)
            {
                break;
            }

            let chunk_best = if meta.max_ts <= t_end {
                meta.max_ts_val.map(|value| (meta.max_ts, value))
            } else {
                None
            };
            let chunk_best = match chunk_best {
                Some(point) => Some(point),
                None => {
                    let points = self.read_chunk_data(&meta, t_start, t_end)?;
                    let mut decoded_best: Option<(i64, f64)> = None;
                    for (ts, value) in points {
                        // Do not replace on equality: the first point within a
                        // chunk is also first after the stable range sort.
                        if decoded_best
                            .as_ref()
                            .is_none_or(|(best_ts, _)| ts > *best_ts)
                        {
                            decoded_best = Some((ts, value));
                        }
                    }
                    decoded_best
                }
            };

            let Some((ts, value)) = chunk_best else {
                continue;
            };
            let replace = match best {
                None => true,
                Some((best_ts, _, best_rank)) => {
                    ts > best_ts || (ts == best_ts && rank < best_rank)
                }
            };
            if replace {
                best = Some((ts, value, rank));
            }
        }

        Ok(best.map(|(ts, value, _)| (ts, value)))
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
        let _transition = self.transition_read();
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
                let summary = self.query_aggregate_summary_by_id_inner(sid, t_start, t_end)?;
                Ok(summary.map(|summary| (labels, summary.value(agg))))
            })
            .filter_map(|result: EngineResult<Option<(Labels, f64)>>| match result {
                Ok(Some(value)) => Some(Ok(value)),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            })
            .collect()
    }

    /// Compute the chunk-aware scalar summary for one series without Rayon.
    ///
    /// This is the callback-safe primitive used by SQLite extensions. Fully
    /// covered chunks use their persisted count/sum/min/max metadata; only
    /// boundary chunks are decoded. The public wrapper holds the transition
    /// read guard for the complete operation.
    pub fn query_aggregate_summary_by_id(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Option<AggregateSummary>> {
        let _transition = self.transition_read();
        self.query_aggregate_summary_by_id_inner(series_id, t_start, t_end)
    }

    /// Compute chunk-aware scalar summaries for an ordered batch of durable
    /// series IDs without Rayon.
    ///
    /// Fully covered chunks retain the metadata fast path. Boundary and legacy
    /// NaN-metadata chunks are fetched through one ordered store batch. Results
    /// retain input ID order, including repeated and missing IDs.
    pub fn query_aggregate_summary_batch_by_id(
        &self,
        series_ids: &[i64],
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<AggregateSummaryBatch> {
        let _transition = self.transition_read();
        let matching: Vec<Vec<ChunkMeta>> = {
            let index = self.index_read();
            series_ids
                .iter()
                .map(|&series_id| {
                    let pk = PartitionKey { series_id };
                    index
                        .range((pk, i64::MIN, u64::MIN)..)
                        .take_while(|((key, _, _), _)| key == &pk)
                        .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                        .map(|(_, meta)| meta.clone())
                        .collect()
                })
                .collect()
        };

        let mut locs = Vec::new();
        let work: Vec<Vec<(ChunkMeta, Option<usize>)>> = matching
            .into_iter()
            .map(|chunks| {
                chunks
                    .into_iter()
                    .map(|meta| {
                        let covered = meta.min_ts >= t_start
                            && meta.max_ts <= t_end
                            && !meta.min_val.is_nan()
                            && !meta.max_val.is_nan();
                        let decode_index = if covered {
                            None
                        } else {
                            let index = locs.len();
                            locs.push(meta.loc.clone());
                            Some(index)
                        };
                        (meta, decode_index)
                    })
                    .collect()
            })
            .collect();
        let payloads = self.store.read_chunks(&locs)?;
        if payloads.len() != locs.len() {
            return Err(format!(
                "batch chunk read returned {} payloads for {} locations",
                payloads.len(),
                locs.len()
            ));
        }

        let mut result = Vec::with_capacity(series_ids.len());
        for (&series_id, chunks) in series_ids.iter().zip(work) {
            let mut total_count: u64 = 0;
            let mut total_sum: f64 = 0.0;
            let mut global_min: Option<f64> = None;
            let mut global_max: Option<f64> = None;

            for (meta, decode_index) in chunks {
                if let Some(index) = decode_index {
                    let points = Self::decode_chunk_data(&meta, &payloads[index], t_start, t_end)?;
                    for (_, value) in points {
                        total_count += 1;
                        total_sum += value;
                        global_min = Some(match global_min {
                            Some(current) => current.min(value),
                            None => value,
                        });
                        global_max = Some(match global_max {
                            Some(current) => current.max(value),
                            None => value,
                        });
                    }
                } else {
                    total_count += meta.point_count as u64;
                    total_sum += meta.sum_val;
                    global_min = Some(match global_min {
                        Some(current) => current.min(meta.min_val),
                        None => meta.min_val,
                    });
                    global_max = Some(match global_max {
                        Some(current) => current.max(meta.max_val),
                        None => meta.max_val,
                    });
                }
            }

            let pk = PartitionKey { series_id };
            if let Some(buffer) = self.partitions.get(&pk) {
                for index in 0..buffer.timestamps.len() {
                    if buffer.timestamps[index] >= t_start && buffer.timestamps[index] <= t_end {
                        let value = buffer.values[index];
                        total_count += 1;
                        total_sum += value;
                        global_min = Some(match global_min {
                            Some(current) => current.min(value),
                            None => value,
                        });
                        global_max = Some(match global_max {
                            Some(current) => current.max(value),
                            None => value,
                        });
                    }
                }
            }

            let summary = (total_count != 0).then(|| AggregateSummary {
                count: total_count,
                sum: total_sum,
                min: global_min.expect("non-empty aggregate has a minimum"),
                max: global_max.expect("non-empty aggregate has a maximum"),
            });
            result.push((series_id, summary));
        }
        Ok(result)
    }

    fn query_aggregate_summary_by_id_inner(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Option<AggregateSummary>> {
        let pk = PartitionKey { series_id };

        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut global_min: Option<f64> = None;
        let mut global_max: Option<f64> = None;

        let chunks: Vec<ChunkMeta> = {
            let index = self.index_read();
            index
                .range((pk, i64::MIN, u64::MIN)..)
                .take_while(|((k, _, _), _)| k == &pk)
                .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                .map(|(_, meta)| meta.clone())
                .collect()
        };

        for meta in &chunks {
            if meta.min_ts >= t_start
                && meta.max_ts <= t_end
                && !meta.min_val.is_nan()
                && !meta.max_val.is_nan()
            {
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
                // Boundary chunks must be decoded for bounds. Full chunks
                // with NaN min/max are decoded as well: current writers store
                // numeric extrema when any exist, but older chunks could have
                // inherited a leading NaN in their metadata.
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
        Ok(Some(AggregateSummary {
            count: total_count,
            sum: total_sum,
            min: global_min.unwrap(),
            max: global_max.unwrap(),
        }))
    }

    // ── Q2 reduction kernels (PLAN.md "Query interface tiers") ───────
    //
    // Semantics-free data reduction ONLY: a fixed arithmetic grid, a
    // half-open (t - width, t] window, and mechanical folds. No lookback
    // defaults, no staleness, no rate/reset math, no __name__ policy —
    // everything a VM referee has ever corrected stays above the waist.
    //
    // Bit-exactness contract (what the property tests pin down):
    //   - samples are consumed in the engine's sorted order
    //     (query_range_by_id order; ties keep engine order, last wins),
    //   - Sum/Avg use compensated folds in ascending timestamp order,
    //   - Min uses an ordered comparison seeded by the first sample: an
    //     incoming NaN is ignored, a leading NaN is replaced by the first
    //     ordered value, and equal signed zeros retain the first sample.
    //   - Max mirrors Min's ordered comparison and first-sample stability.
    // A naive evaluator over the same raw samples must agree on every
    // bit, which is what makes these kernels safe to push down.

    /// Largest number of grid points one kernel call may produce per
    /// series. Purely a resource guard against `step` typos (e.g. step=1
    /// over an epoch-wide range); well above any dashboard's resolution.
    pub const MAX_GRID_POINTS: i64 = 1_000_000;

    fn grid_len(start: i64, stop: i64, step: i64) -> EngineResult<i64> {
        if step <= 0 {
            return Err(format!("grid step must be positive, got {step}"));
        }
        if stop < start {
            return Ok(0);
        }
        let count = ((stop as i128 - start as i128) / step as i128) + 1;
        if count > Self::MAX_GRID_POINTS as i128 {
            return Err(format!(
                "grid of {count} points exceeds the {} point cap (start {start}, stop {stop}, step {step})",
                Self::MAX_GRID_POINTS
            ));
        }
        Ok(count as i64)
    }

    /// The grid-last walk over one series' ts-sorted samples. O(n + m).
    fn grid_last_walk(
        samples: &[(i64, f64)],
        start: i64,
        stop: i64,
        step: i64,
        lookback: i64,
    ) -> Vec<(i64, f64)> {
        let n = samples.len();
        let mut points = Vec::new();
        let mut k = 0usize; // one past the last sample with ts <= t
        let mut t = start;
        loop {
            while k < n && samples[k].0 <= t {
                k += 1;
            }
            if k > 0 {
                let (ts, val) = samples[k - 1];
                if (ts as i128) > (t as i128) - (lookback as i128) {
                    points.push((t, val));
                }
            }
            match t.checked_add(step) {
                Some(next) if next <= stop => t = next,
                _ => break,
            }
        }
        points
    }

    /// One F7 window operation over one window slice (engine ts order).
    /// Returns None for "no row" (empty after NaN exclusion/trimming).
    /// Definitions pinned in FEATURE_PLAN F7; property tests quote them.
    fn window_op_value(win: &[(i64, f64)], window: i64, op: WindowOp) -> Option<f64> {
        debug_assert!(!win.is_empty());
        match op {
            WindowOp::Agg(agg) => Some(match agg {
                AggFn::Count => win.len() as f64,
                AggFn::Sum => Self::compensated_window_sum(win),
                AggFn::Avg => Self::compensated_window_average(win),
                AggFn::Min => win[1..].iter().fold(win[0].1, |acc, &(_, value)| {
                    if acc > value || acc.is_nan() {
                        value
                    } else {
                        acc
                    }
                }),
                AggFn::Max => win[1..].iter().fold(win[0].1, |acc, &(_, value)| {
                    if acc < value || acc.is_nan() {
                        value
                    } else {
                        acc
                    }
                }),
            }),
            WindowOp::Delta => Some(win[win.len() - 1].1 - win[0].1),
            WindowOp::Increase => Some(Self::increase_of(win)),
            WindowOp::Rate => Some(Self::increase_of(win) / window as f64),
            WindowOp::Percentile(q) => {
                let sorted = Self::sorted_finite_or_nanless(win);
                if sorted.is_empty() {
                    return None;
                }
                let n = sorted.len();
                let rank = ((q / 100.0) * n as f64).ceil() as usize;
                Some(sorted[rank.clamp(1, n) - 1])
            }
            WindowOp::TrimmedMean(q) => {
                let sorted = Self::sorted_finite_or_nanless(win);
                let n = sorted.len();
                let k = ((n as f64) * (q / 100.0)).floor() as usize;
                if n == 0 || 2 * k >= n {
                    return None;
                }
                let kept = &sorted[k..n - k];
                Some(kept.iter().fold(0.0f64, |acc, &v| acc + v) / kept.len() as f64)
            }
        }
    }

    /// Cancellation-safe average with the direct-sum/overflow fallback used
    /// by Prometheus. This remains a general float window reduction: language
    /// parsing and response semantics stay above the storage boundary.
    fn compensated_window_average(win: &[(i64, f64)]) -> f64 {
        debug_assert!(!win.is_empty());
        let mut sum = win[0].1;
        let mut compensation = 0.0;
        let mut mean = sum;
        let mut count = 1.0;
        let mut incremental_mean = false;
        for &(_, value) in &win[1..] {
            count += 1.0;
            if !incremental_mean {
                let (next_sum, next_compensation) = compensated_add(value, sum, compensation);
                if !next_sum.is_infinite() {
                    sum = next_sum;
                    compensation = next_compensation;
                    continue;
                }
                incremental_mean = true;
                mean = sum / (count - 1.0);
                compensation /= count - 1.0;
            }
            let previous_weight = (count - 1.0) / count;
            (mean, compensation) = compensated_add(
                value / count,
                previous_weight * mean,
                previous_weight * compensation,
            );
        }
        if incremental_mean {
            mean + compensation
        } else {
            sum / count + compensation / count
        }
    }

    /// Cancellation-safe sum with Prometheus's compensated add semantics.
    /// Infinite overflow stays infinite; mixed infinities and NaN propagate.
    fn compensated_window_sum(win: &[(i64, f64)]) -> f64 {
        debug_assert!(!win.is_empty());
        let mut sum = win[0].1;
        let mut compensation = 0.0;
        for &(_, value) in &win[1..] {
            (sum, compensation) = compensated_add(value, sum, compensation);
        }
        sum + compensation
    }

    /// The pinned increase rule: reset-adjusted sum of steps, first
    /// sample contributes nothing. NOT PromQL (no extrapolation).
    fn increase_of(win: &[(i64, f64)]) -> f64 {
        let mut acc = 0.0f64;
        for pair in win.windows(2) {
            let (prev, cur) = (pair[0].1, pair[1].1);
            acc += if cur >= prev { cur - prev } else { cur };
        }
        acc
    }

    /// NaN-excluded values sorted by total_cmp (the pNN / tavg:N base).
    fn sorted_finite_or_nanless(win: &[(i64, f64)]) -> Vec<f64> {
        let mut vals: Vec<f64> = win
            .iter()
            .map(|&(_, v)| v)
            .filter(|v| !v.is_nan())
            .collect();
        vals.sort_unstable_by(f64::total_cmp);
        vals
    }

    /// The window-operation walk over one series' ts-sorted samples.
    /// Folds each (t - window, t] window fresh, left-to-right — that IS
    /// the bit-exactness contract, so no prefix-sum tricks.
    fn window_op_walk(
        samples: &[(i64, f64)],
        start: i64,
        stop: i64,
        step: i64,
        window: i64,
        op: WindowOp,
    ) -> Vec<(i64, f64)> {
        let n = samples.len();
        let mut points = Vec::new();
        let mut lo = 0usize; // first sample with ts > t - window
        let mut hi = 0usize; // one past the last sample with ts <= t
        let mut t = start;
        loop {
            while hi < n && samples[hi].0 <= t {
                hi += 1;
            }
            while lo < n && (samples[lo].0 as i128) <= (t as i128) - (window as i128) {
                lo += 1;
            }
            if lo < hi {
                if let Some(value) = Self::window_op_value(&samples[lo..hi], window, op) {
                    points.push((t, value));
                }
            }
            match t.checked_add(step) {
                Some(next) if next <= stop => t = next,
                _ => break,
            }
        }
        points
    }

    fn validate_grid_last(start: i64, stop: i64, step: i64, lookback: i64) -> EngineResult<i64> {
        if lookback < 0 {
            return Err(format!("lookback must be >= 0, got {lookback}"));
        }
        Self::grid_len(start, stop, step)
    }

    fn validate_window(start: i64, stop: i64, step: i64, window: i64) -> EngineResult<i64> {
        if window <= 0 {
            return Err(format!("window must be positive, got {window}"));
        }
        Self::grid_len(start, stop, step)
    }

    /// Q2(a), single series, rayon-free — safe from vtab callbacks (the
    /// same reason collect_metric loops query_range_by_id: worker
    /// threads have no bound host connection).
    pub fn query_grid_last_by_id(
        &self,
        series_id: i64,
        start: i64,
        stop: i64,
        step: i64,
        lookback: i64,
    ) -> EngineResult<Vec<(i64, f64)>> {
        if Self::validate_grid_last(start, stop, step, lookback)? == 0 {
            return Ok(Vec::new());
        }
        let _transition = self.transition_read();
        let samples =
            self.query_range_by_id_inner(series_id, start.saturating_sub(lookback), stop)?;
        Ok(Self::grid_last_walk(&samples, start, stop, step, lookback))
    }

    /// F7: the full window vocabulary, single series, rayon-free.
    /// Validation: same grid rules as the classic aggs; op parameters
    /// are validated by the caller (the vtab's parser) AND defensively
    /// here (a q outside its documented range is an error, not a
    /// clamp).
    pub fn query_window_op_by_id(
        &self,
        series_id: i64,
        start: i64,
        stop: i64,
        step: i64,
        window: i64,
        op: WindowOp,
    ) -> EngineResult<Vec<(i64, f64)>> {
        match op {
            WindowOp::Percentile(q) if !(q > 0.0 && q <= 100.0) => {
                return Err(format!("percentile must be in (0, 100], got {q}"));
            }
            WindowOp::TrimmedMean(q) if !(0.0..50.0).contains(&q) => {
                return Err(format!("trim fraction must be in [0, 50), got {q}"));
            }
            _ => {}
        }
        if Self::validate_window(start, stop, step, window)? == 0 {
            return Ok(Vec::new());
        }
        let _transition = self.transition_read();
        let samples =
            self.query_range_by_id_inner(series_id, start.saturating_sub(window), stop)?;
        Ok(Self::window_op_walk(
            &samples, start, stop, step, window, op,
        ))
    }

    /// Evaluate one window operation for an ordered batch of series without
    /// Rayon. This is the callback-safe host/SQLite primitive: validation and
    /// the transition read guard are paid once, while results retain the input
    /// series-id order (including empty vectors).
    pub fn query_window_op_batch_by_id(
        &self,
        series_ids: &[i64],
        start: i64,
        stop: i64,
        step: i64,
        window: i64,
        op: WindowOp,
    ) -> EngineResult<Vec<(i64, Vec<(i64, f64)>)>> {
        self.query_window_op_batch_by_id_inner(series_ids, start, stop, step, window, op, None)
    }

    /// Bounded form of [`Self::query_window_op_batch_by_id`]. Both the
    /// conservative input points inspected and the maximum grid points that
    /// could be materialized are independently capped by the inclusive
    /// `max_work_points` value before chunk payloads are read.
    #[allow(clippy::too_many_arguments)]
    pub fn query_window_op_batch_by_id_limited(
        &self,
        series_ids: &[i64],
        start: i64,
        stop: i64,
        step: i64,
        window: i64,
        op: WindowOp,
        max_work_points: u64,
    ) -> EngineResult<Vec<(i64, Vec<(i64, f64)>)>> {
        self.query_window_op_batch_by_id_inner(
            series_ids,
            start,
            stop,
            step,
            window,
            op,
            Some(max_work_points),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn query_window_op_batch_by_id_inner(
        &self,
        series_ids: &[i64],
        start: i64,
        stop: i64,
        step: i64,
        window: i64,
        op: WindowOp,
        max_work_points: Option<u64>,
    ) -> EngineResult<Vec<(i64, Vec<(i64, f64)>)>> {
        let started = Instant::now();
        match op {
            WindowOp::Percentile(q) if !(q > 0.0 && q <= 100.0) => {
                return Err(format!("percentile must be in (0, 100], got {q}"));
            }
            WindowOp::TrimmedMean(q) if !(0.0..50.0).contains(&q) => {
                return Err(format!("trim fraction must be in [0, 50), got {q}"));
            }
            _ => {}
        }
        let grid_points = Self::validate_window(start, stop, step, window)?;
        if grid_points == 0 {
            self.record_window_batch_query(started, series_ids.len(), 0, 0, 0, 0, 0);
            return Ok(series_ids.iter().map(|&sid| (sid, Vec::new())).collect());
        }
        let possible_output_points = (series_ids.len() as u64).saturating_mul(grid_points as u64);
        if let Some(limit) = max_work_points.filter(|limit| possible_output_points > *limit) {
            self.record_window_batch_query(started, series_ids.len(), 0, 0, 0, 0, 0);
            return Err(format!(
                "window batch work point limit {limit} exceeded (possible output points: {possible_output_points})"
            ));
        }

        let _transition = self.transition_read();
        let range_start = start.saturating_sub(window);
        let matching: Vec<Vec<ChunkMeta>> = {
            let index = self.index_read();
            series_ids
                .iter()
                .map(|&sid| {
                    let pk = PartitionKey { series_id: sid };
                    index
                        .range((pk, i64::MIN, u64::MIN)..)
                        .take_while(|((key, _, _), _)| key == &pk)
                        .filter(|(_, meta)| meta.min_ts <= stop && meta.max_ts >= range_start)
                        .map(|(_, meta)| meta.clone())
                        .collect()
                })
                .collect()
        };
        let locs: Vec<ChunkLoc> = matching
            .iter()
            .flat_map(|chunks| chunks.iter().map(|meta| meta.loc.clone()))
            .collect();
        let decoded_points = matching
            .iter()
            .flat_map(|chunks| chunks.iter())
            .map(|meta| u64::from(meta.point_count))
            .sum::<u64>();
        let preflight_buffered_points = series_ids.iter().fold(0_u64, |total, &series_id| {
            let pk = PartitionKey { series_id };
            total.saturating_add(
                self.partitions
                    .get(&pk)
                    .map_or(0, |buffer| buffer.timestamps.len() as u64),
            )
        });
        let preflight_work_points = decoded_points.saturating_add(preflight_buffered_points);
        if let Some(limit) = max_work_points.filter(|limit| preflight_work_points > *limit) {
            self.record_window_batch_query(
                started,
                series_ids.len(),
                locs.len(),
                0,
                decoded_points,
                preflight_buffered_points,
                0,
            );
            return Err(format!(
                "window batch work point limit {limit} exceeded (candidate input points: {preflight_work_points})"
            ));
        }
        let chunk_bytes = self.store.read_chunks(&locs)?;
        if chunk_bytes.len() != locs.len() {
            return Err(format!(
                "batch chunk read returned {} payloads for {} locations",
                chunk_bytes.len(),
                locs.len()
            ));
        }

        let payload_bytes = chunk_bytes
            .iter()
            .map(|bytes| bytes.ts().len().saturating_add(bytes.val().len()))
            .sum::<usize>();
        let candidate_chunks = locs.len();
        let mut payloads = chunk_bytes.into_iter();
        let mut result = Vec::with_capacity(series_ids.len());
        let mut buffered_points_considered = 0_u64;
        let mut returned_points = 0_u64;
        for (&sid, chunks) in series_ids.iter().zip(matching) {
            let mut samples = Vec::new();
            for meta in chunks {
                let bytes = payloads
                    .next()
                    .ok_or_else(|| "batch chunk payload order underflow".to_string())?;
                samples.extend(Self::decode_chunk_data(&meta, &bytes, range_start, stop)?);
            }

            let pk = PartitionKey { series_id: sid };
            if let Some(buf) = self.partitions.get(&pk) {
                buffered_points_considered =
                    buffered_points_considered.saturating_add(buf.timestamps.len() as u64);
                let observed_work_points =
                    decoded_points.saturating_add(buffered_points_considered);
                if let Some(limit) = max_work_points.filter(|limit| observed_work_points > *limit) {
                    self.record_window_batch_query(
                        started,
                        series_ids.len(),
                        candidate_chunks,
                        payload_bytes,
                        decoded_points,
                        buffered_points_considered,
                        returned_points,
                    );
                    return Err(format!(
                        "window batch work point limit {limit} exceeded (candidate input points: {observed_work_points})"
                    ));
                }
                for index in 0..buf.timestamps.len() {
                    let timestamp = buf.timestamps[index];
                    if timestamp >= range_start && timestamp <= stop {
                        samples.push((timestamp, buf.values[index]));
                    }
                }
            }
            samples.sort_by_key(|&(timestamp, _)| timestamp);
            let points = Self::window_op_walk(&samples, start, stop, step, window, op);
            returned_points = returned_points.saturating_add(points.len() as u64);
            result.push((sid, points));
        }
        debug_assert!(payloads.next().is_none());
        self.record_window_batch_query(
            started,
            series_ids.len(),
            candidate_chunks,
            payload_bytes,
            decoded_points,
            buffered_points_considered,
            returned_points,
        );
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_window_batch_query(
        &self,
        started: Instant,
        series_considered: usize,
        candidate_chunks: usize,
        payload_bytes: usize,
        decoded_points: u64,
        buffered_points_considered: u64,
        returned_points: u64,
    ) {
        let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.window_batch_query_count
            .fetch_add(1, Ordering::Relaxed);
        self.window_batch_query_total_ns
            .fetch_add(elapsed, Ordering::Relaxed);
        self.window_batch_query_series_considered.fetch_add(
            u64::try_from(series_considered).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.window_batch_query_candidate_chunks.fetch_add(
            u64::try_from(candidate_chunks).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.window_batch_query_payload_bytes_read.fetch_add(
            u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.window_batch_query_decoded_points
            .fetch_add(decoded_points, Ordering::Relaxed);
        self.window_batch_query_buffered_points_considered
            .fetch_add(buffered_points_considered, Ordering::Relaxed);
        self.window_batch_query_returned_points
            .fetch_add(returned_points, Ordering::Relaxed);
    }

    /// Q2(b), single series, rayon-free — safe from vtab callbacks.
    pub fn query_window_agg_by_id(
        &self,
        series_id: i64,
        start: i64,
        stop: i64,
        step: i64,
        window: i64,
        agg: AggFn,
    ) -> EngineResult<Vec<(i64, f64)>> {
        if Self::validate_window(start, stop, step, window)? == 0 {
            return Ok(Vec::new());
        }
        let _transition = self.transition_read();
        let samples =
            self.query_range_by_id_inner(series_id, start.saturating_sub(window), stop)?;
        Ok(Self::window_op_walk(
            &samples,
            start,
            stop,
            step,
            window,
            WindowOp::Agg(agg),
        ))
    }

    /// Q2(a): last sample per grid point, all matching series, parallel.
    /// For each t in start, start+step, ..= stop returns the newest
    /// sample with ts in (t - lookback, t]; grid points with no sample
    /// produce no row. Embedded callers only — NOT vtab-callback-safe
    /// (rayon; see query_grid_last_by_id).
    pub fn query_grid_last(
        &self,
        metric_name: &str,
        label_filter: &Labels,
        start: i64,
        stop: i64,
        step: i64,
        lookback: i64,
    ) -> EngineResult<Vec<(Labels, Vec<(i64, f64)>)>> {
        if Self::validate_grid_last(start, stop, step, lookback)? == 0 {
            return Ok(Vec::new());
        }
        let _transition = self.transition_read();
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
                let samples =
                    self.query_range_by_id_inner(sid, start.saturating_sub(lookback), stop)?;
                let points = Self::grid_last_walk(&samples, start, stop, step, lookback);
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

    /// Q2(b): sliding-window aggregate per grid point, all matching
    /// series, parallel. Embedded callers only — NOT vtab-callback-safe
    /// (rayon; see query_window_agg_by_id).
    pub fn query_window_agg(
        &self,
        metric_name: &str,
        label_filter: &Labels,
        start: i64,
        stop: i64,
        step: i64,
        window: i64,
        agg: AggFn,
    ) -> EngineResult<Vec<(Labels, Vec<(i64, f64)>)>> {
        if Self::validate_window(start, stop, step, window)? == 0 {
            return Ok(Vec::new());
        }
        let _transition = self.transition_read();
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
                let samples =
                    self.query_range_by_id_inner(sid, start.saturating_sub(window), stop)?;
                let points =
                    Self::window_op_walk(&samples, start, stop, step, window, WindowOp::Agg(agg));
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

    // ── F3 rollup ladder (FEATURE_PLAN.md) ───────────────────────────

    fn replace_rollup_index(&self, stored: Vec<StoredRollupChunk>) {
        let mut rollups = self.rollup_write();
        rollups.clear();
        for chunk in stored {
            let key = (
                PartitionKey {
                    series_id: chunk.series_id,
                },
                chunk.resolution,
                chunk.meta.min_ts,
                self.next_chunk_seq(),
            );
            rollups.insert(key, chunk.meta);
        }
    }

    /// Configure the ladder (idempotent per connect, like set_retention).
    pub fn set_rollups(&self, tiers: Vec<RollupTier>) {
        *self.rollup_tiers.lock().unwrap_or_else(|e| e.into_inner()) = tiers;
    }

    pub fn rollup_tiers(&self) -> Vec<RollupTier> {
        self.rollup_tiers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Max queryable RAW ts (chunk index + buffers) — the data-time
    /// reference for retention and rollup settling.
    fn raw_high_water(&self) -> Option<i64> {
        let mut high_water = i64::MIN;
        {
            let index = self.index_read();
            for meta in index.values() {
                high_water = high_water.max(meta.max_ts);
            }
        }
        for entry in self.partitions.iter() {
            if let Some(&mx) = entry.value().timestamps.iter().max() {
                high_water = high_water.max(mx);
            }
        }
        (high_water != i64::MIN).then_some(high_water)
    }

    /// Produce rollup chunks for every tier: per series, buckets from
    /// the per-(series, tier) watermark up to the SETTLE margin (one
    /// full bucket width below the raw high-water mark). Append-only and
    /// idempotent by construction — the watermark is the max coverage
    /// end already persisted, so re-running produces nothing new.
    /// DOCUMENTED v1 LIMIT: samples arriving later than the settle
    /// margin are never re-rolled (they stay queryable in raw until raw
    /// retention).
    ///
    /// Returns (chunks written, buckets written). Store writes happen
    /// with NO engine locks held (invariant 1); index recording is
    /// journaled for rollback.
    pub fn rollup(&self) -> EngineResult<(usize, usize)> {
        let tiers = self.rollup_tiers();
        if tiers.is_empty() {
            return Ok((0, 0));
        }
        let Some(high_water) = self.raw_high_water() else {
            return Ok((0, 0));
        };

        let series_ids: Vec<i64> = {
            let index = self.index_read();
            let mut ids: Vec<i64> = index.keys().map(|(pk, _, _)| pk.series_id).collect();
            ids.dedup();
            ids.sort_unstable();
            ids.dedup();
            ids
        };

        let mut batch: Vec<EncodedRollupChunk> = Vec::new();
        for tier in &tiers {
            let r = tier.resolution;
            // Eligible buckets: B + R - 1 <= high_water - R (one full
            // bucket of settle). produce_to is that last coverage end.
            let Some(limit) = high_water.checked_sub(r) else {
                continue;
            };
            let last_bucket = match limit.checked_sub(r - 1) {
                Some(x) => x.div_euclid(r) * r,
                None => continue,
            };
            let produce_to = last_bucket + r - 1;
            for &sid in &series_ids {
                let pk = PartitionKey { series_id: sid };
                let watermark = {
                    let rollups = self.rollup_read();
                    rollups
                        .range((pk, r, i64::MIN, u64::MIN)..=(pk, r, i64::MAX, u64::MAX))
                        .map(|(_, meta)| meta.max_ts)
                        .max()
                };
                let start = match watermark {
                    Some(w) if w >= produce_to => continue,
                    Some(w) => w.saturating_add(1),
                    None => i64::MIN,
                };
                let samples = self.query_range_by_id(sid, start, produce_to)?;
                if samples.is_empty() {
                    continue;
                }
                let buckets = rollup_buckets(&samples, r);
                let payload = encode_rollup_payload(&buckets)?;
                batch.push(EncodedRollupChunk {
                    series_id: sid,
                    resolution: r,
                    min_ts: buckets[0].bucket_ts,
                    max_ts: buckets[buckets.len() - 1].bucket_ts + r - 1,
                    bucket_count: buckets.len() as u32,
                    payload,
                });
            }
        }
        if batch.is_empty() {
            return Ok((0, 0));
        }

        // NO engine locks here: multi-row store DML can re-enter the
        // vtab's savepoint hooks (invariant 1).
        let locs = self
            .store
            .put_rollup_chunks(&batch)
            .map_err(|err| format!("rollup chunk write failed: {err}"))?;
        if locs.len() != batch.len() {
            return Err(format!(
                "rollup store wrote {} of {} chunks",
                locs.len(),
                batch.len()
            ));
        }

        let buckets_total: usize = batch.iter().map(|c| c.bucket_count as usize).sum();
        let mut journal = self.txn_guard();
        let mut rollups = self.rollup_write();
        for (chunk, loc) in batch.iter().zip(locs) {
            let key = (
                PartitionKey {
                    series_id: chunk.series_id,
                },
                chunk.resolution,
                chunk.min_ts,
                self.next_chunk_seq(),
            );
            let meta = ChunkMeta {
                min_ts: chunk.min_ts,
                max_ts: chunk.max_ts,
                max_ts_val: None,
                point_count: chunk.bucket_count,
                min_val: 0.0,
                max_val: 0.0,
                sum_val: 0.0,
                loc,
                encoding: ENC_ROLLUP_V1,
            };
            rollups.insert(key, meta);
            if let Some(journal) = journal.as_deref_mut() {
                journal.rollup_added.insert(key);
            }
        }
        Ok((batch.len(), buckets_total))
    }

    /// Read rolled buckets for one series/tier overlapping [start, stop].
    /// Sequential and rayon-free — vtab-callback safe. Only SETTLED
    /// (rolled) buckets are returned; the raw tail is raw's job.
    pub fn query_rollup_by_id(
        &self,
        series_id: i64,
        resolution: i64,
        start: i64,
        stop: i64,
    ) -> EngineResult<Vec<RollupBucket>> {
        if resolution <= 0 {
            return Err(format!("resolution must be positive, got {resolution}"));
        }
        let _transition = self.transition_read();
        let pk = PartitionKey { series_id };
        let metas: Vec<ChunkMeta> = {
            let rollups = self.rollup_read();
            rollups
                .range((pk, resolution, i64::MIN, u64::MIN)..=(pk, resolution, i64::MAX, u64::MAX))
                .filter(|(_, meta)| meta.min_ts <= stop && meta.max_ts >= start)
                .map(|(_, meta)| meta.clone())
                .collect()
        };
        let mut out = Vec::new();
        for meta in metas {
            let bytes = self
                .store
                .read_chunk(&meta.loc)
                .map_err(|err| format!("rollup chunk read failed: {err}"))?;
            let payload = &bytes.data[bytes.ts_range.clone()];
            let buckets = decode_rollup_payload(payload)?;
            out.extend(buckets.into_iter().filter(|b| {
                b.bucket_ts <= stop && b.bucket_ts.saturating_add(resolution - 1) >= start
            }));
        }
        Ok(out)
    }

    /// Read one explicit rollup tier for an ordered series batch. This is the
    /// callback-safe packed-TVF primitive: one transition guard and one store
    /// batch read replace a separate SQLite chunk lookup per series. Results
    /// retain the input series-id order, including empty vectors.
    pub fn query_rollup_batch_by_id(
        &self,
        series_ids: &[i64],
        resolution: i64,
        start: i64,
        stop: i64,
    ) -> EngineResult<Vec<(i64, Vec<RollupBucket>)>> {
        if resolution <= 0 {
            return Err(format!("resolution must be positive, got {resolution}"));
        }

        let _transition = self.transition_read();
        let matching: Vec<Vec<ChunkMeta>> = {
            let rollups = self.rollup_read();
            series_ids
                .iter()
                .map(|&series_id| {
                    let pk = PartitionKey { series_id };
                    rollups
                        .range(
                            (pk, resolution, i64::MIN, u64::MIN)
                                ..=(pk, resolution, i64::MAX, u64::MAX),
                        )
                        .filter(|(_, meta)| meta.min_ts <= stop && meta.max_ts >= start)
                        .map(|(_, meta)| meta.clone())
                        .collect()
                })
                .collect()
        };
        let locs: Vec<ChunkLoc> = matching
            .iter()
            .flat_map(|chunks| chunks.iter().map(|meta| meta.loc.clone()))
            .collect();
        let chunk_bytes = self
            .store
            .read_chunks(&locs)
            .map_err(|err| format!("rollup chunk read failed: {err}"))?;
        if chunk_bytes.len() != locs.len() {
            return Err(format!(
                "batch rollup read returned {} payloads for {} locations",
                chunk_bytes.len(),
                locs.len()
            ));
        }

        let mut payloads = chunk_bytes.into_iter();
        let mut result = Vec::with_capacity(series_ids.len());
        for (&series_id, chunks) in series_ids.iter().zip(matching) {
            let mut buckets = Vec::new();
            for _meta in chunks {
                let bytes = payloads
                    .next()
                    .ok_or_else(|| "batch rollup payload order underflow".to_string())?;
                let payload = &bytes.data[bytes.ts_range.clone()];
                buckets.extend(
                    decode_rollup_payload(payload)?
                        .into_iter()
                        .filter(|bucket| {
                            bucket.bucket_ts <= stop
                                && bucket.bucket_ts.saturating_add(resolution - 1) >= start
                        }),
                );
            }
            result.push((series_id, buckets));
        }
        debug_assert!(payloads.next().is_none());
        Ok(result)
    }

    /// Per-tier retention: drop rollup chunks whose coverage ended
    /// before `cutoff`. Mirrors delete_before's structure (transition
    /// exclusive, journaled removals, rows deleted in the caller's
    /// transaction).
    fn delete_rollups_before(&self, resolution: i64, cutoff: i64) -> (usize, Vec<String>) {
        let _transition = self.transition_write();
        let mut journal = self.txn_guard();
        let mut rollups = self.rollup_write();
        let victims: Vec<(RollupKey, ChunkMeta)> = rollups
            .iter()
            .filter(|((_, res, _, _), meta)| *res == resolution && meta.max_ts < cutoff)
            .map(|(k, m)| (*k, m.clone()))
            .collect();
        if victims.is_empty() {
            return (0, Vec::new());
        }
        let locs: Vec<ChunkLoc> = victims.iter().map(|(_, m)| m.loc.clone()).collect();
        let errors = self.store.delete_chunks(&locs);
        if !errors.is_empty() {
            return (0, errors);
        }
        for (key, meta) in &victims {
            rollups.remove(key);
            if let Some(journal) = journal.as_deref_mut() {
                if !journal.rollup_added.remove(key) {
                    journal.rollup_removed.push((*key, meta.clone()));
                }
            }
        }
        (victims.len(), Vec::new())
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
        Self::decode_chunk_data(meta, &bytes, t_start, t_end)
    }

    fn decode_chunk_data(
        meta: &ChunkMeta,
        bytes: &ChunkBytes,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, f64)>, String> {
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

    /// F2: configure the automatic retention window (NATIVE ts units;
    /// None disables). Idempotent — called at every connect with the
    /// persisted table setting.
    pub fn set_retention(&self, native: Option<i64>) {
        self.retention_native
            .store(native.unwrap_or(0).max(0), Ordering::Relaxed);
    }

    /// Apply the configured retention window, if any. Cutoff is DATA
    /// time: (max queryable ts) - retention — deterministic, replay-safe,
    /// and inert for pure backfills (invariant 5 in FEATURE_PLAN.md).
    /// The high-water mark is derived on demand from the chunk index +
    /// buffers, so the write hot path pays nothing.
    ///
    /// Called by the maintenance wrappers AFTER their transition guard
    /// is released (delete_before takes it again); must never run under
    /// an engine lock. Manual `prune:<ts>` is unaffected.
    pub fn apply_retention(&self) -> EngineResult<usize> {
        let retention = self.retention_native.load(Ordering::Relaxed);
        let tiers: Vec<RollupTier> = self
            .rollup_tiers()
            .into_iter()
            .filter(|t| t.retention > 0)
            .collect();
        if retention == 0 && tiers.is_empty() {
            return Ok(0);
        }
        let Some(high_water) = self.raw_high_water() else {
            return Ok(0); // empty table
        };
        // One advance guard for raw + all tiers, tracking the high-water
        // mark itself; the slice is 1/16 of the SMALLEST active window.
        let window_min = std::iter::once(retention)
            .filter(|&r| r > 0)
            .chain(tiers.iter().map(|t| t.retention))
            .min()
            .expect("at least one active window");
        let slice = (window_min / 16).max(1);
        let floor = self.retention_floor.load(Ordering::Relaxed);
        if floor != i64::MIN && high_water < floor.saturating_add(slice) {
            return Ok(0); // hasn't advanced meaningfully since last time
        }
        let mut pruned = 0usize;
        if retention > 0 {
            let cutoff = high_water.saturating_sub(retention);
            let (chunks, _units, errors) = self.delete_before(cutoff);
            if !errors.is_empty() {
                return Err(format!("retention prune failed: {}", errors.join("; ")));
            }
            pruned += chunks;
        }
        for tier in &tiers {
            let cutoff = high_water.saturating_sub(tier.retention);
            let (chunks, errors) = self.delete_rollups_before(tier.resolution, cutoff);
            if !errors.is_empty() {
                return Err(format!(
                    "rollup tier {} retention prune failed: {}",
                    tier.resolution,
                    errors.join("; ")
                ));
            }
            pruned += chunks;
        }
        self.retention_floor.store(high_water, Ordering::Relaxed);
        Ok(pruned)
    }

    pub fn delete_before(&self, before_ts: i64) -> (usize, usize, Vec<String>) {
        let _transition = self.transition_write();
        // Transition is already exclusive; journal before index. Pruned rows are
        // DELETEd through the store inside the host transaction, so a
        // rollback restores them — the journal restores the matching
        // index entries. Entries added by this same txn cancel instead
        // (their rows will not come back).
        let mut j = self.txn_guard();
        let mut index = self.index_write();

        let to_remove: Vec<ChunkKey> = index
            .iter()
            .filter(|(_, meta)| meta.max_ts < before_ts)
            .map(|(k, _)| *k)
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

    // ── Authoritative state recovery/refresh ─────────────────────────

    /// Read the authoritative store token for transaction publication.
    /// SQLite hosts call this from xSync, while the calling connection still
    /// sees its own final transactional state and excludes other writers.
    /// Non-authoritative stores return None and keep the reload fallback.
    pub fn capture_catalog_generation(&self) -> EngineResult<Option<(i64, i64)>> {
        if !self.authoritative_series {
            return Ok(None);
        }
        self.store
            .catalog_generation()
            .map_err(|err| format!("failed to capture catalog generation: {err}"))
    }

    fn validate_chunk_series(
        registry: &SeriesRegistry,
        chunks: &[StoredChunk],
    ) -> EngineResult<()> {
        for chunk in chunks {
            if registry.info_for(chunk.series_id).is_none() {
                return Err(format!(
                    "persisted chunk {:?} references unknown series {}",
                    chunk.meta.loc, chunk.series_id
                ));
            }
        }
        Ok(())
    }

    fn replace_index(&self, stored: Vec<StoredChunk>) {
        let mut index = self.index_write();
        index.clear();
        for chunk in stored {
            let key = PartitionKey {
                series_id: chunk.series_id,
            };
            // Recovery assigns fresh sequence numbers while scanning —
            // same as the donor fix (the seq is in-memory only, never
            // persisted), so duplicate-min_ts chunks on disk can never
            // shadow each other after a restart either.
            index.insert((key, chunk.meta.min_ts, self.next_chunk_seq()), chunk.meta);
        }
    }

    /// Refresh process-local catalog and chunk snapshots from a store that
    /// may be changed by another process. Callers bind the host SQLite
    /// connection first, so both scans observe that connection's transaction
    /// snapshot. Active writer transactions retain their journaled view.
    pub fn refresh_authoritative_state(&self) -> EngineResult<()> {
        if !self.authoritative_series {
            return Ok(());
        }

        // Fast path: skip the reload when the store's catalog generation
        // matches the one the last reload observed. The unlocked
        // txn_active pre-check mirrors the locked check below (an active
        // writer keeps its journaled view either way); a raced skip is
        // equivalent to the pre-existing early return. Reading the
        // generation BEFORE the reload below keeps a concurrent commit
        // safe: at worst we cache a stale token and reload once more.
        if self.txn_active.load(Ordering::SeqCst) {
            return Ok(());
        }
        let observed = self
            .store
            .catalog_generation()
            .map_err(|err| format!("failed to read catalog generation: {err}"))?;
        if observed.is_some() && observed == *self.catalog_gen_lock() {
            return Ok(());
        }

        let _transition = self.transition_write();
        let _journal = self.txn_lock();
        if self.txn_active.load(Ordering::SeqCst) {
            return Ok(());
        }

        // P2: pure-append delta. If the shape generation is unchanged
        // since the last reload, every change is appended rows — apply
        // only those. Any doubt (no cached tokens, shape changed, store
        // can't answer, or a delta-application error) falls through to
        // the full reload, which is always correct.
        let wm_new = self
            .store
            .append_watermark()
            .map_err(|err| format!("failed to read append watermark: {err}"))?;
        let cached_gen = *self.catalog_gen_lock();
        let cached_wm = *self.append_wm_lock();
        if let (Some(old_gen), Some(old_wm), Some(new_wm)) = (cached_gen, cached_wm, wm_new) {
            if new_wm.0 == old_wm.0 {
                match self.apply_append_delta(old_gen.0, old_wm.1, observed, new_wm) {
                    Ok(()) => return Ok(()),
                    // Partial application is harmless: the reload below
                    // replaces registry and indexes wholesale.
                    Err(_) => {}
                }
            }
        }

        let rows = self
            .store
            .load_series()
            .map_err(|err| format!("failed to refresh series catalog: {err}"))?;
        let registry = SeriesRegistry::from_stored(&rows)
            .map_err(|err| format!("refreshed series catalog is invalid: {err}"))?;
        let chunks = self
            .store
            .scan()
            .map_err(|err| format!("failed to refresh chunk index: {err}"))?;
        Self::validate_chunk_series(&registry, &chunks)?;
        let rollup_chunks = self
            .store
            .scan_rollups()
            .map_err(|err| format!("failed to refresh rollup index: {err}"))?;

        let mut new_index = BTreeMap::new();
        for chunk in chunks {
            let key = PartitionKey {
                series_id: chunk.series_id,
            };
            new_index.insert((key, chunk.meta.min_ts, self.next_chunk_seq()), chunk.meta);
        }

        // Lock order remains transition -> txn -> index -> series. Holding
        // both write locks prevents readers from pairing a newly visible
        // series with an old chunk snapshot.
        let mut new_rollups = BTreeMap::new();
        for chunk in rollup_chunks {
            let key = (
                PartitionKey {
                    series_id: chunk.series_id,
                },
                chunk.resolution,
                chunk.meta.min_ts,
                self.next_chunk_seq(),
            );
            new_rollups.insert(key, chunk.meta);
        }

        // Lock order: transition → txn → index → series → rollup (other
        // paths only ever hold ONE of these at a time).
        let mut index = self.index_write();
        let mut series = self.series_write();
        let mut rollups = self.rollup_write();
        *index = new_index;
        *series = registry;
        *rollups = new_rollups;
        self.resolve_cache.clear();
        *self.catalog_gen_lock() = observed;
        *self.append_wm_lock() = self
            .store
            .append_watermark()
            .map_err(|err| format!("failed to refresh append watermark: {err}"))
            // Read AFTER the reload: if a commit slipped in between, the
            // watermark is newer than the snapshot — but so is the
            // store's generation vs `observed`, so the next refresh
            // reloads again and re-primes. Conservative either way.
            .unwrap_or(None);
        Ok(())
    }

    /// P2: apply a pure-append delta — series rows past the cached
    /// catalog max-id, chunk/rollup rows past the cached rowid
    /// watermark. Caller holds the transition and txn locks and has
    /// proven the shape generation unchanged (no deletes → no rowid
    /// reuse). Application is idempotent by ChunkLoc because a writer's
    /// own flushes advance the store past its cached watermark: rows we
    /// already indexed are skipped, not duplicated.
    fn apply_append_delta(
        &self,
        after_series_id: i64,
        after_rowid: i64,
        observed: Option<(i64, i64)>,
        new_wm: (i64, i64),
    ) -> EngineResult<()> {
        let new_series = self
            .store
            .load_series_since(after_series_id)
            .map_err(|err| format!("failed to load series delta: {err}"))?;
        let (raw, rollup_chunks) = self
            .store
            .scan_since(after_rowid)
            .map_err(|err| format!("failed to scan chunk delta: {err}"))?;

        // Same acquisition order as the full reload: index → series →
        // rollup, under the caller's transition + txn locks.
        let mut index = self.index_write();
        let mut series = self.series_write();
        let mut rollups = self.rollup_write();
        for row in &new_series {
            let labels: Labels = row.labels.iter().cloned().collect();
            // insert_known is idempotent for identical rows and loud on
            // identity conflicts — exactly the semantics a re-read
            // series row needs.
            series.insert_known(row.id, &row.name, &labels, false)?;
        }
        for chunk in raw {
            let pk = PartitionKey {
                series_id: chunk.series_id,
            };
            if series.info_for(chunk.series_id).is_none() {
                return Err(format!(
                    "appended chunk {:?} references unknown series {}",
                    chunk.meta.loc, chunk.series_id
                ));
            }
            let known = index
                .range((pk, i64::MIN, u64::MIN)..)
                .take_while(|((key, _, _), _)| key == &pk)
                .any(|(_, meta)| meta.loc == chunk.meta.loc);
            if known {
                continue;
            }
            index.insert((pk, chunk.meta.min_ts, self.next_chunk_seq()), chunk.meta);
        }
        for chunk in rollup_chunks {
            let pk = PartitionKey {
                series_id: chunk.series_id,
            };
            let known = rollups
                .range((pk, chunk.resolution, i64::MIN, u64::MIN)..)
                .take_while(|((key, res, _, _), _)| key == &pk && *res == chunk.resolution)
                .any(|(_, meta)| meta.loc == chunk.meta.loc);
            if known {
                continue;
            }
            rollups.insert(
                (pk, chunk.resolution, chunk.meta.min_ts, self.next_chunk_seq()),
                chunk.meta,
            );
        }
        *self.catalog_gen_lock() = observed;
        *self.append_wm_lock() = Some(new_wm);
        Ok(())
    }

    /// F1 catalog: one row per known series with chunk-index aggregates
    /// and buffered-state — NO chunk decompression, so it stays cheap at
    /// any table size. Locks are taken strictly one at a time (index,
    /// then partitions, then registry) so no ordering hazard can exist.
    /// min/max ts include buffered points: the catalog describes what a
    /// query would see, not just what is durable.
    pub fn series_overview(&self) -> Vec<SeriesOverview> {
        let _transition = self.transition_read();

        let mut chunk_agg: HashMap<i64, (i64, i64, u64, usize)> = HashMap::new();
        {
            let index = self.index_read();
            for ((pk, _, _), meta) in index.iter() {
                let entry =
                    chunk_agg
                        .entry(pk.series_id)
                        .or_insert((meta.min_ts, meta.max_ts, 0, 0));
                entry.0 = entry.0.min(meta.min_ts);
                entry.1 = entry.1.max(meta.max_ts);
                entry.2 += meta.point_count as u64;
                entry.3 += 1;
            }
        }

        let mut buf_agg: HashMap<i64, (usize, i64, i64)> = HashMap::new();
        for entry in self.partitions.iter() {
            let buf = entry.value();
            let (Some(&mn), Some(&mx)) = (buf.timestamps.iter().min(), buf.timestamps.iter().max())
            else {
                continue;
            };
            buf_agg.insert(entry.key().series_id, (buf.timestamps.len(), mn, mx));
        }

        let reg = self.series_read();
        let mut out: Vec<SeriesOverview> = reg
            .series_map
            .iter()
            .map(|((name, labels), &series_id)| {
                let chunks = chunk_agg.get(&series_id);
                let buffered = buf_agg.get(&series_id);
                let min_ts = match (chunks.map(|c| c.0), buffered.map(|b| b.1)) {
                    (Some(c), Some(b)) => Some(c.min(b)),
                    (c, b) => c.or(b),
                };
                let max_ts = match (chunks.map(|c| c.1), buffered.map(|b| b.2)) {
                    (Some(c), Some(b)) => Some(c.max(b)),
                    (c, b) => c.or(b),
                };
                SeriesOverview {
                    series_id,
                    name: name.clone(),
                    labels: labels.clone(),
                    min_ts,
                    max_ts,
                    disk_points: chunks.map_or(0, |c| c.2),
                    chunks: chunks.map_or(0, |c| c.3),
                    buffered: buffered.map_or(0, |b| b.0),
                }
            })
            .collect();
        out.sort_by(|a, b| (&a.name, a.series_id).cmp(&(&b.name, b.series_id)));
        out
    }

    /// Catalog rows for one metric and an indexed equality subset.
    ///
    /// Unlike `series_overview`, this walks chunk-index ranges and buffers
    /// only for candidate series. SQL discovery TVFs apply regex/negative
    /// matchers to these rows before crossing the host boundary, so a
    /// selective catalog query does not pay O(all chunks) first.
    pub fn series_overview_matching(
        &self,
        metric_name: &str,
        label_filter: &Labels,
    ) -> Vec<SeriesOverview> {
        let _transition = self.transition_read();
        let series_ids = self.series_read().find_series(metric_name, label_filter);
        self.series_overview_by_ids_inner(&series_ids)
    }

    /// Catalog rows for an already selected series-id set. This lets an SQL
    /// host apply regex/negative matchers to registry labels before it asks
    /// the engine to aggregate chunk and buffer metadata.
    pub fn series_overview_by_ids(&self, series_ids: &[i64]) -> Vec<SeriesOverview> {
        let _transition = self.transition_read();
        self.series_overview_by_ids_inner(series_ids)
    }

    fn series_overview_by_ids_inner(&self, series_ids: &[i64]) -> Vec<SeriesOverview> {
        let candidates: Vec<(i64, String, Labels)> = {
            let reg = self.series_read();
            series_ids
                .iter()
                .copied()
                .filter_map(|series_id| {
                    reg.info_for(series_id)
                        .map(|info| (series_id, info.metric_name.clone(), info.labels.clone()))
                })
                .collect()
        };

        let mut chunk_agg: HashMap<i64, (i64, i64, u64, usize)> = HashMap::new();
        {
            let index = self.index_read();
            for (series_id, _, _) in &candidates {
                let pk = PartitionKey {
                    series_id: *series_id,
                };
                for ((_, _, _), meta) in index
                    .range((pk, i64::MIN, u64::MIN)..)
                    .take_while(|((key, _, _), _)| key == &pk)
                {
                    let entry =
                        chunk_agg
                            .entry(*series_id)
                            .or_insert((meta.min_ts, meta.max_ts, 0, 0));
                    entry.0 = entry.0.min(meta.min_ts);
                    entry.1 = entry.1.max(meta.max_ts);
                    entry.2 += meta.point_count as u64;
                    entry.3 += 1;
                }
            }
        }

        let mut out = Vec::with_capacity(candidates.len());
        for (series_id, name, labels) in candidates {
            let chunks = chunk_agg.get(&series_id);
            let buffered = self.partitions.get(&PartitionKey { series_id });
            let buffer_count = buffered.as_ref().map_or(0, |buf| buf.timestamps.len());
            let buffer_min = buffered
                .as_ref()
                .and_then(|buf| buf.timestamps.iter().min().copied());
            let buffer_max = buffered
                .as_ref()
                .and_then(|buf| buf.timestamps.iter().max().copied());
            let min_ts = match (chunks.map(|c| c.0), buffer_min) {
                (Some(chunk), Some(buffer)) => Some(chunk.min(buffer)),
                (chunk, buffer) => chunk.or(buffer),
            };
            let max_ts = match (chunks.map(|c| c.1), buffer_max) {
                (Some(chunk), Some(buffer)) => Some(chunk.max(buffer)),
                (chunk, buffer) => chunk.or(buffer),
            };

            out.push(SeriesOverview {
                series_id,
                name,
                labels,
                min_ts,
                max_ts,
                disk_points: chunks.map_or(0, |chunk| chunk.2),
                chunks: chunks.map_or(0, |chunk| chunk.3),
                buffered: buffer_count,
            });
        }
        out.sort_by(|a, b| (&a.name, a.series_id).cmp(&(&b.name, b.series_id)));
        out
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
            prometheus_ingest_batches: self.prometheus_ingest_batches.load(Ordering::Relaxed),
            prometheus_ingest_points: self.prometheus_ingest_points.load(Ordering::Relaxed),
            prometheus_ingest_errors: self.prometheus_ingest_errors.load(Ordering::Relaxed),
            prometheus_ingest_total_ns: self.prometheus_ingest_total_ns.load(Ordering::Relaxed),
            raw_batch_query_count: self.raw_batch_query_count.load(Ordering::Relaxed),
            raw_batch_query_total_ns: self.raw_batch_query_total_ns.load(Ordering::Relaxed),
            raw_batch_query_series_considered: self
                .raw_batch_query_series_considered
                .load(Ordering::Relaxed),
            raw_batch_query_candidate_chunks: self
                .raw_batch_query_candidate_chunks
                .load(Ordering::Relaxed),
            raw_batch_query_payload_bytes_read: self
                .raw_batch_query_payload_bytes_read
                .load(Ordering::Relaxed),
            raw_batch_query_decoded_points: self
                .raw_batch_query_decoded_points
                .load(Ordering::Relaxed),
            raw_batch_query_buffered_points_considered: self
                .raw_batch_query_buffered_points_considered
                .load(Ordering::Relaxed),
            raw_batch_query_returned_points: self
                .raw_batch_query_returned_points
                .load(Ordering::Relaxed),
            window_batch_query_count: self.window_batch_query_count.load(Ordering::Relaxed),
            window_batch_query_total_ns: self.window_batch_query_total_ns.load(Ordering::Relaxed),
            window_batch_query_series_considered: self
                .window_batch_query_series_considered
                .load(Ordering::Relaxed),
            window_batch_query_candidate_chunks: self
                .window_batch_query_candidate_chunks
                .load(Ordering::Relaxed),
            window_batch_query_payload_bytes_read: self
                .window_batch_query_payload_bytes_read
                .load(Ordering::Relaxed),
            window_batch_query_decoded_points: self
                .window_batch_query_decoded_points
                .load(Ordering::Relaxed),
            window_batch_query_buffered_points_considered: self
                .window_batch_query_buffered_points_considered
                .load(Ordering::Relaxed),
            window_batch_query_returned_points: self
                .window_batch_query_returned_points
                .load(Ordering::Relaxed),
        }
    }
}

/// One catalog row from Engine::series_overview.
#[derive(Clone, Debug)]
pub struct SeriesOverview {
    pub series_id: i64,
    pub name: String,
    pub labels: Labels,
    /// Include buffered points — the queryable range, not the durable one.
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub disk_points: u64,
    pub chunks: usize,
    pub buffered: usize,
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
    pub prometheus_ingest_batches: u64,
    pub prometheus_ingest_points: u64,
    pub prometheus_ingest_errors: u64,
    pub prometheus_ingest_total_ns: u64,
    pub raw_batch_query_count: u64,
    pub raw_batch_query_total_ns: u64,
    pub raw_batch_query_series_considered: u64,
    pub raw_batch_query_candidate_chunks: u64,
    pub raw_batch_query_payload_bytes_read: u64,
    pub raw_batch_query_decoded_points: u64,
    pub raw_batch_query_buffered_points_considered: u64,
    pub raw_batch_query_returned_points: u64,
    pub window_batch_query_count: u64,
    pub window_batch_query_total_ns: u64,
    pub window_batch_query_series_considered: u64,
    pub window_batch_query_candidate_chunks: u64,
    pub window_batch_query_payload_bytes_read: u64,
    pub window_batch_query_decoded_points: u64,
    pub window_batch_query_buffered_points_considered: u64,
    pub window_batch_query_returned_points: u64,
}

fn compensated_add(increment: f64, sum: f64, compensation: f64) -> (f64, f64) {
    let total = sum + increment;
    let compensation = if total.is_infinite() {
        0.0
    } else if sum.abs() >= increment.abs() {
        compensation + ((sum - total) + increment)
    } else {
        compensation + ((increment - total) + sum)
    };
    (total, compensation)
}

/// F7 window operations (FEATURE_PLAN.md "The SQL query tier") — the
/// full timeless_window vocabulary. Definitions are pinned VERBATIM in
/// FEATURE_PLAN F7's semantic-line section; the property tests quote
/// them. Everything here is a mechanical, parameter-explicit fold —
/// notably NOT PromQL: no extrapolation, no lookback beyond the
/// window, no staleness inference.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WindowOp {
    /// The original five folds (NaN-poisoning semantics unchanged).
    Agg(AggFn),
    /// last − first (engine-order ties, same rule as grid-last).
    Delta,
    /// Σ over consecutive pairs of (`v[i] − v[i−1]`) if `v[i] ≥ v[i−1]`,
    /// else `v[i]` — the stable reset-adjustment rule. The window's
    /// first sample contributes nothing.
    Increase,
    /// increase ÷ window, per NATIVE ts unit.
    Rate,
    /// Exact NEAREST-RANK percentile, q in (0, 100]: exclude NaNs,
    /// sort by f64::total_cmp, take index ceil(q/100 × n) − 1.
    /// Empty after NaN exclusion → no row.
    Percentile(f64),
    /// Trimmed mean, q in [0, 50): after the NaN-excluded sort, drop
    /// floor(n × q/100) from EACH tail, average the rest
    /// left-to-right. Empty after trimming → no row.
    TrimmedMean(f64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFn {
    Avg,
    Sum,
    Min,
    Max,
    Count,
}

/// Chunk-aware statistics for one non-empty series range.
///
/// `sum` follows the engine's persisted-chunk accumulation order: points are
/// folded left-to-right within a chunk, then chunk sums and buffered points are
/// folded in index/insertion order. Callers that compare against a completely
/// flat point scan should use an explicit floating-point tolerance for
/// `sum`/`avg`; count is integer-exact and min/max preserve the engine rules.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AggregateSummary {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl AggregateSummary {
    pub fn count(self) -> u64 {
        self.count
    }

    pub fn value(self, agg: AggFn) -> f64 {
        match agg {
            AggFn::Avg => self.sum / self.count as f64,
            AggFn::Sum => self.sum,
            AggFn::Min => self.min,
            AggFn::Max => self.max,
            AggFn::Count => self.count as f64,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Prometheus text-format parser (bench prototype)
//
// Mirrors c_src/prometheus_nif.cpp semantics: entries are
// (name, [(label_key, label_value)], value, timestamp), timestamp 0 when
// absent, IEEE float values preserved, malformed non-comment lines counted
// as errors. Exposed as two NIFs so parse cost and term-materialization
// cost can be measured separately.
// ═══════════════════════════════════════════════════════════════════════

/// Parse a Prometheus sample value. Non-finite IEEE values are valid
/// Prometheus samples and the Rust/libSQL data plane preserves their bits.
fn parse_prom_value(bytes: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse().ok()
}

fn take_prom_quoted(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    if bytes.first() != Some(&b'"') {
        return None;
    }
    let mut index = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if index + 1 < bytes.len() => index += 2,
            b'"' => return Some((&bytes[1..index], &bytes[index + 1..])),
            _ => index += 1,
        }
    }
    None
}

fn find_prom_label_close(line: &[u8], open: usize) -> Option<usize> {
    let mut quoted = false;
    let mut index = open + 1;
    while index < line.len() {
        match line[index] {
            b'\\' if quoted && index + 1 < line.len() => index += 2,
            b'"' => {
                quoted = !quoted;
                index += 1;
            }
            b'}' if !quoted => return Some(index),
            _ => index += 1,
        }
    }
    None
}

fn trim_prom_separators(mut bytes: &[u8]) -> &[u8] {
    while bytes
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b','))
    {
        bytes = &bytes[1..];
    }
    bytes
}

/// Parse the inside of a `{key="val",key2="val2"}` label block into `out`.
/// The scanner keeps escaped bytes borrowed here. `resolve_entry` decodes the
/// three Prometheus exposition escapes only on the uncommon escaped-identity
/// path, preserving zero-allocation resolution for ordinary identities.
fn parse_prom_labels_into<'a>(mut s: &'a [u8], out: &mut Vec<(&'a [u8], &'a [u8])>) -> bool {
    loop {
        s = trim_prom_separators(s);
        if s.is_empty() {
            return true;
        }

        let (key, rest) = if s[0] == b'"' {
            let Some((key, rest)) = take_prom_quoted(s) else {
                return false;
            };
            (key, rest)
        } else {
            let Some(eq) = s.iter().position(|&b| b == b'=') else {
                return false;
            };
            let mut key = &s[..eq];
            while let [rest @ .., b' ' | b'\t'] = key {
                key = rest;
            }
            (key, &s[eq..])
        };
        if key.is_empty() {
            return false;
        }
        s = rest;
        while s.first().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
            s = &s[1..];
        }
        let Some((&b'=', rest)) = s.split_first() else {
            return false;
        };
        s = rest;
        while s.first().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
            s = &s[1..];
        }
        let Some((value, rest)) = take_prom_quoted(s) else {
            return false;
        };
        out.push((key, value));
        s = rest;
    }
}

fn unescape_prom_label_value(value: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] != b'\\' || index + 1 >= value.len() {
            output.push(value[index]);
            index += 1;
            continue;
        }
        let escaped = value[index + 1];
        match escaped {
            b'n' => output.push(b'\n'),
            b'\\' | b'"' => output.push(escaped),
            _ => {
                // Preserve unknown escapes exactly. Existing partial-success
                // ingest did not reject them; tightening malformed-line
                // policy is a separate compatibility decision.
                output.push(b'\\');
                output.push(escaped);
            }
        }
        index += 2;
    }
    output
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

    let (name, rest) = if line[0] == b'{' {
        let close = find_prom_label_close(line, 0)?;
        let inside = line[1..close].trim_ascii();
        let (name, remaining) = take_prom_quoted(inside)?;
        if name.is_empty() {
            return None;
        }
        let remaining = remaining.trim_ascii();
        if !remaining.is_empty() {
            let remaining = remaining.strip_prefix(b",")?;
            if !parse_prom_labels_into(remaining, labels) {
                return None;
            }
        }
        (name, &line[close + 1..])
    } else {
        let name_end = line
            .iter()
            .position(|&b| b == b'{' || b == b' ' || b == b'\t')?;
        if name_end == 0 {
            return None;
        }
        let name = &line[..name_end];
        let rest = if line[name_end] == b'{' {
            let close = find_prom_label_close(line, name_end)?;
            if !parse_prom_labels_into(&line[name_end + 1..close], labels) {
                return None;
            }
            &line[close + 1..]
        } else {
            &line[name_end..]
        };
        (name, rest)
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
