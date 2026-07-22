//! BlockEngine: the buffer → raw block → optimized block state machine,
//! plus the query path. One instance per logs vtab (and, in Session 6,
//! per traces vtab).
//!
//! Concurrency model: every public method takes &self and guards state
//! with Mutexes, matching the metrics Engine so a vtab cursor can hold
//! an Arc<BlockEngine> next to the table object. NOTHING in here uses
//! rayon or spawns threads — every store call happens on the caller's
//! thread. This is a hard rule (PLAN.md Session 3 lesson): store calls
//! re-enter SQLite on the host connection whose mutex the vtab callback
//! thread holds; a worker thread touching the store would deadlock.

use std::collections::BTreeSet;
use std::sync::Mutex;

use super::codec::{decode_block, encode_block, CODEC_RAW, CODEC_ZSTD};
use super::{level_name, BlockLoc, BlockMeta, BlockStore, EncodedBlock, LogEntry};

/// Tuning knobs. All ts_* values are in the SAME opaque unit as
/// LogEntry.ts — the engine never assumes seconds/millis/nanos.
pub struct BlockEngineConfig {
    /// Buffered entries that trigger an automatic flush inside push().
    pub flush_threshold: usize,
    /// zstd level for CODEC_ZSTD blocks (7 = the measured sweet spot).
    pub zstd_level: i32,
    /// optimize() aims for merged blocks of ~this many entries (the
    /// donor's merge_compaction_target_size; larger = better dictionary
    /// window, up to diminishing returns around a few thousand).
    pub merge_target_entries: usize,
    /// HARD CAP on the ts span (ts_max - ts_min) of a block produced by
    /// MERGING multiple blocks. PLAN.md "Pruning & retention": pruning
    /// deletes whole blocks by ts_max, so a merged block straddling a
    /// retention boundary would pin expired data until the entire block
    /// expires. Capping merge output at (say) one retention granule
    /// keeps prune effective. Default i64::MAX = uncapped (unit-agnostic
    /// engine can't pick a sane default); the logs vtab passes 1h in ms.
    pub merge_max_ts_span: i64,
    /// Metadata keys whose values become index terms ("key:value").
    /// SELECTIVE on purpose (the timeless_logs lesson): only stable,
    /// low-cardinality keys belong here — indexing identifier-like
    /// values (request ids...) would bloat the term table past the data.
    pub index_keys: Vec<String>,
}

impl Default for BlockEngineConfig {
    fn default() -> Self {
        BlockEngineConfig {
            flush_threshold: 8192,
            zstd_level: 7,
            merge_target_entries: 8192,
            merge_max_ts_span: i64::MAX,
            index_keys: Vec::new(),
        }
    }
}

/// One query. All filters are optional except the ts range (pass
/// i64::MIN+1 / i64::MAX-1 for "unbounded", like the metrics vtab).
pub struct LogQuery {
    pub ts_min: i64,
    pub ts_max: i64,
    /// Exact level match (0..=3).
    pub level: Option<u8>,
    /// Metadata equality filters; ALL must match. Pairs whose key is in
    /// index_keys also prune blocks via the term index; the rest are
    /// checked per-entry only.
    pub metadata_eq: Vec<(String, String)>,
    /// Case-sensitive substring match on the message.
    pub message_contains: Option<String>,
}

pub struct BlockEngine {
    store: Box<dyn BlockStore>,
    config: BlockEngineConfig,
    /// Entries pushed but not yet flushed into a block. Queryable (the
    /// same queryable-before-flush property the metrics engine has).
    buffer: Mutex<Vec<LogEntry>>,
    /// In-memory metadata index of every persisted block, rebuilt from
    /// store.scan() at construction. optimize() and prune() plan from
    /// this; the QUERY path asks the store instead (posting lists live
    /// store-side).
    index: Mutex<Vec<(BlockMeta, BlockLoc)>>,
}

impl BlockEngine {
    /// Construct over a store, recovering the block index via scan().
    /// The store is expected to be able to answer scan() immediately
    /// (in the vtab this runs re-entrantly during xCreate/xConnect,
    /// which is safe: the calling thread already holds the connection).
    pub fn new(store: Box<dyn BlockStore>, config: BlockEngineConfig) -> Result<Self, String> {
        let index = store.scan()?;
        Ok(BlockEngine {
            store,
            config,
            buffer: Mutex::new(Vec::new()),
            index: Mutex::new(index),
        })
    }

    pub fn config(&self) -> &BlockEngineConfig {
        &self.config
    }

    /// Poison-tolerant locks, same style as the rest of timeless-core:
    /// a panic while holding the lock still yields the data.
    fn buffer_lock(&self) -> std::sync::MutexGuard<'_, Vec<LogEntry>> {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn index_lock(&self) -> std::sync::MutexGuard<'_, Vec<(BlockMeta, BlockLoc)>> {
        self.index.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Append one entry to the buffer. Validates the level, sorts the
    /// metadata pairs (canonical order; duplicate keys keep the LAST
    /// value, matching JSON-parser convention), and auto-flushes when
    /// the buffer reaches flush_threshold.
    pub fn push(&self, mut entry: LogEntry) -> Result<(), String> {
        if entry.level > 3 {
            return Err(format!(
                "invalid level {} (0=debug 1=info 2=warning 3=error)",
                entry.level
            ));
        }
        // Sort by key; stable sort keeps insertion order among equal
        // keys, so "last one wins" = keep the LAST of each run.
        entry.metadata.sort_by(|a, b| a.0.cmp(&b.0));
        entry.metadata.reverse(); // last duplicates first...
        entry.metadata.dedup_by(|a, b| a.0 == b.0); // ...survive dedup
        entry.metadata.reverse(); // back to ascending key order

        let should_flush = {
            let mut buf = self.buffer_lock();
            buf.push(entry);
            buf.len() >= self.config.flush_threshold
        };
        if should_flush {
            self.flush()?;
        }
        Ok(())
    }

    pub fn buffered_count(&self) -> usize {
        self.buffer_lock().len()
    }

    /// Drain the buffer into one RAW block (codec 1 — cheap framing, no
    /// compression: flush is the ingest hot path, optimize() pays the
    /// compression bill later). No-op on an empty buffer. Returns the
    /// number of entries flushed.
    pub fn flush(&self) -> Result<usize, String> {
        // Hold the buffer lock for the whole flush so a concurrent push
        // can't slip entries between encode and clear. Single-threaded
        // in the vtab anyway; correctness is free here.
        let mut buf = self.buffer_lock();
        if buf.is_empty() {
            return Ok(0);
        }
        // Sort by ts so blocks are internally time-ordered (better
        // delta compression later, and queries stay merge-friendly).
        buf.sort_by_key(|e| e.ts);

        let (data, meta) = encode_block(&buf, CODEC_RAW, self.config.zstd_level)?;
        let terms = self.extract_terms(&buf);
        let loc = self.store.put_block(&EncodedBlock { meta, data, terms })?;
        self.index_lock().push((meta, loc));
        let n = buf.len();
        buf.clear();
        Ok(n)
    }

    /// Terms for a batch of entries: `level:<name>` always, plus
    /// `<key>:<value>` for every metadata pair whose key is in the
    /// index_keys allowlist. Deduplicated + sorted (a block-level index
    /// only cares that the term occurs at all).
    fn extract_terms(&self, entries: &[LogEntry]) -> Vec<String> {
        let mut set = BTreeSet::new();
        for e in entries {
            set.insert(format!("level:{}", level_name(e.level)));
            for (k, v) in &e.metadata {
                if self.config.index_keys.iter().any(|ik| ik == k) {
                    set.insert(format!("{k}:{v}"));
                }
            }
        }
        set.into_iter().collect()
    }

    /// The two-tier compaction pass ('optimize' command):
    ///   1. every RAW block gets recompressed to CODEC_ZSTD, and
    ///   2. small compressed blocks get MERGED into ~merge_target_entries
    ///      blocks (bigger dictionary window → better ratio), subject to
    ///      the merge_max_ts_span hard cap (see config — the retention
    ///      boundary rule).
    ///
    /// Both happen in ONE store.replace_blocks call: in the SQLite
    /// backend that means one host transaction covers the whole swap —
    /// new blocks + terms in, old blocks + terms out, atomically.
    ///
    /// Returns (blocks_removed, blocks_written).
    pub fn optimize(&self) -> Result<(usize, usize), String> {
        // Snapshot the index; plan on the copy (no lock held while we
        // read/decode block payloads).
        let mut candidates: Vec<(BlockMeta, BlockLoc)> = self
            .index_lock()
            .iter()
            .filter(|(m, _)| {
                m.codec == CODEC_RAW
                    || (m.entry_count as usize) < self.config.merge_target_entries
            })
            .copied()
            .collect();
        if candidates.is_empty() {
            return Ok((0, 0));
        }
        // Group by time locality: neighbors in ts_min order merge into
        // blocks with tight ts ranges (which is what makes both range
        // pruning and retention deletes effective).
        candidates.sort_by_key(|(m, _)| (m.ts_min, m.ts_max));

        // Greedy grouping under two constraints: target entry count and
        // the merged-span hard cap. A group is "worth rewriting" if it
        // contains any RAW block (must transition to zstd regardless) or
        // at least two blocks (an actual merge). A lone already-zstd
        // small block is left alone — rewriting it would be pure write
        // amplification for zero gain.
        let mut groups: Vec<Vec<(BlockMeta, BlockLoc)>> = Vec::new();
        let mut cur: Vec<(BlockMeta, BlockLoc)> = Vec::new();
        let mut cur_entries = 0usize;
        let (mut cur_min, mut cur_max) = (0i64, 0i64);
        for (m, loc) in candidates {
            let fits = if cur.is_empty() {
                true
            } else {
                let new_min = cur_min.min(m.ts_min);
                let new_max = cur_max.max(m.ts_max);
                // saturating_sub: spans near i64 extremes must not wrap.
                let span_ok = new_max.saturating_sub(new_min) <= self.config.merge_max_ts_span;
                let size_ok =
                    cur_entries + m.entry_count as usize <= self.config.merge_target_entries;
                span_ok && size_ok
            };
            if !fits {
                groups.push(std::mem::take(&mut cur));
                cur_entries = 0;
            }
            if cur.is_empty() {
                cur_min = m.ts_min;
                cur_max = m.ts_max;
            } else {
                cur_min = cur_min.min(m.ts_min);
                cur_max = cur_max.max(m.ts_max);
            }
            cur_entries += m.entry_count as usize;
            cur.push((m, loc));
        }
        if !cur.is_empty() {
            groups.push(cur);
        }

        // Decode each rewrite-worthy group and re-encode as one zstd
        // block. Sequential reads on THIS thread — see module header.
        let mut adds: Vec<EncodedBlock> = Vec::new();
        let mut removes: Vec<BlockLoc> = Vec::new();
        for group in &groups {
            let worth_it =
                group.len() >= 2 || group.iter().any(|(m, _)| m.codec == CODEC_RAW);
            if !worth_it {
                continue;
            }
            let mut entries: Vec<LogEntry> = Vec::new();
            for (_, loc) in group {
                let bytes = self.store.read_block(loc)?;
                entries.extend(decode_block(&bytes)?);
            }
            entries.sort_by_key(|e| e.ts);
            let terms = self.extract_terms(&entries);
            let (data, meta) = encode_block(&entries, CODEC_ZSTD, self.config.zstd_level)?;
            adds.push(EncodedBlock { meta, data, terms });
            removes.extend(group.iter().map(|(_, loc)| *loc));
        }
        if adds.is_empty() {
            return Ok((0, 0));
        }

        // One atomic swap. The on_committed callback rewrites the
        // in-memory index at the moment both generations exist in the
        // store, so no query window ever sees a missing block.
        let add_metas: Vec<BlockMeta> = adds.iter().map(|b| b.meta).collect();
        let removed = removes.len();
        self.store
            .replace_blocks(&adds, &removes, &mut |new_locs: &[BlockLoc]| {
                let mut index = self.index_lock();
                index.retain(|(_, loc)| !removes.contains(loc));
                for (meta, loc) in add_metas.iter().zip(new_locs) {
                    index.push((*meta, *loc));
                }
            })?;
        Ok((removed, add_metas.len()))
    }

    /// Retention: delete every block whose ts_max < cutoff (whole-block
    /// granularity — the structural win from PLAN.md: one row delete
    /// removes thousands of entries) plus any buffered entries older
    /// than the cutoff. The store removes term rows in the same
    /// operation. Returns the number of blocks deleted.
    pub fn prune(&self, cutoff: i64) -> Result<usize, String> {
        let victims: Vec<BlockLoc> = self
            .index_lock()
            .iter()
            .filter(|(m, _)| m.ts_max < cutoff)
            .map(|(_, loc)| *loc)
            .collect();
        self.buffer_lock().retain(|e| e.ts >= cutoff);
        if victims.is_empty() {
            return Ok(0);
        }
        let errors = self.store.delete_blocks(&victims);
        if !errors.is_empty() {
            return Err(format!("prune errors: {}", errors.join("; ")));
        }
        self.index_lock()
            .retain(|(m, _)| m.ts_max >= cutoff);
        Ok(victims.len())
    }

    /// The query path. NO rayon (module header): candidate blocks are
    /// read and decoded sequentially on the calling thread.
    ///
    ///   1. indexed filters → terms → store.query_terms (posting-list
    ///      intersection + ts overlap, all inside the store),
    ///   2. read + decode each candidate block,
    ///   3. exact per-entry filtering (the term index is block-granular
    ///      — a matching block still contains non-matching entries),
    ///   4. merge in matching BUFFERED entries (queryable-before-flush),
    ///   5. sort by ts.
    pub fn query(&self, q: &LogQuery) -> Result<Vec<LogEntry>, String> {
        let mut terms: Vec<String> = Vec::new();
        if let Some(lvl) = q.level {
            if lvl > 3 {
                return Err(format!("invalid level {lvl} in query"));
            }
            terms.push(format!("level:{}", level_name(lvl)));
        }
        for (k, v) in &q.metadata_eq {
            if self.config.index_keys.iter().any(|ik| ik == k) {
                terms.push(format!("{k}:{v}"));
            }
            // Non-indexed keys contribute no term — they are exact-
            // filtered per entry in step 3 (scan-only, by design).
        }

        let locs = self.store.query_terms(&terms, q.ts_min, q.ts_max)?;
        let mut out: Vec<LogEntry> = Vec::new();
        for loc in &locs {
            let bytes = self.store.read_block(loc)?;
            for entry in decode_block(&bytes)? {
                if entry_matches(&entry, q) {
                    out.push(entry);
                }
            }
        }
        for entry in self.buffer_lock().iter() {
            if entry_matches(entry, q) {
                out.push(entry.clone());
            }
        }
        // Stable sort: entries with equal ts keep block order, buffered
        // entries land after flushed ones — deterministic either way.
        out.sort_by_key(|e| e.ts);
        Ok(out)
    }

    /// (persisted blocks, raw blocks, buffered entries) — for stats or
    /// debugging; cheap, index-only.
    pub fn stats(&self) -> (usize, usize, usize) {
        let index = self.index_lock();
        let raw = index.iter().filter(|(m, _)| m.codec == CODEC_RAW).count();
        (index.len(), raw, self.buffered_count())
    }
}

/// Exact per-entry filter — the truth the block-level term index only
/// approximates.
fn entry_matches(e: &LogEntry, q: &LogQuery) -> bool {
    if e.ts < q.ts_min || e.ts > q.ts_max {
        return false;
    }
    if let Some(lvl) = q.level {
        if e.level != lvl {
            return false;
        }
    }
    for (k, v) in &q.metadata_eq {
        if e.meta_value(k) != Some(v.as_str()) {
            return false;
        }
    }
    if let Some(needle) = &q.message_contains {
        if !e.message.contains(needle.as_str()) {
            return false;
        }
    }
    true
}
