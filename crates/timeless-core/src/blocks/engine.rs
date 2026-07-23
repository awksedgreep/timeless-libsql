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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use super::codec::{decode_block, encode_block, CODEC_COLUMNAR_V2, CODEC_RAW};
use super::{level_name, BlockLoc, BlockMeta, BlockStore, EncodedBlock, LogEntry};

/// Tuning knobs. All ts_* values are in the SAME opaque unit as
/// LogEntry.ts — the engine never assumes seconds/millis/nanos.
pub struct BlockEngineConfig {
    /// Buffered entries that trigger an automatic flush inside push().
    pub flush_threshold: usize,
    /// zstd level for compressed blocks (7 = the measured sweet spot;
    /// codec 4's per-column zstd strategies use it too).
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

/// One entry in the engine's in-memory block index: the store-persisted
/// metadata plus the LEVEL PARTITION tag.
///
/// The partition tag is the fix for the Session 5 "level-term weakness"
/// (bench-logs measured `level=error` at 356ms over 1M entries — SLOWER
/// than a plain table scan): flush used to write level-MIXED 8192-entry
/// blocks, so with a 70%-info workload virtually every block carried
/// every `level:` term and the posting-list intersection pruned nothing.
/// flush() now writes LEVEL-PURE blocks (one per level present), each
/// carrying exactly ONE `level:` term, so the existing query_terms
/// intersection prunes perfectly — no store, schema or vtab changes.
///
/// `partition` is IN-MEMORY ONLY, never persisted (shadow tables are
/// frozen — no schema changes allowed). It is:
///   - known exactly at flush/optimize time (we just encoded the block),
///   - re-DERIVED at recovery from the `level:` posting lists the store
///     already keeps: a block listed under exactly one `level:` term is
///     pure for that level; two or more terms = a pre-partitioning
///     mixed block. Deriving from terms costs four query_terms calls at
///     construction (metadata-only, no payload reads) and needs zero
///     new persistence — the posting lists ARE the partition record.
///
/// `Some(level)` = level-pure block; `None` = mixed (written before
/// this change). Mixed blocks are their own merge partition: optimize()
/// never merges them with pure blocks (that would re-pollute the level
/// terms), only with each other.
#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    meta: BlockMeta,
    loc: BlockLoc,
    partition: Option<u8>,
}

/// Transaction journal (PLAN.md risk R5) — the blocks twin of the
/// metrics Engine's TxnJournal; read engine.rs (metrics) for the full
/// design story. Short version: block/term rows ride the host SQLite
/// transaction, engine memory does not. While a journal is active
/// (between txn_begin and txn_commit/txn_rollback — SQLite brackets
/// EVERY write, including autocommit single statements), mutations
/// record their undo:
///
///   - buffer_mark: buffered entry count at begin. Entries above the
///     mark were pushed during the txn; rollback truncates them.
///   - saved: PRE-txn buffered entries that an intra-txn flush drained
///     into a block (whose row rolls back!) or an intra-txn prune
///     dropped (whose... nothing — but the buffer retain must undo
///     together with the block DELETEs it accompanied). Restored into
///     the buffer on rollback. Whenever an operation is about to
///     disturb the pre-txn prefix (flush sorts + drains, prune
///     retains), it first snapshots buffer[..mark] here and zeroes the
///     mark — from then on the whole buffer is txn-era.
///   - added: BlockLoc ids of blocks persisted during the txn; their
///     rows vanish on rollback, so the index entries must go too.
///   - removed: pre-txn IndexEntry values removed during the txn
///     (optimize/prune). Host rollback restores the deleted rows under
///     their original rowids (page-level undo), so restoring these
///     verbatim — including the partition tag — is exactly right.
///     Dedup rule: removing a block that `added` contains cancels the
///     add instead (a block born and deleted inside one txn must not
///     be resurrected).
///
/// NOT journaled (accepted): nothing — blocks have no registry; term
/// and trace rows are store-side and ride the host transaction.
/// Precondition: the store must be transactional (shadow tables); the
/// txn_* API is meaningless over MemBlockStore except in tests that
/// treat it as always-committed.
///
/// LOCK ORDER: txn journal → buffer → index. Every site that touches
/// the journal acquires it first.
#[derive(Default)]
struct TxnJournal {
    buffer_mark: usize,
    saved: Vec<LogEntry>,
    added: HashSet<i64>,
    removed: Vec<IndexEntry>,
}

pub struct BlockEngine {
    store: Box<dyn BlockStore>,
    config: BlockEngineConfig,
    /// Entries pushed but not yet flushed into a block. Queryable (the
    /// same queryable-before-flush property the metrics engine has).
    buffer: Mutex<Vec<LogEntry>>,
    /// In-memory metadata index of every persisted block, rebuilt from
    /// store.scan() (+ level-term partition derivation) at construction.
    /// optimize() and prune() plan from this; the QUERY path asks the
    /// store instead (posting lists live store-side).
    index: Mutex<Vec<IndexEntry>>,
    /// True between txn_begin and txn_commit/txn_rollback; an atomic so
    /// the no-transaction fast path costs one load (see TxnJournal).
    txn_active: AtomicBool,
    txn: Mutex<TxnJournal>,
}

impl BlockEngine {
    /// Construct over a store, recovering the block index via scan()
    /// and each block's level partition via the `level:` posting lists
    /// (see IndexEntry). The store is expected to be able to answer
    /// these immediately (in the vtab this runs re-entrantly during
    /// xCreate/xConnect, which is safe: the calling thread already
    /// holds the connection).
    pub fn new(store: Box<dyn BlockStore>, config: BlockEngineConfig) -> Result<Self, String> {
        let scanned = store.scan()?;

        // Partition derivation: ask the term index which blocks carry
        // each of the four `level:` terms (full ts range → every block).
        // Every block has at least one level term by construction
        // (extract_terms emits one per entry, blocks are never empty);
        // exactly one hit = level-pure, several = mixed. Four cheap
        // metadata-only queries replace any need to persist the tag.
        let mut hits: HashMap<i64, (u32, u8)> = HashMap::new(); // id → (count, last level)
        for lvl in 0u8..4 {
            let term = vec![format!("level:{}", level_name(lvl))];
            for (loc, _) in store.query_terms(&term, i64::MIN, i64::MAX)? {
                let e = hits.entry(loc.id).or_insert((0, lvl));
                e.0 += 1;
                e.1 = lvl;
            }
        }
        let index = scanned
            .into_iter()
            .map(|(meta, loc)| IndexEntry {
                meta,
                loc,
                partition: match hits.get(&loc.id) {
                    Some((1, lvl)) => Some(*lvl),
                    // 0 hits should be impossible; treat it like mixed
                    // (the conservative bucket) rather than guessing.
                    _ => None,
                },
            })
            .collect();

        Ok(BlockEngine {
            store,
            config,
            buffer: Mutex::new(Vec::new()),
            index: Mutex::new(index),
            txn_active: AtomicBool::new(false),
            txn: Mutex::new(TxnJournal::default()),
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

    fn index_lock(&self) -> std::sync::MutexGuard<'_, Vec<IndexEntry>> {
        self.index.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn txn_lock(&self) -> std::sync::MutexGuard<'_, TxnJournal> {
        self.txn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Acquire the journal iff a transaction is active. Mutation sites
    /// call this FIRST (lock order: txn → buffer → index).
    fn txn_guard(&self) -> Option<std::sync::MutexGuard<'_, TxnJournal>> {
        if self.txn_active.load(Ordering::SeqCst) {
            Some(self.txn_lock())
        } else {
            None
        }
    }

    // ── Transaction journal API (PLAN.md R5; see TxnJournal docs) ────

    /// Start journaling. Called from the vtab's xBegin — which SQLite
    /// fires before the first write of EVERY transaction, including
    /// the implicit per-statement one in autocommit mode — so it must
    /// stay cheap: one usize mark, capacity-retaining clears, zero
    /// steady-state allocations. Nested begins are impossible from
    /// SQLite; debug builds assert, release builds restart the journal.
    pub fn txn_begin(&self) {
        let mut j = self.txn_lock();
        debug_assert!(
            !self.txn_active.load(Ordering::SeqCst),
            "txn_begin while a transaction journal is already active (nested xBegin?)"
        );
        j.buffer_mark = self.buffer_lock().len();
        j.saved.clear();
        j.added.clear();
        j.removed.clear();
        self.txn_active.store(true, Ordering::SeqCst);
    }

    /// Commit: the host transaction made everything permanent — drop
    /// the journal (contents are cleared lazily by the next begin).
    pub fn txn_commit(&self) {
        let _j = self.txn_lock(); // serialize against in-flight recorders
        self.txn_active.store(false, Ordering::SeqCst);
    }

    /// Rollback: undo journaled mutations in the mirror image of what
    /// the host rollback did to the shadow tables — truncate txn-era
    /// buffered entries, restore drained pre-txn entries, drop index
    /// entries whose block rows vanished, restore entries whose rows
    /// came back (verbatim, partition tag included — same rowids).
    pub fn txn_rollback(&self) {
        let mut j = self.txn_lock();
        if !self.txn_active.load(Ordering::SeqCst) {
            return; // xRollback without xBegin — nothing recorded
        }
        // Split-borrow the journal so the retain closure below can
        // borrow `added` while we drain `removed`.
        let TxnJournal {
            buffer_mark,
            saved,
            added,
            removed,
        } = &mut *j;
        {
            let mut buf = self.buffer_lock();
            buf.truncate(*buffer_mark);
            // Order inside the buffer is irrelevant: flush sorts before
            // encoding and queries sort their results.
            buf.append(saved);
        }
        {
            let mut index = self.index_lock();
            index.retain(|e| !added.contains(&e.loc.id));
            index.append(removed);
        }
        self.txn_active.store(false, Ordering::SeqCst);
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

    /// Drain the buffer into RAW blocks (codec 1 — cheap framing, no
    /// compression: flush is the ingest hot path, optimize() pays the
    /// compression bill later). No-op on an empty buffer. Returns the
    /// number of entries flushed.
    ///
    /// LEVEL-PARTITIONED FLUSH (the "level-term weakness" fix, see
    /// IndexEntry): the buffer is grouped by level and ONE BLOCK PER
    /// LEVEL PRESENT is written (error entries → an error-pure block,
    /// and so on — at most four blocks). A level-pure block's term set
    /// contains exactly one `level:` term, which is what lets the
    /// store's posting-list intersection skip, say, the ~95% of blocks
    /// that contain no errors instead of listing every block under
    /// every level. The cost is up to 4x more (proportionally smaller)
    /// raw blocks per flush; optimize() merges them back to
    /// ~merge_target_entries WITHIN each level partition, so the
    /// steady-state block count barely changes. All blocks go to the
    /// store in ONE put_blocks call (one lock + prepared-statement
    /// reuse in the SQLite backend).
    pub fn flush(&self) -> Result<usize, String> {
        // Journal first (lock order: txn → buffer → index), then hold
        // the buffer lock for the whole flush so a concurrent push
        // can't slip entries between encode and clear. Single-threaded
        // in the vtab anyway; correctness is free here.
        let mut j = self.txn_guard();
        let mut buf = self.buffer_lock();
        if buf.is_empty() {
            return Ok(0);
        }
        // R5: this flush drains PRE-txn entries (below the mark) into
        // blocks whose rows roll back with the host transaction — and
        // the sort below scrambles positions anyway. Snapshot the
        // pre-txn prefix into the journal and zero the mark: from here
        // on, rollback restores from `saved` and truncates the rest.
        if let Some(j) = j.as_deref_mut() {
            if j.buffer_mark > 0 {
                j.saved.extend_from_slice(&buf[..j.buffer_mark]);
                j.buffer_mark = 0;
            }
        }
        // Sort by (level, ts): this makes each level's entries one
        // CONTIGUOUS ts-ordered run, so the per-level blocks can be
        // encoded straight from buffer slices — no clones, and the
        // buffer stays intact (still queryable, nothing lost) if any
        // encode or store call below fails. Within a run the entries
        // are time-ordered, which is what the delta codec and merge-
        // friendly queries want.
        buf.sort_by_key(|e| (e.level, e.ts));

        let mut blocks: Vec<EncodedBlock> = Vec::new();
        let mut levels: Vec<u8> = Vec::new(); // partition tag per block
        let mut start = 0usize;
        while start < buf.len() {
            let level = buf[start].level;
            let end = start + buf[start..].iter().take_while(|e| e.level == level).count();
            let run = &buf[start..end];
            let (data, meta) = encode_block(run, CODEC_RAW, self.config.zstd_level)?;
            // A level-pure run yields exactly one level: term here.
            let terms = self.extract_terms(run);
            blocks.push(EncodedBlock { meta, data, terms });
            levels.push(level);
            start = end;
        }

        let locs = self.store.put_blocks(&blocks)?;
        {
            let mut index = self.index_lock();
            for ((block, loc), level) in blocks.iter().zip(&locs).zip(&levels) {
                // R5: blocks born inside a transaction are journaled so
                // rollback can drop their index entries when their rows
                // vanish.
                if let Some(j) = j.as_deref_mut() {
                    j.added.insert(loc.id);
                }
                index.push(IndexEntry {
                    meta: block.meta,
                    loc: *loc,
                    partition: Some(*level),
                });
            }
        }
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
    ///   1. every RAW block gets recompressed to CODEC_COLUMNAR_V2
    ///      (codec 5, adaptive per-column strategies + shredded
    ///      metadata — the Session 8 shredding winner; legacy codec-2/4
    ///      blocks remain decodable and are upgraded whenever a merge
    ///      rewrites them anyway), and
    ///   2. small compressed blocks get MERGED into ~merge_target_entries
    ///      blocks (bigger dictionary window → better ratio), subject to
    ///      the merge_max_ts_span hard cap (see config — the retention
    ///      boundary rule; the cap applies PER PARTITION, unchanged).
    ///
    /// PARTITION RULE (the "level-term weakness" fix, see IndexEntry):
    /// blocks only merge with blocks of the SAME level partition.
    /// Merging an error-pure block into an info-pure one would re-create
    /// exactly the mixed blocks the partitioned flush exists to prevent
    /// (the merged block would carry both `level:` terms and stop being
    /// prunable by either). Pre-existing mixed blocks (written before
    /// partitioning) form their own partition: they may merge with each
    /// other, never with pure blocks.
    ///
    /// Everything happens in ONE store.replace_blocks call: in the
    /// SQLite backend that means one host transaction covers the whole
    /// swap — new blocks + terms in, old blocks + terms out, atomically.
    ///
    /// Returns (blocks_removed, blocks_written).
    pub fn optimize(&self) -> Result<(usize, usize), String> {
        // Snapshot the index; plan on the copy (no lock held while we
        // read/decode block payloads).
        let candidates: Vec<IndexEntry> = self
            .index_lock()
            .iter()
            .filter(|e| {
                e.meta.codec == CODEC_RAW
                    || (e.meta.entry_count as usize) < self.config.merge_target_entries
            })
            .copied()
            .collect();
        if candidates.is_empty() {
            return Ok((0, 0));
        }

        // Split candidates into merge partitions: one bucket per pure
        // level (0..=3) plus one for mixed legacy blocks. The greedy
        // time-locality grouping below then runs INSIDE each bucket, so
        // no group can span two partitions.
        let mut buckets: [Vec<IndexEntry>; 5] = Default::default();
        for e in candidates {
            let b = match e.partition {
                Some(lvl) => lvl as usize,
                None => 4, // the mixed bucket
            };
            buckets[b].push(e);
        }

        // (group of source blocks, partition tag for the merged output).
        // A merged pure group stays pure — all its entries share one
        // level; a merged mixed group stays mixed.
        let mut groups: Vec<(Vec<IndexEntry>, Option<u8>)> = Vec::new();
        for (b, bucket) in buckets.iter_mut().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let partition = if b < 4 { Some(b as u8) } else { None };
            // Group by time locality: neighbors in ts_min order merge
            // into blocks with tight ts ranges (which is what makes both
            // range pruning and retention deletes effective).
            bucket.sort_by_key(|e| (e.meta.ts_min, e.meta.ts_max));

            // Greedy grouping under two constraints: target entry count
            // and the merged-span hard cap.
            let mut cur: Vec<IndexEntry> = Vec::new();
            let mut cur_entries = 0usize;
            let (mut cur_min, mut cur_max) = (0i64, 0i64);
            for e in bucket.drain(..) {
                let m = e.meta;
                let fits = if cur.is_empty() {
                    true
                } else {
                    let new_min = cur_min.min(m.ts_min);
                    let new_max = cur_max.max(m.ts_max);
                    // saturating_sub: spans near i64 extremes must not wrap.
                    let span_ok =
                        new_max.saturating_sub(new_min) <= self.config.merge_max_ts_span;
                    let size_ok = cur_entries + m.entry_count as usize
                        <= self.config.merge_target_entries;
                    span_ok && size_ok
                };
                if !fits {
                    groups.push((std::mem::take(&mut cur), partition));
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
                cur.push(e);
            }
            if !cur.is_empty() {
                groups.push((std::mem::take(&mut cur), partition));
            }
        }

        // Decode each rewrite-worthy group and re-encode as one zstd
        // block. A group is "worth rewriting" if it contains any RAW
        // block (must transition to zstd regardless) or at least two
        // blocks (an actual merge). A lone already-zstd small block is
        // left alone — rewriting it would be pure write amplification
        // for zero gain. Sequential reads on THIS thread — see module
        // header.
        let mut adds: Vec<EncodedBlock> = Vec::new();
        let mut add_partitions: Vec<Option<u8>> = Vec::new();
        let mut removes: Vec<BlockLoc> = Vec::new();
        for (group, partition) in &groups {
            let worth_it =
                group.len() >= 2 || group.iter().any(|e| e.meta.codec == CODEC_RAW);
            if !worth_it {
                continue;
            }
            let mut entries: Vec<LogEntry> = Vec::new();
            for e in group {
                let bytes = self.store.read_block(&e.loc)?;
                entries.extend(decode_block(&bytes)?);
            }
            entries.sort_by_key(|e| e.ts);
            let terms = self.extract_terms(&entries);
            let (data, meta) =
                encode_block(&entries, CODEC_COLUMNAR_V2, self.config.zstd_level)?;
            adds.push(EncodedBlock { meta, data, terms });
            add_partitions.push(*partition);
            removes.extend(group.iter().map(|e| e.loc));
        }
        if adds.is_empty() {
            return Ok((0, 0));
        }

        // One atomic swap. The on_committed callback rewrites the
        // in-memory index at the moment both generations exist in the
        // store, so no query window ever sees a missing block.
        //
        // Journal (R5): grabbed BEFORE replace_blocks so the lock order
        // inside the callback stays txn → index. Removed pre-txn
        // entries are journaled verbatim (host rollback restores their
        // rows under the same rowids, partition tags ride along);
        // removing a block this txn itself created just cancels the
        // add; new blocks journal their locs.
        let mut j = self.txn_guard();
        let add_metas: Vec<BlockMeta> = adds.iter().map(|b| b.meta).collect();
        let removed = removes.len();
        self.store
            .replace_blocks(&adds, &removes, &mut |new_locs: &[BlockLoc]| {
                let mut index = self.index_lock();
                if let Some(j) = j.as_deref_mut() {
                    for e in index.iter().filter(|e| removes.contains(&e.loc)) {
                        if !j.added.remove(&e.loc.id) {
                            j.removed.push(*e);
                        }
                    }
                }
                index.retain(|e| !removes.contains(&e.loc));
                for ((meta, loc), partition) in
                    add_metas.iter().zip(new_locs).zip(&add_partitions)
                {
                    if let Some(j) = j.as_deref_mut() {
                        j.added.insert(loc.id);
                    }
                    index.push(IndexEntry {
                        meta: *meta,
                        loc: *loc,
                        partition: *partition,
                    });
                }
            })?;
        drop(j);
        Ok((removed, add_metas.len()))
    }

    /// Retention: delete every block whose ts_max < cutoff (whole-block
    /// granularity — the structural win from PLAN.md: one row delete
    /// removes thousands of entries) plus any buffered entries older
    /// than the cutoff. The store removes term rows in the same
    /// operation. Returns the number of blocks deleted.
    pub fn prune(&self, cutoff: i64) -> Result<usize, String> {
        // Journal first (lock order: txn → buffer → index). Two things
        // must be undoable: the buffer retain (it may drop PRE-txn
        // entries, and it shifts positions, invalidating the mark) and
        // the index removals (their rows are restored by host
        // rollback). Same prefix-snapshot trick as flush: preserve
        // buffer[..mark] into `saved`, zero the mark, then mutate
        // freely — rollback truncates everything and restores `saved`.
        let mut j = self.txn_guard();
        let victims: Vec<BlockLoc> = self
            .index_lock()
            .iter()
            .filter(|e| e.meta.ts_max < cutoff)
            .map(|e| e.loc)
            .collect();
        {
            let mut buf = self.buffer_lock();
            if let Some(j) = j.as_deref_mut() {
                if j.buffer_mark > 0 {
                    j.saved.extend_from_slice(&buf[..j.buffer_mark]);
                    j.buffer_mark = 0;
                }
            }
            buf.retain(|e| e.ts >= cutoff);
        }
        if victims.is_empty() {
            return Ok(0);
        }
        let errors = self.store.delete_blocks(&victims);
        if !errors.is_empty() {
            return Err(format!("prune errors: {}", errors.join("; ")));
        }
        {
            let mut index = self.index_lock();
            if let Some(j) = j.as_deref_mut() {
                for e in index.iter().filter(|e| e.meta.ts_max < cutoff) {
                    if !j.added.remove(&e.loc.id) {
                        j.removed.push(*e);
                    }
                }
            }
            index.retain(|e| e.meta.ts_max >= cutoff);
        }
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
        for (loc, _meta) in &locs {
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
        let raw = index
            .iter()
            .filter(|e| e.meta.codec == CODEC_RAW)
            .count();
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
