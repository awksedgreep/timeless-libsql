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
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

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
    /// F6: index message TRIGRAMS per block (`tg:<hex>` terms + the
    /// `tg:` marker), enabling sound block pruning for substring LIKE.
    /// Opt-in — the index costs term-table space.
    pub message_trigrams: bool,
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
            message_trigrams: false,
            index_keys: Vec::new(),
        }
    }
}

/// Cumulative, process-local work counters. These deliberately measure work
/// performed rather than durable logical state: a later SQLite rollback does
/// not erase CPU time or bytes decoded. Callers take before/after snapshots
/// when attributing one workload interval.
#[derive(Clone, Copy, Debug, Default)]
pub struct BlockEngineProfileSnapshot {
    pub ingest_batch_count: u64,
    pub ingest_batch_entries: u64,
    pub ingest_wire_decode_ns: u64,
    pub ingest_normalize_ns: u64,
    pub ingest_buffer_append_ns: u64,
    pub flush_count: u64,
    pub flush_entries: u64,
    pub flush_total_ns: u64,
    pub flush_partition_ns: u64,
    pub flush_encode_terms_ns: u64,
    pub flush_store_ns: u64,
    pub query_count: u64,
    pub query_total_ns: u64,
    pub query_snapshot_ns: u64,
    pub query_materialize_ns: u64,
    pub query_snapshot_payload_bytes: u64,
    pub query_snapshot_payload_max_bytes: u64,
    pub query_snapshot_buffered_entries: u64,
    pub query_stable_location_snapshots: u64,
    pub query_payload_bytes_read: u64,
    pub query_candidate_blocks: u64,
    pub query_decoded_entries: u64,
    pub query_returned_entries: u64,
    pub optimize_count: u64,
    pub optimize_total_ns: u64,
    pub optimize_blocks_removed: u64,
    pub optimize_blocks_written: u64,
}

#[derive(Default)]
struct BlockEngineProfile {
    ingest_batch_count: AtomicU64,
    ingest_batch_entries: AtomicU64,
    ingest_wire_decode_ns: AtomicU64,
    ingest_normalize_ns: AtomicU64,
    ingest_buffer_append_ns: AtomicU64,
    flush_count: AtomicU64,
    flush_entries: AtomicU64,
    flush_total_ns: AtomicU64,
    flush_partition_ns: AtomicU64,
    flush_encode_terms_ns: AtomicU64,
    flush_store_ns: AtomicU64,
    query_count: AtomicU64,
    query_total_ns: AtomicU64,
    query_snapshot_ns: AtomicU64,
    query_materialize_ns: AtomicU64,
    query_snapshot_payload_bytes: AtomicU64,
    query_snapshot_payload_max_bytes: AtomicU64,
    query_snapshot_buffered_entries: AtomicU64,
    query_stable_location_snapshots: AtomicU64,
    query_payload_bytes_read: AtomicU64,
    query_candidate_blocks: AtomicU64,
    query_decoded_entries: AtomicU64,
    query_returned_entries: AtomicU64,
    optimize_count: AtomicU64,
    optimize_total_ns: AtomicU64,
    optimize_blocks_removed: AtomicU64,
    optimize_blocks_written: AtomicU64,
}

impl BlockEngineProfile {
    fn snapshot(&self) -> BlockEngineProfileSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        BlockEngineProfileSnapshot {
            ingest_batch_count: load(&self.ingest_batch_count),
            ingest_batch_entries: load(&self.ingest_batch_entries),
            ingest_wire_decode_ns: load(&self.ingest_wire_decode_ns),
            ingest_normalize_ns: load(&self.ingest_normalize_ns),
            ingest_buffer_append_ns: load(&self.ingest_buffer_append_ns),
            flush_count: load(&self.flush_count),
            flush_entries: load(&self.flush_entries),
            flush_total_ns: load(&self.flush_total_ns),
            flush_partition_ns: load(&self.flush_partition_ns),
            flush_encode_terms_ns: load(&self.flush_encode_terms_ns),
            flush_store_ns: load(&self.flush_store_ns),
            query_count: load(&self.query_count),
            query_total_ns: load(&self.query_total_ns),
            query_snapshot_ns: load(&self.query_snapshot_ns),
            query_materialize_ns: load(&self.query_materialize_ns),
            query_snapshot_payload_bytes: load(&self.query_snapshot_payload_bytes),
            query_snapshot_payload_max_bytes: load(&self.query_snapshot_payload_max_bytes),
            query_snapshot_buffered_entries: load(&self.query_snapshot_buffered_entries),
            query_stable_location_snapshots: load(&self.query_stable_location_snapshots),
            query_payload_bytes_read: load(&self.query_payload_bytes_read),
            query_candidate_blocks: load(&self.query_candidate_blocks),
            query_decoded_entries: load(&self.query_decoded_entries),
            query_returned_entries: load(&self.query_returned_entries),
            optimize_count: load(&self.optimize_count),
            optimize_total_ns: load(&self.optimize_total_ns),
            optimize_blocks_removed: load(&self.optimize_blocks_removed),
            optimize_blocks_written: load(&self.optimize_blocks_written),
        }
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// One query. All filters are optional except the ts range (pass
/// i64::MIN / i64::MAX for "unbounded", like the metrics vtab).
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
    /// F6: a LIKE pattern used ONLY for trigram block PRUNING — no
    /// entries are filtered by it (the SQL layer rechecks LIKE exactly;
    /// the vtab never sets omit on the constraint). Sound by
    /// construction: only blocks that provably cannot contain a match
    /// are skipped, and blocks without the `tg:` marker (pre-F6 data,
    /// trigram-capped blocks, disabled index) are never skipped.
    pub message_like_prune: Option<String>,
}

struct LogQuerySnapshot {
    payloads: Vec<Vec<u8>>,
    locations: Vec<BlockLoc>,
    buffered: Vec<LogEntry>,
    candidate_blocks: u64,
    payload_bytes: u64,
    stable_locations: bool,
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
/// LOCK ORDER: transition → txn journal → buffer → store callbacks →
/// index. Queries hold a shared transition guard through candidate
/// lookup, payload decoding, and the buffered merge. Store callbacks
/// never call back into this engine.
#[derive(Default)]
struct TxnFrame {
    savepoint: Option<i32>,
    buffer_mark: usize,
    saved: Vec<LogEntry>,
    added: HashSet<i64>,
    removed: Vec<IndexEntry>,
}

#[derive(Default)]
struct TxnJournal {
    frames: Vec<TxnFrame>,
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

pub struct BlockEngine {
    store: Box<dyn BlockStore>,
    config: BlockEngineConfig,
    /// Pins the buffer/store/index generation seen by a complete query.
    transition: RwLock<()>,
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
    /// F2 retention window in NATIVE ts units; 0 = disabled.
    retention_native: AtomicI64,
    /// Last retention cutoff applied (advance guard); i64::MIN = never.
    retention_floor: AtomicI64,
    profile: BlockEngineProfile,
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
            transition: RwLock::new(()),
            buffer: Mutex::new(Vec::new()),
            index: Mutex::new(index),
            txn_active: AtomicBool::new(false),
            txn: Mutex::new(TxnJournal::default()),
            retention_native: AtomicI64::new(0),
            retention_floor: AtomicI64::new(i64::MIN),
            profile: BlockEngineProfile::default(),
        })
    }

    pub fn config(&self) -> &BlockEngineConfig {
        &self.config
    }

    pub fn profile(&self) -> BlockEngineProfileSnapshot {
        self.profile.snapshot()
    }

    /// Attribute successful extension wire decoding without coupling the
    /// storage engine to a particular batch format.
    pub fn record_ingest_wire_decode(&self, duration: Duration) {
        self.profile
            .ingest_wire_decode_ns
            .fetch_add(duration_ns(duration), Ordering::Relaxed);
    }

    /// Poison-tolerant locks, same style as the rest of timeless-core:
    /// a panic while holding the lock still yields the data.
    fn buffer_lock(&self) -> std::sync::MutexGuard<'_, Vec<LogEntry>> {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn transition_read(&self) -> RwLockReadGuard<'_, ()> {
        self.transition.read().unwrap_or_else(|e| e.into_inner())
    }

    fn transition_write(&self) -> RwLockWriteGuard<'_, ()> {
        self.transition.write().unwrap_or_else(|e| e.into_inner())
    }

    fn index_lock(&self) -> std::sync::MutexGuard<'_, Vec<IndexEntry>> {
        self.index.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn txn_lock(&self) -> std::sync::MutexGuard<'_, TxnJournal> {
        self.txn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Acquire the journal iff a transaction is active. Mutation sites
    /// call this after the transition guard and before buffer/index.
    fn txn_guard(&self) -> Option<std::sync::MutexGuard<'_, TxnJournal>> {
        if !self.txn_active.load(Ordering::SeqCst) {
            return None;
        }
        let journal = self.txn_lock();
        self.txn_active.load(Ordering::SeqCst).then_some(journal)
    }

    // ── Transaction journal API (PLAN.md R5; see TxnJournal docs) ────

    /// Start journaling. Called from the vtab's xBegin — which SQLite
    /// fires before the first write of EVERY transaction, including
    /// the implicit per-statement one in autocommit mode — so it must
    /// stay cheap: one usize mark. Nested begins are impossible from
    /// SQLite; savepoints add undo frames through txn_savepoint.
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

    /// Commit: the host transaction made everything permanent — drop
    /// the journal (contents are cleared lazily by the next begin).
    pub fn txn_commit(&self) {
        let mut j = self.txn_lock(); // serialize against in-flight recorders
        while let Some(frame) = j.frames.pop() {
            j.spares.push(frame);
        }
        self.txn_active.store(false, Ordering::SeqCst);
    }

    /// Rollback: undo journaled mutations in the mirror image of what
    /// the host rollback did to the shadow tables — truncate txn-era
    /// buffered entries, restore drained pre-txn entries, drop index
    /// entries whose block rows vanished, restore entries whose rows
    /// came back (verbatim, partition tag included — same rowids).
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
        self.txn_active.store(false, Ordering::SeqCst);
    }

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
    }

    fn reset_txn_frame(&self, mut frame: TxnFrame, savepoint: Option<i32>) -> TxnFrame {
        frame.savepoint = savepoint;
        frame.buffer_mark = self.buffer_lock().len();
        frame.saved.clear();
        frame.added.clear();
        frame.removed.clear();
        frame
    }

    fn rollback_txn_frame(&self, frame: &mut TxnFrame) {
        let TxnFrame {
            buffer_mark,
            saved,
            added,
            removed,
            ..
        } = frame;
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
    }

    fn merge_txn_frame(parent: &mut TxnFrame, child: &mut TxnFrame) {
        if parent.buffer_mark > 0 && !child.saved.is_empty() {
            child.saved.truncate(parent.buffer_mark);
            parent.saved.append(&mut child.saved);
            parent.buffer_mark = 0;
        }
        for entry in child.removed.drain(..) {
            if !parent.added.remove(&entry.loc.id) {
                parent.removed.push(entry);
            }
        }
        parent.added.extend(child.added.drain());
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

    /// F5 bulk append: same validation/normalization and auto-flush
    /// contract as push(), one buffer lock for the whole batch. The
    /// CALLER validates the batch wholesale before this runs (all-or-
    /// nothing at the wire layer); level bytes are re-checked here
    /// because they are engine invariants, and a bad one mid-batch
    /// aborts BEFORE anything is appended.
    pub fn push_batch(&self, mut entries: Vec<LogEntry>) -> Result<usize, String> {
        let normalize_started = Instant::now();
        for entry in &mut entries {
            if entry.level > 3 {
                return Err(format!(
                    "invalid level {} (0=debug 1=info 2=warning 3=error)",
                    entry.level
                ));
            }
            entry.metadata.sort_by(|a, b| a.0.cmp(&b.0));
            entry.metadata.reverse();
            entry.metadata.dedup_by(|a, b| a.0 == b.0);
            entry.metadata.reverse();
        }
        let normalize_ns = elapsed_ns(normalize_started);
        let n = entries.len();
        let append_started = Instant::now();
        let should_flush = {
            let mut buf = self.buffer_lock();
            buf.extend(entries);
            buf.len() >= self.config.flush_threshold
        };
        let append_ns = elapsed_ns(append_started);
        self.profile
            .ingest_batch_count
            .fetch_add(1, Ordering::Relaxed);
        self.profile
            .ingest_batch_entries
            .fetch_add(n as u64, Ordering::Relaxed);
        self.profile
            .ingest_normalize_ns
            .fetch_add(normalize_ns, Ordering::Relaxed);
        self.profile
            .ingest_buffer_append_ns
            .fetch_add(append_ns, Ordering::Relaxed);
        if should_flush {
            self.flush()?;
        }
        Ok(n)
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
        let started = Instant::now();
        let out = self.flush_inner()?;
        self.apply_retention()?;
        if out > 0 {
            self.profile.flush_count.fetch_add(1, Ordering::Relaxed);
            self.profile
                .flush_entries
                .fetch_add(out as u64, Ordering::Relaxed);
            self.profile
                .flush_total_ns
                .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        }
        Ok(out)
    }

    fn flush_inner(&self) -> Result<usize, String> {
        let _transition = self.transition_write();
        // Transition is already exclusive; journal before buffer/index, then hold
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
                let mark = j.buffer_mark;
                j.saved.extend_from_slice(&buf[..mark]);
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
        let partition_started = Instant::now();
        buf.sort_by_key(|e| (e.level, e.ts));
        let partition_ns = elapsed_ns(partition_started);

        let encode_started = Instant::now();
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
        let encode_terms_ns = elapsed_ns(encode_started);

        let store_started = Instant::now();
        let locs = self.store.put_blocks(&blocks)?;
        let store_ns = elapsed_ns(store_started);
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
        self.profile
            .flush_partition_ns
            .fetch_add(partition_ns, Ordering::Relaxed);
        self.profile
            .flush_encode_terms_ns
            .fetch_add(encode_terms_ns, Ordering::Relaxed);
        self.profile
            .flush_store_ns
            .fetch_add(store_ns, Ordering::Relaxed);
        Ok(n)
    }

    /// F6 trigram machinery. Terms are `tg:` + lowercase hex of THREE
    /// lowercased message BYTES (hex because a byte window can split a
    /// UTF-8 sequence, and terms are TEXT). ASCII-only case folding —
    /// exactly the folding SQLite's default LIKE performs, so the index
    /// is a sound superset filter under both default and
    /// case_sensitive_like. The bare `tg:` MARKER term declares "this
    /// block is trigram-indexed"; blocks without it are never pruned.
    fn tg_term(w: [u8; 3]) -> String {
        format!("tg:{:02x}{:02x}{:02x}", w[0], w[1], w[2])
    }

    fn fold_byte(b: u8) -> u8 {
        if b.is_ascii_uppercase() {
            b + 32
        } else {
            b
        }
    }

    fn message_trigrams_of(text: &str, out: &mut BTreeSet<[u8; 3]>) {
        let bytes = text.as_bytes();
        for w in bytes.windows(3) {
            out.insert([
                Self::fold_byte(w[0]),
                Self::fold_byte(w[1]),
                Self::fold_byte(w[2]),
            ]);
        }
    }

    /// Required trigram terms for a LIKE pattern: every 3-byte window of
    /// every literal run (split on `%`/`_`) must appear in a matching
    /// message. Empty = the pattern yields no pruning power (all blocks
    /// stay candidates). NOTE: SQLite never forwards `LIKE ... ESCAPE`
    /// as a vtab LIKE constraint, so wildcard-escaping cannot reach us.
    pub fn like_pattern_trigrams(pattern: &str) -> Vec<String> {
        let mut set = BTreeSet::new();
        for run in pattern.split(|c| c == '%' || c == '_') {
            Self::message_trigrams_of(run, &mut set);
        }
        set.into_iter().map(Self::tg_term).collect()
    }

    /// Per-block trigram budget: a block whose messages exceed this many
    /// DISTINCT trigrams is left unindexed (no marker → never pruned)
    /// rather than bloating `_terms` past the data.
    const MAX_BLOCK_TRIGRAMS: usize = 4096;

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
        if self.config.message_trigrams {
            let mut trigrams = BTreeSet::new();
            for e in entries {
                Self::message_trigrams_of(&e.message, &mut trigrams);
                if trigrams.len() > Self::MAX_BLOCK_TRIGRAMS {
                    break;
                }
            }
            if trigrams.len() <= Self::MAX_BLOCK_TRIGRAMS {
                set.insert("tg:".to_string()); // the indexed marker
                set.extend(trigrams.into_iter().map(Self::tg_term));
            }
            // over budget: no marker, block stays always-decoded
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
        let started = Instant::now();
        let out = self.optimize_inner()?;
        self.apply_retention()?;
        self.profile.optimize_count.fetch_add(1, Ordering::Relaxed);
        self.profile
            .optimize_total_ns
            .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        self.profile
            .optimize_blocks_removed
            .fetch_add(out.0 as u64, Ordering::Relaxed);
        self.profile
            .optimize_blocks_written
            .fetch_add(out.1 as u64, Ordering::Relaxed);
        Ok(out)
    }

    fn optimize_inner(&self) -> Result<(usize, usize), String> {
        let _transition = self.transition_write();
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
                    let span_ok = new_max.saturating_sub(new_min) <= self.config.merge_max_ts_span;
                    let size_ok =
                        cur_entries + m.entry_count as usize <= self.config.merge_target_entries;
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
            let worth_it = group.len() >= 2 || group.iter().any(|e| e.meta.codec == CODEC_RAW);
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
            let (data, meta) = encode_block(&entries, CODEC_COLUMNAR_V2, self.config.zstd_level)?;
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
                for ((meta, loc), partition) in add_metas.iter().zip(new_locs).zip(&add_partitions)
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
    /// F2: configure the automatic retention window (NATIVE ts units;
    /// None disables). Idempotent per connect.
    pub fn set_retention(&self, native: Option<i64>) {
        self.retention_native
            .store(native.unwrap_or(0).max(0), Ordering::Relaxed);
    }

    /// Apply the configured retention window at a maintenance boundary.
    /// Cutoff is DATA time: max queryable ts (ts_range) - retention.
    /// Called by the flush/optimize wrappers AFTER their transition
    /// guard is released (prune takes it again); never under a lock.
    pub fn apply_retention(&self) -> Result<usize, String> {
        let retention = self.retention_native.load(Ordering::Relaxed);
        if retention == 0 {
            return Ok(0);
        }
        let Some(high_water) = self.ts_range().1 else {
            return Ok(0); // empty
        };
        let cutoff = high_water.saturating_sub(retention);
        let slice = (retention / 16).max(1);
        let floor = self.retention_floor.load(Ordering::Relaxed);
        if floor != i64::MIN && cutoff < floor.saturating_add(slice) {
            return Ok(0);
        }
        let pruned = self.prune(cutoff)?;
        self.retention_floor.store(cutoff, Ordering::Relaxed);
        Ok(pruned)
    }

    pub fn prune(&self, cutoff: i64) -> Result<usize, String> {
        let _transition = self.transition_write();
        // Transition is already exclusive; journal before buffer/index. Two things
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
                    let mark = j.buffer_mark;
                    j.saved.extend_from_slice(&buf[..mark]);
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
        self.query_after_snapshot(q, || {})
    }

    /// Query with a synchronous notification at the ownership boundary.
    ///
    /// `after_snapshot` runs after candidate payload ownership (stable store
    /// locations or conservative owned bytes) and the matching buffer
    /// generation have been captured under the transition read guard, but
    /// before any payload is decoded or results are sorted.
    /// The SQLite extension uses this point to release its cross-connection
    /// read permit, allowing a waiting writer to publish while CPU-only result
    /// materialization continues. Callbacks must stay short and must not call
    /// back into this engine.
    pub fn query_after_snapshot<F>(
        &self,
        q: &LogQuery,
        after_snapshot: F,
    ) -> Result<Vec<LogEntry>, String>
    where
        F: FnOnce(),
    {
        let started = Instant::now();
        let snapshot_started = Instant::now();
        let snapshot = self.snapshot_query(q)?;
        let snapshot_ns = elapsed_ns(snapshot_started);
        after_snapshot();

        let materialize_started = Instant::now();
        let candidate_blocks = snapshot.candidate_blocks;
        let buffered_entries = snapshot.buffered.len() as u64;
        let snapshot_payload_bytes = snapshot.payload_bytes;
        let stable_locations = snapshot.stable_locations;
        let mut payload_bytes_read = snapshot_payload_bytes;
        let mut decoded_entries = 0u64;
        let mut out: Vec<LogEntry> = Vec::new();
        for bytes in snapshot.payloads {
            let entries = decode_block(&bytes)?;
            decoded_entries = decoded_entries.saturating_add(entries.len() as u64);
            for entry in entries {
                if entry_matches(&entry, q) {
                    out.push(entry);
                }
            }
        }
        for loc in snapshot.locations {
            let bytes = self.store.read_block(&loc)?;
            payload_bytes_read = payload_bytes_read.saturating_add(bytes.len() as u64);
            let entries = decode_block(&bytes)?;
            decoded_entries = decoded_entries.saturating_add(entries.len() as u64);
            for entry in entries {
                if entry_matches(&entry, q) {
                    out.push(entry);
                }
            }
        }
        out.extend(snapshot.buffered);
        // Stable sort: entries with equal ts keep block order, buffered
        // entries land after flushed ones — deterministic either way.
        out.sort_by_key(|e| e.ts);
        let materialize_ns = elapsed_ns(materialize_started);

        self.profile.query_count.fetch_add(1, Ordering::Relaxed);
        self.profile
            .query_total_ns
            .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        self.profile
            .query_snapshot_ns
            .fetch_add(snapshot_ns, Ordering::Relaxed);
        self.profile
            .query_materialize_ns
            .fetch_add(materialize_ns, Ordering::Relaxed);
        self.profile
            .query_snapshot_payload_bytes
            .fetch_add(snapshot_payload_bytes, Ordering::Relaxed);
        self.profile
            .query_snapshot_payload_max_bytes
            .fetch_max(snapshot_payload_bytes, Ordering::Relaxed);
        self.profile
            .query_snapshot_buffered_entries
            .fetch_add(buffered_entries, Ordering::Relaxed);
        if stable_locations {
            self.profile
                .query_stable_location_snapshots
                .fetch_add(1, Ordering::Relaxed);
        }
        self.profile
            .query_payload_bytes_read
            .fetch_add(payload_bytes_read, Ordering::Relaxed);
        self.profile
            .query_candidate_blocks
            .fetch_add(candidate_blocks, Ordering::Relaxed);
        self.profile
            .query_decoded_entries
            .fetch_add(decoded_entries, Ordering::Relaxed);
        self.profile
            .query_returned_entries
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        Ok(out)
    }

    fn snapshot_query(&self, q: &LogQuery) -> Result<LogQuerySnapshot, String> {
        let _transition = self.transition_read();
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

        let mut locs = self.store.query_terms(&terms, q.ts_min, q.ts_max)?;
        // F6 trigram pruning: candidates = (unindexed blocks) ∪ (indexed
        // blocks carrying ALL of the pattern's required trigrams). Blocks
        // without the `tg:` marker are never pruned, so pre-F6 data,
        // budget-capped blocks, and disabled-index tables are all safe.
        if let Some(pattern) = &q.message_like_prune {
            let required = Self::like_pattern_trigrams(pattern);
            if !required.is_empty() {
                let mut marker_terms = terms.clone();
                marker_terms.push("tg:".to_string());
                let indexed: std::collections::HashSet<i64> = self
                    .store
                    .query_terms(&marker_terms, q.ts_min, q.ts_max)?
                    .into_iter()
                    .map(|(loc, _)| loc.id)
                    .collect();
                let mut match_terms = marker_terms;
                match_terms.extend(required);
                let matching: std::collections::HashSet<i64> = self
                    .store
                    .query_terms(&match_terms, q.ts_min, q.ts_max)?
                    .into_iter()
                    .map(|(loc, _)| loc.id)
                    .collect();
                locs.retain(|(loc, _)| !indexed.contains(&loc.id) || matching.contains(&loc.id));
            }
        }
        let candidate_blocks = locs.len() as u64;
        let stable_locations = self.store.query_snapshot_keeps_locations_readable();
        let mut payloads = Vec::new();
        let mut locations = Vec::new();
        let mut payload_bytes = 0u64;
        if stable_locations {
            locations = locs.into_iter().map(|(loc, _meta)| loc).collect();
        } else {
            payloads.reserve(locs.len());
            for (loc, _meta) in &locs {
                let bytes = self.store.read_block(loc)?;
                payload_bytes = payload_bytes.saturating_add(bytes.len() as u64);
                payloads.push(bytes);
            }
        }
        // Buffer membership is part of the protected generation. Filter while
        // borrowing it so the snapshot owns only matching entries, never an
        // avoidable clone of the complete ingest buffer.
        let buffered = self
            .buffer_lock()
            .iter()
            .filter(|entry| entry_matches(entry, q))
            .cloned()
            .collect();
        Ok(LogQuerySnapshot {
            payloads,
            locations,
            buffered,
            candidate_blocks,
            payload_bytes,
            stable_locations,
        })
    }

    /// F4 bucket kernel (FEATURE_PLAN.md): count entries per bucket,
    /// grouped by `level` or a declared index key. Buckets are
    /// CLOSED-OPEN `[start + k*step, start + k*step + step)` aligned to
    /// the query's ts_min — histograms bin FORWARD (the metrics grid
    /// kernels sample backward with (t-w, t]; both are documented).
    /// Entries missing the group key land in group "". Semantics-free:
    /// filtering is the existing LogQuery machinery (term-pruned),
    /// counting is mechanical. Rows sorted (bucket_ts, group).
    pub fn bucket_counts(
        &self,
        filter: &LogQuery,
        group_by: &str,
        step: i64,
    ) -> Result<Vec<(i64, String, u64)>, String> {
        if step <= 0 {
            return Err(format!("step must be positive, got {step}"));
        }
        if group_by != "level" && !self.config.index_keys.iter().any(|k| k == group_by) {
            return Err(format!(
                "unknown group_by {group_by:?}; expected 'level' or a declared index key ({})",
                self.config.index_keys.join(", ")
            ));
        }
        let (start, stop) = (filter.ts_min, filter.ts_max);
        if stop >= start {
            let buckets = ((stop as i128 - start as i128) / step as i128) + 1;
            if buckets > 1_000_000 {
                return Err(format!(
                    "grid of {buckets} buckets exceeds the 1000000 bucket cap"
                ));
            }
        }
        let bucket_of = |ts: i64| -> i64 {
            let k = (ts as i128 - start as i128) / step as i128;
            (start as i128 + k * step as i128) as i64
        };

        // FAST PATH (the whole reason this kernel beats GROUP BY): when
        // grouping by level with no metadata/message filters, a
        // LEVEL-PURE block (the Session 5 partition tag) that sits
        // entirely inside the range AND inside one bucket contributes
        // meta.entry_count WITHOUT decoding; pure blocks of a
        // filtered-out level are skipped without decoding. Only mixed
        // and bucket-straddling blocks decode. Same guard/lock order as
        // query(): transition → index (scoped) → store reads → buffer.
        if group_by == "level" && filter.metadata_eq.is_empty() && filter.message_contains.is_none()
        {
            let _transition = self.transition_read();
            let mut counts: std::collections::BTreeMap<(i64, u8), u64> =
                std::collections::BTreeMap::new();
            let candidates: Vec<(BlockMeta, BlockLoc, Option<u8>)> = {
                let index = self.index_lock();
                index
                    .iter()
                    .filter(|e| e.meta.ts_min <= stop && e.meta.ts_max >= start)
                    .map(|e| (e.meta, e.loc, e.partition))
                    .collect()
            };
            let mut decode: Vec<BlockLoc> = Vec::new();
            for (meta, loc, partition) in candidates {
                match partition {
                    Some(level) if filter.level.is_none() || filter.level == Some(level) => {
                        let inside = meta.ts_min >= start && meta.ts_max <= stop;
                        if inside && bucket_of(meta.ts_min) == bucket_of(meta.ts_max) {
                            *counts.entry((bucket_of(meta.ts_min), level)).or_insert(0) +=
                                meta.entry_count as u64;
                        } else {
                            decode.push(loc);
                        }
                    }
                    Some(_) => {} // pure block of a filtered-out level: free skip
                    None => decode.push(loc),
                }
            }
            let mut bin = |e: &LogEntry| {
                if e.ts < start || e.ts > stop {
                    return;
                }
                if let Some(fl) = filter.level {
                    if e.level != fl {
                        return;
                    }
                }
                *counts.entry((bucket_of(e.ts), e.level)).or_insert(0) += 1;
            };
            for loc in decode {
                let bytes = self.store.read_block(&loc)?;
                for entry in decode_block(&bytes)? {
                    bin(&entry);
                }
            }
            for entry in self.buffer_lock().iter() {
                bin(entry);
            }
            let mut out: std::collections::BTreeMap<(i64, String), u64> =
                std::collections::BTreeMap::new();
            for ((b, level), n) in counts {
                out.insert((b, level_name(level).to_string()), n);
            }
            return Ok(out.into_iter().map(|((b, g), n)| (b, g, n)).collect());
        }

        let entries = self.query(filter)?;
        let mut counts: std::collections::BTreeMap<(i64, String), u64> =
            std::collections::BTreeMap::new();
        for e in &entries {
            let bucket_ts = bucket_of(e.ts);
            let group = if group_by == "level" {
                level_name(e.level).to_string()
            } else {
                e.meta_value(group_by).unwrap_or("").to_string()
            };
            *counts.entry((bucket_ts, group)).or_insert(0) += 1;
        }
        Ok(counts.into_iter().map(|((b, g), n)| (b, g, n)).collect())
    }

    /// (persisted blocks, raw blocks, buffered entries) — for stats or
    /// debugging; cheap and payload-free.
    pub fn stats(&self) -> (usize, usize, usize) {
        self.stats_with_after_index(|| {})
    }

    /// Queryable ts range (blocks + buffer), payload-free. Same lock
    /// discipline as stats(): index scope dropped before the buffer is
    /// read (R7 — flush acquires buffer then index).
    pub fn ts_range(&self) -> (Option<i64>, Option<i64>) {
        let (mut mn, mut mx) = {
            let index = self.index_lock();
            index
                .iter()
                .fold((None, None), |(mn, mx): (Option<i64>, Option<i64>), e| {
                    (
                        Some(mn.map_or(e.meta.ts_min, |m: i64| m.min(e.meta.ts_min))),
                        Some(mx.map_or(e.meta.ts_max, |m: i64| m.max(e.meta.ts_max))),
                    )
                })
        };
        for e in self.buffer_lock().iter() {
            mn = Some(mn.map_or(e.ts, |m| m.min(e.ts)));
            mx = Some(mx.map_or(e.ts, |m| m.max(e.ts)));
        }
        (mn, mx)
    }

    fn stats_with_after_index(&self, after_index: impl FnOnce()) -> (usize, usize, usize) {
        // Flush holds buffer through persistence and then takes index.
        // Never retain index while reading the buffered count.
        let (blocks, raw) = {
            let index = self.index_lock();
            let raw = index.iter().filter(|e| e.meta.codec == CODEC_RAW).count();
            (index.len(), raw)
        };
        after_index();
        (blocks, raw, self.buffered_count())
    }

    #[cfg(test)]
    pub(super) fn stats_after_index(&self, after_index: impl FnOnce()) -> (usize, usize, usize) {
        self.stats_with_after_index(after_index)
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
