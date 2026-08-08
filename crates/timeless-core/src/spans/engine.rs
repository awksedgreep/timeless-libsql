//! SpanBlockEngine: buffer → raw span block → optimized block state
//! machine plus the query path — the traces twin of blocks/engine.rs,
//! kept deliberately diff-able against it (same method names, same
//! locking, same greedy merge). One instance per traces vtab.
//!
//! Concurrency model — identical, and identically NON-NEGOTIABLE: every
//! public method takes &self, guards state with Mutexes, and NOTHING in
//! here uses rayon or spawns threads (PLAN.md Session 3 lesson: store
//! calls re-enter SQLite on the host connection whose mutex the vtab
//! callback thread holds; a worker thread touching the store would
//! deadlock).
//!
//! Differences from BlockEngine, all traced to the trace-store design:
//!   - partition dimension is STATUS not level (3 pure buckets + mixed);
//!   - query terms are always service:/kind:/status:/name: (no index_keys —
//!     see spans/mod.rs for why span dimensions need no allowlist);
//!     exact service/operation discovery adds compound catalog terms;
//!   - every persisted block carries its deduped TRACE-ID set, and the
//!     query path has a second entrance: query() with a trace_id uses
//!     store.query_trace() instead of the term index.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

use super::codec::{
    decode_span_block, decode_span_block_projected, encode_span_block, SpanPredicateRow,
    CODEC_COLUMNAR_V2, CODEC_RAW,
};
use super::{
    status_name, BlockLoc, BlockMeta, EncodedSpanBlock, SpanBlockStore, SpanColumnMask,
    SpanDecodeProfile, SpanDurationBounds, SpanEntry,
};

/// Tuning knobs. All ts_* values are in the SAME opaque unit as
/// SpanEntry.start_ts — the engine never assumes a unit (the traces
/// vtab feeds it nanoseconds and passes 1h-in-ns for the merge cap).
pub struct SpanEngineConfig {
    /// Buffered spans that trigger an automatic flush inside push().
    pub flush_threshold: usize,
    /// zstd level for compressed blocks (7 = the measured sweet spot;
    /// codec 4's per-column zstd strategies use it too).
    pub zstd_level: i32,
    /// optimize() aims for merged blocks of ~this many spans.
    pub merge_target_entries: usize,
    /// HARD CAP on the ts span of a MERGED block — the retention
    /// boundary rule (PLAN.md "Pruning & retention"), same as logs.
    /// Default uncapped (unit-agnostic engine can't pick a default).
    pub merge_max_ts_span: i64,
}

impl Default for SpanEngineConfig {
    fn default() -> Self {
        SpanEngineConfig {
            flush_threshold: 8192,
            zstd_level: 7,
            merge_target_entries: 8192,
            merge_max_ts_span: i64::MAX,
        }
    }
}

/// One F4 bucket row from SpanBlockEngine::bucket_stats.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceBucketStat {
    pub bucket_ts: i64,
    pub service: String,
    pub spans: u64,
    pub errors: u64,
    pub dur_sum: i64,
    pub dur_min: i64,
    pub dur_max: i64,
    /// F7: exact NEAREST-RANK duration percentiles over the bucket's
    /// spans (rank = ceil(q/100 × n), 1-indexed; i64 durations, no
    /// float subtlety). THE trace dashboard numbers.
    pub dur_p50: i64,
    pub dur_p95: i64,
    pub dur_p99: i64,
}

/// One query. ts range is always present (i64::MIN / i64::MAX for
/// "unbounded", like the other vtabs). `trace_id` switches the plan:
/// when set, candidate blocks come from the TRACE INDEX, not the term
/// posting lists — that is the hero pushdown.
#[derive(Clone, Debug)]
pub struct SpanQuery {
    pub ts_min: i64,
    pub ts_max: i64,
    pub trace_id: Option<[u8; 16]>,
    pub service: Option<String>,
    /// Exact kind match (0..=4).
    pub kind: Option<u8>,
    /// Exact status match (0..=2).
    pub status: Option<u8>,
    /// Exact operation-name match.
    pub name: Option<String>,
}

/// Ordering guaranteed by the bounded query path. Equal timestamps use the
/// packed span id as the public deterministic tie-breaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanQueryOrder {
    Asc,
    Desc,
}

struct SpanQueryBlockSnapshot {
    payload: Option<Vec<u8>>,
    location: Option<BlockLoc>,
    meta: BlockMeta,
    sequence: usize,
}

struct SpanQuerySnapshot {
    blocks: Vec<SpanQueryBlockSnapshot>,
    buffered: Vec<SpanEntry>,
    candidate_blocks: u64,
    payload_bytes: u64,
    stable_locations: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct QuerySequence {
    source: usize,
    row: usize,
}

struct BoundedSpan {
    entry: SpanEntry,
    sequence: QuerySequence,
    order: SpanQueryOrder,
}

impl PartialEq for BoundedSpan {
    fn eq(&self, other: &Self) -> bool {
        self.entry.start_ts == other.entry.start_ts
            && self.entry.span_id == other.entry.span_id
            && self.sequence == other.sequence
            && self.order == other.order
    }
}

impl Eq for BoundedSpan {}

impl PartialOrd for BoundedSpan {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for BoundedSpan {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        debug_assert_eq!(self.order, other.order);
        let key = |entry: &SpanEntry| (entry.start_ts, entry.span_id);
        match self.order {
            SpanQueryOrder::Asc => key(&self.entry)
                .cmp(&key(&other.entry))
                .then_with(|| self.sequence.cmp(&other.sequence)),
            SpanQueryOrder::Desc => key(&other.entry)
                .cmp(&key(&self.entry))
                .then_with(|| self.sequence.cmp(&other.sequence)),
        }
    }
}

/// Incremental unbounded cursor state. At most one decoded block and the
/// 8,191-entry live buffer generation are owned at once; database-sized
/// result vectors are never constructed.
pub struct SpanQueryStream {
    query: SpanQuery,
    duration_min: i64,
    duration_max: i64,
    projection: SpanColumnMask,
    blocks: VecDeque<SpanQueryBlockSnapshot>,
    buffered: std::vec::IntoIter<SpanEntry>,
    decoded: std::vec::IntoIter<SpanEntry>,
    started: Instant,
    payload_blocks_read: u64,
    payload_bytes_read: u64,
    decoded_spans: u64,
    decode_profile: SpanDecodeProfile,
    matched_spans: u64,
    returned_spans: u64,
    finished: bool,
}

/// Cumulative successful query work. Direct SQLite/libSQL users can inspect
/// these through `timeless_stats`; hosts take before/after snapshots around a
/// workload rather than inferring work from returned rows.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpanQueryProfileSnapshot {
    pub query_count: u64,
    pub query_cancelled: u64,
    pub query_total_ns: u64,
    pub query_candidate_blocks: u64,
    pub query_payload_blocks_read: u64,
    pub query_payload_bytes_read: u64,
    pub query_decoded_spans: u64,
    pub query_decoded_columns: u64,
    pub query_decoded_column_bytes: u64,
    pub query_materialized_values: u64,
    pub query_materialized_rich_values: u64,
    pub query_buffered_spans_examined: u64,
    pub query_matched_spans: u64,
    pub query_returned_spans: u64,
    pub query_snapshot_ns: u64,
    pub query_snapshot_payload_bytes: u64,
    pub query_snapshot_payload_max_bytes: u64,
    pub query_stable_location_snapshots: u64,
    pub query_bounded_count: u64,
    pub query_bounded_requested_spans: u64,
    pub query_bounded_max_spans: u64,
    pub query_blocks_skipped_by_bound: u64,
    pub discovery_count: u64,
    pub discovery_total_ns: u64,
    pub discovery_payload_bytes_read: u64,
    pub discovery_decoded_spans: u64,
}

#[derive(Default)]
struct SpanQueryProfile {
    query_count: AtomicU64,
    query_cancelled: AtomicU64,
    query_total_ns: AtomicU64,
    query_candidate_blocks: AtomicU64,
    query_payload_blocks_read: AtomicU64,
    query_payload_bytes_read: AtomicU64,
    query_decoded_spans: AtomicU64,
    query_decoded_columns: AtomicU64,
    query_decoded_column_bytes: AtomicU64,
    query_materialized_values: AtomicU64,
    query_materialized_rich_values: AtomicU64,
    query_buffered_spans_examined: AtomicU64,
    query_matched_spans: AtomicU64,
    query_returned_spans: AtomicU64,
    query_snapshot_ns: AtomicU64,
    query_snapshot_payload_bytes: AtomicU64,
    query_snapshot_payload_max_bytes: AtomicU64,
    query_stable_location_snapshots: AtomicU64,
    query_bounded_count: AtomicU64,
    query_bounded_requested_spans: AtomicU64,
    query_bounded_max_spans: AtomicU64,
    query_blocks_skipped_by_bound: AtomicU64,
    discovery_count: AtomicU64,
    discovery_total_ns: AtomicU64,
    discovery_payload_bytes_read: AtomicU64,
    discovery_decoded_spans: AtomicU64,
}

impl SpanQueryProfile {
    fn snapshot(&self) -> SpanQueryProfileSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        SpanQueryProfileSnapshot {
            query_count: load(&self.query_count),
            query_cancelled: load(&self.query_cancelled),
            query_total_ns: load(&self.query_total_ns),
            query_candidate_blocks: load(&self.query_candidate_blocks),
            query_payload_blocks_read: load(&self.query_payload_blocks_read),
            query_payload_bytes_read: load(&self.query_payload_bytes_read),
            query_decoded_spans: load(&self.query_decoded_spans),
            query_decoded_columns: load(&self.query_decoded_columns),
            query_decoded_column_bytes: load(&self.query_decoded_column_bytes),
            query_materialized_values: load(&self.query_materialized_values),
            query_materialized_rich_values: load(&self.query_materialized_rich_values),
            query_buffered_spans_examined: load(&self.query_buffered_spans_examined),
            query_matched_spans: load(&self.query_matched_spans),
            query_returned_spans: load(&self.query_returned_spans),
            query_snapshot_ns: load(&self.query_snapshot_ns),
            query_snapshot_payload_bytes: load(&self.query_snapshot_payload_bytes),
            query_snapshot_payload_max_bytes: load(&self.query_snapshot_payload_max_bytes),
            query_stable_location_snapshots: load(&self.query_stable_location_snapshots),
            query_bounded_count: load(&self.query_bounded_count),
            query_bounded_requested_spans: load(&self.query_bounded_requested_spans),
            query_bounded_max_spans: load(&self.query_bounded_max_spans),
            query_blocks_skipped_by_bound: load(&self.query_blocks_skipped_by_bound),
            discovery_count: load(&self.discovery_count),
            discovery_total_ns: load(&self.discovery_total_ns),
            discovery_payload_bytes_read: load(&self.discovery_payload_bytes_read),
            discovery_decoded_spans: load(&self.discovery_decoded_spans),
        }
    }
}

/// Cumulative successful optimize work. Raw compression and compressed-block
/// merging stay separate so direct hosts can schedule from measured rewrite
/// work rather than treating every maintenance call as one opaque pause.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpanOptimizeProfileSnapshot {
    pub optimize_count: u64,
    pub optimize_total_ns: u64,
    pub optimize_blocks_removed: u64,
    pub optimize_blocks_written: u64,
    pub optimize_budgeted_count: u64,
    pub optimize_budget_entries: u64,
    pub optimize_budget_limited_count: u64,
    pub optimize_raw_groups: u64,
    pub optimize_raw_blocks: u64,
    pub optimize_raw_entries: u64,
    pub optimize_raw_input_bytes: u64,
    pub optimize_raw_output_bytes: u64,
    pub optimize_raw_total_ns: u64,
    pub optimize_merge_groups: u64,
    pub optimize_merge_blocks: u64,
    pub optimize_merge_entries: u64,
    pub optimize_merge_input_bytes: u64,
    pub optimize_merge_output_bytes: u64,
    pub optimize_merge_total_ns: u64,
    pub optimize_duration_backfill_blocks: u64,
    pub optimize_duration_backfill_entries: u64,
    pub optimize_duration_backfill_input_bytes: u64,
    pub optimize_duration_backfill_total_ns: u64,
}

#[derive(Default)]
struct SpanOptimizeProfile {
    optimize_count: AtomicU64,
    optimize_total_ns: AtomicU64,
    optimize_blocks_removed: AtomicU64,
    optimize_blocks_written: AtomicU64,
    optimize_budgeted_count: AtomicU64,
    optimize_budget_entries: AtomicU64,
    optimize_budget_limited_count: AtomicU64,
    optimize_raw_groups: AtomicU64,
    optimize_raw_blocks: AtomicU64,
    optimize_raw_entries: AtomicU64,
    optimize_raw_input_bytes: AtomicU64,
    optimize_raw_output_bytes: AtomicU64,
    optimize_raw_total_ns: AtomicU64,
    optimize_merge_groups: AtomicU64,
    optimize_merge_blocks: AtomicU64,
    optimize_merge_entries: AtomicU64,
    optimize_merge_input_bytes: AtomicU64,
    optimize_merge_output_bytes: AtomicU64,
    optimize_merge_total_ns: AtomicU64,
    optimize_duration_backfill_blocks: AtomicU64,
    optimize_duration_backfill_entries: AtomicU64,
    optimize_duration_backfill_input_bytes: AtomicU64,
    optimize_duration_backfill_total_ns: AtomicU64,
}

impl SpanOptimizeProfile {
    fn snapshot(&self) -> SpanOptimizeProfileSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        SpanOptimizeProfileSnapshot {
            optimize_count: load(&self.optimize_count),
            optimize_total_ns: load(&self.optimize_total_ns),
            optimize_blocks_removed: load(&self.optimize_blocks_removed),
            optimize_blocks_written: load(&self.optimize_blocks_written),
            optimize_budgeted_count: load(&self.optimize_budgeted_count),
            optimize_budget_entries: load(&self.optimize_budget_entries),
            optimize_budget_limited_count: load(&self.optimize_budget_limited_count),
            optimize_raw_groups: load(&self.optimize_raw_groups),
            optimize_raw_blocks: load(&self.optimize_raw_blocks),
            optimize_raw_entries: load(&self.optimize_raw_entries),
            optimize_raw_input_bytes: load(&self.optimize_raw_input_bytes),
            optimize_raw_output_bytes: load(&self.optimize_raw_output_bytes),
            optimize_raw_total_ns: load(&self.optimize_raw_total_ns),
            optimize_merge_groups: load(&self.optimize_merge_groups),
            optimize_merge_blocks: load(&self.optimize_merge_blocks),
            optimize_merge_entries: load(&self.optimize_merge_entries),
            optimize_merge_input_bytes: load(&self.optimize_merge_input_bytes),
            optimize_merge_output_bytes: load(&self.optimize_merge_output_bytes),
            optimize_merge_total_ns: load(&self.optimize_merge_total_ns),
            optimize_duration_backfill_blocks: load(&self.optimize_duration_backfill_blocks),
            optimize_duration_backfill_entries: load(&self.optimize_duration_backfill_entries),
            optimize_duration_backfill_input_bytes: load(
                &self.optimize_duration_backfill_input_bytes,
            ),
            optimize_duration_backfill_total_ns: load(&self.optimize_duration_backfill_total_ns),
        }
    }
}

/// Metadata-only optimizer backlog. Deferred compressed tails do not trigger
/// maintenance until the size-tiered 2x-growth policy can merge them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpanOptimizeBacklog {
    pub raw_blocks: u64,
    pub raw_entries: u64,
    pub merge_ready_groups: u64,
    pub merge_ready_blocks: u64,
    pub merge_ready_entries: u64,
    pub merge_deferred_blocks: u64,
    pub merge_deferred_entries: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SpanOptimizeKind {
    RawCompression,
    CompressedMerge,
}

struct SpanOptimizeGroup {
    sources: Vec<IndexEntry>,
    partition: Option<u8>,
    kind: SpanOptimizeKind,
}

#[derive(Default)]
struct SpanOptimizeOutcome {
    blocks_removed: usize,
    blocks_written: usize,
    budget_limited: bool,
    raw_groups: u64,
    raw_blocks: u64,
    raw_entries: u64,
    raw_input_bytes: u64,
    raw_output_bytes: u64,
    raw_total_ns: u64,
    merge_groups: u64,
    merge_blocks: u64,
    merge_entries: u64,
    merge_input_bytes: u64,
    merge_output_bytes: u64,
    merge_total_ns: u64,
}

#[derive(Default)]
struct SpanDurationBackfillOutcome {
    blocks: u64,
    entries: u64,
    input_bytes: u64,
    total_ns: u64,
    budget_limited: bool,
}

/// One entry in the engine's in-memory block index: persisted metadata
/// plus the STATUS PARTITION tag — the same design as the logs
/// IndexEntry (read blocks/engine.rs for the full "level-term weakness"
/// story; status plays level's role here because 'find the failed
/// requests' is THE trace query and error-pure blocks make
/// `status:error` prune everything else).
///
/// The tag is IN-MEMORY ONLY, re-derived at recovery from the `status:`
/// posting lists (a block under exactly one status: term is pure, ≥2 is
/// mixed — three metadata-only query_terms calls, zero new persistence).
/// `Some(status)` = status-pure; `None` = mixed. Mixed blocks form
/// their own merge partition and never merge with pure ones.
#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    meta: BlockMeta,
    loc: BlockLoc,
    partition: Option<u8>,
}

/// Transaction journal (PLAN.md risk R5) — the spans twin of the
/// blocks TxnJournal, line-for-line (read blocks/engine.rs for the
/// full story, and the metrics engine.rs for the original design
/// rationale). Block/term/trace-index rows ride the host SQLite
/// transaction; engine memory does not — the journal records enough
/// to undo buffer growth, intra-txn drains (flush) and retains
/// (prune), and index entry adds/removals, so txn_rollback leaves the
/// engine exactly as the host rollback leaves the shadow tables.
/// Dedup rule, preconditions and LOCK ORDER (transition → txn → buffer
/// → store callbacks → index) are identical to blocks.
#[derive(Default)]
struct TxnFrame {
    savepoint: Option<i32>,
    buffer_mark: usize,
    saved: Vec<SpanEntry>,
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

pub struct SpanBlockEngine {
    store: Box<dyn SpanBlockStore>,
    config: SpanEngineConfig,
    /// Pins the buffer/store/index generation seen by a complete query.
    transition: RwLock<()>,
    /// Spans pushed but not yet flushed. Queryable (same
    /// queryable-before-flush property as every timeless engine).
    buffer: Mutex<Vec<SpanEntry>>,
    /// In-memory metadata index of every persisted block; optimize()
    /// and prune() plan from this, the query path asks the store.
    index: Mutex<Vec<IndexEntry>>,
    /// True between txn_begin and txn_commit/txn_rollback; an atomic
    /// so the no-transaction fast path costs one load.
    txn_active: AtomicBool,
    txn: Mutex<TxnJournal>,
    /// F2 retention window in NATIVE ts units; 0 = disabled.
    retention_native: AtomicI64,
    /// Last retention cutoff applied (advance guard); i64::MIN = never.
    retention_floor: AtomicI64,
    query_profile: SpanQueryProfile,
    optimize_profile: SpanOptimizeProfile,
}

impl SpanBlockEngine {
    /// Construct over a store, recovering the block index via scan()
    /// and each block's status partition via the `status:` posting
    /// lists (see IndexEntry). Safe to call re-entrantly from
    /// xCreate/xConnect — the calling thread already holds the host
    /// connection.
    pub fn new(store: Box<dyn SpanBlockStore>, config: SpanEngineConfig) -> Result<Self, String> {
        let scanned = store.scan()?;

        // Partition derivation: which blocks carry each of the three
        // status: terms? Exactly one hit = status-pure, several = mixed
        // (0 hits should be impossible — every block emits at least one
        // status term — but is treated as mixed, the conservative
        // bucket, rather than guessed at).
        let mut hits: HashMap<i64, (u32, u8)> = HashMap::new(); // id → (count, last status)
        for st in 0u8..3 {
            let term = vec![format!("status:{}", status_name(st))];
            for (loc, _) in store.query_terms(&term, i64::MIN, i64::MAX)? {
                let e = hits.entry(loc.id).or_insert((0, st));
                e.0 += 1;
                e.1 = st;
            }
        }
        let index = scanned
            .into_iter()
            .map(|(meta, loc)| IndexEntry {
                meta,
                loc,
                partition: match hits.get(&loc.id) {
                    Some((1, st)) => Some(*st),
                    _ => None,
                },
            })
            .collect();

        Ok(SpanBlockEngine {
            store,
            config,
            transition: RwLock::new(()),
            buffer: Mutex::new(Vec::new()),
            index: Mutex::new(index),
            txn_active: AtomicBool::new(false),
            txn: Mutex::new(TxnJournal::default()),
            retention_native: AtomicI64::new(0),
            retention_floor: AtomicI64::new(i64::MIN),
            query_profile: SpanQueryProfile::default(),
            optimize_profile: SpanOptimizeProfile::default(),
        })
    }

    pub fn config(&self) -> &SpanEngineConfig {
        &self.config
    }

    pub fn query_profile(&self) -> SpanQueryProfileSnapshot {
        self.query_profile.snapshot()
    }

    pub fn optimize_profile(&self) -> SpanOptimizeProfileSnapshot {
        self.optimize_profile.snapshot()
    }

    /// Poison-tolerant locks, same style as the rest of timeless-core.
    fn buffer_lock(&self) -> std::sync::MutexGuard<'_, Vec<SpanEntry>> {
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

    // ── Transaction journal API (PLAN.md R5; see blocks/engine.rs) ───

    /// Start journaling — cheap on purpose (one usize mark, capacity-
    /// retaining clears): SQLite calls xBegin before the first write
    /// of EVERY transaction, autocommit statements included. Nested
    /// begins are impossible; savepoints add undo frames instead.
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

    /// Commit: drop the journal (cleared lazily by the next begin).
    pub fn txn_commit(&self) {
        let mut j = self.txn_lock(); // serialize against in-flight recorders
        while let Some(frame) = j.frames.pop() {
            j.spares.push(frame);
        }
        self.txn_active.store(false, Ordering::SeqCst);
    }

    /// Rollback: truncate txn-era buffered spans, restore drained
    /// pre-txn spans, drop index entries whose block rows vanished,
    /// restore entries whose rows came back (verbatim, partition tag
    /// included — host rollback restores the same rowids).
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

    /// Append one span. The SQLite boundary has already validated and
    /// canonicalized its JSON fields; the engine validates compact
    /// enums and auto-flushes at the authoritative threshold.
    pub fn push(&self, entry: SpanEntry) -> Result<(), String> {
        if entry.kind > 4 {
            return Err(format!(
                "invalid span kind {} (0=internal 1=server 2=client 3=producer 4=consumer)",
                entry.kind
            ));
        }
        if entry.status > 2 {
            return Err(format!(
                "invalid span status {} (0=unset 1=ok 2=error)",
                entry.status
            ));
        }
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

    /// F5 bulk append — the spans twin of BlockEngine::push_batch: same
    /// validation/normalization and auto-flush contract as push(), one
    /// buffer lock for the batch, invariants re-checked BEFORE anything
    /// is appended (all-or-nothing).
    pub fn push_batch(&self, entries: Vec<SpanEntry>) -> Result<usize, String> {
        for entry in &entries {
            if entry.kind > 4 {
                return Err(format!(
                    "invalid span kind {} (0=internal 1=server 2=client 3=producer 4=consumer)",
                    entry.kind
                ));
            }
            if entry.status > 2 {
                return Err(format!(
                    "invalid span status {} (0=unset 1=ok 2=error)",
                    entry.status
                ));
            }
        }
        let n = entries.len();
        let should_flush = {
            let mut buf = self.buffer_lock();
            buf.extend(entries);
            buf.len() >= self.config.flush_threshold
        };
        if should_flush {
            self.flush()?;
        }
        Ok(n)
    }

    pub fn buffered_count(&self) -> usize {
        self.buffer_lock().len()
    }

    /// Drain the buffer into RAW blocks — STATUS-PARTITIONED, exactly
    /// like the logs level-partitioned flush: sort by (status,
    /// start_ts), write one status-PURE block per status present (≤3),
    /// so each block emits exactly one `status:` term and the posting-
    /// list intersection prunes perfectly. Each block also records its
    /// deduped trace-id set for the trace index — created in the same
    /// put_blocks operation as the block rows (never dangles). Returns
    /// the number of spans flushed.
    pub fn flush(&self) -> Result<usize, String> {
        let out = self.flush_inner()?;
        self.apply_retention()?;
        Ok(out)
    }

    fn flush_inner(&self) -> Result<usize, String> {
        let _transition = self.transition_write();
        // Transition is already exclusive; journal before buffer/index, then hold
        // the buffer lock for the whole flush (single-threaded in
        // the vtab anyway; correctness is free). The buffer stays
        // intact if any encode or store call fails.
        let mut j = self.txn_guard();
        let mut buf = self.buffer_lock();
        if buf.is_empty() {
            return Ok(0);
        }
        // R5: this flush drains PRE-txn spans (below the mark) into
        // blocks whose rows roll back with the host transaction — and
        // the sort below scrambles positions anyway. Snapshot the
        // pre-txn prefix into the journal and zero the mark.
        if let Some(j) = j.as_deref_mut() {
            if j.buffer_mark > 0 {
                let mark = j.buffer_mark;
                j.saved.extend_from_slice(&buf[..mark]);
                j.buffer_mark = 0;
            }
        }
        buf.sort_by_key(|e| (e.status, e.start_ts));

        let mut blocks: Vec<EncodedSpanBlock> = Vec::new();
        let mut duration_bounds: Vec<SpanDurationBounds> = Vec::new();
        let mut statuses: Vec<u8> = Vec::new(); // partition tag per block
        let mut start = 0usize;
        while start < buf.len() {
            let status = buf[start].status;
            let end = start
                + buf[start..]
                    .iter()
                    .take_while(|e| e.status == status)
                    .count();
            let run = &buf[start..end];
            let (data, meta) = encode_span_block(run, CODEC_RAW, self.config.zstd_level)?;
            blocks.push(EncodedSpanBlock {
                meta,
                data,
                terms: extract_terms(run),
                trace_ids: extract_trace_ids(run),
            });
            duration_bounds.push(span_duration_bounds(run)?);
            statuses.push(status);
            start = end;
        }

        let locs = self
            .store
            .put_blocks_with_duration_bounds(&blocks, &duration_bounds)?;
        {
            let mut index = self.index_lock();
            for ((block, loc), status) in blocks.iter().zip(&locs).zip(&statuses) {
                // R5: blocks born inside a transaction are journaled so
                // rollback can drop their index entries when their rows
                // (and trace-index rows) vanish.
                if let Some(j) = j.as_deref_mut() {
                    j.added.insert(loc.id);
                }
                index.push(IndexEntry {
                    meta: block.meta,
                    loc: *loc,
                    partition: Some(*status),
                });
            }
        }
        let n = buf.len();
        buf.clear();
        Ok(n)
    }

    /// Two-tier compaction ('optimize' command) — the same pass as
    /// blocks/engine.rs::optimize, with STATUS partitions: raw blocks
    /// are recompressed to CODEC_COLUMNAR_V2 (codec 5, adaptive
    /// per-column strategies + typed JSON string columns — legacy codec-2/4
    /// blocks stay decodable and upgrade whenever a merge rewrites
    /// them), small blocks merge toward
    /// merge_target_entries WITHIN their status partition only (merging
    /// an error-pure block into an ok-pure one would re-create exactly
    /// the mixed blocks the partitioned flush prevents), subject to the
    /// merge_max_ts_span retention-boundary cap, all in ONE
    /// replace_blocks call (the SQLite backend rides one host
    /// transaction: new blocks + terms + trace rows in, old ones out,
    /// atomically). Merged blocks recompute BOTH index row sets from
    /// the merged spans. Returns (blocks_removed, blocks_written).
    pub fn optimize(&self) -> Result<(usize, usize), String> {
        self.optimize_with_budget(None)
    }

    pub fn optimize_budgeted(&self, max_entries: usize) -> Result<(usize, usize), String> {
        if max_entries == 0 {
            return Err("optimize span budget must be positive".into());
        }
        self.optimize_with_budget(Some(max_entries))
    }

    fn optimize_with_budget(&self, max_entries: Option<usize>) -> Result<(usize, usize), String> {
        let started = Instant::now();
        let mut out = self.optimize_inner(max_entries)?;
        let rewritten_entries = out.raw_entries.saturating_add(out.merge_entries);
        let backfill_budget = max_entries.map(|budget| {
            budget.saturating_sub(usize::try_from(rewritten_entries).unwrap_or(usize::MAX))
        });
        let backfill = self.backfill_duration_bounds(backfill_budget)?;
        out.budget_limited |= backfill.budget_limited;
        self.apply_retention()?;
        self.optimize_profile
            .optimize_count
            .fetch_add(1, Ordering::Relaxed);
        self.optimize_profile
            .optimize_total_ns
            .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        self.optimize_profile
            .optimize_blocks_removed
            .fetch_add(out.blocks_removed as u64, Ordering::Relaxed);
        self.optimize_profile
            .optimize_blocks_written
            .fetch_add(out.blocks_written as u64, Ordering::Relaxed);
        if let Some(budget) = max_entries {
            self.optimize_profile
                .optimize_budgeted_count
                .fetch_add(1, Ordering::Relaxed);
            self.optimize_profile
                .optimize_budget_entries
                .fetch_add(budget as u64, Ordering::Relaxed);
        }
        if out.budget_limited {
            self.optimize_profile
                .optimize_budget_limited_count
                .fetch_add(1, Ordering::Relaxed);
        }
        for (counter, value) in [
            (&self.optimize_profile.optimize_raw_groups, out.raw_groups),
            (&self.optimize_profile.optimize_raw_blocks, out.raw_blocks),
            (&self.optimize_profile.optimize_raw_entries, out.raw_entries),
            (
                &self.optimize_profile.optimize_raw_input_bytes,
                out.raw_input_bytes,
            ),
            (
                &self.optimize_profile.optimize_raw_output_bytes,
                out.raw_output_bytes,
            ),
            (
                &self.optimize_profile.optimize_raw_total_ns,
                out.raw_total_ns,
            ),
            (
                &self.optimize_profile.optimize_merge_groups,
                out.merge_groups,
            ),
            (
                &self.optimize_profile.optimize_merge_blocks,
                out.merge_blocks,
            ),
            (
                &self.optimize_profile.optimize_merge_entries,
                out.merge_entries,
            ),
            (
                &self.optimize_profile.optimize_merge_input_bytes,
                out.merge_input_bytes,
            ),
            (
                &self.optimize_profile.optimize_merge_output_bytes,
                out.merge_output_bytes,
            ),
            (
                &self.optimize_profile.optimize_merge_total_ns,
                out.merge_total_ns,
            ),
            (
                &self.optimize_profile.optimize_duration_backfill_blocks,
                backfill.blocks,
            ),
            (
                &self.optimize_profile.optimize_duration_backfill_entries,
                backfill.entries,
            ),
            (
                &self.optimize_profile.optimize_duration_backfill_input_bytes,
                backfill.input_bytes,
            ),
            (
                &self.optimize_profile.optimize_duration_backfill_total_ns,
                backfill.total_ns,
            ),
        ] {
            counter.fetch_add(value, Ordering::Relaxed);
        }
        Ok((out.blocks_removed, out.blocks_written))
    }

    fn backfill_duration_bounds(
        &self,
        max_entries: Option<usize>,
    ) -> Result<SpanDurationBackfillOutcome, String> {
        let _transition = self.transition_write();
        let candidates = self.store.blocks_missing_duration_bounds()?;
        if candidates.is_empty() {
            return Ok(SpanDurationBackfillOutcome::default());
        }

        let started = Instant::now();
        let mut selected = Vec::new();
        let mut selected_entries = 0usize;
        let mut budget_limited = false;
        for (location, meta) in candidates {
            let entries = meta.entry_count as usize;
            if max_entries == Some(0)
                || (!selected.is_empty()
                    && max_entries
                        .is_some_and(|budget| selected_entries.saturating_add(entries) > budget))
            {
                budget_limited = true;
                break;
            }
            selected_entries = selected_entries.saturating_add(entries);
            selected.push((location, meta));
        }

        let mut updates = Vec::with_capacity(selected.len());
        let mut input_bytes = 0u64;
        for (location, meta) in &selected {
            let bytes = self.store.read_block(location)?;
            input_bytes = input_bytes.saturating_add(bytes.len() as u64);
            let entries = decode_span_block(&bytes)?;
            if entries.len() != meta.entry_count as usize {
                return Err(format!(
                    "span block {} metadata declares {} entries but payload decoded {}",
                    location.id,
                    meta.entry_count,
                    entries.len()
                ));
            }
            updates.push((*location, span_duration_bounds(&entries)?));
        }
        self.store.update_duration_bounds(&updates)?;

        Ok(SpanDurationBackfillOutcome {
            blocks: updates.len() as u64,
            entries: selected_entries as u64,
            input_bytes,
            total_ns: elapsed_ns(started),
            budget_limited,
        })
    }

    pub fn optimize_backlog(&self) -> SpanOptimizeBacklog {
        let candidates: Vec<IndexEntry> = self
            .index_lock()
            .iter()
            .filter(|entry| {
                entry.meta.codec == CODEC_RAW
                    || (entry.meta.entry_count as usize) < self.config.merge_target_entries
            })
            .copied()
            .collect();
        let groups = self.plan_optimize(&candidates);
        let mut backlog = SpanOptimizeBacklog::default();
        let mut ready_compressed = HashSet::new();
        for group in &groups {
            let entries = group
                .sources
                .iter()
                .map(|entry| entry.meta.entry_count as u64)
                .sum::<u64>();
            match group.kind {
                SpanOptimizeKind::RawCompression => {
                    backlog.raw_blocks += group.sources.len() as u64;
                    backlog.raw_entries += entries;
                }
                SpanOptimizeKind::CompressedMerge => {
                    backlog.merge_ready_groups += 1;
                    backlog.merge_ready_blocks += group.sources.len() as u64;
                    backlog.merge_ready_entries += entries;
                    ready_compressed.extend(group.sources.iter().map(|entry| entry.loc.id));
                }
            }
        }
        for entry in candidates.iter().filter(|entry| {
            entry.meta.codec != CODEC_RAW && !ready_compressed.contains(&entry.loc.id)
        }) {
            backlog.merge_deferred_blocks += 1;
            backlog.merge_deferred_entries += entry.meta.entry_count as u64;
        }
        backlog
    }

    fn plan_optimize(&self, candidates: &[IndexEntry]) -> Vec<SpanOptimizeGroup> {
        let mut raw_buckets: [Vec<IndexEntry>; 4] = Default::default();
        let mut compressed_buckets: [Vec<IndexEntry>; 4] = Default::default();
        for entry in candidates {
            let bucket = entry.partition.map_or(3, usize::from);
            if entry.meta.codec == CODEC_RAW {
                raw_buckets[bucket].push(*entry);
            } else {
                compressed_buckets[bucket].push(*entry);
            }
        }
        let mut groups = Vec::new();
        for bucket in 0..4 {
            let partition = (bucket < 3).then_some(bucket as u8);
            groups
                .extend(self.plan_raw_groups(std::mem::take(&mut raw_buckets[bucket]), partition));
            groups.extend(self.plan_compressed_groups(
                std::mem::take(&mut compressed_buckets[bucket]),
                partition,
            ));
        }
        groups.sort_by_key(|group| {
            (
                group.kind,
                group
                    .sources
                    .iter()
                    .map(|entry| entry.meta.ts_min)
                    .min()
                    .unwrap_or(i64::MAX),
                group.partition,
            )
        });
        groups
    }

    fn plan_raw_groups(
        &self,
        mut entries: Vec<IndexEntry>,
        partition: Option<u8>,
    ) -> Vec<SpanOptimizeGroup> {
        entries.sort_by_key(|entry| (entry.meta.ts_min, entry.meta.ts_max));
        let mut groups = Vec::new();
        let mut current = Vec::new();
        let mut current_entries = 0usize;
        let (mut current_min, mut current_max) = (0i64, 0i64);
        for entry in entries {
            let fits = current.is_empty()
                || (current_entries.saturating_add(entry.meta.entry_count as usize)
                    <= self.config.merge_target_entries
                    && Self::merged_span_fits(
                        current_min,
                        current_max,
                        entry.meta,
                        self.config.merge_max_ts_span,
                    ));
            if !fits {
                groups.push(SpanOptimizeGroup {
                    sources: std::mem::take(&mut current),
                    partition,
                    kind: SpanOptimizeKind::RawCompression,
                });
                current_entries = 0;
            }
            if current.is_empty() {
                current_min = entry.meta.ts_min;
                current_max = entry.meta.ts_max;
            } else {
                current_min = current_min.min(entry.meta.ts_min);
                current_max = current_max.max(entry.meta.ts_max);
            }
            current_entries = current_entries.saturating_add(entry.meta.entry_count as usize);
            current.push(entry);
        }
        if !current.is_empty() {
            groups.push(SpanOptimizeGroup {
                sources: current,
                partition,
                kind: SpanOptimizeKind::RawCompression,
            });
        }
        groups
    }

    fn plan_compressed_groups(
        &self,
        mut entries: Vec<IndexEntry>,
        partition: Option<u8>,
    ) -> Vec<SpanOptimizeGroup> {
        entries.sort_by_key(|entry| (entry.meta.ts_min, entry.meta.ts_max));
        let mut groups = Vec::new();
        let mut segment = Vec::new();
        let (mut segment_min, mut segment_max) = (0i64, 0i64);
        for entry in entries {
            let fits = segment.is_empty()
                || Self::merged_span_fits(
                    segment_min,
                    segment_max,
                    entry.meta,
                    self.config.merge_max_ts_span,
                );
            if !fits {
                self.plan_compressed_segment(std::mem::take(&mut segment), partition, &mut groups);
            }
            if segment.is_empty() {
                segment_min = entry.meta.ts_min;
                segment_max = entry.meta.ts_max;
            } else {
                segment_min = segment_min.min(entry.meta.ts_min);
                segment_max = segment_max.max(entry.meta.ts_max);
            }
            segment.push(entry);
        }
        self.plan_compressed_segment(segment, partition, &mut groups);
        groups
    }

    fn plan_compressed_segment(
        &self,
        mut entries: Vec<IndexEntry>,
        partition: Option<u8>,
        groups: &mut Vec<SpanOptimizeGroup>,
    ) {
        entries.sort_by_key(|entry| (entry.meta.entry_count, entry.meta.ts_min, entry.meta.ts_max));
        let mut current = Vec::new();
        let mut current_entries = 0usize;
        let merge_limit = self
            .config
            .merge_target_entries
            .saturating_add(self.config.merge_target_entries.div_ceil(4));
        for entry in entries {
            let count = entry.meta.entry_count as usize;
            if !current.is_empty() && current_entries.saturating_add(count) > merge_limit {
                self.push_compressed_group(std::mem::take(&mut current), partition, groups);
                current_entries = 0;
            }
            current_entries = current_entries.saturating_add(count);
            current.push(entry);
        }
        self.push_compressed_group(current, partition, groups);
    }

    fn push_compressed_group(
        &self,
        sources: Vec<IndexEntry>,
        partition: Option<u8>,
        groups: &mut Vec<SpanOptimizeGroup>,
    ) {
        if sources.len() < 2 {
            return;
        }
        let entries = sources
            .iter()
            .map(|entry| entry.meta.entry_count as usize)
            .sum::<usize>();
        let largest = sources
            .iter()
            .map(|entry| entry.meta.entry_count as usize)
            .max()
            .unwrap_or(0);
        let minimum_fill = self.config.merge_target_entries.div_ceil(2);
        if entries >= minimum_fill && entries >= largest.saturating_mul(2) {
            groups.push(SpanOptimizeGroup {
                sources,
                partition,
                kind: SpanOptimizeKind::CompressedMerge,
            });
        }
    }

    fn merged_span_fits(
        current_min: i64,
        current_max: i64,
        next: BlockMeta,
        max_span: i64,
    ) -> bool {
        current_max
            .max(next.ts_max)
            .saturating_sub(current_min.min(next.ts_min))
            <= max_span
    }

    fn optimize_inner(&self, max_entries: Option<usize>) -> Result<SpanOptimizeOutcome, String> {
        let _transition = self.transition_write();
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
            return Ok(SpanOptimizeOutcome::default());
        }
        let planned = self.plan_optimize(&candidates);
        if planned.is_empty() {
            return Ok(SpanOptimizeOutcome::default());
        }

        let mut selected = Vec::new();
        let mut selected_entries = 0usize;
        let mut budget_limited = false;
        for group in planned {
            let entries = group
                .sources
                .iter()
                .map(|entry| entry.meta.entry_count as usize)
                .sum::<usize>();
            if let Some(budget) = max_entries {
                if !selected.is_empty() && selected_entries.saturating_add(entries) > budget {
                    budget_limited = true;
                    break;
                }
            }
            selected_entries = selected_entries.saturating_add(entries);
            selected.push(group);
        }

        let mut adds: Vec<EncodedSpanBlock> = Vec::new();
        let mut add_duration_bounds: Vec<SpanDurationBounds> = Vec::new();
        let mut add_partitions: Vec<Option<u8>> = Vec::new();
        let mut removes: Vec<BlockLoc> = Vec::new();
        let mut outcome = SpanOptimizeOutcome {
            budget_limited,
            ..SpanOptimizeOutcome::default()
        };
        for group in &selected {
            let phase_started = Instant::now();
            let expected_entries = group
                .sources
                .iter()
                .map(|entry| entry.meta.entry_count as usize)
                .sum();
            let mut entries: Vec<SpanEntry> = Vec::with_capacity(expected_entries);
            let mut input_bytes = 0u64;
            for source in &group.sources {
                let bytes = self.store.read_block(&source.loc)?;
                input_bytes = input_bytes.saturating_add(bytes.len() as u64);
                entries.extend(decode_span_block(&bytes)?);
            }
            entries.sort_by_key(|entry| entry.start_ts);
            let terms = extract_terms(&entries);
            let trace_ids = extract_trace_ids(&entries);
            let (data, meta) =
                encode_span_block(&entries, CODEC_COLUMNAR_V2, self.config.zstd_level)?;
            let output_bytes = data.len() as u64;
            adds.push(EncodedSpanBlock {
                meta,
                data,
                terms,
                trace_ids,
            });
            add_duration_bounds.push(span_duration_bounds(&entries)?);
            add_partitions.push(group.partition);
            removes.extend(group.sources.iter().map(|entry| entry.loc));
            let elapsed = elapsed_ns(phase_started);
            match group.kind {
                SpanOptimizeKind::RawCompression => {
                    outcome.raw_groups += 1;
                    outcome.raw_blocks += group.sources.len() as u64;
                    outcome.raw_entries += entries.len() as u64;
                    outcome.raw_input_bytes = outcome.raw_input_bytes.saturating_add(input_bytes);
                    outcome.raw_output_bytes =
                        outcome.raw_output_bytes.saturating_add(output_bytes);
                    outcome.raw_total_ns = outcome.raw_total_ns.saturating_add(elapsed);
                }
                SpanOptimizeKind::CompressedMerge => {
                    outcome.merge_groups += 1;
                    outcome.merge_blocks += group.sources.len() as u64;
                    outcome.merge_entries += entries.len() as u64;
                    outcome.merge_input_bytes =
                        outcome.merge_input_bytes.saturating_add(input_bytes);
                    outcome.merge_output_bytes =
                        outcome.merge_output_bytes.saturating_add(output_bytes);
                    outcome.merge_total_ns = outcome.merge_total_ns.saturating_add(elapsed);
                }
            }
        }
        if adds.is_empty() {
            return Ok(outcome);
        }

        // One atomic swap; the callback rewrites the in-memory index at
        // the moment both generations exist in the store.
        //
        // Journal (R5): grabbed BEFORE replace_blocks so the lock order
        // inside the callback stays txn → index. Same rules as blocks:
        // removed pre-txn entries journaled verbatim (host rollback
        // restores their rows — and trace-index rows — under the same
        // rowids), intra-txn blocks cancel their own add, new blocks
        // journal their locs.
        let mut j = self.txn_guard();
        let add_metas: Vec<BlockMeta> = adds.iter().map(|b| b.meta).collect();
        outcome.blocks_removed = removes.len();
        outcome.blocks_written = add_metas.len();
        self.store.replace_blocks_with_duration_bounds(
            &adds,
            &add_duration_bounds,
            &removes,
            &mut |new_locs: &[BlockLoc]| {
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
            },
        )?;
        drop(j);
        Ok(outcome)
    }

    /// Retention: delete every block whose ts_max < cutoff plus any
    /// buffered spans older than the cutoff. The store removes term AND
    /// trace-index rows in the same operation (never-dangle rule).
    /// Returns the number of blocks deleted.
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
        // Transition is already exclusive; journal before buffer/index. Same
        // prefix-snapshot trick as flush: the buffer retain may drop
        // PRE-txn spans and shifts positions, so preserve
        // buffer[..mark] into `saved` and zero the mark before
        // mutating; index removals journal their entries (host
        // rollback restores the rows).
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
            buf.retain(|e| e.start_ts >= cutoff);
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

    /// Materialized engine query retained for direct core callers and kernels.
    /// The SQLite vtab uses `query_stream_after_snapshot` for unbounded scans
    /// and this bounded path for ordered LIMIT/OFFSET queries.
    pub fn query(&self, q: &SpanQuery) -> Result<Vec<SpanEntry>, String> {
        self.query_ordered_after_snapshot(q, SpanQueryOrder::Asc, None, || {})
    }

    pub fn query_bounded(
        &self,
        q: &SpanQuery,
        order: SpanQueryOrder,
        max_spans: usize,
    ) -> Result<Vec<SpanEntry>, String> {
        self.query_ordered_after_snapshot(q, order, Some(max_spans), || {})
    }

    pub fn query_ordered_after_snapshot<F>(
        &self,
        q: &SpanQuery,
        order: SpanQueryOrder,
        max_spans: Option<usize>,
        after_snapshot: F,
    ) -> Result<Vec<SpanEntry>, String>
    where
        F: FnOnce(),
    {
        self.query_ordered_with_duration_after_snapshot(
            q,
            i64::MIN,
            i64::MAX,
            order,
            max_spans,
            after_snapshot,
        )
    }

    /// Duration-aware ordered query used by the traces vtab. Duration bounds
    /// are kept out of `SpanQuery` so the existing public core struct remains
    /// source-compatible for direct users.
    pub fn query_ordered_with_duration_after_snapshot<F>(
        &self,
        q: &SpanQuery,
        duration_min: i64,
        duration_max: i64,
        order: SpanQueryOrder,
        max_spans: Option<usize>,
        after_snapshot: F,
    ) -> Result<Vec<SpanEntry>, String>
    where
        F: FnOnce(),
    {
        self.query_ordered_projected_with_duration_after_snapshot(
            q,
            duration_min,
            duration_max,
            order,
            max_spans,
            SpanColumnMask::ALL,
            after_snapshot,
        )
    }

    /// Projection-aware bounded query used by the SQLite trace vtable. The
    /// returned entries populate the requested columns plus internal predicate
    /// and ordering columns; callers must read only their declared projection.
    #[allow(clippy::too_many_arguments)]
    pub fn query_ordered_projected_with_duration_after_snapshot<F>(
        &self,
        q: &SpanQuery,
        duration_min: i64,
        duration_max: i64,
        order: SpanQueryOrder,
        max_spans: Option<usize>,
        projection: SpanColumnMask,
        after_snapshot: F,
    ) -> Result<Vec<SpanEntry>, String>
    where
        F: FnOnce(),
    {
        let started = Instant::now();
        let snapshot_started = Instant::now();
        let snapshot = self.snapshot_query(q, duration_min, duration_max)?;
        let snapshot_ns = elapsed_ns(snapshot_started);
        after_snapshot();

        let candidate_blocks = snapshot.candidate_blocks;
        let buffered_spans_examined = snapshot.buffered.len() as u64;
        let snapshot_payload_bytes = snapshot.payload_bytes;
        let stable_locations = snapshot.stable_locations;
        let mut payload_blocks_read = if stable_locations {
            0
        } else {
            candidate_blocks
        };
        let mut payload_bytes_read = snapshot_payload_bytes;
        let mut decoded_spans = 0_u64;
        let mut decode_profile = SpanDecodeProfile::default();
        let mut matched_spans = 0_u64;
        let mut blocks_skipped_by_bound = 0_u64;
        let mut out;

        if let Some(capacity) = max_spans {
            let mut blocks = snapshot.blocks;
            match order {
                SpanQueryOrder::Asc => {
                    blocks.sort_by_key(|block| (block.meta.ts_min, block.sequence))
                }
                SpanQueryOrder::Desc => blocks.sort_by(|a, b| {
                    b.meta
                        .ts_max
                        .cmp(&a.meta.ts_max)
                        .then_with(|| a.sequence.cmp(&b.sequence))
                }),
            }
            let mut heap: BinaryHeap<BoundedSpan> = BinaryHeap::new();
            let buffered_source = candidate_blocks as usize;
            for (row, entry) in snapshot.buffered.into_iter().enumerate() {
                matched_spans = matched_spans.saturating_add(1);
                Self::retain_bounded(
                    &mut heap,
                    BoundedSpan {
                        entry,
                        sequence: QuerySequence {
                            source: buffered_source,
                            row,
                        },
                        order,
                    },
                    capacity,
                );
            }
            let block_count = blocks.len();
            for (position, block) in blocks.into_iter().enumerate() {
                let cannot_displace = capacity == 0
                    || (heap.len() == capacity
                        && heap.peek().is_some_and(|worst| match order {
                            SpanQueryOrder::Asc => block.meta.ts_min > worst.entry.start_ts,
                            SpanQueryOrder::Desc => block.meta.ts_max < worst.entry.start_ts,
                        }));
                if cannot_displace {
                    blocks_skipped_by_bound =
                        blocks_skipped_by_bound.saturating_add((block_count - position) as u64);
                    break;
                }
                let (bytes, store_read) = self.query_block_bytes(block)?;
                payload_blocks_read = payload_blocks_read.saturating_add(store_read);
                if store_read != 0 {
                    payload_bytes_read = payload_bytes_read.saturating_add(bytes.len() as u64);
                }
                let predicate_mask = query_predicate_mask(q, duration_min, duration_max);
                let output_mask = projection
                    .union(SpanColumnMask::START_TS)
                    .union(SpanColumnMask::SPAN_ID);
                let (entries, profile) =
                    decode_span_block_projected(&bytes, predicate_mask, output_mask, |row| {
                        predicate_row_matches(row, q, duration_min, duration_max)
                    })?;
                decoded_spans = decoded_spans.saturating_add(profile.examined_spans);
                decode_profile.add(profile);
                for (row, entry) in entries.into_iter().enumerate() {
                    matched_spans = matched_spans.saturating_add(1);
                    Self::retain_bounded(
                        &mut heap,
                        BoundedSpan {
                            entry,
                            sequence: QuerySequence {
                                source: position,
                                row,
                            },
                            order,
                        },
                        capacity,
                    );
                }
            }
            let mut ranked = heap.into_vec();
            ranked.sort_by(|a, b| compare_spans(&a.entry, &b.entry, order));
            out = ranked.into_iter().map(|ranked| ranked.entry).collect();
        } else {
            out = Vec::new();
            for block in snapshot.blocks {
                let (bytes, store_read) = self.query_block_bytes(block)?;
                payload_blocks_read = payload_blocks_read.saturating_add(store_read);
                if store_read != 0 {
                    payload_bytes_read = payload_bytes_read.saturating_add(bytes.len() as u64);
                }
                let predicate_mask = query_predicate_mask(q, duration_min, duration_max);
                let output_mask = projection
                    .union(SpanColumnMask::START_TS)
                    .union(SpanColumnMask::SPAN_ID);
                let (entries, profile) =
                    decode_span_block_projected(&bytes, predicate_mask, output_mask, |row| {
                        predicate_row_matches(row, q, duration_min, duration_max)
                    })?;
                decoded_spans = decoded_spans.saturating_add(profile.examined_spans);
                decode_profile.add(profile);
                matched_spans = matched_spans.saturating_add(entries.len() as u64);
                out.extend(entries);
            }
            matched_spans = matched_spans.saturating_add(snapshot.buffered.len() as u64);
            out.extend(snapshot.buffered);
            out.sort_by(|a, b| compare_spans(a, b, order));
        }
        self.store
            .check_cancelled()
            .map_err(|error| self.query_error(error))?;
        self.record_query(
            started,
            snapshot_ns,
            candidate_blocks,
            payload_blocks_read,
            payload_bytes_read,
            snapshot_payload_bytes,
            stable_locations,
            decoded_spans,
            buffered_spans_examined,
            matched_spans,
            out.len() as u64,
            decode_profile,
        );
        if let Some(capacity) = max_spans {
            let requested = capacity as u64;
            self.query_profile
                .query_bounded_count
                .fetch_add(1, Ordering::Relaxed);
            self.query_profile
                .query_bounded_requested_spans
                .fetch_add(requested, Ordering::Relaxed);
            self.query_profile
                .query_bounded_max_spans
                .fetch_max(requested, Ordering::Relaxed);
            self.query_profile
                .query_blocks_skipped_by_bound
                .fetch_add(blocks_skipped_by_bound, Ordering::Relaxed);
        }
        Ok(out)
    }

    /// Capture one stable query generation, then let the vtab stream it a
    /// block at a time after releasing the cross-connection read permit.
    pub fn query_stream_after_snapshot<F>(
        &self,
        q: &SpanQuery,
        after_snapshot: F,
    ) -> Result<SpanQueryStream, String>
    where
        F: FnOnce(),
    {
        self.query_stream_with_duration_after_snapshot(q, i64::MIN, i64::MAX, after_snapshot)
    }

    pub fn query_stream_with_duration_after_snapshot<F>(
        &self,
        q: &SpanQuery,
        duration_min: i64,
        duration_max: i64,
        after_snapshot: F,
    ) -> Result<SpanQueryStream, String>
    where
        F: FnOnce(),
    {
        self.query_stream_projected_with_duration_after_snapshot(
            q,
            duration_min,
            duration_max,
            SpanColumnMask::ALL,
            after_snapshot,
        )
    }

    /// Projection-aware streaming query used by SQLite unbounded scans and
    /// scalar trace kernels.
    pub fn query_stream_projected_with_duration_after_snapshot<F>(
        &self,
        q: &SpanQuery,
        duration_min: i64,
        duration_max: i64,
        projection: SpanColumnMask,
        after_snapshot: F,
    ) -> Result<SpanQueryStream, String>
    where
        F: FnOnce(),
    {
        let started = Instant::now();
        let snapshot_started = Instant::now();
        let snapshot = self.snapshot_query(q, duration_min, duration_max)?;
        let snapshot_ns = elapsed_ns(snapshot_started);
        after_snapshot();
        self.query_profile
            .query_count
            .fetch_add(1, Ordering::Relaxed);
        self.query_profile
            .query_snapshot_ns
            .fetch_add(snapshot_ns, Ordering::Relaxed);
        self.query_profile
            .query_candidate_blocks
            .fetch_add(snapshot.candidate_blocks, Ordering::Relaxed);
        self.query_profile
            .query_buffered_spans_examined
            .fetch_add(snapshot.buffered.len() as u64, Ordering::Relaxed);
        self.query_profile
            .query_snapshot_payload_bytes
            .fetch_add(snapshot.payload_bytes, Ordering::Relaxed);
        self.query_profile
            .query_snapshot_payload_max_bytes
            .fetch_max(snapshot.payload_bytes, Ordering::Relaxed);
        if snapshot.stable_locations {
            self.query_profile
                .query_stable_location_snapshots
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(SpanQueryStream {
            query: q.clone(),
            duration_min,
            duration_max,
            blocks: snapshot.blocks.into(),
            buffered: snapshot.buffered.into_iter(),
            decoded: Vec::new().into_iter(),
            started,
            payload_blocks_read: if snapshot.stable_locations {
                0
            } else {
                snapshot.candidate_blocks
            },
            payload_bytes_read: snapshot.payload_bytes,
            decoded_spans: 0,
            decode_profile: SpanDecodeProfile::default(),
            matched_spans: 0,
            returned_spans: 0,
            finished: false,
            projection,
        })
    }

    pub fn query_stream_next(
        &self,
        stream: &mut SpanQueryStream,
    ) -> Result<Option<SpanEntry>, String> {
        loop {
            if let Some(entry) = stream.decoded.next() {
                stream.returned_spans = stream.returned_spans.saturating_add(1);
                return Ok(Some(entry));
            }
            if let Some(block) = stream.blocks.pop_front() {
                let (bytes, store_read) = self.query_block_bytes(block)?;
                stream.payload_blocks_read = stream.payload_blocks_read.saturating_add(store_read);
                if store_read != 0 {
                    stream.payload_bytes_read =
                        stream.payload_bytes_read.saturating_add(bytes.len() as u64);
                }
                let predicate_mask =
                    query_predicate_mask(&stream.query, stream.duration_min, stream.duration_max);
                let (entries, profile) = decode_span_block_projected(
                    &bytes,
                    predicate_mask,
                    stream.projection,
                    |row| {
                        predicate_row_matches(
                            row,
                            &stream.query,
                            stream.duration_min,
                            stream.duration_max,
                        )
                    },
                )?;
                stream.decoded_spans = stream.decoded_spans.saturating_add(profile.examined_spans);
                stream.matched_spans = stream.matched_spans.saturating_add(entries.len() as u64);
                stream.decode_profile.add(profile);
                self.store
                    .check_cancelled()
                    .map_err(|error| self.query_error(error))?;
                stream.decoded = entries.into_iter();
                continue;
            }
            if let Some(entry) = stream.buffered.next() {
                stream.matched_spans = stream.matched_spans.saturating_add(1);
                stream.returned_spans = stream.returned_spans.saturating_add(1);
                return Ok(Some(entry));
            }
            self.finish_query_stream(stream);
            return Ok(None);
        }
    }

    pub fn finish_query_stream(&self, stream: &mut SpanQueryStream) {
        if stream.finished {
            return;
        }
        stream.finished = true;
        self.query_profile
            .query_total_ns
            .fetch_add(elapsed_ns(stream.started), Ordering::Relaxed);
        self.query_profile
            .query_payload_blocks_read
            .fetch_add(stream.payload_blocks_read, Ordering::Relaxed);
        self.query_profile
            .query_payload_bytes_read
            .fetch_add(stream.payload_bytes_read, Ordering::Relaxed);
        self.query_profile
            .query_decoded_spans
            .fetch_add(stream.decoded_spans, Ordering::Relaxed);
        self.query_profile
            .query_decoded_columns
            .fetch_add(stream.decode_profile.columns, Ordering::Relaxed);
        self.query_profile
            .query_decoded_column_bytes
            .fetch_add(stream.decode_profile.column_bytes, Ordering::Relaxed);
        self.query_profile
            .query_materialized_values
            .fetch_add(stream.decode_profile.materialized_values, Ordering::Relaxed);
        self.query_profile.query_materialized_rich_values.fetch_add(
            stream.decode_profile.materialized_rich_values,
            Ordering::Relaxed,
        );
        self.query_profile
            .query_matched_spans
            .fetch_add(stream.matched_spans, Ordering::Relaxed);
        self.query_profile
            .query_returned_spans
            .fetch_add(stream.returned_spans, Ordering::Relaxed);
    }

    fn snapshot_query(
        &self,
        q: &SpanQuery,
        duration_min: i64,
        duration_max: i64,
    ) -> Result<SpanQuerySnapshot, String> {
        let _transition = self.transition_read();
        self.validate_query(q)?;
        let locs = self.query_locations(q, duration_min, duration_max)?;
        let candidate_blocks = locs.len() as u64;
        let stable_locations = self.store.query_snapshot_keeps_locations_readable();
        let mut blocks = Vec::with_capacity(locs.len());
        let mut payload_bytes = 0_u64;
        for (sequence, (location, meta)) in locs.into_iter().enumerate() {
            if stable_locations {
                blocks.push(SpanQueryBlockSnapshot {
                    payload: None,
                    location: Some(location),
                    meta,
                    sequence,
                });
            } else {
                let bytes = self
                    .store
                    .read_block(&location)
                    .map_err(|error| self.query_error(error))?;
                payload_bytes = payload_bytes.saturating_add(bytes.len() as u64);
                blocks.push(SpanQueryBlockSnapshot {
                    payload: Some(bytes),
                    location: None,
                    meta,
                    sequence,
                });
            }
        }
        let buffered = self
            .buffer_lock()
            .iter()
            .filter(|entry| entry_matches(entry, q, duration_min, duration_max))
            .cloned()
            .collect();
        Ok(SpanQuerySnapshot {
            blocks,
            buffered,
            candidate_blocks,
            payload_bytes,
            stable_locations,
        })
    }

    fn validate_query(&self, q: &SpanQuery) -> Result<(), String> {
        if q.kind.is_some_and(|kind| kind > 4) {
            return Err(format!("invalid kind {} in query", q.kind.unwrap()));
        }
        if q.status.is_some_and(|status| status > 2) {
            return Err(format!("invalid status {} in query", q.status.unwrap()));
        }
        Ok(())
    }

    fn query_locations(
        &self,
        q: &SpanQuery,
        duration_min: i64,
        duration_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        if duration_min > duration_max {
            return Ok(Vec::new());
        }
        match &q.trace_id {
            Some(trace_id) => Ok(self
                .store
                .query_trace_with_duration_bounds(
                    trace_id,
                    q.ts_min,
                    q.ts_max,
                    duration_min,
                    duration_max,
                )
                .map_err(|error| self.query_error(error))?
                .into_iter()
                .filter(|(_, meta)| meta.ts_min <= q.ts_max && meta.ts_max >= q.ts_min)
                .collect()),
            None => {
                let mut terms = Vec::new();
                if let Some(service) = &q.service {
                    terms.push(format!("service:{service}"));
                }
                if let Some(kind) = q.kind {
                    terms.push(format!("kind:{}", super::kind_name(kind)));
                }
                if let Some(status) = q.status {
                    terms.push(format!("status:{}", status_name(status)));
                }
                if let Some(name) = &q.name {
                    terms.push(format!("name:{name}"));
                }
                self.store
                    .query_terms_with_duration_bounds(
                        &terms,
                        q.ts_min,
                        q.ts_max,
                        duration_min,
                        duration_max,
                    )
                    .map_err(|error| self.query_error(error))
            }
        }
    }

    fn query_block_bytes(&self, block: SpanQueryBlockSnapshot) -> Result<(Vec<u8>, u64), String> {
        match (block.payload, block.location) {
            (Some(bytes), None) => Ok((bytes, 0)),
            (None, Some(location)) => self
                .store
                .read_block(&location)
                .map(|bytes| (bytes, 1))
                .map_err(|error| self.query_error(error)),
            _ => Err("invalid span query block snapshot".into()),
        }
    }

    fn retain_bounded(heap: &mut BinaryHeap<BoundedSpan>, span: BoundedSpan, capacity: usize) {
        if capacity == 0 {
            return;
        }
        if heap.len() < capacity {
            heap.push(span);
        } else if heap.peek().is_some_and(|worst| span < *worst) {
            let _ = heap.pop();
            heap.push(span);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_query(
        &self,
        started: Instant,
        snapshot_ns: u64,
        candidate_blocks: u64,
        payload_blocks_read: u64,
        payload_bytes_read: u64,
        snapshot_payload_bytes: u64,
        stable_locations: bool,
        decoded_spans: u64,
        buffered_spans_examined: u64,
        matched_spans: u64,
        returned_spans: u64,
        decode_profile: SpanDecodeProfile,
    ) {
        for (counter, value) in [
            (&self.query_profile.query_count, 1),
            (&self.query_profile.query_total_ns, elapsed_ns(started)),
            (&self.query_profile.query_snapshot_ns, snapshot_ns),
            (&self.query_profile.query_candidate_blocks, candidate_blocks),
            (
                &self.query_profile.query_payload_blocks_read,
                payload_blocks_read,
            ),
            (
                &self.query_profile.query_payload_bytes_read,
                payload_bytes_read,
            ),
            (
                &self.query_profile.query_snapshot_payload_bytes,
                snapshot_payload_bytes,
            ),
            (&self.query_profile.query_decoded_spans, decoded_spans),
            (
                &self.query_profile.query_decoded_columns,
                decode_profile.columns,
            ),
            (
                &self.query_profile.query_decoded_column_bytes,
                decode_profile.column_bytes,
            ),
            (
                &self.query_profile.query_materialized_values,
                decode_profile.materialized_values,
            ),
            (
                &self.query_profile.query_materialized_rich_values,
                decode_profile.materialized_rich_values,
            ),
            (
                &self.query_profile.query_buffered_spans_examined,
                buffered_spans_examined,
            ),
            (&self.query_profile.query_matched_spans, matched_spans),
            (&self.query_profile.query_returned_spans, returned_spans),
        ] {
            counter.fetch_add(value, Ordering::Relaxed);
        }
        self.query_profile
            .query_snapshot_payload_max_bytes
            .fetch_max(snapshot_payload_bytes, Ordering::Relaxed);
        if stable_locations {
            self.query_profile
                .query_stable_location_snapshots
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Exact service discovery from the public posting-list catalog. The
    /// bounded live buffer is merged so queryable-before-flush semantics are
    /// identical to row scans. Stores without catalog discovery fall back to
    /// an exact streamed decode.
    pub fn discover_services(&self) -> Result<Vec<String>, String> {
        let started = Instant::now();
        let (catalog, buffered) = {
            let _transition = self.transition_read();
            let catalog = self
                .store
                .query_term_values("service:")
                .map_err(|error| self.query_error(error))?;
            let buffered = self
                .buffer_lock()
                .iter()
                .map(|span| span.service.clone())
                .filter(|service| !service.is_empty())
                .collect::<Vec<_>>();
            (catalog, buffered)
        };
        let mut payload_bytes_read = 0;
        let mut decoded_spans = 0;
        let mut values = match catalog {
            Some(values) => values.into_iter().collect::<BTreeSet<_>>(),
            None => {
                let mut stream = self.query_stream_projected_with_duration_after_snapshot(
                    &unbounded_span_query(),
                    i64::MIN,
                    i64::MAX,
                    SpanColumnMask::SERVICE,
                    || {},
                )?;
                let mut values = BTreeSet::new();
                while let Some(span) = self.query_stream_next(&mut stream)? {
                    if !span.service.is_empty() {
                        values.insert(span.service);
                    }
                }
                payload_bytes_read = stream.payload_bytes_read;
                decoded_spans = stream.decoded_spans;
                values
            }
        };
        values.extend(buffered);
        self.query_profile
            .discovery_count
            .fetch_add(1, Ordering::Relaxed);
        self.query_profile
            .discovery_total_ns
            .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        self.query_profile
            .discovery_payload_bytes_read
            .fetch_add(payload_bytes_read, Ordering::Relaxed);
        self.query_profile
            .discovery_decoded_spans
            .fetch_add(decoded_spans, Ordering::Relaxed);
        Ok(values.into_iter().collect())
    }

    /// Exact operation discovery for one service. New blocks carry a
    /// collision-free compound term plus an `operations:` generation marker.
    /// If any selected legacy block lacks the marker, decode fallback keeps
    /// mixed-version databases exact instead of silently omitting operations.
    pub fn discover_operations(&self, service: &str) -> Result<Vec<String>, String> {
        let started = Instant::now();
        let prefix = operation_prefix(service);
        let (catalog, all_marked, buffered) = {
            let _transition = self.transition_read();
            let service_term = format!("service:{service}");
            let candidates = self
                .store
                .query_terms(std::slice::from_ref(&service_term), i64::MIN, i64::MAX)
                .map_err(|error| self.query_error(error))?;
            let marked = self
                .store
                .query_terms(
                    &[service_term, "operations:".to_string()],
                    i64::MIN,
                    i64::MAX,
                )
                .map_err(|error| self.query_error(error))?;
            let catalog = self
                .store
                .query_term_values(&prefix)
                .map_err(|error| self.query_error(error))?;
            let buffered = self
                .buffer_lock()
                .iter()
                .filter(|span| span.service == service)
                .map(|span| span.name.clone())
                .collect::<Vec<_>>();
            (catalog, candidates.len() == marked.len(), buffered)
        };
        let mut payload_bytes_read = 0;
        let mut decoded_spans = 0;
        let mut values = match (all_marked, catalog) {
            (true, Some(values)) => values.into_iter().collect::<BTreeSet<_>>(),
            _ => {
                let mut query = unbounded_span_query();
                query.service = Some(service.to_string());
                let mut stream = self.query_stream_after_snapshot(&query, || {})?;
                let mut values = BTreeSet::new();
                while let Some(span) = self.query_stream_next(&mut stream)? {
                    values.insert(span.name);
                }
                payload_bytes_read = stream.payload_bytes_read;
                decoded_spans = stream.decoded_spans;
                values
            }
        };
        values.extend(buffered);
        self.query_profile
            .discovery_count
            .fetch_add(1, Ordering::Relaxed);
        self.query_profile
            .discovery_total_ns
            .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        self.query_profile
            .discovery_payload_bytes_read
            .fetch_add(payload_bytes_read, Ordering::Relaxed);
        self.query_profile
            .discovery_decoded_spans
            .fetch_add(decoded_spans, Ordering::Relaxed);
        Ok(values.into_iter().collect())
    }

    fn query_error(&self, error: String) -> String {
        if error.contains("interrupt") || error.contains("cancel") {
            self.query_profile
                .query_cancelled
                .fetch_add(1, Ordering::Relaxed);
            "span query cancelled".into()
        } else {
            error
        }
    }

    /// F4/F7 bucket kernel (FEATURE_PLAN.md): per-service span stats per
    /// CLOSED-OPEN `[start + k*step, +step)` bucket aligned to the
    /// query's ts_min (histograms bin forward). Per (bucket, service):
    /// span count, error count (status byte == error), and duration
    /// sum/min/max (saturating i64 sums; ns fit comfortably), plus exact
    /// nearest-rank p50/p95/p99. Rows sorted (bucket_ts, service).
    pub fn bucket_stats(
        &self,
        filter: &SpanQuery,
        step: i64,
    ) -> Result<Vec<TraceBucketStat>, String> {
        self.bucket_stats_after_snapshot(filter, step, || {})
    }

    /// Streaming bucket kernel with the same ownership callback as row
    /// queries. It retains only duration vectors required for exact
    /// percentiles, never the rich span rowset, and lets SQLite release its
    /// writer gate before decode/sort CPU begins.
    pub fn bucket_stats_after_snapshot<F>(
        &self,
        filter: &SpanQuery,
        step: i64,
        after_snapshot: F,
    ) -> Result<Vec<TraceBucketStat>, String>
    where
        F: FnOnce(),
    {
        if step <= 0 {
            return Err(format!("step must be positive, got {step}"));
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
        let projection = SpanColumnMask::SERVICE
            .union(SpanColumnMask::STATUS)
            .union(SpanColumnMask::START_TS)
            .union(SpanColumnMask::DURATION_NS);
        let mut stream = self.query_stream_projected_with_duration_after_snapshot(
            filter,
            i64::MIN,
            i64::MAX,
            projection,
            after_snapshot,
        )?;
        // Exact percentiles require duration vectors, but not the rich span
        // rows that previously dominated memory.
        let mut stats: std::collections::BTreeMap<(i64, String), (TraceBucketStat, Vec<i64>)> =
            std::collections::BTreeMap::new();
        loop {
            let s = match self.query_stream_next(&mut stream) {
                Ok(Some(span)) => span,
                Ok(None) => break,
                Err(error) => {
                    self.finish_query_stream(&mut stream);
                    return Err(error);
                }
            };
            let k = (s.start_ts as i128 - start as i128) / step as i128;
            let bucket_ts = (start as i128 + k * step as i128) as i64;
            let entry = stats.entry((bucket_ts, s.service.clone())).or_insert((
                TraceBucketStat {
                    bucket_ts,
                    service: s.service.clone(),
                    spans: 0,
                    errors: 0,
                    dur_sum: 0,
                    dur_min: i64::MAX,
                    dur_max: i64::MIN,
                    dur_p50: 0,
                    dur_p95: 0,
                    dur_p99: 0,
                },
                Vec::new(),
            ));
            entry.0.spans += 1;
            if s.status == 2 {
                entry.0.errors += 1;
            }
            entry.0.dur_sum = entry.0.dur_sum.saturating_add(s.duration_ns);
            entry.0.dur_min = entry.0.dur_min.min(s.duration_ns);
            entry.0.dur_max = entry.0.dur_max.max(s.duration_ns);
            entry.1.push(s.duration_ns);
        }
        let mut result = Vec::with_capacity(stats.len());
        for (mut stat, mut durations) in stats.into_values() {
            self.store
                .check_cancelled()
                .map_err(|error| self.query_error(error))?;
            durations.sort_unstable();
            self.store
                .check_cancelled()
                .map_err(|error| self.query_error(error))?;
            let nearest_rank = |percent: usize| {
                let one_based = (durations.len() * percent).div_ceil(100);
                durations[one_based.clamp(1, durations.len()) - 1]
            };
            stat.dur_p50 = nearest_rank(50);
            stat.dur_p95 = nearest_rank(95);
            stat.dur_p99 = nearest_rank(99);
            result.push(stat);
        }
        Ok(result)
    }

    /// (persisted blocks, raw blocks, buffered spans) — cheap and payload-free.
    pub fn stats(&self) -> (usize, usize, usize) {
        self.stats_with_after_index(|| {})
    }

    /// (persisted spans, buffered spans) from block metadata plus the live
    /// buffer, without decoding payloads. The transition guard prevents a
    /// flush from moving the same spans between both sides mid-snapshot.
    pub fn span_counts(&self) -> (u64, u64) {
        let _transition = self.transition_read();
        let persisted = self
            .index_lock()
            .iter()
            .map(|entry| u64::from(entry.meta.entry_count))
            .sum();
        let buffered = self.buffer_lock().len() as u64;
        (persisted, buffered)
    }

    /// Queryable start_ts range (blocks + buffer), payload-free. Same
    /// lock discipline as stats(): index scope dropped before the buffer
    /// is read (R7 — flush acquires buffer then index).
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
            mn = Some(mn.map_or(e.start_ts, |m| m.min(e.start_ts)));
            mx = Some(mx.map_or(e.start_ts, |m| m.max(e.start_ts)));
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

/// Terms for a batch of spans: service:/kind:/status:/name: — ALWAYS
/// all four (no index_keys allowlist; spans/mod.rs explains why traces
/// differ from logs here). Deduplicated + sorted; a block-level index
/// only cares that the term occurs at all. Compound operation terms are
/// additive discovery metadata, not row-filtering dimensions.
fn extract_terms(entries: &[SpanEntry]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for e in entries {
        set.insert(format!("kind:{}", super::kind_name(e.kind)));
        set.insert(format!("status:{}", status_name(e.status)));
        set.insert(format!("name:{}", e.name));
        if !e.service.is_empty() {
            set.insert(format!("service:{}", e.service));
            // `operations:` marks blocks that carry exact
            // service/operation pair terms. The length prefix makes
            // arbitrary UTF-8 service names collision-free without
            // reserving a separator byte.
            set.insert("operations:".to_string());
            set.insert(operation_term(&e.service, &e.name));
        }
    }
    set.into_iter().collect()
}

fn operation_term(service: &str, name: &str) -> String {
    format!("operation:{}:{service}{name}", service.len())
}

fn operation_prefix(service: &str) -> String {
    format!("operation:{}:{service}", service.len())
}

/// Deduped, sorted trace ids of a batch — the block's trace-index rows.
/// BTreeSet gives dedup + deterministic order in one move ([u8;16] is
/// Ord); a trace with many spans in one block still costs one row.
fn extract_trace_ids(entries: &[SpanEntry]) -> Vec<[u8; 16]> {
    let mut set = BTreeSet::new();
    for e in entries {
        set.insert(e.trace_id);
    }
    set.into_iter().collect()
}

fn span_duration_bounds(entries: &[SpanEntry]) -> Result<SpanDurationBounds, String> {
    let mut durations = entries.iter().map(|entry| entry.duration_ns);
    let Some(first) = durations.next() else {
        return Err("cannot encode duration bounds for an empty span block".into());
    };
    let (minimum, maximum) = durations.fold((first, first), |(minimum, maximum), duration| {
        (minimum.min(duration), maximum.max(duration))
    });
    SpanDurationBounds::new(minimum, maximum)
}

/// Exact per-span filter — the truth the block-level indexes only
/// approximate (a block containing the trace still contains other
/// traces' spans; a status-pure block still spans a ts range).
fn entry_matches(e: &SpanEntry, q: &SpanQuery, duration_min: i64, duration_max: i64) -> bool {
    if e.start_ts < q.ts_min || e.start_ts > q.ts_max {
        return false;
    }
    if e.duration_ns < duration_min || e.duration_ns > duration_max {
        return false;
    }
    if let Some(tid) = &q.trace_id {
        if &e.trace_id != tid {
            return false;
        }
    }
    if let Some(svc) = &q.service {
        if &e.service != svc {
            return false;
        }
    }
    if let Some(k) = q.kind {
        if e.kind != k {
            return false;
        }
    }
    if let Some(s) = q.status {
        if e.status != s {
            return false;
        }
    }
    if let Some(n) = &q.name {
        if &e.name != n {
            return false;
        }
    }
    true
}

fn query_predicate_mask(query: &SpanQuery, duration_min: i64, duration_max: i64) -> SpanColumnMask {
    let mut mask = SpanColumnMask::default();
    if query.trace_id.is_some() {
        mask = mask.union(SpanColumnMask::TRACE_ID);
    }
    if query.name.is_some() {
        mask = mask.union(SpanColumnMask::NAME);
    }
    if query.service.is_some() {
        mask = mask.union(SpanColumnMask::SERVICE);
    }
    if query.kind.is_some() {
        mask = mask.union(SpanColumnMask::KIND);
    }
    if query.status.is_some() {
        mask = mask.union(SpanColumnMask::STATUS);
    }
    if query.ts_min != i64::MIN || query.ts_max != i64::MAX {
        mask = mask.union(SpanColumnMask::START_TS);
    }
    if duration_min != i64::MIN || duration_max != i64::MAX {
        mask = mask.union(SpanColumnMask::DURATION_NS);
    }
    mask
}

fn predicate_row_matches(
    row: SpanPredicateRow<'_>,
    query: &SpanQuery,
    duration_min: i64,
    duration_max: i64,
) -> bool {
    if row.start_ts < query.ts_min || row.start_ts > query.ts_max {
        return false;
    }
    if row.duration_ns < duration_min || row.duration_ns > duration_max {
        return false;
    }
    if query
        .trace_id
        .as_ref()
        .is_some_and(|value| row.trace_id != value)
    {
        return false;
    }
    if query
        .service
        .as_deref()
        .is_some_and(|value| row.service != value)
    {
        return false;
    }
    if query.kind.is_some_and(|value| row.kind != value) {
        return false;
    }
    if query.status.is_some_and(|value| row.status != value) {
        return false;
    }
    if query.name.as_deref().is_some_and(|value| row.name != value) {
        return false;
    }
    true
}

fn compare_spans(a: &SpanEntry, b: &SpanEntry, order: SpanQueryOrder) -> CmpOrdering {
    let a_key = (a.start_ts, a.span_id);
    let b_key = (b.start_ts, b.span_id);
    match order {
        SpanQueryOrder::Asc => a_key.cmp(&b_key),
        SpanQueryOrder::Desc => b_key.cmp(&a_key),
    }
}

fn unbounded_span_query() -> SpanQuery {
    SpanQuery {
        ts_min: i64::MIN,
        ts_max: i64::MAX,
        trace_id: None,
        service: None,
        kind: None,
        status: None,
        name: None,
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
