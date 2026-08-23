//! BlockEngine: the buffer → raw block → optimized block state machine,
//! plus the query path. One instance per logs vtab (and, in Session 6,
//! per traces vtab).
//!
//! Concurrency model: every public method takes &self and guards state
//! with Mutexes, matching the metrics Engine so a vtab cursor can hold
//! an `Arc<BlockEngine>` next to the table object. NOTHING in here uses
//! rayon or spawns threads — every store call happens on the caller's
//! thread. This is a hard rule (PLAN.md Session 3 lesson): store calls
//! re-enter SQLite on the host connection whose mutex the vtab callback
//! thread holds; a worker thread touching the store would deadlock.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use super::codec::{
    block_message_feasible, decode_block, decode_block_filtered, encode_block, is_raw_codec,
    CODEC_COLUMNAR_V2, CODEC_RAW, CODEC_RICH_COLUMNAR, CODEC_RICH_RAW, CODEC_RICH_TEMPLATE,
};
use super::{
    canonical_severity, level_from_name, level_name, BlockLoc, BlockMeta, BlockStore, EncodedBlock,
    LogEntry,
};

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
    /// Run a budgeted optimize from inside flush() every this many flush
    /// calls (when the exact planner reports actionable work). 0 disables
    /// auto-optimize. An extension has no timer of its own, so maintenance
    /// must ride a host call — and flush is the one call every host already
    /// makes on a heartbeat. Hosts that schedule optimize externally (the
    /// API services) just find an emptier backlog. 30 matches the services'
    /// 30s optimize cadence against the embedded engines' 1s flush timers.
    pub auto_optimize_interval_flushes: usize,
    /// Entry budget for each auto-optimize pass — and the raw-backlog size
    /// that triggers a pass immediately, without waiting out the interval.
    /// Bounds the pause a flush caller can absorb; under sustained ingest
    /// the immediate trigger keeps raw debt near this bound instead of
    /// letting it grow interval-wide.
    pub auto_optimize_budget_entries: usize,
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
            auto_optimize_interval_flushes: 30,
            auto_optimize_budget_entries: 32_768,
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
    pub query_clp_pruned_blocks: u64,
    pub query_clp_skipped_rows: u64,
    pub query_matched_entries: u64,
    pub query_returned_entries: u64,
    pub query_bounded_count: u64,
    pub query_bounded_requested_entries: u64,
    pub query_bounded_max_entries: u64,
    pub query_blocks_skipped_by_bound: u64,
    pub native_count_count: u64,
    pub native_count_total_ns: u64,
    pub native_count_snapshot_ns: u64,
    pub native_count_payload_bytes_read: u64,
    pub native_count_metadata_blocks: u64,
    pub native_count_metadata_entries: u64,
    pub native_count_decoded_blocks: u64,
    pub native_count_decoded_entries: u64,
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
}

/// Work performed by one successful log-row query.
///
/// Unlike [`BlockEngineProfileSnapshot`], this report is request-owned. It is
/// assembled from the query's local counters before they are added to the
/// process-wide profile, so concurrent readers cannot contaminate it. The
/// SQLite extension publishes this report only for the connection that ran
/// the query and clears it before the next scan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogQueryExecutionReport {
    pub query_total_ns: u64,
    pub query_snapshot_ns: u64,
    pub query_materialize_ns: u64,
    /// Payload bytes copied while taking a conservative snapshot. File-backed
    /// SQLite stores normally retain stable locations, so this is usually zero.
    pub snapshot_payload_bytes: u64,
    /// Complete encoded block payload bytes read or copied for this query.
    pub payload_bytes_read: u64,
    /// Blocks selected by timestamp/posting/trigram pruning before an ordered
    /// result bound can stop the scan.
    pub candidate_blocks: u64,
    /// Candidate blocks actually decoded.
    pub processed_blocks: u64,
    pub blocks_skipped_by_bound: u64,
    /// Live-buffer entries whose predicates were examined.
    pub buffered_entries_processed: u64,
    /// Persisted entries decoded from processed blocks.
    pub decoded_entries: u64,
    /// Persisted plus live-buffer entries whose predicates were examined.
    pub processed_entries: u64,
    pub matched_entries: u64,
    pub returned_entries: u64,
    /// Timeless log blocks have three logical non-timestamp value slots per
    /// row: severity, message, and the complete metadata envelope. Current
    /// codecs decode all three together rather than selecting physical fields.
    pub values_read: u64,
    pub timestamps_read: u64,
    pub stable_location_snapshot: bool,
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
    query_clp_pruned_blocks: AtomicU64,
    query_clp_skipped_rows: AtomicU64,
    query_matched_entries: AtomicU64,
    query_returned_entries: AtomicU64,
    query_bounded_count: AtomicU64,
    query_bounded_requested_entries: AtomicU64,
    query_bounded_max_entries: AtomicU64,
    query_blocks_skipped_by_bound: AtomicU64,
    native_count_count: AtomicU64,
    native_count_total_ns: AtomicU64,
    native_count_snapshot_ns: AtomicU64,
    native_count_payload_bytes_read: AtomicU64,
    native_count_metadata_blocks: AtomicU64,
    native_count_metadata_entries: AtomicU64,
    native_count_decoded_blocks: AtomicU64,
    native_count_decoded_entries: AtomicU64,
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
            query_clp_pruned_blocks: load(&self.query_clp_pruned_blocks),
            query_clp_skipped_rows: load(&self.query_clp_skipped_rows),
            query_matched_entries: load(&self.query_matched_entries),
            query_returned_entries: load(&self.query_returned_entries),
            query_bounded_count: load(&self.query_bounded_count),
            query_bounded_requested_entries: load(&self.query_bounded_requested_entries),
            query_bounded_max_entries: load(&self.query_bounded_max_entries),
            query_blocks_skipped_by_bound: load(&self.query_blocks_skipped_by_bound),
            native_count_count: load(&self.native_count_count),
            native_count_total_ns: load(&self.native_count_total_ns),
            native_count_snapshot_ns: load(&self.native_count_snapshot_ns),
            native_count_payload_bytes_read: load(&self.native_count_payload_bytes_read),
            native_count_metadata_blocks: load(&self.native_count_metadata_blocks),
            native_count_metadata_entries: load(&self.native_count_metadata_entries),
            native_count_decoded_blocks: load(&self.native_count_decoded_blocks),
            native_count_decoded_entries: load(&self.native_count_decoded_entries),
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
    /// Exact rich severity spelling. `level` remains the coarse partition
    /// constraint; this distinguishes notice/info and the error family.
    pub severity: Option<String>,
    /// Metadata equality filters; ALL must match. Pairs whose key is in
    /// index_keys also prune blocks via the term index; the rest are
    /// checked per-entry only.
    pub metadata_eq: Vec<(String, String)>,
    /// Case-insensitive substring match on the message. ASCII matching is
    /// allocation-free; non-ASCII falls back to Unicode lowercase matching.
    pub message_contains: Option<String>,
    /// F6: a LIKE pattern used ONLY for trigram block PRUNING — no
    /// entries are filtered by it (the SQL layer rechecks LIKE exactly;
    /// the vtab never sets omit on the constraint). Sound by
    /// construction: only blocks that provably cannot contain a match
    /// are skipped, and blocks without the `tg:` marker (pre-F6 data,
    /// trigram-capped blocks, disabled index) are never skipped.
    pub message_like_prune: Option<String>,
}

/// Ordering guaranteed by the bounded log query path. Equal timestamps keep
/// their canonical engine sequence in both directions, making pagination
/// deterministic without inventing a user-visible secondary SQL key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogQueryOrder {
    Asc,
    Desc,
}

struct LogQueryBlockSnapshot {
    payload: Option<Vec<u8>>,
    location: Option<BlockLoc>,
    meta: BlockMeta,
    partition: Option<u8>,
    sequence: usize,
}

struct LogQuerySnapshot {
    blocks: Vec<LogQueryBlockSnapshot>,
    buffered: Vec<LogEntry>,
    buffered_entries_considered: usize,
    candidate_blocks: u64,
    payload_bytes: u64,
    stable_locations: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct QuerySequence {
    source: usize,
    row: usize,
}

struct BoundedEntry {
    entry: LogEntry,
    sequence: QuerySequence,
    order: LogQueryOrder,
}

impl PartialEq for BoundedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.ts == other.entry.ts
            && self.sequence == other.sequence
            && self.order == other.order
    }
}

impl Eq for BoundedEntry {}

impl PartialOrd for BoundedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for BoundedEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        debug_assert_eq!(self.order, other.order);
        match self.order {
            // BinaryHeap keeps the greatest item at the root. For ASC the
            // greatest key is the worst retained row.
            LogQueryOrder::Asc => self
                .entry
                .ts
                .cmp(&other.entry.ts)
                .then_with(|| canonical_entry_cmp(&self.entry, &other.entry))
                .then_with(|| self.sequence.cmp(&other.sequence)),
            // For DESC, an older timestamp is worse. At equal timestamps the
            // later canonical row is worse, so ties remain stable.
            LogQueryOrder::Desc => other
                .entry
                .ts
                .cmp(&self.entry.ts)
                .then_with(|| canonical_entry_cmp(&self.entry, &other.entry))
                .then_with(|| self.sequence.cmp(&other.sequence)),
        }
    }
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

/// Current metadata-only optimizer backlog. `merge_ready_*` counts only
/// compressed groups that satisfy the size-tiered rewrite policy; deferred
/// small tails are reported separately and do not cause maintenance work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockOptimizeBacklog {
    pub raw_blocks: u64,
    pub raw_entries: u64,
    pub merge_ready_groups: u64,
    pub merge_ready_blocks: u64,
    pub merge_ready_entries: u64,
    pub merge_deferred_blocks: u64,
    pub merge_deferred_entries: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OptimizeKind {
    RawCompression,
    CompressedMerge,
}

struct OptimizeGroup {
    sources: Vec<IndexEntry>,
    partition: Option<u8>,
    kind: OptimizeKind,
}

#[derive(Default)]
struct OptimizeOutcome {
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
    /// Flush calls since the last auto-optimize backlog check.
    flushes_since_auto_optimize: AtomicUsize,
    /// One-shot guard for the trigram-posting purge on stores that did
    /// not opt into `message_index='trigram'` (see optimize()).
    trigram_purge_checked: AtomicBool,
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
            flushes_since_auto_optimize: AtomicUsize::new(0),
            trigram_purge_checked: AtomicBool::new(false),
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
        normalize_rich_entry(&mut entry)?;
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
            normalize_rich_entry(entry)?;
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
        self.maybe_auto_optimize()?;
        Ok(out)
    }

    /// Auto-optimize, riding the flush path (see
    /// BlockEngineConfig::auto_optimize_interval_flushes). Every interval-th
    /// flush call — including empty heartbeat flushes, so an idle store
    /// still drains its debt — consults the exact planner and runs one
    /// budgeted pass if it found actionable work. A raw backlog at or past
    /// the budget triggers immediately instead of waiting out the interval.
    fn maybe_auto_optimize(&self) -> Result<(), String> {
        let interval = self.config.auto_optimize_interval_flushes;
        if interval == 0 {
            return Ok(());
        }
        let budget = self.config.auto_optimize_budget_entries;
        let calls = self
            .flushes_since_auto_optimize
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if calls < interval {
            // Cheap urgency probe between interval checks: raw entry count
            // from index metadata only (no planning).
            let raw_entries: u64 = self
                .index_lock()
                .iter()
                .filter(|entry| is_raw_codec(entry.meta.codec))
                .map(|entry| entry.meta.entry_count as u64)
                .sum();
            if raw_entries < budget as u64 {
                return Ok(());
            }
        }
        self.flushes_since_auto_optimize.store(0, Ordering::Relaxed);
        let backlog = self.optimize_backlog();
        if backlog.raw_blocks == 0 && backlog.merge_ready_groups == 0 {
            return Ok(());
        }
        self.optimize_budgeted(budget)?;
        Ok(())
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
            let codec = if run.iter().any(LogEntry::is_rich) {
                CODEC_RICH_RAW
            } else {
                CODEC_RAW
            };
            let (data, meta) = encode_block(run, codec, self.config.zstd_level)?;
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
        // The entries just became durable: accrue their logical raw
        // bytes in the same host transaction as the block rows, so a
        // rollback takes the increment with it (see
        // persist_ingest_raw_total).
        let raw_ingest_bytes: u64 = buf.iter().map(LogEntry::raw_ingest_bytes).sum();
        self.persist_ingest_raw_total(raw_ingest_bytes)?;
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
        for run in pattern.split(['%', '_']) {
            Self::message_trigrams_of(run, &mut set);
        }
        set.into_iter().map(Self::tg_term).collect()
    }

    /// Required trigram terms for the exact case-insensitive contains
    /// predicate. The persisted index performs ASCII folding, so non-ASCII
    /// needles deliberately get no pruning: Unicode lowercase equivalence can
    /// change UTF-8 bytes and must never create a false negative.
    pub fn message_contains_trigrams(needle: &str) -> Vec<String> {
        if !needle.is_ascii() {
            return Vec::new();
        }
        let mut set = BTreeSet::new();
        Self::message_trigrams_of(needle, &mut set);
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
        self.extract_terms_with(entries, &self.config.index_keys)
    }

    /// As extract_terms, against an explicit allowlist. reindex() passes the
    /// NEW allowlist so it can rewrite postings for blocks that were written
    /// under the old one, without mutating the live config.
    fn extract_terms_with(&self, entries: &[LogEntry], index_keys: &[String]) -> Vec<String> {
        let mut set = BTreeSet::new();
        for e in entries {
            set.insert(format!("level:{}", level_name(e.level)));
            for (k, v) in &e.metadata {
                if index_keys.iter().any(|ik| ik == k) {
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
    ///   1. every RAW block gets grouped with RAW peers and recompressed to
    ///      CODEC_COLUMNAR_V2, and
    ///   2. existing compressed blocks merge only when the output is at least
    ///      half full AND at least twice the largest input block.
    ///
    /// Keeping the phases disjoint is intentional. The old planner appended
    /// each new raw arrival directly into an existing compressed tail, so the
    /// complete growing tail was decoded and rewritten on every maintenance
    /// call. The half-full + 2x-growth rule is a compact size-tiered policy:
    /// small tails accumulate independently, then merge in logarithmic steps.
    /// Raw data is still compressed on its first optimize call, never held
    /// hostage waiting for a merge cohort.
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
    /// Rewrite every block's postings against a new index_keys allowlist and
    /// persist the allowlist.
    ///
    /// Postings are written at insert time, so a block carries postings only
    /// for the keys indexed when it was written. Widening index_keys without
    /// this makes pruning on a newly indexed key skip every older block: the
    /// entries are still stored, but `query_terms` never returns their blocks,
    /// so a search silently loses history. Narrowing is equally unsound in the
    /// other direction — stale postings would keep pruning on a key the engine
    /// no longer applies.
    ///
    /// One block is read, decoded and released per iteration, so peak memory
    /// is one block rather than the store. The allowlist is saved only after
    /// every block succeeds; a failure part-way leaves the persisted allowlist
    /// unchanged, so the next connect still uses the old one and the partially
    /// rewritten postings remain a superset of what it prunes on. Re-running
    /// is safe and idempotent.
    ///
    /// Returns the number of blocks rewritten.
    pub fn reindex(&self, index_keys: &[String]) -> Result<usize, String> {
        let blocks = self.store.scan()?;
        let mut rewritten = 0usize;

        for (_meta, loc) in blocks {
            let bytes = self.store.read_block(&loc)?;
            let entries = decode_block(&bytes)?;
            let terms = self.extract_terms_with(&entries, index_keys);
            self.store.replace_terms(&loc, &terms)?;
            self.store.check_cancelled()?;
            rewritten += 1;
        }

        self.store
            .save_meta("index_keys", index_keys.join(",").as_bytes())?;

        Ok(rewritten)
    }

    pub fn optimize(&self) -> Result<(usize, usize), String> {
        self.optimize_with_budget(None)
    }

    /// The default upgrade path for the F6 trigram index: a store whose
    /// engine was built WITHOUT the trigram opt-in sheds any `tg:`
    /// postings at its first maintenance boundary. Opted-in stores are
    /// never touched, and the check costs one atomic load after the
    /// first call. Explicit opt-out goes through the same purge via the
    /// `message_index:none` command.
    fn maybe_purge_unopted_trigrams(&self) -> Result<(), String> {
        if self.config.message_trigrams || self.trigram_purge_checked.swap(true, Ordering::Relaxed)
        {
            return Ok(());
        }
        self.store.purge_term_prefix("tg:")?;
        Ok(())
    }

    /// Persist the `message_index` choice for future connects (live
    /// connections keep the setting they loaded — same contract as
    /// reindex()).
    pub fn save_message_index_meta(&self, value: &str) -> Result<(), String> {
        self.store.save_meta("message_index", value.as_bytes())
    }

    /// Drop every trigram posting now — the `message_index:none` path.
    pub fn purge_trigram_postings(&self) -> Result<u64, String> {
        self.trigram_purge_checked.store(true, Ordering::Relaxed);
        self.store.purge_term_prefix("tg:")
    }

    /// Incremental optimize. The entry budget limits source entries rewritten
    /// by one call; a single group (raw groups up to merge_target_entries,
    /// compressed tiers up to 125% of that target, or a pre-existing
    /// oversized raw block) is always allowed so maintenance makes progress.
    /// The SQL extension exposes this as
    /// `optimize:<max_entries>` for hosts that schedule from observed backlog
    /// bytes instead of accepting one unbounded maintenance pause.
    pub fn optimize_budgeted(&self, max_entries: usize) -> Result<(usize, usize), String> {
        if max_entries == 0 {
            return Err("optimize entry budget must be positive".into());
        }
        self.optimize_with_budget(Some(max_entries))
    }

    fn optimize_with_budget(&self, max_entries: Option<usize>) -> Result<(usize, usize), String> {
        self.maybe_purge_unopted_trigrams()?;
        let started = Instant::now();
        let out = self.optimize_inner(max_entries)?;
        self.apply_retention()?;
        self.profile.optimize_count.fetch_add(1, Ordering::Relaxed);
        self.profile
            .optimize_total_ns
            .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        self.profile
            .optimize_blocks_removed
            .fetch_add(out.blocks_removed as u64, Ordering::Relaxed);
        self.profile
            .optimize_blocks_written
            .fetch_add(out.blocks_written as u64, Ordering::Relaxed);
        if let Some(budget) = max_entries {
            self.profile
                .optimize_budgeted_count
                .fetch_add(1, Ordering::Relaxed);
            self.profile
                .optimize_budget_entries
                .fetch_add(budget as u64, Ordering::Relaxed);
        }
        if out.budget_limited {
            self.profile
                .optimize_budget_limited_count
                .fetch_add(1, Ordering::Relaxed);
        }
        for (counter, value) in [
            (&self.profile.optimize_raw_groups, out.raw_groups),
            (&self.profile.optimize_raw_blocks, out.raw_blocks),
            (&self.profile.optimize_raw_entries, out.raw_entries),
            (&self.profile.optimize_raw_input_bytes, out.raw_input_bytes),
            (
                &self.profile.optimize_raw_output_bytes,
                out.raw_output_bytes,
            ),
            (&self.profile.optimize_raw_total_ns, out.raw_total_ns),
            (&self.profile.optimize_merge_groups, out.merge_groups),
            (&self.profile.optimize_merge_blocks, out.merge_blocks),
            (&self.profile.optimize_merge_entries, out.merge_entries),
            (
                &self.profile.optimize_merge_input_bytes,
                out.merge_input_bytes,
            ),
            (
                &self.profile.optimize_merge_output_bytes,
                out.merge_output_bytes,
            ),
            (&self.profile.optimize_merge_total_ns, out.merge_total_ns),
        ] {
            counter.fetch_add(value, Ordering::Relaxed);
        }
        Ok((out.blocks_removed, out.blocks_written))
    }

    /// Metadata-only view of work an optimize call can perform now. This uses
    /// the exact planner, so hosts do not wake maintenance for permanently
    /// deferred singleton/underfilled tails.
    pub fn optimize_backlog(&self) -> BlockOptimizeBacklog {
        let candidates: Vec<IndexEntry> = self
            .index_lock()
            .iter()
            .filter(|entry| {
                is_raw_codec(entry.meta.codec)
                    || (entry.meta.entry_count as usize) < self.config.merge_target_entries
            })
            .copied()
            .collect();
        let groups = self.plan_optimize(&candidates);
        let mut backlog = BlockOptimizeBacklog::default();
        let mut ready_compressed = HashSet::new();
        for group in &groups {
            let entries = group
                .sources
                .iter()
                .map(|entry| entry.meta.entry_count as u64)
                .sum::<u64>();
            match group.kind {
                OptimizeKind::RawCompression => {
                    backlog.raw_blocks += group.sources.len() as u64;
                    backlog.raw_entries += entries;
                }
                OptimizeKind::CompressedMerge => {
                    backlog.merge_ready_groups += 1;
                    backlog.merge_ready_blocks += group.sources.len() as u64;
                    backlog.merge_ready_entries += entries;
                    ready_compressed.extend(group.sources.iter().map(|entry| entry.loc.id));
                }
            }
        }
        for entry in candidates.iter().filter(|entry| {
            !is_raw_codec(entry.meta.codec) && !ready_compressed.contains(&entry.loc.id)
        }) {
            backlog.merge_deferred_blocks += 1;
            backlog.merge_deferred_entries += entry.meta.entry_count as u64;
        }
        backlog
    }

    fn plan_optimize(&self, candidates: &[IndexEntry]) -> Vec<OptimizeGroup> {
        // Data-time high-water mark across ALL blocks (not just merge
        // candidates): a window that ended a full merge span before this
        // is CLOSED — no further arrivals are expected there, so its
        // stragglers may coalesce below the open-window fill rules.
        let store_newest = self
            .index_lock()
            .iter()
            .map(|entry| entry.meta.ts_max)
            .max()
            .unwrap_or(i64::MIN);
        let mut raw_buckets: [Vec<IndexEntry>; 5] = Default::default();
        let mut compressed_buckets: [Vec<IndexEntry>; 5] = Default::default();
        for entry in candidates {
            let bucket = entry.partition.map_or(4, usize::from);
            if is_raw_codec(entry.meta.codec) {
                raw_buckets[bucket].push(*entry);
            } else {
                compressed_buckets[bucket].push(*entry);
            }
        }

        let mut groups = Vec::new();
        for bucket in 0..5 {
            let partition = (bucket < 4).then_some(bucket as u8);
            groups
                .extend(self.plan_raw_groups(std::mem::take(&mut raw_buckets[bucket]), partition));
            groups.extend(self.plan_compressed_groups(
                std::mem::take(&mut compressed_buckets[bucket]),
                partition,
                store_newest,
            ));
        }
        // Compress raw backlog before spending a bounded call on optional
        // merges; within each phase, oldest data advances first.
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
    ) -> Vec<OptimizeGroup> {
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
                groups.push(OptimizeGroup {
                    sources: std::mem::take(&mut current),
                    partition,
                    kind: OptimizeKind::RawCompression,
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
            groups.push(OptimizeGroup {
                sources: current,
                partition,
                kind: OptimizeKind::RawCompression,
            });
        }
        groups
    }

    fn plan_compressed_groups(
        &self,
        mut entries: Vec<IndexEntry>,
        partition: Option<u8>,
        store_newest: i64,
    ) -> Vec<OptimizeGroup> {
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
                self.plan_compressed_segment(
                    std::mem::take(&mut segment),
                    partition,
                    store_newest,
                    &mut groups,
                );
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
        self.plan_compressed_segment(segment, partition, store_newest, &mut groups);
        groups
    }

    fn plan_compressed_segment(
        &self,
        mut entries: Vec<IndexEntry>,
        partition: Option<u8>,
        store_newest: i64,
        groups: &mut Vec<OptimizeGroup>,
    ) {
        // CLOSED segment: its window ended at least one full merge span
        // before the store's newest data. Low-volume level partitions
        // (a few hundred warnings per hour against a 4,096-entry fill
        // floor) otherwise strand their trickle blocks in every closed
        // hour FOREVER — the production signature is thousands of
        // ~20-entry blocks that never converge. Once closed, the
        // anti-amplification rules protect nothing: there is no tail to
        // keep appending to, so stragglers coalesce unconditionally.
        let closed = entries
            .iter()
            .map(|entry| entry.meta.ts_max)
            .max()
            .is_some_and(|segment_max| {
                segment_max <= store_newest.saturating_sub(self.config.merge_max_ts_span)
            });
        entries.sort_by_key(|entry| (entry.meta.entry_count, entry.meta.ts_min, entry.meta.ts_max));
        let mut current = Vec::new();
        let mut current_entries = 0usize;
        // A hard target ceiling strands two valid half-full tiers whenever
        // their combined size lands just above the target (for example,
        // 4,300 + 4,300 with an 8,192 target). A small bounded overshoot lets
        // equal-size tiers reach their required 2x growth and become terminal
        // blocks without reopening the append-to-tail amplification path.
        let merge_limit = self
            .config
            .merge_target_entries
            .saturating_add(self.config.merge_target_entries.div_ceil(4));
        for entry in entries {
            let count = entry.meta.entry_count as usize;
            if !current.is_empty() && current_entries.saturating_add(count) > merge_limit {
                self.push_compressed_group(std::mem::take(&mut current), partition, closed, groups);
                current_entries = 0;
            }
            current_entries = current_entries.saturating_add(count);
            current.push(entry);
        }
        self.push_compressed_group(current, partition, closed, groups);
    }

    fn push_compressed_group(
        &self,
        sources: Vec<IndexEntry>,
        partition: Option<u8>,
        closed: bool,
        groups: &mut Vec<OptimizeGroup>,
    ) {
        if sources.len() < 2 {
            return;
        }
        // Open windows keep the amplification guards: a merged block must
        // be at least half-full AND at least double its largest source,
        // or repeated tail-appends rewrite the same data endlessly.
        // Closed windows coalesce unconditionally — final compaction.
        if !closed {
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
            if entries < minimum_fill || entries < largest.saturating_mul(2) {
                return;
            }
        }
        groups.push(OptimizeGroup {
            sources,
            partition,
            kind: OptimizeKind::CompressedMerge,
        });
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

    fn optimize_inner(&self, max_entries: Option<usize>) -> Result<OptimizeOutcome, String> {
        let _transition = self.transition_write();
        // Snapshot the index; plan on the copy (no lock held while payloads
        // are read/decoded).
        let candidates: Vec<IndexEntry> = self
            .index_lock()
            .iter()
            .filter(|e| {
                is_raw_codec(e.meta.codec)
                    || (e.meta.entry_count as usize) < self.config.merge_target_entries
            })
            .copied()
            .collect();
        if candidates.is_empty() {
            return Ok(OptimizeOutcome::default());
        }
        let planned = self.plan_optimize(&candidates);
        if planned.is_empty() {
            return Ok(OptimizeOutcome::default());
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

        let mut adds: Vec<EncodedBlock> = Vec::new();
        let mut add_partitions: Vec<Option<u8>> = Vec::new();
        let mut removes: Vec<BlockLoc> = Vec::new();
        let mut outcome = OptimizeOutcome {
            budget_limited,
            ..OptimizeOutcome::default()
        };
        for group in &selected {
            let phase_started = Instant::now();
            let expected_entries = group
                .sources
                .iter()
                .map(|entry| entry.meta.entry_count as usize)
                .sum();
            let mut entries: Vec<LogEntry> = Vec::with_capacity(expected_entries);
            let mut input_bytes = 0u64;
            for source in &group.sources {
                let bytes = self.store.read_block(&source.loc)?;
                input_bytes = input_bytes.saturating_add(bytes.len() as u64);
                entries.extend(decode_block(&bytes)?);
            }
            entries.sort_by_key(|e| e.ts);
            let terms = self.extract_terms(&entries);
            // Rich groups request the template codec; encode_block
            // falls back to codec 7 per block when templates lose
            // (CLP_PLAN.md), so this is never a size regression.
            let codec = if entries.iter().any(LogEntry::is_rich) {
                CODEC_RICH_TEMPLATE
            } else {
                CODEC_COLUMNAR_V2
            };
            let (data, meta) = encode_block(&entries, codec, self.config.zstd_level)?;
            let output_bytes = data.len() as u64;
            adds.push(EncodedBlock { meta, data, terms });
            add_partitions.push(group.partition);
            removes.extend(group.sources.iter().map(|entry| entry.loc));
            let elapsed = elapsed_ns(phase_started);
            match group.kind {
                OptimizeKind::RawCompression => {
                    outcome.raw_groups += 1;
                    outcome.raw_blocks += group.sources.len() as u64;
                    outcome.raw_entries += entries.len() as u64;
                    outcome.raw_input_bytes = outcome.raw_input_bytes.saturating_add(input_bytes);
                    outcome.raw_output_bytes =
                        outcome.raw_output_bytes.saturating_add(output_bytes);
                    outcome.raw_total_ns = outcome.raw_total_ns.saturating_add(elapsed);
                }
                OptimizeKind::CompressedMerge => {
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
        outcome.blocks_removed = removes.len();
        outcome.blocks_written = add_metas.len();
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
        self.persist_compression_totals(
            outcome.raw_input_bytes,
            outcome.raw_output_bytes,
            outcome.merge_input_bytes,
            outcome.merge_output_bytes,
        )?;
        Ok(outcome)
    }

    /// Lifetime raw->compressed byte totals, persisted in _meta inside the
    /// same host transaction as the optimize swap. The in-memory profile
    /// counters are process-local by design (work attribution); a
    /// compression-RATIO display backed by them read "pending" on a fully
    /// compressed store after every restart.
    ///
    /// Accounting: raw phases add to both sides. Merge phases adjust the
    /// OUTPUT side only (out += merge_out - merge_in) — the logical bytes a
    /// merged block represents are unchanged, but its footprint shrinks, and
    /// crediting that is what lets the ratio converge to the store's true
    /// figure. Excluding merges (the first design) froze the ratio at the
    /// first-pass compression of trickle-sized blocks, permanently
    /// underselling low-traffic stores. Merges of blocks compressed before
    /// this counter existed subtract bytes that were never added; the
    /// saturating floor bounds that transition-window skew, and retention
    /// ages the affected blocks out.
    fn persist_compression_totals(
        &self,
        raw_in: u64,
        raw_out: u64,
        merge_in: u64,
        merge_out: u64,
    ) -> Result<(), String> {
        if raw_in == 0 && merge_in == 0 {
            return Ok(());
        }
        let (input_total, output_total) = self.load_compression_totals()?;
        let value = format!(
            "{} {}",
            input_total.saturating_add(raw_in),
            output_total
                .saturating_add(raw_out)
                .saturating_add(merge_out)
                .saturating_sub(merge_in)
        );
        self.store.save_meta("compression_totals", value.as_bytes())
    }

    /// Lifetime logical raw bytes made durable by flushes — the honest
    /// raw side of a compression ratio (8 ts + 1 level + message +
    /// metadata bytes per entry; see LogEntry::raw_ingest_bytes).
    /// Persisted in _meta inside the same host transaction as the
    /// put_blocks call, so a rolled-back ingest never leaks an
    /// increment. Monotonic: optimize, merges, and retention rewrite or
    /// drop blocks but never re-persist entries, so nothing here ever
    /// moves it back down.
    fn persist_ingest_raw_total(&self, added: u64) -> Result<(), String> {
        if added == 0 {
            return Ok(());
        }
        let total = self.load_ingest_raw_total()?;
        self.store.save_meta(
            "ingest_raw_bytes_total",
            total.saturating_add(added).to_string().as_bytes(),
        )
    }

    /// Parse the persisted total; absent or malformed reads as zero (a
    /// pre-upgrade store simply starts counting from its next flush).
    pub fn load_ingest_raw_total(&self) -> Result<u64, String> {
        let Some(bytes) = self.store.load_meta("ingest_raw_bytes_total")? else {
            return Ok(0);
        };
        let text = String::from_utf8_lossy(&bytes);
        Ok(text.trim().parse().unwrap_or(0))
    }

    /// Parse the persisted "in out" pair; absent or malformed reads as zero
    /// (a pre-upgrade store simply starts counting from its next optimize).
    pub fn load_compression_totals(&self) -> Result<(u64, u64), String> {
        let Some(bytes) = self.store.load_meta("compression_totals")? else {
            return Ok((0, 0));
        };
        let text = String::from_utf8_lossy(&bytes);
        let mut parts = text.split_ascii_whitespace();
        let input = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let output = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        Ok((input, output))
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

    /// Change the retention window and persist it for future connects —
    /// the runtime-command counterpart of the CREATE-time `retention`
    /// arg, mirroring how [`Self::reindex`] persists `index_keys`.
    pub fn set_retention_persistent(&self, native: i64) -> Result<(), String> {
        self.store
            .save_meta("retention", native.to_string().as_bytes())?;
        self.set_retention(Some(native));
        Ok(())
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

    /// Return at most `max_entries` rows in exact timestamp order. This is
    /// the engine half of SQLite's `ORDER BY ts ... LIMIT/OFFSET` pushdown:
    /// callers pass `LIMIT + OFFSET`, then SQLite may apply OFFSET and LIMIT
    /// to this already-bounded prefix. Memory is O(max_entries), and block
    /// bounds stop the scan once later blocks cannot enter the prefix.
    pub fn query_bounded(
        &self,
        q: &LogQuery,
        order: LogQueryOrder,
        max_entries: usize,
    ) -> Result<Vec<LogEntry>, String> {
        self.query_ordered_after_snapshot(q, order, Some(max_entries), || {})
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
        self.query_ordered_after_snapshot(q, LogQueryOrder::Asc, None, after_snapshot)
    }

    /// Ordered query with an optional result-window bound and a synchronous
    /// notification at the snapshot ownership boundary. `None` preserves the
    /// original unbounded query behavior; `Some(n)` retains only the first n
    /// rows in the requested order.
    pub fn query_ordered_after_snapshot<F>(
        &self,
        q: &LogQuery,
        order: LogQueryOrder,
        max_entries: Option<usize>,
        after_snapshot: F,
    ) -> Result<Vec<LogEntry>, String>
    where
        F: FnOnce(),
    {
        self.query_ordered_with_work_limit_after_snapshot(
            q,
            order,
            max_entries,
            None,
            after_snapshot,
        )
    }

    /// Ordered query with both an output-window bound and a hard cap on the
    /// number of log entries examined. The work cap is independent of result
    /// cardinality: an unselective predicate cannot hide an unbounded decode
    /// behind `LIMIT 1`.
    pub fn query_ordered_with_work_limit_after_snapshot<F>(
        &self,
        q: &LogQuery,
        order: LogQueryOrder,
        max_entries: Option<usize>,
        max_work_entries: Option<usize>,
        after_snapshot: F,
    ) -> Result<Vec<LogEntry>, String>
    where
        F: FnOnce(),
    {
        self.query_ordered_with_work_limit_report_after_snapshot(
            q,
            order,
            max_entries,
            max_work_entries,
            after_snapshot,
        )
        .map(|(entries, _report)| entries)
    }

    /// The request-owned form of
    /// [`Self::query_ordered_with_work_limit_after_snapshot`]. Existing callers
    /// retain the row-only API; SQLite query accounting uses this form so it
    /// never derives one request from racy process-wide counter deltas.
    pub fn query_ordered_with_work_limit_report_after_snapshot<F>(
        &self,
        q: &LogQuery,
        order: LogQueryOrder,
        max_entries: Option<usize>,
        max_work_entries: Option<usize>,
        after_snapshot: F,
    ) -> Result<(Vec<LogEntry>, LogQueryExecutionReport), String>
    where
        F: FnOnce(),
    {
        if max_work_entries == Some(0) {
            return Err("max_work_entries must be positive".into());
        }
        let started = Instant::now();
        let snapshot_started = Instant::now();
        let snapshot = self.snapshot_query(q, false, max_work_entries)?;
        let snapshot_ns = elapsed_ns(snapshot_started);
        after_snapshot();

        let materialize_started = Instant::now();
        let candidate_blocks = snapshot.candidate_blocks;
        let buffered_entries = snapshot.buffered.len() as u64;
        let snapshot_payload_bytes = snapshot.payload_bytes;
        let stable_locations = snapshot.stable_locations;
        let mut payload_bytes_read = snapshot_payload_bytes;
        let mut decoded_entries = 0u64;
        let mut matched_entries = 0u64;
        let mut blocks_skipped_by_bound = 0u64;
        let mut clp_pruned_blocks = 0u64;
        let mut clp_skipped_rows = 0u64;
        let mut work_entries = snapshot.buffered_entries_considered;
        Self::enforce_query_work_limit(work_entries, max_work_entries)?;
        let mut out: Vec<LogEntry>;

        if let Some(capacity) = max_entries {
            let mut blocks = snapshot.blocks;
            match order {
                LogQueryOrder::Asc => {
                    blocks.sort_by_key(|block| (block.meta.ts_min, block.sequence))
                }
                LogQueryOrder::Desc => blocks.sort_by(|a, b| {
                    b.meta
                        .ts_max
                        .cmp(&a.meta.ts_max)
                        .then_with(|| a.sequence.cmp(&b.sequence))
                }),
            }

            // Do not reserve the SQL LIMIT up front: direct callers may use a
            // huge sentinel limit, and allocation must follow actual matches
            // rather than an untrusted integer in the statement.
            let mut heap: BinaryHeap<BoundedEntry> = BinaryHeap::new();
            let buffered_source = candidate_blocks as usize;
            for (row, entry) in snapshot.buffered.into_iter().enumerate() {
                matched_entries = matched_entries.saturating_add(1);
                Self::retain_bounded(
                    &mut heap,
                    BoundedEntry {
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
                            // Equality is deliberately not enough: a row at
                            // the same timestamp may win on canonical order.
                            LogQueryOrder::Asc => block.meta.ts_min > worst.entry.ts,
                            LogQueryOrder::Desc => block.meta.ts_max < worst.entry.ts,
                        }));
                if cannot_displace {
                    blocks_skipped_by_bound =
                        blocks_skipped_by_bound.saturating_add((block_count - position) as u64);
                    break;
                }

                self.store.check_cancelled()?;
                let bytes = match (block.payload, block.location) {
                    (Some(bytes), None) => bytes,
                    (None, Some(loc)) => {
                        let bytes = self.store.read_block(&loc)?;
                        payload_bytes_read = payload_bytes_read.saturating_add(bytes.len() as u64);
                        bytes
                    }
                    _ => return Err("invalid log query block snapshot".into()),
                };
                // CLP-dictionary path (issue #2): a proven-absent needle
                // skips the block before decode; otherwise the filtered
                // decode materializes only candidate rows. Work charged
                // to max_work_entries is the decode work actually done —
                // zero for pruned blocks, candidate rows for the rest.
                if let Some(needle) = q
                    .message_contains
                    .as_deref()
                    .filter(|needle| !needle.is_empty())
                {
                    if matches!(block_message_feasible(&bytes, needle), Ok(false)) {
                        clp_pruned_blocks = clp_pruned_blocks.saturating_add(1);
                        continue;
                    }
                    let filtered = decode_block_filtered(&bytes, needle)?;
                    clp_skipped_rows = clp_skipped_rows.saturating_add(
                        (filtered.total_rows as u64).saturating_sub(filtered.candidate_rows),
                    );
                    work_entries = work_entries.saturating_add(filtered.candidate_rows as usize);
                    Self::enforce_query_work_limit(work_entries, max_work_entries)?;
                    self.store.check_cancelled()?;
                    decoded_entries = decoded_entries.saturating_add(filtered.candidate_rows);
                    for (row, entry) in filtered.rows {
                        if entry_matches(&entry, q) {
                            matched_entries = matched_entries.saturating_add(1);
                            Self::retain_bounded(
                                &mut heap,
                                BoundedEntry {
                                    entry,
                                    sequence: QuerySequence {
                                        source: block.sequence,
                                        row,
                                    },
                                    order,
                                },
                                capacity,
                            );
                        }
                    }
                    continue;
                }
                work_entries = work_entries.saturating_add(block.meta.entry_count as usize);
                Self::enforce_query_work_limit(work_entries, max_work_entries)?;
                let entries = decode_block(&bytes)?;
                self.store.check_cancelled()?;
                decoded_entries = decoded_entries.saturating_add(entries.len() as u64);
                for (row, entry) in entries.into_iter().enumerate() {
                    if entry_matches(&entry, q) {
                        matched_entries = matched_entries.saturating_add(1);
                        Self::retain_bounded(
                            &mut heap,
                            BoundedEntry {
                                entry,
                                sequence: QuerySequence {
                                    source: block.sequence,
                                    row,
                                },
                                order,
                            },
                            capacity,
                        );
                    }
                }
            }

            let mut ranked = heap.into_vec();
            ranked.sort_by(|a, b| match order {
                LogQueryOrder::Asc => a
                    .entry
                    .ts
                    .cmp(&b.entry.ts)
                    .then_with(|| canonical_entry_cmp(&a.entry, &b.entry))
                    .then_with(|| a.sequence.cmp(&b.sequence)),
                LogQueryOrder::Desc => b
                    .entry
                    .ts
                    .cmp(&a.entry.ts)
                    .then_with(|| canonical_entry_cmp(&a.entry, &b.entry))
                    .then_with(|| a.sequence.cmp(&b.sequence)),
            });
            out = ranked.into_iter().map(|ranked| ranked.entry).collect();
        } else {
            let mut blocks = snapshot.blocks;
            blocks.sort_by_key(|block| block.sequence);
            out = Vec::new();
            for block in blocks {
                self.store.check_cancelled()?;
                let bytes = match (block.payload, block.location) {
                    (Some(bytes), None) => bytes,
                    (None, Some(loc)) => {
                        let bytes = self.store.read_block(&loc)?;
                        payload_bytes_read = payload_bytes_read.saturating_add(bytes.len() as u64);
                        bytes
                    }
                    _ => return Err("invalid log query block snapshot".into()),
                };
                if let Some(needle) = q
                    .message_contains
                    .as_deref()
                    .filter(|needle| !needle.is_empty())
                {
                    if matches!(block_message_feasible(&bytes, needle), Ok(false)) {
                        clp_pruned_blocks = clp_pruned_blocks.saturating_add(1);
                        continue;
                    }
                    let filtered = decode_block_filtered(&bytes, needle)?;
                    clp_skipped_rows = clp_skipped_rows.saturating_add(
                        (filtered.total_rows as u64).saturating_sub(filtered.candidate_rows),
                    );
                    work_entries = work_entries.saturating_add(filtered.candidate_rows as usize);
                    Self::enforce_query_work_limit(work_entries, max_work_entries)?;
                    self.store.check_cancelled()?;
                    decoded_entries = decoded_entries.saturating_add(filtered.candidate_rows);
                    for (_row, entry) in filtered.rows {
                        if entry_matches(&entry, q) {
                            matched_entries = matched_entries.saturating_add(1);
                            out.push(entry);
                        }
                    }
                    continue;
                }
                work_entries = work_entries.saturating_add(block.meta.entry_count as usize);
                Self::enforce_query_work_limit(work_entries, max_work_entries)?;
                let entries = decode_block(&bytes)?;
                self.store.check_cancelled()?;
                decoded_entries = decoded_entries.saturating_add(entries.len() as u64);
                for entry in entries {
                    if entry_matches(&entry, q) {
                        matched_entries = matched_entries.saturating_add(1);
                        out.push(entry);
                    }
                }
            }
            matched_entries = matched_entries.saturating_add(snapshot.buffered.len() as u64);
            out.extend(snapshot.buffered);
            // Equal timestamps use the released product's canonical payload
            // comparator; exact duplicate rows retain stable source order.
            match order {
                LogQueryOrder::Asc => {
                    out.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| canonical_entry_cmp(a, b)))
                }
                LogQueryOrder::Desc => {
                    out.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| canonical_entry_cmp(a, b)))
                }
            }
        }
        self.store.check_cancelled()?;
        let materialize_ns = elapsed_ns(materialize_started);
        let total_ns = elapsed_ns(started);
        let buffered_entries_processed = snapshot.buffered_entries_considered as u64;
        let processed_entries = decoded_entries.saturating_add(buffered_entries_processed);
        let processed_blocks = candidate_blocks.saturating_sub(blocks_skipped_by_bound);
        let report = LogQueryExecutionReport {
            query_total_ns: total_ns,
            query_snapshot_ns: snapshot_ns,
            query_materialize_ns: materialize_ns,
            snapshot_payload_bytes,
            payload_bytes_read,
            candidate_blocks,
            processed_blocks,
            blocks_skipped_by_bound,
            buffered_entries_processed,
            decoded_entries,
            processed_entries,
            matched_entries,
            returned_entries: out.len() as u64,
            values_read: processed_entries.saturating_mul(3),
            timestamps_read: processed_entries,
            stable_location_snapshot: stable_locations,
        };

        self.profile.query_count.fetch_add(1, Ordering::Relaxed);
        self.profile
            .query_total_ns
            .fetch_add(total_ns, Ordering::Relaxed);
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
            .query_clp_pruned_blocks
            .fetch_add(clp_pruned_blocks, Ordering::Relaxed);
        self.profile
            .query_clp_skipped_rows
            .fetch_add(clp_skipped_rows, Ordering::Relaxed);
        self.profile
            .query_matched_entries
            .fetch_add(matched_entries, Ordering::Relaxed);
        self.profile
            .query_returned_entries
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        if let Some(capacity) = max_entries {
            let capacity = capacity as u64;
            self.profile
                .query_bounded_count
                .fetch_add(1, Ordering::Relaxed);
            self.profile
                .query_bounded_requested_entries
                .fetch_add(capacity, Ordering::Relaxed);
            self.profile
                .query_bounded_max_entries
                .fetch_max(capacity, Ordering::Relaxed);
            self.profile
                .query_blocks_skipped_by_bound
                .fetch_add(blocks_skipped_by_bound, Ordering::Relaxed);
        }
        Ok((out, report))
    }

    fn enforce_query_work_limit(
        work_entries: usize,
        max_work_entries: Option<usize>,
    ) -> Result<(), String> {
        if max_work_entries.is_some_and(|limit| work_entries > limit) {
            return Err(format!(
                "log query exceeded max_work_entries={}",
                max_work_entries.expect("checked above")
            ));
        }
        Ok(())
    }

    fn retain_bounded(heap: &mut BinaryHeap<BoundedEntry>, entry: BoundedEntry, capacity: usize) {
        if capacity == 0 {
            return;
        }
        if heap.len() < capacity {
            heap.push(entry);
        } else if heap.peek().is_some_and(|worst| entry < *worst) {
            let _ = heap.pop();
            heap.push(entry);
        }
    }

    /// Return the lexicographically first `max_values` distinct string
    /// projections for one metadata key across the exact query predicate.
    ///
    /// The scan owns at most one decoded block and `max_values + 1` strings at
    /// a time. Keeping the smallest values, rather than stopping after the
    /// first full set, makes the result deterministic across compaction and
    /// block-layout changes without materializing every matching entry.
    pub fn field_values(
        &self,
        q: &LogQuery,
        key: &str,
        max_values: usize,
    ) -> Result<Vec<String>, String> {
        self.field_values_after_snapshot(q, key, max_values, || {})
    }

    pub fn field_values_after_snapshot<F>(
        &self,
        q: &LogQuery,
        key: &str,
        max_values: usize,
        after_snapshot: F,
    ) -> Result<Vec<String>, String>
    where
        F: FnOnce(),
    {
        self.field_values_with_work_limit_after_snapshot(q, key, max_values, None, after_snapshot)
    }

    pub fn field_values_with_work_limit_after_snapshot<F>(
        &self,
        q: &LogQuery,
        key: &str,
        max_values: usize,
        max_work_entries: Option<usize>,
        after_snapshot: F,
    ) -> Result<Vec<String>, String>
    where
        F: FnOnce(),
    {
        if key.is_empty() {
            return Err("log field-values key must not be empty".into());
        }
        if max_work_entries == Some(0) {
            return Err("max_work_entries must be positive".into());
        }

        let snapshot = self.snapshot_query(q, false, max_work_entries)?;
        after_snapshot();
        let mut work_entries = snapshot.buffered_entries_considered;
        Self::enforce_query_work_limit(work_entries, max_work_entries)?;
        let mut values = BTreeSet::new();

        for entry in &snapshot.buffered {
            if let Some(value) = entry.meta_value(key) {
                Self::retain_field_value(&mut values, value, max_values);
            }
        }

        for block in snapshot.blocks {
            self.store.check_cancelled()?;
            let bytes = match (block.payload, block.location) {
                (Some(bytes), None) => bytes,
                (None, Some(location)) => self.store.read_block(&location)?,
                _ => return Err("invalid log field-values block snapshot".into()),
            };
            if let Some(needle) = q
                .message_contains
                .as_deref()
                .filter(|needle| !needle.is_empty())
            {
                if matches!(block_message_feasible(&bytes, needle), Ok(false)) {
                    self.profile
                        .query_clp_pruned_blocks
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let filtered = decode_block_filtered(&bytes, needle)?;
                self.profile.query_clp_skipped_rows.fetch_add(
                    (filtered.total_rows as u64).saturating_sub(filtered.candidate_rows),
                    Ordering::Relaxed,
                );
                work_entries = work_entries.saturating_add(filtered.candidate_rows as usize);
                Self::enforce_query_work_limit(work_entries, max_work_entries)?;
                self.store.check_cancelled()?;
                for (_row, entry) in filtered.rows {
                    if entry_matches(&entry, q) {
                        if let Some(value) = entry.meta_value(key) {
                            Self::retain_field_value(&mut values, value, max_values);
                        }
                    }
                }
                continue;
            }
            work_entries = work_entries.saturating_add(block.meta.entry_count as usize);
            Self::enforce_query_work_limit(work_entries, max_work_entries)?;
            let entries = decode_block(&bytes)?;
            self.store.check_cancelled()?;
            for entry in entries {
                if entry_matches(&entry, q) {
                    if let Some(value) = entry.meta_value(key) {
                        Self::retain_field_value(&mut values, value, max_values);
                    }
                }
            }
        }

        self.store.check_cancelled()?;
        Ok(values.into_iter().collect())
    }

    fn retain_field_value(values: &mut BTreeSet<String>, value: &str, max_values: usize) {
        if max_values == 0 {
            return;
        }
        values.insert(value.to_owned());
        if values.len() > max_values {
            let _ = values.pop_last();
        }
    }

    /// Count exact matches without materializing a rowset. Fully covered
    /// blocks are answered from `entry_count` when every filter is proven by
    /// the block itself (unfiltered blocks, or a matching level-pure block).
    /// Boundary, legacy-mixed, metadata-filtered, and message-filtered blocks
    /// are decoded one at a time.
    pub fn count(&self, q: &LogQuery) -> Result<u64, String> {
        self.count_after_snapshot(q, || {})
    }

    /// Native count with the same snapshot ownership callback used by row
    /// queries. The extension releases its cross-connection read permit after
    /// the generation is captured, before boundary blocks are decoded.
    pub fn count_after_snapshot<F>(&self, q: &LogQuery, after_snapshot: F) -> Result<u64, String>
    where
        F: FnOnce(),
    {
        self.count_with_work_limit_after_snapshot(q, None, after_snapshot)
    }

    pub fn count_with_work_limit_after_snapshot<F>(
        &self,
        q: &LogQuery,
        max_work_entries: Option<usize>,
        after_snapshot: F,
    ) -> Result<u64, String>
    where
        F: FnOnce(),
    {
        if q.message_like_prune.is_some() {
            return Err(
                "native count requires an exact message_contains predicate, not LIKE pruning"
                    .into(),
            );
        }
        if max_work_entries == Some(0) {
            return Err("max_work_entries must be positive".into());
        }

        let started = Instant::now();
        let snapshot_started = Instant::now();
        let snapshot = self.snapshot_query(q, q.level.is_some(), max_work_entries)?;
        let snapshot_ns = elapsed_ns(snapshot_started);
        after_snapshot();

        let mut total = snapshot.buffered.len() as u64;
        let mut payload_bytes_read = snapshot.payload_bytes;
        let mut metadata_blocks = 0u64;
        let mut metadata_entries = 0u64;
        let mut decoded_blocks = 0u64;
        let mut decoded_entries = 0u64;
        let mut work_entries = snapshot.buffered_entries_considered;
        Self::enforce_query_work_limit(work_entries, max_work_entries)?;

        for block in snapshot.blocks {
            self.store.check_cancelled()?;
            let fully_covered = block.meta.ts_min >= q.ts_min && block.meta.ts_max <= q.ts_max;
            let message_free = q
                .message_contains
                .as_deref()
                .is_none_or(|needle| needle.is_empty());
            let level_proven = match (q.level, q.severity.as_ref()) {
                (None, None) => true,
                (Some(level), None) => block.partition == Some(level),
                // Legacy codecs only contain the original four severity
                // names, so an exact predicate matching the bucket name is
                // proven by their partition metadata. Rich codecs must decode
                // because that same bucket can contain critical/alert/etc.
                (Some(level), Some(severity))
                    if severity == level_name(level)
                        && !matches!(
                            block.meta.codec,
                            CODEC_RICH_RAW | CODEC_RICH_COLUMNAR | CODEC_RICH_TEMPLATE
                        ) =>
                {
                    block.partition == Some(level)
                }
                _ => false,
            };
            if fully_covered && message_free && q.metadata_eq.is_empty() && level_proven {
                let entries = block.meta.entry_count as u64;
                total = total.saturating_add(entries);
                metadata_blocks = metadata_blocks.saturating_add(1);
                metadata_entries = metadata_entries.saturating_add(entries);
                continue;
            }

            let bytes = match (block.payload, block.location) {
                (Some(bytes), None) => bytes,
                (None, Some(loc)) => {
                    let bytes = self.store.read_block(&loc)?;
                    payload_bytes_read = payload_bytes_read.saturating_add(bytes.len() as u64);
                    bytes
                }
                _ => return Err("invalid log count block snapshot".into()),
            };
            if let Some(needle) = q
                .message_contains
                .as_deref()
                .filter(|needle| !needle.is_empty())
            {
                if matches!(block_message_feasible(&bytes, needle), Ok(false)) {
                    self.profile
                        .query_clp_pruned_blocks
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let filtered = decode_block_filtered(&bytes, needle)?;
                self.profile.query_clp_skipped_rows.fetch_add(
                    (filtered.total_rows as u64).saturating_sub(filtered.candidate_rows),
                    Ordering::Relaxed,
                );
                work_entries = work_entries.saturating_add(filtered.candidate_rows as usize);
                Self::enforce_query_work_limit(work_entries, max_work_entries)?;
                self.store.check_cancelled()?;
                decoded_blocks = decoded_blocks.saturating_add(1);
                decoded_entries = decoded_entries.saturating_add(filtered.candidate_rows);
                total = total.saturating_add(
                    filtered
                        .rows
                        .iter()
                        .filter(|(_, entry)| entry_matches(entry, q))
                        .count() as u64,
                );
                continue;
            }
            work_entries = work_entries.saturating_add(block.meta.entry_count as usize);
            Self::enforce_query_work_limit(work_entries, max_work_entries)?;
            let entries = decode_block(&bytes)?;
            self.store.check_cancelled()?;
            decoded_blocks = decoded_blocks.saturating_add(1);
            decoded_entries = decoded_entries.saturating_add(entries.len() as u64);
            total = total.saturating_add(
                entries
                    .iter()
                    .filter(|entry| entry_matches(entry, q))
                    .count() as u64,
            );
        }

        self.store.check_cancelled()?;

        self.profile
            .native_count_count
            .fetch_add(1, Ordering::Relaxed);
        self.profile
            .native_count_total_ns
            .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        self.profile
            .native_count_snapshot_ns
            .fetch_add(snapshot_ns, Ordering::Relaxed);
        self.profile
            .native_count_payload_bytes_read
            .fetch_add(payload_bytes_read, Ordering::Relaxed);
        self.profile
            .native_count_metadata_blocks
            .fetch_add(metadata_blocks, Ordering::Relaxed);
        self.profile
            .native_count_metadata_entries
            .fetch_add(metadata_entries, Ordering::Relaxed);
        self.profile
            .native_count_decoded_blocks
            .fetch_add(decoded_blocks, Ordering::Relaxed);
        self.profile
            .native_count_decoded_entries
            .fetch_add(decoded_entries, Ordering::Relaxed);
        Ok(total)
    }

    fn snapshot_query(
        &self,
        q: &LogQuery,
        include_partitions: bool,
        max_work_entries: Option<usize>,
    ) -> Result<LogQuerySnapshot, String> {
        let _transition = self.transition_read();
        self.store.check_cancelled()?;
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
        if q.message_contains.is_some() || q.message_like_prune.is_some() {
            let mut required = BTreeSet::new();
            if let Some(needle) = &q.message_contains {
                required.extend(Self::message_contains_trigrams(needle));
            }
            if let Some(pattern) = &q.message_like_prune {
                required.extend(Self::like_pattern_trigrams(pattern));
            }
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
        // Only native count needs level-purity proof. Row queries already
        // filter decoded entries exactly, so avoid an O(total blocks) map and
        // allocation on their latency-sensitive snapshot path.
        let partitions: Option<HashMap<i64, Option<u8>>> = include_partitions.then(|| {
            let index = self.index_lock();
            index
                .iter()
                .map(|entry| (entry.loc.id, entry.partition))
                .collect()
        });
        // Buffer membership is part of the protected generation. Reject its
        // complete examined-entry cost before evaluating predicates or
        // cloning matching entries; otherwise a very small hard budget could
        // still pay the whole live-buffer CPU/allocation cost before failing.
        let buffer = self.buffer_lock();
        let buffered_entries_considered = buffer.len();
        Self::enforce_query_work_limit(buffered_entries_considered, max_work_entries)?;
        let buffered = buffer
            .iter()
            .filter(|entry| entry_matches(entry, q))
            .cloned()
            .collect();
        drop(buffer);

        let stable_locations = self.store.query_snapshot_keeps_locations_readable();
        let mut blocks = Vec::with_capacity(locs.len());
        let mut payload_bytes = 0u64;
        if stable_locations {
            for (sequence, (location, meta)) in locs.into_iter().enumerate() {
                blocks.push(LogQueryBlockSnapshot {
                    payload: None,
                    location: Some(location),
                    meta,
                    partition: partitions
                        .as_ref()
                        .and_then(|partitions| partitions.get(&location.id).copied().flatten()),
                    sequence,
                });
            }
        } else {
            for (sequence, (location, meta)) in locs.into_iter().enumerate() {
                let bytes = self.store.read_block(&location)?;
                payload_bytes = payload_bytes.saturating_add(bytes.len() as u64);
                blocks.push(LogQueryBlockSnapshot {
                    payload: Some(bytes),
                    location: None,
                    meta,
                    partition: partitions
                        .as_ref()
                        .and_then(|partitions| partitions.get(&location.id).copied().flatten()),
                    sequence,
                });
            }
        }
        self.store.check_cancelled()?;
        Ok(LogQuerySnapshot {
            blocks,
            buffered,
            buffered_entries_considered,
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
            let mut counts: std::collections::BTreeMap<(i64, String), u64> =
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
                // Rich blocks partition on the four storage buckets but expose
                // the product's complete severity vocabulary. Their payload is
                // therefore authoritative for both grouping and exact filters.
                if matches!(meta.codec, CODEC_RICH_RAW | CODEC_RICH_COLUMNAR) {
                    decode.push(loc);
                    continue;
                }
                match partition {
                    Some(level)
                        if (filter.level.is_none() || filter.level == Some(level))
                            && filter
                                .severity
                                .as_deref()
                                .is_none_or(|severity| severity == level_name(level)) =>
                    {
                        let inside = meta.ts_min >= start && meta.ts_max <= stop;
                        if inside && bucket_of(meta.ts_min) == bucket_of(meta.ts_max) {
                            *counts
                                .entry((bucket_of(meta.ts_min), level_name(level).to_string()))
                                .or_insert(0) += meta.entry_count as u64;
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
                if let Some(severity) = &filter.severity {
                    if e.severity_name() != severity {
                        return;
                    }
                }
                *counts
                    .entry((bucket_of(e.ts), e.severity_name().to_string()))
                    .or_insert(0) += 1;
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
            return Ok(counts.into_iter().map(|((b, g), n)| (b, g, n)).collect());
        }

        let entries = self.query(filter)?;
        let mut counts: std::collections::BTreeMap<(i64, String), u64> =
            std::collections::BTreeMap::new();
        for e in &entries {
            let bucket_ts = bucket_of(e.ts);
            let group = if group_by == "level" {
                e.severity_name().to_string()
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
            let raw = index.iter().filter(|e| is_raw_codec(e.meta.codec)).count();
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
    if let Some(severity) = &q.severity {
        if e.severity_name() != severity {
            return false;
        }
    }
    for (k, v) in &q.metadata_eq {
        if e.meta_value(k) != Some(v.as_str()) {
            return false;
        }
    }
    if let Some(needle) = &q.message_contains {
        if !message_contains_case_insensitive(&e.message, needle) {
            return false;
        }
    }
    true
}

fn normalize_rich_entry(entry: &mut LogEntry) -> Result<(), String> {
    if let Some(severity) = &entry.severity {
        let canonical = canonical_severity(severity)?;
        let bucket = level_from_name(canonical)?;
        if bucket != entry.level {
            return Err(format!(
                "severity {canonical:?} belongs to level bucket {bucket}, not {}",
                entry.level
            ));
        }
        if canonical != severity {
            entry.severity = Some(canonical.to_owned());
        }
    }
    Ok(())
}

fn canonical_entry_cmp(left: &LogEntry, right: &LogEntry) -> CmpOrdering {
    left.message
        .cmp(&right.message)
        .then_with(|| left.severity_name().cmp(right.severity_name()))
        .then_with(|| match (&left.metadata_json, &right.metadata_json) {
            (Some(left), Some(right)) => left.cmp(right),
            _ => left.metadata.cmp(&right.metadata),
        })
}

fn message_contains_case_insensitive(message: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.is_ascii() {
        let needle = needle.as_bytes();
        return message
            .as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle));
    }
    message.to_lowercase().contains(&needle.to_lowercase())
}
