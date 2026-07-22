//! Span block store for traces (PLAN.md "Phase 2 — trace store",
//! Session 6) — the traces twin of `blocks/`. Fresh Rust implementation
//! of the timeless_traces (Elixir) design: spans accumulate in a buffer,
//! flush as RAW blocks, 'optimize' compacts to zstd-columnar and merges
//! small blocks, an inverted TERM index prunes reads — plus the one
//! structure logs don't have: a TRACE INDEX mapping each packed 16-byte
//! trace id to the blocks containing its spans, so
//! `WHERE trace_id = x'...'` decompresses only those blocks (the hero
//! query of the whole trace store).
//!
//! WHY A PARALLEL MODULE instead of genericizing BlockEngine: the trace
//! index changes the STORE CONTRACT itself (blocks carry trace-id rows
//! that must be created and deleted in the same operation as the block;
//! the store answers query_trace). Making BlockStore generic over a
//! per-payload aux type would ripple through every existing store impl
//! and test wrapper for zero logs benefit — and "logs behavior must not
//! change" is a Session 6 gate. So this module mirrors the blocks/
//! skeleton line-for-line where the logic is identical (flush
//! partitioning, greedy merge with the ts-span cap, recovery partition
//! derivation, buffer-merge queries) and shares the actual primitives:
//! BlockLoc/BlockMeta, the codec constants, the zstd helpers and the
//! bounds-checked Reader (blocks/codec.rs, now pub(crate)). Any future
//! fix to the shared skeleton should be applied to BOTH engines — they
//! are deliberately diff-able against each other.
//!
//! Deliberate design choices (mirroring or contrasting with logs):
//!   - Timestamps are NANOSECONDS by OTel convention (logs are ms,
//!     metrics s). The engine itself stays unit-agnostic — every ts
//!     knob is "in ts units" and the traces vtab passes ns values; the
//!     vtab records the unit in `_meta` for tooling.
//!   - Partition dimension = STATUS (unset/ok/error), the traces analog
//!     of the Session 5 "level-term weakness" fix: 'find the failed
//!     requests' is THE trace query, and status-pure blocks mean a
//!     `status:error` posting-list lookup prunes the ~95%+ of blocks
//!     with no errors instead of matching all of them.
//!   - Terms are ALWAYS service:/kind:/status:/name: — no index_keys
//!     knob. Logs need an allowlist because log metadata is open-ended
//!     user data where indexing an identifier-valued key would bloat
//!     the term table past the data. Span dimensions are the opposite:
//!     all four are low-cardinality BY OTEL CONVENTION (services and
//!     operation names are small bounded sets, kind and status are
//!     enums), so they are indexed unconditionally. High-cardinality
//!     span data lives in `attributes`, which is scan-only, exactly
//!     like non-indexed log metadata.

pub mod codec;
pub mod engine;
pub mod mem;

#[cfg(test)]
mod tests;

pub use codec::{decode_span_block, encode_span_block};
pub use engine::{SpanBlockEngine, SpanEngineConfig, SpanQuery};
pub use mem::MemSpanStore;

// Shared with the logs block store on purpose: a BlockLoc is a BlockLoc
// (opaque store-chosen row id) and BlockMeta's fields are exactly the
// metadata columns both shadow schemas keep. Re-exported here so spans
// users don't need to know where they were born.
pub use crate::blocks::{BlockLoc, BlockMeta};

/// OTel span kinds, stored as one byte per span:
/// 0=internal 1=server 2=client 3=producer 4=consumer.
pub const KIND_NAMES: [&str; 5] = ["internal", "server", "client", "producer", "consumer"];

/// OTel span statuses: 0=unset 1=ok 2=error. Also the flush PARTITION
/// dimension (see module header).
pub const STATUS_NAMES: [&str; 3] = ["unset", "ok", "error"];

/// Strict name → byte mapping, same policy as log levels: a typo'd kind
/// silently coerced to "internal" would be data corruption.
pub fn kind_from_name(name: &str) -> Result<u8, String> {
    match name {
        "internal" => Ok(0),
        "server" => Ok(1),
        "client" => Ok(2),
        "producer" => Ok(3),
        "consumer" => Ok(4),
        other => Err(format!(
            "unknown span kind {other:?}; expected one of: internal, server, client, producer, consumer"
        )),
    }
}

/// Byte back to name. Only call with a validated kind (decode and push
/// both reject out-of-range bytes).
pub fn kind_name(kind: u8) -> &'static str {
    KIND_NAMES[kind as usize]
}

pub fn status_from_name(name: &str) -> Result<u8, String> {
    match name {
        "unset" => Ok(0),
        "ok" => Ok(1),
        "error" => Ok(2),
        other => Err(format!(
            "unknown span status {other:?}; expected one of: unset, ok, error"
        )),
    }
}

pub fn status_name(status: u8) -> &'static str {
    STATUS_NAMES[status as usize]
}

/// One span — the unit the buffer holds and span blocks store.
///
/// Ids are PACKED BINARY (the timeless_traces lesson: no hex text
/// anywhere in storage — hex doubles the bytes and compresses worse).
/// `attributes` is a flat (key, value) list kept SORTED by key, same
/// contract as LogEntry.metadata (canonical JSON for free, binary-
/// searchable, compression-friendly).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanEntry {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// None for root spans.
    pub parent_span_id: Option<[u8; 8]>,
    /// Operation name ("GET /api/orders", "db.query", ...).
    pub name: String,
    pub service: String,
    /// 0=internal 1=server 2=client 3=producer 4=consumer (KIND_NAMES).
    pub kind: u8,
    /// 0=unset 1=ok 2=error (STATUS_NAMES).
    pub status: u8,
    /// Start time in NANOSECONDS (OTel convention).
    pub start_ts: i64,
    pub duration_ns: i64,
    pub attributes: Vec<(String, String)>,
}

impl SpanEntry {
    /// Value for `key`, if present (attributes are sorted → binary search).
    pub fn attr_value(&self, key: &str) -> Option<&str> {
        self.attributes
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.attributes[i].1.as_str())
    }
}

/// A fully-encoded span block ready to persist: payload + metadata +
/// index rows. Compared to blocks::EncodedBlock there is ONE extra
/// field, and it is the whole reason this module exists: the deduped
/// set of trace ids present in the block, which the store must record
/// in its trace index IN THE SAME OPERATION as the block row (the
/// PLAN.md never-dangle rule, extended from posting lists to the trace
/// index).
pub struct EncodedSpanBlock {
    pub meta: BlockMeta,
    pub data: Vec<u8>,
    /// Deduplicated, sorted terms ("status:error", "service:api", ...).
    pub terms: Vec<String>,
    /// Deduplicated, sorted packed trace ids present in this block.
    pub trace_ids: Vec<[u8; 16]>,
}

/// Storage backend seam for span blocks — blocks::BlockStore plus the
/// trace index. Same transaction contract as every other store trait in
/// this crate: methods must NOT open transactions; in the extension
/// they run re-entrantly inside vtab callbacks and ride the host's
/// enclosing transaction, which IS the atomicity that lets block rows,
/// term rows and trace-index rows appear and disappear together.
pub trait SpanBlockStore: Send + Sync {
    /// Persist a batch of blocks (a status-partitioned flush emits up
    /// to three). Each block's term rows AND trace-index rows are
    /// written in the same operation — a block is never visible without
    /// its index rows. Locs come back in input order.
    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String>;

    /// Atomic swap for compaction: persist `add` (with their term +
    /// trace rows), remove `remove` (and THEIR term + trace rows).
    /// `on_committed` fires after the adds are readable and before the
    /// removes, so the engine can swap its index with no window where a
    /// query could hit a missing block.
    fn replace_blocks(
        &self,
        add: &[EncodedSpanBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String>;

    /// Read one block's stored payload bytes.
    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String>;

    /// Remove blocks AND their term + trace-index rows in the same
    /// operation. Per-block error strings; missing = reported, not fatal.
    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String>;

    /// Recovery: every persisted block's metadata (never the payloads).
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String>;

    /// Posting-list intersection + ts-range overlap, identical contract
    /// to blocks::BlockStore::query_terms (returns metas so callers
    /// never re-read rows the store already visited).
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String>;

    /// THE trace-store operation: every block containing spans of
    /// `trace_id`, via the trace index — never a scan. The hero query
    /// (`WHERE trace_id = x'...'`) reads exactly these blocks.
    fn query_trace(&self, trace_id: &[u8; 16]) -> Result<Vec<(BlockLoc, BlockMeta)>, String>;

    /// Small key/value config persistence (ts unit, schema version).
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String>;
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
}
