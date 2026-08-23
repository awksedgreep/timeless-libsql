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
//!   - Query terms are ALWAYS service:/kind:/status:/name: — no index_keys
//!     knob. Logs need an allowlist because log metadata is open-ended
//!     user data where indexing an identifier-valued key would bloat
//!     the term table past the data. Span dimensions are the opposite:
//!     all four are low-cardinality BY OTEL CONVENTION (services and
//!     operation names are small bounded sets, kind and status are
//!     enums), so they are indexed unconditionally. High-cardinality
//!     span data lives in `attributes`, which is scan-only, exactly
//!     like non-indexed log metadata.
//!   - Blocks also carry collision-free service/operation compound terms and
//!     an `operations:` generation marker. Public discovery can therefore be
//!     metadata-native while mixed legacy/new databases fall back to decode.

use std::borrow::Cow;

mod attributes;
pub mod codec;
pub mod engine;
pub mod mem;

#[cfg(test)]
mod tests;

pub use attributes::{
    build_span_attribute_blooms, encode_span_attribute_indexes, parse_span_attribute_indexes,
    span_attribute_bloom_checksum, validate_span_attribute_bloom, SpanAttributeBloom,
    SpanAttributeFilter, SpanAttributeIndex, SpanAttributeScope, MAX_SPAN_ATTRIBUTE_INDEXES,
    SPAN_ATTRIBUTE_BLOOM_BYTES, SPAN_ATTRIBUTE_BLOOM_HASHES, SPAN_ATTRIBUTE_BLOOM_VERSION,
};
pub use codec::{decode_span_block, encode_span_block, SpanColumnMask, SpanDecodeProfile};
pub use engine::{
    SpanBlockEngine, SpanEngineConfig, SpanOptimizeBacklog, SpanOptimizeProfileSnapshot, SpanQuery,
    SpanQueryOrder, SpanQueryStream, TraceBucketStat,
};
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
/// The five JSON fields contain canonical JSON text. Keeping JSON at
/// this public boundary is intentional: OTel values are typed and may
/// be nested, so flattening them to string pairs loses information.
/// The SQLite extension validates and canonicalizes all four values
/// before they reach the engine. Direct engine users must provide an
/// object for `attributes`/`resource`/`instrumentation_scope` and arrays
/// for `events`/`links`.
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
    /// OTel status message. Empty string is the documented default for
    /// generation-1 blocks, which did not store this field.
    pub status_description: Cow<'static, str>,
    /// Start time in NANOSECONDS (OTel convention).
    pub start_ts: i64,
    pub duration_ns: i64,
    /// Canonical JSON object preserving scalar types and nested values.
    pub attributes: Cow<'static, str>,
    /// Canonical JSON array of OTel span events.
    pub events: Cow<'static, str>,
    /// Canonical JSON object of resource attributes.
    pub resource: Cow<'static, str>,
    /// Canonical JSON object containing instrumentation scope name,
    /// version, and attributes when supplied.
    pub instrumentation_scope: Cow<'static, str>,
    /// Canonical JSON array of OTel span links. Link ids are lowercase
    /// hexadecimal strings because JSON has no packed-byte type.
    pub links: Cow<'static, str>,
    pub trace_state: Cow<'static, str>,
    pub trace_flags: u32,
    pub dropped_attributes_count: u32,
    pub dropped_events_count: u32,
    pub dropped_links_count: u32,
    pub resource_schema_url: Cow<'static, str>,
    pub scope_schema_url: Cow<'static, str>,
    pub resource_dropped_attributes_count: u32,
    pub scope_dropped_attributes_count: u32,
}

impl SpanEntry {
    /// Logical raw bytes of this span as the public surface returns it:
    /// 50 fixed (16+8+8 ids, 1+1 kind/status, 8+8 start/duration) plus
    /// the byte lengths of the name, service, attributes, status
    /// message, events, resource, and scope strings. This is the demogen
    /// ground-truth definition (tools/demogen drive.rs) — the
    /// per-row/denormalized convention the Victoria/Tempo family quotes;
    /// links, trace_state, and the OTel bookkeeping counters are not
    /// part of it.
    pub fn raw_ingest_bytes(&self) -> u64 {
        50 + (self.name.len()
            + self.service.len()
            + self.attributes.len()
            + self.status_description.len()
            + self.events.len()
            + self.resource.len()
            + self.instrumentation_scope.len()) as u64
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
    /// Fixed-size, opt-in attribute filters for this block. Empty when the
    /// trace table has no configured attribute indexes.
    pub attribute_blooms: Vec<SpanAttributeBloom>,
}

/// Exact duration extrema for one persisted span block. Stores may persist
/// these alongside the ordinary [`BlockMeta`] to reject a duration-bounded
/// query without reading or decoding the payload. The bounds are additive:
/// legacy stores and legacy blocks may omit them and retain the exact decode
/// fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanDurationBounds {
    pub min_ns: i64,
    pub max_ns: i64,
}

impl SpanDurationBounds {
    pub fn new(min_ns: i64, max_ns: i64) -> Result<Self, String> {
        if min_ns > max_ns {
            return Err(format!(
                "invalid span duration bounds: minimum {min_ns} exceeds maximum {max_ns}"
            ));
        }
        Ok(Self { min_ns, max_ns })
    }

    pub fn excludes(self, query_min_ns: i64, query_max_ns: i64) -> bool {
        self.max_ns < query_min_ns || self.min_ns > query_max_ns
    }
}

/// Storage backend seam for span blocks — blocks::BlockStore plus the
/// trace index. Same transaction contract as every other store trait in
/// this crate: methods must NOT open transactions; in the extension
/// they run re-entrantly inside vtab callbacks and ride the host's
/// enclosing transaction, which IS the atomicity that lets block rows,
/// term rows and trace-index rows appear and disappear together.
pub trait SpanBlockStore: Send + Sync {
    /// Whether block locations captured by a query remain readable from the
    /// same logical snapshot after a concurrent writer replaces them.
    ///
    /// SQLite-backed stores return true because the outer virtual-table
    /// statement pins a database snapshot. Conservative stores leave this
    /// false, causing the engine to own candidate payload bytes before it
    /// releases its publication guard.
    fn query_snapshot_keeps_locations_readable(&self) -> bool {
        false
    }

    /// Persist a batch of blocks (a status-partitioned flush emits up
    /// to three). Each block's term rows AND trace-index rows are
    /// written in the same operation — a block is never visible without
    /// its index rows. Locs come back in input order.
    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String>;

    /// Persist blocks with exact duration extrema. Existing store
    /// implementations remain source-compatible through this conservative
    /// default: the blocks are stored normally and duration queries decode
    /// them. Stores that persist the additive metadata override this method.
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
        self.put_blocks(blocks)
    }

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

    /// Compaction counterpart to [`SpanBlockStore::put_blocks_with_duration_bounds`].
    /// The default preserves the established atomic replacement contract and
    /// merely omits the optional pruning metadata.
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
        self.replace_blocks(add, remove, on_committed)
    }

    /// Read one block's stored payload bytes.
    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String>;

    /// Remove blocks AND their term + trace-index rows in the same
    /// operation. Per-block error strings; missing = reported, not fatal.
    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String>;

    /// Recovery: every persisted block's metadata (never the payloads).
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String>;

    /// Metadata-only discovery for persisted blocks that predate duration
    /// extrema. The default is empty because stores that do not persist the
    /// additive optimization have nothing to backfill.
    fn blocks_missing_duration_bounds(&self) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        Ok(Vec::new())
    }

    /// Publish duration extrema for existing blocks without replacing their
    /// payloads or index rows. Implementations must update the complete slice
    /// in the caller's transaction or return an error.
    fn update_duration_bounds(
        &self,
        updates: &[(BlockLoc, SpanDurationBounds)],
    ) -> Result<(), String> {
        if updates.is_empty() {
            Ok(())
        } else {
            Err("span store does not support duration-bound backfill".into())
        }
    }

    /// Posting-list intersection + ts-range overlap, identical contract
    /// to blocks::BlockStore::query_terms (returns metas so callers
    /// never re-read rows the store already visited).
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String>;

    /// Duration-aware posting-list query. The default deliberately ignores
    /// the optional block metadata and preserves exact decode-time filtering.
    /// Stores with persisted extrema override it to reject only blocks that
    /// provably cannot overlap the inclusive duration interval.
    fn query_terms_with_duration_bounds(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
        _duration_min_ns: i64,
        _duration_max_ns: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.query_terms(terms, ts_min, ts_max)
    }

    /// THE trace-store operation: every block containing spans of
    /// `trace_id`, via the trace index — never a scan. The hero query
    /// (`WHERE trace_id = x'...'`) reads exactly these blocks.
    fn query_trace(&self, trace_id: &[u8; 16]) -> Result<Vec<(BlockLoc, BlockMeta)>, String>;

    /// Duration/time-aware trace-index query. Legacy/custom stores keep the
    /// existing trace lookup and the engine applies exact row filtering.
    fn query_trace_with_duration_bounds(
        &self,
        trace_id: &[u8; 16],
        _ts_min: i64,
        _ts_max: i64,
        _duration_min_ns: i64,
        _duration_max_ns: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.query_trace(trace_id)
    }

    /// Conservatively reject candidate blocks whose configured attribute
    /// filter proves the typed scalar absent. The default retains every block
    /// so legacy/custom stores remain exact without implementing the optional
    /// accelerator.
    fn filter_attribute_blocks(
        &self,
        _filter: &SpanAttributeFilter,
        blocks: &[(BlockLoc, BlockMeta)],
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        Ok(blocks.to_vec())
    }

    /// Distinct suffixes of terms beginning with `prefix`, when the backend
    /// can answer that from its posting-list catalog without payload reads.
    /// `None` asks the engine to use its exact decode fallback. This optional
    /// seam keeps discovery reusable without making an index capability a
    /// requirement for non-SQL stores.
    fn query_term_values(&self, _prefix: &str) -> Result<Option<Vec<String>>, String> {
        Ok(None)
    }

    /// Portable cancellation checkpoint. SQLite-backed stores execute a
    /// minimal statement so `sqlite3_interrupt` is observed even while a
    /// virtual-table query is doing host-side block work. Other stores have
    /// no ambient cancellation source.
    fn check_cancelled(&self) -> Result<(), String> {
        Ok(())
    }

    /// Small key/value config persistence (ts unit, schema version).
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String>;
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
}
