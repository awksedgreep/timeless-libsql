//! Generic block engine for log-shaped telemetry (PLAN.md "Phase 2 —
//! logs store", Session 5). Fresh Rust implementation of the design
//! proven by timeless_logs (Elixir): entries accumulate in an in-memory
//! buffer, flush as RAW blocks, and a later 'optimize' pass compacts
//! them into zstd-compressed columnar blocks and merges small blocks
//! (bigger dictionary window = better ratio). An inverted TERM index —
//! `level:<name>` plus a selective allowlist of metadata keys — lets
//! queries skip blocks without decompressing them. Blocks are
//! LEVEL-PARTITIONED: flush writes one level-pure block per level
//! present and optimize never merges across levels, so a single
//! `level:` term identifies exactly the blocks worth reading (see
//! engine::IndexEntry for the full story and the bench numbers).
//!
//! Deliberate differences from the Elixir donor:
//!   - No GenServers / background processes: flush, optimize and prune
//!     are explicit calls, driven by the vtab command idiom.
//!   - No snapshot/disk_log index durability machinery: the BlockStore
//!     backend rides SQLite transactions, which replace all of it.
//!   - Timestamps are OPAQUE i64s — the engine never assumes a unit
//!     (logs use ms, traces will use ns; PLAN.md says the shared block
//!     code must not assume). Every ts_* config knob is "in ts units".
//!
//! Session 6 reuse note: everything here except `LogEntry` itself is
//! payload-agnostic in design; traces will reuse the block/term/merge
//! skeleton with a span payload and a trace-id side index.

pub mod codec;
pub mod engine;
pub mod mem;

#[cfg(test)]
mod tests;

pub use codec::{
    decode_block, encode_block, CODEC_COLUMNAR, CODEC_COLUMNAR_V2, CODEC_RAW, CODEC_ZSTD,
};
pub use engine::{BlockEngine, BlockEngineConfig, LogQuery};
pub use mem::MemBlockStore;

/// Log severity levels, matching the timeless_logs on-disk convention:
/// 0=debug 1=info 2=warning 3=error. Stored as one byte per entry.
pub const LEVEL_NAMES: [&str; 4] = ["debug", "info", "warning", "error"];

/// Map a level name to its byte. Strict on purpose: a typo'd level
/// silently mapped to "info" would be a data corruption, so anything
/// outside the four known names is an error.
pub fn level_from_name(name: &str) -> Result<u8, String> {
    match name {
        "debug" => Ok(0),
        "info" => Ok(1),
        "warning" => Ok(2),
        "error" => Ok(3),
        other => Err(format!(
            "unknown log level {other:?}; expected one of: debug, info, warning, error"
        )),
    }
}

/// Byte back to name. Only call with a validated level (decode rejects
/// out-of-range bytes, push rejects out-of-range entries).
pub fn level_name(level: u8) -> &'static str {
    LEVEL_NAMES[level as usize]
}

/// One log entry — the unit the buffer holds and blocks store.
///
/// `metadata` is a flat list of (key, value) string pairs, kept SORTED
/// by key (push() enforces this). Sorted pairs give us: canonical JSON
/// output for free, binary-searchable equality filters, and identical
/// bytes for identical metadata (compression likes that too).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub ts: i64,
    /// 0=debug 1=info 2=warning 3=error (see LEVEL_NAMES).
    pub level: u8,
    pub message: String,
    pub metadata: Vec<(String, String)>,
}

impl LogEntry {
    /// Value for `key`, if present (metadata is sorted → binary search).
    pub fn meta_value(&self, key: &str) -> Option<&str> {
        self.metadata
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.metadata[i].1.as_str())
    }
}

/// Where a persisted block lives. An opaque i64 id chosen by the store:
/// the SQLite backend uses the `_blocks` rowid (explicit INTEGER PRIMARY
/// KEY — bare rowids can be renumbered by VACUUM), MemBlockStore uses a
/// counter. Mirrors ChunkLoc::Row from the metrics store seam, without
/// the File variant nobody needs here (see mem.rs on why there is no fs
/// backend).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlockLoc {
    pub id: i64,
}

/// Everything the engine's in-memory index needs about one persisted
/// block — same fields the store keeps in its metadata columns, so
/// scan() can rebuild this at recovery without touching block payloads.
#[derive(Clone, Copy, Debug)]
pub struct BlockMeta {
    pub ts_min: i64,
    pub ts_max: i64,
    pub entry_count: u32,
    /// CODEC_RAW, CODEC_ZSTD / CODEC_COLUMNAR (legacy, still
    /// decodable) or CODEC_COLUMNAR_V2 (codec byte 3 is reserved for
    /// OpenZL — PLAN.md "Codec strategy"; blocks with different codecs
    /// coexist).
    pub codec: u8,
}

/// A fully-encoded block ready to persist: payload bytes + the metadata
/// the store must record + the terms that index it. What flush() and
/// optimize() hand to the store.
pub struct EncodedBlock {
    pub meta: BlockMeta,
    pub data: Vec<u8>,
    /// Deduplicated, sorted terms ("level:error", "service:api", ...).
    pub terms: Vec<String>,
}

/// Storage backend seam for blocks, mirroring the ChunkStore shape (see
/// store/mod.rs). The engine owns encoding/decoding and the in-memory
/// block index; the store owns bytes-at-rest AND the term posting lists
/// — term storage lives store-side so a SQL backend can answer
/// query_terms() with an INTERSECT instead of shipping posting lists
/// into Rust.
///
/// Transaction contract (identical to ChunkStore): methods must NOT
/// open transactions. In the extension they run re-entrantly inside
/// vtab callbacks and ride the host's enclosing transaction — that IS
/// the atomicity (and the reason replace/delete can drop term rows "in
/// the same operation" without any manifest machinery).
pub trait BlockStore: Send + Sync {
    /// Persist one block and its terms. Same-operation term insert:
    /// a block is never visible without its posting-list rows.
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String>;

    /// Persist a BATCH of blocks in one store call. Added for the
    /// level-partitioned flush (see BlockEngine::flush): a flush now
    /// emits up to four blocks (one per level present), and calling
    /// put_block four times from the engine would mean four lock/
    /// connection round-trips in the SQLite backend. The default just
    /// loops put_block — correct for any store; ShadowBlockStore
    /// overrides it to reuse one connection + prepared statement for
    /// the whole batch. Locs come back in input order.
    fn put_blocks(&self, blocks: &[EncodedBlock]) -> Result<Vec<BlockLoc>, String> {
        blocks.iter().map(|b| self.put_block(b)).collect()
    }

    /// Atomic swap for compaction: persist `add`, remove `remove` (and
    /// the removed blocks' term rows — posting lists never dangle, the
    /// PLAN.md pruning rule). `on_committed` fires after the adds are
    /// readable and before the removes, so the engine can swap its
    /// index with no window where a query could hit a removed block.
    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String>;

    /// Read one block's stored payload bytes.
    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String>;

    /// Remove blocks AND their term rows in the same operation.
    /// Per-block error strings; a missing block is reported, not fatal.
    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String>;

    /// Recovery: enumerate every persisted block's metadata (never the
    /// payloads) so the engine can rebuild its index on connect.
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String>;

    /// Posting-list intersection + time-range overlap: return every
    /// block that carries ALL of `terms` and overlaps [ts_min, ts_max].
    /// Empty `terms` = every block in range (a pure time scan).
    ///
    /// Returns (loc, meta) pairs — the store already has the metadata
    /// columns in hand when it answers this (both backends read the
    /// blocks row/entry to do the ts-overlap test), so shipping the
    /// meta costs nothing and saves the caller a second lookup. The
    /// engine uses this at recovery to classify blocks by their
    /// `level:` terms without re-reading anything it already indexed
    /// (Session 5 friction fix).
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String>;

    /// Small key/value config persistence (index_keys, schema version).
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String>;
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
}
