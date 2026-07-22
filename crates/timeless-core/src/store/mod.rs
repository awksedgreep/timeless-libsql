//! Storage-backend seam: chunk persistence behind the `ChunkStore` trait,
//! so chunks can live in filesystem files (`FsStore`) today and SQLite
//! shadow tables (rowid-addressed) later. The engine owns encoding,
//! decoding, and the in-memory index; the store owns bytes-at-rest.

pub mod fs;

pub use fs::FsStore;

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

/// Payload encoding for a chunk: pco-compressed (the durable format) or
/// raw big-endian arrays (transient, written by deferred-compression
/// flushes and consumed by compaction).
pub const ENC_PCO: u8 = 0;
pub const ENC_RAW: u8 = 1;

/// Where a persisted chunk lives. Backend-specific.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ChunkLoc {
    File {
        path: PathBuf,
        offset: u64,
        len: u32,
    },
    /// For future SQLite shadow-table backend (rowid). Unused by FsStore.
    Row { rowid: i64 },
}

impl ChunkLoc {
    /// Identity of the underlying storage unit. Chunks packed into one
    /// batch file share a unit; a unit is deletable only when no live
    /// chunk references it (the engine refcounts units, the store never
    /// sees the index).
    pub fn unit(&self) -> ChunkLoc {
        match self {
            ChunkLoc::File { path, .. } => ChunkLoc::File {
                path: path.clone(),
                offset: 0,
                len: 0,
            },
            ChunkLoc::Row { rowid } => ChunkLoc::Row { rowid: *rowid },
        }
    }
}

/// Everything the engine's index needs to know about one persisted chunk.
#[derive(Clone)]
pub struct ChunkMeta {
    pub min_ts: i64,
    pub max_ts: i64,
    pub point_count: u32,
    pub min_val: f64,
    pub max_val: f64,
    pub sum_val: f64,
    pub loc: ChunkLoc,
    pub encoding: u8,
}

/// A fully-encoded chunk ready to persist (what the engine's flush path
/// produces), plus its series identity.
pub struct EncodedChunk {
    pub series_id: i64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub point_count: u32,
    pub min_val: f64,
    pub max_val: f64,
    pub sum_val: f64,
    /// ENC_PCO or ENC_RAW — what ts_bytes/val_bytes contain.
    pub encoding: u8,
    pub ts_bytes: Vec<u8>,
    pub val_bytes: Vec<u8>,
}

impl EncodedChunk {
    /// Index metadata for this chunk once the store has placed it at `loc`.
    pub fn meta(&self, loc: ChunkLoc) -> ChunkMeta {
        ChunkMeta {
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            point_count: self.point_count,
            min_val: self.min_val,
            max_val: self.max_val,
            sum_val: self.sum_val,
            loc,
            encoding: self.encoding,
        }
    }
}

/// Metadata returned by scan() for one persisted chunk (everything the
/// engine's index needs, with a ChunkLoc instead of path/offset/len).
pub struct StoredChunk {
    pub series_id: i64,
    pub meta: ChunkMeta,
}

/// One chunk's stored payload: ts/val byte ranges into a shared buffer.
/// Fs chunks are slices of a cached whole file; a backend holding ts and
/// val separately can concatenate them into one buffer.
#[derive(Clone)]
pub struct ChunkBytes {
    pub data: Arc<Vec<u8>>,
    pub ts_range: Range<usize>,
    pub val_range: Range<usize>,
}

impl ChunkBytes {
    pub fn ts(&self) -> &[u8] {
        &self.data[self.ts_range.clone()]
    }

    pub fn val(&self) -> &[u8] {
        &self.data[self.val_range.clone()]
    }
}

pub trait ChunkStore: Send + Sync {
    /// Persist a batch of chunks (one flush cycle). The backend may pack
    /// them into one file. Returns one ChunkLoc per chunk, same order.
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String>;

    /// Atomic swap for compaction: persist `add`, remove `remove` (unit
    /// locs, see ChunkLoc::unit), such that a crash never loses both.
    /// `on_committed` fires once the new chunks are durable and readable
    /// but before the old ones are removed — the engine swaps its index
    /// there, so queries never see a removed unit. The fs backend keeps
    /// the pre-seam pending/manifest/rename crash-recovery machinery.
    fn replace_chunks(
        &self,
        add: &[EncodedChunk],
        remove: &[ChunkLoc],
        on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String>;

    /// Read one chunk's stored ts/val bytes.
    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String>;

    /// Remove storage units (unit locs). Returns per-unit error strings;
    /// a missing unit is reported, not fatal.
    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String>;

    /// Recovery: enumerate all persisted chunks with their metadata.
    fn scan(&self) -> Result<Vec<StoredChunk>, String>;

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String>;
    fn load_registry(&self) -> Result<Option<Vec<u8>>, String>;

    /// For Engine::info(): (total_bytes, file_or_row_count).
    fn storage_stats(&self) -> (u64, usize);

    /// Backend-internal cache maintenance (fs: TTL file cache sweep).
    /// No-op ok.
    fn sweep_cache(&self);
}
