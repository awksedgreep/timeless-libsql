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
    /// Value of the first point at `max_ts` in stable chunk order. New
    /// stores persist it so latest-point queries can avoid decompression;
    /// legacy formats leave it absent and use the decode fallback.
    pub max_ts_val: Option<f64>,
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
    pub max_ts_val: f64,
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
            max_ts_val: Some(self.max_ts_val),
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

/// F3: a rollup chunk ready to persist. Lives in the same chunk table
/// with `resolution` > 0; payload goes in the ts-bytes slot (val slot
/// empty), min/max/sum val columns are unused (0). meta semantics:
/// min_ts = first bucket start, max_ts = last bucket's COVERAGE END
/// (bucket_start + resolution - 1), point_count = bucket count.
pub struct EncodedRollupChunk {
    pub series_id: i64,
    pub resolution: i64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub bucket_count: u32,
    pub payload: Vec<u8>,
}

/// F3: one persisted rollup chunk's identity + metadata, from
/// scan_rollups. meta.loc addresses it for read_chunk/delete_chunks.
pub struct StoredRollupChunk {
    pub series_id: i64,
    pub resolution: i64,
    pub meta: ChunkMeta,
}

/// One durable series identity. Stores with an authoritative catalog use
/// these rows instead of replacing a process-local registry snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredSeries {
    pub id: i64,
    pub name: String,
    pub labels: Vec<(String, String)>,
}

/// Result of resolving a durable series identity.
pub struct ResolvedSeries {
    pub id: i64,
    /// True only when this call inserted the authoritative row. The engine
    /// journals that fact so a host-transaction rollback can invalidate its
    /// matching in-memory cache entry.
    pub created: bool,
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

    /// Read several chunks in input order. Backends may override this to
    /// amortize transport and statement overhead; the default preserves the
    /// original one-at-a-time behavior.
    fn read_chunks(&self, locs: &[ChunkLoc]) -> Result<Vec<ChunkBytes>, String> {
        locs.iter().map(|loc| self.read_chunk(loc)).collect()
    }

    /// Remove storage units (unit locs). Returns per-unit error strings;
    /// a missing unit is reported, not fatal.
    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String>;

    /// Recovery: enumerate all persisted chunks with their metadata.
    fn scan(&self) -> Result<Vec<StoredChunk>, String>;

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String>;
    fn load_registry(&self) -> Result<Option<Vec<u8>>, String>;

    /// Whether series identity is owned by normalized store rows rather than
    /// the legacy whole-registry blob. Filesystem stores retain the blob
    /// implementation; SQLite stores override this catalog API.
    fn has_authoritative_series(&self) -> bool {
        false
    }

    fn load_series(&self) -> Result<Vec<StoredSeries>, String> {
        Err("store does not provide an authoritative series catalog".to_string())
    }

    fn resolve_series(
        &self,
        _name: &str,
        _labels: &[(String, String)],
    ) -> Result<ResolvedSeries, String> {
        Err("store does not provide authoritative series resolution".to_string())
    }

    /// Resolve many (name, labels) pairs at once. Same contract per entry
    /// as resolve_series (allocate-or-return the authoritative id inside
    /// the caller's transaction, `created` true only if THIS call inserted
    /// the row); results are positional. The default loops; SQL-backed
    /// stores override with multi-row statements so first-touch of a large
    /// series population is not one round-trip per series.
    fn resolve_series_bulk(
        &self,
        entries: &[(&str, Vec<(String, String)>)],
    ) -> Result<Vec<ResolvedSeries>, String> {
        entries
            .iter()
            .map(|(name, labels)| self.resolve_series(name, labels))
            .collect()
    }

    /// Import legacy registry rows. Implementations must be idempotent so
    /// two processes opening the same legacy database cannot corrupt it.
    fn migrate_series(&self, _series: &[StoredSeries]) -> Result<(), String> {
        Err("store does not support series migration".to_string())
    }

    /// F3: enumerate persisted ROLLUP chunk metadata (resolution > 0)
    /// for the engine's rollup index. Default: none (FsStore has no
    /// rollup support — rollups are a shadow-store feature; an engine
    /// over a store without them simply never sees a ladder).
    fn scan_rollups(&self) -> Result<Vec<StoredRollupChunk>, String> {
        Ok(Vec::new())
    }

    /// F3: persist encoded rollup chunks. Same transactional contract as
    /// put_chunks — rows ride the caller's transaction. Deletion reuses
    /// delete_chunks (rollup rows live in the same chunk table and are
    /// addressed by the same locs).
    fn put_rollup_chunks(&self, _chunks: &[EncodedRollupChunk]) -> Result<Vec<ChunkLoc>, String> {
        Err("store does not support rollup chunks".to_string())
    }

    /// Cheap change-detection token for the authoritative catalog + chunk
    /// state, so `Engine::refresh_authoritative_state` can skip its full
    /// reload when nothing has been committed since the last refresh.
    ///
    /// Contract: any committed change that would alter the result of
    /// `load_series()` or `scan()` MUST change the returned value, and the
    /// token must ride the same transaction as the change it describes.
    /// `None` means the store cannot answer — callers must do the full
    /// reload (the always-correct fallback).
    ///
    /// The shadow-store implementation returns
    /// `(max _series id, chunk generation counter)`: the series half is
    /// sound because committed catalog rows are append-only (rollback undo
    /// is page-level and removes only never-committed rows) — if a series
    /// GC/delete path is ever added, that half MUST move to a bumped
    /// counter too.
    fn catalog_generation(&self) -> Result<Option<(i64, i64)>, String> {
        Ok(None)
    }

    /// For Engine::info(): (total_bytes, file_or_row_count).
    fn storage_stats(&self) -> (u64, usize);

    /// Backend-internal cache maintenance (fs: TTL file cache sweep).
    /// No-op ok.
    fn sweep_cache(&self);
}
