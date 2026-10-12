//! Storage-backend seam: chunk persistence behind the `ChunkStore` trait,
//! so chunks can live in filesystem files (`FsStore`) today and SQLite
//! shadow tables (rowid-addressed) later. The engine owns encoding,
//! decoding, and the in-memory index; the store owns bytes-at-rest.
//!
//! BYTE ORDER is split by layer, deliberately: metrics chunks, the
//! `FsStore` file framing, and the series registry are big-endian;
//! logs/span containers, `timeless-codec` columns, and rollup payloads
//! are little-endian. Every decoder matches its own encoder, so this
//! is not a live bug — but it is a trap for cross-store migration or
//! byte-level reuse (e.g. `ChunkMeta` floats): always convert at the
//! seam, never reinterpret foreign bytes.

pub mod mem;

pub use mem::{MemFault, MemStore};

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

/// Payload encoding for a chunk: pco-compressed (the durable format) or
/// raw big-endian arrays (transient, written by deferred-compression
/// flushes and consumed by compaction).
pub const ENC_PCO: u8 = 0;
pub const ENC_RAW: u8 = 1;

/// The series a read of an index covers.
#[derive(Clone, Copy, Debug)]
pub enum SeriesSpan<'a> {
    /// Every series with an id in this range, both ends included.
    Between(i64, i64),
    /// These series: ascending, each once.
    Of(&'a [i64]),
}

/// What makes a stored chunk the one a plan was made with: where it is,
/// whose it is, where it begins, how many points (or buckets) it holds and
/// which tier it is of (0 for raw).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChunkIdentity {
    pub loc: ChunkLoc,
    pub series_id: i64,
    pub resolution: i64,
    pub min_ts: i64,
    pub point_count: u32,
}

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
    /// Encoded timestamp + value payload bytes, excluding backend framing.
    /// Kept in the index so maintenance can enforce byte budgets without
    /// reading candidate blobs just to size a plan.
    pub payload_bytes: u64,
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
    pub fn payload_bytes(&self) -> u64 {
        (self.ts_bytes.len() as u64).saturating_add(self.val_bytes.len() as u64)
    }

    /// Index metadata for this chunk once the store has placed it at `loc`.
    pub fn meta(&self, loc: ChunkLoc) -> ChunkMeta {
        ChunkMeta {
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_ts_val: Some(self.max_ts_val),
            point_count: self.point_count,
            payload_bytes: self.payload_bytes(),
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

    /// Whether every chunk is its own storage unit however it was put, so
    /// that putting chunks together shares nothing among them. A flush
    /// then persists every partition in one batched call instead of one
    /// call per partition. False keeps a backend that packs a batch into
    /// one unit (FsStore files) on its individual-or-packed split.
    fn chunks_are_rows(&self) -> bool {
        false
    }

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

    /// The registry blob the first versions kept the whole series catalog
    /// in, if this store still holds one. Read once, to bring its series
    /// into the catalog rows (`migrate_series`).
    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
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

    /// Remove authoritative series rows: the engine calls this for series
    /// that retention has left no chunk of, inside the caller's
    /// transaction, so a rollback puts the rows back. A store that owns
    /// series identity must implement it; the catalog token and the
    /// append watermark must both change, since rows are gone and a
    /// series id may be given out again.
    fn delete_series(&self, _ids: &[i64]) -> Result<(), String> {
        Err("this store cannot remove series".to_string())
    }

    // ── On-disk label index (#132) ───────────────────────────────────
    //
    // A store that owns series identity may also keep a label index beside
    // it, so the engine can answer selectors and discovery without holding
    // every series' labels in memory. The defaults say "no index"; an
    // engine that needs one over such a store refuses rather than guess.

    /// The table's `index_cache` budget in bytes for remembered series
    /// identities and selections (`0` remembers nothing); `None` is no
    /// limit.
    fn series_index_budget(&self) -> Result<Option<u64>, String> {
        Ok(None)
    }

    /// Every series id in the catalog, ascending.
    fn series_ids(&self) -> Result<Vec<i64>, String> {
        Err("store does not keep a label index".to_string())
    }

    /// How many series the catalog holds.
    fn series_total(&self) -> Result<usize, String> {
        Err("store does not keep a label index".to_string())
    }

    /// Create the label index if it is missing and bring it up to date with
    /// the series catalog, in the caller's transaction.
    fn ensure_label_index(&self) -> Result<(), String> {
        Err("store does not keep a label index".to_string())
    }

    /// Whether the label index reflects the series catalog as it now is.
    fn label_index_current(&self) -> Result<bool, String> {
        Ok(false)
    }

    /// Ids, ascending, of the series of `metric` (when given) that carry
    /// every `(key, value)` in `eq`. At least one of the two is given.
    fn find_series_ids(
        &self,
        _metric: Option<&str>,
        _eq: &[(String, String)],
    ) -> Result<Vec<i64>, String> {
        Err("store does not keep a label index".to_string())
    }

    /// As [`Self::find_series_ids`], further requiring, for each entry of
    /// `any_of`, that the series carry that key with one of its values.
    fn find_series_ids_any(
        &self,
        _metric: Option<&str>,
        _eq: &[(String, String)],
        _any_of: &[(String, Vec<String>)],
    ) -> Result<Vec<i64>, String> {
        Err("store does not keep a label index".to_string())
    }

    /// The catalog rows of `ids`, in id order; absent ids are skipped.
    fn series_by_ids(&self, _ids: &[i64]) -> Result<Vec<StoredSeries>, String> {
        Err("store does not keep a label index".to_string())
    }

    /// Distinct metric names, sorted.
    fn metric_names(&self) -> Result<Vec<String>, String> {
        Err("store does not keep a label index".to_string())
    }

    /// Values `key` takes, across every series or within `metric`, sorted.
    fn label_values(&self, _metric: Option<&str>, _key: &str) -> Result<Vec<String>, String> {
        Err("store does not keep a label index".to_string())
    }

    /// Label names, across every series or within `metric`, sorted; both
    /// include `__name__`.
    fn label_names(&self, _metric: Option<&str>) -> Result<Vec<String>, String> {
        Err("store does not keep a label index".to_string())
    }

    // ── On-disk chunk index (#132 phase 2) ───────────────────────────
    //
    // A store whose chunk rows carry their own metadata can answer the
    // engine's index questions itself, so the engine need not hold every
    // chunk's metadata in memory. Raw chunks only (rollups have their own
    // index). The defaults say the store cannot.

    /// Whether this store can answer the chunk index queries below.
    fn has_chunk_index(&self) -> bool {
        false
    }

    /// Bring whatever on-store index those queries need in line with how
    /// the engine is about to read its chunk index: present when `wanted`
    /// (the chunk index is read from the store), absent otherwise, so that
    /// a table whose chunk index is held in memory does not pay to maintain
    /// one nobody reads. Called once when an engine is opened; an error
    /// leaves things as they are, and `has_chunk_index` then says what the
    /// store can answer. That covers a connection that cannot write, and
    /// an engine first opened from inside a running statement (a table-
    /// valued function), where the schema cannot be changed: the next open
    /// that can settles it.
    fn ensure_chunk_index(&self, _wanted: bool) -> Result<(), String> {
        Ok(())
    }

    /// Metadata of the raw chunks of `series_ids` overlapping
    /// `[t_start, t_end]`, ordered by series id, then `min_ts`, then
    /// insertion.
    fn chunk_metas_for(
        &self,
        _series_ids: &[i64],
        _t_start: i64,
        _t_end: i64,
    ) -> Result<Vec<StoredChunk>, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// Per-series raw-chunk totals `(series_id, min_ts, max_ts, points,
    /// chunks)`, for `series_ids` or, with `None`, every series that has a
    /// raw chunk.
    #[allow(clippy::type_complexity)]
    fn chunk_stats_for(
        &self,
        _series_ids: Option<&[i64]>,
    ) -> Result<Vec<(i64, i64, i64, u64, usize)>, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// The raw chunks [`Self::chunk_metas_for`] finds, each with its
    /// payload, from one read. `None` when the store has no way to do that
    /// cheaper than reading the payloads afterwards, or when the chunks
    /// hold more than `max_points` points: a caller with a work limit must
    /// be able to refuse a read without its payloads having been loaded.
    fn chunk_rows_for(
        &self,
        _series_ids: &[i64],
        _t_start: i64,
        _t_end: i64,
        _max_points: Option<u64>,
    ) -> Result<Option<Vec<(StoredChunk, ChunkBytes)>>, String> {
        Ok(None)
    }

    /// Raw chunks in the store: count, points, oldest and newest timestamp.
    fn chunk_totals(&self) -> Result<(usize, u64, Option<i64>, Option<i64>), String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// Ids of the series that have a raw chunk, ascending.
    fn chunk_series_ids(&self) -> Result<Vec<i64>, String> {
        self.chunk_series_ids_between(i64::MIN, i64::MAX)
    }

    /// [`Self::chunk_series_ids`] for series ids in `[from, upto]`.
    fn chunk_series_ids_between(&self, _from: i64, _upto: i64) -> Result<Vec<i64>, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// The smallest series id at or above `from` that has a raw chunk:
    /// where a walk of the raw index in stretches of series goes next.
    fn next_chunk_series(&self, _from: i64) -> Result<Option<i64>, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// Whether every series that has a raw chunk is in the series catalog.
    /// A store that can answer without listing either side should.
    fn chunk_series_cataloged(&self) -> Result<bool, String> {
        let known: std::collections::HashSet<i64> = self.series_ids()?.into_iter().collect();
        Ok(self.chunk_series_ids()?.iter().all(|id| known.contains(id)))
    }

    /// Raw chunks wholly before `before` (`max_ts < before`) or wholly after
    /// `after` (`min_ts > after`); exactly one bound is given.
    fn chunk_locs_outside(
        &self,
        _before: Option<i64>,
        _after: Option<i64>,
    ) -> Result<Vec<ChunkLoc>, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    // ── Rollup index queries (#144) ─────────────────────────────────
    //
    // A store that answers chunk index queries answers these too, from an
    // index over its rollup rows, so that the engine holds no entry per
    // rollup chunk. Rollup rows always carry `ENC_ROLLUP_V1`.

    /// The newest coverage end among each series' rollup chunks at
    /// `resolution`, for the series that have any: `(series_id, max_ts)`.
    fn rollup_watermarks(
        &self,
        _resolution: i64,
        _series_ids: &[i64],
    ) -> Result<Vec<(i64, i64)>, String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// Rollup chunks of `series_ids` at `resolution` whose coverage
    /// overlaps `[start, stop]`, ordered by series id, then `min_ts`, then
    /// insertion. `payload_bytes` is not read and is zero.
    fn rollup_metas_for(
        &self,
        _resolution: i64,
        _series_ids: &[i64],
        _start: i64,
        _stop: i64,
    ) -> Result<Vec<StoredRollupChunk>, String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// The rollup chunks of the series with ids in `[from, upto]`, a
    /// `(series, resolution)` group at a time and in `min_ts` order within
    /// it. `payload_bytes` is not read and is zero.
    fn visit_rollup_groups(
        &self,
        _from: i64,
        _upto: i64,
        _visitor: &mut dyn FnMut(StoredRollupChunk) -> Result<(), String>,
    ) -> Result<(), String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// The smallest series id at or above `from` that has a rollup chunk.
    fn next_rollup_series(&self, _from: i64) -> Result<Option<i64>, String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// At most `limit` rollup chunks to delete, each with its bucket count:
    /// of `resolution` if one is given, and with coverage ending before
    /// `before` or beginning after `after` if one of those is.
    fn rollup_victims(
        &self,
        _resolution: Option<i64>,
        _before: Option<i64>,
        _after: Option<i64>,
        _limit: usize,
    ) -> Result<Vec<(ChunkLoc, u32)>, String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// Ids of the series that have a rollup chunk, ascending.
    fn rollup_series_ids(&self) -> Result<Vec<i64>, String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// The resolutions rollup chunks are stored at, ascending.
    fn rollup_resolutions(&self) -> Result<Vec<i64>, String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// How many rollup chunks there are, and how many hold one bucket.
    fn rollup_totals(&self) -> Result<(usize, usize), String> {
        Err("store does not answer rollup index queries".to_string())
    }

    /// Of `series`, each given with the timestamp its rollups already
    /// cover to (or none), those with a raw chunk holding anything after
    /// that and at or before `upto`: the series a rollup pass has work
    /// for, found without reading a chunk.
    fn series_with_raw_between(
        &self,
        _series: &[(i64, Option<i64>)],
        _upto: i64,
    ) -> Result<Vec<i64>, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// Raw chunks of the series in `span` that a compaction sweep may act
    /// on: newest sample before `cutoff_ts`, and either still encoded as
    /// `raw_encoding` or holding anything but exactly `target_points`
    /// (smaller ones may merge, larger ones are cut down). A plan is made
    /// of these, so the value statistics are not read and are zero, and
    /// `payload_bytes` may be an estimate. Ordered by series id, then
    /// `min_ts`, then insertion.
    fn compaction_candidates(
        &self,
        _cutoff_ts: i64,
        _raw_encoding: u8,
        _target_points: u32,
        _span: SeriesSpan<'_>,
    ) -> Result<Vec<StoredChunk>, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// Whether every one of `chunks` is still the stored chunk it was when
    /// it was planned with. The row id alone does not say so: SQLite gives a
    /// freed row id to the next row inserted, so after a rollback or a
    /// delete the same id can belong to another series' chunk.
    fn chunks_present(&self, _chunks: &[ChunkIdentity]) -> Result<bool, String> {
        Err("store does not answer chunk index queries".to_string())
    }

    /// Import legacy registry rows. Implementations must be idempotent so
    /// two processes opening the same legacy database cannot corrupt it.
    fn migrate_series(&self, _series: &[StoredSeries]) -> Result<(), String> {
        Err("store does not support series migration".to_string())
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
    /// `(max _series id, chunk generation counter)`: the series half
    /// covers inserts, which only ever raise the maximum, and
    /// `delete_series` bumps the chunk half, so a removal changes the
    /// token too.
    fn catalog_generation(&self) -> Result<Option<(i64, i64)>, String> {
        Ok(None)
    }

    /// For Engine::info(): (total_bytes, file_or_row_count).
    fn storage_stats(&self) -> (u64, usize);

    /// Backend-internal cache maintenance (fs: TTL file cache sweep).
    /// No-op ok.
    fn sweep_cache(&self);
}
