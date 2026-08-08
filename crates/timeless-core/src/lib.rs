//! timeless-core: the metrics storage engine, extracted from timeless_metrics'
//! tms_engine NIF crate (the rustler layer stayed behind; this is the pure
//! engine: series registry, partition buffers, pco chunk codec, persistence,
//! queries, and the Prometheus text parser).
//!
//! Origin: tms_engine/src/lib.rs lines 1-2443 (extracted 2026-07-22).
//! The Elixir repo's crate is intentionally untouched; rewiring it to depend
//! on this crate is a later, post-publication step.

// These public tuple-shaped query contracts and explicit kernel parameters are
// established storage-waist APIs. Changing them just to shorten their Rust
// type spelling would be a compatibility change, not a lint cleanup.
#![allow(clippy::type_complexity, clippy::too_many_arguments)]

pub mod blocks;
mod engine;
pub mod rollup;
pub mod spans;
pub mod store;
pub mod waist;

pub use blocks::{
    canonical_severity, level_from_name, level_name, BlockEngine, BlockEngineConfig, BlockLoc,
    BlockMeta, BlockStore, EncodedBlock, LogEntry, LogQuery, LogQueryExecutionReport,
    LogQueryOrder, MemBlockStore,
};
pub use engine::*;
pub use rollup::{
    decode_rollup_payload, encode_rollup_payload, parse_ladder, rollup_buckets, RollupBucket,
    RollupTier, ENC_ROLLUP_V1,
};
pub use spans::{
    kind_from_name, kind_name, status_from_name, status_name, EncodedSpanBlock, MemSpanStore,
    SpanBlockEngine, SpanBlockStore, SpanDurationBounds, SpanEngineConfig, SpanEntry, SpanQuery,
    SpanQueryOrder, SpanQueryStream, TraceBucketStat,
};
pub use store::{
    ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, EncodedRollupChunk, FsStore,
    ResolvedSeries, StoredChunk, StoredRollupChunk, StoredSeries,
};

pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");
