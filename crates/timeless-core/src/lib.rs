//! timeless-core: the metrics storage engine, extracted from timeless_metrics'
//! tms_engine NIF crate (the rustler layer stayed behind; this is the pure
//! engine: series registry, partition buffers, pco chunk codec, persistence,
//! queries, and the Prometheus text parser).
//!
//! Origin: tms_engine/src/lib.rs lines 1-2443 (extracted 2026-07-22).
//! The Elixir repo's crate is intentionally untouched; rewiring it to depend
//! on this crate is a later, post-publication step.

pub mod blocks;
mod engine;
pub mod spans;
pub mod store;

pub use blocks::{
    level_from_name, level_name, BlockEngine, BlockEngineConfig, BlockLoc, BlockMeta,
    BlockStore, EncodedBlock, LogEntry, LogQuery, MemBlockStore,
};
pub use spans::{
    kind_from_name, kind_name, status_from_name, status_name, EncodedSpanBlock, MemSpanStore,
    SpanBlockEngine, SpanBlockStore, SpanEngineConfig, SpanEntry, SpanQuery,
};
pub use engine::*;
pub use store::{ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, FsStore, StoredChunk};

pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");
