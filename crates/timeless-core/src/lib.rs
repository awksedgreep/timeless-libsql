//! timeless-core: the metrics storage engine, extracted from timeless_metrics'
//! tms_engine NIF crate (the rustler layer stayed behind; this is the pure
//! engine: series registry, partition buffers, pco chunk codec, persistence,
//! queries, and the Prometheus text parser).
//!
//! Origin: tms_engine/src/lib.rs lines 1-2443 (extracted 2026-07-22).
//! The Elixir repo's crate is intentionally untouched; rewiring it to depend
//! on this crate is a later, post-publication step.

mod engine;
pub mod store;

pub use engine::*;
pub use store::{ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, FsStore, StoredChunk};

pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");
