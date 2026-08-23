//! demogen-core: deterministic synthetic telemetry for timeless demos.
//!
//! Three layers, all free of I/O and dependencies:
//! - `fleet`: the synthetic world (services × pods, metric catalog,
//!   correlated logs and traces, one baked-in incident).
//! - `blobs`: Tier 2 batch-blob encoders for all three signals.
//! - `drive`: streaming drivers that turn a fleet into a sequence of
//!   ready-to-insert blobs through a caller-supplied sink.

pub mod blobs;
pub mod drive;
pub mod fleet;
