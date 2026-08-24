//! demogen-core: deterministic synthetic telemetry for timeless demos.
//!
//! Three layers, all free of I/O and dependencies:
//! - `fleet`: the synthetic world (services × pods, metric catalog,
//!   correlated logs and traces, one baked-in incident).
//! - `blobs`: Tier 2 batch-blob encoders for all three signals.
//! - `drive`: streaming drivers that turn a fleet into a sequence of
//!   ready-to-insert blobs through a caller-supplied sink.
//! - `tables`: which signal tables a demo database has and what they are
//!   called, shared by the extension and the CLI so both resolve targets
//!   the same way.

pub mod blobs;
pub mod drive;
pub mod fleet;
pub mod tables;
