# Packed rollup read result — 2026-07-31

Session 4 replaces six independent `timeless_rollup` scans with the public
`timeless_rollup_batches` TVF. The stored `ENC_ROLLUP_V1` payload and shadow
rows are unchanged. The query-only `TRB1` envelope carries bucket timestamp,
exact count, avg, sum, min, max, last timestamp, and last value as eight
little-endian columns.

## Direct extension workload

Command:

```console
cargo run --release --manifest-path tools/bench/Cargo.toml --bin query-read -- \
  target/release/libtimeless_ext.so --runs 10
```

Dataset: 12,000 series × 60 raw points; the 10-second tier contains 60,000
settled buckets. Both compared paths materialize all six public aggregates;
the packed consumer also validates and walks every 64-bit field.

| Path | Median | p95 | Buckets |
|---|---:|---:|---:|
| Six `timeless_rollup` row queries | 1,002.150 ms | 1,027.747 ms | 60,000 |
| One `timeless_rollup_batches` query | 107.741 ms | 114.492 ms | 60,000 |

Median latency is **9.30× lower**, clearing the 3× session gate. The packed
path also preserves `u64` count exactly, unlike the compatibility row TVF's
SQLite REAL representation.

## Correctness and compatibility

- CLI section 25 independently decodes `TRB1` and compares all six aggregates
  bit-for-bit with the row TVF, including reopen recovery.
- Extension unit coverage pins the version, column order, float bits, and a
  count above `2^53`.
- The core batch API retains input series order and performs one ordered store
  batch read under one transition guard.
- No on-disk, shadow-table, transaction, or replication-visible format changed.

The paired host-level result is in the TimelessMetrics benchmark record.
