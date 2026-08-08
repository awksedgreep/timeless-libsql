# Trace query projection and late materialization

Date: 2026-08-07

Session: 2 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)

Measured source: `9421cfbf361da976eae4935df25cbfb9bb08e47e`

Evidence: [`2026-08-07_trace_query_projection.json`](evidence/2026-08-07_trace_query_projection.json)

Evidence SHA-256: `d2e7089022166acf6d6814c26d3e6f0b0ee5dce322770cb0c62f192792af624b`

Baseline: [`2026-08-07_trace_query_baseline.md`](2026-08-07_trace_query_baseline.md)

## Change

SQLite's `colUsed` mask now crosses the `timeless_traces` vtable boundary as
the public trace-column mask. Generation-2 adaptive columnar blocks decode
pushed predicate columns first. Only matching rows then materialize the other
columns SQLite requested. `timeless_trace_buckets` requests only service,
status, start timestamp, and duration.

No on-disk codec, public table schema, batch, index, transaction, retention,
compression, optimize, or migration format changed. Generation-1, raw, and
zstd blocks remain exact through the conservative full-decoder fallback.
`timeless_capabilities()` advertises `signals.traces.projection_decode.version`
`1` and the fallback explicitly.

Four additive cumulative `timeless_stats('traces')` rows make the work visible
to direct SQLite/libSQL users and the Rust trace API:

- `query_decoded_columns`: physical column-decoder invocations;
- `query_decoded_column_bytes`: stored bytes supplied to those decoders;
- `query_materialized_values`: values owned by predicate/result vectors;
- `query_materialized_rich_values`: the attributes, status-description,
  events, resource, and instrumentation-scope subset.

## Exact release comparison

Both runs use the same deterministic 131,072-rich-span, 48-block fixture, 20
measured iterations after three warmups, isolated release trace-server
processes per Jaeger shape, and an isolated direct-SQL child. Percent changes
compare Session 2 with the refined Session 1 run.

| shape | Session 1 p50/p95/p99 | Session 2 p50/p95/p99 | p95 verdict |
|---|---:|---:|---:|
| Jaeger exact trace | 6.423 / 7.185 / 7.229 ms | 2.295 / 2.554 / 2.963 ms | 64.4% faster |
| Jaeger broad full-decode miss | 92.440 / 97.500 / 104.473 ms | 7.328 / 8.530 / 8.616 ms | 91.3% faster |
| Jaeger broad 4,096-span result | 147.772 / 158.529 / 160.117 ms | 153.253 / 164.270 / 168.749 ms | 3.6% slower; within the ±15% noise boundary |
| direct buckets, all 16 time boxes | 60.300 / 69.242 / 69.263 ms | 19.777 / 22.509 / 23.020 ms | 67.5% faster |
| direct buckets, one time box | 3.624 / 3.979 / 3.984 ms | 1.443 / 1.506 / 1.559 ms | 62.2% faster |

The exact trace still reads three blocks and examines 8,192 spans per request,
but it decodes the trace-id column for all rows and materializes the other
thirteen columns for only the eight matches. The broad miss still reads all 48
blocks and examines 131,072 spans, but decodes only service and duration and
materializes no rich value. Each bucket query decodes exactly four physical
columns per block and no rich value.

The 4,096-span result legitimately receives no storage projection win: all
8,192 spans in its three read blocks survive the pushed predicate before the
4,096-row ordered bound. Its 3,874,979-byte Jaeger response remains dominated
by response construction and serialization above storage.

## Memory, write, and storage

| isolated process | Session 1 HWM | Session 2 HWM | change |
|---|---:|---:|---:|
| exact trace server | 35,628 KiB | 29,788 KiB | 16.4% lower |
| broad miss server | 34,704 KiB | 29,056 KiB | 16.3% lower |
| broad 4,096-span server | 158,008 KiB | 157,720 KiB | effectively unchanged |
| direct-SQL buckets | 17,408 KiB | 15,292 KiB | 12.2% lower |

Public batch insertion completed at 241,428 durable spans/s versus 239,087 in
Session 1. Optimize took 356.239 ms versus 361.319 ms. Both are noise-level
improvements, not write-path claims. Logical payload remains 666,844 bytes,
the checkpointed database remains 68,165,632 bytes, and WAL/SHM are zero after
close. The physical fixture, block count, duration coverage, terms, trace index,
timestamp range, response bytes, cardinality, and rich-span probes are exact.

## Regression and validation boundary

- `projected_columnar_decode_filters_before_rich_materialization` pins
  predicate-first work, selective rich values, misses, and zero-column counts;
- the selected-string codec test covers dictionary and concatenated encodings,
  complete validation, sparse materialization, and invalid selections;
- the Rust `rich-traces` real-extension gate covers every individual public
  column, mixed projections, predicates on unselected columns, `count(*)`,
  full rich rows before optimize, after optimize, after reopen, transaction
  rollback accounting, corruption, and rich-span fidelity;
- the Rust `trace-reads` gate retains discovery, duration-upgrade/corruption,
  bounded-read, and stable-snapshot behavior;
- the complete trace API suite retains OTLP JSON/protobuf/gzip, Jaeger/native
  responses, limits, cancellation, publication fairness, durability, shutdown,
  extension-owned 8,192-span batching, and cold reopen.

## Verdict

Session 2 passes. Projection-aware, predicate-first decoding materially
improves exact, miss, and scalar bucket reads for direct SQLite/libSQL and Rust
API users without revising storage. The large-response result is retained as
an honest neutral/slightly-negative control and remains a separate API-memory
target. Session 3 may now test exact percentile selection against the faster
four-column bucket path.
