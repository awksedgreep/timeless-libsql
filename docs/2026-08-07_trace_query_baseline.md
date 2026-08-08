# Trace query baseline

Date: 2026-08-07

Session: 1 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)

Measured source: `ef7c8577d491acd5b48daf9f6638851596cdcab2`

Evidence: [`2026-08-07_trace_query_baseline.json`](evidence/2026-08-07_trace_query_baseline.json)

Evidence SHA-256: `f290ca4355228ca5c1fd2b33fdf679505ec04f81665d3a768d2c7499d3e8da17`

## Workload

The Rust-only harness builds release artifacts from a clean commit and rejects
an extension or trace-server build whose embedded commit differs. It inserts
131,072 deterministic rich spans as 16 public 8,192-span batch blobs. Each
batch occupies a separate one-hour time box; all three status partitions and
both 100/900 microsecond durations occur in every box. Attributes, status
descriptions, events, resource fields, and instrumentation scope fields are
non-empty and checked after optimize, checkpoint, and reopen.

All latency rows are 20 measured requests after three warmups, single-client on
loopback, using release builds. The direct-SQL bucket phase runs in a fresh
child process so fixture construction and HTTP response allocation do not
inflate its HWM.

## Results

| shape | p50 | p95 | p99 | result | work per request |
|---|---:|---:|---:|---:|---|
| Jaeger exact trace | 6.197 ms | 11.460 ms | 12.302 ms | 1 trace / 8 spans / 7,526 B | 3 blocks, 8,192 decoded spans, 41,475 payload B |
| Jaeger broad full-decode miss | 90.235 ms | 97.451 ms | 98.431 ms | 0 spans / 56 B | 48 blocks, 131,072 decoded spans, 666,844 payload B |
| Jaeger broad 4,096-span result | 155.201 ms | 157.523 ms | 159.530 ms | 512 traces / 4,096 spans / 3,874,979 B | 3 payload blocks, 8,192 decoded spans, 41,671 payload B; 45 blocks skipped by bound |
| direct `timeless_trace_buckets`, all 16 boxes | 58.733 ms | 68.475 ms | 68.694 ms | 16 rows / 131,072 spans / 2,961 JSON B | 48 blocks, 131,072 decoded spans, 666,844 payload B |
| direct `timeless_trace_buckets`, one-box control | 3.551 ms | 3.671 ms | 3.680 ms | 1 row / 8,192 spans / 186 JSON B | 3 blocks, 8,192 decoded spans, 41,475 payload B |

The exact and bucket cardinalities, duration minima/maxima, and exact
nearest-rank p50/p95/p99 values are asserted on every iteration. Query stats
show no cancellation, retry, error, buffered-span work, or snapshot payload
copying.

## Write, storage, and memory

- Public batch insertion completed 131,072 durable spans in 584.957 ms
  (224,071 spans/s). The 16 input blobs total 66,817,644 bytes.
- Public optimize completed in 383.131 ms. It converted 48 raw blocks carrying
  64,855,180 logical payload bytes into 48 compressed blocks carrying 666,844
  logical payload bytes. All 48 have duration bounds; none are legacy/unknown.
- Checkpoint/reopen preserved the exact count and timestamp range with zero
  buffered spans and zero WAL/SHM bytes after close.
- Trace-index allocation is 2,523,136 bytes with 49,152 trace-index rows and
  576 term rows.
- The post-optimize database remains 68,165,632 physical bytes because SQLite
  retains 15,852 freelist pages (64,929,792 bytes). Optimize reclaims logical
  payload space for reuse; it does not promise to shrink the database file.
- The isolated direct-SQL bucket process HWM is 17,644 KiB.
- The trace server starts at 28,060 KiB HWM and reaches 157,692 KiB after the
  3.87 MB broad Jaeger response workload; ending RSS is 84,664 KiB. This is a
  whole-process maximum, not the bucket kernel's memory.

## Verdict

Session 1 passes. The measurements separate three costs:

1. Exact trace lookup prunes to the three status blocks in one time box, but
   still decodes 8,192 rich spans to return eight.
2. Broad miss and bucket queries both decode all 131,072 rich rows even though
   the former produces no rows and the latter needs only scalar fields.
3. A large Jaeger result spends most of its time and peak memory constructing
   and serializing the rich API response after storage has already stopped at
   three blocks.

Session 2 should therefore test projection-aware, predicate-first late
materialization against the exact-trace, full-decode-miss, and bucket shapes.
It must not claim the 4,096-span response allocation as a storage defect, and
it must not revise a codec unless the existing columnar representation cannot
provide the measured benefit safely.
