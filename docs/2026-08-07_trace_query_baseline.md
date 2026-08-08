# Trace query baseline

Date: 2026-08-07

Session: 1 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)

Measured source: `ba3df20eef88ba63b57d89e90d3713e723dc6adf`

Evidence: [`2026-08-07_trace_query_baseline.json`](evidence/2026-08-07_trace_query_baseline.json)

Evidence SHA-256: `760715a53db5375d36f5978dbed8bc608030053401cac5d976bc9fce2ca55287`

## Workload

The Rust-only harness builds release artifacts from a clean commit and rejects
an extension or trace-server build whose embedded commit differs. It inserts
131,072 deterministic rich spans as 16 public 8,192-span batch blobs. Each
batch occupies a separate one-hour time box; all three status partitions and
both 100/900 microsecond durations occur in every box. Attributes, status
descriptions, events, resource fields, and instrumentation scope fields are
non-empty and checked after optimize, checkpoint, and reopen.

All latency rows are 20 measured requests after three warmups, single-client on
loopback, using release builds. Each Jaeger shape runs in a fresh trace-server
process, with memory sampled at startup, after warmup, and after measurement.
The direct-SQL bucket phase runs in a separate fresh child process, so neither
fixture construction nor another query shape can inflate a reported HWM.

## Results

| shape | p50 | p95 | p99 | result | work per request |
|---|---:|---:|---:|---:|---|
| Jaeger exact trace | 6.423 ms | 7.185 ms | 7.229 ms | 1 trace / 8 spans / 7,526 B | 3 blocks, 8,192 decoded spans, 41,475 payload B |
| Jaeger broad full-decode miss | 92.440 ms | 97.500 ms | 104.473 ms | 0 spans / 56 B | 48 blocks, 131,072 decoded spans, 666,844 payload B |
| Jaeger broad 4,096-span result | 147.772 ms | 158.529 ms | 160.117 ms | 512 traces / 4,096 spans / 3,874,979 B | 3 payload blocks, 8,192 decoded spans, 41,671 payload B; 45 blocks skipped by bound |
| direct `timeless_trace_buckets`, all 16 boxes | 60.300 ms | 69.242 ms | 69.263 ms | 16 rows / 131,072 spans / 2,961 JSON B | 48 blocks, 131,072 decoded spans, 666,844 payload B |
| direct `timeless_trace_buckets`, one-box control | 3.624 ms | 3.979 ms | 3.984 ms | 1 row / 8,192 spans / 186 JSON B | 3 blocks, 8,192 decoded spans, 41,475 payload B |

The exact and bucket cardinalities, duration minima/maxima, and exact
nearest-rank p50/p95/p99 values are asserted on every iteration. Query stats
show no cancellation, retry, error, buffered-span work, or snapshot payload
copying.

## Write, storage, and memory

- Public batch insertion completed 131,072 durable spans in 548.218 ms
  (239,087 spans/s). The 16 input blobs total 66,817,644 bytes.
- Public optimize completed in 361.319 ms. It converted 48 raw blocks carrying
  64,855,180 logical payload bytes into 48 compressed blocks carrying 666,844
  logical payload bytes. All 48 have duration bounds; none are legacy/unknown.
- Checkpoint/reopen preserved the exact count and timestamp range with zero
  buffered spans and zero WAL/SHM bytes after close.
- Trace-index allocation is 2,523,136 bytes with 49,152 trace-index rows and
  576 term rows.
- The post-optimize database remains 68,165,632 physical bytes because SQLite
  retains 15,852 freelist pages (64,929,792 bytes). Optimize reclaims logical
  payload space for reuse; it does not promise to shrink the database file.
- The isolated direct-SQL bucket process HWM is 17,408 KiB.

Each Jaeger row below is a separate server process:

| shape | startup HWM | after warmup HWM | after measurement HWM | ending RSS |
|---|---:|---:|---:|---:|
| exact trace | 28,124 KiB | 35,124 KiB | 35,628 KiB | 34,216 KiB |
| broad full-decode miss | 28,184 KiB | 34,692 KiB | 34,704 KiB | 34,660 KiB |
| broad 4,096-span result | 28,144 KiB | 157,772 KiB | 158,008 KiB | 84,720 KiB |

The refined boundary confirms that decoding all 131,072 rich spans is not the
source of the roughly 154 MiB peak: the zero-result broad miss peaks at only
34,704 KiB. One 3.87 MB rich Jaeger response reaches 157,772 KiB during warmup
and 158,008 KiB over the measured run. Its response construction,
serialization, and allocator retention are therefore the separate memory
target.

## Verdict

Session 1 passes. The measurements separate three costs:

1. Exact trace lookup prunes to the three status blocks in one time box, but
   still decodes 8,192 rich spans to return eight.
2. Broad miss and bucket queries both decode all 131,072 rich rows even though
   the former produces no rows and the latter needs only scalar fields.
3. Fresh-process evidence proves that a large Jaeger result spends most of its
   time and peak memory constructing and serializing the rich API response
   after storage has already stopped at three blocks.

Session 2 should therefore test projection-aware, predicate-first late
materialization against the exact-trace, full-decode-miss, and bucket shapes.
That comparison is complete in the
[Session 2 projection report](2026-08-07_trace_query_projection.md).
It must not claim the 4,096-span response allocation as a storage defect, and
it must not revise a codec unless the existing columnar representation cannot
provide the measured benefit safely.
