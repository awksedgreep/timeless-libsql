# Query evidence protocol

Every shipped matrix row carries correctness evidence and a reproducible
narrow/wide performance record. Performance never overrides oracle semantics;
it decides whether ordinary Rust/SQL composition is sufficient or whether a
general extension primitive has earned its storage-aware complexity.

The Rust-native `tools/query-harness` evidence command starts the release
metrics and logs binaries on
loopback with authentication disabled, uses a temporary database, ingests a
deterministic fixture through the public HTTP/batch path, crosses the explicit
flush durability barrier, and measures the public query APIs. It never reads a
private shadow table. Shutdown sends `SIGTERM` and requires the server's normal
drain to exit successfully.

The current harness retains the 512-series, 32-point metric baseline, adds 64
one-series metric names for multi-name selector work, and carries a second
512-series metric whose UTF-8 name and label key require Prometheus 3 quoted
syntax. Its log fixture keeps 8,192 entries spanning all eight severities with
typed nested metadata.
Each signal runs indexed narrow and wide shapes for five warmups and 50
recorded single-client iterations. The JSON records:

- admission and durability-barrier time plus completed/failed/queued work;
- p50/p95/p99/min/max latency and result cardinality;
- extension/API counter deltas, including candidate chunks, decoded/returned
  points, frame/payload/response bytes for metrics, and
  candidate/decoded/returned work for logs;
- logical extension storage, SQLite file/WAL/SHM and physical bytes;
- response bytes and Linux process RSS HWM; and
- the cancellation regression that accompanies the query surface.

Run from release binaries and an extension whose build identities match
`HEAD`. The harness reads `timeless_capabilities()` and fails before fixture
setup if the extension or either signal binary is stale:

```bash
TIMELESS_BUILD_COMMIT="$(git rev-parse HEAD)" \
  cargo build -p timeless-ext --release --locked
TIMELESS_BUILD_COMMIT="$(git rev-parse HEAD)" \
  cargo build --manifest-path servers/Cargo.toml \
    -p timeless-metrics-api -p timeless-logs-api --release --locked
cargo run --release --manifest-path tools/query-harness/Cargo.toml --locked -- \
  evidence \
  --output docs/evidence/$(date +%F)_query_baseline.json
```

The command refuses a dirty worktree as well as stale artifacts. This keeps
the recorded commit tied to the exact source used to build and measure every
result.

The checked-in Session 0 record is
[`2026-08-04_query_baseline.json`](evidence/2026-08-04_query_baseline.json).
It is a machine-local comparison anchor, not a universal hardware claim.
Feature sessions must retain the same fixture and host when comparing before
and after, add row-specific boundary fixtures, and explain every regression or
counter change in `QUERY_STORAGE_FINDINGS.md`.

## Session 0 result

Release binaries and the extension were rebuilt with build identity
`1f206492f786eb91a04310f8175441a16b2172ba` on Linux x86-64. Times below are
single-client loopback results in milliseconds; HWM is the whole Rust server
process.

| signal/query | result cardinality | response bytes | p50 ms | p95 ms | p99 ms | RSS HWM KiB |
|---|---:|---:|---:|---:|---:|---:|
| metrics narrow exact host | 1 | 166 | 0.354 | 0.821 | 0.984 | 18,720 |
| metrics wide metric | 512 | 54,521 | 2.825 | 3.259 | 3.261 | 18,720 |
| logs narrow exact severity/service | 1,024 | 174,625 | 6.253 | 6.990 | 7.443 | 54,204 |
| logs wide wildcard | 8,192 | 1,424,639 | 17.870 | 19.155 | 19.544 | 54,204 |

Metrics admitted 16,384 points in 2.46 ms and completed the explicit durability
barrier in 20.00 ms. Its extension payload was 53,831 bytes; the live SQLite
file/WAL/SHM footprint was 525,016 bytes. Logs admitted 8,192 rich entries in
7.14 ms and crossed its durability barrier in 22.34 ms. Its logical block
bytes were 1,088,919 and live physical SQLite footprint was 1,190,496 bytes.
The JSON retains nanosecond measurements and every counter delta used for these
rounded values.

## Session 2 `PQL-S06` range-vector result

The checked-in
[`2026-08-04_session2_pql_s06.json`](evidence/2026-08-04_session2_pql_s06.json)
was captured from exact build `2789f091d3069617eb61996fe5a0b213dc886890`
with the Session 0 fixture and host. Each instant root range vector used a
five-minute window over 32 ten-second samples; the open-left boundary removes
one of the extension's 31 returned samples, leaving 30 API samples per series.

| shape | series | API points/query | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host | 1 | 30 | 741 | 0.540 | 0.838 | 0.953 | 1 | 32 | 131 |
| all 512 series | 512 | 15,360 | 365,393 | 4.096 | 4.833 | 6.011 | 512 | 16,384 | 53,831 |

The wide result crosses 365 KB of JSON and is 1.48x the Session 0 instant
selector p95 while returning 30x as many samples; the verdict is acceptable.
Label selection prunes the narrow query to one of 512 chunks before decode.
The wide query necessarily decodes every fixture chunk. The extension reports
31 raw returned points per series because its public bounds are inclusive;
the API applies the documented `(T-W,T]` boundary without changing storage.

The run durably completed all 16,384 ingested points with zero failed or
queued points, used 525,016 physical SQLite/WAL/SHM bytes, and reached 19,920
KiB RSS HWM. The dropped-request cancellation/reuse contract remained green.
No extension format, batching, compression, rollup, retention, transaction, or
migration behavior changed; only additive per-process read counters were
added to `timeless_stats`.

## Session 2 `PQL-S11` scalar result

The checked-in
[`2026-08-04_session2_pql_s11.json`](evidence/2026-08-04_session2_pql_s11.json)
was captured from exact build `f1f41f2ed9128655213d92ac0645c6b4ff1a3617`.
The instant shape evaluates one `NaN` scalar; the range shape deliberately
hits the 11,000-point grid ceiling and writes one constant matrix series.

| shape | result | response bytes | p50 ms | p95 ms | p99 ms | storage reads |
|---|---:|---:|---:|---:|---:|---:|
| instant scalar | 1 point | 79 | 0.208 | 0.384 | 0.571 | 0 |
| 11,000-step scalar range | 11,000 points | 209,087 | 0.585 | 0.770 | 0.789 | 0 |

The large scalar grid is response-serialization work only and remains below
one millisecond p99 on this host. The run completed all 16,384 fixture points
durably with zero failed/queued work and reached 19,308 KiB RSS HWM; physical
SQLite/WAL/SHM bytes remained 525,016. Because PromQL IEEE strings and
evaluation timestamps are not portable SQLite REAL semantics, this row is
correctly API-owned with no claimed SQL recipe.

## Session 2 `PQL-S12` duration result

The checked-in
[`2026-08-04_session2_pql_s12.json`](evidence/2026-08-04_session2_pql_s12.json)
was captured from exact build `9c8481387241925071eb9b88cb7d9d948a7521e6`.
Each instant query used a five-minute-and-250-millisecond root range. With the
fixture's whole-second samples this includes 31 samples per series and forces
the API's public packed-raw fallback rather than truncating time for the
second-native packed-window TVF.

| shape | series | API points/query | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host | 1 | 31 | 698 | 0.460 | 0.686 | 0.744 | 1 | 32 | 131 |
| all 512 series | 512 | 15,872 | 344,293 | 4.326 | 4.716 | 7.578 | 512 | 16,384 | 53,831 |

The subsecond-correct path does not increase extension work relative to the
neighboring whole-second raw range measurement: the narrow query still prunes
to one chunk and the wide query decodes the fixture once. Its wide p95 is
2.4% lower than the `PQL-S06` run; p99 is 1.57 ms higher, so no speedup is
claimed from separate machine-local samples. The corrected shortest-number
formatter makes response sizes smaller even though this query returns one
additional point per series.

The run completed all 16,384 fixture points durably with zero failed or queued
work, retained the 525,016-byte live SQLite/WAL/SHM footprint, and reached
19,672 KiB RSS HWM. Pinned Prometheus API cases prove exact compound-duration
scalar values, timestamp rounding, and 500 ms grids. The rule oracle and real
extension regression prove subsecond open-left range boundaries. Cancellation
and clean shutdown remained green. No extension format, batching, compression,
rollup, retention, transaction, or migration behavior changed.

## Session 2 `PQL-S13` string result

The checked-in
[`2026-08-04_session2_pql_s13.json`](evidence/2026-08-04_session2_pql_s13.json)
was captured from exact build `e3529c1ef557d3daf2f32648119c1c82e40a0de3`.
The narrow shape evaluates a short escaped string over GET. The wide shape
parses, evaluates, and returns a 64 KiB raw value submitted as a form POST;
both are one-point instant string results and perform no extension read.

| shape | result | response bytes | p50 ms | p95 ms | p99 ms | storage reads |
|---|---:|---:|---:|---:|---:|---:|
| escaped string | 1 point | 91 | 0.201 | 0.352 | 0.359 | 0 |
| 64 KiB string | 1 point | 65,612 | 1.886 | 2.477 | 2.690 | 0 |

The larger value is intentionally a parser/HTTP/serializer pressure shape,
not a normal query recommendation. It remains bounded by the server's request
and response limits; the complete bounded-work evidence is recorded under
`PQL-S20` below.
The process reached 20,092 KiB RSS HWM, completed all 16,384 fixture points
durably with zero failed or queued work, and retained the 525,016-byte live
SQLite/WAL/SHM footprint. Pinned Prometheus cases prove double-quoted escape,
backtick raw-string, millisecond timestamp, and range-query error behavior.
The real-extension regression repeats the query after a clean shutdown and
reopen. No extension state or format changed.

## Session 2 `PQL-S16` grid and lookback result

The checked-in
[`2026-08-04_session2_pql_s16.json`](evidence/2026-08-04_session2_pql_s16.json)
was captured from exact build `5332eaddfacbb6a0197ce3f3101d5551f71481f8`.
Both shapes evaluate a 21-point, 500 ms grid over ten seconds with a
10,001 ms request lookback. Storage contains ten-second, whole-second samples,
so each matched series reads three raw points and reuses them across all grid
evaluations.

| shape | series | API points/query | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension returned points/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host | 1 | 21 | 547 | 0.495 | 0.795 | 1.087 | 1 | 32 | 3 |
| all 512 series | 512 | 10,752 | 258,433 | 3.679 | 3.838 | 3.939 | 512 | 16,384 | 1,536 |

The wide evaluator expands 1,536 stored samples into 10,752 exact grid values
without another extension read per step. Its 3.84 ms p95 is acceptable for a
258 KiB response, and label selection still prunes the narrow shape to one
chunk. The process reached 20,532 KiB RSS HWM, completed all 16,384 fixture
points durably with zero failed or queued work, and retained the 525,016-byte
live SQLite/WAL/SHM footprint.

The pinned Prometheus harness now Remote Writes a real sample before proving
the open-left lookback boundary, one-millisecond inclusion, and explicit-zero
default. It also pins a non-aligned range end. Real-extension tests cover those
semantics on retained storage and invalid input; unit regressions cover
negative/non-finite duration safety and extreme-grid overflow. Cancellation,
shutdown, batching, compression, rollups, retention, and formats are unchanged.

## Session 2 `PQL-S18` value-type result

The checked-in
[`2026-08-04_session2_pql_s18.json`](evidence/2026-08-04_session2_pql_s18.json)
was captured from exact build `53337407a88eed2accdf268dcda6aeafa9ea7ae1`.
This umbrella row remeasures every shipped Prometheus result type on one build;
the real-extension contract separately pins populated and empty type dispatch.

| value/shape | result cardinality | response bytes | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| instant vector, exact host | 1 series | 164 | 0.401 | 0.519 | 0.635 |
| instant vector, 512 series | 512 series | 53,497 | 3.295 | 3.397 | 3.560 |
| range vector, exact host | 1 series / 30 points | 681 | 0.299 | 0.357 | 0.487 |
| range vector, 512 series | 512 series / 15,360 points | 334,673 | 4.752 | 5.120 | 5.537 |
| scalar instant | 1 point | 79 | 0.187 | 0.203 | 0.294 |
| scalar 11,000-point range | 1 series | 209,087 | 0.767 | 0.853 | 0.924 |
| escaped string | 1 point | 91 | 0.203 | 0.534 | 0.662 |
| 64 KiB string | 1 point | 65,612 | 1.460 | 1.644 | 1.735 |

Scalar and string values perform zero storage reads. Vector measurements keep
their existing catalog/pruning/raw-frame behavior; no work moved into another
owner for this row. The process reached 20,612 KiB RSS HWM, durably completed
all 16,384 fixture points with zero failed or queued work, and retained the
525,016-byte live SQLite/WAL/SHM footprint. The combined oracle/API/extension
suite proves exact `scalar`, `string`, `vector`, and `matrix` envelopes,
including empty results and range-query type restrictions. No SQL equivalent
is claimed for an HTTP value envelope.

## Session 2 `PQL-S19` error-envelope result

The checked-in
[`2026-08-04_session2_pql_s19.json`](evidence/2026-08-04_session2_pql_s19.json)
was captured from exact build `a293b15517eb4298060a87437886132e921b6218`.
The narrow shape rejects an invalid range step. The wide shape parses a 64 KiB
form body and rejects its unknown parameter. Both return one exact three-field
`bad_data` envelope before reader admission.

| shape | result | response bytes | p50 ms | p95 ms | p99 ms | storage/API reads |
|---|---:|---:|---:|---:|---:|---:|
| invalid step | 1 error | 120 | 0.144 | 0.235 | 0.273 | 0 |
| 64 KiB invalid request | 1 error | 108 | 0.244 | 0.370 | 0.390 | 0 |

The 64 KiB case is a request-parser pressure shape, not a normal query. Neither
shape allocates a SQLite reader or increments a PromQL execution counter. The
process reached 20,840 KiB RSS HWM, completed all 16,384 fixture points durably
with zero failed or queued work, and retained the 525,016-byte live
SQLite/WAL/SHM footprint.

Pinned Prometheus cases prove the common missing/invalid parameter and reversed
range messages. The real-extension contract adds exact type errors, strict
unknown-parameter rejection, unsupported-node rejection, and GET/POST identity.
Intentional differences for unknown parameters, non-finite evaluation time,
the 11,000-point ceiling, parser-detail text, and unprefixed native-route
dispatch are explicit in the server guide and findings log. Future grammar
rows must extend the same envelope contract before they can ship.

## Session 2 `PQL-S20` bounded-query result

The checked-in
[`2026-08-04_session2_pql_s20.json`](evidence/2026-08-04_session2_pql_s20.json)
was captured from exact extension and server build
`23147d747acb156a3d144bec9881e11b59d77243`. It retains the 512-series,
16,384-point comparison fixture and adds 25 durable series with 4,001 points
each (100,025 points) solely to cross the default 100,000 storage-work bound.
The near-result shape expands ordinary stored samples over a 195-point grid;
the next grid point crosses the final-result bound.

| shape | result | response bytes | p50 ms | p95 ms | p99 ms | storage work/query |
|---|---:|---:|---:|---:|---:|---:|
| exact host | 1 point | 164 | 0.551 | 0.719 | 0.725 | 1 chunk / 32 decoded |
| 512-series instant | 512 points | 53,497 | 3.081 | 3.341 | 3.851 | 512 chunks / 16,384 decoded |
| 99,840-point near limit | 99,840 points | 1,926,899 | 10.844 | 12.233 | 12.261 | 512 chunks / 16,384 decoded |
| 100,352-point attempt | 1 execution error | 108 | 10.383 | 11.005 | 12.365 | 512 chunks / 16,384 decoded |
| 100,025-point work attempt | 1 execution error | 139 | 0.429 | 0.594 | 0.685 | 25 chunks / 100,025 candidates / **0 payload bytes** |

The result-limit rejection intentionally performs the allowed storage read and
stops response construction at 100,000 points; it returns no partial matrix.
The storage-work rejection is the important extension boundary result: across
20 measured requests it reported 2,000,500 conservative candidate points and
500 candidate chunks while reading zero payload bytes and returning zero
points. Thus the limit is paid from chunk metadata before decompression or
TRF1 allocation. Direct SQL regressions prove the same inclusive behavior for
buffered and persisted raw points, packed window input and possible output,
transactions, rollback, old-arity calls, invalid values, and reopen.

The normal fixture completed all 16,384 points durably. The limit fixture then
completed all 100,025 additional points for a final 116,409, with zero failed
or queued work. The final SQLite/WAL/SHM footprint was 607,056 bytes and the
whole process reached 32,380 KiB RSS HWM while repeatedly constructing the
1.93 MiB near-limit response and ingesting the 2.08 MiB limit fixture. This is
a measured, bounded increase from the earlier ~20 MiB Session 2 runs, not a
claim that near-limit queries are free.

The owner defaults are 11,000 points per series, 100,000 result points,
100,000 storage work points, 16 MiB response bytes, and 30 seconds. The real
extension HTTP contract pins each bound, a `504`/`timeout` deadline envelope,
reader reuse, clean shutdown, and cold reopen. The existing dropped-request
cancellation regression remains green and now checks cancellation inside raw
window folds as well. Prometheus 3.13.2's intentional 11,001-point difference
remains documented under `QSF-022`; Timeless keeps its stricter resource tier.

No storage, compression, batching, rollup, retention, migration, or packed
format changed. The two optional hidden limits are additive, old SQL arities
remain valid, and `timeless_capabilities()` explicitly advertises support so a
mismatched server/extension pair fails closed.

## Session 3 `PQL-S04` nameless-selector result

The checked-in
[`2026-08-04_session3_pql_s04.json`](evidence/2026-08-04_session3_pql_s04.json)
was captured from exact extension and server build
`81030cf031fe5745ecc3f94495a62c7bf8201b35`. The fixture retains the 512-series
exact-name workload and adds 64 distinct metric names, one series and 32 points
per name. The narrow selector returns one name by label; the wide selector
returns all 64 names.

| shape | result series | response bytes | p50 ms | p95 ms | p99 ms | exact-name packed calls/query | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| `{selector_id="s0000"}` | 1 | 187 | 1.086 | 1.320 | 1.437 | 1 | 32 |
| `{selector_group="wide"}` | 64 | 8,062 | 3.320 | 3.555 | 6.732 | 64 | 2,048 |

The first correct implementation reopened `timeless_series` once per metric
name. On the identical fixture it measured 2.883/3.145/3.162 ms
p50/p95/p99 for the narrow shape and 5.926/6.192/9.707 ms for the wide shape.
A single streaming public-catalog pass reduced p50 by 62.34% and 43.98%
respectively while preserving exact-name payload pruning. The measured HWM
rose from 32,612 to 34,496 KiB between runs; that 1,884 KiB difference is kept
as an honest tradeoff, and catalog rows plus retained metadata are explicitly
bounded before raw reads.

The final run durably completed 118,457 points across its normal and boundary
fixtures with zero failed or queued points. Live SQLite/WAL/SHM storage was
672,688 bytes. Each query used only `timeless_series` and bounded
`timeless_raw_frame` calls; no shadow table, storage format, batching,
compression, rollup, retention, transaction, or migration behavior changed.

The real-extension contract covers instant/range/function use, missing labels,
invalid empty-matching selectors, result/catalog limits, cancellation, clean
shutdown, and cold reopen. The pinned Prometheus rule oracle matches exact
values and labels.

## Session 3 `PQL-S05` metric-name matcher result

The checked-in
[`2026-08-04_session3_pql_s05.json`](evidence/2026-08-04_session3_pql_s05.json)
was captured from exact extension and server build
`4fcebd42c10fd17897d013841cc4ba5782311036` on the same 576-series,
18,432-point selector fixture. The narrow anchored regex selects four metric
names. The wide negative matcher excludes one of the 64 selector names while
an ordinary label matcher excludes the unrelated 512-series metric.

| shape | result series | response bytes | p50 ms | p95 ms | p99 ms | exact-name packed calls/query | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| `{__name__=~"query_selector_metric_000[0-3]"}` | 4 | 562 | 1.223 | 1.760 | 1.888 | 4 | 128 |
| `{__name__!="query_selector_metric_0000",selector_group="wide"}` | 63 | 7,937 | 3.335 | 3.687 | 5.023 | 63 | 2,016 |

The counters prove catalog-first payload pruning: the regex shape considers
and decodes exactly four chunks, while the negative shape considers 63—not all
601 fixture chunks. The public catalog still crosses once per query, so name
matcher complexity adds no extension opcode or private-table dependency.

The final run durably completed 118,457 normal and boundary points with zero
failed or queued work, used 672,688 live SQLite/WAL/SHM bytes, and reached
34,416 KiB RSS HWM. Real-extension and pinned Prometheus regressions cover
anchoring, negative equality/regex, repeated-name AND semantics, empty-name
legality, deterministic labels/values, clean shutdown, and cold reopen. No
storage format, batching, compression, rollup, retention, transaction, or
migration behavior changed.

## Session 3 `PQL-S07`/`PQL-S08` temporal-modifier result

The checked-in
[`2026-08-04_session3_pql_s07_s08.json`](evidence/2026-08-04_session3_pql_s07_s08.json)
was captured from exact extension and server build
`efa0511a39ad4392ab5db2ebb1c9274cf3e24717`. Narrow shapes select one of the
512 exact-name series. Wide shapes return four outer-grid values for all 512
series while shifting or freezing lookup time.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|
| exact host, `offset 20s` | 1 | 164 | 0.374 | 0.490 | 0.512 | 32 |
| 512 series × 4, `offset -20s` | 2,048 | 84,004 | 3.341 | 3.595 | 3.946 | 16,384 |
| exact host, numeric `@` | 1 | 164 | 0.735 | 1.020 | 1.257 | 32 |
| 512 series × 4, `@ end()` | 2,048 | 84,010 | 3.280 | 3.504 | 4.388 | 16,384 |

All four shapes perform one bounded packed call per exact metric selection,
not one call per outer step. The fixed-`@` wide query reads 15,872 in-lookback
points from the same 16,384 decoded fixture points and reuses the chosen value
across four output timestamps. The offset grid similarly reuses one read.

The run durably completed 118,457 normal and boundary points with zero failed
or queued work, used 672,688 live SQLite/WAL/SHM bytes, and reached 33,344 KiB
RSS HWM. Pinned Prometheus API cases prove positive/negative offset, numeric
anchor-before-offset ordering, outer range timestamps, and range `start()` /
`end()` behavior. The rule oracle and real-extension contract add window
functions, root range-vector timestamps, millisecond/pre-epoch conversion,
limits, cancellation, shutdown, and reopen. Section 45 executes both direct
SQL forms. No extension opcode, storage format, batching, compression, rollup,
retention, transaction, or migration behavior changed.

## Session 3 `PQL-S09` subquery result

The checked-in
[`2026-08-04_session3_pql_s09.json`](evidence/2026-08-04_session3_pql_s09.json)
was captured from exact extension and server build
`7b30ff62eeb33c41a806afda7aa3377e78ac5eea` on the same 512-series,
18,432-point fixture. Root shapes expose a 30-point globally aligned inner
grid. Consuming shapes run `avg_over_time` over that grid; the wide case emits
four outer points for every series.

| shape | final points | intermediate points/query | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| exact host, root `[5m:10s]` | 30 | 0 | 681 | 0.391 | 0.587 | 0.638 | 32 |
| 512 series, root `[5m:10s]` | 15,360 | 0 | 334,673 | 4.372 | 5.597 | 7.055 | 16,384 |
| exact host, `avg_over_time(...[5m:10s])` | 1 | 30 | 134 | 0.412 | 0.658 | 0.714 | 32 |
| 512 series × four outer points, `avg_over_time(...[5m:10s])` | 2,048 | 16,384 | 70,633 | 8.300 | 9.494 | 9.864 | 16,384 |

A root subquery's inner points are its final matrix, so they are not counted
again as intermediate work. A consuming function reports its materialized
inner matrix through `api_promql_intermediate_points`; measured deltas were
exactly 1,500 and 819,200 across 50 narrow/wide requests. Every shape performs
one bounded packed raw read for the selected metric, not one storage read per
outer point. Candidate chunks, payload bytes, decoded points, frame bytes,
final points, intermediate points, and response bytes are all present in the
evidence.

The cumulative intermediate bound includes inherited nested-subquery work.
The real-extension regression isolates a shape where each level stays within
the limit but their sum does not, and rejects it before the outer matrix is
decoded.

The run durably completed all 18,432 fixture points with zero failed or queued
work. Live SQLite/WAL/SHM storage remained exactly 672,688 bytes, matching the
preceding temporal-modifier run. RSS HWM was 36,332 KiB, 2,988 KiB (8.96%)
above that preceding 33,344 KiB measurement. This is the honest cost of the
wide bounded intermediate matrix plus its serialized bridge; ordinary
selector/window hot paths remain streaming. Both point count and bridge bytes
are capped, cancellation is checked during inner evaluation/decode/folding,
and the 4,000-series dropped-request regression proves the sole reader is
released and reusable.

Prometheus 3.13.2 API and promtool fixtures pin open-left ranges, global
alignment, the 15-second default resolution, numeric/start/end anchors,
offset ordering, outer range timestamps, nested subqueries, metric-name
removal, and result types. The real-extension contract additionally pins the
work-limit error, public intermediate counter, clean shutdown, and cold
reopen. Section 45 executes the equivalent aligned selector grid using only
public SQL. No extension opcode, private table, storage format, batching,
compression, rollup, retention, transaction, or migration behavior changed.

## Session 4 `PQL-O01` unary-minus result

The checked-in
[`2026-08-04_session4_pql_o01.json`](evidence/2026-08-04_session4_pql_o01.json)
was captured from exact extension and server build
`20583fb8e95f92a9968a5990a5983d0948601605`. The narrow shape negates one
exact-host instant sample. The wide shape negates a four-point range grid for
all 512 series.

| shape | final points | intermediate points/query | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| exact host, `-metric` | 1 | 1 | 133 | 0.448 | 0.802 | 0.880 | 32 |
| 512 series × four points, `-metric` | 2,048 | 2,048 | 69,668 | 4.348 | 4.810 | 5.242 | 16,384 |

The same-run comparable 512-series × four-point selector grid measured
3.076/3.454/4.949 ms p50/p95/p99. Unary composition therefore adds 1.272 ms
p50 and 1.355 ms p95 for bounded JSON decode, value negation, name removal,
and canonical re-encoding. Candidate chunks, payload bytes, decoded points,
and returned storage points are identical between the two shapes: negation
causes no additional extension read. Its response is smaller because the
pinned Prometheus rule removes `__name__` from every output vector.

The run durably completed all 118,457 main and limit-fixture points with zero
failed or queued work. Live SQLite/WAL/SHM storage remained 672,688 bytes and
whole-process RSS HWM was 35,536 KiB, below Session 3's 36,332 KiB run. Child
points are reported through `api_promql_intermediate_points` and share the
hard cumulative work limit; cancellation is checked during decode,
transformation, and output. Ordinary selector/window hot paths remain
streaming.

Prometheus 3.13.2 API and promtool fixtures pin scalars, range grids, nested
range functions, unary nodes inside subqueries, double negation, type errors,
IEEE strings, timestamps, and metric-name/non-name-label policy. The
real-extension regression adds limits, shutdown, and cold reopen. Section 33
executes ordinary SQLite unary arithmetic through the public grid. No
extension opcode, private table, storage format, batching, compression,
rollup, retention, transaction, or migration behavior changed; the measured
host-only cost does not justify an extension primitive.

## Session 4 `PQL-O02`/`PQL-O05` arithmetic and one-to-one result

The checked-in
[`2026-08-04_session4_pql_o02_o05.json`](evidence/2026-08-04_session4_pql_o02_o05.json)
was captured from exact extension and server build
`7c9e8b402aa55946d7917bcc2be675ba0740c2e5`. The narrow shape multiplies one
exact-host vector by a scalar. The wide shape adds two independently evaluated
512-series vectors over four timestamps using default one-to-one matching.

| shape | final points | intermediate points/query | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| exact host, `metric * 2` | 1 | 2 | 132 | 0.383 | 0.848 | 0.982 | 32 |
| 512 series × four points, `metric + metric` | 2,048 | 4,096 | 67,986 | 8.895 | 9.696 | 10.054 | 32,768 |

The narrow scalar child is generated without storage; the vector performs one
packed read and its two child values are charged as intermediate work. The
wide shape performs exactly two packed reads per selected series—one for each
operand—then matches samples by every non-name label at each evaluation
timestamp. Across 50 iterations, counters report exactly 51,200 candidate
chunks, 1,638,400 decoded points, and 204,800 intermediate points. No operand
is re-read per output timestamp.

The run durably completed all 118,457 fixture points with zero failed or
queued work. Live SQLite/WAL/SHM storage remained 672,688 bytes and
whole-process RSS HWM was 35,164 KiB, below both preceding Session 4 and
Session 3 measurements. The 8.895 ms wide p50 includes two storage decodes,
two bounded child envelopes, per-step cardinality validation, arithmetic, and
canonical output. Common-subexpression elimination for the synthetic
`metric + metric` shape is not generalized without workload evidence.

Prometheus 3.13.2 API and promtool fixtures pin precedence, all six operators,
both scalar/vector directions, default name-excluding match signatures,
unmatched filtering, duplicate-signature errors, metric-name removal, range
grids, and IEEE division/modulo/power results. The real-extension regression
adds exact ordering, cumulative work rejection, shutdown, and cold reopen.
Section 33 executes all six SQLite operations using public grids and an
exact-label join. No extension opcode, private table, storage format,
batching, compression, rollup, retention, transaction, or migration behavior
changed.

## Session 4 `PQL-O03` comparison result

The checked-in
[`2026-08-04_session4_pql_o03.json`](evidence/2026-08-04_session4_pql_o03.json)
was captured from exact extension and server build
`17c5091c1c20a424601b9411ed83819d01cb416a`. The narrow shape applies a true
filter to one exact-host instant vector. The wide shape maps a scalar `bool`
comparison over 512 series and four timestamps.

| shape | final points | intermediate points/query | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| exact host, `metric > 30` | 1 | 2 | 164 | 0.511 | 0.858 | 0.915 | 32 |
| 512 series × four points, `metric > bool 0` | 2,048 | 2,052 | 63,806 | 4.291 | 4.666 | 4.814 | 16,384 |

The filter retains the original vector value and metric name. The `bool`
shape reads the vector once, generates four scalar-grid child points without
storage, maps every vector point to `0` or `1`, and removes the name. Its
storage counters exactly match the same-run unary wide shape; comparison adds
0.294 ms p50 and 0.417 ms p95 for scalar-grid decode and predicate mapping.
All 2,048 vector plus four scalar child points are reported as bounded
intermediate work.

The run durably completed all 118,457 fixture points with zero failed or
queued work. Live SQLite/WAL/SHM storage remained 672,688 bytes and
whole-process RSS HWM was 35,672 KiB. Prometheus 3.13.2 API and promtool
fixtures pin scalar `bool`, NaN behavior, both scalar/vector directions,
true/false filters, sparse range output, vector/vector matching, original
value/name retention, and `bool` name removal. The real-extension regression
adds all six operators, exact ordering, cumulative limits, shutdown, and cold
reopen. Section 33 executes both public-grid SQL forms. No extension opcode,
private table, storage format, batching, compression, rollup, retention,
transaction, or migration behavior changed.

## Session 4 `PQL-O04` set-operator result

The checked-in
[`2026-08-04_session4_pql_o04.json`](evidence/2026-08-04_session4_pql_o04.json)
was captured from exact extension and server build
`a04981e3c3fb25263ff59116f8a288669b248a90`. The narrow shape intersects one
exact-host instant vector with itself. The wide shape forms the
left-preferred union of two 512-series vectors over four timestamps.

| shape | final points | intermediate points/query | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| exact host, `metric and metric` | 1 | 2 | 164 | 0.502 | 0.789 | 0.859 | 64 |
| 512 series × four points, `metric or metric` | 2,048 | 4,096 | 84,004 | 8.562 | 9.632 | 10.069 | 32,768 |

Both operands are independently selectable AST children, so both shapes make
two bounded packed reads and charge every child point as intermediate work.
Across 50 wide requests the extension considered 51,200 candidate chunks,
decoded 1,638,400 stored points, returned 1,638,400 storage points, and the API
reported 204,800 intermediate points. Membership itself performs no further
storage read. The wide response is larger than arithmetic/`bool` responses
because set operators preserve the contributing metric name.

The run durably completed all 118,457 main and boundary-fixture points with
zero failed or queued work. Live SQLite/WAL/SHM storage was 672,688 bytes and
whole-process RSS HWM was 36,460 KiB. The existing cumulative work/response
limits bound both child materializations and the final result; cancellation
is checked during child execution, per-step membership, normalization, and
serialization.

Prometheus 3.13.2 API and promtool fixtures pin `and`, `unless`, left-preferred
`or`, source metric names and values, true many-to-many membership, and
operator precedence. The real-extension regression adds scalar rejection,
step-local range sparsity, exact ordering, cumulative-work rejection,
shutdown, and cold reopen. Section 33 executes `EXISTS`, `NOT EXISTS`, and
left-preferred `UNION ALL` using only public grids. No extension opcode,
private table, storage format, batching, compression, rollup, retention,
transaction, or migration behavior changed; ordinary SQL and bounded Rust
composition remain the justified boundary.

## Session 4 `PQL-O06` explicit-matching result

The checked-in
[`2026-08-04_session4_pql_o06.json`](evidence/2026-08-04_session4_pql_o06.json)
was captured from exact extension and server build
`1aa9b2593824fda5c51459d09820d0af6adfba9b`. Both shapes add identical
vectors with `on(host)`; the narrow request selects one exact host and the
wide request evaluates all 512 series at four timestamps.

| shape | final points | intermediate points/query | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| exact host, `+ on(host)` | 1 | 2 | 116 | 0.559 | 0.694 | 0.742 | 64 |
| 512 series × four points, `+ on(host)` | 2,048 | 4,096 | 59,026 | 9.001 | 11.569 | 22.832 | 32,768 |

The same-run default-matching arithmetic shape measured
9.083/10.504/11.222 ms p50/p95/p99. Explicit matching was 0.082 ms faster at
p50, 1.065 ms slower at p95, and had one retained 22.832 ms p99/max outlier in
the 50-request sample. The complete raw counters remain structurally equal:
each request performs two packed reads, considers 1,024 candidate chunks,
decodes 32,768 stored points, and charges 4,096 child points as intermediate
work. The explicit result is smaller because `on(host)` projects away every
other label. This evidence is retained as measured; it does not support a new
extension primitive or a latency-regression verdict from one scheduling tail.

The run durably completed all 118,457 main and boundary-fixture points with
zero failed or queued work. Live SQLite/WAL/SHM storage was 672,688 bytes and
whole-process RSS HWM was 37,712 KiB. This harness is cumulative and executed
two more full query shapes than the preceding 36,460 KiB run, so the 1,252 KiB
peak delta is an upper bound for added process residency, not a per-query
allocation claim. Work, result, response-byte, deadline, and cancellation
limits remain unchanged.

Prometheus 3.13.2 API and promtool fixtures pin `on`, `on()`, `ignoring`,
missing-as-empty labels, arithmetic/comparison output projection, set-label
retention, and duplicate match-group errors. The real-extension regression
adds range behavior, cumulative-work rejection, shutdown, and cold reopen.
Section 33 executes both JSON-label SQL joins through public grids. No
extension opcode, private table, storage format, batching, compression,
rollup, retention, transaction, or migration behavior changed.

## Session 4 `PQL-O07` group-matching result

The checked-in
[`2026-08-04_session4_pql_o07.json`](evidence/2026-08-04_session4_pql_o07.json)
was captured from exact extension and server build
`b00c93aa240f3ec1295d2591cef7b8631514b895`. This fixture adds two
32-point service-factor series—one unique side for each of the `api` and
`worker` groups—to the established 512-series workload. The narrow shape uses
one CPU series against both factors with `group_left(team)`; the wide shape
uses the two factors against all CPU series with `group_right(team)`.

| shape | final points | intermediate points/query | response bytes | p50 ms | p95 ms | p99 ms | decoded points/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| exact host, `+ group_left(team)` | 1 | 3 | 150 | 0.626 | 0.986 | 1.181 | 96 |
| 2 one-side × 512 many-side × four points, `- group_right(team)` | 2,048 | 2,056 | 78,618 | 6.749 | 7.217 | 7.497 | 16,448 |

Each request reads each operand exactly once. The wide shape considers 514
candidate chunks, decodes 512 CPU plus two factor series, and charges 2,048
many-side plus eight one-side child points as intermediate work. It is faster
than a 512-by-512 self-join because the one side is genuinely small; no group
operation triggers per-many-series storage reads. The retained result bytes
include the copied `team` label.

The run durably completed all 118,521 main and boundary-fixture points with
zero failed or queued work. The two added persisted series raised logical
payload bytes to 64,361, while total SQLite/WAL/SHM storage remained 672,688
bytes. Whole-process RSS HWM was 35,500 KiB. Intermediate, result, response,
deadline, and cancellation limits cover both child vectors, per-step matching,
label construction, uniqueness checks, and serialization.

Prometheus 3.13.2 API and promtool fixtures pin both group directions,
operation direction, comparison left-value/right-name behavior, included and
absent labels, unique-side cardinality, and post-copy result uniqueness. The
real-extension regression adds range grids, exact ordering, cumulative-work
rejection, shutdown, and cold reopen. Section 33 executes both joins plus a
one-side uniqueness preflight using public grids. No extension opcode, private
table, storage format, batching, compression, rollup, retention, transaction,
or migration behavior changed.

## Session 6 `PQL-R06` last value over time

The checked-in
[`2026-08-04_session6_pql_r06.json`](evidence/2026-08-04_session6_pql_r06.json)
was captured from exact extension and server build
`c119518dc29b85373656046f8fe7fabbc5817e77`. The API uses the public packed
raw waist; direct SQLite users can select the same last value with
`timeless_grid`.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `last_over_time(...[5m])` | 1 | 164 | 0.521 | 0.743 | 0.795 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `last_over_time(...[5m])` | 2,048 | 84,004 | 3.225 | 3.499 | 3.507 | 16,384 | 512 | 16,384 | 53,831 |

The wide plan deliberately returns all 16,384 bounded inputs to Rust and
selects 2,048 last values. That is more boundary work than the packed window
reductions, but measured p95 remains 3.499 ms. A new packed-grid extension
surface is therefore not justified; the row-oriented public grid already
serves direct SQL users exactly, while the raw path keeps one packed crossing
per metric and applies outer timestamps/subqueries in Rust.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,012 KiB. Pinned oracle and real-extension regressions cover
open-left boundaries, empty windows, exact NaN/infinity/signed-zero selection,
subqueries, exceptional metric-name retention, work limits, cancellation,
shutdown, and cold reopen. The executable SQL recipe checks last-value and
signed-zero fidelity. No extension primitive or storage, frame, batching,
compression, index, rollup, retention, transaction, or migration behavior
changed.

## Session 5 `PQL-O09` cross-series sum

The checked-in
[`2026-08-04_session5_pql_o09.json`](evidence/2026-08-04_session5_pql_o09.json)
was captured from exact build `e91ca730c80170c5212e65d24a29967fa6ebf049`.
The narrow shape sums one selected host by `host`; the wide shape sums a
512-series, four-step grid into the two `service` groups.

| shape | input series/step | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host | 1 | 1 | 116 | 0.438 | 0.555 | 0.614 | 1 | 32 | 524 |
| all series, two groups × four steps | 512 | 8 | 313 | 3.861 | 4.120 | 4.512 | 512 | 16,384 | 268,304 |

The wide result materializes 2,048 bounded child points, then reduces them in
Rust without another storage read or extension primitive. Per query it reads
53,831 persisted payload bytes and returns 16,384 raw stored points through
the public packed frame; cardinality collapses only after exact PromQL
grouping. The narrow filter prunes to one series and one chunk. This is the
expected cost boundary: direct SQLite/libSQL users perform the same numeric
reduction with ordinary `SUM` over `timeless_grid`.

The run durably completed all 18,496 fixture points with zero failed or queued
work, retained a 672,688-byte live SQLite/WAL/SHM footprint, and reached
35,956 KiB RSS HWM. The real-extension regression covers pre-decode work
limits and shutdown/reopen; the common dropped-request contract covers
cancellation. Pinned Prometheus API and promtool cases prove `by`, `without`,
empty/missing groups, explicit `__name__` grouping, sparse range grids, and
IEEE results. `QSF-035` records the discovered removal of the obsolete
BEAM-era non-finite exposition restriction; batching, compression, indexes,
rollups, retention, transactions, and packed formats are unchanged.

## Session 5 `PQL-O10` cross-series average

The checked-in
[`2026-08-04_session5_pql_o10.json`](evidence/2026-08-04_session5_pql_o10.json)
was captured from exact build `21aa78f1915ec75f560980ce78db638948d03986`.
It repeats the Session 5 narrow and wide grouping shapes with `avg`.

| shape | input series/step | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host | 1 | 1 | 116 | 0.384 | 0.552 | 0.612 | 1 | 32 | 524 |
| all series, two groups × four steps | 512 | 8 | 297 | 3.973 | 4.090 | 4.297 | 512 | 16,384 | 268,304 |

Storage work is identical to `sum`: 31 returned raw points for the narrow
selector and 16,384 for the wide grid, with one public packed-frame read per
query. The wide evaluator charges 2,048 child points as intermediate work and
performs compensated arithmetic only while collapsing them into eight final
points. The 4.09 ms p95 is the honest cost of bounded Rust composition over
the retained public storage waist; it does not justify a new extension
primitive.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes. Whole-process RSS HWM
was 37,304 KiB. This cumulative harness added two measured query shapes after
the preceding run, so its HWM delta is not attributed to one average query.
Oracle and real-extension regressions pin compensated cancellation recovery,
finite overflow fallback, IEEE values, grouping labels, limits, and reopen.
The SQL cookbook executes ordinary `AVG` and explicitly states where its
accumulation differs from Prometheus's compensated evaluator. No storage or
packed-format change was made.

## Session 5 `PQL-O11` cross-series extrema

The checked-in
[`2026-08-04_session5_pql_o11.json`](evidence/2026-08-04_session5_pql_o11.json)
was captured from exact build `a3842405bdbe2040cb89d582aedf26b8537ea5ea`.
The narrow shape runs `min` over one host; the wide shape runs `max` over 512
series grouped into two services across four timestamps.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact-host `min` | 1 | 116 | 0.687 | 0.979 | 1.103 | 1 | 32 | 524 |
| all-series grouped `max` | 8 | 297 | 3.864 | 4.321 | 4.418 | 512 | 16,384 | 268,304 |

Both operations have the same storage and 2,048-point wide intermediate-work
shape as `sum`/`avg`. The narrow run retained a machine-local scheduling tail;
the exact storage counters show no additional reads or decode, so no latency
regression is inferred from that isolated p95. Ordinary public-grid `MIN` and
`MAX` remain sufficient for direct SQL users; packed bits are required only
to distinguish an all-NaN group from SQL NULL.

The run completed all 18,496 fixture points durably with zero failed or queued
work, kept physical SQLite/WAL/SHM storage at 672,688 bytes, and reached
35,656 KiB RSS HWM. Pinned Prometheus and real-extension cases cover grouped
finite values, infinities, mixed/all NaNs, sparse range grids, deterministic
Timeless output, cancellation, and cold reopen. No extension primitive,
storage format, batching, compression, index, rollup, retention, transaction,
or migration behavior changed.

## Session 5 `PQL-O12` cross-series count and group

The checked-in
[`2026-08-04_session5_pql_o12.json`](evidence/2026-08-04_session5_pql_o12.json)
was captured from exact build `af80d8f90854d56fb48f20bbc80e39a356944393`.
The narrow shape counts one selected host; the wide shape reports group
presence for 512 series collapsed into two services across four timestamps.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact-host `count` | 1 | 115 | 0.498 | 0.630 | 0.794 | 1 | 32 | 524 |
| all-series grouped `group` | 8 | 281 | 3.881 | 4.141 | 4.241 | 512 | 16,384 | 268,304 |

The wide evaluator charges all 2,048 bounded child points before reducing
them to eight output points. It performs one packed public-frame read and no
additional storage work. The count path deliberately counts rows, not SQL
numeric values, so NaN and both infinities contribute exactly as they do in
Prometheus. Ordinary `COUNT(*)` and a constant one over the public grid are
therefore the complete direct-SQL foundation and no extension primitive is
warranted.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,536 KiB. Pinned Prometheus API and promtool fixtures cover `by`,
`without`, retained non-grouping labels, and non-finite inputs. The
real-extension regression adds empty grouping, range grids, shutdown, and
cold reopen. Batching, compression, indexes, rollups, retention, transactions,
migrations, and packed formats are unchanged.

## Session 5 `PQL-O13` population dispersion

The checked-in
[`2026-08-04_session5_pql_o13.json`](evidence/2026-08-04_session5_pql_o13.json)
was captured from exact build `b211dfbee6a1fad6e45d99222d10f9f79add6174`.
The narrow shape computes `stdvar` for one host; the wide shape computes
`stddev` for 512 series grouped into two services over four timestamps.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact-host `stdvar` | 1 | 115 | 0.316 | 0.865 | 0.959 | 1 | 32 | 524 |
| all-series grouped `stddev` | 8 | 417 | 3.956 | 5.890 | 10.636 | 512 | 16,384 | 268,304 |

Storage work and the 2,048-point wide intermediate charge are identical to
the other cross-series aggregations. The wide run retained a machine-local
tail above its 3.956 ms median; exact counters show no extra read, candidate,
decode, or allocation path. The result is recorded rather than hidden, and
does not motivate an extension primitive. Each group uses constant memory and
Prometheus's one-pass population-variance update.

The run durably completed all 18,496 fixture points with zero failed or queued
work, retained the 672,688-byte SQLite/WAL/SHM footprint, and reached 37,088
KiB RSS HWM. Oracle and real-extension cases cover grouped population
variance/deviation, singleton zero, NaN/infinity propagation, range grids,
cancellation, and cold reopen. The executable public-SQL second-moment recipe
is explicitly limited to finite, well-scaled values; no storage, packed-frame,
batching, compression, index, rollup, retention, transaction, or migration
behavior changed.

## Session 5 `PQL-O14` step-local ranking

The checked-in
[`2026-08-04_session5_pql_o14.json`](evidence/2026-08-04_session5_pql_o14.json)
was captured from exact build `27ddda571ae11ac28626a9c4ca525f80fc69abd3`.
The narrow shape ranks one selected host with `topk(1, ...)`; the wide shape
selects the bottom four series in each of two services at each of four steps.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact-host `topk(1)` | 1 | 164 | 0.377 | 0.619 | 0.702 | 2 | 32 | 524 |
| grouped `bottomk(4)`, 512 series × four steps | 32 | 1,346 | 4.192 | 5.415 | 8.111 | 2,052 | 16,384 | 268,304 |

The parameter contributes one scalar point per evaluation step, explaining
the wide shape's 2,048 child plus four parameter points. Ranking is bounded by
the already-admitted child vector, retains original series labels, and makes
no second storage read. The wide run retained an 8.111 ms machine-local p99;
candidate, decoded, packed-frame, and persisted-byte counters are identical to
the preceding aggregation shapes, so the tail is reported without attributing
it to new storage work.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and RSS HWM was 35,756
KiB. Pinned Prometheus and real-extension cases cover scalar expressions,
fractional/zero/NaN/infinite parameters, grouped top and bottom selection,
numeric-before-NaN ranking, original labels and metric names, per-step sparse
ranges, cumulative work limits, cancellation, and cold reopen. Public
window-function SQL remains the direct-user foundation. No extension
primitive, storage format, batching, compression, index, rollup, retention,
transaction, or migration behavior changed.

## Session 5 `PQL-O15` cross-series quantile

The checked-in
[`2026-08-04_session5_pql_o15.json`](evidence/2026-08-04_session5_pql_o15.json)
was captured from exact build `359aa4e842289513473620046ef8a569782b4c52`.
The narrow shape interpolates one selected host; the wide shape computes the
0.95 quantile of 512 series grouped into two services at four timestamps.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact-host `quantile(0.5)` | 1 | 102 | 0.639 | 0.959 | 1.198 | 2 | 32 | 524 |
| grouped `quantile(0.95)`, 512 series × four steps | 8 | 313 | 3.838 | 4.177 | 4.276 | 2,052 | 16,384 | 268,304 |

The wide evaluator admits 2,048 child points plus four scalar-parameter
points, then sorts bounded per-group/per-step vectors in memory. It makes one
public packed-frame read and no extra storage pass. The 4.177 ms p95 is in the
same range as simpler reductions, so there is no evidence for adding a
storage-specific quantile opcode. Memory remains bounded by the configured
intermediate-work limit.

The run durably completed all 18,496 points with zero failed or queued work,
retained 672,688 bytes of SQLite/WAL/SHM storage, and reached 37,528 KiB RSS
HWM. Pinned Prometheus and real-extension cases cover interpolation,
singletons, raw-NaN rank, infinities, NaN/out-of-range parameters, grouped and
range evaluation, cumulative limits, cancellation, and cold reopen. The
ordinary public-SQL recipe is explicitly finite-only rather than obscuring
SQLite's NaN-to-NULL boundary. Storage format, batching, compression, indexes,
rollups, retention, transactions, migrations, and packed frames are unchanged.

## Session 5 `PQL-O16` counts by sample value

The checked-in
[`2026-08-04_session5_pql_o16.json`](evidence/2026-08-04_session5_pql_o16.json)
was captured from exact build `01d9d32fb6bc9d3b056c60c69ad864dc8cd63f1e`.
The narrow shape counts one selected host value. The deliberately adversarial
wide shape groups 512 input series by service and distinct value at each of
four steps, retaining all 2,048 points across 1,028 output label sets.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | decoded points/query | extension bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact-host `count_values` | 1 | 113 | 0.375 | 0.481 | 0.486 | 1 | 32 | 524 |
| grouped wide, 512 series × four steps | 2,048 | 91,789 | 5.074 | 5.291 | 5.550 | 2,048 | 16,384 | 268,304 |

Unlike scalar reductions, the wide fixture intentionally cannot collapse
cardinality because its values differ. The 91,789-byte response and 1,028
matrix series are the honest language result, not storage amplification.
Result-point and response-byte limits bound it; grouping consumes the one
admitted child vector, uses memory proportional to bounded output, and makes
no additional extension read.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and RSS HWM was 37,332
KiB. Pinned Prometheus and real-extension cases cover grouping, input-label
overwrite, fixed shortest finite formatting, exponent expansion, signed zero,
infinities, NaN, invalid label names, range counts, output limits,
cancellation, and cold reopen. Direct SQL groups the public grid's raw numeric
value; no extension primitive, format, batching, compression, index, rollup,
retention, transaction, or migration behavior changed.

## Session 6 `PQL-R01` compensated range average

The checked-in
[`2026-08-04_session6_pql_r01.json`](evidence/2026-08-04_session6_pql_r01.json)
was captured from exact extension and server build
`3fdff94781b567715d3fde5eaf68370d2a878027`. Direct shapes use the public
packed window kernel; subquery shapes materialize their already-bounded inner
matrix from the public packed raw frame before applying the same compensated
average in Rust.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, native `avg_over_time(...[5m])` | 1 | 134 | 0.388 | 0.593 | 0.665 | 0 | 1 | 32 | 131 |
| 512 series × four steps, native `avg_over_time(...[5m])` | 2,048 | 70,633 | 2.974 | 3.546 | 4.107 | 0 | 512 | 16,384 | 53,831 |
| exact host, `avg_over_time(...[5m:10s])` | 1 | 134 | 0.402 | 0.520 | 0.600 | 30 | 1 | 32 | 131 |
| 512 series × four steps, `avg_over_time(...[5m:10s])` | 2,048 | 70,633 | 8.466 | 8.863 | 8.949 | 16,384 | 512 | 16,384 | 53,831 |

The direct wide query returns 2,048 sparse grid points after one packed
window call; the new public counters report exactly 512 considered series,
512 candidate chunks, 16,384 decoded inputs, and 2,048 returned window
points per request. The subquery wide shape reads the same chunks/inputs but
also owns 16,384 intermediate points, explaining its additional Rust
composition time without attributing that cost to storage.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,324 KiB. Pinned Prometheus and real-extension cases cover exact
`(T-window,T]` boundaries, sparse/empty windows, cancellation-sensitive
finite sums, overflow fallback, NaN/infinities, subqueries, work limits,
shutdown, and cold reopen. `tests/cli.sh` executes the compensated public SQL
recipe and exact `window_batch_query_*` stats. The correction changes only a
general reduction result and additive observability: batching, compression,
indexes, rollups, retention, transactions, migrations, and packed-frame
formats remain intact.

## Session 6 `PQL-R02` range minimum

The checked-in
[`2026-08-04_session6_pql_r02.json`](evidence/2026-08-04_session6_pql_r02.json)
was captured from exact extension and server build
`218a9aabcf4f260470f7f4952a02239be5d15c62`. Both shapes use the public
packed window kernel and report its complete input/decode/result work.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, native `min_over_time(...[5m])` | 1 | 131 | 0.316 | 0.477 | 0.604 | 0 | 1 | 32 | 131 |
| 512 series × four steps, native `min_over_time(...[5m])` | 2,048 | 67,468 | 3.055 | 6.384 | 7.059 | 0 | 512 | 16,384 | 53,831 |

The wide request produces 2,048 sparse grid points after considering exactly
512 candidate chunks and decoding the expected 32 samples from each series.
The API allocates no intermediate matrix on this whole-second native path.
Subsecond/modifier fallbacks and subqueries are correctness-tested through the
same ordered minimum state, but are not attributed to the extension fast path.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,124 KiB. Pinned Prometheus and real-extension cases cover exact
`(T-window,T]` boundaries, empty windows, later/leading/all-NaN behavior,
infinities, first-sample signed-zero stability, subqueries, work limits,
cancellation of the shared range-reduction executor, shutdown, and cold
reopen. The direct SQL recipe also checks the stable-zero result. The kernel
correction changes no extension signature, storage or frame format, batching,
compression, index, rollup, retention, transaction, or migration behavior.

## Session 6 `PQL-R03` range maximum

The checked-in
[`2026-08-04_session6_pql_r03.json`](evidence/2026-08-04_session6_pql_r03.json)
was captured from exact extension and server build
`f4b010010ecb92cc83b07b93ddffe4807eb8c162`. Both measured shapes use the
public packed window kernel and expose its complete storage work.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, native `max_over_time(...[5m])` | 1 | 132 | 0.609 | 0.829 | 0.901 | 0 | 1 | 32 | 131 |
| 512 series × four steps, native `max_over_time(...[5m])` | 2,048 | 67,620 | 3.190 | 3.671 | 5.132 | 0 | 512 | 16,384 | 53,831 |

The wide query produces 2,048 sparse points from exactly 512 candidate chunks
and 16,384 decoded inputs. No intermediate matrix is allocated on the native
whole-second path; raw modifier/subsecond and subquery composition retain the
same ordered maximum semantics under the shared work limit.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,900 KiB. Oracle and real-extension regressions cover open-left
boundaries, empty windows, leading/later/all-NaN behavior, infinities, both
signed-zero input orders, subqueries, work limits, cancellation of the shared
range executor, shutdown, and cold reopen. The executable SQL recipe checks
the reverse-zero case. No extension signature, storage/frame format, batching,
compression, index, rollup, retention, transaction, or migration behavior
changed.

## Session 6 `PQL-R04` compensated range sum

The checked-in
[`2026-08-04_session6_pql_r04.json`](evidence/2026-08-04_session6_pql_r04.json)
was captured from exact extension and server build
`07ec0f6a97eb6928b6840313e287e5b5bee7634d`. Both measured shapes use the
public packed window kernel and its compensated sum.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, native `sum_over_time(...[5m])` | 1 | 133 | 0.243 | 0.555 | 0.670 | 0 | 1 | 32 | 131 |
| 512 series × four steps, native `sum_over_time(...[5m])` | 2,048 | 70,638 | 3.104 | 3.391 | 3.552 | 0 | 512 | 16,384 | 53,831 |

The wide path returns 2,048 sparse points from 512 candidate chunks and the
expected 16,384 decoded inputs. It materializes no intermediate matrix;
subsecond/modifier and subquery paths use the same compensated reduction in
bounded Rust composition.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,164 KiB. Pinned oracle and real-extension regressions cover
open-left boundaries, empty windows, cancellation-prone finite input, finite
overflow, mixed infinities, NaN, both zero orders, subqueries, work limits,
cancellation of the shared range executor, shutdown, and cold reopen. The
public SQL recipe executes precision and overflow cases. No extension
signature, storage/frame format, batching, compression, index, rollup,
retention, transaction, or migration behavior changed.

## Session 6 `PQL-R05` range sample count

The checked-in
[`2026-08-04_session6_pql_r05.json`](evidence/2026-08-04_session6_pql_r05.json)
was captured from exact extension and server build
`d1db19386378637fcd31ded11fd1fc18d72c5373`. Both measured shapes use the
unchanged public packed count window.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, native `count_over_time(...[5m])` | 1 | 132 | 0.250 | 0.358 | 0.396 | 0 | 1 | 32 | 131 |
| 512 series × four steps, native `count_over_time(...[5m])` | 2,048 | 65,854 | 2.911 | 7.041 | 7.130 | 0 | 512 | 16,384 | 53,831 |

The wide result is exactly 2,048 sparse points from 512 candidate chunks and
16,384 decoded inputs, with no intermediate matrix. Its p95/p99 tail is
recorded honestly rather than smoothed away; storage work and response
cardinality are identical to the neighboring native range reductions.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,436 KiB. Pinned oracle and real-extension regressions cover exact
open-left windows, empty omission, subqueries, NaN, infinities, signed zeros,
work limits, cancellation of the shared range executor, shutdown, and cold
reopen. Direct SQL proves the public count form. No extension primitive or
storage, frame, batching, compression, index, rollup, retention, transaction,
or migration behavior changed.

## Session 6 `PQL-R08` range presence

The checked-in
[`2026-08-04_session6_pql_r08.json`](evidence/2026-08-04_session6_pql_r08.json)
was captured from exact extension and server build
`de35c3723efab2b2e285670fafe136ff8e706823`. Both measured shapes reuse the
public packed count window and map each returned non-empty window to `1` in
the Rust PromQL evaluator.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, native `present_over_time(...[5m])` | 1 | 131 | 0.319 | 0.742 | 0.854 | 0 | 1 | 32 | 131 |
| 512 series × four steps, native `present_over_time(...[5m])` | 2,048 | 63,806 | 2.946 | 3.109 | 4.217 | 0 | 512 | 16,384 | 53,831 |

The wide request returns exactly 2,048 sparse presence points from 512
candidate chunks and 16,384 decoded inputs. It materializes no intermediate
matrix. Subsecond/modifier and subquery paths use the same bounded public raw
composition and return `1` whenever their exact `(T-window,T]` slice is
non-empty.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,168 KiB. Pinned oracle and real-extension regressions cover exact
open-left boundaries, empty omission, subqueries, NaN, infinities, signed
zero, work limits, cancellation of the shared range executor, shutdown, and
cold reopen. The executable SQL recipe maps the existing public count window
with an ordinary `CAST(value > 0 AS REAL)`. No extension primitive or storage,
frame, batching, compression, index, rollup, retention, transaction, or
migration behavior changed, and this row produced no new storage finding.

## Session 6 `PQL-R09` range quantile

The checked-in
[`2026-08-04_session6_pql_r09.json`](evidence/2026-08-04_session6_pql_r09.json)
was captured from exact extension and server build
`a46c666b9795e2ec03e2afd7f9fa12b047b5ed5a`. Both shapes use the public
packed raw frame because the extension's fixed nearest-rank `pXX` statistic is
not PromQL's scalar-parameter linear interpolation.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | scalar intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `quantile_over_time(0.95, ...[5m])` | 1 | 148 | 0.363 | 0.608 | 0.693 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `quantile_over_time(0.95, ...[5m])` | 2,048 | 79,073 | 3.428 | 3.819 | 4.065 | 4 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw inputs, then
sorts each series/window in Rust to produce 2,048 results. The only additional
intermediate materialization is the scalar parameter on the four-point outer
grid. Sorting is bounded by the existing cumulative work limit and checked for
cancellation before collection and after interpolation.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 35,812 KiB. Pinned oracle and real-extension regressions cover scalar
expressions, exact open-left boundaries, subqueries, empty windows, raw-NaN
rank, infinities, stable signed-zero order, NaN/out-of-range parameters, exact
arity/type errors, limits, cancellation, shutdown, and cold reopen. The
finite-value SQL recipe executes through public raw rows. `QSF-045` records the
corrected shared signed-zero comparator and `QSF-046` records why the native
nearest-rank kernel is not used. No extension primitive or storage, frame,
batching, compression, index, rollup, retention, transaction, or migration
behavior changed.

## Session 6 `PQL-R10` population standard deviation over time

The checked-in
[`2026-08-04_session6_pql_r10.json`](evidence/2026-08-04_session6_pql_r10.json)
was captured from exact extension and server build
`079fb3fc1c18bcbbed8e3981b2578a926b92f941`. Both measured shapes use one
bounded public packed raw read followed by the same population Welford fold
already oracle-proven for cross-series aggregation.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `stddev_over_time(...[5m])` | 1 | 146 | 0.581 | 0.825 | 0.840 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `stddev_over_time(...[5m])` | 2,048 | 95,038 | 3.825 | 5.576 | 7.998 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 raw inputs and emits 2,048
population deviations without an intermediate matrix. Its p95/p99 tail is
recorded honestly rather than replaced with the smoother neighboring run;
cardinality and storage work are unchanged, and the bounded single-pass fold
does not justify a variance-specific extension primitive.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,444 KiB. Pinned oracle and real-extension regressions cover exact
open-left boundaries, population rather than sample deviation, singleton and
empty windows, wide magnitudes, NaN, infinities, both zero signs, subqueries,
invalid argument types, work limits, cancellation, shutdown, and cold reopen.
The finite-value recursive Welford SQL recipe executes through public raw
rows. No extension primitive or storage, frame, batching, compression, index,
rollup, retention, transaction, or migration behavior changed, and this row
produced no new storage finding.

## Session 6 `PQL-R11` population variance over time

The checked-in
[`2026-08-04_session6_pql_r11.json`](evidence/2026-08-04_session6_pql_r11.json)
was captured from exact extension and server build
`0dbbadc45d36181fe65423e2947969ecf2dc24e7`. Both measured shapes use one
bounded public packed raw read followed by the same population Welford state
as `stddev_over_time`, returning the variance without the final square root.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `stdvar_over_time(...[5m])` | 1 | 147 | 0.741 | 0.912 | 0.927 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `stdvar_over_time(...[5m])` | 2,048 | 88,894 | 3.601 | 3.887 | 5.165 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw inputs and
emits 2,048 population variances without an intermediate matrix. Its work and
allocation shape matches the neighboring deviation query, so no
variance-specific extension primitive is justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,048 KiB. No benchmark request required cancellation; the shared
range-executor cancellation regression covers cancellation and reader reuse.
Pinned oracle and real-extension regressions cover exact open-left boundaries,
population rather than sample variance, singleton and empty windows, wide
magnitudes, NaN, infinities, both zero signs, subqueries, invalid argument
types, work limits, shutdown, and cold reopen. The finite-value recursive
Welford SQL recipe executes both deviation and variance through public raw
rows. `QSF-047` records Prometheus's distinct scalar versus vector/matrix float
formatting and `QSF-048` records the corrected pre-existing ranking fixture.
No extension primitive or storage, frame, batching, compression, index,
rollup, retention, transaction, or migration behavior changed.

## Session 7 `PQL-R12` float-counter rate

The checked-in
[`2026-08-04_session7_pql_r12.json`](evidence/2026-08-04_session7_pql_r12.json)
was captured from exact extension and server build
`e0c615308cb7cd22ccd897746b00f19e6dac5b35`. Both measured shapes use one
bounded public packed raw read followed by the Rust PromQL counter-reset and
edge-extrapolation fold; the extension's mechanical `rate` kernel is not
relabeled as PromQL.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `rate(...[5m])` | 1 | 133 | 0.371 | 0.476 | 0.543 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `rate(...[5m])` | 2,048 | 68,956 | 3.520 | 3.957 | 5.648 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw counter
samples and emits 2,048 per-second rates without an intermediate matrix. The
fold is linear in each window and checks cancellation while collecting and
reset-correcting samples. Its measured work and allocation shape does not
justify a PromQL-specific extension primitive; direct SQLite/libSQL users have
the executable finite-counter SQL recipe instead.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 35,836 KiB. No benchmark request required cancellation; the shared
range-executor cancellation regression covers cancellation and reader reuse.
Pinned oracle and real-extension regressions cover ordinary counters, resets,
sparse edges, zero-point clamping, exact open-left boundaries, offset output
timestamps, subqueries, NaN, both infinities, singleton omission, invalid
types, work limits, shutdown, and cold reopen. `QSF-049` records the correction
of stale unsupported-function error fixtures. No extension signature, storage/frame
format, batching, compression, index, rollup, retention, transaction, or
migration behavior changed.

## Session 7 `PQL-R13` instantaneous float-counter rate

The checked-in
[`2026-08-04_session7_pql_r13.json`](evidence/2026-08-04_session7_pql_r13.json)
was captured from exact extension and server build
`37a8661f199ac496f18759488eb5902babb8c16c`. Both measured shapes use one
bounded public packed raw read followed by the Rust PromQL final-pair reset and
actual-interval fold.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `irate(...[5m])` | 1 | 133 | 0.601 | 0.903 | 0.907 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `irate(...[5m])` | 2,048 | 67,902 | 3.367 | 3.900 | 4.239 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw samples and
emits 2,048 instantaneous rates without an intermediate matrix. Although only
the final pair contributes to each answer, the existing public packed raw
surface must return each bounded input. At 3.900 ms p95 for the wide shape,
that measured crossing cost does not justify a last-pair-specific extension
primitive; direct SQLite/libSQL users have the executable finite-counter SQL
recipe.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,232 KiB. No benchmark request required cancellation; the shared
range-executor cancellation regression covers cancellation and reader reuse.
Pinned oracle and real-extension regressions cover ordinary counters, reset
substitution, sparse intervals, exact open-left boundaries, offset output
timestamps, subqueries, NaN, both infinities, singleton and zero-interval
omission, invalid types, work limits, shutdown, and cold reopen. No extension
signature, storage/frame format, batching, compression, index, rollup,
retention, transaction, or migration behavior changed, and this row produced
no new storage finding.

## Session 7 `PQL-R14` extrapolated float-counter increase

The checked-in
[`2026-08-04_session7_pql_r14.json`](evidence/2026-08-04_session7_pql_r14.json)
was captured from exact extension and server build
`548bd73f865bd45d56b8f53c514476cfd5b39af3`. Both measured shapes use one
bounded public packed raw read followed by the Rust PromQL reset and edge
extrapolation fold without `rate`'s per-second normalization.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `increase(...[5m])` | 1 | 148 | 0.548 | 0.858 | 0.878 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `increase(...[5m])` | 2,048 | 91,436 | 4.040 | 4.280 | 4.503 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw counter
samples and emits 2,048 extrapolated increases without an intermediate matrix.
Its 22,480 additional response bytes versus the same-run `rate` shape are the
longer unnormalized result strings, not extra storage work. The existing public
packed raw surface plus executable finite-counter SQL recipe remains the right
boundary; the extension's mechanical `increase` kernel is not PromQL.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,036 KiB. No benchmark request required cancellation; the shared
range-executor cancellation regression covers cancellation and reader reuse.
Pinned oracle and real-extension regressions cover ordinary counters, resets,
sparse edges, zero-point clamping, exact open-left boundaries, offset output
timestamps, subqueries, NaN, both infinities, singleton omission, invalid
types, work limits, shutdown, and cold reopen. `QSF-050` records the separately
scoped Prometheus info-annotation gap. No extension signature, storage/frame
format, batching, compression, index, rollup, retention, transaction, or
migration behavior changed.

## Session 7 `PQL-R15` extrapolated float-gauge delta

The checked-in
[`2026-08-04_session7_pql_r15.json`](evidence/2026-08-04_session7_pql_r15.json)
was captured from exact extension and server build
`0f8394e823fb2ddf9c2318ac3c55a89c8473668a`. Both measured shapes use one
bounded public packed raw read followed by the Rust PromQL gauge-difference
and edge-extrapolation fold, with no counter reset or zero-point correction.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `delta(...[5m])` | 1 | 148 | 0.477 | 0.816 | 0.882 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `delta(...[5m])` | 2,048 | 91,454 | 4.063 | 4.318 | 4.490 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw gauge samples
and emits 2,048 extrapolated deltas without an intermediate matrix. Its storage
work and response size are effectively identical to `increase`; the semantic
distinction is the Rust fold preserving decreases rather than treating them as
counter resets. The measured shape does not justify a PromQL-specific
extension primitive, and direct SQLite/libSQL users have the executable
finite-gauge SQL recipe.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,432 KiB. No benchmark request required cancellation; the shared
range-executor cancellation regression covers cancellation and reader reuse.
Pinned oracle and real-extension regressions cover increases, decreases,
sparse edges, absence of zero clamping, exact open-left boundaries, offset
output timestamps, subqueries, NaN, both infinities, singleton omission,
invalid types, work limits, shutdown, and cold reopen. No extension signature,
storage/frame format, batching, compression, index, rollup, retention,
transaction, or migration behavior changed, and this row produced no new
storage finding.

## Session 7 `PQL-R16` instantaneous float-gauge delta

The checked-in
[`2026-08-04_session7_pql_r16.json`](evidence/2026-08-04_session7_pql_r16.json)
was captured from exact extension and server build
`0070c4b3b23270a9e68547de20592eac002ad978`. Both measured shapes use one
bounded public packed raw read followed by the Rust PromQL final-pair gauge
difference, without edge extrapolation or time normalization.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `idelta(...[5m])` | 1 | 131 | 0.389 | 0.542 | 0.591 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `idelta(...[5m])` | 2,048 | 63,806 | 3.521 | 3.919 | 4.024 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw gauge samples
and emits 2,048 final-pair differences without an intermediate matrix. Its
smaller response reflects compact non-extrapolated values; storage work is the
same as the neighboring counter/gauge rows. At 3.919 ms p95, the existing
public packed raw surface plus executable SQL recipe remains preferable to a
last-pair-specific extension primitive.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,264 KiB. No benchmark request required cancellation; the shared
range-executor cancellation regression covers cancellation and reader reuse.
Pinned oracle and real-extension regressions cover positive and negative
changes, sparse pairs, exact open-left boundaries, offset output timestamps,
subqueries, NaN, both infinities, singleton and zero-interval omission,
invalid types, work limits, shutdown, and cold reopen. No extension signature,
storage/frame format, batching, compression, index, rollup, retention,
transaction, or migration behavior changed, and this row produced no new
storage finding.

## Session 7 `PQL-R17` float-gauge linear derivative

The checked-in
[`2026-08-04_session7_pql_r17.json`](evidence/2026-08-04_session7_pql_r17.json)
was captured from exact extension and server build
`75ecce62df1b6aa4999f76003d3664a41dd67d7a`. Both measured shapes use one
bounded public packed raw read followed by the Rust PromQL timestamp-centered,
compensated least-squares fold.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `deriv(...[5m])` | 1 | 133 | 0.622 | 0.913 | 1.056 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `deriv(...[5m])` | 2,048 | 67,902 | 3.812 | 4.014 | 5.201 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly 16,384 bounded raw gauge samples
and emits 2,048 slopes without an intermediate matrix. Centering on each
window's first millisecond timestamp avoids absolute-epoch precision loss;
four compensated sums preserve the pinned Prometheus arithmetic. At 4.014 ms
p95, the existing public packed raw surface plus executable finite-value SQL
recipe remains preferable to a regression-specific extension primitive.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 35,780 KiB. No benchmark request required cancellation; the focused
fold regression and shared range-executor regression cover cancellation and
reader reuse. Pinned oracle and real-extension regressions cover linear and
nonlinear slopes at large epoch timestamps, sparse and constant series, exact
open-left boundaries, offset output timestamps, subqueries, NaN, mixed and
constant infinities, singleton omission, invalid types, work limits,
shutdown, and cold reopen. No extension signature, storage/frame format,
batching, compression, index, rollup, retention, transaction, or migration
behavior changed, and this row produced no new storage finding.

## Session 7 `PQL-R18` float-gauge linear forecast

The checked-in
[`2026-08-04_session7_pql_r18.json`](evidence/2026-08-04_session7_pql_r18.json)
was captured from exact extension and server build
`84c6c4a00fbbb197d54a295b35a6d3e9d1452829`. Both measured shapes evaluate
the scalar horizon on the outer grid, perform one bounded public packed raw
read, and apply the Rust PromQL evaluation-time-centered compensated
least-squares forecast.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `predict_linear(...[5m], 60)` | 1 | 132 | 0.537 | 0.821 | 0.956 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `predict_linear(...[5m], 60)` | 2,048 | 67,644 | 3.790 | 4.336 | 4.442 | 4 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes exactly the same 16,384 bounded raw gauge
samples as `deriv`, emits 2,048 forecasts, and materializes only the four
scalar horizon values—not a series matrix. The 4.336 ms p95 and identical
storage work support retaining public packed raw plus the executable finite
SQL recipe rather than adding a forecast-specific extension primitive.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,748 KiB. No benchmark request required cancellation; the shared
range-executor and regression-fold tests cover cancellation and reader reuse.
Pinned oracle and real-extension regressions cover positive, zero, negative,
expression, NaN, and infinite horizons; linear, nonlinear, constant, and
constant-infinite inputs; exact modifier/evaluation-time anchoring;
subqueries; singleton omission; invalid arity/types; limits; shutdown; and
cold reopen. No extension signature, storage/frame format, batching,
compression, index, rollup, retention, transaction, or migration behavior
changed, and this row produced no new storage finding.

## Session 7 `PQL-R19` float transition count

The checked-in
[`2026-08-04_session7_pql_r19.json`](evidence/2026-08-04_session7_pql_r19.json)
was captured from exact extension and server build
`6b9399c587fad0bf00e289a7d897960494230e90`. Both measured shapes use one
bounded public packed raw read and scan the borrowed decoded values once in
Rust with Prometheus's repeated-NaN equality rule.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `changes(...[5m])` | 1 | 132 | 0.469 | 0.860 | 0.901 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `changes(...[5m])` | 2,048 | 65,854 | 3.512 | 3.862 | 4.201 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes 16,384 bounded raw samples, emits 2,048
transition counts, and creates neither an intermediate matrix nor a temporary
value vector. At 3.862 ms p95, the public packed raw frame plus the exact
ordinary-SQL `LAG` recipe remains the appropriate direct-user boundary; a
transition-specific extension primitive would not avoid material work.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,312 KiB. No benchmark request required cancellation; focused and
shared range-executor regressions cover cancellation and reader reuse. Pinned
oracle and real-extension tests cover every transition, repeated values,
constant and singleton series, repeated NaNs, NaN-to-number, infinities,
signed-zero equality, exact open-left boundaries, offset output timestamps,
subqueries, invalid types, limits, shutdown, and cold reopen. No extension
signature, storage/frame format, batching, compression, index, rollup,
retention, transaction, or migration behavior changed, and this row produced
no new storage finding.

## Session 7 `PQL-R20` float-counter reset count

The checked-in
[`2026-08-04_session7_pql_r20.json`](evidence/2026-08-04_session7_pql_r20.json)
was captured from exact extension and server build
`51b216f804796508997ef25cee52c1bf18a8f2e7`. Both measured shapes use one
bounded public packed raw read and scan the borrowed decoded values once in
Rust, counting only strict IEEE float decreases.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `resets(...[5m])` | 1 | 131 | 0.394 | 0.535 | 0.635 | 0 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `resets(...[5m])` | 2,048 | 63,806 | 3.435 | 4.129 | 4.995 | 0 | 16,384 | 512 | 16,384 | 53,831 |

The wide request returns and decodes 16,384 bounded raw samples, emits 2,048
reset counts, and creates neither an intermediate matrix nor a temporary value
vector. At 4.129 ms p95, the public packed raw frame plus the exact ordinary
SQL `LAG` recipe remains preferable to a reset-count extension primitive. The
extension's mechanical reset-adjusted increase/rate folds remain distinct.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 36,076 KiB. No benchmark request required cancellation; focused and
shared range-executor regressions cover cancellation and reader reuse. Pinned
oracle and real-extension tests cover monotonic and decreasing counters,
repeated values, singleton zero, NaN and signed-zero non-resets, both infinity
directions, exact open-left boundaries, offset output timestamps, subqueries,
invalid types, limits, shutdown, and cold reopen. No extension signature,
storage/frame format, batching, compression, index, rollup, retention,
transaction, or migration behavior changed, and this row produced no new
storage finding.

## Session 8 `PQL-F01` bounded absolute-value transform

The checked-in
[`2026-08-04_session8_pql_f01.json`](evidence/2026-08-04_session8_pql_f01.json)
was captured from exact extension and server build
`7b3fc07cd4a70228668dfd0b5c411384153cbe99`. Both shapes perform one bounded
public packed-raw read, materialize only the already selected vector points,
and apply IEEE absolute value in place in the Rust PromQL evaluator.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `abs(metric)` | 1 | 132 | 0.673 | 0.938 | 1.087 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `abs(metric)` | 2,048 | 67,620 | 4.111 | 4.604 | 4.726 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The wide request has the same candidate chunks, decoded samples, returned raw
points, and intermediate cardinality as the same-run unary-vector bridge. Its
4.604 ms p95 is within 0.186 ms of unary minus's 4.418 ms p95. The only
additional operation is an in-place `f64::abs` per selected point, so neither
storage pushdown nor a new extension primitive would avoid material work.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 37,168 KiB. No benchmark request required cancellation; the shared
composed-evaluator cancellation regression and the focused bounded-limit test
cover cancellation and reader reuse. Pinned oracle and real-extension tests
cover positive and negative finite values, NaN, both infinities, negative
zero, metric-name removal, range grids, nested subquery composition, invalid
types, limits, shutdown, and cold reopen. `QSF-051` records that SQLite's
built-in `abs(-0.0)` retains the negative-zero bits; executable
`SQL-PROM-038` explicitly normalizes zero and otherwise uses only public
extension surfaces. No extension signature, storage/frame format, batching,
compression, index, rollup, retention, transaction, or migration behavior
changed.

## Session 8 `PQL-F02` rounding transforms

The checked-in
[`2026-08-04_session8_pql_f02.json`](evidence/2026-08-04_session8_pql_f02.json)
was captured from exact extension and server build
`155324440a9d0200470073486cc80adbf4b47421`. The measured `round` shapes use
one bounded public packed-raw read, evaluate the scalar nearest-multiple on the
outer grid, and transform the selected vector in place. They are the most
expensive member of the row; `ceil` and `floor` omit the scalar child.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `round(metric, 0.5)` | 1 | 132 | 0.332 | 0.471 | 0.583 | 2 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `round(metric, 0.5)` | 2,048 | 67,620 | 4.473 | 4.742 | 4.794 | 2,052 | 16,384 | 512 | 16,384 | 53,831 |

The parameterized wide request adds exactly four scalar grid points to the
same 2,048-point bounded vector bridge used by `abs`; it does not add a
storage read. Candidate chunks, decoded samples, returned raw samples, packed
payload bytes, and final response bytes are unchanged. At 4.742 ms p95, the
public packed frame plus ordinary in-place Rust arithmetic remains the correct
boundary; moving scalar rounding into the extension would not avoid decode or
cross-boundary work for direct SQL users, who already have executable
`SQL-PROM-039`.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage remained 672,688 bytes and whole-process RSS
HWM was 35,804 KiB. No benchmark request required cancellation; shared
composed-evaluator cancellation plus focused work-limit coverage pins reader
reuse. Pinned oracle and real-extension tests cover `ceil`, `floor`, default
and parameterized `round`, positive and negative values, upward ties,
negative zero, NaN and infinities, zero/signed-zero/NaN/infinite/negative
steps, scalar expressions, range grids, nested transforms, invalid types,
limits, shutdown, and cold reopen. No new storage finding arose, and no
extension signature, storage/frame format, batching, compression, index,
rollup, retention, transaction, or migration behavior changed.

## Session 8 `PQL-F03` clamp transforms

The checked-in
[`2026-08-04_session8_pql_f03.json`](evidence/2026-08-04_session8_pql_f03.json)
was captured from exact extension and server build
`0e3af9fae99040cd959de1733f846cf72ed6ea6c`. The measured `clamp` shapes use
one bounded public packed-raw read, evaluate both scalar bounds on the outer
grid, and compact the selected vector in place. They are the most expensive
member of the row; `clamp_min` and `clamp_max` evaluate only one scalar child.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `clamp(metric, 0, 10000)` | 1 | 132 | 0.393 | 0.549 | 0.613 | 3 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `clamp(metric, 0, 10000)` | 2,048 | 67,620 | 4.413 | 4.800 | 4.824 | 2,056 | 16,384 | 512 | 16,384 | 53,831 |

The parameterized wide request adds exactly eight scalar grid points—two for
each of four outer timestamps—to the same 2,048-point bounded vector bridge
used by the other transforms. It does not add a storage read. Candidate
chunks, decoded samples, returned raw samples, packed payload bytes, and final
response bytes remain identical to the `abs`/`round` wide shapes. Stable
in-place compaction means inverted bounds do not allocate a second result
vector. At 4.800 ms p95, moving clamp into the extension would not avoid the
already-required vector decode or boundary crossing; direct SQL users have
the executable finite-value `SQL-PROM-040` foundation.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 36,508 KiB. No benchmark request required cancellation; shared composed-
evaluator cancellation plus focused work-limit coverage pins reader reuse.
Pinned oracle and real-extension tests cover scalar bounds and expressions,
finite values, NaN, infinities, both signed-zero choices, inverted bounds,
range grids, nested transforms, invalid types, limits, shutdown, and cold
reopen. No new storage finding arose, and no extension signature,
storage/frame format, batching, compression, index, rollup, retention,
transaction, or migration behavior changed.

## Session 8 `PQL-F04` square-root, exponential, and logarithm transforms

The checked-in
[`2026-08-04_session8_pql_f04.json`](evidence/2026-08-04_session8_pql_f04.json)
was captured from exact extension and server build
`267082269d8552456913e3f7985b0579c073596c`. The measured `ln` shapes use one
bounded public packed-raw read and apply the transform in place. They exercise
the same plan and memory shape as `sqrt`, `exp`, `log2`, and `log10`; the
larger response reflects non-integral logarithm strings rather than extra
storage work.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `ln(metric)` | 1 | 148 | 0.417 | 0.637 | 0.753 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `ln(metric)` | 2,048 | 96,662 | 4.744 | 4.965 | 5.119 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The transform adds no scalar child, second storage read, or result copy.
Candidate chunks, decoded samples, returned raw samples, and packed extension
payload bytes match the other transform rows. The wide response is 29,042
bytes larger than the integral-valued clamp fixture because it contains the
actual logarithm decimals. At 4.965 ms p95, ordinary in-place Rust composition
remains the correct PromQL boundary; direct SQLite/libSQL users already have
the standard math functions documented by `SQL-PROM-041`.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 37,600 KiB. No benchmark request required cancellation; the shared
composed-evaluator cancellation contract pins reader reuse. Pinned oracle and
real-extension tests cover all five functions, valid and invalid domains,
NaN, infinities, signed zero, range grids, nested transforms, invalid types,
limits, shutdown, and cold reopen. SQLite's documented SQL-NULL domain result
is an API/SQL representation boundary, not a storage defect; no new storage
finding arose and no extension or storage contract changed.

## Session 8 `PQL-F05` sign transform

The checked-in
[`2026-08-04_session8_pql_f05.json`](evidence/2026-08-04_session8_pql_f05.json)
was captured from exact extension and server build
`9ec6e6bbb670b952e8908c5a4c13a233b13c1d92`. Both measured shapes perform one
bounded public packed-raw read and map each selected float to its unit sign in
place, preserving NaN and negative zero.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `sgn(metric)` | 1 | 131 | 0.669 | 0.860 | 0.997 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `sgn(metric)` | 2,048 | 63,806 | 4.218 | 4.602 | 4.708 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The transform adds no scalar child, second read, or result copy. Its wide
storage counters are identical to the other single-vector transforms; the
smaller response contains only `-1`, `0`, or `1` sample strings. At 4.602 ms
p95, ordinary Rust composition remains the correct PromQL boundary, while
direct SQLite/libSQL users have the exact row-visible `CASE` recipe in
`SQL-PROM-042`.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 37,384 KiB. No benchmark request required cancellation; shared composed-
evaluator cancellation pins reader reuse. Pinned oracle and real-extension
tests cover finite values, NaN, infinities, both signed zeros, range grids,
nested transforms, invalid types, limits, shutdown, and cold reopen. The SQL
row surface's NaN-to-NULL representation remains the documented API boundary;
no new storage finding arose and no extension or storage contract changed.

## Session 8 `PQL-F06` inverse trigonometric and hyperbolic transforms

The checked-in
[`2026-08-04_session8_pql_f06.json`](evidence/2026-08-04_session8_pql_f06.json)
was captured from exact extension and server build
`afa0ac285484e814d8ffe2300cdb746b48261909`. The measured `atan` shapes use
one bounded public packed-raw read and apply a domain-total member of the
family in place; `acos`, `acosh`, `asin`, `asinh`, and `atanh` use the same
plan and allocation shape.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `atan(metric)` | 1 | 148 | 0.433 | 0.586 | 0.616 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `atan(metric)` | 2,048 | 98,193 | 4.394 | 6.399 | 11.227 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The transform adds no scalar child, second read, or result copy. Wide storage
work is identical to neighboring single-vector transforms. `QSF-052`
preserves the run's noisier p95/p99 rather than rerunning it away: its 4.394 ms
median is consistent with the family, and the counters show no storage or
allocation amplification. Direct SQLite/libSQL users already have the
standard valid-domain functions in `SQL-PROM-043`.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 36,680 KiB. No benchmark request required cancellation; shared composed-
evaluator cancellation pins reader reuse. Pinned oracle and real-extension
tests cover all six functions, valid and invalid domains, endpoint
infinities, NaN, signed zero, range grids, nesting, invalid types, limits,
shutdown, and cold reopen. No extension or storage contract changed.

## Session 8 `PQL-F07` trigonometric and hyperbolic transforms

The checked-in
[`2026-08-04_session8_pql_f07.json`](evidence/2026-08-04_session8_pql_f07.json)
was captured from exact extension and server build
`0c102531f0b5386404c51cef6d503d6a7612f515`. The measured `sin` shapes use
one bounded public packed-raw read and apply the transform in place. `cos`,
`cosh`, `sinh`, `tan`, and `tanh` use the same plan and allocation shape.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `sin(metric)` | 1 | 148 | 0.690 | 1.015 | 1.030 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `sin(metric)` | 2,048 | 99,974 | 5.424 | 7.820 | 8.875 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The transform adds no scalar child, second read, or result copy. `QSF-052`
tracks the measured transcendental-family tails: storage work is identical to
neighboring single-vector transforms, while the API computes and serializes
2,048 nontrivial decimal results. Direct SQLite/libSQL users have the same
standard valid-domain functions through `SQL-PROM-044`; pushing them into a
new extension primitive would not avoid the selected-vector decode or final
response work.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 37,524 KiB. No benchmark request required cancellation; shared composed-
evaluator cancellation pins reader reuse. Pinned oracle and real-extension
tests cover all six functions, finite values, trigonometric infinity NaNs,
hyperbolic infinities, signed zero, range grids, nesting, invalid types,
limits, shutdown, and cold reopen. No extension or storage contract changed.

## Session 8 `PQL-F08` angle transforms and scalar pi

The checked-in
[`2026-08-04_session8_pql_f08.json`](evidence/2026-08-04_session8_pql_f08.json)
was captured from exact extension and server build
`a3c0d21f980bdb46ee2e0f08b9339499b3bd8487`. The measured `deg` shapes use
one bounded public packed-raw read and convert each selected float in place;
`rad` has the same plan and allocation shape, while `pi()` is a storage-free
scalar evaluated once per outer timestamp.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `deg(metric)` | 1 | 147 | 0.399 | 0.571 | 0.645 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, `deg(metric)` | 2,048 | 97,554 | 4.124 | 4.601 | 4.836 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The vector conversion adds no scalar child, second read, or result copy.
Candidate chunks, decoded and returned samples, and packed extension payload
bytes are identical to the neighboring single-vector transforms. At 4.601 ms
p95, ordinary in-place Rust composition remains the correct PromQL boundary;
direct SQLite/libSQL users already have standard `pi()` arithmetic through
executable `SQL-PROM-045`, so no extension primitive is justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 36,252 KiB. No benchmark request required cancellation; shared composed-
evaluator cancellation pins reader reuse. Pinned oracle and real-extension
tests cover conversion order, scalar instant and range types, finite values,
NaN, infinities, signed zero, range grids, nesting, invalid types, limits,
shutdown, and cold reopen. No new storage finding arose, and no extension or
storage contract changed.

## Session 8 `PQL-F09` label replacement

The checked-in
[`2026-08-04_session8_pql_f09.json`](evidence/2026-08-04_session8_pql_f09.json)
was captured from exact extension and server build
`356a4e1412a845adcbae2ddd7a1b8a17e4aaa202`. Both shapes perform one bounded
public packed-raw read, decode the selected vector once, and apply the
full-match capture expansion in place.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `label_replace(metric, "node", "$1", "host", "(.*)")` | 1 | 179 | 0.755 | 0.983 | 1.011 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, same replacement | 2,048 | 91,684 | 4.213 | 4.570 | 5.091 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The label operation adds no second read and no point amplification. Candidate
chunks, decoded and returned samples, and packed extension payload bytes are
identical to neighboring one-vector transforms; the larger response is the
expected repeated `node` label. At 4.570 ms wide p95, bounded Rust language
composition remains the correct boundary. SQLite has no portable
RE2-compatible capture-and-expand operation, and these counters provide no
evidence for adding a PromQL-specific extension primitive.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 36,300 KiB. No benchmark request required cancellation; the focused limit
and cancellation regressions pin reader reuse. Pinned oracle and real-
extension tests cover numbered and named captures, missing and empty sources,
unmatched identity, destination overwrite and deletion, metric names,
Prometheus 3 UTF-8 label names, full-match dot-all behavior, range grids,
nesting, errors, limits, shutdown, and cold reopen. `QSF-053` records the
upstream label-name scheme and honest lack of a complete SQL foundation;
`QSF-054` records and fixes standard exposition label escapes on an uncommon
allocation-only path. No extension signature, storage/frame format, batching,
compression, index, rollup, retention, transaction, or migration contract
changed.

## Session 8 `PQL-F10` label joining

The checked-in
[`2026-08-04_session8_pql_f10.json`](evidence/2026-08-04_session8_pql_f10.json)
was captured from exact extension and server build
`6f4bce7092d3fe7aa33b041d1c12dd0014ab344b`. Both shapes perform one bounded
public packed-raw read, decode the selected vector once, and join two ordered
source positions in place; the second source is deliberately missing and
therefore contributes an empty string.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, `label_join(metric, "node", "/", "host", "rack")` | 1 | 180 | 0.448 | 0.597 | 0.662 | 1 | 31 | 1 | 32 | 131 |
| 512 series × four steps, same join | 2,048 | 92,196 | 4.233 | 4.417 | 4.730 | 2,048 | 16,384 | 512 | 16,384 | 53,831 |

The label operation adds no second storage read, point amplification, or
result copy. Candidate chunks, decoded and returned samples, and packed
extension payload bytes exactly match neighboring one-vector transforms; the
response grows only by the expected repeated `node` label. At 4.417 ms wide
p95, ordinary bounded Rust composition remains the correct language boundary.
Direct SQLite/libSQL users have the arbitrary-arity public-JSON statement in
`SQL-PROM-046`, so no label-specific extension primitive is justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 37,100 KiB. No benchmark request required cancellation; focused work and
response-limit regressions pin rejection and reader reuse. `QSF-055` records
the incremental byte budget that now prevents repeated sources or captures
from accumulating beyond `max_response_bytes` before rejection. Pinned oracle
and real-extension tests cover arbitrary ordered arity, missing, explicit-
empty, duplicate, and zero sources, original-label snapshots, overwrite and
deletion, metric names, Prometheus 3 UTF-8 label names, range grids, nesting,
errors, limits, shutdown, and cold reopen. No extension signature,
storage/frame format, batching, compression, index, rollup, retention,
transaction, or migration contract changed.

## Session 8 `PQL-F11` absence detection

The checked-in
[`2026-08-04_session8_pql_f11.json`](evidence/2026-08-04_session8_pql_f11.json)
was captured from exact extension and server build
`9c912c834c071de8dd90a9b7ca2d61231102ccb8`. The narrow shape selects a
missing exact label value and returns one immediately derived sample; the wide
shape proves presence across 512 series and four steps, returning an empty
matrix only after accounting for the complete bounded child.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| missing exact host, instant `absent(metric{host="missing"})` | 1 | 117 | 0.359 | 0.494 | 0.523 | 1 | 0 | 0 | 0 | 0 |
| 512 present series × four steps, `absent(metric)` | 0 | 63 | 3.834 | 4.157 | 4.443 | 2,052 | 16,384 | 512 | 16,384 | 53,831 |

The selective missing-label case is resolved by the public catalog before any
raw chunk query, so it emits one result with zero candidate chunks, decoded
points, or extension payload bytes. The broad present case performs exactly
the same one packed read as neighboring wide vector operations, then charges
2,048 child points plus four inspected outer timestamps even though its final
cardinality is zero. Empty results therefore cannot evade cumulative work
limits. At 4.157 ms wide p95, no absence-specific extension primitive is
justified; direct users have the catalog-aware public-grid anti-join in
`SQL-PROM-047`.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 36,208 KiB. No benchmark request required cancellation; focused grid-work
coverage and the shared dropped-request regression pin cancellation and reader
reuse. Pinned oracle and real-extension tests cover present and absent vectors,
step-local sparse ranges, unique nonempty equality-label derivation,
metric-name, regex, negative, empty, duplicate, and composed-expression
exclusions, NaN presence, type errors, limits, shutdown, and cold reopen. No
new storage finding arose and no extension or storage contract changed.

## Session 8 `PQL-F12` window absence

The checked-in
[`2026-08-04_session8_pql_f12.json`](evidence/2026-08-04_session8_pql_f12.json)
was captured from exact extension and server build
`4d40628dfd21cc8d2bd7aeb918f12ba8e43c9eb9`. The narrow shape proves that a
missing exact label value produces one sample without decoding storage. The
wide shape proves presence across 512 series and four windows, returning an
empty matrix only after the complete bounded window reduction and absence
inversion.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | window results/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| missing exact host, instant `absent_over_time(metric{host="missing"}[30s])` | 1 | 117 | 0.602 | 0.853 | 0.988 | 1 | 0 | 0 | 0 | 0 |
| 512 present series × four steps, `absent_over_time(metric[30s])` | 0 | 63 | 3.833 | 4.118 | 5.595 | 2,052 | 2,048 | 512 | 16,384 | 53,831 |

The exact whole-second selector uses the existing packed public window path
with `present_over_time` semantics and then performs one step-local inversion
in Rust. The broad query therefore crosses the extension once, returns 2,048
mechanical presence results, charges those results plus all four inspected
outer timestamps, and emits no final samples. Empty output cannot evade work
accounting. Modified, subsecond, and subquery shapes retain the already-pinned
public raw fallback. At 4.118 ms wide p95, neither a PromQL parser nor an
absence-specific primitive belongs in the extension. Direct SQLite/libSQL
users have the executable public-raw anti-join in `SQL-PROM-048`.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 37,080 KiB. No benchmark request required cancellation; focused
cumulative-work coverage and the shared dropped-request regression pin
cancellation and reader reuse. Pinned oracle and real-extension tests cover
open-left/millisecond boundaries, present and missing windows, sparse range
steps, unique nonempty equality-label derivation and exclusions, NaN presence,
direct selectors, subqueries, type errors, limits, shutdown, and cold reopen.
The full workspace gate also exposed two shared bind-policy tests racing over
one process environment variable; serializing those mutations preserves their
existing product contract and pins reliable parallel CI. No storage, frame,
batching, compression, index, rollup, retention, transaction, migration, or
extension signature changed, and no new query-storage finding arose.

## Session 8 `PQL-F13` value sorting

The checked-in
[`2026-08-04_session8_pql_f13.json`](evidence/2026-08-04_session8_pql_f13.json)
was captured from exact extension and server build
`32d02ff31153c45354b0f85ae7d30831394cad10`. The narrow shape sorts one exact
series; the wide shape performs an actual descending instant sort over all 512
selected series.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, instant `sort(metric{host="h0000"})` | 1 | 164 | 0.426 | 0.534 | 0.581 | 1 | 31 | 1 | 32 | 131 |
| 512 series, instant `sort_desc(metric)` | 512 | 53,497 | 3.709 | 3.975 | 5.135 | 512 | 15,872 | 512 | 16,384 | 53,831 |

Both shapes read the same bounded packed selector frames as neighboring
one-vector functions. Sorting changes only the order in which already-decoded
instant samples are written; it adds no storage read, result point, frame
copy, or label mutation. NaN is last in both directions, infinities retain
numeric order, and the packed frame preserves signed zero. Equal values have
no PromQL order promise, so canonical labels provide deterministic output.
Pinned Prometheus also confirms that a range endpoint returns a label-ordered
matrix rather than applying one evaluation step's ordering to whole series.
At 3.975 ms wide p95, ordinary bounded Rust composition is the correct
language boundary. Direct SQLite/libSQL users have the executable instant and
range statements in `SQL-PROM-049`; no sort-specific extension primitive is
justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 35,656 KiB. No benchmark request required cancellation; the focused work-
limit regression and shared dropped-request regression pin bounded execution,
cancellation, and reader reuse. Pinned oracle and real-extension tests cover
ascending/descending IEEE order, NaN-last behavior, negative zero, preserved
metric names and child label policy, nested and empty vectors, range-matrix
ordering, type errors, limits, shutdown, and cold reopen. No storage, frame,
batching, compression, index, rollup, retention, transaction, migration, or
extension contract changed, and no new query-storage finding arose.

## Session 8 `PQL-F15` scalar/vector conversion

The checked-in
[`2026-08-04_session8_pql_f15.json`](evidence/2026-08-04_session8_pql_f15.json)
was captured from exact extension and server build
`1ee2a786284951b5425df4752a81e0d1175ae056`. The narrow shape converts one
selected sample to a scalar; the wide shape reads 512 series and proves that
multiple samples at the evaluation step produce one scalar NaN rather than an
empty result.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, instant `scalar(metric{host="h0000"})` | 1 | 78 | 0.384 | 0.505 | 0.684 | 2 | 31 | 1 | 32 | 131 |
| 512 series, instant `scalar(metric)` | 1 | 79 | 3.468 | 3.585 | 3.591 | 513 | 15,872 | 512 | 16,384 | 53,831 |

The conversion adds no storage read or frame copy. It charges every child
sample plus the inspected outer step, then returns the exact sole value or
NaN for zero/multiple cardinality. The companion `vector(scalar)` conversion
reads no storage beyond its scalar child and attaches one empty label set.
Both preserve Prometheus's distinct scalar/vector instant and range result
types. At 3.585 ms wide p95, ordinary Rust cardinality composition over one
bounded packed read remains the correct boundary. Direct SQLite/libSQL users
have the executable per-step cardinality and nameless-vector statements in
`SQL-PROM-050`; no conversion primitive is justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 36,932 KiB. No benchmark request required cancellation; focused empty-
input grid-work coverage and the shared dropped-request regression pin limits,
cancellation, and reader reuse. Pinned oracle and real-extension tests cover
zero/one/multiple series, stored/result NaN, per-step range conversion,
nameless vectors, nested conversions, type errors, limits, shutdown, and cold
reopen. No storage, frame, batching, compression, index, rollup, retention,
transaction, migration, or extension contract changed, and no new query-
storage finding arose.

## Session 8 `PQL-F16` evaluation and sample time

The checked-in
[`2026-08-04_session8_pql_f16.json`](evidence/2026-08-04_session8_pql_f16.json)
was captured from exact extension and server build
`59dc5d976ac98c283e4935753a8ea2a4e55f423f`. Both measured shapes execute
`timestamp` directly over a selector, where correctness requires the selected
stored sample timestamp rather than the outer response timestamp.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, instant `timestamp(metric{host="h0000"})` | 1 | 140 | 0.363 | 0.523 | 0.654 | 1 | 31 | 1 | 32 | 131 |
| 512 series, instant `timestamp(metric)` | 512 | 40,766 | 3.033 | 3.208 | 3.678 | 512 | 15,872 | 512 | 16,384 | 53,831 |

The direct timestamp plan reuses the normal bounded packed selector scan and
decodes the chosen raw timestamp in place. It does not perform a second read,
decode more samples, copy a frame, or change result cardinality. `time()`
requires no storage read. For composed vector expressions, `timestamp` maps
the already-bounded result to the outer evaluation clock, matching the oracle
without pretending the normalized HTTP timestamp retained selector
provenance. At 3.208 ms wide p95, the public raw waist and Rust planner are the
correct boundary. Direct SQLite/libSQL users have the executable provenance
join in `SQL-PROM-051`; no timestamp-specific extension primitive is
justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 37,580 KiB. No benchmark request required cancellation; focused storage-
work rejection and the shared dropped-request regression pin limits,
cancellation, and reader reuse. Pinned oracle and real-extension tests cover
millisecond evaluation clocks, direct selector provenance through lookback,
`offset`, and `@`, normalized response timestamps, unary/value/label/sort/
binary/aggregate/range-function creation time, metric-name removal, labels,
stored NaN, instant and range results, errors, shutdown, and cold reopen.
`QSF-056` records the provenance boundary. No storage, frame, batching,
compression, index, rollup, retention, transaction, migration, or extension
contract changed.

## Session 8 `PQL-F17` UTC calendar extraction, part one

The checked-in
[`2026-08-04_session8_pql_f17.json`](evidence/2026-08-04_session8_pql_f17.json)
was captured from exact extension and server build
`9867a85aa406c759c9c5f8edf804650de94dd6ea`. The narrow shape extracts the
minute from one selected series; the wide shape extracts Sunday-zero weekday
values from 512 selected series.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, instant `minute(metric{host="h0000"})` | 1 | 131 | 0.368 | 0.538 | 0.588 | 1 | 31 | 1 | 32 | 131 |
| 512 series, instant `day_of_week(metric)` | 512 | 36,158 | 3.942 | 4.260 | 4.413 | 512 | 15,872 | 512 | 16,384 | 53,831 |

The calendar plan transforms the already-bounded packed vector once, removes
only metric names, and preserves cardinality and all other labels. Optional
zero-argument forms compose `vector(time())` without storage. The wide shape
retains exactly the selector's 512 candidate chunks, 16,384 decoded points,
and 53,831 packed bytes; there is no second read or calendar-specific storage
amplification. At 4.260 ms wide p95, API-local composition remains the correct
boundary. Direct SQLite/libSQL users have the executable finite-calendar
foundation in `SQL-PROM-052`; no extension primitive is justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 37,224 KiB. No benchmark request required cancellation; focused work-
limit and shared dropped-request regressions pin bounded execution,
cancellation, and reader reuse. Pinned oracle and real-extension tests cover
UTC minute/hour/weekday/month-day values, optional evaluation-time defaults,
Sunday-zero numbering, positive and negative fractional truncation, NaN,
infinities, both overflow directions and the minimum-int64 float boundary,
labels/names, nested ranges, errors, shutdown, and cold reopen. `QSF-057`
records the cross-language overflow boundary. No extension, frame, batching,
compression, index, rollup, retention, transaction, migration, or storage
contract changed.

## Session 8 `PQL-F18` UTC calendar extraction, part two

The checked-in
[`2026-08-04_session8_pql_f18.json`](evidence/2026-08-04_session8_pql_f18.json)
was captured from exact extension and server build
`67c0ffe0d6caea8d74fba829f46fdd683ec88527`. The narrow shape extracts the
year from one selected series; the wide shape extracts one-indexed day-of-year
values from 512 selected series.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | raw points returned/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, instant `year(metric{host="h0000"})` | 1 | 134 | 0.447 | 0.727 | 0.983 | 1 | 31 | 1 | 32 | 131 |
| 512 series, instant `day_of_year(metric)` | 512 | 36,158 | 4.036 | 4.451 | 5.064 | 512 | 15,872 | 512 | 16,384 | 53,831 |

These four functions reuse the same bounded calendar plan as F17. Gregorian
leap-year and civil-date arithmetic run over the already-decoded values; the
wide shape has exactly the selector's candidate chunks, decoded points,
returned points, and packed bytes. There is no second read, new frame, or
storage amplification. At 4.451 ms wide p95, API-local composition remains
the correct boundary. Direct SQLite/libSQL users have the executable ordinary-
SQL and leap-year foundation in `SQL-PROM-053`; no extension primitive is
justified.

All 18,496 fixture points completed durably with zero failed or queued work;
physical SQLite/WAL/SHM storage was 672,688 bytes and whole-process RSS HWM
was 35,928 KiB. No benchmark request required cancellation; focused work-
limit and shared dropped-request regressions pin bounded execution,
cancellation, and reader reuse. Pinned oracle and real-extension tests cover
one-indexed day-of-year and month, leap February length, UTC year, optional
evaluation-time defaults, fractional truncation, NaN/infinity/overflow
sentinels, labels/names, nested ranges, errors, shutdown, and cold reopen.
The previously recorded `QSF-057` conversion boundary applies unchanged. No
extension, frame, batching, compression, index, rollup, retention,
transaction, migration, or storage contract changed.

## Session 9 `PQL-H01` classic histogram quantiles

The checked-in
[`2026-08-04_session9_pql_h01.json`](evidence/2026-08-04_session9_pql_h01.json)
was captured from exact extension and server build
`97c90891592c17578c1cc9347e3b85c32b1b7a92`. The narrow shape evaluates four
bucket series for one classic histogram. The wide shape evaluates four bucket
series for each of 512 histograms.

| shape | bucket points read | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, instant `histogram_quantile(0.5, buckets{host="h0000"})` | 4 | 1 | 131 | 0.607 | 0.816 | 0.834 | 5 | 4 | 4 | 208 |
| all 512 histograms | 2,048 | 512 | 41,450 | 13.869 | 14.589 | 15.295 | 2,049 | 2,048 | 2,048 | 106,496 |

The API reads each selected ordinary float bucket exactly once through the
public packed raw surface, retains metric names only while separating bucket
families, then applies strict bound parsing, equal-bound coalescing, the
Prometheus `1e-12` relative tolerance, monotonic repair, interpolation, and
output collision checks in bounded Rust composition. The wide response
crosses 57,360 packed frame bytes and does no second storage read. At 14.589
ms wide p95, the work scales with the required 2,048 input series and 512
results; `QSF-058` records why a histogram-specific extension primitive is not
justified. Direct SQLite/libSQL users have the finite-data foundation in
`SQL-PROM-054`.

All 20,544 primary fixture points completed durably with zero failed or queued
work. The larger histogram fixture plus the existing work-limit fixture used
1,279,784 physical SQLite/WAL/SHM bytes and reached 45,244 KiB whole-process
RSS HWM. The focused real-extension regression and 485-case pinned Prometheus
run cover invalid quantiles, missing/malformed/infinite bounds, insufficient
buckets, zero and infinite totals, material decreases, tolerated deltas,
equal parsed bounds, NaN counts, aggregation/rate composition, output-family
collisions, range grids, types, arity, cumulative work limits, cancellation,
shutdown, and cold reopen. Native histogram behavior remains deferred pending
an explicit typed storage design. No extension, frame, batching, compression,
index, rollup, retention, transaction, migration, or storage format changed.

## Session 10 LogsQL P0 parity

The checked-in
[`2026-08-04_session10_logsql_p0.json`](evidence/2026-08-04_session10_logsql_p0.json)
was captured from exact extension, metrics-server, and logs-server build
`c70ae1f8466ae1c2b0a6a59b8cd8f7612ff513e4`. The log fixture contains 8,192
typed/nested rich entries across all eight severities and crosses the
extension's authoritative 8,192-entry buffer exactly once. All entries were
completed and durable with zero queued work before any query ran.

| shape | API result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate/decoded blocks per query | decoded entries per query | extension payload bytes per query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact `level:error service:api status:=500`, descending, offset 1, limit 100 | 100 | 17,067 | 6.273 | 9.734 | 9.995 | 1 / 1 | 4,096 | 546,985 |
| exact phrase `"query contract"`, ascending, limit 10,000 | 8,192 | 1,424,639 | 22.222 | 28.450 | 28.809 | 4 / 4 | 8,192 | 1,088,919 |
| `service:api | stats count() as total` | 1 | 15 | 7.366 | 8.986 | 9.412 | 2 / 2 | 5,120 | 679,661 |

The narrow storage predicate prunes to one coarse severity block. That block
contains 4,096 entries because physical severity partitioning is intentionally
coarser than the eight-value logical vocabulary; the public extension emits
1,024 level/service candidates, and bounded Rust composition applies exact
typed numeric equality, offset, and the 100-row result cap. The phrase query
must inspect all four blocks because every message matches and portable SQLite
has no equivalent Unicode word-boundary predicate. The service count prunes to
two mixed-service blocks and performs the necessary exact decode. These are the
retained write/compression tradeoffs already described by `QSF-005`, `QSF-006`,
and `QSF-009`; the evidence does not justify another storage index or a
LogsQL-specific extension primitive.

One ingestion request was admitted at 928,985 entries/s; the request plus
explicit durability barrier completed at 252,299 entries/s. Relative to the
original query baseline, this single run's admission duration was 23.5% higher
and its barrier duration 5.9% higher. Those are one-request latency samples,
not a stable ingestion-throughput regression, and query work did not change
the batching, block, codec, index, or flush path. The resulting aggregate
storage counters exactly match the baseline: 1,088,919 block-payload
bytes, 1,126,400 SQLite page bytes, 1,190,496 physical database/WAL/SHM bytes,
four raw blocks, zero compressed blocks, and a 16,384-byte index allocation.
Whole-process RSS HWM was 54,248 KiB, only 44 KiB above the baseline run.

No benchmark request required cancellation, so `api_query_cancelled` remained
zero and `api_query_in_flight` was zero at capture. The named real-extension
regression deliberately times out active work, observes the cancellation and
in-flight counters until the SQLite reader has actually stopped, then reuses
that reader successfully. The complete real-extension gate also covers work,
row, response, and deadline limits; strict malformed/unsupported JSON errors;
relative and absolute microsecond edges; all integer timestamp units; exact
quoted bytes and escapes; typed nested metadata; optimize; shutdown; and cold
reopen. The pinned VictoriaLogs 1.52.0 run passed all 25 applicable cases.
`QSF-064` records the additional public `timeless_stats('logs')` boundary that
removed every logs-server dependency on private shadow layout. No storage
format, authoritative batching, compression, index, retention, transaction,
migration, or maintenance command changed.

## Session 11 stable PromQL P1 completion

The checked-in
[`2026-08-04_session11_pql_p1.json`](evidence/2026-08-04_session11_pql_p1.json)
was captured from exact extension, metrics-server, and logs-server build
`350f50713a57ed79727c1db7a95a4ab1cea1c37b`. `PQL-O08` measures the new
Go-compatible `atan2` evaluator. `PQL-S23` measures one invalid-quantile
warning and a wide range `rate` query that emits one deduplicated
non-counter-name info annotation.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| exact host, instant `metric atan2 2` | 1 | 147 | 0.463 | 0.880 | 0.940 | 1 | 32 | 131 |
| 512 series, four-step `metric atan2 2` | 2,048 | 98,160 | 4.732 | 4.963 | 5.046 | 512 | 16,384 | 53,831 |
| exact host, invalid aggregate quantile warning | 1 | 191 | 0.612 | 0.682 | 0.855 | 1 | 32 | 131 |
| 512 series, four-step non-counter `rate` info | 2,048 | 69,090 | 3.928 | 4.274 | 4.723 | 512 | 16,384 | 53,831 |

The wide `atan2` shape performs one existing packed read and 2,052 bounded
API intermediate points per query: 2,048 vector points plus one scalar at
each of four steps. Its 4.963 ms p95 is effectively the same as the
same-run `sin` shape's 4.981 ms p95, with identical storage work. The exact
Prometheus last-bit behavior therefore remains API-local; neither another
read nor an extension primitive is justified.

The wide annotation shape has exactly the same query and storage work as the
pre-annotation Session 9 `range_rate_wide` shape. Adding the top-level info
increased the response by 134 bytes. This run's 4.274 ms p95 is 8.2% above
the earlier run's 3.949 ms p95, while candidate chunks, decoded points,
payload bytes, cardinality, and storage are unchanged. The identical query
also ran later in this same Session 11 process as `range_rate_wide` at 4.663
ms p95, demonstrating appreciable run-order noise. The conservative verdict
retains the observed cross-run increase and accepts the bounded parser,
deduplication, and serialization cost; there is no evidence of storage
amplification. The narrow warning shape is below the same-run valid-quantile
p95 despite its 89 additional response bytes, so no narrow slowdown is
claimed.

All 20,544 primary fixture points completed durably with zero failed or queued
work. Logical and physical storage are byte-for-byte unchanged from Session 9:
170,857 extension bytes, 2,651 chunks/series, 360,448 SQLite index bytes, and
1,279,784 database/WAL/SHM bytes. The one-request admission measurement was
8.331 ms versus 6.797 ms in Session 9, while the durability barrier was
68.010 ms versus 67.439 ms; no ingestion code changed, so these samples are
preserved without calling them a throughput regression. Whole-process RSS HWM
was 46,356 KiB, 1,112 KiB (2.46%) above Session 9 after the expanded query
suite.

The complete 496-case pinned Prometheus 3.13.2 oracle and all 75 metrics
real-extension tests passed. They include exact annotation text, type, source
positions, deterministic local deduplication, the ten-item cap and omission
summary, repair merge, GET/POST instant/range envelopes, response limits,
shutdown/reopen, and unaffected omission. `PQL-S17` remains deferred until a
bit-preserving stale-marker ingress and marker-aware selectors/windows exist.
`PQL-R21` is correctly classified experimental and is rejected by default,
matching the pinned oracle; neither disposition performs storage work. No
extension, storage format, batching, compression, index, rollup, retention,
transaction, migration, or public batch/SQL contract changed.

## Session 12 LogsQL P1 filters and logic

The checked-in
[`2026-08-04_session12_logsql_p1.json`](evidence/2026-08-04_session12_logsql_p1.json)
was captured from exact extension, metrics-server, and logs-server build
`647b43627d0da45083a7107d78e9848a3961fe6d`. It measures every shipped Session
12 filter in both an indexed-host narrow shape and a full 8,192-entry decoded
shape over the same typed rich-log fixture.

| filter | narrow rows | narrow p50/p95/p99 ms | wide rows | wide p50/p95/p99 ms |
|---|---:|---:|---:|---:|
| word | 128 | 2.070 / 2.275 / 2.384 | 8,192 | 22.832 / 25.029 / 27.446 |
| prefix | 128 | 2.274 / 2.759 / 3.230 | 8,192 | 21.962 / 25.446 / 26.965 |
| substring | 128 | 1.895 / 2.817 / 3.069 | 8,192 | 21.255 / 23.404 / 25.170 |
| regexp | 128 | 2.009 / 2.268 / 2.756 | 8,192 | 22.073 / 24.616 / 25.433 |
| case-insensitive | 128 | 2.159 / 2.420 / 2.638 | 8,192 | 21.249 / 22.949 / 28.368 |
| exact | 1 | 1.856 / 2.115 / 2.619 | 0 | 14.011 / 15.653 / 15.841 |
| empty | 128 | 2.178 / 2.903 / 3.071 | 8,192 | 24.725 / 28.732 / 29.427 |
| any value | 128 | 2.153 / 2.440 / 2.944 | 8,192 | 26.066 / 28.099 / 29.061 |
| numeric range | 51 | 1.970 / 2.228 / 2.644 | 3,276 | 20.470 / 21.671 / 21.892 |
| logical value type | 128 | 2.160 / 2.699 / 2.965 | 8,192 | 25.502 / 27.131 / 27.762 |
| boolean composition | 128 | 2.160 / 2.349 / 2.433 | 8,192 | 23.756 / 25.498 / 26.194 |

Every indexed-host query selects one candidate block and charges 1,024
decoded entries; every full query selects all four blocks and charges exactly
8,192. The dedicated logical-pushdown regression is more selective: its safe
`service:="api"` conjunct reduces an 8,192-entry expression to one candidate
block and exactly 410 decoded rows before evaluating the `OR` branch. No atom
below `OR` or `NOT` is pushed. Exact response cardinality and bytes, cumulative
candidate/decoded/matched/returned work, payload bytes, response bytes, and
p50/p95/p99 for all 22 shapes are retained in the JSON artifact.

All 8,192 entries completed durably with zero queued work. One-request
admission took 9.693ms and the explicit durability barrier took 20.411ms;
those are preserved as latency samples, not claimed as throughput changes.
Storage is byte-identical to Session 10: 1,088,919 logical block bytes,
1,126,400 SQLite page bytes, 1,190,496 physical database/WAL/SHM bytes, four
raw blocks, zero compressed blocks, and a 16,384-byte index. Whole-process RSS
HWM was 58,500KiB, 4,252KiB (7.84%) above Session 10 after 22 additional query
shapes; `QSF-075` records and accepts the bounded increase.

All 64 pinned VictoriaLogs 1.52.0 applicable cases, all 14 real-extension log
API/flush/optimize/reopen/backup tests, all 32 LogsQL/storage unit tests, and
all 64 executable SQL recipes pass. The findings pin upstream differences for
typed empty/any predicates, numeric-string coercion and integers beyond 2^53,
and physical versus logical `value_type`. Regex cancellation, the inclusive
work cap, parser ambiguities, and safe logical pushdown each have regressions.
No authoritative batching, block format, compression, index, retention,
transaction, migration, maintenance, or public extension contract changed.

## Session 13 LogsQL P1 discovery and statistics

The checked-in
[`2026-08-04_session13_logsql_p1_stats.json`](evidence/2026-08-04_session13_logsql_p1_stats.json)
was captured from exact extension and signal-server build
`3269a36c5654934cb9482144b6cb675bc29bdf14`. It measures all shipped Session
13 typed discovery, projection, ordered-filter, value-statistics, numeric, and
rate surfaces over the unchanged 8,192-entry rich-log fixture.

| operation | narrow rows | narrow p50/p95/p99 ms | wide rows | wide p50/p95/p99 ms |
|---|---:|---:|---:|---:|
| `field_values` | 5 | 2.047 / 2.291 / 2.390 | 5 | 21.048 / 23.127 / 25.583 |
| `field_names` | 7 | 2.229 / 2.406 / 2.453 | 7 | 22.685 / 25.886 / 25.962 |
| typed projection | 128 | 2.175 / 2.515 / 3.344 | 8,192 | 23.350 / 26.037 / 27.644 |
| ordered pipeline filter | 51 | 2.133 / 2.698 / 2.777 | 3,276 | 21.952 / 23.100 / 23.691 |
| field counts | 1 | 2.045 / 2.246 / 2.672 | 1 | 21.048 / 22.360 / 24.410 |
| unique counts | 1 | 2.144 / 2.411 / 2.470 | 1 | 23.422 / 25.970 / 26.601 |
| typed values | 1 | 2.203 / 2.356 / 2.472 | 1 | 22.213 / 31.108 / 38.778 |
| numeric aggregates | 1 | 2.243 / 4.007 / 4.182 | 1 | 24.194 / 25.228 / 26.380 |
| rates | 1 | 2.112 / 2.418 / 2.685 | 1 | 23.363 / 26.684 / 29.512 |

Every narrow query selects one candidate block, decodes 1,024 entries, and
reads 132,676 extension payload bytes. Every wide query selects all four
blocks, decodes 8,192 entries exactly once, and reads the 1,088,919-byte
fixture payload. The artifact retains exact response bytes, result
cardinality, cumulative matched/returned rows, reader-permit timing, and all
50-iteration nanosecond distributions. `typed values` returns one aggregate
row whose wide response is 16,440 bytes; projection returns 809,898 response
bytes. Those materially different response shapes explain why cardinality
alone is not used as a cost proxy.

All 8,192 entries completed durably with zero queued work. Admission took
9.946ms and the explicit durability barrier took 23.116ms. Storage remains
byte-identical to Sessions 10 and 12: 1,088,919 logical block bytes, 1,126,400
SQLite page bytes, 1,190,496 physical database/WAL/SHM bytes, four raw blocks,
zero compressed blocks, and a 16,384-byte index. Whole-process RSS HWM was
64,068KiB, 5,568KiB (9.52%) above Session 12 after 18 additional shapes;
`QSF-081` preserves that measured cost without claiming a storage speedup.

All 75 pinned VictoriaLogs 1.52.0 applicable cases, all 15 real-extension log
API/flush/optimize/reopen/backup tests, all 36 LogsQL/storage unit tests, and
all 68 executable SQL recipes (92 statements) pass. The oracle fixture pins
intentional differences for rich typed values, missing/null/empty identity,
numeric-string coercion, stream-field synthesis, value envelopes, and hash
identity. Regressions additionally pin operator `limit 0`, independent result
and work caps, ordered projection/filter semantics, exact integers beyond
2^53, overflow-safe finite median, independently bounded median state,
cancellation, durability, and reader reuse. No batching, block format,
compression, index, retention, transaction, migration, maintenance, or public
extension contract changed.

## Session 14 stable PromQL P2 completion

The checked-in
[`2026-08-04_session14_promql_p2.json`](evidence/2026-08-04_session14_promql_p2.json)
was captured from exact extension, metrics-server, and logs-server build
`066fb2bba87b79fb7763c4a15fdc11f10aed282b`. It measures the three shipped
rows: quoted UTF-8 metric/label names (`PQL-S14`), comments (`PQL-S15`), and
classic-bucket `histogram_fraction` (`PQL-H02`).

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| quoted exact series | 1 | 172 | 0.571 | 0.801 | 0.822 | 1 | 32 | 131 |
| quoted metric, 512 series / four steps | 2,048 | 88,100 | 3.134 | 3.600 | 4.753 | 512 | 16,384 | 53,831 |
| commented exact selector | 1 | 164 | 0.548 | 0.751 | 0.796 | 1 | 32 | 131 |
| commented selector, 512 series / four steps | 2,048 | 84,004 | 3.299 | 3.615 | 4.340 | 512 | 16,384 | 53,831 |
| classic fraction, one family | 1 | 134 | 0.649 | 1.410 | 2.291 | 4 | 4 | 208 |
| classic fraction, 512 families | 512 | 45,322 | 12.775 | 14.935 | 16.111 | 2,048 | 2,048 | 106,496 |

Quoted names and comments do not amplify storage work. Their narrow and wide
shapes perform exactly the same candidate-chunk, decode, returned-point, and
payload work as ordinary exact selectors of the same cardinality. The quoted
wide response is 4,096 bytes larger solely because each returned metric and
label name contains additional UTF-8 bytes; its p95 differs from the
same-work commented shape by less than 0.5%. The exposition change preserves
the allocation-free path for ordinary names and allocates only when an
identity actually contains an escape.

The wide fraction evaluates four public bucket series per family once, then
does bounded Rust composition. It has identical storage work to the same-run
`histogram_quantile` shape and one additional scalar intermediate per query;
its 14.935 ms p95 is 2.2% lower than that shape's 15.271 ms p95, so no speedup
is claimed. The fraction response is 3,872 bytes larger. The narrow fraction
p50 is lower than the same-run quantile, but its 1.410 ms p95 is 65.8% higher
and its 2.291 ms p99 is retained as an honest tail result. The absolute tail
remains bounded, storage counters are unchanged, and there is no evidence
that an extension primitive would avoid any read or decode work.

The primary fixture deliberately adds 512 quoted-name series so the wide
shape is comparable to the established 512-series metric. All 36,928 points
completed the explicit durability barrier with zero failed or queued work;
admission took 8.000 ms and the barrier took 92.961 ms. The additional 16,384
points produce exactly 512 additional chunks, 53,831 extension payload bytes,
49,152 SQLite index bytes, and 262,528 physical database/WAL/SHM bytes versus
Session 11. Final metrics RSS HWM was 49,116 KiB, 2,760 KiB (5.95%) above
Session 11 after doubling the main-series fixture and adding six query shapes.
The unchanged log fixture remained byte-identical at 1,088,919 logical block
bytes and 1,190,496 physical bytes; its 64,440 KiB HWM is 372 KiB (0.58%)
above Session 13.

All 528 pinned Prometheus 3.13.2 API cases, the six-case pinned
VictoriaMetrics 1.148.0 step-relative fixture, all 80 metrics real-extension
tests, all 45 metrics parser/evaluator/storage units, all 69 SQL recipes (93
statements), both complete Rust workspaces, formatting, and clippy pass.
`PQL-S10` is correctly deferred to MetricsQL row `MQL-09`.
`PQL-F19`, `PQL-F20`, and `PQL-H03` are feature-gated experimental PromQL;
the stable endpoint pins their upstream disabled/unknown diagnostics and
tracks the distinct MetricsQL work as `MQL-10` through `MQL-12`. Cancellation,
limits, GET/POST envelopes, compact, shutdown, reopen, and public catalog
identity have real-extension regressions. No extension primitive, storage
format, batching, compression, index, rollup, retention, transaction,
migration, maintenance, or public batch/SQL contract changed.

## Session 15 MetricsQL P2 progress: conditional operators

The checked-in
[`2026-08-05_session15_metricsql_p2.json`](evidence/2026-08-05_session15_metricsql_p2.json)
was captured from exact extension, metrics-server, and logs-server build
`93fac6ae3134e480d522f70c0bc2a3e3fa900f43`. It closes `MQL-01` on the
explicit MetricsQL routes with the worst-case identity shape: every comparison
value is filtered, then `default 0` must retain and fill every selected series.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query | intermediate work/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| filtered `default`, exact host | 1 | 163 | 0.662 | 0.887 | 0.931 | 1 | 32 | 131 | 4 |
| ordinary exact selector | 1 | 164 | 0.654 | 0.798 | 0.856 | 1 | 32 | 131 | 0 |
| filtered `default`, 512 series / four steps | 2,048 | 80,190 | 4.918 | 5.277 | 5.423 | 512 | 16,384 | 53,831 | 2,568 |
| ordinary selector, 512 series / four steps | 2,048 | 84,004 | 3.131 | 3.522 | 4.359 | 512 | 16,384 | 53,831 | 0 |
| boolean comparison, 512 series / four steps | 2,048 | 63,806 | 4.466 | 4.631 | 5.073 | 512 | 16,384 | 53,831 | 2,052 |

The first oracle-correct implementation recovered a comparison's otherwise
empty identities with a second public evaluation. Its preliminary wide p95
was 9.228 ms and every storage counter doubled to 1,024 chunks, 32,768 decoded
points, 107,662 payload bytes, and 536,608 frame bytes per query. That path was
removed before shipment. The final evaluator retains only candidate labels
during the original comparison evaluation. It therefore reads exactly the
same chunks, decoded points, payload bytes, and 268,304 frame bytes as the
ordinary selector while also handling matched vectors whose samples never
overlap in time.

The retained language work is visible rather than hidden: the wide shape
charges 2,568 intermediate items, comprising the comparison grid, scalar
grid, and 512 candidate identities. Wide p95 is 49.8% above the ordinary
selector but only 14.0% above the same-run boolean comparison, while returning
16,384 additional response bytes. Narrow p95 is 11.1% above its selector.
These are bounded API composition costs, not evidence for a new extension
primitive; direct SQLite/libSQL users already have the single-grid
`SQL-MQL-001` recipes.

The unchanged 36,928-point fixture completed its durability barrier with zero
failed or queued work. Admission took 8.880 ms and the barrier took 81.725 ms.
Logical and physical metrics storage are byte-identical to Session 14:
224,688 payload bytes, 409,600 index bytes, and 1,542,312 database/WAL/SHM
bytes. The logs fixture is likewise byte-identical at 1,088,919 logical and
1,190,496 physical bytes. Metrics RSS HWM was 51,876 KiB, 2,760 KiB (5.62%)
above Session 14 after adding two measured shapes; logs HWM was 64,536 KiB,
96 KiB (0.15%) above Session 14. The exact HWM increase is preserved without
attributing it to the label-only candidate state.

All 19 pinned VictoriaMetrics 1.148.0 cases, all 81 metrics real-extension
tests, all 49 metrics parser/evaluator/storage units, all 70 SQL recipes (96
statements), the 24-test Rust query harness, query-contract validation, both
complete Rust workspaces, formatting, and clippy pass. The real-extension
contract covers GET/POST, instant/range envelopes, stable PromQL isolation,
errors, limits, cancellation boundary, flush, shutdown, and reopen. No
extension primitive, private table access, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed.

## Session 15 MetricsQL P2 progress: operation-level metric names

The checked-in
[`2026-08-05_session15_mql_02_keep_metric_names.json`](evidence/2026-08-05_session15_mql_02_keep_metric_names.json)
was captured from exact extension, metrics-server, and logs-server build
`770301a41b16248f319000a1e4415d95487ec9ab`. It closes `MQL-02` with a
same-run comparison against the stable `abs` operation.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query | intermediate work/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `abs` + `keep_metric_names`, exact host | 1 | 164 | 0.924 | 1.263 | 1.823 | 1 | 32 | 131 | 1 |
| stable `abs`, exact host | 1 | 132 | 0.862 | 1.012 | 1.054 | 1 | 32 | 131 | 1 |
| `abs` + `keep_metric_names`, 512 series / four steps | 2,048 | 84,004 | 4.470 | 5.142 | 7.008 | 512 | 16,384 | 53,831 | 2,048 |
| stable `abs`, 512 series / four steps | 2,048 | 67,620 | 4.604 | 5.296 | 8.385 | 512 | 16,384 | 53,831 | 2,048 |

Name retention adds no storage read, decode, frame crossing, or intermediate
point. The wide named response contains 16,384 additional bytes, yet its p95
was 2.9% below the same-run stable transform; that difference is treated as
noise, not an optimization claim. Narrow p95 was 24.8% higher, covering the
explicit MetricsQL parser path and 32 additional response bytes. There is no
evidence for a new extension primitive: direct users already select the
public metric identity as shown by `SQL-MQL-002`.

The 36,928-point fixture completed with zero failed or queued work. Admission
took 11.781 ms and the durability barrier took 86.462 ms. Metrics storage is
byte-identical to the preceding Session 15 capture: 224,688 payload bytes,
409,600 index bytes, and 1,542,312 physical database/WAL/SHM bytes. Logs
remain 1,088,919 logical and 1,190,496 physical bytes. Metrics RSS HWM was
52,740 KiB, 864 KiB (1.67%) above the preceding capture; logs HWM was
64,320 KiB, 216 KiB lower. These whole-process observations are retained
without attributing the metrics increase to the modifier.

Pinned VictoriaMetrics covers six success and three error cases: multi-name
transforms, rollups, scalar and vector binaries, default name-aware matching,
the explicit-`on(...)` exception, nested aggregation, and invalid bare,
aggregate, and unary targets. The real-extension contract additionally covers
stable PromQL isolation, GET/form POST, work limits, flush, shutdown, and
reopen. No extension syntax, private table access, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed.

All 528 pinned Prometheus cases and all 28 pinned VictoriaMetrics cases pass.
The complete 82-test metrics real-extension suite, 51 metrics library/binary
unit tests, both complete Rust workspaces, clippy with warnings denied,
formatting, the 24-test Rust query harness, documentation contracts, and all
71 SQL recipes (97 statements) pass locally. No CI workflow was invoked.

## Session 15 MetricsQL P2 progress: union and alias

The checked-in
[`2026-08-05_session15_mql_03_union_alias.json`](evidence/2026-08-05_session15_mql_03_union_alias.json)
was captured from exact extension, metrics-server, and logs-server build
`b56cf052def2d499d55380b87022533fd736c8f3`. It closes `MQL-03` with
independent alias and union shapes over the same public float-series grid.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query | intermediate work/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `alias`, exact host | 1 | 166 | 0.467 | 0.826 | 1.000 | 1 | 32 | 131 | 1 |
| `alias`, 512 series / four steps | 2,048 | 85,028 | 4.851 | 5.116 | 5.477 | 512 | 16,384 | 53,831 | 2,048 |
| two-branch `union`, exact host | 2 | 274 | 0.890 | 1.003 | 1.043 | 2 | 64 | 262 | 4 |
| two-branch `union`, 1,024 series / four steps | 4,096 | 172,042 | 12.316 | 13.076 | 14.692 | 1,024 | 32,768 | 107,662 | 8,192 |

Alias performs exactly the same candidate-chunk, decode, returned-point,
payload, and frame work as the same-run `keep_metric_names` transform. Its
wide p95 was 5.1% higher (5.116 versus 4.866 ms) while returning 1,024 more
bytes; its narrow p95 was 6.8% lower. Both differences are retained as run
variance rather than a speed claim. The generated name and every retained
label map are charged incrementally to the existing response limit, so the
bounded result does not depend on the final encoder noticing excessive label
fan-out.

The union shape deliberately evaluates two independently named public-grid
branches and returns both. It therefore performs exactly twice the alias
shape's storage work and returns twice as many points. Its wide p95 is 2.56x
the alias p95, its response is 2.02x as large, and its intermediate accounting
is 4x because the two child aliases and the union's retained output are each
bounded. These measurements do not justify an extension primitive: a generic
union may contain unrelated plans, while a future Rust planner optimization
for provably identical subexpressions would be language composition rather
than a new storage contract. No common-subexpression speedup is claimed here.

All 36,928 points completed durably with zero failed or queued work. Admission
took 8.507 ms and the explicit durability barrier took 96.020 ms. Metrics
storage is byte-identical to the preceding capture: 224,688 payload bytes,
409,600 index bytes, and 1,542,312 physical database/WAL/SHM bytes. Logs remain
1,088,919 logical and 1,190,496 physical bytes. Metrics RSS HWM was 53,028 KiB,
288 KiB (0.55%) above the preceding capture; logs HWM was 63,976 KiB, 344 KiB
lower. These whole-process measurements are not attributed to a single query
node.

All 528 pinned Prometheus 3.13.2 cases and all 47 pinned VictoriaMetrics
1.148.0 cases pass. The complete 83-test metrics real-extension suite, 53
metrics library/binary tests, both complete Rust workspaces, clippy with
warnings denied, formatting, the 24-test Rust query harness, documentation
contracts, and all 72 SQL recipes (100 statements) pass locally. Regressions
cover duplicate identities, first-branch precedence, scalar collisions,
zero/single/trailing-comma forms, case behavior, stable PromQL isolation,
GET/POST, instant/range, limits, cancellation, flush, shutdown, reopen, and
reader reuse after a rejected label fan-out. No extension primitive, private
table access, storage format, batching, compression, index, rollup, retention,
transaction, migration, maintenance, or public batch/SQL contract changed. No
CI workflow was invoked.

## Session 15 MetricsQL P2 progress: label transformations

The checked-in
[`2026-08-05_session15_mql_04_labels.json`](evidence/2026-08-05_session15_mql_04_labels.json)
was captured from exact extension, metrics-server, and logs-server build
`bcafebd21145657e093f8f825f178dff461835d3`. It closes `MQL-04` with
independent `label_set` and `label_del` shapes over the same public float-series
grid.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query | intermediate work/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `label_set`, exact host | 1 | 195 | 0.806 | 0.940 | 1.090 | 1 | 32 | 131 | 1 |
| `label_set`, 512 series / four steps | 2,048 | 97,828 | 4.808 | 5.351 | 7.998 | 512 | 16,384 | 53,831 | 2,048 |
| `label_del`, exact host | 1 | 149 | 0.701 | 0.822 | 0.871 | 1 | 32 | 131 | 1 |
| `label_del`, 512 series / four steps | 2,048 | 67,620 | 4.377 | 4.659 | 5.161 | 512 | 16,384 | 53,831 | 2,048 |

Both operations perform exactly one public-grid read and have the same
candidate-chunk, decode, payload, frame, and intermediate-point work as the
same-run alias and name-retention transforms. `label_set` wide p95 is 10.0%
above alias (5.351 versus 4.864 ms) while returning 12,800 additional bytes;
its narrow p95 is 15.8% above alias. `label_del` wide p95 is 7.1% above
`keep_metric_names` (4.659 versus 4.351 ms), while deleting `__name__` reduces
the response by 16,384 bytes; its narrow p95 is 2.8% above the comparison.
These are bounded label-map and encoding costs, not avoidable storage work, so
no extension primitive is justified.

The first evidence attempt deleted the only distinguishing `host` label from
all 512 series and correctly received the pinned duplicate-output 422 error.
The valid wide deletion shape removes `__name__` while retaining `host`; the
real-extension collision regression continues to prove that deleting a
distinguishing label fails rather than silently dropping series. This fixture
correction changes neither product semantics nor the measured storage path.

All 36,928 points completed durably with zero failed or queued work. Admission
took 7.763 ms and the explicit durability barrier took 82.725 ms. Metrics
storage remains byte-identical to the preceding Session 15 captures: 224,688
payload bytes, 409,600 index bytes, and 1,542,312 physical database/WAL/SHM
bytes. Logs remain 1,088,919 logical and 1,190,496 physical bytes. Metrics RSS
HWM was 51,116 KiB, 1,912 KiB below the preceding capture; logs HWM was 64,524
KiB, 548 KiB higher. These whole-process variations are retained without
attributing either direction to one label operation.

All 528 pinned Prometheus 3.13.2 cases and all 65 pinned VictoriaMetrics
1.148.0 cases pass. The complete 84-test metrics real-extension suite, 54
metrics library/binary tests, both complete Rust workspaces, clippy with
warnings denied, formatting, the 24-test Rust query harness, documentation
contracts, and all 73 SQL recipes (102 statements) pass locally. Regressions
cover ordered set/delete behavior, empty-value deletion, metric-name mutation,
identity and scalar forms, duplicate outputs, case behavior, stable PromQL
isolation, GET/POST, instant/range, work and response limits, flush, shutdown,
reopen, and reader reuse after rejection. No extension primitive, private table
access, storage format, batching, compression, index, rollup, retention,
transaction, migration, maintenance, or public batch/SQL contract changed. No
CI workflow was invoked.

## Session 15 MetricsQL P2 progress: automatic and window-less rollups

The checked-in
[`2026-08-05_session15_mql_05_rollups.json`](evidence/2026-08-05_session15_mql_05_rollups.json)
was captured from exact extension, metrics-server, and logs-server build
`8a7cf6cf973be308021366723a7b650920a82b60`. It closes `MQL-05` with separate
implicit-default, step-window aggregate, and previous-sample counter shapes.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query | extension frame bytes/query | raw points returned/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| implicit `default_rollup`, exact host | 1 | 164 | 0.680 | 0.954 | 1.045 | 1 | 32 | 131 | 540 | 32 |
| implicit `default_rollup`, 512 series / four steps | 2,048 | 84,004 | 3.733 | 5.409 | 5.448 | 512 | 16,384 | 53,831 | 268,304 | 16,384 |
| window-less `avg_over_time`, exact host | 1 | 166 | 0.736 | 0.966 | 1.038 | 1 | 32 | 131 | 524 | 31 |
| window-less `avg_over_time`, 512 series / four steps | 2,048 | 84,004 | 3.636 | 4.083 | 4.188 | 512 | 16,384 | 53,831 | 47,120 | 2,560 |
| window-less `rate`, exact host | 1 | 133 | 0.838 | 0.989 | 1.068 | 1 | 32 | 131 | 540 | 32 |
| window-less `rate`, 512 series / four steps | 2,048 | 67,902 | 3.870 | 4.558 | 10.257 | 512 | 16,384 | 53,831 | 268,304 | 16,384 |

Every shape performs one public packed-raw query. Candidate chunks, decoded
points, and persisted payload bytes are identical to the corresponding
retained selector grid; cadence inference, stale-bit inspection, prior-sample
selection, reset correction, and output-name policy cause no second storage
read. The three wide paths return 2,048 result points and remain within the
same work, result, response, deadline, and cancellation envelopes.

The implicit-default wide p95 is 45.2% above the same-run stable range selector
(5.409 versus 3.725 ms) with the same 84,004-byte response and exactly the same
storage and frame work. Narrow p95 is 2.3% below its selector reference despite
retaining one additional cadence sample. The wide difference is therefore
bounded per-series inference/evaluation variation rather than storage
amplification; it is retained without claiming the narrow direction as a win.

Window-less average wide p95 is 10.3% above the same-run explicit five-minute
average (4.083 versus 3.703 ms). The comparison is directional, not semantic:
the MetricsQL form uses a step window, retains `__name__`, and returns 13,371
more response bytes. Its unified packed-raw frame is 47,120 bytes versus
37,376 bytes from the existing `timeless_window` aggregate. That public window
surface already gives direct SQLite/libSQL users the smaller reduction;
automatic syntax, raw stale/ordinary-NaN bits, and cross-window composition
remain API semantics, so no new extension primitive is justified.

Window-less rate wide p95 is 8.1% above the same-run explicit five-minute rate
(4.558 versus 4.215 ms), while narrow p95 is 2.2% higher. One wide iteration was
the 10.257 ms p99/max; the other 49 observations leave p95 at 4.558 ms. Those
expressions also have different windows and annotation behavior, so the
evidence does not claim direct semantic equivalence. It proves that scrape
inference, bounded silence history, previous-sample use, and reset correction
stay within one retained raw read while preserving the observed tail.

All 36,928 points completed durably with zero failed or queued work. Admission
took 9.262 ms and the explicit durability barrier took 91.532 ms. Metrics
storage is byte-identical to MQL-04: 224,688 payload bytes, 409,600 index bytes,
and 1,542,312 physical database/WAL/SHM bytes. Logs remain 1,088,919 logical and
1,190,496 physical bytes. Metrics RSS HWM was 51,444 KiB, 328 KiB above MQL-04;
logs HWM was 64,452 KiB, 72 KiB lower. These whole-process variations are
retained without attributing either direction to one query shape.

All 528 pinned Prometheus 3.13.2 cases and all 96 pinned VictoriaMetrics
1.148.0 cases pass. The complete 85-test metrics real-extension suite, 54
metrics library tests, both complete Rust workspaces, clippy with warnings
denied, formatting, the 25-test Rust query harness, documentation contracts,
and all 74 SQL recipes (104 statements) pass locally. Regressions cover
implicit/explicit rollups, inferred and capped windows, open-left boundaries,
stale and ordinary NaN bits, all retained window-less functions, previous
transitions, reset correction, name and timestamp policy, scalar and aggregate
composition, invalid arity, stable PromQL isolation, GET/POST, limits,
cancellation, flush, shutdown, durability, and reopen. No extension primitive,
private table access, storage format, batching, compression, index, rollup,
retention, transaction, migration, maintenance, or public batch/SQL contract
changed. No CI workflow was invoked.

## Session 15 MetricsQL P2 progress: complete-grid range aggregates

The checked-in
[`2026-08-05_session15_mql_06_range_aggregates.json`](evidence/2026-08-05_session15_mql_06_range_aggregates.json)
was captured from exact extension, metrics-server, and logs-server build
`45e29395ab68a1deddd34ec689abc713909c2a09`. It closes `MQL-06` with
complete-request-grid average and sum shapes; the same correctness suite also
pins `range_min` and `range_max`.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query | extension frame bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `range_avg`, exact host / four steps | 4 | 197 | 0.824 | 0.996 | 1.057 | 12 | 1 | 32 | 131 | 540 |
| `range_avg`, 512 series / four steps | 2,048 | 71,714 | 4.766 | 5.359 | 6.925 | 6,144 | 512 | 16,384 | 53,831 | 268,304 |
| `range_sum`, exact host / four steps | 4 | 193 | 0.919 | 1.090 | 1.190 | 12 | 1 | 32 | 131 | 540 |
| `range_sum`, 512 series / four steps | 2,048 | 69,066 | 4.769 | 5.121 | 5.815 | 6,144 | 512 | 16,384 | 53,831 | 268,304 |

Every shape evaluates one retained MetricsQL selector grid through one public
packed-raw query. The narrow paths read one chunk and 32 stored points; the
wide paths read 512 chunks and 16,384 stored points. The complete-grid fold
and repeated final value add no second storage query. Existing cumulative work
accounting charges the child points, reduction steps, and generated result:
12 points for the four-step narrow shape and 6,144 for the 2,048-point wide
shape. Result, response, deadline, and cancellation limits therefore bound the
entire operation rather than only its child selector.

The same-run MetricsQL selector measured 0.943 ms narrow and 4.180 ms wide
p95. `range_avg` is 5.5% and 28.2% above those references; `range_sum` is
15.5% and 22.5% above. Candidate chunks, decoded points, persisted payload
bytes, and packed-frame bytes are identical, so the difference is bounded
Rust grid reduction, fill, collision checking, and response encoding rather
than storage-read amplification. The wide average retained a 6.925 ms
p99/max observation. These results do not justify an extension primitive:
the executable public-SQL recipe exposes the row-visible complete-grid fold,
while MetricsQL's missing-slot, name, collision, and response semantics remain
language-owned composition.

All 36,928 points completed durably with zero failed or queued work. Admission
took 8.239 ms and the explicit durability barrier took 88.322 ms. Metrics
storage remained 224,688 payload bytes, 409,600 index bytes, and 1,542,312
physical database/WAL/SHM bytes. Logs remained 1,088,919 logical and 1,190,496
physical bytes. Metrics RSS HWM was 50,108 KiB, 1,336 KiB below MQL-05; logs
HWM was 64,572 KiB, 120 KiB above it. Both are whole-process variations and
neither direction is attributed to the four added query shapes.

All 528 pinned Prometheus 3.13.2 cases and all 116 pinned VictoriaMetrics
1.148.0 cases pass. The complete 86-test metrics real-extension suite, 55
metrics library tests, both complete Rust workspaces, clippy with warnings
denied, formatting, the 25-test Rust query harness, documentation contracts,
and all 75 SQL recipes (105 statements) pass locally. Regressions cover
leading NaN and later missing slots, incremental average, ordinary sum,
later-operand extrema ties, signed zero, complete-grid fill, name removal,
`keep_metric_names`, duplicate outputs, scalar/expression composition, case
and trailing-comma syntax, stable PromQL isolation, GET/POST, work/result
limits, deadline cancellation and recovery, shutdown, durability, and reopen.
No extension primitive, private table access, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was invoked.

## Session 15 MetricsQL P2 progress: cumulative running aggregates

The checked-in
[`2026-08-05_session15_mql_07_running_aggregates.json`](evidence/2026-08-05_session15_mql_07_running_aggregates.json)
was captured from exact extension, metrics-server, and logs-server build
`0e0f73d942c887123bb6c11105168a0ca6e328c4`. It closes `MQL-07` with
cumulative running-average and running-sum shapes; the same correctness suite
also pins `running_min` and `running_max`.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query | extension frame bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `running_avg`, exact host / four steps | 4 | 193 | 0.855 | 1.058 | 1.104 | 12 | 1 | 32 | 131 | 540 |
| `running_avg`, 512 series / four steps | 2,048 | 69,664 | 4.745 | 5.260 | 5.722 | 6,144 | 512 | 16,384 | 53,831 | 268,304 |
| `running_sum`, exact host / four steps | 4 | 190 | 0.895 | 1.057 | 1.071 | 12 | 1 | 32 | 131 | 540 |
| `running_sum`, 512 series / four steps | 2,048 | 68,341 | 4.755 | 5.202 | 5.507 | 6,144 | 512 | 16,384 | 53,831 | 268,304 |

Every shape performs one child evaluation through one public packed-raw
query. Narrow paths read one chunk and 32 stored points; wide paths read 512
chunks and 16,384 stored points. Cumulative folding, stale/missing carry, and
computed-NaN omission cause no second storage query. Work accounting charges
the child, each evaluated grid slot, and each emitted result: 12 points for a
four-step narrow series and 6,144 for the 2,048-point wide result. Existing
result, response, deadline, and cancellation limits therefore bound the
complete operation.

The same-run MetricsQL selector measured 0.931 ms narrow and 3.927 ms wide
p95. `running_avg` is 13.6% and 34.0% above those references;
`running_sum` is 13.5% and 32.5% above. Candidate chunks, decoded points,
persisted payload bytes, and packed-frame bytes are identical, so the measured
difference is bounded Rust cumulative evaluation, name/collision handling,
and response encoding—not storage amplification. No measured path justifies
an extension primitive; direct SQLite/libSQL users already have the
executable recursive public-grid form in `SQL-MQL-007`.

All 36,928 points completed durably with zero failed or queued work. Admission
took 7.864 ms and the explicit durability barrier took 88.825 ms. Metrics
storage remained 224,688 payload bytes, 409,600 index bytes, and 1,542,312
physical database/WAL/SHM bytes. Logs remained 1,088,919 logical and 1,190,496
physical bytes. Metrics RSS HWM was 52,184 KiB, 2,076 KiB above MQL-06; logs
HWM was 64,328 KiB, 244 KiB below it. Both are whole-process variations and
neither direction is attributed to the four added query shapes.

All 528 pinned Prometheus 3.13.2 cases and all 140 pinned VictoriaMetrics
1.148.0 cases pass, as does the pinned VictoriaLogs corpus. The complete
87-test metrics real-extension suite, 56 metrics library tests, both complete
Rust workspaces, clippy with warnings denied, formatting, the 25-test Rust
query harness, documentation contracts, and all 76 SQL recipes (106
statements) pass locally. Regressions cover cumulative average/minimum/
maximum/sum, instant identity, slot-index arithmetic, leading and stale/
missing carry, computed NaN omission and propagation, infinity, overflow,
Timeless signed-zero fidelity, unconditional name removal,
`keep_metric_names`, scalar/expression/case/trailing-comma forms, arity,
duplicate outputs, stable PromQL isolation, one public read, work/result
limits, deadline cancellation and recovery, GET/POST, durability, shutdown,
and reopen. No extension primitive, private table access, storage format,
batching, compression, index, rollup, retention, transaction, migration,
maintenance, or public batch/SQL contract changed. No CI workflow was invoked.

## Session 15 MetricsQL P2 progress: request-step-relative durations

The checked-in
[`2026-08-05_session15_mql_09_step_relative_durations.json`](evidence/2026-08-05_session15_mql_09_step_relative_durations.json)
was captured from exact extension, metrics-server, and logs-server build
`b2b1f8610894dbd2271d24b7b224f2551c443a7d`. It closes `MQL-09` with
finite request-step windows, request-step offsets, and adaptive zero-window
rate shapes. The correctness suite separately pins subquery windows and
resolutions, decimal and compound arithmetic, signed saturation, and all
documented lexical boundaries.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query | extension frame bytes/query | extension points returned/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `[5i]` count, exact host / four steps | 4 | 185 | 0.444 | 0.715 | 0.786 | 1 | 32 | 131 | 73 | 4 |
| `[5i]` count, 512 series / four steps | 2,048 | 63,806 | 2.922 | 3.168 | 3.271 | 512 | 16,384 | 53,831 | 37,376 | 2,048 |
| `offset 5i`, exact host / four steps | 4 | 221 | 0.590 | 0.822 | 0.879 | 1 | 32 | 131 | 460 | 27 |
| `offset 5i`, 512 series / four steps | 2,048 | 83,984 | 3.290 | 3.579 | 4.188 | 512 | 16,384 | 53,831 | 227,344 | 16,384 |
| adaptive `rate(...[0i])`, exact host / four steps | 4 | 193 | 0.603 | 0.781 | 0.811 | 1 | 32 | 131 | 540 | 32 |
| adaptive `rate(...[0i])`, 512 series / four steps | 2,048 | 67,902 | 3.299 | 3.692 | 3.729 | 512 | 16,384 | 53,831 | 268,304 | 16,384 |

Every shape performs exactly one public extension query. Finite ranges use the
existing packed window reduction; offsets and adaptive zero windows use the
existing bounded packed-raw plan. The wide finite-window shape has exactly the
same candidate chunks, decoded points, persisted payload bytes, and packed
frame bytes as the same-run stable `count_over_time` shape. Its 3.168 ms p95
is 1.0% below the stable shape's 3.201 ms, while dropping the metric name
reduces the response by 2,048 bytes. This is parity within run variation, not
a speed claim.

The wide request-step offset measured 3.579 ms p95 versus 3.474 ms for the
same-run implicit `default_rollup`, a 3.0% difference. Both make one raw read;
the offset's shifted selection returns a 227,344-byte frame rather than
268,304 bytes and its response is 20 bytes smaller. The adaptive zero-window
rate measured 3.692 ms p95 versus 3.654 ms for the semantically equivalent
window-less rate, a 1.0% difference with byte-identical result, response,
candidate, decode, payload, frame, and returned-point counts. None of these
measurements shows avoidable storage amplification or justifies an extension
primitive.

All 36,928 points completed durably with zero failed or queued work. Admission
took 7.349 ms and the explicit durability barrier took 88.318 ms. Metrics
storage remained 224,688 payload bytes, 409,600 index bytes, and 1,542,312
physical database/WAL/SHM bytes. Logs remained 1,088,919 logical and 1,190,496
physical bytes. Metrics RSS HWM was 50,196 KiB, 1,988 KiB below MQL-07; logs
HWM was 64,468 KiB, 140 KiB above it. Both are whole-process variations and
neither direction is attributed to these query shapes.

All 528 pinned Prometheus 3.13.2 cases, all 151 pinned VictoriaMetrics 1.148.0
cases, and all 75 pinned VictoriaLogs 1.52.0 cases pass. The complete 88-test
metrics real-extension suite, 57 metrics library tests, both complete Rust
workspaces, clippy with warnings denied, formatting, the 25-test Rust query
harness, documentation contracts, and all 77 SQL recipes (108 statements)
pass locally. Regressions cover direct and subquery request-step windows and
resolutions, decimal/compound/case/millisecond arithmetic, inherited negative
offsets, collision-free zero/extrema markers, exact signed saturation,
adaptive selector/subquery zero windows, comments and strings, bare `i`,
stable PromQL isolation, one public read, result/deadline limits, cancellation
recovery, GET/POST, durability, shutdown, and reopen. No extension primitive,
private table access, storage format, batching, compression, index, rollup,
retention, transaction, migration, maintenance, or public batch/SQL contract
changed. No CI workflow was invoked.

## Session 15 MetricsQL P2 progress: query-context values

The checked-in
[`2026-08-05_session15_mql_10_query_context.json`](evidence/2026-08-05_session15_mql_10_query_context.json)
was captured from exact extension, metrics-server, and logs-server build
`3c290202cef902326660a5e540a08d6b1ec4206d`. It closes `MQL-10` with a
storage-free request-context expression plus narrow and wide selector
composition. Correctness separately pins range and instant start/end values,
subsecond step, pre-epoch time, case, arity, unsupported functions, and direct
selector modifier isolation.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | candidate chunks/query | decoded points/query | extension payload bytes/query | extension frame bytes/query | extension points returned/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pure `time() - start() + step() - step()`, four steps | 4 | 158 | 0.480 | 0.649 | 0.701 | 0 | 0 | 0 | 0 | 0 |
| context composition, exact host / four steps | 4 | 189 | 0.255 | 0.756 | 0.896 | 1 | 32 | 131 | 540 | 32 |
| context composition, 512 series / four steps | 2,048 | 67,620 | 5.124 | 5.352 | 5.422 | 512 | 16,384 | 53,831 | 268,304 | 16,384 |

The pure context shape performs no extension query because all three values
are request metadata. Both selector compositions perform exactly one existing
public packed-raw query. Their candidate chunks, decoded points, persisted
payload bytes, frame bytes, and returned extension points are identical to
the same-run implicit MetricsQL rollup at the corresponding width. Context
lowering therefore adds no storage read, decode, frame, or row crossing.

The wide context composition measured 5.352 ms p95 versus 3.880 ms for the
same-run implicit MetricsQL rollup, a 37.9% evaluator/response difference with
identical storage work. The context expression charges 2,060 intermediate
points per request for scalar subtraction and vector addition. Binary
operation name removal makes its response 67,620 bytes, exactly 16,384 bytes
smaller than the named rollup's 84,004 bytes. This is an honest bounded Rust
composition cost; moving request metadata into the extension would not reduce
the storage work and would give direct SQLite users no capability beyond
binding the values in `SQL-MQL-010`.

All 36,928 points completed durably with zero failed or queued work. Admission
took 8.103 ms and the explicit durability barrier took 93.332 ms. Metrics
storage remained 224,688 payload bytes, 409,600 index bytes, and 1,542,312
physical database/WAL/SHM bytes. Logs remained 1,088,919 logical and 1,190,496
physical bytes. Metrics RSS HWM was 50,936 KiB, 740 KiB above MQL-09; logs HWM
was 64,968 KiB, 500 KiB above it. Both are whole-process variations and
neither direction is attributed to the three added query shapes.

All 528 pinned Prometheus 3.13.2 cases, all 164 pinned VictoriaMetrics 1.148.0
cases, and all 75 pinned VictoriaLogs 1.52.0 cases pass. The complete 89-test
metrics real-extension suite, 58 metrics library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 25-test Rust query
harness, documentation contracts, and all 78 SQL recipes (109 statements)
pass locally. Regressions cover range and instant context values,
subsecond/pre-epoch inputs, case-insensitive zero-argument grammar,
scalar/vector composition, direct `@ start()` modifier preservation, exact
unsupported functions and arity, stable PromQL isolation, pure zero-read and
selector one-read accounting, result/deadline limits, cancellation recovery,
GET/POST, durability, shutdown, and reopen. No extension primitive, private
table access, storage format, batching, compression, index, rollup, retention,
transaction, migration, maintenance, or public batch/SQL contract changed. No
CI workflow was invoked.

## Session 15 MetricsQL P2 progress: plural histogram quantiles

The checked-in
[`2026-08-05_session15_mql_12_histogram_quantiles.json`](evidence/2026-08-05_session15_mql_12_histogram_quantiles.json)
was captured from exact extension, metrics-server, and logs-server build
`81e5034f359de840ff7e02eaa7cbb26477562c0a`. It closes `MQL-12` with
one- and two-rank cumulative-histogram shapes; correctness separately pins
time-varying ranks, VictoriaMetrics float labels, destination mutation,
`vmrange`, missing `+Inf`, monotonic/NaN repair, and collision/error behavior.

| shape | result points | response bytes | p50 ms | p95 ms | p99 ms | intermediate points/query | candidate chunks/query | decoded points/query | extension payload bytes/query | extension frame bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| stable `histogram_quantile`, one family | 1 | 131 | 0.688 | 0.921 | 1.052 | 5 | 4 | 4 | 208 | 128 |
| `histogram_quantiles`, one rank / one family | 1 | 144 | 0.864 | 1.055 | 1.075 | 10 | 4 | 4 | 208 | 128 |
| `histogram_quantiles`, two ranks / one family | 2 | 228 | 0.984 | 1.145 | 1.215 | 16 | 4 | 4 | 208 | 128 |
| stable `histogram_quantile`, 512 families | 512 | 41,450 | 13.025 | 13.741 | 14.083 | 2,049 | 2,048 | 2,048 | 106,496 | 57,360 |
| `histogram_quantiles`, one rank / 512 families | 512 | 48,120 | 13.793 | 15.572 | 16.353 | 4,609 | 2,048 | 2,048 | 106,496 | 57,360 |
| `histogram_quantiles`, two ranks / 512 families | 1,024 | 102,699 | 14.040 | 15.449 | 17.523 | 7,170 | 2,048 | 2,048 | 106,496 | 57,360 |

Every MetricsQL shape evaluates the bucket expression exactly once through
the same public packed-raw query as the stable single-quantile reference.
One versus two requested ranks has identical candidate chunks, decoded
points, persisted payload bytes, and packed-frame bytes. Each extra rank adds
only bounded Rust interpolation, collision checking, result accounting, and
response output; it never repeats the storage read. The two-rank wide result
doubles cardinality and response size as expected.

Against the same-run stable reference, one-rank MetricsQL p95 is 14.5% higher
narrow and 13.3% higher wide. Two-rank p95 is 24.3% higher narrow and 12.4%
higher wide; the wide ordering between one and two ranks is run noise, while
the 17.523 ms two-rank p99 honestly records the largest evaluator/response
tail. The storage counters are byte-identical, so no extension primitive can
remove a read, decode, frame, or row crossing. Direct SQLite/libSQL users have
the executable cumulative-bucket foundation in `SQL-MQL-012`; destination,
rank-label, `vmrange`, repair, and response semantics remain language-owned.

All 36,928 points completed durably with zero failed or queued work. Admission
took 8.664 ms and the explicit durability barrier took 93.081 ms. Metrics
storage remained 224,688 payload bytes, 409,600 index bytes, and 1,542,312
physical database/WAL/SHM bytes. Logs remained 1,088,919 logical and 1,190,496
physical bytes. Metrics RSS HWM was 51,980 KiB, 1,044 KiB above MQL-10; logs
HWM was 63,792 KiB, 1,176 KiB below it. Both are whole-process variations and
neither direction is attributed to the added query shapes.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 75 pinned VictoriaLogs 1.52.0 cases pass. The complete 90-test
metrics real-extension suite, 60 metrics library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 25-test Rust query
harness, documentation contracts, and all 79 SQL recipes (110 statements)
pass locally. Regressions cover all documented semantics plus stable PromQL
isolation, one shared public bucket read, work/result/response limits,
deadline cancellation and recovery, GET/POST, durability, shutdown, and
reopen. No extension primitive, private table access, storage format,
batching, compression, index, rollup, retention, transaction, migration,
maintenance, or public batch/SQL contract changed. No CI workflow was
invoked.
