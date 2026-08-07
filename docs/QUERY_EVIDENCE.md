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

## Session 16 LogsQL P2 progress: structural pattern matching

The checked-in
[`2026-08-05_session16_lql_f11_pattern_match.json`](evidence/2026-08-05_session16_lql_f11_pattern_match.json)
was captured from exact extension, metrics-server, and logs-server build
`2f4b2c5d8d0e1623da69d41a943b3cad8e517b60`. It closes `LQL-F11` with
full-message and nested typed-field pattern shapes; correctness separately
pins all four anchors, seven placeholders, Unicode word categories, restart
behavior, unknown literals, case-insensitive function names, strict grammar,
missing/null/empty projection, limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| message pattern, indexed host | 128 | 21,826 | 2.066 | 2.329 | 2.548 | 1 | 1,024 | 132,676 | 128 |
| message pattern, full fixture | 8,192 | 1,424,639 | 21.148 | 23.139 | 24.710 | 4 | 8,192 | 1,088,919 | 8,192 |
| nested numeric-field pattern, indexed host | 128 | 21,826 | 2.166 | 2.381 | 2.515 | 1 | 1,024 | 132,676 | 128 |
| nested numeric-field pattern, full fixture | 8,192 | 1,424,639 | 24.838 | 28.023 | 28.990 | 4 | 8,192 | 1,088,919 | 8,192 |

The message pattern's 2.329/23.139 ms narrow/wide p95 is within run noise of
word matching at 2.346/23.149 ms and regexp matching at 2.722/22.602 ms.
Nested typed-field projection is 2.2%/21.1% above message matching at
narrow/wide p95, but the storage counters are byte-identical: projection and
placeholder evaluation add bounded Rust work after the same public row read.
There is no measured block-read, decode, allocation, or row-crossing saving
that would justify adding LogsQL syntax or a specialized pattern primitive to
the extension. Direct SQLite/libSQL users retain the general public row
surface; no misleading ordinary-SQL equivalent is claimed because SQLite
`LIKE`/`GLOB` cannot implement these placeholder and Unicode-word semantics.

All 8,192 log entries completed durably with zero queued work. Admission took
9.341 ms and the explicit durability barrier took 21.256 ms. Logs storage
remained four raw blocks, 1,088,919 logical bytes, and 1,190,496 physical
database/WAL/SHM bytes. Logs RSS HWM was 64,812 KiB, 1,020 KiB above the
preceding MQL-12 capture. Metrics storage remained byte-identical; its HWM was
53,884 KiB, 1,904 KiB higher. Both are whole-process variations and neither is
attributed to the pattern matcher.

The first exact-build evidence attempt exposed the non-reproduced raw rich-log
decoder incident recorded as `QSF-112`. It is not counted as fixed. The added
regression performs 48 complete 8,192-row reads across both SQLite readers,
raw storage, shutdown/reopen, and compressed storage, plus two complete
pattern reads. Future evidence failures name their exact query and preserve
the database and server log; the storage decoder remains fail-closed and is
never retried into apparent success.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 98 pinned VictoriaLogs 1.52.0 cases pass. The complete 17-test
logs real-extension suite, 39 logs library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 28-test Rust query
harness, documentation contracts, and all 79 SQL recipes (110 statements)
pass locally. No extension primitive, private table access, storage format,
batching, compression, index, rollup, retention, transaction, migration,
maintenance, or public batch/SQL contract changed. No CI workflow was
invoked.

## Session 16 LogsQL P2 progress: exact-prefix matching

The checked-in
[`2026-08-05_session16_lql_f16_exact_prefix.json`](evidence/2026-08-05_session16_lql_f16_exact_prefix.json)
was captured from exact extension, metrics-server, and logs-server build
`e169a9d310890f72e07f98b002e8cefea84eaeb7`. It closes `LQL-F16` with
case-sensitive, start-anchored message and nested typed-field shapes.
Correctness separately pins operator and function forms, absence of word-
boundary searching, arbitrary UTF-8, strict errors, compact rich-value
projection without storage mutation, empty-prefix behavior, logical/pipeline
composition, limits, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| message exact-prefix, indexed host | 128 | 21,826 | 1.919 | 2.103 | 2.326 | 1 | 1,024 | 132,676 | 128 |
| message exact-prefix, full fixture | 8,192 | 1,424,639 | 20.829 | 21.985 | 24.625 | 4 | 8,192 | 1,088,919 | 8,192 |
| nested numeric exact-prefix, indexed host | 25 | 4,264 | 1.744 | 1.935 | 2.037 | 1 | 1,024 | 132,676 | 128 |
| nested numeric exact-prefix, full fixture | 1,639 | 285,036 | 17.631 | 18.252 | 18.539 | 4 | 8,192 | 1,088,919 | 8,192 |

The message form is 7.6% below the same-run word filter at narrow p95 and
0.2% above it at wide p95; its storage work and result cardinality are
identical. It is also 1.8%/9.1% below the same-run pattern matcher and
16.4%/7.9% below regexp narrow/wide p95. These differences are bounded API
predicate cost and run variation, not block pruning. The nested numeric shape
uses the same storage read but emits 80% fewer rows, so its lower latency is
not evidence of a hidden typed-prefix index. Ordinary `SQL-LOG-014` covers
message and retained-text prefixes. No storage-read, decode, allocation, or
row-crossing saving justifies a new extension primitive.

All 8,192 entries completed durably with zero queued work. Admission took
9.166 ms and the explicit durability barrier took 20.208 ms. Logs storage
remained four raw blocks, 1,088,919 logical bytes, and 1,190,496 physical
database/WAL/SHM bytes. Logs RSS HWM was 65,912 KiB, 1,100 KiB above the
preceding capture. Metrics storage remained byte-identical and its HWM was
53,684 KiB, 200 KiB lower. Both are whole-process variations and neither is
attributed to exact-prefix matching.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 114 pinned VictoriaLogs 1.52.0 cases pass. The complete 18-test
logs real-extension suite, 41 logs library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 28-test Rust query
harness, documentation contracts, and all 80 SQL recipes (112 statements)
pass locally. No extension primitive, private table access, storage format,
batching, compression, index, rollup, retention, transaction, migration,
maintenance, or public batch/SQL contract changed. No CI workflow was
invoked.

## Session 16 LogsQL P2 progress: static multi-exact membership

The checked-in
[`2026-08-05_session16_lql_f17_multi_exact.json`](evidence/2026-08-05_session16_lql_f17_multi_exact.json)
was captured from exact extension, metrics-server, and logs-server build
`e782043dc662111f4f6f68b001da64fe9b3a7f00`. It closes `LQL-F17` with
case-sensitive static membership over messages and nested typed fields.
Correctness separately pins quoted/unquoted values, commas and pipes inside
quotes, literal versus standalone wildcard behavior, case-insensitive
function names, empty and duplicate lists, rich textual projection without
storage mutation, logical/pipeline composition, explicit subquery deferral,
malformed errors, limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| two message values, indexed host | 2 | 336 | 1.897 | 2.077 | 2.195 | 1 | 1,024 | 132,676 | 128 |
| two message values, full fixture | 2 | 337 | 13.726 | 15.273 | 19.808 | 4 | 8,192 | 1,088,919 | 8,192 |
| two nested numeric values, indexed host | 51 | 8,699 | 1.971 | 2.235 | 2.338 | 1 | 1,024 | 132,676 | 128 |
| two nested numeric values, full fixture | 3,277 | 569,896 | 21.488 | 22.864 | 24.095 | 4 | 8,192 | 1,088,919 | 8,192 |

The two-value message form is 7.2% below the same-run exact filter at narrow
p95 and 0.1% below it at wide p95, with byte-identical storage work. Its wide
p95 is 36.2% below word matching because it serializes two rows instead of
8,192, not because it prunes another block. Nested numeric membership reads
the same candidates and payload but emits 40% of the fixture; its 10.7%
increase over same-run one-value exact-prefix wide p95 is likewise output and
predicate work. Generic rich membership cannot soundly use the string-only
posting index without losing numeric, boolean, array, object, missing, or null
matches. `SQL-LOG-015` exposes existing hidden-column `IN` pruning to direct
users who declare a string-only index key and ordinary bounded message/text
recipes for the general case. No new extension primitive is justified.

All 8,192 entries completed durably with zero queued work. Admission took
10.954 ms and the explicit durability barrier took 20.493 ms. Logs storage
remained four raw blocks, 1,088,919 logical bytes, and 1,190,496 physical
database/WAL/SHM bytes. Logs RSS HWM was 65,780 KiB, 132 KiB below the prior
capture; metrics HWM was 52,820 KiB, 864 KiB lower. Both are whole-process
variations, and metrics storage remained byte-identical.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 131 pinned VictoriaLogs 1.52.0 cases pass. The complete 19-test
logs real-extension suite, 42 logs library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 28-test Rust query
harness, documentation contracts, and all 81 SQL recipes (115 statements)
pass locally. No private table, storage format, batching, compression, index,
rollup, retention, transaction, migration, maintenance, or public batch/SQL
contract changed. No CI workflow was invoked.

## Session 16 LogsQL P2 progress: field-independent no-op filters

The checked-in
[`2026-08-05_session16_lql_f20_field_noop.json`](evidence/2026-08-05_session16_lql_f20_field_noop.json)
was captured from exact extension, metrics-server, and logs-server build
`e824450234b6f3cc57faa9d342696b222b3b532b`. It closes `LQL-F20` with the
field-independent true predicate produced by any standalone unquoted wildcard
inside `in`, `contains_any`, or `contains_all`. Correctness separately pins
missing fields, mixed lists, quoted-star separation, case-insensitive names,
service/level alias routing, logical/pipeline composition, malformed errors,
explicit LQL-F21/LQL-F22/LQL-F38 boundaries, limits, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| missing-field `contains_any(*)`, indexed host | 128 | 21,826 | 2.125 | 2.344 | 2.390 | 1 | 1,024 | 132,676 | 128 |
| missing-field `contains_all(*)`, full fixture | 8,192 | 1,424,639 | 21.647 | 23.509 | 25.121 | 4 | 8,192 | 1,088,919 | 8,192 |

Narrow p95 is within 0.1% of the same-run empty-field query and 5.9% below
the any-value query. Wide p95 is 2.7% above the same-run word query, 11.4%
below empty-field matching, and 14.3% below any-value matching. Every compared
shape with the same host/full-fixture cardinality reads byte-identical public
storage and emits the same response bytes, so the differences are bounded
predicate/run variation. Direct SQLite/libSQL users express the operation by
omitting the field predicate, as executable `SQL-LOG-016` demonstrates. A new
extension primitive would be strictly redundant.

All 8,192 entries completed durably with zero queued work. Admission took
8.834 ms and the explicit durability barrier took 21.073 ms. Logs storage
remained four raw blocks, 1,088,919 logical bytes, and 1,190,496 physical
database/WAL/SHM bytes. Logs RSS HWM was 65,892 KiB, 112 KiB above the prior
capture; metrics HWM was 49,440 KiB, 3,380 KiB lower. Both are whole-process
variations, and metrics storage remained byte-identical.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 142 pinned VictoriaLogs 1.52.0 cases pass. The complete 19-test
logs real-extension suite, 43 logs library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 28-test Rust query
harness, documentation contracts, and all 82 SQL recipes (116 statements)
pass locally. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was invoked.

## Session 16 LogsQL P2 progress: static `contains_all`

The checked-in
[`2026-08-05_session16_lql_f21_contains_all.json`](evidence/2026-08-05_session16_lql_f21_contains_all.json)
was captured from exact extension, metrics-server, and logs-server build
`16b372ac31bd01e18dafd3aa29244577a0fcf993`. It closes `LQL-F21` with static,
case-sensitive all-phrase matching over messages and retained rich fields.
Correctness separately pins Unicode word boundaries, independent arguments,
empty-list/empty-string identity, duplicates, trailing commas, quoted stars,
case-insensitive function names, missing fields, service/level aliases,
logical/pipeline composition, malformed errors, explicit LQL-F38 deferral,
limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| message `contains_all(query, contract)`, indexed host | 128 | 21,826 | 1.895 | 2.073 | 2.290 | 1 | 1,024 | 132,676 | 128 |
| message `contains_all(query, contract)`, full fixture | 8,192 | 1,424,639 | 21.561 | 26.408 | 31.261 | 4 | 8,192 | 1,088,919 | 8,192 |
| rich object `contains_all(attempt, retry)`, indexed host | 128 | 21,826 | 1.997 | 2.664 | 3.339 | 1 | 1,024 | 132,676 | 128 |
| rich object `contains_all(attempt, retry)`, full fixture | 8,192 | 1,424,639 | 28.772 | 30.442 | 31.148 | 4 | 8,192 | 1,088,919 | 8,192 |

Message p95 is 15.1% below the same-run word query at narrow cardinality and
11.0% above it over the full fixture; it is within 0.5% of the narrow field
no-op and 16.3% above its wide run. Rich-object p95 is 22.5%/11.1% above the
same-run rich-pattern narrow/wide comparison because each row is compact-JSON
projected and checked against two phrases. Every equal-cardinality comparison
reads byte-identical public storage and emits the same response bytes. The
measured difference is bounded language work after decode, not evidence of a
missing storage primitive.

Portable SQLite has no exact substitute for the required Unicode-category
phrase boundaries. `LIKE`, `GLOB`, and `instr` can implement intentionally
different substring contracts but are not documented as LogsQL parity. A new
extension scalar or query vector would not avoid the public row decode and is
therefore rejected under the extension-primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
8.811 ms and the explicit durability barrier took 22.812 ms. Logs storage
remained four raw blocks, 1,088,919 logical bytes, and 1,190,496 physical
database/WAL/SHM bytes. Logs RSS HWM was 65,004 KiB, 888 KiB below the prior
capture; metrics HWM was 53,124 KiB, 3,684 KiB higher. Both are whole-process
variations, and metrics storage remained byte-identical.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 159 pinned VictoriaLogs 1.52.0 cases pass. The complete 19-test
logs real-extension suite, 44 logs library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 28-test Rust query
harness, documentation contracts, and all 82 SQL recipes (116 statements)
pass locally. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was invoked.

## Session 16 LogsQL P2 progress: static `contains_any`

The checked-in
[`2026-08-05_session16_lql_f22_contains_any.json`](evidence/2026-08-05_session16_lql_f22_contains_any.json)
was captured from exact extension, metrics-server, and logs-server build
`61ddbe72416f6858fb546c1fb6ace9c2f88ba3bc`. It closes `LQL-F22` with static,
case-sensitive any-phrase matching over messages and retained rich fields.
Correctness separately pins Unicode word boundaries, empty-list false,
empty-value true, duplicates, trailing commas, quoted stars, case-insensitive
function names, missing fields, numeric/boolean/array/object textual
projection, service/level aliases, logical/pipeline composition, malformed
errors, explicit LQL-F38 deferral, limits, cancellation, durability, and
reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| message `contains_any(query, absent)`, indexed host | 128 | 21,826 | 1.977 | 2.143 | 2.426 | 1 | 1,024 | 132,676 | 128 |
| message `contains_any(query, absent)`, full fixture | 8,192 | 1,424,639 | 20.831 | 22.301 | 23.563 | 4 | 8,192 | 1,088,919 | 8,192 |
| rich object `contains_any(attempt, absent)`, indexed host | 128 | 21,826 | 1.943 | 2.383 | 3.374 | 1 | 1,024 | 132,676 | 128 |
| rich object `contains_any(attempt, absent)`, full fixture | 8,192 | 1,424,639 | 24.998 | 27.391 | 28.512 | 4 | 8,192 | 1,088,919 | 8,192 |

Message p95 is 1.3% above the same-run word query at narrow cardinality and
4.0% below it over the full fixture; it is 6.7% above/13.3% below
`contains_all`. Rich-object p95 is 9.6%/8.0% below same-run `contains_all`
and 17.8%/5.8% above rich pattern matching. Every equal-cardinality comparison
reads byte-identical public storage and emits the same response bytes. The
measured differences are bounded language work and run variation after decode,
not evidence of a missing storage primitive.

Portable SQLite has no exact substitute for the required Unicode-category
phrase boundaries. `LIKE`, `GLOB`, and `instr` can implement intentionally
different substring contracts but are not documented as LogsQL parity. A new
extension scalar or query vector would not avoid the public row decode and is
therefore rejected under the extension-primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
8.593 ms and the explicit durability barrier took 20.232 ms. Logs storage
remained four raw blocks, 1,088,919 logical bytes, and 1,190,496 physical
database/WAL/SHM bytes. Logs RSS HWM was 64,604 KiB, 400 KiB below the prior
capture; metrics HWM was 52,284 KiB, 840 KiB lower. Both are whole-process
variations, and metrics storage remained byte-identical.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 179 pinned VictoriaLogs 1.52.0 cases pass. The complete 19-test
logs real-extension suite, 46 logs library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 28-test Rust query
harness, documentation contracts, and all 82 SQL recipes (116 statements)
pass locally. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was invoked.

## Session 16 LogsQL P2 progress: JSON-array primitive membership

The checked-in
[`2026-08-05_session16_lql_f23_json_array_contains_any.json`](evidence/2026-08-05_session16_lql_f23_json_array_contains_any.json)
was captured from exact extension, metrics-server, and logs-server build
`075d166e962482d2be3aa7622250b13c9dd51768`. It closes `LQL-F23` with strict
`json_array_contains_any(...)` membership over top-level retained JSON
primitives. Correctness separately pins strings, numbers, booleans, null,
empty list/value behavior, nested-value exclusion, non-array fields, escapes,
quoted/unquoted stars, aliases, logical/pipeline composition, malformed
errors, work limits, cancellation, durability, and reopen. Timeless's decoded
semantic-JSON distinction from VictoriaLogs' raw-lexeme shortcut is explicit.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| string `tags:json_array_contains_any(query, absent)`, indexed host | 128 | 24,642 | 2.238 | 2.416 | 2.475 | 1 | 1,024 | 155,204 | 128 |
| string `tags:json_array_contains_any(query, absent)`, full fixture | 8,192 | 1,604,863 | 30.333 | 33.611 | 33.788 | 4 | 8,192 | 1,269,143 | 8,192 |
| boolean `tags:json_array_contains_any(true, absent)`, indexed host | 128 | 24,642 | 2.231 | 2.447 | 2.482 | 1 | 1,024 | 155,204 | 128 |
| boolean `tags:json_array_contains_any(true, absent)`, full fixture | 8,192 | 1,604,863 | 29.961 | 33.549 | 34.581 | 4 | 8,192 | 1,269,143 | 8,192 |

Array-membership p95 is within 2.0% of the same-run word query when indexed
narrow and 16.1–16.3% above it over all 8,192 rows. It is 11.0–12.2% below the
non-equivalent phrase `contains_any` query narrow and 4.5–4.7% above it wide.
Every equal-cardinality comparison reads the same public blocks, entries, and
payload bytes and emits the same response bytes. The measured wide difference
is bounded array/type inspection after decode, not storage amplification.

Direct SQLite/libSQL users receive the exact retained-type operation through
executable `SQL-LOG-017`, which applies public `json_each` to a bounded `logs`
read, filters top-level primitive types, binds every candidate, and applies
the result limit after membership. Its regressions cover every primitive,
decoded escapes, nested/scalar/missing values, empty lists, and post-filter
limits. This ordinary SQL foundation plus unchanged storage work rejects a
new extension primitive under the documented gate.

All 8,192 entries completed durably with zero queued work. Admission took
10.162 ms and the explicit durability barrier took 24.293 ms. Adding the
two-element `tags` field to every evidence row increased logical payload and
wide response size by exactly 180,224 bytes; logs storage is four raw blocks,
1,269,143 logical bytes, and 1,371,776 physical database/WAL/SHM bytes. Logs
RSS HWM was 71,996 KiB, 7,392 KiB above LQL-F22 after four additional full-
response shapes and wider rows; metrics HWM was 50,732 KiB, 1,552 KiB lower.
Both are retained as whole-process variation, and metrics storage is byte-
identical.

All 528 pinned Prometheus 3.13.2 cases, all 184 pinned VictoriaMetrics 1.148.0
cases, and all 203 pinned VictoriaLogs 1.52.0 cases pass. The complete 19-test
logs real-extension suite, 47 logs library tests, both complete Rust
workspaces, Clippy with warnings denied, formatting, the 29-test Rust query
harness, documentation contracts, and all 83 SQL recipes (118 statements)
pass locally. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was invoked.

## Session 16 LogsQL P2 progress: IPv4 range filtering

The checked-in
[`2026-08-05_session16_lql_f25_ipv4_range.json`](evidence/2026-08-05_session16_lql_f25_ipv4_range.json)
was captured from exact extension, metrics-server, and logs-server build
`9081a326cbe9a23be8b877f2cae36b11251becf5`. It closes `LQL-F25` with
inclusive unsigned IPv4 matching over exact retained strings. Correctness
separately pins one address, CIDR and explicit bounds, network/broadcast
edges, host-bit normalization, `/0`, decimal leading zeroes, inverted ranges,
whole-string parsing, message/arbitrary fields, aliases, logical/pipeline
composition, missing/null/non-string/invalid/embedded non-matches, strict
errors, work limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `client_ip:ipv4_range(10.0.0.0/19)`, indexed host | 128 | 27,834 | 2.706 | 3.147 | 3.508 | 1 | 1,024 | 181,028 | 128 |
| `client_ip:ipv4_range(10.0.0.0/19)`, full fixture | 8,192 | 1,811,775 | 33.505 | 37.651 | 38.765 | 4 | 8,192 | 1,476,055 | 8,192 |
| explicit `10.0.0.0` through `10.0.31.255`, indexed host | 128 | 27,834 | 2.679 | 2.827 | 2.870 | 1 | 1,024 | 181,028 | 128 |
| explicit `10.0.0.0` through `10.0.31.255`, full fixture | 8,192 | 1,811,775 | 33.150 | 37.312 | 39.197 | 4 | 8,192 | 1,476,055 | 8,192 |

CIDR p95 is 4.9% above the same-run word query at narrow cardinality and
16.0% above it over the full fixture. Explicit-bound p95 is 5.8% below/15.0%
above the same word comparison. Every equal-cardinality shape reads the same
public blocks, entries, and payload bytes and emits the same response bytes.
The difference is bounded address parsing/evaluation after decode, not storage
amplification.

Direct SQLite/libSQL users receive the exact retained-string operation through
executable `SQL-LOG-018`. It strictly parses four decimal octets with public
JSON1, packs them in unsigned network order, binds inclusive bounds, and
applies the result limit after filtering. The API remains responsible for
address/CIDR grammar, normalization, composition, limits, cancellation, and
error envelopes. Identical storage work plus this ordinary SQL foundation
rejects a new extension primitive under the documented gate.

All 8,192 entries completed durably with zero queued work. Admission took
13.441 ms and the explicit durability barrier took 27.556 ms. Adding exact
`client_ip` to every evidence row increased logical payload and wide response
size by 206,912 bytes; logs storage is four raw blocks, 1,476,055 logical
bytes, and 1,581,896 physical database/WAL/SHM bytes. Logs RSS HWM was 75,200
KiB, 3,204 KiB above LQL-F23; metrics HWM was 52,152 KiB, 1,420 KiB higher.
Both are retained as whole-process variation, and metrics storage is otherwise
unchanged.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 221
pinned VictoriaLogs 1.52.0 cases pass live. The complete 20-test logs
real-extension suite, 48 logs library tests, both complete Rust workspaces,
Clippy with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, and all 84 SQL recipes (119 statements) pass
locally. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was invoked.

## Session 16 LogsQL P2 progress: IPv6 range filtering

The checked-in
[`2026-08-05_session16_lql_f26_ipv6_range.json`](evidence/2026-08-05_session16_lql_f26_ipv6_range.json)
was captured from exact extension, metrics-server, and logs-server build
`756ec7068ca25aef82ea6b1f7d6aa6f4c45b0c97`. It closes `LQL-F26` with
inclusive unsigned 16-byte IP matching over exact retained strings.
Correctness separately pins compressed and uppercase spelling, one address,
CIDR and explicit bounds, network/broadcast edges, host-bit normalization,
`/0`, inverted ranges, IPv4-mapped addresses and 128-bit prefix semantics,
message/arbitrary fields, aliases, logical/pipeline composition,
missing/null/non-string/invalid/embedded non-matches, strict errors, work
limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `client_ipv6:ipv6_range(2001:db8::/115)`, indexed host | 128 | 31,733 | 2.993 | 3.290 | 4.035 | 1 | 1,024 | 212,226 | 128 |
| `client_ipv6:ipv6_range(2001:db8::/115)`, full fixture | 8,192 | 2,061,359 | 38.217 | 39.210 | 39.368 | 4 | 8,192 | 1,725,639 | 8,192 |
| explicit `2001:db8::` through `2001:db8::1fff`, indexed host | 128 | 31,733 | 3.016 | 3.961 | 4.220 | 1 | 1,024 | 212,226 | 128 |
| explicit `2001:db8::` through `2001:db8::1fff`, full fixture | 8,192 | 2,061,359 | 38.043 | 40.015 | 41.268 | 4 | 8,192 | 1,725,639 | 8,192 |

CIDR p95 is 5.6% above the same-run word query at narrow cardinality and 6.1%
above it over the full fixture. Explicit-bound p95 is 27.1%/8.3% above the
same word comparison; the narrow run retained one 4.220 ms p99/max tail.
Every equal-cardinality shape reads the same public blocks, entries, and
payload bytes and emits the same response bytes. The difference is bounded
16-byte parsing and comparison after decode, not storage amplification.

Portable SQLite has no built-in IPv6 parser, so no copyable statement over the
current exact-string storage can honestly promise LogsQL equivalence. Users
with an application-owned canonical packed-address column can compare it in
ordinary SQL, but that is a different stored schema. The Rust API therefore
owns address/CIDR parsing, IPv4 mapping, normalization, composition, limits,
cancellation, and errors. A new extension scalar would not remove any measured
storage read, decode, allocation, copy, or row crossing, so it is rejected by
the documented primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
12.689 ms and the explicit durability barrier took 34.611 ms. Adding exact
`client_ipv6` to every evidence row increased logical payload and wide
response size by 249,584 bytes; logs storage is four raw blocks, 1,725,639
logical bytes, and 1,829,096 physical database/WAL/SHM bytes. Logs RSS HWM was
82,484 KiB, 7,284 KiB above LQL-F25 after four additional full-response shapes
and wider rows; metrics HWM was 51,804 KiB, 348 KiB lower. Both are retained
as whole-process variation, and metrics storage is otherwise unchanged.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 240
pinned VictoriaLogs 1.52.0 cases pass live. The complete 21-test logs
real-extension suite, 49 logs library tests, both complete Rust workspaces,
Clippy with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, and all 84 SQL recipes (119 statements) pass
locally. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was invoked.

## Session 16 LogsQL P2 progress: bytewise string-range filtering

The checked-in
[`2026-08-05_session16_lql_f27_string_range.json`](evidence/2026-08-05_session16_lql_f27_string_range.json)
was captured from exact extension, metrics-server, and logs-server build
`de7ccf8bf53b07c9ae7e381a1c595d442028f8a4`. It closes `LQL-F27` with
lower-inclusive/upper-exclusive plain-byte comparison over the selected rich
textual projection. Correctness separately pins equal-prefix and inverted
ranges, ASCII case, UTF-8, quoted commas, missing/null/empty, numbers,
booleans, arrays, retained objects, nested/message/service fields, aliases,
logical/pipeline composition, strict errors, work limits, cancellation,
durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `range_key:string_range(key-0000,key-2000)`, indexed host | 128 | 34,677 | 3.231 | 3.471 | 3.680 | 1 | 1,024 | 235,778 | 128 |
| `range_key:string_range(key-0000,key-2000)`, full fixture | 8,192 | 2,249,775 | 41.442 | 46.353 | 46.944 | 4 | 8,192 | 1,914,055 | 8,192 |
| numeric `context.attempt:string_range(0,5)`, indexed host | 128 | 34,677 | 3.321 | 3.581 | 3.661 | 1 | 1,024 | 235,778 | 128 |
| numeric `context.attempt:string_range(0,5)`, full fixture | 8,192 | 2,249,775 | 43.428 | 45.926 | 47.882 | 4 | 8,192 | 1,914,055 | 8,192 |

The retained-string p95 is 4.3% below the same-run word query at narrow
cardinality and 7.3% above it over the full fixture. Typed numeric projection
is 1.3% below/6.3% above that word comparison. Every equal-cardinality shape
reads the same public blocks, entries, and bytes and returns the same public
rows. The difference is bounded comparison/projection after decode, not
storage amplification.

Executable `SQL-LOG-019` exposes the exact retained-text and missing/null-as-
empty foundation with public `logs` rows, JSON1, and BLOB byte ordering. The
Rust API owns rich projection, grammar, composition, limits, cancellation,
and response semantics. VictoriaLogs flattens an ingested object into dotted
children before filtering; Timeless preserves the complete object and compact-
projects a selected parent only for the predicate. An extension primitive
would not remove any measured block read, decode, allocation, copy, or row
crossing, so it is rejected by the documented primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
13.515 ms and the explicit durability barrier took 37.430 ms. Adding exact
`range_key` to every evidence row increased wire, logical payload, and wide
response size by 188,416 bytes; logs storage is four raw blocks, 1,914,055
logical bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs RSS HWM was
92,412 KiB, 9,928 KiB above LQL-F26 after four additional full-response shapes
and wider rows; metrics HWM was 53,488 KiB, 1,684 KiB higher. Both are retained
as whole-process variation rather than attributed to the predicate.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 263
pinned VictoriaLogs 1.52.0 cases pass live. The complete 22-test logs real-
extension suite, 50 logs library tests, both complete Rust workspaces, Clippy
with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, and all 85 SQL recipes (120 statements) pass
locally. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed.

## Session 16 LogsQL P2 progress: Unicode codepoint length ranges

The checked-in
[`2026-08-05_session16_lql_f28_len_range.json`](evidence/2026-08-05_session16_lql_f28_len_range.json)
was captured from exact extension, metrics-server, and logs-server build
`e624a15faf99690975515d7958428819f37aad84`. It closes `LQL-F28` with
inclusive Unicode-codepoint counts over the selected rich textual projection.
Correctness separately pins multibyte characters, missing/null/empty length
zero, strings, numbers, booleans, arrays, retained objects, nested/message/
service fields, base-prefixed and human-readable unsigned bounds, `inf`,
negative zero, inverted ranges, aliases, logical/pipeline composition, strict
errors, work limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `range_key:len_range(8,8)`, indexed host | 128 | 34,677 | 3.486 | 3.786 | 3.826 | 1 | 1,024 | 235,778 | 128 |
| `range_key:len_range(8,8)`, full fixture | 8,192 | 2,249,775 | 46.252 | 48.626 | 49.298 | 4 | 8,192 | 1,914,055 | 8,192 |
| numeric `context.attempt:len_range(1,1)`, indexed host | 128 | 34,677 | 3.257 | 4.335 | 4.933 | 1 | 1,024 | 235,778 | 128 |
| numeric `context.attempt:len_range(1,1)`, full fixture | 8,192 | 2,249,775 | 42.206 | 47.566 | 47.903 | 4 | 8,192 | 1,914,055 | 8,192 |

The retained-string p95 is 5.4% below the same-run word query at narrow
cardinality and 12.1% below it over the full fixture. Typed numeric projection
is 8.3% above/14.0% below that word comparison. Against the more similar
same-run `string_range`, retained-string length is 7.3% above/1.5% below and
typed length is 13.7% above/3.3% below at narrow/wide cardinality. These are
single-run CPU/tail variations after byte-identical public reads, not evidence
of storage amplification or a missing query vector.

Executable `SQL-LOG-020` exposes exact inclusive Unicode-codepoint length for
retained text and missing/null-as-empty through public `logs` rows, JSON1, and
SQLite `length(TEXT)`. The Rust API owns rich projection, VictoriaLogs bound
grammar, composition, limits, cancellation, and response semantics.
VictoriaLogs flattens an ingested object into dotted children before
filtering; Timeless preserves the complete object and compact-projects a
selected parent only for the predicate. An extension primitive would not
remove any measured block read, decode, allocation, copy, or row crossing, so
it is rejected by the documented primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
15.488 ms and the explicit durability barrier took 40.488 ms. The fixture and
storage are byte-identical to LQL-F27: four raw blocks, 1,914,055 logical
bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs RSS HWM was 91,544
KiB, 868 KiB below LQL-F27 despite four additional full-response query shapes;
metrics HWM was 53,608 KiB, 120 KiB higher. Both are retained as whole-process
variation rather than attributed to the predicate.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 290
pinned VictoriaLogs 1.52.0 cases pass live. The complete 23-test logs real-
extension suite, 51 logs library tests, both complete Rust workspaces, Clippy
with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, all 86 SQL recipes (121 statements), and the full
45-section CLI/crash suite pass locally. No private table, extension
primitive, storage format, batching, compression, index, rollup, retention,
transaction, migration, maintenance, or public batch/SQL contract changed.
No CI workflow was modified or invoked.

## Session 16 LogsQL P2 progress: same-row field comparisons

The checked-in
[`2026-08-05_session16_lql_f30_field_comparisons.json`](evidence/2026-08-05_session16_lql_f30_field_comparisons.json)
was captured from exact extension, metrics-server, and logs-server build
`afd7804edb5d438a3aa400b1f358a0e287b648ae`. It closes `LQL-F30` with exact
same-row textual equality and VictoriaLogs math-value-or-bytewise ordering.
Correctness separately pins missing/null/empty and rich projections, exact
retained integers beyond 2^53, decimal/base-zero/duration/byte-size/RFC3339/
IPv4 math values, quoted/message/service/nested/right-`_time` fields, aliases,
logical/pipeline composition, strict errors, work limits, cancellation,
durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `range_key:eq_field(range_key)`, indexed host | 128 | 34,677 | 2.954 | 3.311 | 3.849 | 1 | 1,024 | 235,778 | 128 |
| `range_key:eq_field(range_key)`, full fixture | 8,192 | 2,249,775 | 41.398 | 43.262 | 43.369 | 4 | 8,192 | 1,914,055 | 8,192 |
| numeric `context.attempt:lt_field(status)`, indexed host | 128 | 34,677 | 2.925 | 3.332 | 3.419 | 1 | 1,024 | 235,778 | 128 |
| numeric `context.attempt:lt_field(status)`, full fixture | 8,192 | 2,249,775 | 41.496 | 44.726 | 45.952 | 4 | 8,192 | 1,914,055 | 8,192 |

Exact equality p95 is 8.3% below the same-run word query at narrow
cardinality and 0.3% above it over the full fixture. Exact retained-number
ordering is 7.7% below/3.7% above that word comparison. Every equal-
cardinality shape reads the same public blocks, entries, and bytes and returns
the same public rows. The difference is bounded projection/comparison after
decode, not storage amplification.

Executable `SQL-LOG-021` exposes complete retained-model equality and the
exact bytewise ordering fallback with public `logs` rows and JSON1. Its
numeric projection uses JSON `->`, not `json_extract()`, so adjacent unsigned
64-bit values above `i64::MAX` remain distinguishable. The Rust API owns the
VictoriaLogs math-value branch, grammar, composition, limits, cancellation,
and response semantics. VictoriaLogs flattens ingested objects before
filtering; Timeless preserves the complete object and compact-projects a
selected parent only for the predicate. Both operands already cross the same
decoded row, so an extension primitive would not remove a measured block
read, decode, allocation, copy, or row crossing and is rejected by the
documented primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
15.355 ms and the explicit durability barrier took 38.909 ms. The fixture,
wire bytes, and storage are byte-identical to LQL-F28: four raw blocks,
1,914,055 logical bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs
RSS HWM was 91,788 KiB, 244 KiB above LQL-F28; metrics HWM was 53,472 KiB, 136
KiB lower. Both are retained as whole-process variation rather than
attributed to the predicates.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 314
pinned VictoriaLogs 1.52.0 cases pass live. The complete 24-test logs real-
extension suite, 52 logs library tests, both complete Rust workspaces, Clippy
with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, and all 87 SQL recipes (122 statements) pass
locally. The executable SQL command is the Rust harness used by CLI section
45. No private table, extension primitive, storage format, batching,
compression, index, rollup, retention, transaction, migration, maintenance,
or public batch/SQL contract changed. No CI workflow was modified or invoked.

## Session 16 LogsQL P2 progress: prefix-selected field sets

The checked-in
[`2026-08-05_session16_lql_f32_field_prefixes.json`](evidence/2026-08-05_session16_lql_f32_field_prefixes.json)
was captured from exact extension, metrics-server, and logs-server build
`94e1cd4cd715b6ec2a86e580324cf50057943332`. It closes `LQL-F32` with lazy
any-matching-field expansion over literal canonical field-name prefixes.
Correctness separately pins empty and quoted prefixes, punctuation, canonical
special fields, recursively dotted rich-object leaves, retained arrays/null,
independent logical-group operands, `NOT`, current-row pipeline projection,
strict wildcard-comparison errors, work limits, cancellation, durability, and
reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `range_*:key`, indexed host | 128 | 34,677 | 2.964 | 3.122 | 3.224 | 1 | 1,024 | 235,778 | 128 |
| `range_*:key`, full fixture | 8,192 | 2,249,775 | 43.999 | 49.085 | 51.898 | 4 | 8,192 | 1,914,055 | 8,192 |
| `context.*:value_type(uint64)`, indexed host | 128 | 34,677 | 2.948 | 3.216 | 3.523 | 1 | 1,024 | 235,778 | 128 |
| `context.*:value_type(uint64)`, full fixture | 8,192 | 2,249,775 | 42.424 | 47.324 | 48.799 | 4 | 8,192 | 1,914,055 | 8,192 |

Word-prefix p95 is 1.7% above the same-run word query at narrow cardinality and
31.0% above it over the full fixture. Typed-prefix p95 is 0.1% below/2.3%
above the equivalent `value_type` query. Every equal-cardinality shape reads
the same public blocks, entries, and bytes and returns the same public rows.
The wide word-search difference is bounded canonical-name traversal and word
evaluation over decoded row fields, not storage amplification.

Executable `SQL-LOG-022` exposes literal prefix selection plus exact retained
string/null matching through public `logs` rows and recursive JSON1. It gives
each bounded row a query-local identity so multiple matching fields do not
duplicate a row while otherwise identical stored rows remain distinct. The
Rust API owns word, phrase, range, rich-value, RFC3339 `_time`, composition,
limits, cancellation, and response semantics. Field expansion stops at the
first match, retains only one recursive path, and observes cancellation at
each node. An extension primitive would not remove any measured block read,
decode, allocation, copy, or row crossing, so it is rejected by the documented
primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
12.420 ms and the explicit durability barrier took 34.737 ms. The fixture,
wire bytes, and storage are byte-identical to LQL-F30: four raw blocks,
1,914,055 logical bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs
RSS HWM was 92,760 KiB, 972 KiB above LQL-F30 after four additional full-
response shapes; metrics HWM was 51,324 KiB, 2,148 KiB lower. Both are retained
as whole-process variation rather than attributed to the predicates.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 336
pinned VictoriaLogs 1.52.0 cases pass live. The complete 25-test logs real-
extension suite, 53 logs library tests, complete Rust server workspace, Clippy
with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, and all 88 SQL recipes (123 statements) pass
locally. The executable SQL command is the Rust harness used by CLI section
45. No private table, extension primitive, storage format, authoritative
batching, compression, index, rollup, retention, transaction, migration,
maintenance, or public batch/SQL contract changed. No CI workflow was modified
or invoked.

## Session 16 LogsQL P2 progress: UTC day ranges with fixed offsets

The checked-in
[`2026-08-05_session16_lql_f33_day_range.json`](evidence/2026-08-05_session16_lql_f33_day_range.json)
was captured from exact extension, metrics-server, and logs-server build
`aa7bc3001dfb354faccf2c2c2d4f3197b9391d6d`. It closes `LQL-F33` with exact
UTC time-of-day brackets plus an optional fixed signed duration offset.
Correctness separately pins `HH:MM`/`HHMM`, open/closed instants, positive,
negative, and compound offsets, `24:00`, minute 60, midnight full-day, equal
and inverted ranges, deterministic omitted-offset UTC, current-row pipelines,
strict errors, work limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `_time:day_range[08:00,09:00)`, indexed host | 128 | 34,677 | 2.980 | 3.697 | 5.221 | 1 | 1,024 | 235,778 | 128 |
| `_time:day_range[08:00,09:00)`, full fixture | 8,192 | 2,249,775 | 35.894 | 37.123 | 38.867 | 4 | 8,192 | 1,914,055 | 8,192 |

Day-range p95 is 7.3% below the same-run word query at narrow cardinality and
14.9% below it over the full fixture. Narrow p99 is 5.221 ms versus the word
query's 4.427 ms; wide p99 is 38.867 ms versus 48.032 ms. The narrow tail is
retained as run variation rather than discarded. Both equal-cardinality paths
read the same public blocks, entries, and bytes and return the same public
rows, so none of these CPU/tail differences indicate storage amplification.

Executable `SQL-LOG-023` exposes the native timestamp remainder, exact bracket
tests, open-midnight normalization, and explicit fixed offset over bounded
public `logs` rows. It parameterizes millisecond and microsecond storage units.
The Rust API owns clock and signed compound-duration grammar, deterministic UTC
default, logical/current-row pipeline composition, limits, cancellation, and
response semantics. A repeated daily predicate cannot independently prune an
arbitrary absolute timestamp interval, and an extension primitive would not
remove any measured block read, decode, allocation, copy, or row crossing, so
it is rejected by the documented primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
13.389 ms and the explicit durability barrier took 35.154 ms. The fixture,
wire bytes, and storage are byte-identical to LQL-F32: four raw blocks,
1,914,055 logical bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs
RSS HWM was 92,868 KiB, 108 KiB above LQL-F32 after two additional full-
response shapes; metrics HWM was 53,592 KiB, 2,268 KiB higher. Both are retained
as whole-process variation rather than attributed to the predicate.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 359
pinned VictoriaLogs 1.52.0 cases pass live. The complete 26-test logs real-
extension suite, 55 logs library tests, complete Rust server workspace, Clippy
with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, and all 89 SQL recipes (124 statements) pass
locally. The executable SQL command is the Rust harness used by CLI section
45. No private table, extension primitive, storage format, authoritative
batching, compression, index, rollup, retention, transaction, migration,
maintenance, or public batch/SQL contract changed. No CI workflow was modified
or invoked.

## Session 16 LogsQL P2 progress: UTC week ranges with fixed offsets

The checked-in
[`2026-08-05_session16_lql_f34_week_range.json`](evidence/2026-08-05_session16_lql_f34_week_range.json)
was captured from exact extension, metrics-server, and logs-server build
`adb7f027f4a6c5e0d07d8ff3eb3b294e85277818`. It closes `LQL-F34` with
case-insensitive short/full English weekdays, exact open/closed bracket
normalization, and an optional fixed signed duration offset over UTC.
Correctness separately pins Sunday-through-Saturday linear ranges, full-week,
equal, and valid-empty edges, positive/negative/compound offsets,
deterministic omitted-offset UTC, pre-epoch native timestamps, current-row
pipelines, strict errors, work limits, cancellation, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `_time:week_range[Fri,Fri]`, indexed host | 128 | 34,677 | 3.235 | 3.547 | 3.691 | 1 | 1,024 | 235,778 | 128 |
| `_time:week_range[Fri,Fri]`, full fixture | 8,192 | 2,249,775 | 36.841 | 39.768 | 40.239 | 4 | 8,192 | 1,914,055 | 8,192 |

Week-range p95 is 1.4% below the same-run word query at narrow cardinality and
3.4% below it over the full fixture. Narrow p99 is 3.691 ms versus the word
query's 3.703 ms; wide p99 is 40.239 ms versus 41.347 ms. Both equal-
cardinality paths read the same public blocks, entries, and bytes and return
the same public rows, so none of the CPU/tail variation indicates storage
amplification.

Executable `SQL-LOG-024` exposes Euclidean native-timestamp day calculation,
normalized inclusive weekday bounds, and explicit fixed offsets over bounded
public `logs` rows. It parameterizes millisecond and microsecond storage units
and pins pre-epoch behavior despite SQLite's truncating integer division. The
Rust API owns weekday, bracket, and signed compound-duration grammar,
deterministic UTC default, logical/current-row pipeline composition, limits,
cancellation, and response semantics. A repeated weekly predicate cannot
independently prune an arbitrary absolute timestamp interval, and an extension
primitive would not remove any measured block read, decode, allocation, copy,
or row crossing, so it is rejected by the documented primitive gate.

All 8,192 entries completed durably with zero queued work. Admission took
14.819 ms and the explicit durability barrier took 41.395 ms. The fixture,
wire bytes, and storage are byte-identical to LQL-F33: four raw blocks,
1,914,055 logical bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs
RSS HWM was 91,072 KiB, 1,796 KiB below LQL-F33; metrics HWM was 49,376 KiB,
4,216 KiB lower. Both are retained as whole-process variation rather than
attributed to the predicate.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 382
pinned VictoriaLogs 1.52.0 cases pass live. The complete 27-test logs real-
extension suite, 57 logs library tests, complete Rust server workspace, Clippy
with warnings denied, formatting, the 29-test Rust query harness,
documentation contracts, and all 90 SQL recipes (125 statements) pass
locally. The executable SQL command is the Rust harness used by CLI section
45. No private table, extension primitive, storage format, authoritative
batching, compression, index, rollup, retention, transaction, migration,
maintenance, or public batch/SQL contract changed. No CI workflow was modified
or invoked.

## Session 16 LogsQL P2 closeout: comments and multiline source

The checked-in
[`2026-08-05_session16_lql_f40_comments_multiline.json`](evidence/2026-08-05_session16_lql_f40_comments_multiline.json)
was captured from exact extension, metrics-server, and logs-server build
`c23bd23428c2a87e49d60e57ac121b8a346ce3c7`. It closes `LQL-F40` and Session
16 with VictoriaLogs-compatible attached/leading/trailing comments, LF/CRLF
multiline composition, literal hashes inside double/single/backtick literals,
one optional terminal semicolon, and strict malformed tails. Correctness
separately pins one-based lexical line/Unicode-column errors, comment-erased
arguments, ordinary-query no-copy behavior, request bounds, work-limit reader
reuse, durability, and reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| comments/multiline/semicolon, indexed host | 128 | 34,677 | 3.010 | 3.335 | 3.504 | 1 | 1,024 | 235,778 | 128 |
| comments/multiline/semicolon, full fixture | 8,192 | 2,249,775 | 36.100 | 39.610 | 41.707 | 4 | 8,192 | 1,914,055 | 8,192 |

The commented form's p95 is 4.8% below the same-run narrow word query and 4.3%
below it over the full fixture. Narrow p99 is 3.504 ms versus 3.597 ms; wide
p99 is 41.707 ms versus 45.926 ms. Both equal-cardinality paths read the same
public blocks, entries, and bytes and return the same public rows. The timing
difference is retained as run variation rather than credited to the bounded
source scan.

This row intentionally has no SQL recipe. Comments, multiline layout, and a
terminal semicolon are LogsQL source grammar in the Rust API; direct
SQLite/libSQL users already write ordinary parameterized SQL. The common path
borrows source without a normalization copy, and only comments or a terminal
semicolon allocate a request-bounded same-length copy. No parser syntax enters
SQLite, and no extension primitive could remove a measured storage read,
decode, allocation, copy, or row crossing.

All 8,192 entries completed durably with zero queued work. Admission took
13.256 ms and the explicit durability barrier took 38.833 ms. The fixture,
wire bytes, and storage are byte-identical to LQL-F34: four raw blocks,
1,914,055 logical bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs
RSS HWM was 92,216 KiB, 1,144 KiB above LQL-F34; metrics HWM was 53,180 KiB,
3,804 KiB higher. Both are retained as whole-process variation rather than
attributed to parser source layout.

The unchanged 528-case Prometheus 3.13.2 and 184-case VictoriaMetrics 1.148.0
fixtures remain covered by their existing product regressions, and all 402
pinned VictoriaLogs 1.52.0 cases pass live. The complete 28-test logs real-
extension suite, 58 logs library tests, complete extension and Rust server
workspaces, Clippy with warnings denied, formatting, the 29-test Rust query
harness, documentation contracts, and all 90 SQL recipes (125 statements)
pass locally. No private table, extension primitive, storage format,
authoritative batching, compression, index, rollup, retention, transaction,
migration, maintenance, or public batch/SQL contract changed. No CI workflow
was modified or invoked.

## Session 17 LogsQL P2 progress: delete pipelines

The checked-in
[`2026-08-05_session17_lql_p07_delete.json`](evidence/2026-08-05_session17_lql_p07_delete.json)
was captured from exact extension, metrics-server, and logs-server build
`05e40bf722245817ec6d1ae6338332a762707e31`. It closes `LQL-P07` with
case-insensitive `delete`/`del`/`drop`/`rm` aliases; exact, quoted, prefix,
special, nested, and all-field deletion; atomic retained arrays/scalars;
empty-parent and empty-row pruning; ordered current-row composition; strict
comma/wildcard errors; work limits; recursive cancellation; durability; and
reopen.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exact plus nested-prefix delete, indexed host | 128 | 26,912 | 3.510 | 4.011 | 4.769 | 1 | 1,024 | 235,778 | 128 |
| exact plus nested-prefix delete, full fixture | 8,192 | 1,752,794 | 44.485 | 45.768 | 46.688 | 4 | 8,192 | 1,914,055 | 8,192 |

Deletion p95 is 16.9%/17.6% above the same-run narrow/wide word queries,
whose p95 values are 3.431/38.916 ms. Narrow p99 is 4.769 ms versus 4.522
ms; wide p99 is 46.688 ms versus 42.769 ms. The transform removes
`context.*` and `range_key`, reducing response bytes by 22.4%/22.1%. Every
equal-cardinality shape reads the same public blocks, entries, and payload
bytes and crosses the same public rows. The measured cost is recursive
in-place rich-object mutation and response reconstruction after decode, not
storage amplification.

Executable `SQL-LOG-025` gives direct SQLite/libSQL users exact retained-
metadata-path deletion with public JSON1. LogsQL aliases, quoted and prefix
grammar, formatted special fields, recursive empty-parent pruning, fully
empty-row omission, pipeline composition, limits, cancellation, and envelopes
remain bounded Rust API behavior. No extension primitive can eliminate the
measured block read, decode, or row crossing, so the primitive gate rejects
one.

All 8,192 entries completed durably with zero queued work. Admission took
17.214 ms and the explicit durability barrier took 35.190 ms. The fixture,
wire bytes, and storage are byte-identical to LQL-F40: four raw blocks,
1,914,055 logical bytes, and 2,022,736 physical database/WAL/SHM bytes. Logs
RSS HWM was 91,612 KiB, 604 KiB below LQL-F40; metrics HWM was 50,440 KiB,
2,740 KiB lower. Both are retained as whole-process variation rather than
credited to deletion.

All 428 pinned VictoriaLogs 1.52.0 cases pass live. The complete 29-test logs
real-extension suite, 60 logs library tests, complete extension and Rust server
workspaces, all 45 CLI sections, Clippy with warnings denied, formatting, the
29-test Rust query harness, documentation contracts, and all 91 SQL recipes
(126 statements) pass locally. No private table, extension primitive, storage
format, authoritative batching, compression, index, rollup, retention,
transaction, migration, maintenance, or public batch/SQL contract changed. No
CI workflow was modified or invoked.

## Session 17 LogsQL P2: request-local query statistics

The checked-in pre-fast-path
[`2026-08-05_session17_lql_p12_query_stats_before_fast_path.json`](evidence/2026-08-05_session17_lql_p12_query_stats_before_fast_path.json)
was captured from exact build
`fc6281c5f2ba815b13feb0558c94f4dfa8e22dcc`. It exposed an avoidable API-only
cost: a first `query_stats` pipe formatted all matched rich logs into response
JSON before replacing them with the one report row. The checked-in final
[`2026-08-05_session17_lql_p12_query_stats_fast_path.json`](evidence/2026-08-05_session17_lql_p12_query_stats_fast_path.json)
was captured from exact optimized build
`6530dc232010e3e4169fdc9e95154c22b68f4a4d` after a failing-then-passing
regression removed that discarded conversion while preserving ordered behavior
when another transform precedes `query_stats`.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | matched storage rows/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `query_stats`, indexed host | 1 | 380 | 3.082 | 3.436 | 3.586 | 1 | 1,024 | 235,778 | 128 |
| `query_stats`, full fixture | 1 | 385 | 24.078 | 25.046 | 27.026 | 4 | 8,192 | 1,914,055 | 8,192 |
| word/full-row control, indexed host | 128 | 34,677 | 3.538 | 4.811 | 5.175 | 1 | 1,024 | 235,778 | 128 |
| word/full-row control, full fixture | 8,192 | 2,249,775 | 38.137 | 41.649 | 42.763 | 4 | 8,192 | 1,914,055 | 8,192 |

The final `query_stats` endpoint p95 is 28.6%/39.9% below its same-run
narrow/wide full-row control because the physical scan ends in one 380/385-byte
report rather than a 34,677/2,249,775-byte row response. The internal API query
timer averages 2.413/23.122 ms versus 2.528/23.856 ms for those controls, so
the result no longer hides discarded formatting work. Against the exact
pre-fast-path capture, internal query time fell 2.6%/27.9%. Cross-run narrow
endpoint p95 rose 5.3% while its control rose 40.9%; that is retained as run
variation, and only same-run comparisons are used for the verdict.

Every equal-width pair performs identical storage work. The extension report
comes from query-local counters before cumulative publication, is scoped by
SQLite connection and table, and is consumed once. It reports the one/four
candidate and processed blocks, 1,024/8,192 decoded entries,
235,778/1,914,055 payload bytes, and 128/8,192 storage matches actually
performed. The Rust API supplies complete typed post-filter `RowsFound`,
duration through the pipe position, fourteen string fields, later-pipeline
composition, and strict errors. `SQL-LOG-026` exposes all sixteen native
INTEGER counters and the executable compatibility mapping to direct
SQLite/libSQL users.

All 8,192 rich entries completed durably with zero queued work. Admission took
16.514 ms and the explicit durability barrier took 38.385 ms. Both captures
are byte-identical at four raw blocks, 1,914,055 logical payload bytes, and
2,022,736 physical database/WAL/SHM bytes. Final logs HWM was 92,480 KiB,
1,584 KiB below the baseline; the maximum spans the complete workload and is
retained as whole-process variation rather than credited to one query path.
Cancellation finished with zero requests in flight.

All 435 pinned VictoriaLogs 1.52.0 cases pass live. The final complete 30-test
logs real-extension suite, 63 logs library tests, 45-section CLI/crash/
transaction suite, 29-test Rust query harness, documentation contracts,
Clippy with warnings denied, formatting, and all 92 SQL recipes (128
statements) pass locally. The extension's authoritative 8,192-entry batching,
storage formats, compression, indexes, retention, optimize, transactions,
migrations, and public batch/SQL contracts are unchanged. No private shadow
table, Elixir/BEAM/NIF/HTTP fallback, CI workflow, tag, release, or downstream
repository was used or modified.

## Session 17 LogsQL P2: bounded `first`

The checked-in
[`2026-08-05_session17_lql_p13_first.json`](evidence/2026-08-05_session17_lql_p13_first.json)
was captured from exact release build
`9225cc54db38e66707cd0d04837fb656cfc778ea`. The feature shapes select the
first eight rows by numeric `context.attempt` and natural `range_key`, partition
them into two narrow or eight wide groups, insert a partition-local string
rank, and project a rich response. Same-run controls return the same 16/64
cardinality through the established simpler `_time` sort; they are a
storage/cardinality comparison, not a claim that the sort expressions are
semantically interchangeable.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| partitioned/ranked `first`, indexed host | 16 | 2,604 | 3.295 | 3.681 | 3.867 | 1 | 1,024 | 235,778 | 128 |
| partitioned/ranked `first`, full fixture | 64 | 11,864 | 42.508 | 44.182 | 44.321 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 16 | 2,349 | 2.880 | 3.153 | 3.224 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 10,766 | 33.997 | 37.107 | 40.241 | 4 | 8,192 | 1,914,055 | 8,192 |

The `first` p95 is 16.8%/19.1% above its narrow/wide same-run control. Its
internal API timer averages 2.738/40.870 ms versus 2.368/33.224 ms. That
measured cost is bounded Rust composition: exact numeric and natural
comparison, partition grouping, top-eight selection, rank insertion, and rich
projection. The extra rank field makes the response 10.9%/10.2% larger. Every
equal-width pair performs exactly the same public storage scan, block decode,
payload read, and row materialization. There is no evidence that a new
extension primitive would avoid storage work; `SQL-LOG-027` already gives
direct SQLite/libSQL users the honest numeric window-rank foundation.

All 8,192 rich entries completed durably with zero queued work. Admission took
12.670 ms and the explicit durability barrier took 36.348 ms. Storage remains
four raw blocks, 1,914,055 logical payload bytes, and 2,022,736 physical
database/WAL/SHM bytes, exactly matching LQL-P12. Logs HWM was 93,732 KiB,
1,252 KiB above LQL-P12; metrics HWM was 53,612 KiB, 4,296 KiB above it. Each
maximum spans the enlarged complete workload and is recorded as whole-process
variation rather than attributed to one `first` request. Cancellation ended
with zero requests in flight. The evaluator checks cancellation before key
construction, periodically during key/partition/output work, inside long sort
comparison loops, and after sorting; the direct rejection regression and
state-limit HTTP regression prove failure plus reader reuse.

All 453 pinned VictoriaLogs v1.52.0 cases pass live. The final 31-test logs
real-extension suite, 67 logs library tests, complete extension and Rust
server workspaces, 45-section CLI/crash/transaction suite, 29-test Rust query
harness, documentation contracts, Clippy with warnings denied, formatting,
and all 93 SQL recipes (129 statements) pass locally. The extension's
authoritative 8,192-entry batching, storage formats, compression, indexes,
retention, optimize, transactions, migrations, and public batch/SQL contracts
are unchanged. No private shadow table, Elixir/BEAM/NIF/process fallback, CI
workflow, tag, release, or downstream repository was used or modified.

## Session 17 LogsQL P2: bounded `last`

The checked-in
[`2026-08-05_session17_lql_p14_last.json`](evidence/2026-08-05_session17_lql_p14_last.json)
was captured from exact release build
`d2bdf12e2b8d91d04cb8716c860fe4105a86428d`. The feature shapes apply the
same exact two-field coercion, two/eight partitions, top-eight selection,
partition-local string rank, and rich projection as `first`, then reverse the
complete comparison after each field's direction. Same-run `first` controls
use the identical parser/state/evaluator path without the final reversal.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| partitioned/ranked `last`, indexed host | 16 | 2,605 | 2.910 | 3.060 | 3.338 | 1 | 1,024 | 235,778 | 128 |
| partitioned/ranked `last`, full fixture | 64 | 11,864 | 42.841 | 46.268 | 47.424 | 4 | 8,192 | 1,914,055 | 8,192 |
| partitioned/ranked `first` control, indexed host | 16 | 2,604 | 3.123 | 3.290 | 3.455 | 1 | 1,024 | 235,778 | 128 |
| partitioned/ranked `first` control, full fixture | 64 | 11,864 | 40.796 | 44.012 | 47.294 | 4 | 8,192 | 1,914,055 | 8,192 |

The `last` p95 is 7.0% below/5.1% above its narrow/wide same-run `first`
control. Its internal API timer averages 2.461/41.788 ms versus
2.729/40.222 ms, or 9.8% below/3.9% above. The narrow response differs by one
byte because the selected reverse-ordered rich values differ; wide response
size is identical. Every equal-width pair performs exactly the same public
storage scan, block decode, payload read, row materialization, bounded key and
partition construction, rank insertion, and projection. The only operation
difference is the final comparator direction. The small bidirectional
variation does not justify a new extension primitive.

All 8,192 rich entries completed durably with zero queued work. Admission took
16.515 ms and the explicit durability barrier took 34.900 ms. Storage remains
four raw blocks, 1,914,055 logical payload bytes, and 2,022,736 physical
database/WAL/SHM bytes. Logs HWM was 93,484 KiB, 248 KiB below LQL-P13;
metrics HWM was 53,244 KiB, 368 KiB below it. Each maximum spans the complete
workload and is retained as whole-process variation. Cancellation ended with
zero requests in flight.

All 471 pinned VictoriaLogs v1.52.0 cases pass live. The final 32-test logs
real-extension suite, 69 logs library tests, complete extension and Rust
server workspaces, 45-section CLI/crash/transaction suite, 29-test Rust query
harness, documentation contracts, Clippy with warnings denied, formatting,
and all 94 SQL recipes (130 statements) pass locally. The extension's
authoritative 8,192-entry batching, storage formats, compression, indexes,
retention, optimize, transactions, migrations, and public batch/SQL contracts
are unchanged. No private shadow table, Elixir/BEAM/NIF/process fallback, CI
workflow, tag, release, or downstream repository was used or modified.

## Session 17 LogsQL P2: bounded `top`

The checked-in
[`2026-08-05_session17_lql_p15_top.json`](evidence/2026-08-05_session17_lql_p15_top.json)
was captured from exact release build
`2bb2f4dd0046574e116ae05b6d75a77cef04ef20`. The narrow shape scans one
indexed host and counts its five textual `context.attempt` groups. The wide
shape scans all 8,192 entries and counts the eight actual `(service, level)`
groups. Both add explicit default-name hits and a global string rank. Same-run
controls scan the identical source rowsets and return the same five/eight
cardinality through the established `_time` sort and projection. They are
storage/cardinality controls, not semantic equivalents to frequency grouping.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| frequency `top`, indexed host | 5 | 255 | 3.143 | 3.385 | 3.510 | 1 | 1,024 | 235,778 | 128 |
| frequency `top`, full fixture | 8 | 531 | 34.501 | 35.948 | 36.855 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 5 | 130 | 3.153 | 3.330 | 3.352 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 8 | 299 | 35.285 | 38.060 | 38.448 | 4 | 8,192 | 1,914,055 | 8,192 |

The `top` p95 is 1.6% above/5.5% below its narrow/wide same-run control. Its
internal API timer averages 2.491/33.367 ms versus 2.416/34.119 ms, or
3.1% above/2.2% below. Responses are 96.2%/77.6% larger because `top` emits both
the counted string hits and requested string rank; this is intended summary
data, not storage amplification. Every equal-width pair performs exactly the
same public storage scan, block decode, payload read, and row materialization.
The measured bounded textual grouping/key sort is accepted in the Rust API.
`SQL-LOG-029` already gives direct SQLite/libSQL users the ordinary public
`GROUP BY` and window-rank foundation, so a new extension primitive would not
avoid storage work.

The first capture attempt stopped before recording evidence because the API
rejected explicit `hits as hits`. Source audit and a live pinned-oracle case
proved VictoriaLogs accepts the redundant default alias. The parser, unit,
real-extension, and oracle regressions now pin it; the retained failed-run
diagnostic led directly to the exact `QSF-157` correction.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.167 ms and the explicit durability barrier took 38.939 ms. Storage remains
four raw blocks, 1,914,055 logical payload bytes, and 2,022,736 physical
database/WAL/SHM bytes. Logs HWM was 96,884 KiB, 3,400 KiB above LQL-P14;
metrics HWM was 53,100 KiB, 144 KiB below it. Each maximum spans the enlarged
complete workload and is retained as whole-process variation rather than
attributed to one `top` request. Cancellation ended with zero requests in
flight; direct evaluator and HTTP limit regressions pin cancellation, bounded
work/group/result/state, rejection, and reader reuse.

All 490 pinned VictoriaLogs v1.52.0 cases pass live. The final 33-test logs
real-extension suite, 71 logs library tests, complete extension and Rust
server workspaces, 29-test Rust query harness, documentation contracts,
Clippy with warnings denied, formatting, and all 95 SQL recipes (131
statements) pass locally. The extension's authoritative 8,192-entry batching,
storage formats, compression, indexes, retention, optimize, transactions,
migrations, and public batch/SQL contracts are unchanged. No private shadow
table, Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `sample`

The checked-in
[`2026-08-06_session18_lql_p17_sample.json`](evidence/2026-08-06_session18_lql_p17_sample.json)
was captured from exact release extension and server build
`930a1f1c3dd9e4b016456b24c685bf25480bab53` and has SHA-256
`d819f24d43610606fb4fdb4b88027323464d27ea5f9ca32023dd06babbf741ea`.
Both shapes end in a scalar count so stochastic row identity does not make
result cardinality or response transport dominate the comparison. `sample 4`
is compared with exact `sample 1`, which follows the same public row path but
retains every row.

The first attempted wide control used plain `stats count()` and its counters
revealed that it took the extension's native-count reduction: 50 native-count
calls versus 50 full public scans. That capture was rejected rather than
presented as a 30x regression. The harness now fails unless both sample/control
pairs execute one public query per iteration, avoid native count, and report
identical requested entries, candidate blocks, decoded entries, payload bytes,
matched entries, and returned entries.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows returned/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `sample 4`, indexed host | 1 | 13 | 3.027 | 3.657 | 3.910 | 1 | 1,024 | 235,778 | 128 |
| exact `sample 1` control, indexed host | 1 | 14 | 3.140 | 3.307 | 3.447 | 1 | 1,024 | 235,778 | 128 |
| `sample 4`, full fixture | 1 | 15 | 25.179 | 26.060 | 26.533 | 4 | 8,192 | 1,914,055 | 8,192 |
| exact `sample 1` control, full fixture | 1 | 15 | 32.412 | 33.206 | 33.678 | 4 | 8,192 | 1,914,055 | 8,192 |

Narrow sample p95 is 10.6% above its control even though its request-attributed
API timer averages 2.303 ms versus 2.410 ms, or 4.4% lower. The small endpoint
tail is therefore retained as run variation. Wide sample p95 is 21.5% lower,
and its internal API timer averages 24.233 ms versus 31.213 ms, or 22.4% lower.
The wide improvement is consistent with first-stage in-place sampling: all
public blocks and rows still cross the storage boundary, but roughly three
quarters of rows are discarded before rich metadata JSON materialization and
the following scalar reduction. Query materialization/storage time is nearly
identical within each pair; this is API allocation/decode work avoided after
the public scan, not extension pushdown.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.065 ms and the explicit durability barrier took 36.813 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 100,376 KiB, 4,404 KiB below
LQL-F41 despite four additional repeated shapes; metrics HWM was 49,696 KiB,
3,452 KiB below it. Both are whole-process workload variation rather than
query-local attribution. Cancellation ended with zero requests in flight;
the real-extension regression pins deadline cancellation, reader reuse, work,
result, response, optimize, flush, shutdown, and reopen behavior.

All 1,015 pinned VictoriaLogs v1.52.0 cases pass live. The final 58-test logs
real-extension suite, 114 logs library/binary tests, 90-test metrics
real-extension suite, complete extension and Rust server workspaces,
45-section CLI/crash/transaction suite, six focused correctness sections,
standalone dbhealth lifecycle gate, 32-test Rust query harness, documentation
contracts, Clippy with warnings denied, formatting, and all 115 SQL recipes
(153 statements) pass locally. The extension's authoritative 8,192-entry
batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: deterministic `pack_logfmt`

The checked-in
[`2026-08-06_session18_lql_p35_pack_logfmt.json`](evidence/2026-08-06_session18_lql_p35_pack_logfmt.json)
was captured from exact release extension and server build
`fd723df39e72500f6e80126131f4e5a9bf0763a8` and has SHA-256
`9ec0ff4b3c3c6939a0e834f5193955309915d39fe141847c10f1356ce3256e20`.
The narrow shape selects one indexed host, packs `range_key` into one
`range_key=value` destination, and returns 64 rows. The wide shape applies the
same operation to all 8,192 entries before its 64-row limit. Same-run controls
use `format` to produce byte-identical destinations and responses.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `pack_logfmt`, indexed host | 64 | 2,048 | 3.459 | 3.805 | 4.335 | 1 | 1,024 | 235,778 | 128 |
| `pack_logfmt`, full fixture | 64 | 2,048 | 37.975 | 39.144 | 42.387 | 4 | 8,192 | 1,914,055 | 8,192 |
| identical-output `format`, indexed host | 64 | 2,048 | 3.506 | 3.769 | 3.924 | 1 | 1,024 | 235,778 | 128 |
| identical-output `format`, full fixture | 64 | 2,048 | 34.373 | 38.212 | 38.572 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 1.0%/2.4% above its narrow/wide same-run control. Its
internal API timer averages 2.695/36.696 ms versus 2.701/33.927 ms, or 0.2%
below/8.2% above. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, row materialization, and response
encoding. Dynamic selector traversal, deterministic union/order, textual
projection, conditional quoting, and destination writes are bounded Rust API
work. `SQL-LOG-052` already gives direct SQLite/libSQL users the fixed-schema
core-SQL/JSON1 foundation, so an extension primitive would not avoid storage
reads, decode, allocation, or row crossing. The wide API mean is retained
honestly.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.621 ms and the explicit durability barrier took 36.633 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 100,608 KiB, 1,320 KiB above
LQL-P31; metrics HWM was 50,900 KiB, 568 KiB below it. Each maximum spans the
enlarged complete workload and is retained as whole-process variation.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin work/state/result/response bounds, rejection,
cancellation, and reader reuse.

All 1,111 pinned VictoriaLogs v1.52.0 cases pass live. The final 63-test logs
real-extension suite, 122 logs library tests, 90-test metrics real-extension
suite, complete 45-section CLI/crash/transaction suite, standalone dbhealth
lifecycle gate, 32-test Rust query harness, documentation contracts, Clippy
with warnings denied, formatting, and all 118 SQL recipes (156 statements)
pass locally. The extension's authoritative 8,192-entry batching, storage
formats, compression, indexes, retention, optimize, transactions, migrations,
and public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: string `unpack_logfmt`

The checked-in
[`2026-08-06_session18_lql_p37_unpack_logfmt.json`](evidence/2026-08-06_session18_lql_p37_unpack_logfmt.json)
was captured from exact release extension and server build
`66167687b3b4bd464e67f086554914fd468bb4c5` and has SHA-256
`abd74c43658cae37414899199842e6435fcfcb0020d7776bcbfd31b12c359d5c`.
Each shape first packs `range_key` into identical logfmt. The candidate then
decodes that exact key into `decoded_range_key`; its control copies the
original typed value to the same destination. Both return byte-identical
responses, so the comparison isolates bounded logfmt parsing, selection, and
destination construction after common packing and public storage work.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `unpack_logfmt`, indexed host | 64 | 2,112 | 3.355 | 3.619 | 4.494 | 1 | 1,024 | 235,778 | 128 |
| `unpack_logfmt`, full fixture | 64 | 2,112 | 41.166 | 42.437 | 43.357 | 4 | 8,192 | 1,914,055 | 8,192 |
| identical-output copy control, indexed host | 64 | 2,112 | 3.328 | 3.573 | 4.213 | 1 | 1,024 | 235,778 | 128 |
| identical-output copy control, full fixture | 64 | 2,112 | 38.087 | 41.659 | 44.310 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 1.3%/1.9% above its narrow/wide same-run control. Its
request-attributed API timer averages 2.805/40.023 ms versus
2.771/37.806 ms, or 1.2%/5.9% above. Every equal-width pair performs
byte-identical public storage scans, block decodes, payload reads, row
materialization, and response encoding. The candidate's lower wide p99 is
retained alongside the higher median and mean rather than interpreted as an
optimization. Quote/escape decoding, dynamic selection, string allocation,
nested path construction, and destination writes are bounded Rust API work.
`SQL-LOG-053` gives direct SQLite/libSQL users the bounded fixed-key unquoted
foundation, so an extension parser would not avoid storage reads, decode,
allocation, or row crossing.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.079 ms and the explicit durability barrier took 36.142 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 96,524 KiB, 4,084 KiB below
LQL-P35 despite four additional repeated shapes; metrics HWM was 49,204 KiB,
1,696 KiB below it. Each maximum is retained as whole-process workload
variation. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin parsing/work/state/result/response bounds,
rejection, cancellation, and reader reuse.

All 1,134 pinned VictoriaLogs v1.52.0 cases pass live. The final 64-test logs
real-extension suite, 126 logs library/binary tests, 90-test metrics
real-extension suite, complete root and server workspaces, 45-section
CLI/crash/transaction suite, six focused extension correctness sections,
standalone dbhealth lifecycle gate, 32-test Rust query harness, documentation
contracts, Clippy with warnings denied, formatting, and all 119 SQL recipes
(157 statements) pass locally. The extension's authoritative 8,192-entry
batching, storage formats, compression, indexes, retention, optimize,
transactions, migrations, and public batch/SQL contracts are unchanged. No
private shadow table, Elixir/BEAM/NIF/process fallback, CI workflow or
invocation, tag, release, or downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `unpack_syslog`

The checked-in
[`2026-08-06_session18_lql_p38_unpack_syslog.json`](evidence/2026-08-06_session18_lql_p38_unpack_syslog.json)
was captured from exact release extension and server build
`f43af178ba4a7d3208eb2c20d907abd7ae6b3ba5` and has SHA-256
`e0aa76b28ff5b0769ed46c2cb3a6ed3f344c4c1801f376c44e2a1c8fc42d74f4`.
Each shape sorts and materializes the full public candidate set, applies a
deterministic 64-row limit, then formats `range_key` into a no-PRI RFC5424
header. The candidate decodes that header into `decoded_message`; its control
copies the original value to the same destination after identical formatting.
Both return byte-identical responses. This isolates bounded syslog parsing and
destination construction on the returned rows after identical public storage,
sort, limit, format, and response work.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `unpack_syslog`, indexed host | 64 | 1,984 | 3.391 | 3.581 | 3.802 | 1 | 1,024 | 235,778 | 128 |
| `unpack_syslog`, full fixture | 64 | 1,984 | 33.887 | 38.081 | 38.210 | 4 | 8,192 | 1,914,055 | 8,192 |
| identical-output copy control, indexed host | 64 | 1,984 | 2.978 | 3.439 | 3.584 | 1 | 1,024 | 235,778 | 128 |
| identical-output copy control, full fixture | 64 | 1,984 | 34.075 | 38.026 | 38.209 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 4.1%/0.1% above its narrow/wide same-run control. Its
request-attributed API timer averages 2.797/33.909 ms versus
2.555/34.168 ms, or 9.5% above/0.8% below. Every equal-width pair performs
byte-identical public storage scans, block decodes, payload reads, row
materialization, deterministic sorting/limiting, common formatting, and
response encoding. The opposing wide endpoint and API-mean deltas are
retained as whole-run variation rather than interpreted as an optimization.
RFC5424 tokenization, seven decoded string fields, nested path construction,
and destination writes are bounded Rust API work. `SQL-LOG-054` gives direct
SQLite/libSQL users the fixed RFC5424-header foundation, so an extension
parser would not avoid storage reads, decode, allocation, or row crossing.

Two earlier exact-build shapes placed `limit 64` after decoding all 8,192
rows. Both correctly returned HTTP 422 `max_work_rows` under the unchanged
100,000-item default: first with twelve PRI-bearing decoded fields per row,
then with seven no-PRI fields plus shared format work. `QSF-223` preserves that
important operational boundary. The accepted shape does not raise or
undercount the limit; it applies the user's limit before expansion while
retaining the complete one-/four-block public read. Full-batch syslog
expansion requires a larger configured work budget.

All 8,192 rich entries completed durably with zero queued work. Admission took
15.809 ms and the explicit durability barrier took 35.118 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 100,244 KiB, 3,720 KiB above
LQL-P37 after four additional repeated shapes; metrics HWM was 52,800 KiB,
3,596 KiB above it. Each maximum is retained as whole-process workload
variation. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin parsing/work/state/result/response bounds,
rejection, cancellation, and reader reuse.

All 1,155 pinned VictoriaLogs v1.52.0 cases pass live. The final 65-test logs
real-extension suite, 133 logs library/binary tests, 90-test metrics
real-extension suite, complete root and server workspaces, 45-section
CLI/crash/transaction suite, six focused extension correctness sections,
standalone dbhealth lifecycle gate, 32-test Rust query harness, documentation
contracts, Clippy with warnings denied, formatting, and all 120 SQL recipes
(158 statements) pass locally. The extension's authoritative 8,192-entry
batching, storage formats, compression, indexes, retention, optimize,
transactions, migrations, and public batch/SQL contracts are unchanged. No
private shadow table, Elixir/BEAM/NIF/process fallback, CI workflow or
invocation, tag, release, or downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `unpack_words`

The checked-in
[`2026-08-06_session18_lql_p39_unpack_words.json`](evidence/2026-08-06_session18_lql_p39_unpack_words.json)
was captured from exact release extension and server build
`d3c0884cccf4896864b59ad447f7f2f0c59d6040` and has SHA-256
`fb5500640ac3b7418dc41acd67f6e802083a246f37ed32c045cb5602c82a8fe6`.
Each shape sorts and materializes the full public candidate set, applies a
deterministic 64-row limit, projects `range_key`, then either extracts its
ordered distinct words into `words` or copies it unchanged to that
destination. This isolates exact word classification, first-seen duplicate
state, compact JSON-array encoding, destination construction, and the larger
response after identical public storage, sort, limit, and source projection.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `unpack_words`, indexed host | 64 | 1,984 | 3.225 | 3.740 | 4.877 | 1 | 1,024 | 235,778 | 128 |
| `unpack_words`, full fixture | 64 | 1,984 | 36.912 | 38.564 | 39.308 | 4 | 8,192 | 1,914,055 | 8,192 |
| copy control, indexed host | 64 | 1,344 | 3.250 | 3.493 | 3.534 | 1 | 1,024 | 235,778 | 128 |
| copy control, full fixture | 64 | 1,344 | 34.241 | 35.662 | 36.075 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 7.0%/8.1% above its narrow/wide same-run control. Its
request-attributed API timer averages 2.498/34.612 ms versus
2.479/33.106 ms, or 0.8%/4.5% above. Every equal-width pair performs
byte-identical public block selection, decode, payload reads, row
materialization, deterministic sorting/limiting, and source projection. The
candidate's 640 extra response bytes honestly remain part of the endpoint
measurement. Exact Unicode Letter/Decimal_Number/underscore classification,
first-seen byte-identical deduplication, compact JSON-array allocation,
destination writes, limits, cancellation, and errors are bounded Rust API
work. Core SQLite cannot express the exact category split portably; an
extension scalar would still receive the same already-decoded public rows and
would avoid none of the measured storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.426 ms and the explicit durability barrier took 35.198 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 98,328 KiB, 1,916 KiB below
LQL-P38 after four additional repeated shapes; metrics HWM was 51,004 KiB,
1,796 KiB below it. Each maximum is retained as whole-process workload
variation. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin character/token/state/result/response
bounds, rejection, cancellation, and reader reuse.

All 1,175 pinned VictoriaLogs v1.52.0 cases pass live. The final 66-test logs
real-extension suite, 134 logs library tests, 90-test metrics
real-extension suite, complete root and server workspaces, 45-section
CLI/crash/transaction suite, six focused extension correctness sections,
standalone dbhealth lifecycle gate, 33-test Rust query harness, documentation
contracts, Clippy with warnings denied, formatting, and all 120 SQL recipes
(158 statements) pass locally. The extension's authoritative 8,192-entry
batching, storage formats, compression, indexes, retention, optimize,
transactions, migrations, and public batch/SQL contracts are unchanged. No
private shadow table, Elixir/BEAM/NIF/process fallback, CI workflow or
invocation, tag, release, or downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `json_array_concat`

The checked-in
[`2026-08-06_session18_lql_p40_json_array_concat.json`](evidence/2026-08-06_session18_lql_p40_json_array_concat.json)
was captured from exact release extension and server build
`51beb50eb359a2ac2f9d03669547dcff12f8d774` and has SHA-256
`9dd275ddc4ddc94c3129d2a5d6d2e76ab929145e80f304034fa2dfdc2d1d520c`.
Each shape sorts and materializes the full public candidate set, applies a
deterministic 64-row limit, projects the retained native `tags` array, and
then either joins it with `,` or formats the known identical `query,true`
result. Candidate and control therefore return exactly the same rows and
bytes while isolating bounded native-array traversal and destination writing
after identical public storage, sort, limit, and projection work.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `json_array_concat`, indexed host | 64 | 1,536 | 3.202 | 3.872 | 4.930 | 1 | 1,024 | 235,778 | 128 |
| `json_array_concat`, full fixture | 64 | 1,536 | 34.373 | 35.216 | 35.316 | 4 | 8,192 | 1,914,055 | 8,192 |
| equal-output format control, indexed host | 64 | 1,536 | 3.332 | 3.418 | 3.614 | 1 | 1,024 | 235,778 | 128 |
| equal-output format control, full fixture | 64 | 1,536 | 38.555 | 40.200 | 40.598 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 13.3% above the narrow control and 12.4% below the wide
control. Request-attributed API averages are 2.528/33.271 ms versus
2.557/36.315 ms, or 1.1%/8.4% lower. The opposing narrow endpoint tail and
API mean are retained as whole-run variation rather than attributed to array
traversal. Every equal-width pair performs byte-identical public block
selection, decode, payload reads, row materialization, sorting, limiting,
projection, response cardinality, and response bytes. Native traversal,
delimiter insertion, output allocation, and request-local destination writes
are bounded post-scan Rust API work. The string-backed raw-token scanner is
covered semantically rather than substituted into this representative native
fixture; it has the same unavoidable public row boundary.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.125 ms and the explicit durability barrier took 38.957 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 99,612 KiB, 1,284 KiB above
LQL-P39; metrics HWM was 52,056 KiB, 1,052 KiB above it. Both are retained as
whole-process variation after four new repeated query shapes. Cancellation
ended with zero requests in flight; direct evaluator and HTTP deadline
regressions pin JSON depth/token/state/result/response bounds, rejection,
cancellation, and reader reuse.

All 1,199 pinned VictoriaLogs v1.52.0 cases pass live. The final local gates
also pass: 136 logs library tests, 67 logs real-extension tests, 90 metrics
real-extension tests, both complete Rust workspaces, the six focused storage
correctness profiles, all 45 CLI/crash/transaction sections, DB-health, the
33-test Rust query harness, Clippy with warnings denied, and formatting. The
executable SQL cookbook contains 121 recipes and 159 statements, all of which
run through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. Direct SQLite/libSQL users have
executable canonical-array `SQL-LOG-055`; measured equal public work provides
no evidence for a new extension primitive. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `unroll`

The checked-in
[`2026-08-06_session18_lql_p42_unroll.json`](evidence/2026-08-06_session18_lql_p42_unroll.json)
was captured from exact release extension and server build
`eff28bb74482bcadcec292b27f28c4462f663a73` and has SHA-256
`49179a9003845760ec2d8bc591e9664a2beac4dd244b5b624bc517f296a69ba3`.
Each shape sorts and materializes the full public candidate set and applies a
deterministic 64-row limit. The candidate then expands the retained two-value
`tags` array into 128 rows; the control concatenates the same array and
returns 64 rows. There is no independent equal-cardinality expanding control
at this boundary, so the differing cardinality and response bytes are
reported rather than hidden. Every equal-width pair still performs identical
public block selection, decode, payload reads, row materialization, sorting,
limiting, and source projection.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| two-value `unroll`, indexed host | 128 | 2,112 | 3.552 | 3.896 | 4.049 | 1 | 1,024 | 235,778 | 128 |
| two-value `unroll`, full fixture | 128 | 2,112 | 35.071 | 39.121 | 40.075 | 4 | 8,192 | 1,914,055 | 8,192 |
| array-concat control, indexed host | 64 | 1,408 | 3.220 | 3.670 | 4.680 | 1 | 1,024 | 235,778 | 128 |
| array-concat control, full fixture | 64 | 1,408 | 34.595 | 39.180 | 39.443 | 4 | 8,192 | 1,914,055 | 8,192 |

The expansion p95 is 6.1% above the narrow control and 0.2% below the wide
control. Request-attributed API averages are 2.815/34.607 ms versus
2.599/34.616 ms, or 8.3% higher/0.0% lower. These comparisons include twice
the result cardinality and 704 additional response bytes, so they bound the
complete endpoint cost but do not isolate an array-expansion kernel. Source
snapshotting, native traversal, two output rows per selected row, allocation,
and request-local writes remain bounded post-scan Rust API work. An extension
primitive would not avoid any measured storage read, decode, payload, or
public-row crossing.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.058 ms and the explicit durability barrier took 37.563 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 101,928 KiB, 2,316 KiB above
LQL-P40; metrics HWM was 52,824 KiB, 768 KiB above it. Both are retained as
whole-process workload variation; the candidate also retains twice as many
result rows. Cancellation ended with zero requests in flight and zero
cancelled requests at capture. Direct evaluator and HTTP regressions pin
source/result/work/state/response bounds, deadline cancellation, and reader
reuse.

All 1,223 pinned VictoriaLogs v1.52.0 cases pass live. The final local gates
also pass: 138 logs library tests, 68 logs real-extension tests, 90 metrics
real-extension tests, both complete Rust workspaces, the six focused storage
correctness profiles, all 45 CLI/crash/transaction sections, DB-health, the
33-test Rust query harness, documentation contracts, Clippy with warnings
denied, and formatting. The executable SQL cookbook contains 122 recipes and
160 statements, all of which run through the public release extension. The
first complete LogsQL run exposed `QSF-232`: a scalar-parent assertion was in
the wrong fixture and selected no row. Moving it into the durable `unroll`
fixture made the promised HTTP 422 conflict observable; the complete 68-test
rerun passes without a product-code waiver.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. Direct SQLite/libSQL users have
executable single-canonical-array `SQL-LOG-056`; the measured identical public
storage work provides no evidence for a new extension primitive. No private
shadow table, Elixir/BEAM/NIF/process fallback, CI workflow or invocation,
tag, release, or downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `join`

The checked-in
[`2026-08-06_session18_lql_p43_join.json`](evidence/2026-08-06_session18_lql_p43_join.json)
was captured from exact release extension and server build
`8c718dceccfb56f251f7f54084976cc69233de7e` and has SHA-256
`5ceb485d44c1a6d074ab6d9f2cdfd7e2a2bb80345f897300a2d5d76d0f2d3431`.
Each candidate and control performs two independently planned public scans,
sorts and limits each side to 64 rows, and returns an identical 64-row,
1,280-byte response. The candidate materializes the rich right rows, indexes
their exact textual `range_key`, joins them into `joined`, and projects that
field. The control resolves the same right keys through query-backed
`in(...)`, formats the same `joined` value, and projects it. This is an honest
equal-output control for complete request-local RHS materialization, index
construction, lookup, and rich merge work after the unavoidable reads.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | public scans/request | candidate blocks/scan | decoded entries/scan | extension payload bytes/scan | public rows/scan |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| query-backed `join`, indexed host | 64 | 1,280 | 6.568 | 6.875 | 7.299 | 2 | 1 | 1,024 | 235,778 | 128 |
| query-backed `join`, full fixture | 64 | 1,280 | 66.254 | 73.000 | 73.984 | 2 | 4 | 8,192 | 1,914,055 | 8,192 |
| query-backed `in(...)` control, indexed host | 64 | 1,280 | 5.934 | 6.225 | 6.666 | 2 | 1 | 1,024 | 235,778 | 128 |
| query-backed `in(...)` control, full fixture | 64 | 1,280 | 65.809 | 72.087 | 72.452 | 2 | 4 | 8,192 | 1,914,055 | 8,192 |

Join p95 is 10.4% above the narrow control and 1.3% above the wide control.
Request-attributed API averages are 5.663/66.109 ms versus
4.935/65.786 ms, or 14.7%/0.5% higher. Every equal-width pair executes
exactly 100 public queries across 50 measured requests and has byte-identical
candidate-block selection, entry decode, payload reads, matches, public-row
returns, result cardinality, and response bytes. The join's richer retained
state deliberately reserves 192 additional work items per request—64 rows
plus two object members per row—so its outer scan requests 192 fewer entries
than the control. `QSF-234` and `QSF-235` record the two evidence failures
that made scan multiplicity and this exact state reservation explicit rather
than weakening either physical-work assertion.

All 8,192 rich entries completed durably with zero queued work. Admission took
15.861 ms and the explicit durability barrier took 36.064 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 103,508 KiB, 1,580 KiB above
LQL-P42 after four additional two-scan shapes; metrics HWM was 53,020 KiB,
196 KiB above it. Both are retained as whole-process complete-workload
variation. Cancellation ended with zero requests in flight and zero cancelled
requests at capture. RHS retention, textual indexing, duplicate buckets,
matching, and rich merge construction remain bounded Rust API work. An
extension primitive would avoid neither public scan, decode, payload crossing,
nor row crossing, so executable `SQL-LOG-057` remains the direct-user
foundation and no LogsQL-specific extension opcode is justified.

The complete 1,249-case pinned VictoriaLogs v1.52.0 corpus passes live. Direct
parser/evaluator and real-extension regressions pin strict grammar,
default-left/inner and duplicate semantics, complete retained rich fidelity,
work/state/result/response/deadline limits, cancellation, immutable source
rows, optimize, flush, shutdown, and reopen. Final local gates pass: 140 logs
library tests, two logs binary tests, all 69 logs and 90 metrics
real-extension tests, both complete Rust workspaces, the 34-test Rust query
harness, all six focused correctness profiles, the standalone DB-health
lifecycle gate, all 45 CLI/crash/transaction sections, documentation/oracle
contracts, formatting, and Clippy with warnings denied. All 123 executable SQL
recipes and 161 statements pass through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `union`

The checked-in
[`2026-08-06_session18_lql_p44_union.json`](evidence/2026-08-06_session18_lql_p44_union.json)
was captured from exact release extension and server build
`c3ce658ed9475691fc8e4a51e9fe0e02fb57fe7c` and has SHA-256
`e9d434b4e0bc6d1ede914377fcfec13984e06b88d5b430939fd9de16f83da404`.
Each candidate and control performs two independently planned public scans,
sorts and limits each side to 64 rows, and returns identical 128-row,
2,560-byte output. The candidate appends the complete rich query source. The
control resolves the same source keys through query-backed `in(...)`, writes
two identical values, unrolls them, and projects the same constant field.
This is an honest equal-output control for retaining and cloning one bounded
source after the unavoidable reads.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | public scans/request | candidate blocks/scan | decoded entries/scan | extension payload bytes/scan | public rows/scan |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| query-backed `union`, indexed host | 128 | 2,560 | 6.442 | 6.794 | 6.917 | 2 | 1 | 1,024 | 235,778 | 128 |
| query-backed `union`, full fixture | 128 | 2,560 | 67.816 | 74.143 | 75.364 | 2 | 4 | 8,192 | 1,914,055 | 8,192 |
| query-backed `in(...)`/`unroll` control, indexed host | 128 | 2,560 | 5.846 | 6.363 | 7.126 | 2 | 1 | 1,024 | 235,778 | 128 |
| query-backed `in(...)`/`unroll` control, full fixture | 128 | 2,560 | 68.604 | 73.714 | 79.489 | 2 | 4 | 8,192 | 1,914,055 | 8,192 |

Union p95 is 6.8% above the narrow control and 0.6% above the wide control.
Request-attributed API averages are 5.370/67.574 ms versus
4.931/67.884 ms, or 8.9% higher/0.5% lower. Every equal-width pair executes
exactly 100 public queries across 50 measured requests and has byte-identical
candidate-block selection, entry decode, payload reads, matches, public-row
returns, result cardinality, and response bytes. Retaining 64 one-member rich
source rows deliberately reserves 128 additional work items per request, so
the candidate's outer scan requests exactly 128 fewer entries than the
control. The evidence harness requires that exact delta as well as exact scan
multiplicity and physical storage equality.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.750 ms and the explicit durability barrier took 44.750 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 101,512 KiB, 1,996 KiB below
LQL-P43 after four additional two-scan shapes; metrics HWM was 53,460 KiB,
440 KiB above it. Both are retained as whole-process complete-workload
variation. Cancellation ended with zero requests in flight and zero cancelled
requests at capture. Retained-source traversal and cloning remain bounded
Rust API work. Ordinary `UNION ALL` already gives direct users the useful
composition, and an extension primitive would avoid neither scan, decode,
payload crossing, nor row crossing.

The complete 1,267-case pinned VictoriaLogs v1.52.0 corpus passes live. Direct
parser/evaluator and real-extension regressions pin strict query/inline
grammar, recursively query-backed sources, empty-source behavior, duplicate
preservation, deterministic Timeless left/source order, complete retained
rich fidelity, subsequent pipeline composition, cumulative
work/state/result/response/deadline limits, cancellation, immutable source
rows, optimize, flush, shutdown, and reopen. Final local gates pass: 142 logs
library tests, two logs binary tests, all 70 logs and 90 metrics
real-extension tests, both complete Rust workspaces, the 34-test Rust query
harness, all six focused correctness profiles, the standalone DB-health
lifecycle gate, all 45 CLI/crash/transaction sections, documentation/oracle
contracts, formatting, and Clippy with warnings denied. All 124 executable SQL
recipes and 162 statements pass through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `running_stats`

The checked-in
[`2026-08-06_session18_lql_p45_running_stats.json`](evidence/2026-08-06_session18_lql_p45_running_stats.json)
was captured from exact release extension and server build
`3f4ef107973f73361cfd90eff6e31ea53bd58f0c` and has SHA-256
`b58634502d9d9c771fc1079620f3d4de029e0a5207bc8ed9601917ad3f9b84de`.
This artifact is also the exact evidence for function-catalog row `LQL-S14`:
upstream exposes its running `count`/`last`/`min`/`max`/`sum` functions only
through the `LQL-P45` `running_stats` pipe. It is not evidence for a separate
set of standalone function names. Reconciliation on 2026-08-07 re-ran the
targeted real-extension durability test, the current complete 1,388-case live
VictoriaLogs corpus, and all 130 public SQL recipes/168 statements without
changing code, storage, or the measured artifact.
Each candidate and control performs one public scan, sorts the same candidate
set chronologically, limits the response to 64 rows, and performs identical
physical storage work. The candidate partitions by service and severity and
writes a cumulative count. The control writes one constant field after the
same sort and limit; it isolates the bounded grouping, partitioned ordering,
state update, and result write without pretending the response values or
field-name lengths are identical.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| grouped `running_stats count()`, indexed host | 64 | 951 | 3.174 | 3.437 | 3.524 | 1 | 1,024 | 235,778 | 128 |
| grouped `running_stats count()`, full fixture | 64 | 951 | 44.922 | 48.000 | 49.893 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/constant control, indexed host | 64 | 1,024 | 3.207 | 3.771 | 4.821 | 1 | 1,024 | 235,778 | 128 |
| time-sort/constant control, full fixture | 64 | 1,024 | 34.292 | 38.690 | 39.518 | 4 | 8,192 | 1,914,055 | 8,192 |

Running statistics p95 is 8.9% below the narrow control and 24.1% above the
wide control. Request-attributed API averages are 2.467/44.051 ms versus
2.554/33.894 ms, or 3.4% lower/30.0% higher. Every equal-width pair executes
exactly 50 public queries and has byte-identical candidate-block selection,
entry decode, payload reads, matches, public-row returns, and requested work.
The candidate's shorter 951-byte response versus 1,024 bytes is an honest
consequence of the projected field names and cumulative values. The evidence
harness requires physical-work equality and does not erase that output
difference.

All 8,192 rich entries completed durably with zero queued work. Admission took
16.403 ms and the explicit durability barrier took 39.731 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 102,368 KiB, 856 KiB above
LQL-P44 after four additional one-scan shapes; metrics HWM was 50,756 KiB,
2,704 KiB below it. Both are retained as whole-process complete-workload
variation. Cancellation ended with zero requests in flight and zero cancelled
requests at capture. The measured wide partition/sort/state cost remains
bounded Rust API work. Fixed-key SQLite window functions already give direct
users the useful foundation, and an extension primitive would avoid neither
scan, decode, payload crossing, nor row crossing.

The complete 1,300-case pinned VictoriaLogs v1.52.0 corpus passes live. Direct
parser/evaluator and real-extension regressions pin strict grammar, numeric
microsecond chronology, stable ties, independent groups, cumulative count,
sum, natural-order typed min/max, offset first/last, recursive prefix/all
selection, rich exact values, alias snapshots and conflicts,
work/state/result/response/deadline limits, cancellation, immutable source
rows, optimize, flush, shutdown, and reopen. Executable `SQL-LOG-059` proves
the fixed-key numeric window foundation through the public release extension.

Final local gates pass: 144 logs library tests, two logs binary tests, all 71
logs and 90 metrics real-extension tests, both complete Rust workspaces, the
35-test Rust query harness, all six focused correctness profiles, the
standalone DB-health lifecycle gate, all 45 CLI/crash/transaction sections,
documentation/oracle contracts, formatting, and Clippy with warnings denied.
All 125 executable SQL recipes and 163 statements pass through the public
release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `total_stats`

The checked-in
[`2026-08-06_session18_lql_p46_total_stats.json`](evidence/2026-08-06_session18_lql_p46_total_stats.json)
was captured from exact release extension and server build
`3bfdf2c843ebc221916d20b35a1df98d111e3eb9` and has SHA-256
`11605cde0ec229e35d2af601aa1b134934676e771a80c6e9a62fd95c37f0019f`.
This artifact is also the exact evidence for function-catalog row `LQL-S15`:
upstream exposes total `count`/`first`/`last`/`min`/`max`/`sum` only through
the `LQL-P46` `total_stats` pipe. It is not evidence for separate standalone
function names. Reconciliation on 2026-08-07 re-runs the targeted real-
function names. Reconciliation on 2026-08-07 re-ran the targeted real-
extension durability test, the current complete 1,388-case live VictoriaLogs
corpus, and all 130 public SQL recipes/168 statements without changing code,
storage, or the measured artifact.
Each candidate and control performs one public scan, sorts the same candidate
set chronologically, and limits the response to 64 rows. The candidate
partitions by service and severity, computes complete-group count, and writes
the final native count onto every retained row. The controls write quoted
constant values `128` narrow and `1024` wide after the same sort and limit;
they isolate bounded grouping, full-partition accumulation, and result writes
without pretending that native-count and `math` output types are identical.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| grouped `total_stats count()`, indexed host | 64 | 896 | 3.557 | 3.744 | 4.797 | 1 | 1,024 | 235,778 | 128 |
| grouped `total_stats count()`, full fixture | 64 | 960 | 44.051 | 47.706 | 49.552 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/constant control, indexed host | 64 | 1,024 | 3.280 | 3.618 | 3.805 | 1 | 1,024 | 235,778 | 128 |
| time-sort/constant control, full fixture | 64 | 1,088 | 33.905 | 34.716 | 35.739 | 4 | 8,192 | 1,914,055 | 8,192 |

Total statistics p95 is 3.5% above the narrow control and 37.4% above the
wide control. Request-attributed API averages are 2.796/43.581 ms versus
2.575/32.879 ms, or 8.6%/32.6% higher. Every equal-width pair executes
exactly 50 public queries and has byte-identical candidate-block selection,
entry decode, payload reads, matches, public-row returns, and requested work.
The candidate's 128-byte-smaller response in both fixtures is the honest
two-quote-per-row consequence of native numeric count results versus quoted
`math` constants. The evidence harness requires physical-work equality and
does not erase that output-type difference.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.420 ms and the explicit durability barrier took 42.435 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 100,688 KiB, 1,680 KiB below
LQL-P45 after four additional one-scan shapes; metrics HWM was 52,240 KiB,
1,484 KiB above it. Both are retained as whole-process complete-workload
variation. Cancellation ended with zero requests in flight and zero cancelled
requests at capture. The measured wide complete-group prepass and rich
snapshot-write cost remain bounded Rust API work. Fixed-key full-partition
SQLite windows already give direct users the useful foundation, and an
extension primitive would avoid neither scan, decode, payload crossing, nor
row crossing.

The complete 1,329-case pinned VictoriaLogs v1.52.0 corpus passes live. Direct
parser/evaluator and real-extension regressions pin strict grammar, numeric
microsecond chronology, stable ties, independent groups, final count/sum,
natural-order typed min/max, fixed-offset first/last, recursive prefix/all
selection, rich exact values, alias snapshots and conflicts, later-pipe
visibility, work/state/result/response/deadline limits, cancellation and
reader reuse, immutable source rows, optimize, flush, shutdown, and reopen.
Executable `SQL-LOG-060` proves the fixed-key full-partition numeric window
foundation through the public release extension.

Final local gates pass: 146 logs library tests, two logs binary tests, all 72
logs and 90 metrics real-extension tests, both complete Rust workspaces, the
35-test Rust query harness, all six focused correctness profiles, the
standalone DB-health lifecycle gate, all 45 CLI/crash/transaction sections,
the complete 1,329-case live oracle, documentation/oracle contracts,
formatting, and Clippy with warnings denied. All 126 executable SQL recipes
and 164 statements pass through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `time_add`

The checked-in
[`2026-08-07_session18_lql_p47_time_add.json`](evidence/2026-08-07_session18_lql_p47_time_add.json)
was captured from exact release extension and server build
`db274f37b217862ef5ed35d2100675f7d1183b75` and has SHA-256
`253a5cb9abc99a1758d2e387236179182ee4c8d8f7221bc2ad888a9829d5ee32`.
Each candidate and control performs one public scan, applies the same
chronological sort and 64-row limit, and projects only `_time`. The candidate
then adds one second with the shipped RFC3339Nano semantics; the control omits
only that transform.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `time_add 1s`, indexed host | 64 | 2,539 | 3.243 | 3.519 | 3.640 | 1 | 1,024 | 235,778 | 128 |
| `time_add 1s`, full fixture | 64 | 2,547 | 36.297 | 40.756 | 41.869 | 4 | 8,192 | 1,914,055 | 8,192 |
| projection control, indexed host | 64 | 2,560 | 2.884 | 3.197 | 3.303 | 1 | 1,024 | 235,778 | 128 |
| projection control, full fixture | 64 | 2,560 | 32.900 | 35.390 | 36.245 | 4 | 8,192 | 1,914,055 | 8,192 |

Timestamp addition p95 is 10.1% above the narrow control and 15.2% above the
wide control. Request-attributed API averages are 2.722/35.182 ms versus
2.529/32.495 ms, or 7.6%/8.3% higher. Every equal-width pair executes exactly
50 public queries and has byte-identical candidate-block selection, entry
decode, payload reads, matches, public-row returns, and requested work. The
candidate responses are 21/13 bytes smaller because canonical RFC3339Nano
formatting removes insignificant fractional zeroes. The difference is output
semantics, not hidden storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.402 ms and the explicit durability barrier took 39.025 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 106,108 KiB, 5,420 KiB above
LQL-P46 after four additional one-scan shapes; metrics HWM was 51,044 KiB,
1,196 KiB below it. The whole-process variation and bounded canonical-string
allocation are retained honestly. Cancellation ended with zero requests in
flight and zero cancelled requests at capture.

The complete 1,341-case pinned VictoriaLogs v1.52.0 corpus passes live.
Direct parser/evaluator and real-extension regressions pin strict grammar,
compound signed duration units, RFC3339Nano and SQL-space parsing, timezone
normalization, deterministic zone-less UTC, nanosecond output, sentinel-aware
saturation, invalid-string and native-rich no-op fidelity, exact nested
mutation, later-pipe visibility, cumulative limits, cancellation and reader
reuse, immutable source rows, optimize, shutdown, and reopen. Executable
`SQL-LOG-061` proves the saturating native-unit SQL foundation through the
public release extension.

Final local gates pass: 148 logs library tests, two logs binary tests, all 73
logs and 90 metrics real-extension tests, both complete Rust workspaces, the
35-test Rust query harness, all six focused correctness profiles, the
standalone DB-health lifecycle gate, all 45 CLI/crash/transaction sections,
the complete 1,341-case live oracle, documentation/oracle contracts,
formatting, and Clippy with warnings denied. All 127 executable SQL recipes
and 165 statements pass through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `generate_sequence`

The checked-in
[`2026-08-07_session18_lql_p48_generate_sequence.json`](evidence/2026-08-07_session18_lql_p48_generate_sequence.json)
was captured from exact release extension and server build
`8b6a5e7cb722ceeb7a5f25221994d2d8b619ed7f` and has SHA-256
`4626cb29b8340bcb33dd0c8f4abceef4542e3824b6a2f16ff300d1897d119999`.
Both shapes generate the same 64 decimal `_msg` strings. One starts with an
indexed-host source expression and the other with a full-fixture expression;
the final generator must make both sources observationally dead.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | public queries/request | candidate blocks/request | decoded entries/request | extension payload bytes/request | public rows/request |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `generate_sequence 64`, indexed-host source | 64 | 886 | 0.691 | 0.836 | 0.900 | 0 | 0 | 0 | 0 | 0 |
| `generate_sequence 64`, full-fixture source | 64 | 886 | 0.640 | 0.780 | 0.839 | 0 | 0 | 0 | 0 | 0 |

The full-source spelling is 6.6% lower at p95. Request-attributed API means
are 0.007752/0.007524 ms, or 2.9% lower. These tiny differences are
loopback/parser variation between semantically identical scan-free plans, not
a storage effect. Across 50 measured requests per shape, neither path records
a public query, native count, bounded query, candidate block, decoded entry,
payload byte, match, returned entry, materialization, snapshot, or storage
query timer. Both emit exactly 3,200 measured rows and 44,300 response bytes.

All 8,192 rich fixture entries completed durably with zero queued work.
Admission took 13.963 ms and the explicit durability barrier took 40.840 ms.
Storage remains exactly four raw blocks, 1,914,055 logical payload bytes, and
2,022,736 physical database/WAL/SHM bytes. Logs HWM was 107,436 KiB, 1,328 KiB
above LQL-P47 after two additional scan-free shapes; metrics HWM was 50,720
KiB, 324 KiB below it. Both complete-workload maxima are retained as
whole-process variation. Cancellation ended with zero requests in flight and
zero cancelled requests at capture.

The complete 1,357-case pinned VictoriaLogs v1.52.0 corpus passes live.
Direct parser/evaluator and real-extension regressions pin shared numeric
spellings, positive fractional truncation, string output, no-match generation,
complete prefix replacement, last-generator-wins behavior, later-pipe
composition, strict errors, cumulative limits, cancellation, immutable rich
source rows, zero public storage work, optimize, shutdown, and reopen.
Executable `SQL-LOG-062` proves the scan-free recursive-CTE foundation through
the public SQLite/libSQL interface.

Final local gates pass: 150 logs library tests, two logs binary tests, all 74
logs and 90 metrics real-extension tests, the complete default root workspace
and server workspace targets, the 36-test Rust query harness, all six focused
correctness profiles, the standalone DB-health lifecycle gate, all 45
CLI/crash/transaction sections including 150,000 oracle operations and five
kill-9 rounds, the complete 1,357-case live oracle, documentation/oracle
contracts, formatting, and Clippy with warnings denied. All 128 executable SQL
recipes and 166 statements pass through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No extension opcode, private shadow
table, language fallback, tag, release, or downstream repository was used or
modified.

## Session 18 LogsQL P3: bounded typed `json_values`

The checked-in
[`2026-08-07_session18_lql_s12_json_values.json`](evidence/2026-08-07_session18_lql_s12_json_values.json)
was captured from exact release extension and server build
`898006684a82b5fd6cc0f7ff477c75e5c1778367` and has SHA-256
`ad8e05fcb10cee4efc863d66830d7a5b61025c8ebd20c871e66994e7aab91637`.
Candidate shapes return one statistics row whose JSON string contains 64
selected typed objects. Controls return one bounded `values` statistics row
from the same sources and preserve exact public storage work.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/request | decoded entries/request | extension payload bytes/request | public rows/request |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| typed natural top-k, indexed host | 1 | 3,663 | 3.275 | 3.557 | 3.967 | 1 | 1,024 | 235,778 | 128 |
| typed natural top-k, full fixture | 1 | 3,663 | 35.728 | 37.237 | 37.801 | 4 | 8,192 | 1,914,055 | 8,192 |
| bounded unique-value control, indexed host | 1 | 451 | 3.199 | 3.364 | 3.429 | 1 | 1,024 | 235,778 | 128 |
| bounded unique-value control, full fixture | 1 | 451 | 33.646 | 34.563 | 37.043 | 4 | 8,192 | 1,914,055 | 8,192 |

Candidate p95 is 5.7% higher narrow and 7.7% higher wide.
Request-attributed API means are 2.546/34.506 ms versus
2.437/32.417 ms, or 4.5%/6.4% higher. The candidate response is 3,212
bytes larger because it contains 64 native nested objects rather than a
bounded unique-value envelope. Across 50 measured requests per shape, each
candidate/control pair nevertheless records exactly the same public query,
candidate-block, decoded-entry, payload-byte, matched-row, returned-row, and
bounded-request work. Both request 100,001 bounded entries per query; the
hard limit remains an explicit failure boundary rather than truncation.

All 8,192 rich fixture entries completed durably with zero queued work.
Admission took 12.859 ms and the explicit durability barrier took 36.741 ms.
Storage remains exactly four raw blocks, 1,914,055 logical payload bytes, and
2,022,736 physical database/WAL/SHM bytes. Logs HWM was 106,908 KiB, 528 KiB
below LQL-P48 after four additional one-scan shapes; metrics HWM was 51,432
KiB, 712 KiB above it. Both complete-workload maxima are retained as
whole-process variation. Cancellation ended with zero requests in flight and
zero cancelled requests at capture.

The complete 1,374-case pinned VictoriaLogs v1.52.0 corpus passes live.
Direct parser/evaluator and real-extension regressions pin statistics and
standalone grammar, normalized implicit aliases, exact/prefix/all selectors,
rich native values, missing and empty-object behavior, natural multi-key
ordering, stable ties, bounded top-k, zero-limit handling, cumulative limits,
cancellation and reader reuse, immutable rows, optimize, flush, shutdown, and
reopen. Executable `SQL-LOG-063` proves the fixed-path typed JSON1 foundation
through the public SQLite/libSQL interface.

Final local gates pass: 152 logs library tests, two logs binary tests, all 75
logs and 90 metrics real-extension tests, the complete default root workspace
and server workspace targets, the 36-test Rust query harness, all six focused
correctness profiles, the standalone DB-health lifecycle gate, all 45
CLI/crash/transaction sections including 150,000 oracle operations and five
kill-9 rounds, the complete 1,374-case live oracle, documentation/oracle
contracts, formatting, and Clippy with warnings denied. All 129 executable SQL
recipes and 167 statements pass through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No extension opcode, private shadow
table, language fallback, tag, release, or downstream repository was used or
modified.

## Session 18 LogsQL P3: bounded logarithmic `histogram`

The checked-in
[`2026-08-07_session18_lql_s13_histogram.json`](evidence/2026-08-07_session18_lql_s13_histogram.json)
was captured from exact release extension and server build
`63081644c87fa67fe1c9874ea375195146262433` and has SHA-256
`3deeadafc7c57442e0953f4586264d1f86ab8cc87486b4073a983566a68c940d`.
Candidate shapes aggregate the retained native `context.attempt` field into
one Victoria-compatible histogram JSON string. Controls return one bounded
`values` statistics row from the same exact sources, preserving public
storage work while isolating the fixed bucket update, natural label sort, and
encoding cost.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/request | decoded entries/request | extension payload bytes/request | public rows/request |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| histogram, indexed host | 1 | 268 | 3.317 | 4.329 | 5.812 | 1 | 1,024 | 235,778 | 128 |
| histogram, full fixture | 1 | 278 | 37.928 | 39.536 | 42.954 | 4 | 8,192 | 1,914,055 | 8,192 |
| bounded-values control, indexed host | 1 | 164 | 3.341 | 3.517 | 3.611 | 1 | 1,024 | 235,778 | 128 |
| bounded-values control, full fixture | 1 | 164 | 37.326 | 38.826 | 44.811 | 4 | 8,192 | 1,914,055 | 8,192 |

Candidate p95 is 23.1% higher narrow and 1.8% higher wide.
Request-attributed API means are 3.008/37.328 ms versus
2.813/36.612 ms, or 6.9%/2.0% higher. Candidate responses add 104/114
bytes for the compact nonempty `vmrange`/native-hit objects. Across 50
measured requests per shape, each candidate/control pair nevertheless
records exactly the same public query, candidate-block, decoded-entry,
payload-byte, matched-row, returned-row, and bounded-request work.

All 8,192 rich fixture entries completed durably with zero queued work.
Admission took 14.416 ms and the explicit durability barrier took 38.072 ms.
Storage remains exactly four raw blocks, 1,914,055 logical payload bytes, and
2,022,736 physical database/WAL/SHM bytes. Logs HWM was 102,964 KiB, 3,944
KiB below LQL-S12 after four additional one-scan shapes; metrics HWM was
52,840 KiB, 1,408 KiB above it. Both complete-workload maxima are retained as
whole-process variation. Cancellation ended with zero requests in flight and
zero cancelled requests at capture.

The complete 1,388-case pinned VictoriaLogs v1.52.0 corpus passes live.
Direct parser/evaluator and real-extension regressions pin strict statistics
and standalone grammar, exact logarithmic bounds, textual/native numeric
input, duration and byte sizes, skipped negative/NaN/IPv4/timestamp/rich
values, natural output order, empty input, cumulative limits, cancellation
and reader reuse, immutable rich nested rows, optimize, flush, shutdown, and
reopen. Executable `SQL-LOG-064` proves the fixed-path native-number math/
group/JSON foundation through the public SQLite/libSQL interface.

Final local gates pass: 154 logs library tests, two logs binary tests, all 76
logs and 90 metrics real-extension tests, the complete default root workspace
and server workspace targets, the 36-test Rust query harness, all six focused
correctness profiles, the standalone DB-health lifecycle gate, all 45
CLI/crash/transaction sections including 150,000 oracle operations and five
kill-9 rounds, the complete 1,388-case live oracle, documentation/oracle
contracts, formatting, and Clippy with warnings denied. All 130 executable SQL
recipes and 168 statements pass through the public release extension.

The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No extension opcode, private shadow
table, language fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded `hash`

The checked-in
[`2026-08-06_session18_lql_p25_hash.json`](evidence/2026-08-06_session18_lql_p25_hash.json)
was captured from exact release extension and server build
`36e269fcb875262827f1f6cd49e9fc2d8ae46b3b` and has SHA-256
`c13f32bc39fb7420f8ffd2e0aa1a0b83986cd32224f7b1320745109f356e931e`.
The narrow shapes select one indexed host and return 64 rows; the wide shapes
hash all 8,192 retained entries before their 64-row limit. Controls copy the
same source into the same destination, so both paths perform identical public
storage work while isolating the bounded hash transform and larger result.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| masked xxHash64, indexed host | 64 | 2,038 | 3.206 | 3.455 | 3.566 | 1 | 1,024 | 235,778 | 128 |
| masked xxHash64, full fixture | 64 | 2,042 | 34.628 | 36.785 | 37.534 | 4 | 8,192 | 1,914,055 | 8,192 |
| exact-field copy control, indexed host | 64 | 1,536 | 3.168 | 3.481 | 3.922 | 1 | 1,024 | 235,778 | 128 |
| exact-field copy control, full fixture | 64 | 1,536 | 35.594 | 36.223 | 36.263 | 4 | 8,192 | 1,914,055 | 8,192 |

Hash p95 is 0.7% lower/1.6% higher than its narrow/wide control. Its
request-attributed API timer averages 2.464/33.501 ms versus 2.459/34.423 ms,
or 0.2% higher/2.7% lower. Every equal-width pair executes 50 public queries
and performs identical candidate-block selection, entry decode, payload read,
row match, and row return work. Decimal hash values add 502/506 bytes to the
64-row response. The differences are bounded row-local hash and response work,
not storage pushdown. Core SQLite/libSQL has no portable exact xxHash64 scalar;
adding one would not avoid any block read, decode, payload transfer, or row
crossing, so the explicit no-SQL/no-extension disposition remains correct.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.295 ms and the explicit durability barrier took 37.144 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 101,196 KiB and metrics HWM was
52,848 KiB across the complete enlarged workload. Cancellation ended with
zero requests in flight; direct evaluator and HTTP regressions pin grammar,
exact bits, rich-value fidelity, work/state/result/response bounds,
cancellation, conflicts, optimize, flush, shutdown, durability, and reopen.

All 1,033 pinned VictoriaLogs cases pass live. The complete local extension,
real-extension logs, server workspace, Rust query harness, documentation,
oracle, SQL-cookbook, formatting, Clippy, CLI/crash/transaction, and dbhealth
gates pass. The authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 18 LogsQL P3: bounded collapse nums

The checked-in
[`2026-08-06_session18_lql_p26_collapse_nums.json`](evidence/2026-08-06_session18_lql_p26_collapse_nums.json)
was captured from exact release extension and server build
`a6047ffe8537188152c882e49b50b98ced7ceced` and has SHA-256
`b5d3250ab63605af37ab789f929b48867054d59ca10e779edf7aef73a3d24b6b`.
The narrow shapes select one indexed host and return 64 rows; the wide shapes
transform all 8,192 retained entries before their 64-row limit. Controls
format the same source into the same destination and output, so both paths
perform identical public storage and wire work while isolating the bounded
number scanner and prettifier.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| number collapse/prettify, indexed host | 64 | 1,536 | 2.975 | 3.135 | 3.175 | 1 | 1,024 | 235,778 | 128 |
| number collapse/prettify, full fixture | 64 | 1,536 | 33.889 | 34.525 | 34.728 | 4 | 8,192 | 1,914,055 | 8,192 |
| exact-output format control, indexed host | 64 | 1,536 | 3.026 | 3.143 | 3.414 | 1 | 1,024 | 235,778 | 128 |
| exact-output format control, full fixture | 64 | 1,536 | 33.720 | 36.735 | 42.843 | 4 | 8,192 | 1,914,055 | 8,192 |

Collapse p95 is 0.3%/6.0% lower than its narrow/wide control. Its
request-attributed API timer averages 2.439/33.156 ms versus 2.468/33.406 ms,
or 1.2%/0.7% lower. Every equal-width pair executes 50 public queries and
performs identical candidate-block selection, entry decode, payload read,
row match, row return, result-cardinality, and response-byte work. The lower
wide tail is retained as whole-run/API variation rather than claimed as an
optimization. Core SQLite/libSQL has no portable exact tokenizer for this
language behavior, and adding one would not avoid a block read, decode,
payload transfer, or row crossing, so the no-SQL/no-extension disposition is
correct.

All 8,192 rich entries completed durably with zero queued work. Admission took
15.719 ms and the explicit durability barrier took 34.073 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 99,356 KiB and metrics HWM was
50,264 KiB across the complete enlarged workload. Cancellation ended with
zero requests in flight; direct evaluator and HTTP regressions pin grammar,
exact token/prettify semantics, rich-value fidelity, work/state/result/
response bounds, cancellation, optimize, flush, shutdown, durability, and
reopen.

All 1,055 pinned VictoriaLogs cases pass live. The complete local extension,
real-extension logs, server workspace, Rust query harness, documentation,
oracle, SQL-cookbook, formatting, Clippy, CLI/crash/transaction, and dbhealth
gates pass. The authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, tag, release, or downstream repository was
used or modified.

## Session 18 LogsQL P3: bounded decolorize

The checked-in
[`2026-08-06_session18_lql_p27_decolorize.json`](evidence/2026-08-06_session18_lql_p27_decolorize.json)
was captured from exact release extension and server build
`6971681946d9e445ebc13eef5fcd9b188e23a8e9` and has SHA-256
`a0479b12488e8bd917d8f7540247e89d3d6ec678ad943988f7c866605a16c135`.
The candidate first formats each `range_key` as a realistic SGR-colored
current-row value, removes both CSI sequences, and emits the original value.
The control formats the original value directly to the same destination.
Both return byte-identical rows and perform identical public storage and wire
work; the measured difference therefore includes both the longer temporary
colored value and the bounded CSI scanner rather than hiding input
construction cost.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| format + decolorize, indexed host | 64 | 1,536 | 2.969 | 3.101 | 3.405 | 1 | 1,024 | 235,778 | 128 |
| format + decolorize, full fixture | 64 | 1,536 | 34.973 | 36.385 | 39.133 | 4 | 8,192 | 1,914,055 | 8,192 |
| identical-output format control, indexed host | 64 | 1,536 | 3.007 | 3.169 | 3.756 | 1 | 1,024 | 235,778 | 128 |
| identical-output format control, full fixture | 64 | 1,536 | 33.785 | 34.872 | 35.393 | 4 | 8,192 | 1,914,055 | 8,192 |

Decolorize p95 is 2.2% lower/4.3% higher than its narrow/wide control. Its
request-attributed API timer averages 2.498/34.422 ms versus 2.464/33.193 ms,
or 1.4%/3.7% higher. Every equal-width pair executes 50 public queries and
performs identical candidate-block selection, entry decode, payload read,
row match, row return, result cardinality, and response-byte work. The narrow
tail is retained as run variation; the wide difference is accepted bounded
current-row allocation/scanning work after the unavoidable public scan.
Because the transform cannot reduce a storage read or row crossing,
executable `SQL-LOG-050` remains the direct SQLite/libSQL foundation and no
extension primitive is justified.

All 8,192 rich entries completed durably with zero queued work. Admission took
12.808 ms and the explicit durability barrier took 39.328 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 97,868 KiB and metrics HWM was
53,396 KiB across the complete enlarged workload. Cancellation ended with
zero requests in flight; direct evaluator, direct SQL, and HTTP regressions
pin grammar, CSI byte classes, incomplete/invalid/non-CSI behavior, rich-value
fidelity, work/state/result/response bounds, cancellation, optimize, flush,
shutdown, durability, and reopen.

All 1,069 pinned VictoriaLogs cases pass live. The complete local extension,
real-extension logs, server workspace, Rust query harness, documentation,
oracle, 116-recipe/154-statement SQL cookbook, formatting, Clippy,
CLI/crash/transaction, and dbhealth gates pass. The authoritative 8,192-entry
batching, storage formats, compression, indexes, retention, optimize,
transactions, migrations, and public batch/SQL contracts are unchanged. No
private shadow table, Elixir/BEAM/NIF/process fallback, workflow invocation,
tag, release, or downstream repository was used or modified.

## Session 18 LogsQL P3: bounded literal split

The checked-in
[`2026-08-06_session18_lql_p31_split.json`](evidence/2026-08-06_session18_lql_p31_split.json)
was captured from exact release extension and server build
`e80b1afb9d7f0cfc559f2db0338575f1f8caa4e1` and has SHA-256
`20157693726cd77f47bb272c0c7204273163dfba5d4e46435bf0cec33761ed46`.
The candidate splits `range_key` on `-` into compact JSON-array text. The
control extracts the same suffix and formats byte-identical JSON-array text.
Both return identical rows and perform identical public storage and wire
work; measured differences are therefore row-local API composition and
whole-run variation after the unavoidable public scan.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| literal split, indexed host | 64 | 1,984 | 3.219 | 3.481 | 4.063 | 1 | 1,024 | 235,778 | 128 |
| literal split, full fixture | 64 | 1,984 | 37.529 | 38.655 | 40.113 | 4 | 8,192 | 1,914,055 | 8,192 |
| identical-output extract/format control, indexed host | 64 | 1,984 | 3.078 | 4.786 | 4.878 | 1 | 1,024 | 235,778 | 128 |
| identical-output extract/format control, full fixture | 64 | 1,984 | 37.964 | 40.047 | 40.151 | 4 | 8,192 | 1,914,055 | 8,192 |

Split p95 is 27.3%/3.5% lower than its narrow/wide control. Its
request-attributed API timer averages 2.813/36.053 ms versus 2.729/36.562 ms,
or 3.1% higher/1.4% lower. Every equal-width pair executes 50 public queries
and performs identical candidate-block selection, entry decode, payload read,
row match, row return, result cardinality, and response-byte work. The larger
narrow control tail is retained honestly; the opposing API-mean direction
precludes a split speedup claim. Bounded literal scanning, JSON escaping, and
current-row assignment happen after unchanged storage work. Executable
`SQL-LOG-051` remains the direct SQLite/libSQL foundation, and no extension
primitive is justified.

All 8,192 rich entries completed durably with zero queued work. Admission took
12.792 ms and the explicit durability barrier took 38.656 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 99,288 KiB and metrics HWM was
51,468 KiB across the complete enlarged workload. Cancellation ended with
zero requests in flight; direct evaluator, direct SQL, and HTTP regressions
pin grammar, literal/Unicode/empty/escape behavior, rich-value fidelity,
work/state/result/response bounds, cancellation, optimize, flush, shutdown,
durability, and reopen.

All 1,091 pinned VictoriaLogs cases pass live. The complete local extension,
real-extension logs, server workspace, Rust query harness, documentation,
oracle, 117-recipe/155-statement SQL cookbook, formatting, Clippy,
CLI/crash/transaction, and dbhealth gates pass. The authoritative 8,192-entry
batching, storage formats, compression, indexes, retention, optimize,
transactions, migrations, and public batch/SQL contracts are unchanged. No
private shadow table, Elixir/BEAM/NIF/process fallback, workflow invocation,
tag, release, or downstream repository was used or modified.

## Session 17 LogsQL P2: `quantile` and `stddev`

The checked-in
[`2026-08-06_session17_lql_s07_quantile_stddev.json`](evidence/2026-08-06_session17_lql_s07_quantile_stddev.json)
was captured from exact release extension and server build
`f8d5b81bc16e2dadaf7e764273eb82cbfc0de272` and has SHA-256
`1d59631a21848ada5d2cbcdc3a71c774ecb49ca3ae475b69e9ab88a3da6e1f35`.
The narrow shapes select one indexed host (128 public rows); the wide shapes
select all 8,192 retained entries. Median is the same-output sorted-state
control for textual quantile. Average is the one-pass numeric-state control
for population deviation.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| textual `quantile(0.5)`, indexed host | 1 | 14 | 3.279 | 3.636 | 3.834 | 1 | 1,024 | 235,778 | 128 |
| textual `quantile(0.5)`, full fixture | 1 | 14 | 37.390 | 38.585 | 40.331 | 4 | 8,192 | 1,914,055 | 8,192 |
| numeric median control, indexed host | 1 | 14 | 3.430 | 3.766 | 4.126 | 1 | 1,024 | 235,778 | 128 |
| numeric median control, full fixture | 1 | 14 | 36.078 | 38.121 | 45.283 | 4 | 8,192 | 1,914,055 | 8,192 |
| population `stddev`, indexed host | 1 | 26 | 3.330 | 3.517 | 3.676 | 1 | 1,024 | 235,778 | 128 |
| population `stddev`, full fixture | 1 | 29 | 36.149 | 37.372 | 37.815 | 4 | 8,192 | 1,914,055 | 8,192 |
| numeric average control, indexed host | 1 | 20 | 3.367 | 3.622 | 3.727 | 1 | 1,024 | 235,778 | 128 |
| numeric average control, full fixture | 1 | 26 | 36.303 | 37.426 | 38.269 | 4 | 8,192 | 1,914,055 | 8,192 |

Quantile p95 is 3.5% below/1.2% above its narrow/wide control. Its internal
API timer averages 2.547/36.357 ms versus 2.677/34.564 ms, or 4.8% below/5.2%
above. Population deviation p95 is 2.9%/0.1% below its control, and its
internal API timer averages 2.543/35.014 ms versus 2.685/35.303 ms, or
5.3%/0.8% below. Every equal-width pair performs byte-identical public block
selection, entry decode, payload reads, and row materialization. The stddev
response is slightly larger because its result has more decimal digits; this
does not change storage work.

Text projection, VictoriaLogs natural comparison, exact sorting, Welford
state, strict errors, limits, cancellation, and envelopes remain bounded Rust
API work after the required public scan. `SQL-LOG-044` gives direct
SQLite/libSQL users an executable finite-native-number foundation. Core
SQLite does not have the complete mixed textual comparator, and moving either
operation into an extension opcode would not avoid a block read, decode, or
row crossing, so no new extension primitive is justified.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.936 ms and the explicit durability barrier took 35.361 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 98,268 KiB, 3,608 KiB (3.81%)
above LQL-P41; metrics HWM was 52,872 KiB, 456 KiB (0.86%) below it. Each
maximum spans eight additional repeated query shapes and is retained as
whole-process variation, not attributed to persistent statistic state.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin work/state/result/response bounds, rejection,
cancellation, and reader reuse.

All 907 pinned VictoriaLogs v1.52.0 cases pass live. The final 51-test logs
real-extension suite, 105 logs library tests, 90-test metrics real-extension
suite, complete 45-section CLI/crash/transaction suite, standalone dbhealth
lifecycle gate, 31-test Rust query harness, documentation contracts, Clippy
with warnings denied, formatting, and all 110 SQL recipes (146 statements)
pass locally. The declared root gate is `cargo test --workspace`;
`cargo test --workspace --all-targets` deliberately overrides
`dbhealth-ext`'s `test = false` and cannot link the two separate loadable
extensions' identically named SQLite entrypoints. The crate manifest documents
that boundary, and its authoritative `tests/dbhealth.sh` built and exercised
the standalone artifact successfully. The extension's authoritative
8,192-entry batching, storage
formats, compression, indexes, retention, optimize, transactions, migrations,
and public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, workflow invocation, tag, release, or
downstream repository was used or modified.

## Rust-only extension release-gate restoration

Before beginning `LQL-P16`, every executable Python driver retained by the
extension release gate was replaced with a subcommand of the existing
standalone Rust `tools/query-harness` crate. The shell suites remain the
canonical orchestration and assertion boundary, while Rust now owns binary
fixture construction, persistent multi-connection and multi-process SQLite
hosts, packed-frame decoding, rich log/trace fidelity fixtures, randomized
crash SQL generation, and the standalone dbhealth lifecycle host.

The complete 45-section `tests/cli.sh` gate passes without invoking Python.
That run includes three 50,000-operation randomized plain-table oracle seeds,
five `kill -9` durability iterations, all 95 documented SQL recipes (131
statements), transaction/reopen/corruption checks, exact rich trace and log
fidelity, and every persistent-process extension contract. The focused Rust
correctness gate also passes `R1`, all six `R2` multi-process cases for three
rounds, all four `R3` attached-schema cases, all five `R4` shared-engine
cases, `R8`, and rich logs. The query-harness crate passes 29 tests, Clippy
with warnings denied, and formatting.

The first complete run correctly stopped on three harness-adapter defects:
the `EXPLAIN QUERY PLAN` reader selected column zero instead of the detail in
column three, and the log/trace stats readers did not accept valid SQL NULL
values. Correcting the adapters made all 45 sections pass without changing
extension behavior. The standalone dbhealth gate then found a separate
product regression under both the Rust host and stock `sqlite3`: automatic
sampling collected no rows after metrics gained the hidden `series_id`
column. The health wrapper still read the command from argument six, so it
delegated `sample:auto` to the ordinary metrics command handler. Reading the
command from argument seven restores create/reopen scheduling, manual mode,
drop cleanup, legacy blob-meta migration, and sqld scheduler suppression.
`QSF-159` records the boundary and retained regression. A speculative strong
scheduler ownership change was reverted; weak lifecycle ownership remains.

The release gate now has no executable Python dependency. The public
`examples/query_frames.py` client example and historical
`tools/bench/session6_log_compaction.py` benchmark remain separate artifacts;
neither is imported or executed by these suites. No CI workflow, tag,
release, publication, or downstream repository was used or modified.

## Session 17 LogsQL P2: bounded `uniq`

The checked-in
[`2026-08-05_session17_lql_p16_uniq.json`](evidence/2026-08-05_session17_lql_p16_uniq.json)
was captured from exact release build
`0203fa8b3e959dd5ab76a587d3e90a49961b07b5`. The narrow shape scans one
indexed host and emits its five unique textual `context.attempt` groups with
hits. The wide shape scans all 8,192 entries and emits the eight actual
`(service, level)` groups with hits. Same-run controls scan the identical
source rowsets and return the same five/eight cardinality through established
time sort and projection. They control storage and result size; they are not
semantic equivalents to uniqueness grouping.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `uniq`, indexed host | 5 | 180 | 3.054 | 3.416 | 3.496 | 1 | 1,024 | 235,778 | 128 |
| `uniq`, full fixture | 8 | 411 | 38.318 | 41.705 | 43.542 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 5 | 130 | 3.385 | 3.594 | 3.625 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 8 | 299 | 38.435 | 39.851 | 43.415 | 4 | 8,192 | 1,914,055 | 8,192 |

The `uniq` p95 is 5.0% below/4.7% above its narrow/wide same-run control. Its
internal API timer averages 2.538/37.876 ms versus 2.841/37.806 ms, or 10.7%
below/0.2% above. Responses are 38.5%/37.5% larger because `uniq` emits the
requested string hits. Every equal-width pair performs exactly the same
public storage scan, block decode, payload read, and row materialization. The
measured bounded textual structural-key grouping is accepted in the Rust API.
`SQL-LOG-030` already gives direct SQLite/libSQL users ordinary public
grouping, filtering, hits, and deterministic limiting, so a new extension
primitive would not avoid storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
17.156 ms and the explicit durability barrier took 38.792 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P15.
Logs HWM was 93,156 KiB, 3,728 KiB below LQL-P15; metrics HWM was 52,612 KiB,
488 KiB below it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `uniq`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin cancellation, bounded work/group/result/
state, rejection, and reader reuse.

All 516 pinned VictoriaLogs v1.52.0 cases pass live. The final 34-test logs
real-extension suite, 73 logs library tests, complete extension and Rust
server workspaces, complete 45-section CLI/crash/transaction suite, 30-test
Rust query harness, documentation contracts, Clippy with warnings denied,
formatting, and all 96 SQL recipes (132 statements) pass locally. The
extension's authoritative 8,192-entry batching, storage formats, compression,
indexes, retention, optimize, transactions, migrations, and public batch/SQL
contracts are unchanged. No private shadow table, Elixir/BEAM/NIF/process
fallback, CI workflow, tag, release, or downstream repository was used or
modified.

## Session 17 LogsQL P2: bounded `facets`

The checked-in
[`2026-08-05_session17_lql_p18_facets.json`](evidence/2026-08-05_session17_lql_p18_facets.json)
was captured from exact release build
`85332944a7a9d9c2f476558e1d519eaa6aecf23e`. The narrow shape scans one
indexed host and summarizes seven values across projected `context.attempt`,
`context.retry`, and `status` fields. The wide shape scans all 8,192 entries
and summarizes nineteen values across projected `service`, `level`, `status`,
`context.attempt`, and `context.retry` fields. Same-run controls scan the
identical source rowsets and return the same seven/nineteen cardinality using
established time sort and projection. They control storage and result count;
they are not semantic equivalents to recursive faceting.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `facets`, indexed host | 7 | 444 | 3.062 | 3.239 | 3.248 | 1 | 1,024 | 235,778 | 128 |
| `facets`, full fixture | 19 | 1,176 | 39.765 | 44.087 | 51.685 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 7 | 368 | 3.322 | 4.464 | 5.080 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 19 | 1,668 | 37.103 | 41.116 | 49.215 | 4 | 8,192 | 1,914,055 | 8,192 |

The `facets` p95 is 27.4% below/7.2% above its narrow/wide same-run control.
Its internal API timer averages 2.529/39.820 ms versus 2.919/36.348 ms, or
13.4% below/9.6% above. Responses are 20.7% larger narrow and 29.5% smaller
wide because facet summaries replace source rows. Every equal-width pair
performs exactly the same public storage scan, block decode, payload read, and
row materialization. The bounded recursive flattening, textual grouping,
exclusion, and deterministic ranking cost is accepted in the Rust API.
`SQL-LOG-031` already gives direct SQLite/libSQL users the complete public
JSON1/window equivalent, so a new extension primitive would not avoid storage
work.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.253 ms and the explicit durability barrier took 39.013 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P16.
Logs HWM was 93,268 KiB, 112 KiB above LQL-P16; metrics HWM was 54,056 KiB,
1,444 KiB above it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `facets`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin cancellation, bounded input/field/value/
traversal/sort/output/result/state, rejection, and reader reuse.

All 537 pinned VictoriaLogs v1.52.0 cases pass live. The final 35-test logs
real-extension suite, 75 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 97 SQL recipes (133 statements) pass locally. The
extension's authoritative 8,192-entry batching, storage formats, compression,
indexes, retention, optimize, transactions, migrations, and public batch/SQL
contracts are unchanged. No private shadow table, Elixir/BEAM/NIF/process
fallback, CI workflow, tag, release, or downstream repository was used or
modified.

## Session 17 LogsQL P2: bounded `coalesce`

The checked-in
[`2026-08-05_session17_lql_p19_coalesce.json`](evidence/2026-08-05_session17_lql_p19_coalesce.json)
was captured from exact release extension and server build
`1b36c239a58d96d7ce8348064dfe03d2ac58c470`. The narrow shape scans one
indexed host, expands `context.*` before exact `status`, and returns 64 rows
with the first nonempty textual value in `selected`. The wide shape performs
the same transform across all 8,192 entries before its 64-row limit. Same-run
controls scan the identical source rowsets and return the same cardinality
using established time sort and projection. They control storage and result
count; they are not semantic equivalents to recursive coalescing.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `coalesce`, indexed host | 64 | 1,088 | 3.326 | 3.570 | 3.793 | 1 | 1,024 | 235,778 | 128 |
| `coalesce`, full fixture | 64 | 1,088 | 35.808 | 39.597 | 42.566 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 960 | 2.942 | 3.329 | 4.286 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 960 | 34.360 | 38.277 | 40.596 | 4 | 8,192 | 1,914,055 | 8,192 |

The `coalesce` p95 is 7.2%/3.4% above its narrow/wide same-run control. Its
internal API timer averages 2.703/35.742 ms versus 2.441/33.992 ms, or
10.7%/5.1% above. Responses are 13.3% larger because they contain the
selected destination field. Every equal-width pair performs exactly the same
public storage scan, block decode, payload read, and row materialization. The
bounded ordered expansion, recursive flattening, textual selection, and
destination mutation cost is accepted in the Rust API. `SQL-LOG-032` already
gives direct SQLite/libSQL users the public exact-field equivalent, so a new
extension primitive would not avoid storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
15.684 ms and the explicit durability barrier took 36.114 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P18.
Logs HWM was 94,864 KiB, 1,596 KiB above LQL-P18; metrics HWM was 53,300 KiB,
756 KiB below it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `coalesce`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin cancellation, bounded work/path/state/
result/response behavior, rejection, and reader reuse.

All 559 pinned VictoriaLogs v1.52.0 cases pass live. The final 36-test logs
real-extension suite, 77 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 98 SQL recipes (134 statements) pass locally. The
extension's authoritative 8,192-entry batching, storage formats, compression,
indexes, retention, optimize, transactions, migrations, and public batch/SQL
contracts are unchanged. No private shadow table, Elixir/BEAM/NIF/process
fallback, CI workflow, tag, release, or downstream repository was used or
modified.

## Session 17 LogsQL P2: bounded `copy`

The checked-in
[`2026-08-05_session17_lql_p20_copy.json`](evidence/2026-08-05_session17_lql_p20_copy.json)
was captured from exact release extension and server build
`aac910be8a13bdc7f96be48175602cef566cffa3`. The narrow shape scans one
indexed host, recursively copies `context.*` to `copied.*`, and returns 64
rich rows containing the copied object. The wide shape performs the same
transform across all 8,192 entries before its 64-row limit. Same-run controls
scan the identical source rowsets and return the same cardinality using the
established time sort and original `context` projection. They control storage
and result count; they are not semantic equivalents to sequential typed copy.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `copy`, indexed host | 64 | 2,474 | 2.995 | 3.229 | 3.483 | 1 | 1,024 | 235,778 | 128 |
| `copy`, full fixture | 64 | 2,474 | 39.746 | 46.025 | 46.293 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 2,538 | 3.346 | 3.659 | 3.783 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 2,538 | 39.132 | 41.958 | 43.491 | 4 | 8,192 | 1,914,055 | 8,192 |

The `copy` p95 is 11.8% below/9.7% above its narrow/wide same-run control.
Its internal API timer averages 2.479/39.751 ms versus 2.791/38.307 ms, or
11.2% below/3.8% above. Responses are 2.5% smaller because destination name
`copied` is one byte shorter than control field `context`. Every equal-width
pair performs exactly the same public storage scan, block decode, payload
read, and row materialization. The bounded wildcard snapshot, recursive
flattening, typed clone, and destination mutation cost is accepted in the
Rust API. `SQL-LOG-033` already gives direct SQLite/libSQL users the public
exact-field equivalent, so a new extension primitive would not avoid storage
work.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.095 ms and the explicit durability barrier took 35.741 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P19.
Logs HWM was 94,120 KiB, 744 KiB below LQL-P19; metrics HWM was 52,744 KiB,
556 KiB below it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `copy`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin cancellation, bounded work/state/result/
response behavior, rejection, and reader reuse.

All 587 pinned VictoriaLogs v1.52.0 cases pass live. The final 37-test logs
real-extension suite, 79 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 99 SQL recipes (135 statements) pass locally. The
extension's authoritative 8,192-entry batching, storage formats, compression,
indexes, retention, optimize, transactions, migrations, and public batch/SQL
contracts are unchanged. No private shadow table, Elixir/BEAM/NIF/process
fallback, CI workflow, tag, release, or downstream repository was used or
modified.

## Session 17 LogsQL P2: bounded `rename`

The checked-in
[`2026-08-05_session17_lql_p21_rename.json`](evidence/2026-08-05_session17_lql_p21_rename.json)
was captured from exact release extension and server build
`0431fbc6b548e4a0153ff9c4e5997dfa0baf5968`. The narrow shape scans one
indexed host, recursively moves `context.*` to `moved.*`, and returns 64 rich
rows containing the reconstructed object. The wide shape performs the same
transform across all 8,192 entries before its 64-row limit. Same-run controls
scan the identical source rowsets and return the same cardinality using the
established time sort and original `context` projection. They control storage
and result count; they are not semantic equivalents to sequential typed
movement and source removal.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `rename`, indexed host | 64 | 2,410 | 3.548 | 3.770 | 4.322 | 1 | 1,024 | 235,778 | 128 |
| `rename`, full fixture | 64 | 2,410 | 39.524 | 43.696 | 43.930 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 2,538 | 3.239 | 3.553 | 3.827 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 2,538 | 33.771 | 36.673 | 38.738 | 4 | 8,192 | 1,914,055 | 8,192 |

The `rename` p95 is 6.1%/19.1% above its narrow/wide same-run control. Its
internal API timer averages 2.749/38.868 ms versus 2.464/32.756 ms, or
11.5%/18.7% above. Responses are 5.0% smaller because destination name
`moved` is two bytes shorter than control field `context`. Every equal-width
pair performs exactly the same public storage scan, block decode, payload
read, and row materialization. The bounded wildcard snapshot, recursive
flattening, typed value clone, source removal, empty-parent pruning, and
destination reconstruction cost is accepted in the Rust API. `SQL-LOG-034`
already gives direct SQLite/libSQL users the public exact top-level
equivalent, so a new extension primitive would not avoid storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.341 ms and the explicit durability barrier took 36.559 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P20.
Logs HWM was 97,552 KiB, 3,432 KiB above LQL-P20; metrics HWM was 51,288 KiB,
1,456 KiB below it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `rename`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin cancellation, bounded work/state/result/
response behavior, rejection, and reader reuse.

All 616 pinned VictoriaLogs v1.52.0 cases pass live. The final 38-test logs
real-extension suite, 81 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 100 SQL recipes (136 statements) pass locally.
The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or downstream
repository was used or modified.

## Session 17 LogsQL P2: bounded `format`

The checked-in
[`2026-08-05_session17_lql_p22_format.json`](evidence/2026-08-05_session17_lql_p22_format.json)
was captured from exact release extension and server build
`49761cdc2d980ffc2110a9e3483994b8a4d9b47b`. The narrow shape scans one
indexed host, projects `context` and `status`, formats three rich values into
`rendered`, and returns 64 rows. The wide shape performs the same transform
across all 8,192 entries before its 64-row limit. Same-run controls scan the
identical source rowsets and return the same cardinality using established
time sort and `range_key` projection. They control storage and result count;
they are not semantic equivalents to pattern interpolation and transforms.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `format`, indexed host | 64 | 1,706 | 2.986 | 3.297 | 3.903 | 1 | 1,024 | 235,778 | 128 |
| `format`, full fixture | 64 | 1,706 | 36.493 | 39.353 | 40.036 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 1,600 | 2.868 | 3.090 | 4.200 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 1,600 | 33.004 | 35.941 | 36.801 | 4 | 8,192 | 1,914,055 | 8,192 |

The `format` p95 is 6.7%/9.5% above its narrow/wide same-run control. Its
internal API timer averages 2.502/36.118 ms versus 2.406/32.465 ms, or
4.0%/11.3% above. Responses are 6.6% larger because
the formatted field and value are longer than the control. Every equal-width
pair performs exactly the same public storage scan, block decode, payload
read, and row materialization. Pattern traversal, rich textual projection,
transforms, output expansion, conditions, preservation, and destination
mutation remain bounded Rust API work. `SQL-LOG-035` already gives direct
SQLite/libSQL users ordinary public JSON1/`printf` interpolation, so a new
extension primitive would not avoid storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
15.217 ms and the explicit durability barrier took 34.807 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P21.
Logs HWM was 99,516 KiB, 1,964 KiB above LQL-P21; metrics HWM was 51,752 KiB,
464 KiB above it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `format`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin cancellation, bounded work/state/result/
response behavior, rejection, and reader reuse.

All 649 pinned VictoriaLogs v1.52.0 cases pass live. The final 39-test logs
real-extension suite, 83 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 101 SQL recipes (137 statements) pass locally.
The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or downstream
repository was used or modified.

## Session 17 LogsQL P2: bounded `math` / `eval`

The checked-in
[`2026-08-06_session17_lql_p23_math.json`](evidence/2026-08-06_session17_lql_p23_math.json)
was captured from exact release extension and server build
`84c1a77352aa33fc32139efe8c814e9dcadcff3c`. The narrow shape scans one
indexed host, projects `context` and `status`, computes
`context.attempt * 2 + status`, and returns 64 result strings. The wide shape
performs the same calculation across all 8,192 entries before its 64-row
limit. Same-run controls scan the identical source rowsets and return the same
cardinality using established time sort and `status` projection. They control
storage and result count; they are not semantic equivalents to expression
evaluation.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `math`, indexed host | 64 | 1,216 | 3.077 | 3.357 | 4.491 | 1 | 1,024 | 235,778 | 128 |
| `math`, full fixture | 64 | 1,216 | 36.702 | 39.127 | 40.861 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 960 | 3.000 | 3.292 | 4.772 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 960 | 32.975 | 37.655 | 40.066 | 4 | 8,192 | 1,914,055 | 8,192 |

The `math` p95 is 2.0%/3.9% above its narrow/wide same-run control. Its
internal API timer averages 2.534/36.284 ms versus 2.475/33.097 ms, or
2.4%/9.6% above. Responses are 26.7% larger because `computed` and its value
are longer than the projected control field. Every equal-width pair performs
exactly the same public storage scan, block decode, payload read, and row
materialization. AST evaluation, current-row coercion, float formatting,
sequential destinations, limits, cancellation, and errors remain bounded
Rust API work. `SQL-LOG-036` already gives direct SQLite/libSQL users ordinary
public JSON1 arithmetic, so a new extension primitive would not avoid storage
work.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.050 ms and the explicit durability barrier took 33.474 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P22.
Logs HWM was 94,168 KiB, 5,348 KiB below LQL-P22; metrics HWM was 52,568 KiB,
816 KiB above it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `math`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin AST/work/state/result/response bounds,
rejection, cancellation, and reader reuse.

All 690 pinned VictoriaLogs v1.52.0 cases pass live. The final 40-test logs
real-extension suite, 86 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 102 SQL recipes (138 statements) pass locally.
The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or downstream
repository was used or modified.

## Session 17 LogsQL P2: bounded `len`

The checked-in
[`2026-08-06_session17_lql_p24_len.json`](evidence/2026-08-06_session17_lql_p24_len.json)
was captured from exact release extension and server build
`64ec776668de88fdc3cf8bd6649ba7de2ad47b6e`. The narrow shape scans one
indexed host, projects `context` and `status`, measures the UTF-8 byte length
of `context.attempt`, and returns 64 decimal result strings. The wide shape
performs the same transform across all 8,192 entries before its 64-row limit.
Same-run controls scan the identical source rowsets and return the same
cardinality using established time sort and `status` projection. They control
storage and result count; they are not semantic equivalents to byte-length
projection.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `len`, indexed host | 64 | 1,088 | 3.431 | 3.785 | 4.816 | 1 | 1,024 | 235,778 | 128 |
| `len`, full fixture | 64 | 1,088 | 38.289 | 40.724 | 44.752 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 960 | 3.355 | 3.620 | 4.006 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 960 | 34.650 | 36.622 | 39.475 | 4 | 8,192 | 1,914,055 | 8,192 |

The `len` p95 is 4.6%/11.2% above its narrow/wide same-run control. Its
internal API timer averages 2.641/37.264 ms versus 2.669/33.096 ms, or 1.0%
lower/12.6% higher. Responses are 13.3% larger because `computed` and its
string value are longer than the projected control field. Every equal-width
pair performs exactly the same public storage scan, block decode, payload
read, and row materialization. Exact current-row lookup, byte counting,
compact-JSON traversal where needed, result rendering, destination mutation,
limits, cancellation, and errors remain bounded Rust API work.
`SQL-LOG-037` already gives direct SQLite/libSQL users ordinary public JSON1
and BLOB byte-length composition, so a new extension primitive would not
avoid storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
14.900 ms and the explicit durability barrier took 37.129 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P23.
Logs HWM was 96,060 KiB, 1,892 KiB above LQL-P23; metrics HWM was 52,748 KiB,
180 KiB above it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one `len`
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin work/state/result/response bounds,
rejection, cancellation, and reader reuse.

All 711 pinned VictoriaLogs v1.52.0 cases pass live. The final 41-test logs
real-extension suite, 88 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 103 SQL recipes (139 statements) pass locally.
The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or downstream
repository was used or modified.

## Session 17 LogsQL P2: bounded `drop_empty_fields`

The checked-in
[`2026-08-06_session17_lql_p28_drop_empty_fields.json`](evidence/2026-08-06_session17_lql_p28_drop_empty_fields.json)
was captured from exact release extension and server build
`88b26b01194a2d863107406f6aba099380683dd7`. The narrow shape scans one
indexed host, projects the rich `context` object and numeric `status`, applies
`drop_empty_fields`, and returns 64 rows. The wide shape performs the same
recursive typed traversal across all 8,192 entries before its 64-row limit.
Same-run controls scan the identical source rowsets, sort by time, and project
the same fields and cardinality. The fixture contains no empty values in this
projection, so responses are byte-identical; removal paths, sequentially
created empties, parent/row pruning, and native-type retention are exercised
by the semantic regressions rather than changing benchmark output.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `drop_empty_fields`, indexed host | 64 | 3,370 | 3.392 | 4.542 | 4.727 | 1 | 1,024 | 235,778 | 128 |
| `drop_empty_fields`, full fixture | 64 | 3,370 | 37.190 | 38.151 | 38.617 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 3,370 | 3.176 | 6.994 | 7.827 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 3,370 | 32.644 | 35.779 | 39.217 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 35.1% below/6.6% above its narrow/wide same-run control.
Its internal API timer averages 2.681/35.722 ms versus 2.772/32.183 ms, or
3.3% lower/11.0% higher. The narrow control's higher tail is retained as run
variation rather than treated as an optimization. Every equal-width pair
performs exactly the same public storage scan, block decode, payload read, row
materialization, and response encoding. Recursive rich-object traversal,
typed empty decisions, parent/row pruning, nesting/work limits, cancellation,
and errors remain bounded Rust API work. `SQL-LOG-038` already gives direct
SQLite/libSQL users ordinary exact-path JSON1 removal, so a new extension
primitive would not avoid storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
17.354 ms and the explicit durability barrier took 35.479 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Those values are byte-identical to LQL-P24.
Logs HWM was 92,544 KiB, 3,516 KiB below LQL-P24; metrics HWM was 50,516 KiB,
2,232 KiB below it. Each maximum spans the enlarged complete workload and is
retained as whole-process variation rather than attributed to one transform
request. Cancellation ended with zero requests in flight; direct evaluator
and HTTP deadline regressions pin nesting/work/result/response bounds,
rejection, cancellation, and reader reuse.

All 722 pinned VictoriaLogs v1.52.0 cases pass live. The final 42-test logs
real-extension suite, 90 logs library tests, complete supported extension and
Rust server workspaces, complete 45-section CLI/crash/transaction suite,
30-test Rust query harness, documentation contracts, Clippy with warnings
denied, formatting, and all 104 SQL recipes (140 statements) pass locally.
The extension's authoritative 8,192-entry batching, storage formats,
compression, indexes, retention, optimize, transactions, migrations, and
public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or downstream
repository was used or modified.

## Session 17 LogsQL P2: bounded literal `replace`

The checked-in
[`2026-08-06_session17_lql_p29_replace.json`](evidence/2026-08-06_session17_lql_p29_replace.json)
was captured from exact release extension and server build
`2288307503c1110d9fa5ac9056a35d965afb61a4`. The narrow shape scans one
indexed host, projects `range_key`, replaces literal `key` with equal-width
`log`, and returns 64 rows. The wide shape performs the same transform across
all 8,192 entries before its 64-row limit. Same-run controls scan the
identical source rowsets, sort by time, and project the unchanged field.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `replace`, indexed host | 64 | 1,600 | 3.240 | 3.520 | 3.576 | 1 | 1,024 | 235,778 | 128 |
| `replace`, full fixture | 64 | 1,600 | 36.716 | 37.711 | 37.948 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 1,600 | 3.261 | 3.908 | 4.406 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 1,600 | 35.734 | 36.882 | 37.846 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 9.9% below/2.2% above its narrow/wide same-run control.
Its internal API timer averages 2.509/35.530 ms versus 2.603/34.596 ms, or
3.6% below/2.7% above. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, row materialization, and response
encoding. Literal matching, exact two-pass output sizing, rich no-op policy,
field mutation, limits, cancellation, and errors remain bounded Rust API
work. `SQL-LOG-039` already gives direct SQLite/libSQL users ordinary public
JSON1/core-`replace()` composition, so a new extension primitive would not
avoid storage work.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.106 ms and the explicit durability barrier took 35.430 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 92,060 KiB and metrics HWM was
53,448 KiB. Each maximum spans the enlarged complete workload and is retained
as whole-process variation. Cancellation ended with zero requests in flight.

All 740 pinned VictoriaLogs v1.52.0 cases pass live. The final 43-test logs
real-extension suite, 92 logs library tests, complete 45-section CLI/crash/
transaction suite, 30-test Rust query harness, documentation contracts,
Clippy with warnings denied, formatting, and all 105 SQL recipes (141
statements) pass locally. Storage formats, batching, compression, indexes,
retention, optimize, transactions, migrations, and public batch/SQL contracts
are unchanged. No private table, fallback process, CI workflow, tag, release,
or downstream repository was used or modified.

## Session 17 LogsQL P2: bounded `replace_regexp`

The checked-in
[`2026-08-06_session17_lql_p30_replace_regexp.json`](evidence/2026-08-06_session17_lql_p30_replace_regexp.json)
was captured from exact release extension and server build
`8c7c27d4f98f7bfd58c0b27764c7f48f2b2ff425`. The narrow shape scans one
indexed host, projects `range_key`, matches `^key-([0-9a-f]+)$`, expands the
capture into equal-width `log-$1`, and returns 64 rows. The wide shape performs
the same transform across all 8,192 entries before its 64-row limit. Same-run
controls scan the identical source rowsets, sort by time, and project the
unchanged field.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `replace_regexp`, indexed host | 64 | 1,600 | 3.303 | 3.442 | 3.533 | 1 | 1,024 | 235,778 | 128 |
| `replace_regexp`, full fixture | 64 | 1,600 | 39.620 | 40.628 | 45.369 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 1,600 | 3.200 | 3.391 | 3.646 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 1,600 | 33.545 | 35.822 | 36.126 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 1.5%/13.4% above its narrow/wide same-run control. Its
internal API timer averages 2.698/38.925 ms versus 2.693/32.479 ms, or
0.2%/19.8% above. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, row materialization, and response
encoding. Request-once pattern compilation, automata matching, capture-
template sizing/rendering, field mutation, limits, cancellation, and errors
remain bounded Rust API work. Core SQLite and the public extension have no
portable RE2 capture-replacement scalar, so no false SQL recipe is claimed;
the measured post-scan API cost does not justify a LogsQL-specific extension
primitive.

All 8,192 rich entries completed durably with zero queued work. Admission took
12.971 ms and the explicit durability barrier took 36.993 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 93,256 KiB, 1,196 KiB above
LQL-P29; metrics HWM was 52,100 KiB, 1,348 KiB below it. Each maximum spans
the enlarged complete workload and is retained as whole-process variation.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin work/state/result/response bounds, rejection,
cancellation, and reader reuse.

All 765 pinned VictoriaLogs v1.52.0 cases pass live. The final 44-test logs
real-extension suite, 94 logs library tests, complete 45-section CLI/crash/
transaction suite, 30-test Rust query harness, documentation contracts,
Clippy with warnings denied, formatting, and all 105 SQL recipes (141
statements) pass locally. The extension's authoritative 8,192-entry batching,
storage formats, compression, indexes, retention, optimize, transactions,
migrations, and public batch/SQL contracts are unchanged. No private shadow
table, Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or
downstream repository was used or modified.

## Session 17 LogsQL P2: bounded literal `extract`

The checked-in
[`2026-08-06_session17_lql_p32_extract.json`](evidence/2026-08-06_session17_lql_p32_extract.json)
was captured from exact release extension and server build
`0656dcf0c752729a2dfa755d322e6de281a8b007`. The narrow shape scans one
indexed host, projects `range_key`, extracts its suffix from the fixed
`key-<extracted_key>` pattern, and returns 64 rows. The wide shape performs
the same transform across all 8,192 entries before its 64-row limit. Same-run
controls scan the identical source rowsets, sort by time, and project the
unchanged `range_key`. Responses are byte-identical because the extracted
value loses the four-byte `key-` prefix while the destination field name adds
four bytes.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `extract`, indexed host | 64 | 1,600 | 2.977 | 3.269 | 3.673 | 1 | 1,024 | 235,778 | 128 |
| `extract`, full fixture | 64 | 1,600 | 37.121 | 39.052 | 43.064 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 1,600 | 2.925 | 3.201 | 3.315 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 1,600 | 32.704 | 33.944 | 34.306 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 2.1%/15.0% above its narrow/wide same-run control. Its
internal API timer averages 2.409/36.020 ms versus 2.451/32.095 ms, or 1.7%
below/12.2% above. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, row materialization, and response
encoding. Literal pattern traversal, quoted-prefix decoding, capture sizing,
preservation decisions, exact current-row writes, limits, cancellation, and
errors remain bounded Rust API work. `SQL-LOG-040` already gives direct
SQLite/libSQL users ordinary public JSON1/core-string composition for two
fixed unquoted captures, so a new extension primitive would not avoid storage
work.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.273 ms and the explicit durability barrier took 36.747 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 92,524 KiB, 732 KiB below
LQL-P30; metrics HWM was 51,640 KiB, 460 KiB below it. Each maximum spans the
enlarged complete workload and is retained as whole-process variation.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin quoted-decode/work/state/result/response bounds,
rejection, cancellation, and reader reuse.

All 802 pinned VictoriaLogs v1.52.0 cases pass live. The final 45-test logs
real-extension suite, 96 logs library tests, complete 45-section CLI/crash/
transaction suite, 31-test Rust query harness, documentation contracts,
Clippy with warnings denied, formatting, and all 106 SQL recipes (142
statements) pass locally. The extension's authoritative 8,192-entry batching,
storage formats, compression, indexes, retention, optimize, transactions,
migrations, and public batch/SQL contracts are unchanged. No private shadow
table, Elixir/BEAM/NIF/process fallback, CI workflow, tag, release, or
downstream repository was used or modified.

## Session 17 LogsQL P2: bounded RE2 `extract_regexp`

The checked-in
[`2026-08-06_session17_lql_p33_extract_regexp.json`](evidence/2026-08-06_session17_lql_p33_extract_regexp.json)
was captured from exact release extension and server build
`5bb72dafd2d93209c686e89f8ed03361b06ed425` and has SHA-256
`9118bc5d9b1e76b388dd6011595de98753e2f5c05fd2b903611318f81087493a`.
The narrow shape scans one indexed host, projects `range_key`, captures its
suffix with `^key-(?P<extracted_key>[0-9a-f]+)$`, and returns 64 rows. The
wide shape performs the same transform across all 8,192 entries before its
64-row limit. Same-run controls scan the identical source rowsets, sort by
time, and project the unchanged `range_key`. Responses are byte-identical
because the captured value loses the four-byte `key-` prefix while the
destination field name adds four bytes.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `extract_regexp`, indexed host | 64 | 1,600 | 3.027 | 3.154 | 4.780 | 1 | 1,024 | 235,778 | 128 |
| `extract_regexp`, full fixture | 64 | 1,600 | 34.890 | 35.517 | 37.085 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/cardinality control, indexed host | 64 | 1,600 | 3.034 | 3.922 | 4.296 | 1 | 1,024 | 235,778 | 128 |
| time-sort/cardinality control, full fixture | 64 | 1,600 | 32.696 | 33.808 | 38.479 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 19.6% below/5.1% above its narrow/wide same-run control.
Its internal API timer averages 2.490/34.052 ms versus 2.530/32.089 ms, or
1.6% below/6.1% above. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, row materialization, and response
encoding. Request-once pattern compilation, first-match automata execution,
named-capture sizing, preservation decisions, exact current-row writes,
limits, cancellation, and errors remain bounded Rust API work. Core SQLite
and the public extension have no portable RE2 named-capture extraction
scalar, so no false SQL recipe is claimed; the measured post-scan API cost
does not justify a LogsQL-specific extension primitive.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.781 ms and the explicit durability barrier took 34.762 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 95,424 KiB, 2,900 KiB above
LQL-P32; metrics HWM was 53,124 KiB, 1,484 KiB above it. Each maximum spans
the enlarged complete workload and is retained as whole-process variation.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin compiled-pattern/work/state/result/response bounds,
rejection, cancellation, and reader reuse.

All 835 pinned VictoriaLogs v1.52.0 cases pass live. The final 46-test logs
real-extension suite, 98 logs library tests, complete 45-section CLI/crash/
transaction suite, standalone dbhealth lifecycle gate, 31-test Rust query
harness, documentation contracts, Clippy with warnings denied, formatting,
and all 106 SQL recipes (142 statements) pass locally. The extension's
authoritative 8,192-entry batching, storage formats, compression, indexes,
retention, optimize, transactions, migrations, and public batch/SQL contracts
are unchanged. No private shadow table, Elixir/BEAM/NIF/process fallback, CI
workflow, tag, release, or downstream repository was used or modified.

## Session 17 LogsQL P2: typed `pack_json`

The checked-in
[`2026-08-06_session17_lql_p34_pack_json.json`](evidence/2026-08-06_session17_lql_p34_pack_json.json)
was captured from exact release extension and server build
`1e20d8f49e440e27f0f12930e71778277bf92f06` and has SHA-256
`570c0df848052cbfb1fe463cf10074cb7c381a3b0ed3f34e3a59d0e882c2a7e4`.
The narrow shape scans one indexed host, projects `range_key`, packs that
field into a native JSON object string named `packed`, and returns 64 rows.
The wide shape performs the same transform across all 8,192 entries before
its 64-row limit. Same-run controls scan the identical source rowsets, sort
by time, and project the unchanged `range_key`; their smaller response is an
intentional measurement of the JSON wrapper bytes as well as API work.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `pack_json`, indexed host | 64 | 2,688 | 2.946 | 3.146 | 4.588 | 1 | 1,024 | 235,778 | 128 |
| `pack_json`, full fixture | 64 | 2,688 | 35.570 | 37.921 | 38.085 | 4 | 8,192 | 1,914,055 | 8,192 |
| time-sort/plain-field control, indexed host | 64 | 1,600 | 2.959 | 3.098 | 3.227 | 1 | 1,024 | 235,778 | 128 |
| time-sort/plain-field control, full fixture | 64 | 1,600 | 32.567 | 35.717 | 37.101 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 1.5%/6.2% above its narrow/wide same-run control. Its
internal API timer averages 2.420/34.685 ms versus 2.452/32.300 ms, or 1.3%
below/7.4% above. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, and row materialization.
Pre-write snapshots, deterministic recursive selector union, native rich JSON
preservation, compact serialization, destination writes, limits,
cancellation, and errors remain bounded Rust API work. `SQL-LOG-041` already
gives direct SQLite/libSQL users public JSON1 composition for bounded exact
paths, so the measured post-scan API cost does not justify a LogsQL-specific
extension primitive.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.131 ms and the explicit durability barrier took 37.670 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 93,644 KiB, 1,780 KiB below
LQL-P33; metrics HWM was 49,944 KiB, 3,180 KiB below it. Each maximum spans
the enlarged complete workload and is retained as whole-process variation.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin nesting/work/state/result/response bounds,
rejection, cancellation, and reader reuse.

All 850 pinned VictoriaLogs v1.52.0 cases pass live. The final 47-test logs
real-extension suite, 100 logs library tests, complete 45-section CLI/crash/
transaction suite, standalone dbhealth lifecycle gate, 31-test Rust query
harness, documentation contracts, Clippy with warnings denied, formatting,
and all 107 SQL recipes (143 statements) pass locally. The extension's
authoritative 8,192-entry batching, storage formats, compression, indexes,
retention, optimize, transactions, migrations, and public batch/SQL contracts
are unchanged. No private shadow table, Elixir/BEAM/NIF/process fallback, CI
workflow, tag, release, or downstream repository was used or modified.

## Session 17 LogsQL P2: typed `unpack_json`

The checked-in
[`2026-08-06_session17_lql_p36_unpack_json.json`](evidence/2026-08-06_session17_lql_p36_unpack_json.json)
was captured from exact release extension and server build
`035c76baf3b684e29714bd1a2b8f955864fa752a` and has SHA-256
`540bb59a867d7b9c980084e7e61e3d616b386a02ca1c34b14017721be47370d4`.
The narrow shape scans one indexed host, packs `range_key` as a JSON object,
unpacks its exact key into `decoded_range_key`, and returns 64 rows. The wide
shape performs the same transform across all 8,192 entries before its 64-row
limit. Same-run controls perform the same pack, copy its typed source value to
the same destination, and return byte-identical responses.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `unpack_json`, indexed host | 64 | 2,112 | 2.933 | 3.151 | 3.256 | 1 | 1,024 | 235,778 | 128 |
| `unpack_json`, full fixture | 64 | 2,112 | 36.963 | 40.062 | 65.292 | 4 | 8,192 | 1,914,055 | 8,192 |
| pack-plus-copy control, indexed host | 64 | 2,112 | 2.962 | 3.763 | 4.407 | 1 | 1,024 | 235,778 | 128 |
| pack-plus-copy control, full fixture | 64 | 2,112 | 36.314 | 38.694 | 41.375 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 16.3% below/3.5% above its narrow/wide same-run control.
Its internal API timer averages 2.502/37.005 ms versus 2.516/35.815 ms, or
0.5% below/3.3% above. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, row materialization, and response
encoding. Source snapshotting, JSON validation and parsing, native type
preservation, recursive selection and nesting reconstruction, exact
missing-field synthesis, destination writes, limits, cancellation, and errors
remain bounded Rust API work. `SQL-LOG-042` already gives direct SQLite/libSQL
users public JSON1 composition for bounded fixed paths, so a new extension
primitive would not avoid storage reads, decode, allocation, or row crossing.
The 65.292 ms wide p99 is retained honestly.

All 8,192 rich entries completed durably with zero queued work. Admission took
16.278 ms and the explicit durability barrier took 40.251 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 93,280 KiB, 364 KiB below
LQL-P34; metrics HWM was 50,944 KiB, 1,000 KiB above it. Each maximum spans
the enlarged complete workload and is retained as whole-process variation.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin nesting/work/state/result/response bounds, rejection,
cancellation, and reader reuse.

All 875 pinned VictoriaLogs v1.52.0 cases pass live. The final 48-test logs
real-extension suite, 102 logs library tests, 90-test metrics real-extension
suite, complete 45-section CLI/crash/transaction suite, standalone dbhealth
lifecycle gate, 31-test Rust query harness, documentation contracts, Clippy
with warnings denied, formatting, and all 108 SQL recipes (144 statements)
pass locally. The extension's authoritative 8,192-entry batching, storage
formats, compression, indexes, retention, optimize, transactions, migrations,
and public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 17 LogsQL P2: top-level `json_array_len`

The checked-in
[`2026-08-06_session17_lql_p41_json_array_len.json`](evidence/2026-08-06_session17_lql_p41_json_array_len.json)
was captured from exact release extension and server build
`745eb01b059b3c0dd6b7b62d152bd23a423f0f00` and has SHA-256
`be29595859bbd3b277b79aab5f4c28b5fa71e7cd2d397b6b76dd3825151c2f0c`.
The narrow shape selects one indexed host, counts the retained native `tags`
array, and returns 64 textual lengths. The wide shape applies the same
operation to all 8,192 entries before its 64-row limit. Same-run controls
write the known textual length through `format` and return byte-identical
responses.

| shape | result rows | response bytes | p50 ms | p95 ms | p99 ms | candidate blocks/query | decoded entries/query | extension payload bytes/query | public rows materialized/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `json_array_len`, indexed host | 64 | 1,344 | 3.285 | 3.558 | 3.788 | 1 | 1,024 | 235,778 | 128 |
| `json_array_len`, full fixture | 64 | 1,344 | 39.914 | 41.563 | 44.668 | 4 | 8,192 | 1,914,055 | 8,192 |
| constant-format control, indexed host | 64 | 1,344 | 3.276 | 3.454 | 3.514 | 1 | 1,024 | 235,778 | 128 |
| constant-format control, full fixture | 64 | 1,344 | 39.836 | 40.607 | 43.555 | 4 | 8,192 | 1,914,055 | 8,192 |

The transform p95 is 3.0%/2.4% above its narrow/wide same-run control. Its
internal API timer averages 2.794/39.027 ms versus 2.729/39.039 ms, or 2.4%
above/0.0% below. Every equal-width pair performs byte-identical public
storage scans, block decodes, payload reads, row materialization, and response
encoding. Reading the length of a native retained array is O(1); source
snapshotting, optional bounded JSON-text parsing, destination writes, limits,
cancellation, and errors remain bounded Rust API work. `SQL-LOG-043` already
gives direct SQLite/libSQL users public JSON1 composition for bounded fixed
paths, so a new extension primitive would not avoid storage reads, decode,
allocation, or row crossing.

All 8,192 rich entries completed durably with zero queued work. Admission took
13.119 ms and the explicit durability barrier took 35.315 ms. Storage remains
exactly four raw blocks, 1,914,055 logical payload bytes, and 2,022,736
physical database/WAL/SHM bytes. Logs HWM was 94,660 KiB, 1,380 KiB above
LQL-P36; metrics HWM was 53,328 KiB, 2,384 KiB above it. Each maximum spans
the enlarged complete workload and is retained as whole-process variation.
Cancellation ended with zero requests in flight; direct evaluator and HTTP
deadline regressions pin parse/work/state/result/response bounds, rejection,
cancellation, and reader reuse.

All 897 pinned VictoriaLogs v1.52.0 cases pass live. The final 49-test logs
real-extension suite, 104 logs library tests, 90-test metrics real-extension
suite, complete 45-section CLI/crash/transaction suite, standalone dbhealth
lifecycle gate, 31-test Rust query harness, documentation contracts, Clippy
with warnings denied, formatting, and all 109 SQL recipes (145 statements)
pass locally. The extension's authoritative 8,192-entry batching, storage
formats, compression, indexes, retention, optimize, transactions, migrations,
and public batch/SQL contracts are unchanged. No private shadow table,
Elixir/BEAM/NIF/process fallback, CI workflow or invocation, tag, release, or
downstream repository was used or modified.

## Session 19 experimental disposition: `limitk` and `limit_ratio`

`PQL-O17` is classified, not implemented. Two new cases pass against the
immutable Prometheus 3.13.2 API oracle and prove that its stable configuration
rejects `limitk` and modifier-form `limit_ratio` with the exact
`promql-experimental-functions` diagnostic. The complete Prometheus fixture is
now 530 cases. A failing-then-passing Rust parser regression pinned the same
source positions, and the real-extension regression pins exact GET envelopes,
POST diagnostics, shutdown, reopen, and the absence of raw/window extension
queries on every rejected request.

No narrow/wide p50/p95/p99, result-cardinality, storage-byte, or RSS comparison
is reported because the stable product deliberately performs no query and
returns no result for this experimental row. Manufacturing an execution
benchmark would imply support that does not exist. The relevant cost verdict
is exact: rejection happens before public storage work. No extension surface,
private table, storage format, batching, compression, index, rollup, retention,
transaction, migration, optimize, or maintenance behavior changed.

## Session 19 experimental disposition: binary fill modifiers

`PQL-O18` is classified, not enabled. Two new default-tier cases pass against
the immutable Prometheus 3.13.2 API oracle, bringing that fixture to 532 cases.
They pin symmetric `fill` and modifier-last `on`/`group_left`/`fill_right`
syntax plus the exact stable feature-gate diagnostic. Rust and real-extension
regressions failed first on the former internal “binary matching modifiers are
not shipped” message, then passed with exact GET envelopes, POST diagnostics,
shutdown, reopen, and zero raw/window extension queries.

Direct users receive executable `SQL-PROM-057`: 131 recipes and 169 statements
now pass against the public extension. It performs one bounded public grid per
side and ordinary full-outer one-to-one float composition with independently
optional defaults. No extension primitive can remove either required child
scan. No API narrow/wide p50/p95/p99, result-cardinality, storage-byte, or RSS
comparison is reported because stable Timeless never executes this
experimental syntax. The SQL reference is not relabeled as PromQL support. No
storage format, private access, batching, compression, index, rollup,
retention, transaction, migration, optimize, or maintenance behavior changed.

## Session 19 data-model disposition: native-histogram trim operators

`PQL-O19` remains deferred with a precise public boundary. Two new cases pass
against immutable Prometheus 3.13.2, bringing the complete API fixture to 534
cases. They prove that stable upstream syntax accepts `</` and `>/` and drops
float/float inputs with exact incompatible-type infos. Source at pinned commit
`bb5dff00cf8fdfbf5c65e0531aa835fa238a43a2` proves that useful evaluation
requires a native histogram on the left, a scalar on the right, complete
bucket decoding, threshold interpolation, and count/sum reconstruction.

Rust and real-extension regressions failed first on the opaque `parse error:
invalid promql query`, then passed with a typed-native-histogram prerequisite,
exact operator positions, quoted/comment exclusion, GET and POST envelopes,
shutdown, reopen, and zero raw/window extension queries. No SQL recipe exists
because public float rows and classic cumulative `_bucket` series do not
represent one typed native-histogram sample. No narrow/wide p50/p95/p99,
result-cardinality, storage-byte, or RSS comparison is reported because the
current product deliberately performs no query for this deferred row. No
extension surface, private access, storage format, batching, compression,
index, rollup, retention, transaction, migration, optimize, or maintenance
behavior changed.

## Session 19 experimental disposition: `mad_over_time`

`PQL-R22` is classified, not enabled. One new exact case passes against
immutable Prometheus 3.13.2, bringing the complete API fixture to 535 cases
and pinning its default `promql-experimental-functions` gate. Rust and
real-extension regressions failed first on the internal `unsupported PromQL
expression (parsed as function call)` message, then passed with the pinned
GET envelope, POST diagnostic, shutdown, reopen, and zero raw/window extension
queries.

Direct users receive executable `SQL-PROM-058`: 132 recipes and 170 statements
now pass against the public extension. The recipe performs bounded finite-float
linear median ranking twice over public raw rows and semantically pins even and
odd series partitions. No extension primitive can prune either pass or avoid
the required raw decode. No API narrow/wide p50/p95/p99, result-cardinality,
storage-byte, or RSS comparison is reported because stable Timeless never
executes this experimental function. The SQL reference is not relabeled as
PromQL support. No storage format, private access, batching, compression,
index, rollup, retention, transaction, migration, optimize, or maintenance
behavior changed.

## Session 19 experimental disposition: timestamp-of-range functions

`PQL-R23` is classified, not enabled. Four exact cases pass against immutable
Prometheus 3.13.2, bringing the complete API fixture to 539 cases and pinning
the default feature gate for first, last, min, and max timestamp functions.
Rust and real-extension regressions failed first on the internal `unsupported
PromQL expression (parsed as function call)` message, then passed with exact
GET envelopes, POST diagnostics, shutdown, reopen, and zero raw/window
extension queries for all four names.

Direct users receive executable `SQL-PROM-059`: 133 recipes and 171 statements
now pass against the public extension. One bounded public-raw query supports
all four finite-float modes and semantically pins earliest/latest source
timestamps plus latest-tied min/max timestamps across multiple series. No
extension primitive can avoid the required raw timestamps/value decode. No API
narrow/wide p50/p95/p99, result-cardinality, storage-byte, or RSS comparison is
reported because stable Timeless never executes these experimental functions.
The SQL reference is not relabeled as PromQL support. No storage format,
private access, batching, compression, index, rollup, retention, transaction,
migration, optimize, or maintenance behavior changed.

## Session 19 experimental disposition: label sorting

`PQL-F14` is classified, not enabled. Two exact cases pass against immutable
Prometheus 3.13.2, bringing the complete API fixture to 541 cases and pinning
the default feature gate for ascending and descending label sorting. Rust and
real-extension regressions failed first on the internal `unsupported PromQL
expression (parsed as function call)` message, then passed with exact GET
envelopes, POST diagnostics, shutdown, reopen, and zero raw/window extension
queries for both names.

No SQL recipe is published: portable SQLite/libSQL text ordering cannot
reproduce upstream's natural numeric runs, ordered variadic label keys,
missing-label empty strings, and exact full-label-set tie-break. No API
narrow/wide p50/p95/p99, result-cardinality, storage-byte, or RSS comparison is
reported because stable Timeless never evaluates these experimental
functions. Future work is bounded Rust sorting after child evaluation, so no
extension primitive, private access, storage format, batching, compression,
index, rollup, retention, transaction, migration, optimize, or maintenance
behavior changed.

## Session 19 catalog disposition: remaining MetricsQL names

`MQL-08` is classified, not enabled. A read-only audit of the old
`TimelessMetrics.PromQL` parser proved that its twelve-name `@metricsql_fns`
list was rejection-only, so the matrix's prior `partial` Elixir claim had no
implementation behind it. Twelve deterministic cases pass against immutable
VictoriaMetrics 1.148.0, bringing its API fixture from 184 to 196 cases and
proving that every catalog item is a real upstream construct. Pinned source
places them in four distinct evaluator families plus parser-time `WITH`
expansion; one catch-all cannot be an honest compatibility implementation.

The Rust unit regression failed first on PromQL-parser “unknown function”
errors. The completed real-extension regression now pins case-insensitive,
source-positioned HTTP 400 `bad_data` failures across all twelve names, GET,
POST, range, shutdown, and reopen. Quoted strings and line comments are
isolated, and raw/window extension query counters remain unchanged. There is
no SQL recipe because the row is a rejection catalog rather than an
expression; each future construct must receive an individual SQL/API
ownership decision.

No API narrow/wide p50/p95/p99, result-cardinality, storage-byte, or RSS
comparison is reported because Timeless deliberately does not evaluate these
constructs. An individual admitted row must add those measurements. No
extension primitive, private access, storage format, batching, compression,
index, rollup, retention, transaction, migration, optimize, or maintenance
behavior changed.

## Session 19 experimental disposition: info-series enrichment

`PQL-F21` is classified, not enabled. Two exact cases pass against immutable
Prometheus 3.13.2, bringing the complete API fixture to 543 cases and pinning
the default feature gate for the one-argument and selector-argument forms.
Rust and real-extension regressions failed first on the internal `unsupported
PromQL expression (parsed as function call)` message, then passed with exact
GET envelopes, POST diagnostics, shutdown, reopen, and zero raw/window
extension queries for both forms.

No SQL recipe is published. Pinned source and behavior require a separate
lookback-aware info selection, fixed `job`/`instance` identity, newest-source
conflict resolution, exact stale markers, matcher-versus-empty behavior, and
float/native-histogram base values. The retained model needs `PQL-S17` and
`PQL-S22` before a feature-enabled Rust tier can claim parity. No API
narrow/wide p50/p95/p99, result-cardinality, storage-byte, or RSS comparison is
reported because stable Timeless never evaluates the function. No extension
primitive, private access, storage format, batching, compression, index,
rollup, retention, transaction, migration, optimize, or maintenance behavior
changed.

## Session 19 data-model disposition: native histogram samples

`PQL-S22` remains deferred, with a stronger public boundary. The extension
capability test and loaded-SQL regression failed first because
`signals.metrics.sample_types` and `native_histograms` were absent. They now
pass with exact values `["float64"]` and `false`. The declaration is additive:
`data_abi` remains 1, existing hosts ignore unknown JSON members, and every
named/resolved batch, chunk, rollup, row, and packed frame remains unchanged.

Pinned Prometheus 3.13.2 source establishes that an honest future design must
retain schema, zero/count/sum, positive/negative spans and populations, custom
bounds, counter-reset/gauge hints, mixed sample histories, and exact query
semantics. The checked prerequisite list additionally covers marker-capable
ingress, SQL and packed results, compression, rollups, retention,
transactions, migration/downgrade, corruption, limits, cancellation, backup,
and reopen. There is no SQL recipe and no narrow/wide latency, cardinality,
storage, or RSS benchmark for a sample type the product does not store. No
private table, storage format, batch, compression, index, rollup, retention,
transaction, migration, optimize, or maintenance behavior changed.

## Session 19 retained-model disposition: native-histogram scalar functions

`PQL-H04` remains deferred for value-producing typed samples, but its complete
float-only behavior now executes. Six exact cases pass against immutable
Prometheus 3.13.2, bringing the API fixture to 549 cases. Rust and
real-extension regressions failed first on the internal unsupported-call
error, then passed for all five functions with child evaluation, classic
bucket/sum/count non-coercion, exact empty GET/POST/range envelopes, shutdown,
and reopen.

Exact release build `f6947a6bb810a55e2cd171723b6a0a1f1961b5a5`
measures `histogram_count(float-vector)` at 0.657/0.823/0.846 ms
narrow and 3.907/4.203/4.367 ms wide p50/p95/p99. Equal-read empty float
comparison controls measure 0.552/0.786/0.941 and 3.947/4.239/4.502 ms.
The candidate p95 is 4.6% higher narrow and 0.8% lower wide. Both candidate
and control return zero samples in identical 63-byte responses.

Every narrow pair executes one public raw query per request, considers one
chunk, decodes 32 points, reads 131 payload bytes, and returns 31 child points.
Every wide pair considers 512 chunks, decodes and returns 16,384 child points,
and reads 53,831 payload bytes. Thus the function preserves required child
work without storage amplification. The run completed 136,953 metric points
across the primary and limit fixtures with zero failed or queued points;
final metrics storage was 224,688 logical bytes, 409,600 index bytes, and
1,542,312 physical database/WAL/SHM bytes. Metrics RSS HWM was 52,224 KiB.

The artifact is
[`2026-08-07_session19_pql_h04_native_histogram_float.json`](evidence/2026-08-07_session19_pql_h04_native_histogram_float.json)
(SHA-256 `0d63c1896c590f1afc451b5cd7e8a0c8681b3316641ae27c2beea5c1edd51b47`).
There is no SQL recipe: returning no rows without evaluating the child would
not reproduce errors, limits, cancellation, or public storage work. No
extension primitive, private access, storage format, batching, compression,
index, rollup, retention, transaction, migration, optimize, or maintenance
behavior changed.

## Session 19 data-model disposition: LogsQL stream selectors

`LQL-F35` is classified, not enabled. Four deterministic cases pass against
immutable VictoriaLogs 1.52.0, bringing the complete LogsQL fixture from
1,388 to 1,392 cases. They pin bare and `_stream:`-prefixed selectors,
equality, regular expression, static membership, and disjunction over the
oracle harness's ingestion-declared `case` stream field. Source audit pins the
upstream boundary: ingestion canonicalizes nonempty configured stream fields,
hashes them into a tenant-scoped ID, indexes that identity, and applies the
selector before ordinary row filtering.

The Rust unit regression failed first on the opaque `unsupported LogsQL term`
message. The real-extension regression now pins source-positioned HTTP 422
`unsupported_logsql` envelopes across POST base and filter-pipeline forms,
shutdown, and reopen. The native-parameter GET route remains outside the
LogsQL grammar. Quoted and commented braces are isolated, one rich retained
row survives optimize/reopen, and API row-query, native-count, payload-byte,
and decoded-entry counters remain unchanged for every rejection.

The complete parser suite initially failed three existing plans because an
overbroad detector treated inline `rows({...})` objects as selectors. The
corrected scanner tracks explicit case-insensitive `rows` parenthesis groups,
including their nested groups, while still rejecting selectors in ordinary
parenthesized subqueries. The existing `join`, `union`, and discarded-prefix
`generate_sequence` regressions plus the new focused inline-row cases now pin
that boundary; see `QSF-262`.

There is no SQL recipe: row-wise metadata equality cannot reproduce a stream
identity that the current public batch and storage formats never recorded.
There is no narrow/wide latency, cardinality, storage-byte, or RSS benchmark
for syntax Timeless deliberately does not execute. The exact versioned
storage prerequisite is recorded in the matrix, plan, SQL disposition,
`QSF-261`, and `QSF-262`. No extension primitive, private access, storage format,
authoritative batching, compression, index, rollup, retention, transaction,
migration, optimize, or maintenance behavior changed.

## Session 19 data-model disposition: LogsQL stream IDs

`LQL-F36` is classified, not enabled. Six accepted and two error cases pass
against immutable VictoriaLogs 1.52.0, bringing the complete LogsQL fixture
from 1,392 to 1,400 cases. They pin a positive exact 48-hex ID derived from the
fixture's own `case="phrase-exact"` stream, quoted syntax, static and
query-backed membership, case-insensitive unquoted spelling, whitespace, and
strict malformed-ID behavior. The source audit confirms that `_stream_id` is
the 8-byte tenant identity plus 16-byte canonical stream hash and is a
physical block/search key, not ordinary row metadata.

The Rust regression failed first because `_stream_id:<id>` returned a valid
plan containing `MetadataExact { path: ["_stream_id"], ... }`. The corrected
pre-parser returns a source-positioned HTTP 422 before storage for exact,
quoted, static-list, query-backed, base, filter-pipe, shutdown, and reopen
forms. It distinguishes bare/message text, nested `foo._stream_id`, field
projection, comments, and explicit `rows({...})` data. One retained row with
nested `_stream_id`, integer, array, null, and boolean data remains exact
after optimize/reopen; API row-query, native-count, payload-byte, and
decoded-entry counters remain unchanged for every rejection.

There is no SQL recipe or benchmark: current public tables expose no tenant
prefix, canonical stream hash, compatible ID, or stream index. JSON extraction
over a nested application field remains useful row SQL but is not relabeled
as `LQL-F36`. The exact versioned prerequisite is shared with `LQL-F35` and
recorded in the matrix, plan, SQL disposition, and `QSF-263`. No extension
primitive, private access, storage format, authoritative batching,
compression, index, rollup, retention, transaction, migration, optimize, or
maintenance behavior changed.

## Session 19 LogsQL P3 result stream synthesis

`LQL-P49` is shipped as a bounded Rust API result transform. Eleven successful
and eight error witnesses pass inside the complete 1,419-case immutable
VictoriaLogs 1.52.0 fixture. The source audit and exact responses pin
exact/prefix/all-current selection, empty omission, bytewise name order, Go
quoting, `_stream` replacement, `_stream_id` clearing, prior-pipe visibility,
direct and query-backed optional conditions, false-row preservation,
case-insensitive grammar, and strict errors. Rich Timeless object leaves,
atomic arrays, overlap, limits, cancellation, immutable durable metadata,
optimize, flush, shutdown, and reopen pass against the real extension.

Exact release build `bf82f7625a15170b93d2a7ea8e8fd5ec94d6300c`
produces these loopback measurements over 50 requests after five warmups:

| shape | candidate p50/p95/p99 | equal-read control p50/p95/p99 | p95 delta | API-owned mean delta | result |
|---|---:|---:|---:|---:|---:|
| indexed host | 3.040/4.564/5.399 ms | 3.376/3.633/4.119 ms | +25.6% | -4.4% | 64 rows / 1,856 bytes |
| full fixture | 38.316/40.091/45.371 ms | 37.514/38.555/39.040 ms | +4.0% | +1.4% | 64 rows / 1,856 bytes |

Candidate and control each execute one public query per request. The narrow
pair considers one block, decodes 1,024 entries, reads 235,778 payload bytes,
matches and returns 128 public rows per request before the shared sort/limit.
The wide pair considers four blocks, decodes, matches, and returns all 8,192
entries, and reads 1,914,055 payload bytes per request. Thus P49 adds no
storage-read, decode, payload-transfer, or row-crossing amplification. The
narrow p95 tail is visible and honestly retained, but the lower API-owned mean
and identical physical work show no storage primitive opportunity; the wide
cost is a small bounded post-scan delta. `QSF-264`–`QSF-266` retain the exact
ownership, resolver, and performance verdicts.

All 8,192 log entries completed durably with zero queued work. Admission took
12.834 ms and the ordered durability barrier 38.400 ms. Storage remained four
raw blocks, 1,914,055 logical bytes, and 2,022,736 physical database/WAL/SHM
bytes. Logs RSS HWM was 103,820 KiB. The companion metrics workload completed
136,953 points with zero failed or queued points; metrics storage was 224,688
logical bytes, 409,600 index bytes, and 1,542,312 physical bytes, with 52,032
KiB RSS HWM. These are complete-workload process maxima, not allocations
attributed to this pipe.

The artifact is
[`2026-08-07_session19_lql_p49_set_stream_fields.json`](evidence/2026-08-07_session19_lql_p49_set_stream_fields.json)
(SHA-256 `5e947e05d0ff7cb49a55510be66ed42c9ba50b0cb23695297ea24ad397f2ee66`).
There is no SQL recipe because dynamic current-pipeline fields and exact Go
quoting have no honest portable core-SQL equivalent. No extension primitive,
private access, storage format, authoritative batching, compression, index,
rollup, retention, transaction, migration, optimize, or maintenance behavior
changed.

## Session 19 data-model disposition: LogsQL stream context

`LQL-P50` remains deferred. Five successful and five parser-error cases pass
against immutable VictoriaLogs 1.52.0, bringing the complete LogsQL fixture to
1,429 cases. They pin before/after/zero counts, option order,
case-insensitive spelling, configurable `time_window`, later projection,
empty input, and strict missing/negative/invalid values.

Pinned source establishes the incompatible boundary: `stream_context` must be
the first pipe after the filter; it reads each selected row's stored
`_stream_id`, groups at most 1,000 selected rows per each of at most 100
streams, derives the tenant from the ID, and performs additional exact-ID
time-range searches. It keeps bounded before/after rows, deduplicates
overlapping contexts, orders streams by their earliest match and rows by time
and fields, and emits `---` delimiter rows carrying `_stream_id` and `_stream`.
The retained Timeless model has no ingestion-declared stream fields, tenant
prefix, canonical stream hash, or stream index, so ordinary timestamp
adjacency or metadata grouping would mix unrelated streams.

The Rust parser regression failed first on the generic message `unsupported
LogsQL pipeline "stream_context before 1"`. The corrected parser returns a
source-positioned HTTP 422 before planning or storage for top-level and nested
genuine pipes. The first scanner revision then exposed a nested-query gap:
top-level-only segmentation produced HTTP 400 for
`case:in(* | stream_context before 1 | fields case)`. The quote-aware
all-depth scanner now gives the same explicit prerequisite while preserving
base words, quoted pipeline text, comments, field names, and nested application
metadata.

The real-extension regression pins exact envelopes, zero changes to public
row-query, native-count, payload-byte, and decoded-entry counters, rich typed
ordinary data, optimize, flush, shutdown, and reopen. There is no narrow/wide
latency, cardinality, storage-byte, or RSS benchmark because Timeless
deliberately does not execute the pipe. There is no SQL recipe: the public
storage model lacks the identity on which correct same-stream rereads depend.
`QSF-267`–`QSF-268` record the prerequisite and nested-scanner regression. No
extension primitive, private access, storage format, authoritative batching,
compression, index, rollup, retention, transaction, migration, optimize, or
maintenance behavior changed.
