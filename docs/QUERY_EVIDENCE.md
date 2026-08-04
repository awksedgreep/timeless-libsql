# Query evidence protocol

Every shipped matrix row carries correctness evidence and a reproducible
narrow/wide performance record. Performance never overrides oracle semantics;
it decides whether ordinary Rust/SQL composition is sufficient or whether a
general extension primitive has earned its storage-aware complexity.

`tools/query_evidence.py` starts the release metrics and logs binaries on
loopback with authentication disabled, uses a temporary database, ingests a
deterministic fixture through the public HTTP/batch path, crosses the explicit
flush durability barrier, and measures the public query APIs. It never reads a
private shadow table. Shutdown sends `SIGTERM` and requires the server's normal
drain to exit successfully.

The current harness retains the 512-series, 32-point metric baseline and adds
64 one-series metric names for multi-name selector work; its log fixture keeps
8,192 entries spanning all eight severities with typed nested metadata.
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
python3 tools/query_evidence.py \
  --output docs/evidence/$(date +%F)_query_baseline.json
```

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
