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

The baseline workload contains 512 metric series with 32 float points each and
8,192 logs spanning all eight severities with typed nested metadata. Each
signal runs one indexed narrow query and one wide query for five warmups and 50
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
