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

Run from a release build whose build identity matches `HEAD`:

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
