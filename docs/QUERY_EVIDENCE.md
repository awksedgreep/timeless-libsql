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
- extension/API counter deltas, including frames/response bytes for metrics
  and candidate/decoded/returned work for logs;
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
