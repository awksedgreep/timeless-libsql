# timeless-logs-api POC

This is an API-boundary proof of concept, not a replacement storage engine.

The storage contract is fixed:

- NDJSON requests are parsed into the existing logs batch-blob v0 format.
- `INSERT INTO logs(logs) VALUES (?1)` feeds the existing extension buffer.
- The extension's hard-coded 8,192-entry automatic flush is unchanged.
- The API never flushes at a request or producer-batch boundary.
- A one-second low-volume timer sends the existing `flush` command.
- A 30-second maintenance timer sends the existing `optimize` command only
  when raw blocks exist. `TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS` can defer it
  for isolated benchmarks without changing the production default.
- Graceful shutdown sends an ordered `flush` after all accepted batches.

`204` means the parsed batch was admitted to the bounded SQLite-writer queue,
matching the asynchronous Elixir ingestion contract. It does not claim raw
durability. `/api/v1/flush` is the explicit ordered durability barrier.

## Implemented POC surface

- `GET /health`
- `POST /insert/jsonline`
- `GET /select/logsql/query`
- `POST /select/logsql/query` for the current benchmark's LogsQL shapes
- `GET /select/logsql/stats`
- `GET /api/v1/flush`

Auth, backup, cluster administration, and generic metrics/traces abstractions
are deliberately absent.

## Run

```bash
cargo build -p timeless-ext --release
cargo build --manifest-path poc/timeless-logs-api/Cargo.toml --release

poc/timeless-logs-api/target/release/timeless-logs-api \
  target/release/libtimeless_ext.so \
  /tmp/timeless-logs-api.db \
  127.0.0.1:19429
```

The API uses one SQLite writer and a small pool of SQLite readers. Retryable
extension publication conflicts wait inside the API rather than leaking as
HTTP 500 responses. Health and stats expose admitted/completed work, queue
depth and age, API phase timers, extension flush/query/optimize counters, and
read-permit/writer-wait counters so admission cannot be confused with
completed SQLite ingestion.

The ignored end-to-end contract test pins the storage boundary explicitly:

```bash
TIMELESS_EXT_TEST_PATH=../../target/release/libtimeless_ext.so \
  cargo test --manifest-path poc/timeless-logs-api/Cargo.toml \
  --test api_e2e -- --ignored
```

It proves that a 100-entry HTTP request remains buffered with zero raw blocks,
and that reaching exactly 8,192 entries triggers the extension's own four
level-partitioned raw blocks with zero compressed blocks. No API flush occurs
between those requests.

## Current POC result

The deterministic Session 1 baseline reaches 478.7K completed entries/s with
no queries. With one and two query workers, it saturates at 162.3K and 85.5K
completed entries/s respectively while the unchanged Elixir API reaches
489.5K and 465.5K. Extension telemetry locates the difference: mixed queries
held read permits for 7.53–10.31 aggregate seconds while writers waited
7.06–7.56 seconds.

Session 2 writer fairness raises completed ingestion at equal offered load
from 162.3K to 225.5K entries/s with one query worker and from 85.5K to
152.0K with two. New readers retry while a writer is queued, so they cannot
starve it; the logs cursor also releases its permit before metadata JSON
rendering. Both measured runs had zero HTTP errors and drained to zero.

Session 3 then moves payload decoding, filtering, sorting, and JSON rendering
past the publication boundary. SQLite's read snapshot keeps captured block
locations readable while the extension streams one payload at a time, so this
does not retain every candidate payload in memory. With one and two query
workers the API reaches 479.7K and 463.3K completed entries/s respectively.

Session 4 pushes exact `ORDER BY ts ASC|DESC LIMIT/OFFSET` windows through the
virtual-table planner into a bounded engine query. The engine retains at most
`LIMIT + OFFSET` entries and stops on block timestamp bounds. An isolated
latest-100 over 3.109M raw entries returned 100 engine rows in 77.91ms and
skipped 1,424 of 1,492 candidate blocks. Strict bounds and message LIKE remain
on the conservative unbounded path until exact filtering moves into the
extension; native count is likewise Session 5 work.

The POC still uses the unchanged storage mechanism. No alternate buffer size,
block layout, partition scheme, or compaction policy was introduced to hide
the result. The next measured change is native exact message filtering and
counts. Those two unbounded shapes still dominate peak memory in the mixed
workload even though limited row queries are now bounded.

The measured follow-up work is organized in
[`LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md`](../../LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md).
The pinned Session 1 results are in
[`2026-08-01_rust_logs_api_session1.md`](../../../timeless_logs/bench/results/2026-08-01_rust_logs_api_session1.md).
