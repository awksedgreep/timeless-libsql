# timeless-logs-api POC

This is an API-boundary proof of concept, not a replacement storage engine.

The storage contract is fixed:

- NDJSON requests are parsed into the existing logs batch-blob v0 format.
- `INSERT INTO logs(logs) VALUES (?1)` feeds the existing extension buffer.
- The extension's hard-coded 8,192-entry automatic flush is unchanged.
- The API never flushes at a request or producer-batch boundary.
- A one-second low-volume timer sends the existing `flush` command.
- A 30-second maintenance timer sends the existing `optimize` command only
  when raw blocks exist.
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
HTTP 500 responses. Stats expose `queued_batches` and `queued_entries` so
admission throughput cannot be confused with completed SQLite ingestion.

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

The initial mixed API workload is useful boundary evidence, not a final engine
benchmark. With four writers, 500-entry requests, and two concurrent query
workers, the current Rust POC sustains 76.3K admitted entries/s with 1.60ms
write p99. At the next ramp step it admits 124.0K entries/s but crosses the
100ms p99 limit as the bounded queue fills behind long reads. The untouched
Elixir API reaches 456.5K entries/s on the same workload configuration.

The important finding is now correctly located: concurrent extension reads
hold the publication boundary long enough to limit writer drain. No alternate
buffer size or block-store policy was introduced to hide that result.

The measured follow-up work is organized in
[`LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md`](../../LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md).
