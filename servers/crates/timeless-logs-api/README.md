# timeless-logs-api

`timeless-logs-api` is the standalone Rust log ingest and LogsQL data-plane
server for `timeless-libsql`. It owns HTTP parsing, bounded queues/readers,
strict LogsQL parsing and evaluation, maintenance scheduling, and process
lifecycle. The `timeless_logs` extension remains the only storage owner.

Every write, query, flush, optimize, retention, and statistics operation uses
a public extension SQL or rich-batch surface. The server does not read private
shadow tables and has no Elixir, BEAM, NIF, Rocket, or alternate-storage
fallback.

The canonical route, environment, authentication, limit, response, backup,
and shutdown contract is the
[Rust signal server API reference](../../../docs/SERVER_API_REFERENCE.md).
The exact public storage contract is in the
[SQLite extension API reference](../../../docs/SQL_API_REFERENCE.md).

## Storage and lifecycle

- NDJSON requests are parsed into the public rich-log batch format.
- Epoch-microsecond timestamps, all eight severities, and canonical typed and
  nested JSON metadata survive flush, optimize, backup, and reopen.
- The extension owns the authoritative 8,192-entry buffer. The API does not
  flush or reshape storage at an HTTP-request boundary.
- `204` means the parsed request was admitted to the bounded SQLite writer;
  it is not a durability claim. `GET /api/v1/flush` is the ordered durability
  barrier.
- A low-volume timer sends the public `flush` command. A separate maintenance
  timer reads public actionable backlog and invokes bounded
  `optimize:<entries>`; it never builds or rewrites blocks itself.
- SIGINT/SIGTERM stops admission, drains accepted work, flushes, checkpoints
  WAL, closes workers, and releases the exclusive owner lease.

## HTTP surface

The server provides:

- `GET /live`, `/ready`, and `/health`;
- `GET /metrics` for Prometheus self-metrics;
- `POST /insert/jsonline` for rich NDJSON ingestion;
- `GET|POST /select/logsql/query` for native parameters and strict LogsQL;
- `GET /select/logsql/field_values` and `/select/logsql/stats`;
- `GET /api/v1/flush`; and
- `POST /api/v1/backup` for a verified no-overwrite SQLite backup.

See the [server API reference](../../../docs/SERVER_API_REFERENCE.md#complete-route-inventory)
for exact methods, scopes, parameters, and envelopes.

## Storage and compression series

`GET /metrics` and `GET /select/logsql/stats` publish the storage split as
separate series so a compression ratio is never blended with operational
bytes:

- `timeless_logs_storage_bytes` — data block payload bytes on disk, the only
  stored side of a compression ratio (JSON `total_bytes`);
- `timeless_logs_index_bytes` — term index bytes on disk, reported beside
  compression series, never inside them (JSON `index_size`; also exported as
  the pre-existing `timeless_logs_index_size_bytes`);
- `timeless_logs_wal_bytes`, `timeless_logs_freelist_bytes`, and
  `timeless_logs_database_file_bytes` — operational gauges, never part of a
  compression number;
- `timeless_logs_compression_input_bytes_total` and
  `timeless_logs_compression_output_bytes_total` — lifetime byte counters
  persisted inside the database (they survive restarts); the input side
  counts each entry's block payload once at first-pass compression, and the
  output side is net of optimize merge savings;
- `timeless_logs_raw_ingested_bytes_total` — interim raw-side counter derived
  from the compression input total (JSON `raw_ingested_bytes_total`); it
  excludes optimize/merge recompression and does not yet include buffered or
  still-raw entries.

## LogsQL compatibility

LogsQL parsing and pipeline composition live in this Rust API. Storage-aware
filters are pushed through public `timeless_logs` inputs when safe; remaining
typed filters, transformations, statistics, sorting, pagination, joins, and
pipelines execute over bounded public rows. Invalid or unsupported syntax
fails explicitly before storage rather than being ignored.

The [LogsQL feature matrix](../../../docs/LOGSQL_FEATURE_MATRIX.md) is the
compatibility authority. At the `0.7.x` source line, 107 rows are shipped and
7 are deferred behind named storage/topology prerequisites. Important
deferrals include tenant-scoped VictoriaLogs stream identity, same-stream
context, physical VictoriaLogs block diagnostics, intra-query parallelism,
and distributed partial results. An application metadata field is never
silently reinterpreted as one of those models.

The following marker is checked against every shipped API-owned matrix row:

<!-- query-contract-shipped: LQL-F01 LQL-F02 LQL-F03 LQL-F04 LQL-F05 LQL-F06 LQL-F07 LQL-F08 LQL-F09 LQL-F10 LQL-F11 LQL-F12 LQL-F13 LQL-F14 LQL-F15 LQL-F16 LQL-F17 LQL-F18 LQL-F19 LQL-F20 LQL-F21 LQL-F22 LQL-F23 LQL-F24 LQL-F25 LQL-F26 LQL-F27 LQL-F28 LQL-F29 LQL-F30 LQL-F31 LQL-F32 LQL-F33 LQL-F34 LQL-F37 LQL-F38 LQL-F39 LQL-F40 LQL-F41 LQL-P01 LQL-P02 LQL-P03 LQL-P04 LQL-P05 LQL-P06 LQL-P07 LQL-P08 LQL-P09 LQL-P12 LQL-P13 LQL-P14 LQL-P15 LQL-P16 LQL-P17 LQL-P18 LQL-P19 LQL-P20 LQL-P21 LQL-P22 LQL-P23 LQL-P24 LQL-P25 LQL-P26 LQL-P27 LQL-P28 LQL-P29 LQL-P30 LQL-P31 LQL-P32 LQL-P33 LQL-P34 LQL-P35 LQL-P36 LQL-P37 LQL-P38 LQL-P39 LQL-P40 LQL-P41 LQL-P42 LQL-P43 LQL-P44 LQL-P45 LQL-P46 LQL-P47 LQL-P48 LQL-P49 LQL-Q01 LQL-Q02 LQL-Q04 LQL-Q05 LQL-Q07 LQL-Q08 LQL-S01 LQL-S02 LQL-S03 LQL-S04 LQL-S05 LQL-S06 LQL-S07 LQL-S08 LQL-S09 LQL-S10 LQL-S11 LQL-S12 LQL-S13 LQL-S14 LQL-S15 -->

## Run locally

```sh
cargo build --release -p timeless-ext --locked
cargo build --release --manifest-path servers/Cargo.toml \
  -p timeless-logs-api --locked

timeless_logs_dir="$(mktemp -d)"
trap 'rm -rf -- "$timeless_logs_dir"' EXIT
\
  servers/target/release/timeless-logs-api \
  target/release/libtimeless_ext.so \
  "$timeless_logs_dir/logs.db" \
  127.0.0.1:19429
```

Use `.dylib` on macOS. Authentication is off by default. To harden a deployment, opt in with
`TIMELESS_AUTH_MODE=required`, `TIMELESS_AUTH_POLICY_FILE`, and
`TIMELESS_TENANT`; non-loopback binding additionally requires
`TIMELESS_ALLOW_NON_LOOPBACK=1` in a separately secured deployment.

## Verify

Run from the repository root after building the current release extension:

```sh
TIMELESS_EXT_PATH="$PWD/target/release/libtimeless_ext.so" \
TIMELESS_EXT_TEST_PATH="$PWD/target/release/libtimeless_ext.so" \
  cargo test --manifest-path servers/Cargo.toml \
    -p timeless-logs-api --locked -- --include-ignored
```

The complete ordering, query-contract, oracle, performance, cancellation,
fault, soak, and packaging gates are in [TESTING.md](../../../TESTING.md).
