# timeless-metrics-api

`timeless-metrics-api` is the standalone Rust metrics data-plane server for
`timeless-libsql`. It owns HTTP parsing, Prometheus scraping, bounded queues
and readers, PromQL/MetricsQL evaluation, maintenance scheduling, and process
lifecycle. The `timeless_metrics` extension remains the only storage owner.

Every write, query, flush, compact, rollup, retention, and statistics operation
uses a public extension SQL or batch surface. The server does not read private
shadow tables and has no Elixir, BEAM, NIF, Rocket, or alternate-storage
fallback.

The canonical route, environment, authentication, limit, response, backup,
and shutdown contract is the
[Rust signal server API reference](../../../docs/SERVER_API_REFERENCE.md).
The exact public storage contract is in the
[SQLite extension API reference](../../../docs/SQL_API_REFERENCE.md).

## Storage and lifecycle

- Fresh databases use the `metric_samples` `timeless_metrics` virtual table;
  compatible existing public metrics tables remain readable.
- The extension owns durable series identity, its 4,096-point per-series
  threshold, compression, chunks, rollups, retention, and transactions.
- Native VictoriaMetrics JSON-line and Prometheus exposition imports are
  encoded through the extension's public ingestion contracts.
- The process-owned Prometheus scrape loop parses targets in Rust and feeds
  the same bounded writer path. Elixir is not in the scrape or ingest path.
- A successful ingest response follows the route-specific admission/completion
  contract. `POST /api/v1/flush` is the ordered durability barrier.
- SIGINT/SIGTERM stops admission, drains accepted work, flushes, checkpoints
  WAL, closes workers, and releases the exclusive owner lease.

## Storage observability

`GET /metrics` (Prometheus exposition) and `GET /select/metrics/stats` (JSON)
publish the honest storage split:

- `timeless_metrics_storage_bytes` (`bytes_on_disk` in JSON) is chunk payload
  bytes on disk — the only stored side of a compression ratio.
- `timeless_metrics_raw_ingested_bytes` (`raw_ingested_bytes`) is the raw
  comparator at the standard 16 bytes per sample (8-byte timestamp + 8-byte
  value; series identity is the amortized catalog, not the row). It derives
  from durable point counts — disk plus buffered — so it stays
  lifetime-accurate across restarts.
- `timeless_metrics_index_bytes` (`sqlite_index_bytes`) is SQLite index
  bytes, reported beside a compression ratio, never inside it.
- `timeless_metrics_database_file_bytes`, `timeless_metrics_wal_bytes`, and
  `timeless_metrics_freelist_bytes` are operational series and are never part
  of a compression number.

## HTTP surface

The server provides:

- liveness, readiness, health, storage/queue statistics, flush, and verified
  no-overwrite backup;
- VictoriaMetrics JSON-line import/export;
- Prometheus exposition import and process-local scrape-target management;
- native exact-latest, range, label, value, and series discovery;
- Prometheus-compatible instant/range and discovery routes; and
- separately named MetricsQL instant/range routes.

See the [server API reference](../../../docs/SERVER_API_REFERENCE.md#complete-route-inventory)
for exact methods, paths, scopes, parameters, and envelopes.

## Query compatibility

The stable PromQL implementation is float-series native Rust evaluation over
bounded public extension reads. MetricsQL-only behavior is exposed only on the
explicit MetricsQL routes and never changes stable PromQL semantics.

The [PromQL feature matrix](../../../docs/PROMQL_FEATURE_MATRIX.md) is the
compatibility authority. It records each selector, operator, function,
aggregation, modifier, histogram behavior, experimental gate, and deferred
data-model prerequisite. Unsupported syntax fails explicitly before storage;
it is never silently ignored or delegated.

At the `0.7.x` source line, the matrix dispositions are:

| tier | shipped | experimental | deferred |
|---|---:|---:|---:|
| PromQL | 74 | 11 | 5 |
| MetricsQL | 10 | 0 | 2 |

Native histograms and complete stale-marker behavior remain deferred until
the extension has the required typed/versioned storage and ingress model.
Classic histogram series remain ordinary float series.

The following marker is checked against every shipped API-owned matrix row:

<!-- query-contract-shipped: PQL-S01 PQL-S02 PQL-S03 PQL-S04 PQL-S05 PQL-S06 PQL-S07 PQL-S08 PQL-S09 PQL-S11 PQL-S12 PQL-S13 PQL-S14 PQL-S15 PQL-S16 PQL-S18 PQL-S19 PQL-S20 PQL-S21 PQL-S23 PQL-O01 PQL-O02 PQL-O03 PQL-O04 PQL-O05 PQL-O06 PQL-O07 PQL-O08 PQL-O09 PQL-O10 PQL-O11 PQL-O12 PQL-O13 PQL-O14 PQL-O15 PQL-O16 PQL-R01 PQL-R02 PQL-R03 PQL-R04 PQL-R05 PQL-R06 PQL-R08 PQL-R09 PQL-R10 PQL-R11 PQL-R12 PQL-R13 PQL-R14 PQL-R15 PQL-R16 PQL-R17 PQL-R18 PQL-R19 PQL-R20 PQL-F01 PQL-F02 PQL-F03 PQL-F04 PQL-F05 PQL-F06 PQL-F07 PQL-F08 PQL-F09 PQL-F10 PQL-F11 PQL-F12 PQL-F13 PQL-F15 PQL-F16 PQL-F17 PQL-F18 PQL-H01 PQL-H02 MQL-01 MQL-02 MQL-03 MQL-04 MQL-05 MQL-06 MQL-07 MQL-09 MQL-10 MQL-12 -->

## Run locally

```sh
cargo build --release -p timeless-ext --locked
cargo build --release --manifest-path servers/Cargo.toml \
  -p timeless-metrics-api --locked

timeless_metrics_dir="$(mktemp -d)"
trap 'rm -rf -- "$timeless_metrics_dir"' EXIT
\
  servers/target/release/timeless-metrics-api \
  target/release/libtimeless_ext.so \
  "$timeless_metrics_dir/metrics.db" \
  127.0.0.1:19439
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
    -p timeless-metrics-api --locked -- --include-ignored
```

The complete ordering, query-contract, oracle, performance, cancellation,
fault, soak, and packaging gates are in [TESTING.md](../../../TESTING.md).
