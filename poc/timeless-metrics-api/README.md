# timeless-metrics-api POC

This is the metrics API process-boundary proof of concept. It is not a new
storage engine. The binary owns HTTP scheduling and SQLite connections while
the existing `timeless_metrics` extension continues to own series identity,
the 4,096-point per-series buffer threshold, compression, chunks, rollups, and
retention commands.

## Session 1 surface

- `GET /health`
- `GET /select/metrics/stats`
- `POST /api/v1/flush`

There is deliberately no ingest, query, auth, cluster, or product route yet.
The contract test submits public named-batch `0x01` blobs through an internal
seam only; Session 2 will put the established Prometheus and VictoriaMetrics
HTTP contracts in front of that seam.

The process starts one ordered SQLite writer and a configurable reader pool.
It creates the same rollup ladder as `TimelessMetrics.LibsqlEngine`, schedules
the same 10-second flush, five-minute compact/rollup, and hourly seven-day raw
retention prune, and sends only public extension commands. Graceful shutdown
places an ordered `flush` behind all admitted writes.

`POST /api/v1/flush` reports the admitted batch and point watermark covered by
the command together with completed, failed, queued, and in-flight work. It is
a real completion/durability barrier, not queue admission.

## Run

```bash
cargo build -p timeless-ext --release
cargo build --manifest-path poc/timeless-metrics-api/Cargo.toml --release

poc/timeless-metrics-api/target/release/timeless-metrics-api \
  target/release/libtimeless_ext.so \
  /tmp/timeless-metrics-api.db \
  127.0.0.1:19439
```

The provisional default is two readers. This is a correctness default, not a
copied performance conclusion from logs; Session 6 will sweep 1/2/4/8.

Positive environment overrides:

- `TIMELESS_METRICS_READER_CONNECTIONS` (default `2`)
- `TIMELESS_METRICS_COMMAND_QUEUE_BATCHES` (default `256`)
- `TIMELESS_METRICS_FLUSH_INTERVAL_SECS` (default `10`)
- `TIMELESS_METRICS_COMPACT_INTERVAL_SECS` (default `300`)
- `TIMELESS_METRICS_RETENTION_INTERVAL_SECS` (default `3600`)

## Ownership and accounting

The server takes an advisory exclusive lock on
`<database>.timeless-metrics-api.lock`. A second server using this contract is
rejected before opening SQLite. The lease cannot stop an unrelated process
that ignores it, so deployments must still route all runtime access through
the API owner.

Stats separate units instead of conflating them:

- series, raw-chunk, and rollup index entries are row counts;
- `sqlite_index_bytes` is SQLite page allocation for the catalog/chunk indexes;
- logical compressed payload, database/WAL/SHM bytes, SQLite page high-water,
  and freelist bytes are distinct fields;
- admitted/completed/failed batches and points, queued and in-flight work, and
  oldest queue age are distinct;
- API admission/queue/SQLite/stats/flush timers and maintenance timers are
  cumulative nanoseconds. Parse and batch-encode timers remain zero until
  Session 2 adds ingest.

## Validation

```bash
cargo test --manifest-path poc/timeless-metrics-api/Cargo.toml

TIMELESS_EXT_PATH=target/release/libtimeless_ext.so \
  cargo test --manifest-path poc/timeless-metrics-api/Cargo.toml \
  --test storage_contract -- --ignored
```

The extension-backed test proves 4,095 points remain buffered, point 4,096
automatically becomes one durable raw chunk, an explicit flush persists a
smaller tail, the ordered counters drain to zero, a second owner is rejected,
and all 4,106 points recover after shutdown/reopen. It also proves Session 1
has no accidental ingest route.

That test exposed and fixed an existing extension gap: the engine queued a
series at 4,096 points but the metrics virtual table never drained its pending
queue. Every metrics ingest surface now calls the shared pending-flush path,
so direct SQLite/libSQL users receive the advertised behavior. Below threshold
the empty-queue path performs no store write. `tests/correctness.sh r1` pins
the direct-SQL threshold and transaction regressions.

## Shell smoke result

On the Session 0 host, an empty release server with two readers handled the
three sequential control-route loops below with zero errors:

| route | requests | sequential req/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| `GET /health` | 2,000 | 2,047.2 | 471.2us | 788.8us | 914.1us |
| `GET /select/metrics/stats` | 2,000 | 2,007.9 | 490.5us | 790.2us | 914.6us |
| `POST /api/v1/flush` | 200 | 4,131.6 | 214.4us | 453.2us | 560.3us |

Linux `VmHWM` was 9,176 KiB (`VmRSS` 9,176 KiB after the run). These are shell
sanity numbers, not an ingest comparison with Session 0: Session 1 intentionally
has no HTTP ingest path. Reproduce them with `python3 bench_shell.py` while the
server is running. Completed-points throughput begins in Session 2.
