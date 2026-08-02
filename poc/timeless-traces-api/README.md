# timeless-traces-api POC

This traces-specific process-boundary POC owns HTTP scheduling and SQLite
connections. It does not implement storage. Every span and command crosses the
public `timeless_traces` virtual table supplied by `libtimeless_ext`.

Session 2 provides the server lifecycle shell only. OTLP ingest and Jaeger
query routes intentionally remain unimplemented until Sessions 3 and 4.

## Run

```sh
cargo build -p timeless-ext
cd poc/timeless-traces-api
cargo run -- ../../target/debug/libtimeless_ext.so /tmp/traces.db
```

The default listener is loopback-only at `127.0.0.1:19449`. Configuration:

| variable | default | meaning |
|---|---:|---|
| `TIMELESS_TRACES_READER_CONNECTIONS` | `2` | bounded SQLite readers; provisional until Session 7 |
| `TIMELESS_TRACES_COMMAND_QUEUE_BATCHES` | `256` | maximum queued writer requests |
| `TIMELESS_TRACES_RETENTION_SECS` | `604800` | vtab retention; `0` disables it |
| `TIMELESS_TRACES_FLUSH_INTERVAL_SECS` | `1` | ordered extension flush interval |
| `TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS` | `30` | ordered extension optimize interval |

## Session 2 endpoints

- `GET /live` reports process liveness without touching SQLite.
- `GET /ready` and `GET /health` verify a live reader and expose the negotiated
  `timeless_traces/rich-span-batch-v1` capability.
- `GET /select/traces/stats` reports extension, SQLite-file, connection, and
  exact request/span/body queue watermarks.
- `POST /api/v1/flush` is an ordered completion and durability barrier. Its
  response identifies the admitted request watermark covered by the flush.
- `POST /insert/opentelemetry/v1/traces` is reserved but returns `501` in this
  session. Bodies over 10 MiB are rejected with `413` before the handler and
  cannot alter admission counters.

Startup acquires `<database>.timeless-traces-api.lock` before opening SQLite.
It then validates the full rich-span schema, module identity, configured
retention, and public batch version `0x02` before binding the listener. A
second owner or incompatible extension fails startup with a descriptive error.

One writer consumes a bounded FIFO. A request is admitted only after queue
capacity is reserved, and request/span/body watermarks change atomically.
There is no host span buffer: one future OTLP request maps to one public rich
batch insertion, while the extension retains its fixed 8,192-span automatic
flush and all block/index/compression behavior.

`SIGINT` and `SIGTERM` stop HTTP admission, drain accepted requests, issue the
public `flush` command, checkpoint the WAL, close workers, and release the
owner lease. `SIGKILL` cannot run cleanup: already flushed SQLite transactions
remain recoverable, while an admitted extension-buffer tail is deliberately
not claimed durable.

## Verify

```sh
cargo build -p timeless-ext
cd poc/timeless-traces-api
TIMELESS_EXT_PATH=../../target/debug/libtimeless_ext.so cargo test
cargo clippy --all-targets -- -D warnings
```

The extension-backed contract covers owner fencing, capability and retention
mismatch, body rejection before admission, a physically saturated one-request
writer queue, exact drain watermarks, explicit flush, rich-field cold reopen,
WAL checkpoint, graceful process termination, and kill-9 recovery.
