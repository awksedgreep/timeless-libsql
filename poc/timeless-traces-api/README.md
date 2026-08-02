# timeless-traces-api POC

This traces-specific process-boundary POC owns HTTP scheduling and SQLite
connections. It does not implement storage. Every span and command crosses the
public `timeless_traces` virtual table supplied by `libtimeless_ext`.

Sessions 2–4 provide the server lifecycle shell, OTLP ingest, and the pinned
Jaeger read surface.

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

## Sessions 2–4 endpoints

- `GET /live` reports process liveness without touching SQLite.
- `GET /ready` and `GET /health` verify a live reader and expose the negotiated
  `timeless_traces/rich-span-batch-v1` capability.
- `GET /select/traces/stats` reports extension, SQLite-file, connection, and
  exact request/span/body queue watermarks.
- `POST /api/v1/flush` is an ordered completion and durability barrier. Its
  response identifies the admitted request watermark covered by the flush.
- `POST /insert/opentelemetry/v1/traces` accepts OTLP JSON, protobuf, and
  gzip-compressed protobuf. It validates the complete request, encodes one
  public rich-span v1 batch, waits for its one SQLite statement, and returns
  the established `{"partialSuccess":{}}` response. Raw and decompressed
  bodies are independently capped at 10 MiB.
- `GET /select/jaeger/api/services` and
  `GET /select/jaeger/api/services/:service/operations` provide sorted
  discovery.
- `GET /select/jaeger/api/traces/:trace_id` uses the extension's packed trace
  index and assembles every span across block boundaries in deterministic
  start order.
- `GET /select/jaeger/api/traces` supports the established `service`,
  `operation`, `start`, `end`, `limit`, `minDuration`, and `maxDuration`
  parameters. Compatibility is explicit: `limit` still counts spans before
  grouping, not traces, so a search result may be incomplete.

Startup acquires `<database>.timeless-traces-api.lock` before opening SQLite.
It then validates the full rich-span schema, module identity, configured
retention, and public batch version `0x02` before binding the listener. A
second owner or incompatible extension fails startup with a descriptive error.

One writer consumes a bounded FIFO. A request is admitted only after queue
capacity is reserved, and request/span/body watermarks change atomically.
There is no host span buffer: one OTLP request maps to one public rich batch
insertion, while the extension retains its fixed 8,192-span automatic flush
and all block/index/compression behavior. A successful response covers SQLite
statement completion; explicit flush additionally covers extension-buffer
durability.

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

The extension-backed contracts cover owner fencing, capability and retention
mismatch, body rejection before admission, a physically saturated one-request
writer queue, exact drain watermarks, explicit flush, rich-field cold reopen,
WAL checkpoint, graceful process termination, kill-9 recovery, JSON/protobuf/
gzip parity, atomic malformed-request rejection, decompression bounds, and the
8,191-to-8,192 automatic-flush boundary through HTTP. They also cover the
Session 0 Jaeger oracle, rich tags/logs/processes/references, span-limit-before-
grouping behavior, traces split across multiple extension blocks, scoped
SQLite interruption, cancellation during extension work, progress-handler
cleanup, and reuse of the same reader after cancellation.
