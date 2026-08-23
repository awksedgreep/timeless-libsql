# timeless-traces-api release server

This first-class traces-specific release server owns HTTP scheduling and
SQLite connections but does not implement storage. Every span and command
crosses the public `timeless_traces` virtual table supplied by
`libtimeless_ext`.

The canonical binary, route, configuration, authentication, lifecycle,
backup, and error contract is the
[Rust signal server API reference](../../../docs/SERVER_API_REFERENCE.md).

The release binary requires policy authentication by default. An external
control plane may own cluster, user, session, and token administration.

The current binary provides OTLP ingest, the pinned Jaeger read surface, a
reusable bounded/streaming extension read path, and a narrow lossless
historical-query surface for TimelessTracesDashboard.

## Run

```sh
cargo build -p timeless-ext
timeless_traces_dir="$(mktemp -d)"
trap 'rm -rf -- "$timeless_traces_dir"' EXIT
cargo run --manifest-path servers/Cargo.toml -p timeless-traces-api -- \
  target/debug/libtimeless_ext.so "$timeless_traces_dir/traces.db"
```

Authentication is off by default. To harden a deployment, opt in with
`TIMELESS_AUTH_MODE=required`, `TIMELESS_AUTH_POLICY_FILE`, and
`TIMELESS_TENANT`; non-loopback binding additionally requires
`TIMELESS_ALLOW_NON_LOOPBACK=1` in a separately secured deployment.

The default listener is loopback-only at `127.0.0.1:19449`. Configuration:

| variable | default | meaning |
|---|---:|---|
| `TIMELESS_TRACES_READER_CONNECTIONS` | `2` | measured bounded SQLite reader default |
| `TIMELESS_TRACES_COMMAND_QUEUE_BATCHES` | `256` | maximum queued writer requests |
| `TIMELESS_TRACES_RETENTION_SECS` | absent/inherit | preserve the stored vtab policy; positive overrides it; `0` disables it |
| `TIMELESS_TRACES_FLUSH_INTERVAL_SECS` | `1` | ordered extension flush interval |
| `TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS` | `30` | ordered, byte-budgeted extension optimize interval |

## Implemented endpoints

- `GET /live` reports process liveness without touching SQLite.
- `GET /ready` and `GET /health` verify a live reader and expose the negotiated
  `timeless_traces/rich-span-batch-v2` capability.
- `GET /select/traces/stats` reports extension, SQLite-file, connection, and
  exact request/span/body queue watermarks.
- `GET|POST /api/v1/flush` is an ordered completion and durability barrier. Its
  response identifies the admitted request watermark covered by the flush.
- `POST /api/v1/backup` flushes, drains actionable optimize backlog,
  checkpoints the WAL, and publishes a verified no-overwrite SQLite backup.
- `POST /insert/opentelemetry/v1/traces` accepts OTLP JSON, protobuf, and
  gzip-compressed protobuf. It validates the complete request, encodes one
  public rich-span v2 batch without dropping links, trace state/flags,
  dropped-value counters, schema URLs, or resource/scope metadata, waits for
  its one SQLite statement, and returns
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
- `GET /select/timeless/api/spans` implements the dashboard's exact historical
  span search, pagination, and ordering contract. `name` retains the current
  case-insensitive name/string-attribute match; `service`, `kind`, `status`,
  `since`, and `until` are exact. Responses include every stored native field.
- `GET /select/timeless/api/traces/:trace_id` returns a deterministically
  ordered native trace with typed attributes, status description, events,
  per-span resources, and instrumentation scope intact. These two routes are
  traces-specific; they do not change or masquerade as the Jaeger contract.
- `GET|POST /select/timeless/api/spans/tail` streams admitted spans as
  NDJSON — the streaming twin of the span search. Filters reuse the search
  surface's live-matchable parameters (`service`, `name`, `kind`, `status`)
  plus an `attributes` JSON object of exact scalar matches; invalid values
  are rejected rather than ignored. Spans publish only after storage durably
  accepts the batch, and slow consumers drop spans (counted in stats) rather
  than backpressuring ingest.

The public SQL waist remains useful outside this daemon. Unbounded
`timeless_traces` cursors stream one decoded block at a time; exact
`ORDER BY start_ts[,span_id] ASC|DESC LIMIT/OFFSET` queries retain only their
bounded prefix; inclusive duration predicates filter inside the engine; and
metadata-native discovery is available directly:

```sql
SELECT value FROM timeless_trace_services('traces');
SELECT value FROM timeless_trace_operations('traces', 'checkout');
```

The service/operation catalog is additive block metadata. Mixed databases
containing legacy blocks fall back to exact streaming decode, so upgrading
never silently omits an operation. `timeless_stats('traces')` exposes snapshot,
bounded-query, discovery, payload, decode, match, return, and cancellation work
counters. The extension also exposes size-tiered optimize backlog and
raw-compression/merge phase counters. The timer derives a bounded span budget
from that exact backlog and a 32 MiB source-byte target, then invokes the
public `optimize:<spans>` command; it never creates or reshapes blocks itself.

Startup acquires `<database>.timeless-traces-api.lock` before opening SQLite.
It then validates the full rich-span schema, module identity, configured
retention, and public batch version `0x03` before binding the listener. A
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
TIMELESS_EXT_PATH="$PWD/target/debug/libtimeless_ext.so" \
  cargo test --manifest-path servers/Cargo.toml -p timeless-traces-api
cargo clippy --manifest-path servers/Cargo.toml --all-targets -- -D warnings
```

The extension-backed contracts cover owner fencing, capability and retention
mismatch, body rejection before admission, a physically saturated one-request
writer queue, exact drain watermarks, explicit flush, rich-field cold reopen,
WAL checkpoint, graceful process termination, kill-9 recovery, JSON/protobuf/
gzip parity, atomic malformed-request rejection, decompression bounds, and the
8,191-to-8,192 automatic-flush boundary through HTTP. They also cover the
baseline Jaeger oracle, rich tags/logs/processes/references, span-limit-before-
grouping behavior, traces split across multiple extension blocks, scoped
SQLite interruption, cancellation during extension work, progress-handler
cleanup, reuse of the same reader after cancellation, bounded-order planner
selection, duration pushdown, native discovery, streaming snapshot memory, and
writer publication while broad-query decode CPU is still active.
