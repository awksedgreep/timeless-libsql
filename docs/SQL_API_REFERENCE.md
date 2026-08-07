# SQLite extension API reference

This is the canonical reference for the SQL surface exported by
`timeless-libsql` 0.4.x. It covers both the telemetry loadable artifact
(`libtimeless_ext`) and the separate health artifact (`libdbhealth_ext`). The
[query-language matrices](QUERY_FEATURES.md) describe PromQL, MetricsQL, and
LogsQL behavior in the Rust signal APIs; they do not change the SQL contracts
defined here. Binary launch, HTTP routes, authentication, runtime limits,
shutdown, and coordinated backup are in the
[Rust signal server API reference](SERVER_API_REFERENCE.md).
Artifact/database pairing rules are in the
[compatibility statement](COMPATIBILITY.md), and replacement procedures are
in the [upgrade and rollback guide](UPGRADE.md).

All implementation-owned shadow tables are private. Applications must use
the virtual tables, table-valued functions, scalar, commands, and batch
formats in this reference. A name such as `metrics_chunks`, `logs_blocks`, or
`traces_trace_blocks` is an implementation detail even though SQLite stores
it in the same database file.

## Loading and embedding

Build and load the telemetry artifact:

```sh
cargo build --release -p timeless-ext
sqlite3 telemetry.db ".load ./target/release/libtimeless_ext.so"
```

Linux uses `.so`; macOS uses `.dylib`. The artifact exports
`sqlite3_extension_init`, `sqlite3_timelessext_init`, and
`sqlite3_timeless_ext_init`. A Rust host can instead link the
`timeless-ext` crate and call `timeless_ext::register_telemetry(&connection)`.
That embedding call installs the production telemetry modules and capability
scalar, but not the compatibility-only `timeless_spike` module or dbhealth.

Build `dbhealth` independently with `cargo build --release -p dbhealth-ext`.
Its artifact registers only `dbhealth` and its `timeless_health` alias. A Rust
host can call `timeless_ext::register_dbhealth(&connection)` explicitly.
Loading both artifacts into one connection is supported; linking both
loadable entry points into one Rust test binary is not, because each artifact
must export SQLite's conventional `sqlite3_extension_init` symbol.

## Capability and version handshake

Probe before creating or opening telemetry tables:

```sql
SELECT timeless_capabilities();
```

The result is deterministic JSON. These fields are contractual:

- `extension_version`: semantic version of the loaded extension.
- `data_abi`: on-disk/public data compatibility generation. It is `1` for
  0.4.x; additive SQL or JSON fields do not change it.
- `sql_surface_version`: generation of the advertised SQL inventory. It is
  `1` for the surface documented here.
- `minimum_server_version`: oldest compatible Timeless Rust signal server.
- `build`: commit, target triple, and profile compiled into the artifact.
- `signals`: storage module, authoritative batching, timestamp units, and
  fidelity declarations for metrics, logs, and traces.
- `query_surfaces`: packed result formats and required work-limit/report
  capabilities.
- `sql_surfaces`: the exact production scalar, storage-module, and query-module
  inventory installed by `register_telemetry`.

Consumers must reject an unsupported `data_abi` or a missing capability they
require. They must tolerate additive object members and array entries. The
compatibility-only `timeless_spike` module is intentionally omitted from
`sql_surfaces`; it is not a production storage contract.

## Registered SQL symbol inventory

The following table is machine-checked against the Rust registration source.

<!-- public-sql-symbols:start -->

| SQL symbol | Kind | Artifact or embedding call | Purpose |
|---|---|---|---|
| `timeless_capabilities` | scalar | telemetry | Machine-readable build, ABI, batching, format, and SQL-surface handshake. |
| `timeless_metrics` | stored virtual-table module | telemetry | Compressed float metric series. |
| `timeless_logs` | stored virtual-table module | telemetry | Compressed rich logs with exact severity and typed metadata. |
| `timeless_traces` | stored virtual-table module | telemetry | Compressed rich spans with trace and term indexes. |
| `timeless_aggregate` | eponymous TVF | telemetry | One scalar aggregate row per non-empty metric series. |
| `timeless_aggregate_frame` | eponymous TVF | telemetry | All scalar aggregate results in one `TAF1` frame. |
| `timeless_grid` | eponymous TVF | telemetry | Last sample on an evaluation grid. |
| `timeless_label_values` | eponymous TVF | telemetry | Distinct metric-label values. |
| `timeless_latest` | eponymous TVF | telemetry | Newest metric point per series. |
| `timeless_latest_frame` | eponymous TVF | telemetry | All newest points in one `TLF1` frame. |
| `timeless_log_buckets` | eponymous TVF | telemetry | Counts grouped into forward time buckets. |
| `timeless_log_count` | eponymous TVF | telemetry | Exact bounded scalar log count. |
| `timeless_log_query_stats` | eponymous TVF | telemetry | Single-use request-local report for the preceding log scan. |
| `timeless_log_values` | eponymous TVF | telemetry | Bounded distinct log-field discovery. |
| `timeless_raw` | eponymous TVF | telemetry | Matcher-aware metric rows for a bounded range. |
| `timeless_raw_batches` | eponymous TVF | telemetry | One legacy packed point blob per metric series. |
| `timeless_raw_frame` | eponymous TVF | telemetry | A complete wide metric result in one `TRF1` frame. |
| `timeless_rollup` | eponymous TVF | telemetry | One stored metric-rollup aggregate at a selected tier. |
| `timeless_rollup_batches` | eponymous TVF | telemetry | All stored rollup fields in one `TRB1` blob per series. |
| `timeless_series` | eponymous TVF | telemetry | Durable metric-series catalog and ranges. |
| `timeless_stats` | eponymous TVF | telemetry | Extension, storage, maintenance, and query counters. |
| `timeless_trace_buckets` | eponymous TVF | telemetry | Exact span/error/duration bucket statistics. |
| `timeless_trace_operations` | eponymous TVF | telemetry | Distinct trace operation names, optionally by service. |
| `timeless_trace_services` | eponymous TVF | telemetry | Distinct trace service names. |
| `timeless_window` | eponymous TVF | telemetry | Per-series metric window reductions on a grid. |
| `timeless_window_batches` | eponymous TVF | telemetry | Per-series window results in `TWB1` frames. |
| `timeless_spike` | compatibility/reference vtab | telemetry loadable artifact only | Compiling virtual-table reference; not installed by `register_telemetry` and not a production telemetry surface. |
| `dbhealth` | stored virtual-table module | dbhealth | Database-health metrics and companion views. |
| `timeless_health` | stored virtual-table alias | dbhealth | Alias for `dbhealth`, retained for existing databases. |

<!-- public-sql-symbols:end -->

## Shared SQL conventions

Eponymous TVFs may be called positionally, as in
`timeless_raw('metrics', 'cpu', NULL, :start, :stop)`, or through equality
constraints on their hidden inputs. A required hidden input must be bound
directly on every virtual-table scan. Do not rely on SQLite propagating a
value from a joined CTE into a hidden virtual-table input.

`tbl` accepts `table` in `main` or `schema.table` for an attached database.
Metric `filter` inputs are JSON objects. A string value means equality; matcher
objects are `{"neq":"v"}`, `{"re":"pattern"}`, or
`{"nre":"pattern"}`. Regular expressions use Rust's RE2-family engine,
are fully anchored, and treat an absent label as the empty string.

Metric raw, aggregate, latest, rollup, and catalog bounds are inclusive unless
the individual entry says otherwise. Grid lookback and window reduction
ranges are `(T-width,T]`. Logs use the table's persisted millisecond or
microsecond unit. Metrics use epoch seconds. Traces use epoch nanoseconds.
All integer parsing and size arithmetic is checked; malformed, overflowing,
or unknown values fail rather than wrapping or being ignored.

## Stored telemetry virtual tables

### `timeless_metrics`

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics(
  retention='14d',
  rollups='5m@90d,1h@0'
);
```

Columns are `name TEXT`, `ts INTEGER`, `value REAL`, `labels TEXT`, hidden
`series_id INTEGER`, and the hidden command column named after the created
table. `ts` is epoch seconds. `labels` is a canonical flat JSON object of
string values; omitted labels become `{}`. Float values retain all IEEE-754
bits through binary ingestion and packed queries. Ordinary SQLite REAL
projection follows SQLite's NaN behavior.

Creation arguments:

- `retention=<n>[s|m|h|d]`: raw data-time window; a bare integer is seconds.
- `rollups=RESOLUTION@RETENTION,...`: ascending, divisible resolution ladder;
  `0` or `forever` retains a tier indefinitely.

Writes are append-only. Insert rows with `(name,ts,value,labels)` or with a
previously resolved `series_id`. Use the hidden command column for:

- `resolve`, supplied with `name` and optional `labels`, returns the durable
  table-scoped series id through `last_insert_rowid()`.
- `flush` drains every series buffer into compressed chunks.
- `compact` merges eligible metric chunks and runs declared rollups.
- `rollup` builds settled buckets for the declared ladder.
- `prune:<unix-seconds>` removes whole raw/rollup chunks older than the
  explicit cutoff.

The authoritative per-series flush threshold is 4,096 points. A successful
command participates in the surrounding SQLite transaction; rollback restores
the prior live and durable state.

### `timeless_logs`

```sql
CREATE VIRTUAL TABLE logs USING timeless_logs(
  index_keys='service,path,status',
  retention='7d',
  message_index='trigram',
  timestamp_unit='us'
);
```

Fixed columns are `ts INTEGER`, `level TEXT`, `message TEXT`, and
`metadata TEXT`. Every `index_keys` entry becomes a hidden TEXT projection and
equality input. Hidden `message_contains TEXT` performs exact
case-insensitive substring filtering; hidden `max_work_entries INTEGER`
applies a positive inclusive pre-decode work cap. The final hidden command
column is named after the table.

Creation arguments:

- `index_keys=a,b,...`: metadata keys to index and expose as hidden columns;
  the empty string means none.
- `retention=<n>[s|m|h|d]`: data-time retention in the persisted timestamp
  unit.
- `message_index=none|trigram`: optional conservative trigram block pruning.
- `timestamp_unit=ms|us`: persisted row and batch timestamp unit; default
  `ms`.

The exact severity vocabulary is `debug`, `info`, `notice`, `warning`,
`error`, `critical`, `alert`, and `emergency`. `metadata` is canonical typed
JSON and preserves missing, null, empty, scalar, array, and nested-object
distinctions. Writes are append-only. Commands are `flush`, `optimize`,
`optimize:<positive max source entries>`, and `prune:<timestamp>`. A bounded
optimize may finish one merge cohort beyond the requested entry budget so it
always makes progress. The authoritative ingest buffer is 8,192 entries.

### `timeless_traces`

```sql
CREATE VIRTUAL TABLE traces USING timeless_traces(retention='72h');
```

Columns are `trace_id BLOB`, `span_id BLOB`, `parent_span_id BLOB`,
`name TEXT`, `service TEXT`, `kind TEXT`, `status TEXT`, `start_ts INTEGER`,
`duration_ns INTEGER`, `attributes TEXT`, `status_description TEXT`,
`events TEXT`, `resource TEXT`, `instrumentation_scope TEXT`, and the hidden
command column named after the table.

Trace/span/parent IDs accept packed 16/8/8-byte BLOBs or 32/16/16-digit hex
TEXT and are returned as BLOBs. An all-zero parent means no parent. `kind` is
`internal|server|client|producer|consumer`; `status` is
`unset|ok|error`. `start_ts` and `duration_ns` use nanoseconds.
`attributes`, `resource`, and `instrumentation_scope` are typed JSON objects;
`events` is a typed JSON array. Service identity uses the stored
`service.name` precedence documented in the [user guide](GUIDE.md#6-storing-traces).

The only creation argument is `retention`. Writes are append-only. Commands
are `flush`, `optimize`, `optimize:<positive max source spans>`, and
`prune:<epoch-nanoseconds>`. The authoritative ingest buffer is 8,192 spans.

## Ingestion batch formats

Every integer and float word below is little-endian. Flags and reserved words
must be zero. The complete blob is length-, UTF-8-, type-, vocabulary-, and
reference-validated before any row is buffered; a malformed blob stores
nothing.

Insert a batch into the table's hidden command column. The first byte selects
the format:

| Signal and name | Version byte | Body after common header |
|---|---:|---|
| metrics `named-v0` | `0x01` | `n_series:u32`, `n_points:u32`; each series is `name_len:u32`, UTF-8 name, `labels_len:u32`, flat labels JSON; then `series_index:u32[n]`, `ts:i64[n]`, `value_bits:u64[n]`. |
| metrics `resolved-v1` | `0x02` | `n_points:u32`, then `series_id:i64[n]`, `ts:i64[n]`, `value_bits:u64[n]`. Every id must already exist in the same table. |
| logs `flat-v0` | `0x01` | `n_entries:u32`, `ts:i64[n]`, four-level byte `level:u8[n]`, then length-prefixed UTF-8 messages and flat string-only metadata JSON. Timestamps are milliseconds. |
| logs `rich-v1` | `0x02` | `n_entries:u32`, `ts:i64[n]`, then length-prefixed exact eight-level severities, messages, and canonical typed metadata JSON. Timestamps follow the table's persisted `timestamp_unit`. |
| traces `span-v0` | `0x01` | `n_spans:u32`; packed trace/span/parent ids, length-prefixed names/services, `kind:u8[n]`, `status:u8[n]`, `start_ts:i64[n]`, `duration_ns:i64[n]`, and flat attributes JSON. |
| traces `rich-span-v1` | `0x02` | The complete v0 prefix with typed attributes, followed by length-prefixed status descriptions, events arrays, resource objects, and instrumentation-scope objects. |

The common prefix is `version:u8`, `flags:u8=0`, `reserved:u16=0`. A
length-prefixed string is `length:u32` followed by that many UTF-8 bytes.
Empty JSON payloads mean the format-specific empty object or array. Existing
version bytes remain readable; unknown versions fail explicitly.

## Metrics query modules

`R` means required; `O` means optional. `series_id` is an optional equality
constraint on row-oriented per-series modules where listed.

| Module | Output columns | Hidden inputs in positional order |
|---|---|---|
| `timeless_raw` | `series_id, labels, ts, value` | `tbl` R, `metric` R, `filter` O, `start` R, `stop` R; `series_id` O constraint. |
| `timeless_raw_batches` | `series_id, labels, points` | Same as `timeless_raw`; `series_id` O constraint. |
| `timeless_raw_frame` | `frame` | `tbl` R, `metric` R, `filter` O, `start` R, `stop` R, `max_work_points` O. |
| `timeless_aggregate` | `series_id, labels, value` | `tbl` R, `metric` R, `filter` O, `start` R, `stop` R, `agg` R; `series_id` O constraint. |
| `timeless_aggregate_frame` | `frame` | Same inputs as `timeless_aggregate`. |
| `timeless_latest` | `series_id, labels, ts, value` | `tbl` R, `metric` R, `filter` O, `start` R, `stop` R; `series_id` O constraint. |
| `timeless_latest_frame` | `frame` | Same hidden inputs as `timeless_latest`. |
| `timeless_grid` | `labels, ts, value`; hidden `series_id` | `tbl` R, `metric` R, `filter` O, `start` R, `stop` R, `step` R, `lookback` R, `fill` O. |
| `timeless_window` | `labels, ts, value`; hidden `series_id` | `tbl` R, `metric` R, `filter` O, `start` R, `stop` R, `step` R, `window` R, `agg` R, `fill` O. |
| `timeless_window_batches` | `series_id, labels, buckets` | Window inputs plus `max_work_points` O. |
| `timeless_rollup` | `labels, ts, value`; hidden `series_id` | `tbl` R, `metric` R, `filter` O, `resolution` R, `start` R, `stop` R, `agg` R. |
| `timeless_rollup_batches` | `series_id, labels, buckets` | `tbl` R, `metric` R, `filter` O, `resolution` R, `start` R, `stop` R. |
| `timeless_series` | `name, labels, series_id, min_ts, max_ts, points, chunks, buffered` | `tbl` R, `metric` O, `filter` O. |
| `timeless_label_values` | `value` | `tbl` R, `metric` R, `key` R, `filter` O. |
| `timeless_stats` | `key, value` | `tbl` R. |

Raw/aggregate/latest bounds are inclusive. Empty series emit no row.
`timeless_aggregate` supports `avg|sum|min|max|count`; count is SQLite
INTEGER. `timeless_window` supports `sum|min|max|count|avg|delta|increase|rate`,
exact nearest-rank `pNN`, and `tavg:N`. These kernels are storage reductions,
not complete PromQL; language lookback, staleness, extrapolation, labels, and
result typing belong to the Rust metrics API.

Grid/window `fill` is `none` (default sparse) or `null` (dense grid points for
series present on the grid). `max_work_points` must be a positive integer and
is an inclusive conservative pre-decode limit; failure returns no partial
frame.

## Logs and traces query modules

| Module | Output columns | Hidden inputs in positional order |
|---|---|---|
| `timeless_log_count` | `n` | `tbl` R; `filter`, `message_contains`, `start`, `stop`, `max_work_entries` O. |
| `timeless_log_values` | `value` | `tbl` R, `key` R; `filter`, `message_contains`, `start`, `stop`, `max_values`, `max_work_entries` O. |
| `timeless_log_buckets` | `bucket_ts, group_key, n` | `tbl` R, `group_by` R, `filter` O, `start` R, `stop` R, `step` R. |
| `timeless_log_query_stats` | sixteen INTEGER report columns | `tbl` R. Must immediately follow a fully consumed successful scan on the same connection; reading consumes the report. |
| `timeless_trace_services` | `value` | `tbl` R. |
| `timeless_trace_operations` | `value` | `tbl` R, `service` O. |
| `timeless_trace_buckets` | `bucket_ts, service, spans, errors, dur_sum, dur_min, dur_max, dur_p50, dur_p95, dur_p99` | `tbl` R, `service_filter` O, `start` R, `stop` R, `step` R. |

Log `filter` is a flat JSON object: `level` selects severity and other string
members are indexed metadata equality predicates. `message_contains` is the
same exact predicate as the base table. Missing count/value bounds mean the
full integer range. A positive `max_work_entries` is charged before a
candidate block decodes and fails without partial rows. `max_values` bounds
retained distinct strings.

`timeless_log_query_stats` exposes `query_total_ns`, `query_snapshot_ns`,
`query_materialize_ns`, `snapshot_payload_bytes`, `payload_bytes_read`,
`candidate_blocks`, `processed_blocks`, `blocks_skipped_by_bound`,
`buffered_entries_processed`, `decoded_entries`, `processed_entries`,
`matched_entries`, `returned_entries`, `values_read`, `timestamps_read`, and
`stable_location_snapshot`. A new, failed, cancelled, or partially consumed
scan cannot expose a stale report.

Bucket intervals are `[bucket_ts,bucket_ts+step)`, aligned to `start`.
Percentiles in `timeless_trace_buckets` are exact nearest-rank durations.

## Packed query-result formats

Packed query blobs are transport results, never on-disk shadow formats. A
decoder must reject unknown magic/version, nonzero reserved bits, impossible
counts, trailing bytes, and noncanonical validity padding. Do not guess a
future layout.

| Surface | Format | Contract |
|---|---|---|
| `timeless_raw_batches.points` | `raw-series-v0` (legacy, no magic) | `count:u32`, `timestamp:i64[count]`, `value_bits:u64[count]`. Use only when the selected SQL module is known; prefer `TRF1` for a self-identifying wide result. |
| `timeless_raw_frame.frame` | `TRF1` | magic, `series_count:u32`, `total_points:u64`, series ids, per-series counts, timestamps, and value bits. |
| `timeless_window_batches.buckets` | `TWB1` | magic, `count:u32`, timestamps, low-bit-first validity bitmap, value bits. |
| `timeless_rollup_batches.buckets` | `TRB1` | magic, `count:u32`, then bucket timestamp, exact count, avg/sum/min/max bits, last timestamp, and last-value bits. |
| `timeless_aggregate_frame.frame` | `TAF1` | magic, aggregate kind, zero flags/reserved, count, series ids, validity bitmap, and typed value words. |
| `timeless_latest_frame.frame` | `TLF1` | magic, count, series ids, timestamps, validity bitmap, and value bits. |

Exact byte layouts and copyable SQL live in the
[query cookbook](QUERIES.md#packed-window-batches). Strict public Rust
decoders for `TAF1` and `TLF1` are
`timeless_ext::query_frame::decode_aggregate_frame` and
`decode_latest_frame`. Row-oriented TVFs remain the portability floor.

## Transactions, durability, and concurrency

Buffered rows are visible to queries through the same process-owned engine.
`flush` encodes them into ordinary SQLite writes, but durability is not final
until the surrounding SQLite transaction commits according to the host's
journal and synchronous policy. Rollback restores buffers, catalog state,
blocks, terms, trace indexes, rollups, and maintenance swaps.

Connections in one process that open the same database file, attached schema,
table, and durable instance id share one engine. Writers are serialized by a
five-second bounded owner gate. A reader that would observe another
connection's transaction-private locations receives a retryable busy-style
error instead of inconsistent rows. Queries merge committed blocks with the
caller's visible buffer.

Backup and replication operate on the containing SQLite/libSQL database, not
on individual shadow tables. Use SQLite's online backup API or a coordinated
WAL checkpoint; do not copy only the visible virtual-table rows and do not
selectively omit shadow tables. Retention never deletes the legacy/source file
or any external backup.

## Errors and compatibility rules

- Unknown creation arguments, commands, severities, kinds, statuses, batch
  versions, flags, and malformed JSON fail explicitly.
- `DELETE` and ordinary `UPDATE` fail because telemetry tables are
  append-only.
- Optional limits must be positive integers when supplied; a rejected limit
  produces no partial result.
- Additive SQL modules, hidden inputs, capability members, stats keys, and
  versioned frame formats may appear in compatible releases.
- Existing batch bytes and frame magics retain their meaning. A new
  incompatible storage encoding requires a new readable version and an
  advertised capability; it does not repurpose an existing version.
- Shadow schemas and physical block codecs may evolve additively. They are not
  an application API and must never be queried by a signal server.

Every SQL recipe linked from the feature matrices is executable in
[the SQL-equivalence cookbook](QUERY_SQL_EQUIVALENTS.md), and the real
extension runs all of them in `tests/cli.sh` section 45.
