# Rust signal server API reference

This is the canonical reference for the three signal-specific Rust data-plane
binaries shipped from the `servers` workspace. The binaries own HTTP parsing,
bounded queues, language evaluation, SQLite connections, and process
lifecycle. They do not implement a second storage engine: every durable write,
query, and maintenance operation uses the public surfaces of
`libtimeless_ext` documented in the
[SQLite extension API reference](SQL_API_REFERENCE.md).

The servers are independently usable. Phoenix is a control plane that may
supervise them and issue policy files and tokens, but no request requires
BEAM, Elixir, a NIF, or a fallback process. PromQL, MetricsQL, and LogsQL
support is defined by the [query feature maps](QUERY_FEATURES.md). TraceQL is
not implemented.

## Binaries and launch contract

| Binary | Default TCP listener | Storage table | Required extension batch capability |
|---|---:|---|---|
| `timeless-metrics-api` | `127.0.0.1:19439` | `metric_samples` for a fresh database; legacy public `metrics` vtab remains readable | `named-v0` |
| `timeless-logs-api` | `127.0.0.1:19429` | `logs` with epoch-microsecond timestamps | `rich-v1` |
| `timeless-traces-api` | `127.0.0.1:19449` | `traces` | `rich-span-v2` |

All three use the same positional command line:

```text
timeless-<signal>-api <libtimeless_ext.so> <database> [listen-address]
```

The extension and database paths are required. The listener is the optional
third positional argument. An extra or malformed argument exits with status
2. `--version`, supplied by itself, prints a JSON object containing the binary
name, Cargo version, source commit, target triple, and build profile.

TCP is the implemented transport. Loopback is the safe default. An operator
who has separately secured a container or host network may set
`TIMELESS_ALLOW_NON_LOOPBACK=1` and pass a non-loopback address. There is no
Unix-socket listener in this release.

Authentication is opt-in at every level, and the library and binaries agree:
`Config::default()` and an unconfigured binary both start open. Set
`TIMELESS_AUTH_MODE=required` with `TIMELESS_AUTH_POLICY_FILE` to enable
token verification; a bad policy still fails closed before the listener
binds.

## Complete route inventory

The following marked inventory is checked against every production Axum
`.route(...)` registration, including its exact method set.

<!-- public-server-routes:start -->

The "Required scope" column applies **only when auth is enabled**
(`TIMELESS_AUTH_MODE=required`); an open server enforces no scopes.

| Signal | Methods | Path | Required scope | Contract |
|---|---|---|---|---|
| `metrics` | `GET` | `/live` | none | Process liveness only; does not touch SQLite. |
| `metrics` | `GET` | `/ready` | `metrics:stats` | Readiness plus build, storage, queue, rollup, and file accounting. |
| `metrics` | `GET` | `/health` | `metrics:stats` | Alias of `/ready`. |
| `metrics` | `GET` | `/metrics` | none | Prometheus text exposition of the plane's own operational stats plus `timeless_build_info`; unauthenticated like the probe endpoints. |
| `metrics` | `GET` | `/select/metrics/stats` | `metrics:stats` | Complete serialized `StorageStats`. |
| `metrics` | `POST` | `/api/v1/flush` | `metrics:maintenance` | Ordered writer completion, extension flush, and durability barrier. |
| `metrics` | `POST` | `/api/v1/backup` | `metrics:maintenance` | Flush, compact/roll up, checkpoint, and verified SQLite backup. |
| `metrics` | `POST` | `/api/v1/import` | `metrics:write` | VictoriaMetrics JSON-line import. |
| `metrics` | `POST` | `/api/v1/import/prometheus` | `metrics:write` | Prometheus text exposition import through the public extension parser. |
| `metrics` | `GET, PUT` | `/api/v1/scrape/targets` | `metrics:read` for GET; `metrics:write` for PUT | Read or atomically replace the process-local scrape target set. |
| `metrics` | `GET, POST` | `/api/v1/query` | `metrics:read` | Native exact-latest query with `metric=`, or PromQL instant query with `query=`. |
| `metrics` | `GET` | `/api/v1/export` | `metrics:read` | VictoriaMetrics JSON-line raw export. |
| `metrics` | `GET, POST` | `/api/v1/query_range` | `metrics:read` | Native range query with `metric=`, or PromQL range query with `query=`. |
| `metrics` | `GET` | `/api/v1/labels` | `metrics:read` | Native label-name discovery. |
| `metrics` | `GET` | `/api/v1/label/{name}/values` | `metrics:read` | Native label-value discovery. |
| `metrics` | `GET` | `/api/v1/series` | `metrics:read` | Native series discovery. |
| `metrics` | `GET` | `/prometheus/api/v1/labels` | `metrics:read` | Prometheus-compatible label-name alias. |
| `metrics` | `GET` | `/prometheus/api/v1/label/{name}/values` | `metrics:read` | Prometheus-compatible label-value alias. |
| `metrics` | `GET` | `/prometheus/api/v1/series` | `metrics:read` | Prometheus-compatible series discovery; requires `match[]`. |
| `metrics` | `GET, POST` | `/prometheus/api/v1/query` | `metrics:read` | Stable PromQL instant endpoint. |
| `metrics` | `GET, POST` | `/prometheus/api/v1/query_range` | `metrics:read` | Stable PromQL range endpoint. |
| `metrics` | `GET, POST` | `/metricsql/api/v1/query` | `metrics:read` | Explicit MetricsQL instant compatibility tier. |
| `metrics` | `GET, POST` | `/metricsql/api/v1/query_range` | `metrics:read` | Explicit MetricsQL range compatibility tier. |
| `logs` | `GET` | `/live` | none | Process liveness only; does not touch SQLite. |
| `logs` | `GET` | `/ready` | `logs:stats` | Readiness plus build, storage, and queue accounting. |
| `logs` | `GET` | `/health` | `logs:stats` | Alias of `/ready`. |
| `logs` | `GET` | `/metrics` | none | Prometheus text exposition of the plane's own operational stats plus `timeless_build_info`; unauthenticated like the probe endpoints. |
| `logs` | `POST` | `/insert/jsonline` | `logs:write` | NDJSON ingestion into one public rich-log batch per request. |
| `logs` | `GET, POST` | `/select/logsql/query` | `logs:read` | Native parameter query on GET; LogsQL compatibility grammar on POST. |
| `logs` | `GET` | `/select/logsql/field_values` | `logs:read` | Bounded discovery for `service`, `host`, `path`, or `status`. |
| `logs` | `GET` | `/select/logsql/stats` | `logs:stats` | Complete serialized `StorageStats`. |
| `logs` | `GET, POST` | `/select/logsql/tail` | `logs:read` | Live tail: streams admitted entries matching one LogsQL filter expression as NDJSON; pipelines are rejected; slow consumers drop (counted in stats). |
| `logs` | `POST` | `/api/v1/flush` | `logs:maintenance` | Ordered writer completion and extension durability barrier. |
| `logs` | `POST` | `/api/v1/backup` | `logs:maintenance` | Flush, optimize, checkpoint, and verified SQLite backup. |
| `traces` | `GET` | `/live` | none | Process liveness only; does not touch SQLite. |
| `traces` | `GET` | `/ready` | `traces:stats` | Readiness plus build, negotiated rich-span capability, and queue watermarks. |
| `traces` | `GET` | `/health` | `traces:stats` | Alias of `/ready`. |
| `traces` | `GET` | `/metrics` | none | Prometheus text exposition of the plane's own operational stats plus `timeless_build_info`; unauthenticated like the probe endpoints. |
| `traces` | `GET` | `/select/traces/stats` | `traces:stats` | Complete serialized `StorageStats`. |
| `traces` | `GET` | `/select/jaeger/api/services` | `traces:read` | Sorted Jaeger service discovery. |
| `traces` | `GET` | `/select/jaeger/api/services/{service}/operations` | `traces:read` | Sorted operations for one service. |
| `traces` | `GET` | `/select/jaeger/api/traces` | `traces:read` | Jaeger trace search. |
| `traces` | `GET` | `/select/jaeger/api/traces/{trace_id}` | `traces:read` | One Jaeger trace with all matching spans. |
| `traces` | `GET` | `/select/timeless/api/spans` | `traces:read` | Native rich-span search for the Timeless dashboard contract. |
| `traces` | `GET, POST` | `/select/timeless/api/spans/tail` | `traces:read` | Live tail: streams admitted spans as NDJSON, filtered by the search surface's live-matchable parameters (`service`, `name`, `kind`, `status`) plus an `attributes` JSON object of exact scalar matches; invalid values are rejected; slow consumers drop spans (counted in stats). |
| `traces` | `GET` | `/select/timeless/api/traces/{trace_id}` | `traces:read` | One native trace with complete rich-span fields. |
| `traces` | `POST` | `/api/v1/flush` | `traces:maintenance` | Ordered writer completion, extension flush, and durability barrier. |
| `traces` | `POST` | `/api/v1/backup` | `traces:maintenance` | Flush, optimize, checkpoint, and verified SQLite backup. |

A backup occupies the signal's single writer for its whole duration — flush,
the entire optimize backlog, a WAL checkpoint, then the copy — so queued
ingest waits behind it. At most one runs per process at a time: an
overlapping `POST /api/v1/backup` is answered `409 Conflict` with
`{"error":"conflict","reason":"a backup is already running"}` rather than
queued, so repeating the call costs the writer nothing. Retry after the
in-flight backup completes.

| `traces` | `POST` | `/insert/opentelemetry/v1/traces` | `traces:write` | OTLP JSON, protobuf, or gzip-compressed protobuf ingestion. |

<!-- public-server-routes:end -->

An unregistered route always returns HTTP 422 with
`{"error":"unsupported_capability","reason":"unsupported_route"}`. The
servers never forward it to Rocket, Phoenix, another process, or another
storage owner.

The three serialized `StorageStats` responses obtain extension-owned tier,
block, index-byte, and optimizer-source accounting only from the public
`timeless_stats` TVF. Servers never query implementation-owned shadow tables.
Whole-database page/freelist and file/WAL/SHM sizes still come from public
SQLite PRAGMAs and filesystem metadata. If `dbstat` is unavailable,
`sqlite_index_bytes` is zero while all logical counts remain available.
Trace responses additionally pass through the cumulative public
`attribute_index_fields`, `attribute_bloom_rows`, and
`attribute_bloom_bytes` storage values, plus the cumulative public
`query_decoded_columns`, `query_decoded_column_bytes`,
`query_materialized_values`, and `query_materialized_rich_values` values as
`extension_query_*` fields; they remain aggregate diagnostics rather than
request-local attribution when readers overlap.

### Storage and compression series

Every plane's `/metrics` exposition publishes the honest storage split as
separate families: `timeless_<plane>_storage_bytes` (the engine's
`bytes_on_disk` — data-block payload only), `timeless_<plane>_index_bytes`,
`timeless_<plane>_wal_bytes`, `timeless_<plane>_freelist_bytes`, and
`timeless_<plane>_database_file_bytes`. A compression ratio is
raw-vs-storage; index, WAL, freelist, and whole-file bytes are operational
series and are never part of one.

The raw side per plane: `timeless_metrics_raw_ingested_bytes` is
`16 × total_points` (8-byte timestamp + 8-byte value per sample — the
standard raw comparator; series identity is the amortized catalog), and is
lifetime-accurate because it derives from durable point counts.
`timeless_logs_raw_ingested_bytes_total` and
`timeless_traces_raw_ingested_bytes_total` surface the extension's
persisted `ingest_raw_bytes_total` counter: logical row bytes (log
timestamp + level + message + metadata; span ids + kind/status + timings +
string fields) counted once when rows become durable, monotonic under
optimize and prune, restart-safe. Rows still buffered in memory are not
yet counted, so the ratio briefly understates after bursts and
self-corrects on flush; pre-upgrade databases start the counter at their
next flush. The paired
`timeless_logs|traces_compression_input|output_bytes_total` counters are
exported alongside. The same values appear in the serialized `StorageStats`
responses; each plane README documents its exact series list.

## Metrics requests

GET and POST query endpoints consume URL-encoded parameters. On POST, form
body values follow URL values and therefore win for a duplicated singleton
parameter. Unknown Prometheus/MetricsQL parameters fail explicitly.

- Native latest requires `metric`; other non-reserved parameters are exact
  label matchers.
- Native export requires `metric`; `start`/`from` and `end`/`to` are epoch
  seconds and default to the previous hour and now.
- Native range requires `metric`; accepts the same bounds, a positive
  whole-second `step` (default 60), and
  `aggregate=avg|min|max|sum|count|last|first|rate`. Label values beginning
  with `=~` are regular expressions. Unsupported dashboard composition
  parameters fail rather than being ignored.
- PromQL instant accepts `query`, optional `time`, and optional
  `lookback_delta`. Range accepts `query`, `start`, `end`, `step`, and optional
  `lookback_delta`.
- MetricsQL instant additionally accepts `step` and `max_lookback`; range
  additionally accepts `max_lookback`.
- Discovery accepts repeated `match[]` or `match`. Native label values and
  series may also use `metric`; the Prometheus series alias requires at least
  one `match[]`.

The exact expression compatibility and response behavior are in the
[PromQL matrix](PROMQL_FEATURE_MATRIX.md) and
[query cookbook](QUERIES.md#promql-nameless-and-multi-name-selectors).
Prometheus-family success and error envelopes follow the documented
`status`, `data`, `errorType`, `error`, `warnings`, and `infos` contract.

`POST /api/v1/import` accepts VictoriaMetrics JSON lines. Valid rows from a
partially malformed request are admitted and parse failures are counted.
`POST /api/v1/import/prometheus` accepts Prometheus text exposition and gives
the complete body directly to the extension's public parser. A successful
204 means bounded writer-queue admission, not an implicit extension flush.

Scrape target replacement accepts:

```json
{
  "version": 1,
  "targets": [{
    "id": 1,
    "job_name": "self",
    "scheme": "http",
    "address": "127.0.0.1:4000",
    "metrics_path": "/metrics",
    "scrape_interval_secs": 15,
    "scrape_timeout_secs": 5,
    "labels": {"instance": "local"},
    "auth": null,
    "enabled": true
  }]
}
```

The set is process-local control-plane state and must be supplied again after
restart. Versions cannot move backward. A set has at most 10,000 targets;
IDs are unique and positive; job and address are nonempty; intervals are
1–86,400 seconds; timeouts are 1–300 seconds and no longer than the interval;
label names contain only ASCII letters, digits, and underscore. HTTP and
HTTPS are supported. Each response is capped at 10 MiB. Bearer auth takes
precedence over optional basic-auth username/password. Configured labels do
not overwrite labels already present in a sample.

## Logs requests

`POST /insert/jsonline` accepts one JSON object per line. `_msg_field` and
`_time_field` query parameters select alternate source keys (defaults `_msg`
and `_time`). The release table uses epoch microseconds. Every other retained
field remains typed/nested metadata, and all eight severities remain exact.
A fully valid request returns 204; a partially valid request returns 200 with
accepted/error counts. That response means admission to the bounded writer
queue, not durability; call the flush route for the ordered barrier.

Native `GET /select/logsql/query` rejects unknown parameters and accepts
`level`, `message`, `service`, `host`, `path`, `status`, `start`, `end`,
`limit`, `offset`, and `order`. `order` is `asc` or `desc`; bounds accept the
documented native time forms. Field discovery requires
`field=service|host|path|status`, accepts the same filters except offset/order,
and defaults to 1,000 values.

`POST /select/logsql/query` uses an URL-encoded form with required `query` and
optional `allow_partial_response`. `false` is the complete fail-closed mode.
`true` fails explicitly because one authoritative SQLite owner cannot produce
an honest distributed partial result. The complete shipped grammar, strict
errors, rich values, ordering, and intentional VictoriaLogs differences are
in the [LogsQL matrix](LOGSQL_FEATURE_MATRIX.md) and
[query cookbook](QUERIES.md#bounded-log-queries).

## Trace requests

`POST /insert/opentelemetry/v1/traces` accepts OTLP JSON by default and OTLP
protobuf when `Content-Type` contains `application/x-protobuf`. Protobuf may
also use `Content-Encoding: gzip`. The handler validates the complete request,
encodes one public rich-span-v2 batch, and waits for its SQLite insertion
statement before returning `{"partialSuccess":{}}`. That statement may leave
spans in the extension's authoritative 8,192-span buffer; call flush when a
durability barrier is required. There is no server-owned span buffer.

Jaeger search accepts `service`, `operation`, microsecond `start` and `end`,
non-negative `limit` (default 100), and `minDuration`/`maxDuration` duration
strings. Its compatibility limit counts selected spans before trace grouping,
so a limited trace may be incomplete. Unknown parameters return explicit 422.

Native dashboard span search accepts `name`, `service`,
`kind=internal|server|client|producer|consumer`,
`status=unset|ok|error`, nanosecond `since`/`until`, `limit` from 1 through
100 (default 100), non-negative `offset`, and `order=asc|desc` (default desc).
It returns the stored status description, typed attributes, events, resource,
and instrumentation scope rather than blanking fields for a lower-fidelity
wire format.

## Runtime environment

The marked inventory below is checked against production source before its
test modules. Unless a row says otherwise, numeric values are positive base-10
integers and an invalid value stops startup with status 2.

<!-- public-server-environment:start -->

| Variable | Applies to | Default | Meaning and constraints |
|---|---|---:|---|
| `TIMELESS_ALLOW_NON_LOOPBACK` | all | unset | Only `1`, `true`, or `TRUE` permits an explicitly supplied non-loopback TCP listener. |
| `TIMELESS_AUTH_MODE` | all | `disabled` | Set `required` to enable token verification; auth is opt-in. |
| `TIMELESS_AUTH_POLICY_FILE` | all | none | Readable policy-v1 JSON path; required when `TIMELESS_AUTH_MODE=required`. |
| `TIMELESS_TENANT` | all | `default` | Exact tenant required in both policy and token. |
| `TIMELESS_BACKUP_DIR` | all | `backups/` beside the database file | Directory backups are confined to; destinations that canonicalize outside it are rejected. Relative destinations resolve inside it. |
| `TIMELESS_ADMIN_KEY` | all | unset | When set, `/api/v1/scrape/targets`, `/api/v1/backup`, `/api/v1/flush`, and `/api/v1/optimize` additionally require it (header `x-timeless-admin-key`), independent of `TIMELESS_AUTH_MODE`. Unset means open. Header-only by design: a query-string key leaks into access/proxy logs and browser history. Keys must be visible ASCII (an HTTP header-value constraint). |
| `TIMELESS_METRICS_READER_CONNECTIONS` | metrics | `2` | Independent bounded SQLite readers. |
| `TIMELESS_METRICS_COMMAND_QUEUE_BATCHES` | metrics | `256` | Writer-command queue capacity in admitted request batches. |
| `TIMELESS_METRICS_QUEUE_BYTES` | metrics | `134217728` | Queued-payload admission gate in bytes; admissions wait while in-flight ingest bytes exceed it (a batch larger than the gate is admitted alone). |
| `TIMELESS_METRICS_FLUSH_INTERVAL_SECS` | metrics | `10` | Ordered public extension flush cadence. |
| `TIMELESS_METRICS_COMPACT_INTERVAL_SECS` | metrics | `300` | Public compact/rollup cadence. |
| `TIMELESS_METRICS_RETENTION_INTERVAL_SECS` | metrics | `3600` | Seven-day raw-retention prune check cadence. |
| `TIMELESS_METRICS_PROMQL_MAX_POINTS_PER_SERIES` | metrics | `11000` | Evaluation-grid points per series; valid range 1–11,000. |
| `TIMELESS_METRICS_PROMQL_MAX_RESULT_POINTS` | metrics | `100000` | Final serialized result points. |
| `TIMELESS_METRICS_PROMQL_MAX_WORK_POINTS` | metrics | `100000` | Cumulative storage and intermediate evaluation points. |
| `TIMELESS_METRICS_PROMQL_MAX_RESPONSE_BYTES` | metrics | `16777216` | Serialized response bytes. |
| `TIMELESS_METRICS_PROMQL_DEFAULT_SUBQUERY_STEP_MS` | metrics | `15000` | Omitted PromQL subquery resolution in milliseconds. |
| `TIMELESS_METRICS_PROMQL_DEADLINE_MS` | metrics | `30000` | Hard PromQL/MetricsQL execution deadline. |
| `TIMELESS_LOGS_READER_CONNECTIONS` | logs | `2` | Independent bounded SQLite readers. |
| `TIMELESS_LOGS_COMMAND_QUEUE_BATCHES` | logs | `256` | Writer-command queue capacity in admitted request batches. |
| `TIMELESS_LOGS_QUEUE_BYTES` | logs | `134217728` | Queued-payload admission gate in bytes; admissions wait while in-flight ingest bytes exceed it (a batch larger than the gate is admitted alone). |
| `TIMELESS_LOGS_FLUSH_INTERVAL_SECS` | logs | `1` | Ordered public extension flush cadence. |
| `TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS` | logs | `30` | Bounded public optimize cadence. |
| `TIMELESS_LOGS_LOGSQL_MAX_RESULT_ROWS` | logs | `100000` | Final rows; valid range 1–100,000. |
| `TIMELESS_LOGS_LOGSQL_MAX_WORK_ROWS` | logs | `100000` | Cumulative decoded/examined rows and bounded state items. |
| `TIMELESS_LOGS_LOGSQL_MAX_RESPONSE_BYTES` | logs | `16777216` | Response and bounded pipeline-state bytes. |
| `TIMELESS_LOGS_LOGSQL_DEADLINE_MS` | logs | `30000` | Hard LogsQL/native-query deadline in milliseconds. |
| `TIMELESS_LOGS_INDEX_KEYS` | logs | absent/inherit | Comma-separated indexed-metadata allowlist. New stores are created with it; an existing store whose persisted allowlist differs is reindexed once at startup (postings rewritten for every block). Absent preserves the store's current allowlist. |
| `TIMELESS_LOGS_RETENTION` | logs | absent/inherit | Retention window as `<n>` plus a required unit suffix `s`, `m`, `h`, or `d`. Applied to new and existing stores at startup; enforcement remains at flush/optimize boundaries. Absent preserves the store's current window. |
| `TIMELESS_TRACES_READER_CONNECTIONS` | traces | `2` | Independent bounded SQLite readers. |
| `TIMELESS_TRACES_COMMAND_QUEUE_BATCHES` | traces | `256` | Writer-command queue capacity in admitted requests. |
| `TIMELESS_TRACES_QUEUE_BYTES` | traces | `134217728` | Queued-payload admission gate in bytes; admissions wait while in-flight ingest bytes exceed it (a batch larger than the gate is admitted alone). |
| `TIMELESS_TRACES_RETENTION_SECS` | traces | absent/inherit | If absent, preserve the vtab's stored retention (a fresh table has none); positive sets and enforces it; `0` explicitly disables it. |
| `TIMELESS_TRACES_FLUSH_INTERVAL_SECS` | traces | `1` | Ordered public extension flush cadence. |
| `TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS` | traces | `30` | Bounded public optimize cadence. |

<!-- public-server-environment:end -->

`TIMELESS_BUILD_COMMIT` is a build-time override, not a runtime setting. If
unset, the build script records the current Git commit when available. Cargo
supplies target and profile. These values are returned by `--version` and
health/readiness build identity.

## Authentication and admission

Authentication is **opt-in**: servers start open, and verification engages
only under `TIMELESS_AUTH_MODE=required` with a policy file. For running the
binaries standalone — including non-loopback binding and sharing a database
with an embedding application — see
[Running standalone](../servers/README.md#running-standalone-no-control-plane-no-elixir).

Enabling it takes four commands — no Elixir control plane required:

```bash
timeless-authctl keygen --out ./auth
timeless-authctl policy init --signal metrics --key "$(cat ./auth/timeless-auth.pub)" \
    --out ./auth/policy.json
TIMELESS_AUTH_MODE=required TIMELESS_AUTH_POLICY_FILE=./auth/policy.json ./timeless-metrics-api &
curl -H "Authorization: Bearer $(timeless-authctl token mint --key ./auth/timeless-auth.key \
    --policy ./auth/policy.json --subject default --signal metrics --ttl 1h)" \
    http://127.0.0.1:19439/api/v1/status
```

`timeless-authctl policy add-subject` narrows scopes per caller, and
`timeless-authctl token inspect` decodes a token without verifying it. For
operators who want ingest and query open but administration closed without
any token machinery, `TIMELESS_ADMIN_KEY` independently gates the
administrative routes (see the environment table). When enabled,
every route except the probe endpoints `/live`, `/ready`, and `/health`,
and the `/metrics` self-metrics endpoint (exact-match exemptions, so
container probes, load balancers, and Prometheus scrapers need no
credentials) requires an exact `Authorization: Bearer <token>`. Tokens are compact JWS/JWT values signed with
Ed25519 (`alg=EdDSA`); `typ`, if present, must be `JWT`. The policy contains
base64url-no-padding 32-byte public keys. Rust validates tokens but never
issues them.

Policy version 1 contains `issuer`, `audience`, `tenant`,
`minimum_auth_version`, positive `max_token_seconds`, `maximum_limits`, a
nonempty `subjects` object, a nonempty `keys` array, and optional
`revoked_jtis`. A subject contains exact allowed scopes, `auth_version`,
`maximum_limits`, and optional `enabled` (default true). A key contains
`kid`, `public_key`, `not_before`, `expires_at`, and optional `revoked`.

Token claims are `iss`, `aud`, `sub`, `jti`, `tenant`, `signal`, `scopes`,
`auth_version`, `iat`, `nbf`, `exp`, and `limits`. The signal is exactly
`metrics`, `logs`, or `traces`. The seven positive limit fields are:

| Claim limit | Enforcement |
|---|---|
| `max_request_bytes` | Auth middleware buffers and caps the request before route execution. The hard server body cap remains 10 MiB. |
| `max_decompressed_bytes` | Caps gzip OTLP protobuf after decompression; other current request formats are not compressed. |
| `max_response_bytes` | Auth middleware caps the final body; signal query limits may be tighter. |
| `max_query_rows` | Prechecks `limit`, `max_rows`, `max_points`, and the LogsQL `limit` pipeline stage; then verifies the handler's internal exact result-row header. |
| `max_request_ms` | Deadline for read and stats routes. Writes are not response-cancelled because queued durability would make timeout ambiguous. |
| `max_concurrent_requests` | Per-subject active request cap. |
| `max_queue_ms` | Per-subject admission wait before HTTP 429. |

Claim limits cannot exceed either policy-wide or subject-wide maxima, and
auth cannot raise the signal server's hard query/body limits. A token may not
claim a scope absent from its subject even if the current route needs only one
of the claimed scopes. Policy files are at most 1 MiB; bearer tokens are at
most 32 KiB; not-before and issued-at checks allow 30 seconds of positive
clock skew. Policy replacements are noticed by size, modification time, and
file identity so atomic same-size rewrites reload correctly.

Scope derivation is deterministic: readiness, health, and paths ending in
`/stats` use `<signal>:stats`; paths ending in `/flush`, `/optimize`, or
`/backup` use maintenance; remaining GET/HEAD and query/discovery routes use
read; other routes use write. The exact result is listed beside every route
above.

Authorization errors use
`{"error":"data_plane_authorization","reason":"<code>"}`. Stable reason
codes are:

- credential/token/policy: `missing_credentials`, `invalid_credentials`,
  `invalid_token`, `invalid_key`, `invalid_signature`, `unknown_key`,
  `unknown_subject`, `authorization_policy_unavailable`,
  `authorization_policy_invalid`, `clock_unavailable`, `expired_token`,
  `token_lifetime_exceeded`, `token_not_yet_valid`, `revoked_key`,
  `revoked_token`, and `stale_auth_version`;
- identity/authority: `wrong_issuer`, `wrong_audience`, `wrong_tenant`,
  `wrong_signal`, `insufficient_scope`, `scope_policy_exceeded`, and
  `claim_limits_exceeded`;
- limits/admission: `request_bytes_exceeded`, `response_bytes_exceeded`,
  `invalid_query_limit`, `query_rows_exceeded`, `request_time_exceeded`,
  `queue_unavailable`, and `queue_wait_exceeded`.

Credential failures are HTTP 401, authority failures HTTP 403, malformed
limit parameters HTTP 400, byte caps HTTP 413, row caps HTTP 422, admission
exhaustion HTTP 429/503, and read deadlines HTTP 504.

## Startup, ownership, and shutdown

Startup fails closed before binding the listener if configuration, auth
policy, extension loading, capability negotiation, data ABI, minimum version,
signal batch support, public vtab schema, stored retention, or database schema
is incompatible. The current server data-schema ledger version and extension
data ABI are both 1. A database written by a newer schema version is not
mutated. A pre-ledger database receives the additive, idempotent
`_timeless_schema_migrations` v1 row on the writer connection.

Each server holds an advisory exclusive
`<database>.timeless-<signal>-api.lock` lease. This fences another conforming
server owner. It cannot protect against an unrelated SQLite process that
ignores the lease; an operator must not give another writer the database while
the server owns it.

The writer is a bounded FIFO. Scheduled flush/compact/optimize/retention work
uses the same ordered public command path as explicit maintenance. On SIGINT
or SIGTERM, Axum stops new admission and drains accepted HTTP requests. The
server stops periodic maintenance, closes readers, places a final flush behind
all admitted writer commands, checkpoints the WAL with `TRUNCATE`, joins its
workers, and releases the owner lease. Metrics also stop all scrape tasks.
Shutdown returns failure if the final flush or complete checkpoint fails.

SIGKILL cannot run this sequence. Already flushed SQLite transactions remain
recoverable; an extension-buffer tail is intentionally not claimed durable.

## Flush, backup, restore, and WAL

Explicit flush is the durability boundary for every signal. Metrics and
traces return exact admitted/completed/failed/queued/in-flight watermarks;
logs returns `{"status":"ok"}` after the same ordered writer barrier.

Every backup route accepts exactly:

```json
{"destination":"/absolute/path/to/new-backup.db"}
```

The parent directory must already exist. The destination must be absolute,
must name a file, and must not exist. Backup runs on the established sole
writer connection: metrics flushes and compacts/rolls up; logs and traces
flush and drain actionable optimize backlog. The writer then requires a
complete WAL `TRUNCATE` checkpoint, copies pages with SQLite's online-backup
API, runs `PRAGMA quick_check`, validates the signal schema ledger, fsyncs the
staging file, and publishes without overwrite in the destination directory.
An error removes the staging name and does not label a partial copy complete.

The success report contains signal, final destination, bytes, copied pages,
schema version, WAL/checkpoint frame counts, completion Unix seconds, and
elapsed nanoseconds. Restore is deliberately offline: stop the owner, retain
the current database and WAL/SHM as rollback material, copy or rename the
verified backup into the configured database path, and start the same or a
compatible newer server. Startup performs the capability/schema check before
serving. Do not replace a live database file behind an owner process.

## Error envelopes

The auth envelope is documented above. Signal handlers use these additional
stable families:

| Family | Status | Shape |
|---|---:|---|
| Unsupported route/parameter/capability | 422 | `{"error":"unsupported_capability","reason":"..."}`; LogsQL may add `message`. |
| Malformed LogsQL | 400 | `{"error":"invalid_query","reason":"malformed_logsql","message":"..."}` |
| LogsQL execution conflict | 422 | `{"error":"query_execution","reason":"field_conflict","message":"..."}` |
| Logs query limit | 422 | `{"error":"query_limit",...}` with reason `max_result_rows`, `max_work_rows`, or `max_response_bytes` and numeric `limit`. |
| Logs query timeout | 504 | `{"error":"timeout","reason":"query_deadline","deadline_ms":N}` |
| Prometheus/MetricsQL parse | 400 | `{"status":"error","errorType":"bad_data","error":"..."}` |
| Prometheus/MetricsQL execution/limit | 422 | Same envelope with `errorType="execution"`. |
| Prometheus/MetricsQL timeout | 504 | Same envelope with `errorType="timeout"`. |
| Native metrics server/client | 500/400 | `{"status":"error","error":"..."}` |
| Native logs server | 500 | `{"error":"..."}` |
| Native traces server/client | 500 or route-specific 400/413 | `{"status":"error","error":"..."}` or `{"error":"..."}` |

Unknown parameters and unsupported language constructs fail explicitly; no
server silently delegates to Elixir or another query implementation. Detailed
language diagnostics and intentional compatibility differences are versioned
with their matrix rows.

## Platform and artifact boundary

The intended release matrix covers x86-64 and AArch64 Linux GNU and macOS.
Linux loadable extensions use `.so`; macOS uses `.dylib`. A complete archive
contains the telemetry extension and all three signal-specific binaries.
`dbhealth` is a separate extension artifact and is not registered by these
servers.

`v0.4.1` passed all four intended native package jobs and the complete outer
checksum gate. The archives are retained as authenticated GitHub Actions
artifacts until 2026-11-06; the workflow did not attach them to a permanent
GitHub Release. Other targets may compile from source but are not claimed as
release artifacts. Exact archive paths, current channel status, checksums,
manifest/SBOM content, install layout, and removal behavior are in the
[artifact inventory](ARTIFACTS.md).

The binaries use TCP, a local SQLite/libSQL-compatible database file, WAL
mode, and the loadable extension API. A direct Rust embedding that does not
need HTTP should use the mutually exclusive `embedded` crate feature,
`timeless_ext::register_telemetry`, and the
[embedded guide](EMBEDDED_RUST.md) instead of launching a signal server.

Before replacing either side of the binary/extension pair, follow the
[compatibility statement](COMPATIBILITY.md) and
[upgrade and rollback guide](UPGRADE.md).
