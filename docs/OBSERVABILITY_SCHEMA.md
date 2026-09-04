# Timeless observability schema

Friendly query surface over the telemetry virtual tables (#20), in the
spirit of MySQL Performance Schema: stable output shapes for ordinary
SQL users, no expert recipes required. This document states the
lifecycle and compatibility policy (#21). Per-signal objects and query
examples grow here as phases land; the current MVP covers traces,
metrics, and logs companion views (no evaluators).

## Visual tour

`tools/demogen/schema_screencast.py` drives a real `sqlite3` session
through the whole story — install, inventory, friendly queries across
all three signals, owned-only removal — with simulated typing. A
recorded take ships in this directory: `schema-demo.cast` (asciinema
v3) and `schema-demo.gif`. Play the cast with `asciinema play` or
asciinema.org; re-record with:

```sh
cargo build --release -p timeless-ext
python3 tools/demogen/schema_screencast.py tour.db --cast schema-demo.cast
agg --cols 120 --rows 32 schema-demo.cast schema-demo.gif   # optional GIF
```

## Version contract

- The binary advertises `observability_schema` in `timeless_capabilities()`
  (currently `1`). Clients compare it against installed rows before
  relying on new columns.
- Every installed object carries its own `schema_version` in
  `timeless_schema_inventory`. Versions are per object, not per
  database: a release that changes one view upgrades exactly that view.
- Compatibility within a schema version is additive. A new binary never
  renames or narrows an installed shape at the same version; changes
  ship as a version bump plus migration.

## Installation

- Creating a signal virtual table installs its companion objects in the
  same `CREATE VIRTUAL TABLE` transaction: all-or-nothing, rollback
  removes everything, and a name collision with an object this
  installation does not own fails the statement without touching it.
- Object names derive deterministically: `timeless_<source>_<kind>`
  (e.g. source `traces` owns `timeless_traces_spans`).
- Opening a database refreshs stale definitions best-effort: a writable
  open drops and recreates objects whose recorded version differs;
  read-only opens keep working with whatever is installed. Refresh never
  fails the open itself.
- Two consequences of lazy (first-touch) refresh, by design:
  - the statement that triggers an upgrade may still serve the previous
    shape; the next statement sees the new one (dashboards self-heal on
    their next refresh; the inventory row says exactly what is
    installed);
  - a stale definition fails loudly at query time (SQLite errors on
    unknown columns) rather than returning wrong rows.

## Removal and upgrade

- `DROP TABLE` on a source table removes exactly the objects the
  inventory attributes to it, in the same transaction. User objects are
  never touched; the shared inventory table itself stays.
- The empty inventory table is deliberately never dropped: removing
  shared infrastructure from a per-source `DROP` would race concurrent
  installs (deleted rows, dropped table, orphaned views). Reinstalling
  into the schema reuses it; nothing redundant accumulates.
- Upgrades are per-object `DROP` + `CREATE` at the recorded version
  boundary — migrations in the web-framework sense, executed
  automatically on a writable open. There is no separate upgrade
  command to forget.
- Downgrade direction: an older binary leaves newer-versioned objects
  alone and keeps serving the base tables. Newer shapes may error when
  queried (unknown columns), which is loud, not wrong.

## Schema migrations and the extension

Companion views reference TVFs and base vtabs, so SQLite schema
rewrites that re-resolve view definitions (`ALTER TABLE ... DROP
COLUMN`, `RENAME COLUMN`) require the extension loaded — the same
standing rule as any TVF-dependent schema (and as FTS5-style shadow
triggers). `ADD COLUMN`, `VACUUM`, backup, and `DROP TABLE` are
unaffected. Databases created before companions existed have none, so
legacy upgrade flows are unchanged.

View bodies never schema-qualify their sources: unqualified names in a
stored view resolve to the view's home database, so installed views
survive direct opens, backup copies, and `ATTACH` under any alias.
(Qualifying bodies bricks all three — schema validation fails when the
file opens without that alias attached.) Only `CREATE`/`DROP` targets
and installer bookkeeping queries carry schema qualifiers.

## Inventory

`timeless_schema_inventory`, one row per owned object:

| column | meaning |
|---|---|
| `source_database`, `source_table` | the signal table this object belongs to |
| `object_name`, `object_kind` | what was installed (`view` today) |
| `schema_version` | definition version of that object |
| `description` | human-readable reference for users |
| `installed_at` | unix epoch seconds |

Querying this table is the capability/version query: it identifies
every installed object, its source, and its version. Descriptions are
mandatory — the contract harness rejects empty ones.

## Timestamp convention

Stored units differ per signal (metrics seconds, logs milli/microseconds,
traces nanoseconds) and stay exact in native columns for joins and
filtering. Every friendly surface additionally exposes human-readable
UTC alongside: ISO-8601 with millis (`start_time`) and millisecond
durations (`duration_ms`). One convention across all three signals;
native columns are never replaced, only accompanied.

## Current MVP surface (schema 1, traces)

| object | shape | source |
|---|---|---|
| `timeless_<source>_spans` | one row per span: hex `trace_id`/`span_id`/`parent_span_id`, `name`, `service`, `kind`, `status`, `start_ts` + `start_time`, `duration_ns` + `duration_ms`, verbatim `attributes` | `timeless_traces` base-table scan |
| `timeless_<source>_summary` | one row per retained trace: span/error counts, envelope timing (native plus human-readable), invalid-end count, root rows/state, service count and ordered set, `completeness` always `'unknown'` | `timeless_<source>_spans` |
| `timeless_<source>_services` | distinct retained service names, ordered | `timeless_<source>_spans` |
| `timeless_<source>_operations` | distinct retained service/operation pairs, ordered | `timeless_<source>_spans` |
| `timeless_<source>_errors` | spans-view rows with `status = 'error'` | `timeless_<source>_spans` |
| `timeless_<source>_roots` | spans-view rows with no parent | `timeless_<source>_spans` |
| `timeless_<source>_entries` | one row per log entry: native timestamp plus human-readable UTC, severity, message, verbatim typed metadata | `timeless_logs` base-table scan |
| `timeless_<source>_services` | distinct service values, ordered (logs: only when `service` is a declared index key) | spans view / logs base table |
| `timeless_<source>_fields` | declared logs index keys in declaration order | install-time configuration, no scan |
| `timeless_<source>_series` | every retained series with counts and span | `timeless_series` TVF (catalog reads only) |
| `timeless_<source>_latest` | newest sample per series with human-readable UTC; tied timestamps all returned | metrics base-table arg-max self-join |

All columns are verbatim public vtab outputs or exact formattings
thereof. No shadow tables, no lossy projections, no new evaluators.

## Object catalog

Machine-readable registry of every shipped schema object. The query
harness validates this table: object names are unique and follow the
`timeless_<source>_<kind>` convention, every `refs` identifier
resolves to a matrix row, recipe, or trace contract, and every
description is non-empty. Code-side installers are pinned by
exact-inventory-row tests to the same set — an object in either place
but not the other fails the suite. Example rows below use the canonical
`traces`/`metrics`/`logs` source names; each additional installed
source table owns the same shapes under its own derived names.

| object | kind | source tables | refs | description |
|---|---|---|---|---|
| `timeless_traces_spans` | view | `traces` | `TSQ-01` | One row per span: hex trace/span IDs, service and timing (native nanoseconds plus human-readable UTC), and verbatim attributes. |
| `timeless_traces_summary` | view | `traces` | `TSQ-04`, `TSQ-05` | One row per retained trace: span, distinct-span, and error counts; envelope start/end/duration with invalid-end handling; root rows, state, and scalar root fields only when unique; ordered service set and count; completeness always unknown. |
| `timeless_traces_services` | view | `traces` | `TSQ-05` | Distinct retained service names, ordered. |
| `timeless_traces_operations` | view | `traces` | `TSQ-05` | Distinct retained service/operation pairs, ordered. |
| `timeless_traces_errors` | view | `traces` | `TSQ-04` | Retained spans whose status is exactly error, in spans shape. |
| `timeless_traces_roots` | view | `traces` | `TSQ-04` | Retained spans with no parent, in spans shape. |
| `timeless_metrics_series` | view | `metrics` | `SQL-PROM-001`, `PQL-S04` | Every retained series: metric name, canonical labels, id, span, and point/chunk/buffer counts. Catalog reads only. |
| `timeless_metrics_latest` | view | `metrics` | `SQL-PROM-001` | Newest sample per series with human-readable UTC. Duplicate timestamps return all tied rows; full scan with documented cost. |
| `timeless_logs_entries` | view | `logs` | `SQL-LOG-001` | One row per log entry: native timestamp plus human-readable UTC, severity level, message, and verbatim typed metadata JSON. |
| `timeless_logs_services` | view | `logs` | `SQL-LOG-004` | Distinct service values retained in this table, ordered. Only installed when service is a declared index key. |
| `timeless_logs_fields` | view | `logs` | `SQL-LOG-010` | Declared index keys of this table, in declaration order: the fields that filter efficiently. |

## Query examples

Every statement below was executed against real databases through a
stock `sqlite3` shell (no extensions beyond `libtimeless_ext`). The
companion views require no special host: the signal vtabs are marked
innocuous, so views resolve under `trusted_schema=off`.

```sql
.load target/release/libtimeless_ext

-- What is installed, and at which versions.
SELECT object_name, schema_version FROM timeless_schema_inventory
 ORDER BY object_name;

-- Traces: friendly spans, then the incident summary.
SELECT trace_id, name, start_time, duration_ms
  FROM timeless_traces_spans;
SELECT trace_id, span_rows, error_rows, root_state, services,
       completeness
  FROM timeless_traces_summary;
SELECT service FROM timeless_traces_services;
SELECT service, operation FROM timeless_traces_operations;
SELECT name FROM timeless_traces_errors;
SELECT name FROM timeless_traces_roots;

-- Logs: entries with receipt-style UTC, discovery without memorized keys.
SELECT ts, ts_time, level, message FROM timeless_logs_entries;
SELECT service FROM timeless_logs_services;
SELECT field FROM timeless_logs_fields;

-- Metrics: catalog, then current values.
SELECT name, points FROM timeless_metrics_series;
SELECT name, labels, ts, value, ts_time FROM timeless_metrics_latest;
```

Notes:

- `start_time`, `ts_time`, `end_time` are ISO-8601 UTC with millis;
  the adjacent native columns stay exact for joins and filtering.
- `completeness` is always `'unknown'`; `root_state` is one of
  `missing`, `unique`, `ambiguous`, and the scalar root fields are
  populated only for `unique`.
- `timeless_metrics_latest` scans: one row per series (all tied
  duplicates on timestamp ties), ordered by name and labels.
