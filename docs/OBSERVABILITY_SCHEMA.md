# Timeless observability schema

Friendly query surface over the telemetry virtual tables (#20), in the
spirit of MySQL Performance Schema: stable output shapes for ordinary
SQL users, no expert recipes required. This document states the
lifecycle and compatibility policy (#21). Per-signal objects and query
examples grow here as phases land; the current MVP is traces-only.

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
- Upgrades are per-object `DROP` + `CREATE` at the recorded version
  boundary — migrations in the web-framework sense, executed
  automatically on a writable open. There is no separate upgrade
  command to forget.
- Downgrade direction: an older binary leaves newer-versioned objects
  alone and keeps serving the base tables. Newer shapes may error when
  queried (unknown columns), which is loud, not wrong.

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

All columns are verbatim public vtab outputs or exact formattings
thereof. No shadow tables, no lossy projections, no new evaluators.
