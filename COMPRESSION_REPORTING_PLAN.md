# Honest compression reporting, everywhere

Goal: every reporting surface — server `/metrics` expositions, stats JSON,
downstream dashboards — shows the same compression numbers the demogen
`timeless_demo('report')` shows, computed the same way. No surface may
blend WAL, freelist, or index bytes into a compression ratio again.

Context (found 2026-08-23, while building the demogen report):

- The engine already exposes the honest stored side publicly via
  `timeless_stats`: `bytes_on_disk` (data blocks only), `index_bytes`,
  `disk_points/disk_entries/disk_spans`, and for logs/traces
  `compression_input/output_bytes_total`.
- The **logs and traces planes read those counters but never export
  them**: their `/metrics` expositions emit only whole-file and freelist
  bytes (`timeless-logs-api/src/api.rs:201-213`). A dashboard scraping
  them cannot build an honest ratio — that is why the UI never had one.
- The metrics plane exports the clean stored split
  (`timeless_metrics_storage_bytes`, file/wal/freelist as separate
  gauges) but no raw side, so no ratio there either.
- `compression_input/output_bytes_total` are PERSISTENT (stored in the
  extension `_meta` since 0.6.2/0.6.3, updated in the same transaction
  as each optimize swap), and the input side accrues only first-pass
  compression of raw blocks — merges adjust the output side only. So
  the counter is already a lifetime-honest raw proxy; note it measures
  pre-codec columnar block bytes, which is close to but not exactly the
  logical-row definition below (a dedicated ingest counter remains the
  definition-exact fix). The `optimize_*_input_bytes` breakouts are
  in-memory since-open profile counters — never mix them with the
  persistent totals. (Verified in-engine 2026-08-23.)

## Invariant: the measurement definition

- **raw** = the logical rows as the public surface returns them.
  Metrics: 16 B/sample (ts + value; series identity is the amortized
  catalog). Logs: ts + level + message + metadata bytes. Spans: ids (32)
  + kind/status (2) + timings (16) + every string field.
- **stored** = engine data-block bytes on disk (`bytes_on_disk`). Only.
- **indexes** are reported *beside* the ratio, never inside it.
- **file, WAL, freelist** are operational series, each its own gauge,
  never part of a compression number.

Ground truth harness: seed a fresh db with `tools/demogen`, then any
surface's ratio must agree with `SELECT timeless_demo('report')` on the
same db. If a dashboard disagrees, the dashboard is wrong.

## Phase 1 — engine: ingest-raw counters (additive stats keys)

Add per-signal `ingest_raw_bytes_total` to `timeless_stats`, counted at
ingest (both row-at-a-time and batch paths) using the definition above.
Purely additive stats keys: no data-ABI or SQL-surface bump.

- [x] metrics: RESOLVED as no-counter-needed — `16 × total_points` from
      durable point counts IS the definition, exactly, so the Phase 2
      derivation is already lifetime-accurate. Nothing to add.
- [ ] logs: ts+level+message+metadata per entry, accrued when entries
      become durable (same transaction as the block write).
- [ ] traces: fixed 50 B + string fields per span, same accrual point.
- [ ] Persistence: follow the `compression_totals` `_meta` pattern
      (decided; see resolved open question 1).
- [ ] Unit coverage: ingest N known rows, assert exact counter values;
      optimize/prune must not move it; reopen must preserve it.

## Phase 2 — servers: export the honest series (all three planes)

For each plane, add to both the Prometheus `/metrics` exposition and the
stats JSON endpoint:

- [x] `timeless_<signal>_storage_bytes` (all three planes)
- [x] `timeless_<signal>_index_bytes` (all three planes; logs/traces
      keep their pre-existing `*_index_size_bytes` aliases for scraper
      compatibility — candidates for later deprecation)
- [x] `timeless_<signal>_raw_ingested_bytes_total` (interim: logs and
      traces use `compression_input_bytes_total` directly — persistent,
      first-pass-only, no subtraction; metrics uses `16 × total_points`,
      lifetime-accurate from durable counts)
- [x] logs/traces: compression input/output counters exported;
      `wal_bytes` present on all three planes
- [x] Each plane's `storage_contract.rs` asserts the exposition keys and
      reconciles exactly against `timeless_stats` (traces suite: 9 tests;
      logs gained its first storage_contract.rs; metrics: 95).
- [x] Plane READMEs done; `docs/SERVER_API_REFERENCE.md` documents the
      series and the ratio rules ("Storage and compression series").

## Phase 3 — downstream dashboards (separate repos)

The panel contract, for the Elixir metrics/logs/traces apps and any
Grafana boards:

- [ ] Compression panel: `raw_ingested_bytes` vs `storage_bytes`
      (window-delta ratio, not lifetime, so restarts and pre-counter
      history do not skew it).
- [ ] Storage panel: stored / indexes / wal / freelist as separate
      stacked series — bloat becomes visible instead of blended.
- [ ] Remove any panel that divides by `database_file_bytes`.
- [ ] Acceptance: point each dashboard at a demogen-seeded plane and
      match `timeless_demo('report')`.

## Phase 4 — ops guardrails (docs)

- [x] Deployment note: `PRAGMA auto_vacuum=INCREMENTAL` must run before
      the first write to a fresh database (even the WAL switch writes
      page 1; set late it silently no-ops) — otherwise freelist
      accumulates forever. Include periodic `incremental_vacuum`
      guidance. (GUIDE §8, "Keeping the file size honest".)
- [x] GUIDE section: how to read the storage series; what is and is not
      in a compression number. (Same section; server gauge names get a
      pointer once Phase 2 lands.)

## Open questions

1. **Counter persistence.** Partially answered: the engine already
   persists `compression_*_bytes_total` in `_meta` (the
   `compression_totals` row). The Phase 1 `ingest_raw_bytes_total`
   counter should follow that exact pattern — one integer per signal,
   updated in the same transactions — so lifetime ratios survive
   restarts and match `report` forever.
2. **Span raw convention. RESOLVED 2026-08-23:** per-row counting stays
   (owner's call — conform where easy, and per-row matches "logical
   rows as queried" plus the Victoria/Tempo-family convention). Already
   implemented and documented everywhere.

## Non-goals

- No retention/rollup reporting changes.
- No new query surfaces; only additive stats keys and exposition gauges.
- demogen itself is done (this plan's ground-truth harness); only its
  README gains a pointer once Phase 2 lands.
