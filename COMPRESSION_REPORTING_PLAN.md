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
- `compression_input_bytes_total` re-accrues on every optimize/merge
  recompression, so it slowly overstates raw on a long-lived database
  (the optimize inputs are broken out and can be subtracted, but a
  dedicated ingest counter is cleaner).

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

- [ ] metrics: count 16 B per accepted point.
- [ ] logs: ts+level+message+metadata per accepted entry.
- [ ] traces: fixed 50 B + string fields per accepted span.
- [ ] Decide persistence (open question below) and document the choice
      in the stats key description.
- [ ] Unit coverage: ingest N known rows, assert exact counter values;
      re-run optimize, assert the counter does NOT move.

## Phase 2 — servers: export the honest series (all three planes)

For each plane, add to both the Prometheus `/metrics` exposition and the
stats JSON endpoint:

- [ ] `timeless_<signal>_storage_bytes` (logs/traces are missing it;
      metrics already has it)
- [ ] `timeless_<signal>_index_bytes`
- [ ] `timeless_<signal>_raw_ingested_bytes_total` (Phase 1 counter;
      until it lands, logs/traces may interim-derive
      `compression_input − optimize/merge inputs`, metrics
      `16 × total_points`)
- [ ] logs/traces: add `wal_bytes` gauge (metrics already exports it)
- [ ] Extend each plane's `storage_contract.rs` to assert the exposition
      keys exist and reconcile against `timeless_stats` on a seeded db.
- [ ] Plane READMEs + `docs/SERVER_API_REFERENCE.md`. ⚠ Those files have
      uncommitted local edits in flight — coordinate before touching.

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

1. **Counter persistence.** Existing compression counters are
   in-memory-since-open; a lifetime-honest raw counter would need a row
   in `*_meta`. Recommendation: persist it (one integer per signal,
   updated with the same transactions as ingest) — window-delta
   dashboards work either way, but lifetime ratios survive restarts and
   match `report` forever. If persistence is declined, document that
   ratios are since-open.
2. **Span raw convention.** Per-span resource/scope strings are counted
   per row (the denormalized shape the vtab returns, same convention the
   Victoria/Tempo family quotes). Alternative (OTLP wire batches share
   resource blocks) would shrink raw ~15-20%. Staying with per-row: it
   matches "logical rows as queried" and is defensible; note it wherever
   the ratio is documented.

## Non-goals

- No retention/rollup reporting changes.
- No new query surfaces; only additive stats keys and exposition gauges.
- demogen itself is done (this plan's ground-truth harness); only its
  README gains a pointer once Phase 2 lands.
