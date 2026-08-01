# Standalone SQLite query API completion plan

Status: Sessions 0–5 complete on `feat/standalone-query-api`; optional
TimelessMetrics adoption remains deferred until the Rust telemetry data-plane
POC decision.

This plan finishes the query interfaces that make `timeless-libsql` more useful
as a TSDB loaded directly into SQLite, libSQL, or `sqld`. Faster
TimelessMetrics queries are welcome, but they are not the reason to publish an
API in this phase.

Implementation should happen on a fresh `timeless-libsql` branch. The later
Rust data-plane POC remains a separate TimelessMetrics branch and may consume
these interfaces without owning them.

## Objective

Give direct SQL users two capabilities that the storage engine already has but
the public extension does not expose completely:

1. Reuse durable `series_id` values as relational query handles instead of
   resolving the same metric and matcher set for every operation.
2. Fetch high-cardinality aggregate and latest results in versioned, packed
   frames as well as ordinary SQL rows.

These are standalone extension features. They must be usable and tested from
the SQLite CLI without Elixir, Exqlite, a NIF, or TimelessMetrics.

## Product boundary

This phase belongs in `timeless-libsql`:

- `series_id` equality pushdown and joins against `timeless_series`
- chunk-aware raw, aggregate, latest, grid, window, and rollup reads selected
  by a durable series handle
- one-row aggregate and latest result frames for embedded and remote hosts
- callback-safe batch primitives in `timeless-core`
- versioned binary contracts, direct SQL documentation, and decoder examples

This phase does **not** include:

- an Exqlite-specific row decoder or a fused SQLite-to-BEAM NIF
- BEAM reader-pool scheduling or separate small/wide query lanes
- PromQL planning, staleness, extrapolation, or vector matching
- a filter-plan cache added only to improve a benchmark
- changing the TimelessMetrics default engine
- starting the Rust telemetry server POC

The first two excluded items may still be valuable, but they belong at the
later Rust data-plane boundary and provide no direct SQLite API.

## Public SQL contracts

The exact planner bit layout is internal. The SQL behavior below is the public
contract.

### Durable series handles

`timeless_series` already exposes the durable catalog ID:

```sql
SELECT series_id, name, labels
FROM timeless_series('metrics', 'cpu_usage', '{"env":"prod"}');
```

Add equality pushdown for the visible `series_id` column on the existing query
TVFs. A direct caller can resolve once and reuse the handle:

```sql
SELECT series_id, ts, value
FROM timeless_raw('metrics', 'cpu_usage', NULL, :start, :stop)
WHERE series_id = :series_id;

SELECT series_id, value
FROM timeless_aggregate(
  'metrics', 'cpu_usage', NULL, :start, :stop, 'avg'
)
WHERE series_id = :series_id;

SELECT series_id, ts, value
FROM timeless_latest('metrics', 'cpu_usage', NULL, :start, :stop)
WHERE series_id = :series_id;
```

The constraint is an intersection, not an override. The ID must belong to the
named table, metric, and matcher selection. A missing ID or a mismatch returns
no rows. This preserves the meaning of every existing call while allowing the
extension to skip broad candidate enumeration.

Apply the same rule to every per-series metrics TVF that exposes `series_id`:

- `timeless_series`
- `timeless_raw`
- `timeless_raw_batches`
- `timeless_aggregate`
- `timeless_latest`
- `timeless_grid`
- `timeless_window`
- `timeless_window_batches`
- `timeless_rollup`
- `timeless_rollup_batches`

Also push `series_id = ?` into the base `timeless_metrics` virtual table. Its
hidden `series_id` column is already a documented write handle; it should be a
read handle too:

```sql
SELECT ts, value
FROM metrics
WHERE series_id = :series_id
  AND ts >= :start
  AND ts <= :stop;
```

Do not add a family of `*_by_id` modules unless the Session 0 planner spike
proves that SQLite cannot reliably pass an output-column equality constraint
to these eponymous virtual tables. Constraint pushdown is preferred because it
composes naturally with joins and avoids duplicating the whole query surface.

The initial contract supports one equality constraint. `IN (...)` and an
arbitrary packed ID-list argument are deferred until a real direct-user case
justifies their additional planner and compatibility surface.

### Relational composition

The intended advanced use is a catalog-driven join:

```sql
SELECT s.labels, q.ts, q.value
FROM timeless_series(
       'metrics', 'cpu_usage', '{"env":"prod"}'
     ) AS s
JOIN timeless_latest(
       'metrics', 'cpu_usage', NULL, :start, :stop
     ) AS q
  ON q.series_id = s.series_id;
```

Session 0 must confirm with `EXPLAIN QUERY PLAN` and engine counters that
SQLite chooses a parameterized inner scan rather than materializing every
series. If planner behavior cannot be made stable, document the limitation and
fall back to explicit `*_by_id` TVFs.

### Packed aggregate frame

Keep `timeless_aggregate` as the ordinary row-oriented SQL interface and add a
one-row companion for host-language and remote boundaries:

```sql
SELECT frame
FROM timeless_aggregate_frame(
  'metrics', 'cpu_usage', '{"env":"prod"}',
  :start, :stop, 'avg'
);
```

The frozen `TAF1` envelope is columnar and self-describing:

```text
"TAF1" | aggregate_kind:u8 | flags:u8=0 | reserved:u16=0 |
series_count:u32 | series_ids:i64[series_count] |
validity_bitmap:u8[ceil(series_count/8)] |
value_words:u64[series_count]
```

- Empty series are omitted.
- Series order is unspecified, as with SQL rows without `ORDER BY`.
- `count` words represent exact integer counts and retain the row interface's
  SQLite INTEGER range contract.
- Other words contain IEEE-754 value bits.
- The validity bitmap represents the row interface's SQL `NULL` behavior,
  including propagated NaN results, without relying on a host's NaN coercion.
- Unknown magic, flags, reserved bits, kind, inconsistent counts, and size
  overflow fail loudly.
- Labels are not repeated in the frame; callers attach them by `series_id`
  through `timeless_series`.

This layout is now a compatibility contract. Version, flags, reserved bytes,
lengths, bitmap padding, NULL words, NaNs marked valid, and count overflow are
all validated strictly by the public decoder.

### Packed latest frame

Keep `timeless_latest` for relational SQL and add the matching high-cardinality
transport:

```sql
SELECT frame
FROM timeless_latest_frame(
  'metrics', 'cpu_usage', '{"env":"prod"}', :start, :stop
);
```

The frozen `TLF1` envelope is:

```text
"TLF1" | series_count:u32 |
series_ids:i64[series_count] | timestamps:i64[series_count] |
validity_bitmap:u8[ceil(series_count/8)] |
value_bits:u64[series_count]
```

Only series with a point in the inclusive range are present. The duplicate
timestamp winner must be identical to `timeless_latest`. The validity bitmap
pins SQL `NULL`/NaN behavior consistently across SQLite host bindings.

## Sessions

- [x] Session 0: freeze contracts and prove SQLite planner behavior
- [x] Session 1: make `series_id` a first-class read constraint
- [x] Session 2: add callback-safe aggregate/latest batch primitives
- [x] Session 3: ship `timeless_aggregate_frame`
- [x] Session 4: ship `timeless_latest_frame`
- [x] Session 5: direct-user hardening, documentation, and release gate
- [x] Session 6: optional TimelessMetrics adoption

Sessions 0 through 5 belong entirely to `timeless-libsql`. Session 6 is
optional integration work and must not determine whether the extension APIs
are sound.

### Implementation record

- Starting revision: `650fe13dc8ee2766c2d9d2aa60586b3091b71435`.
- Planner decision: keep the existing TVFs. Visible/output-column equality
  constraints are reliable for literals, bound parameters, and parameterized
  catalog joins, so no duplicate `*_by_id` family was added.
- Planner encoding: hidden function arguments retain their existing low-bit
  mask; the internal high bit marks the appended `series_id` argument.
  Unusable output constraints remain legal broad outer-loop plans, preventing
  dependency cycles when two series-aware virtual tables are joined.
- SQLite INTEGER affinity is reproduced when the virtual table omits the
  original equality predicate: integral INTEGER, REAL, and numeric TEXT values
  select the same ID; NULL, BLOB, malformed, non-integral, and out-of-range
  values match nothing.
- `TAF1` and `TLF1` are frozen exactly as documented above. Strict public Rust
  decoders and a dependency-free Python decoder consume pinned byte fixtures.
- Aggregate/latest row TVFs and frames share callback-safe core batch
  primitives with one transition guard, one index snapshot, ordered batch
  payload reads, metadata fast paths, and stable input-ID order.
- The 12,000-series run measured 4.452 ms median / 4.994 ms p95 and 193,512
  returned bytes for `TAF1`; `TLF1` measured 4.438 ms / 5.040 ms and 289,508
  bytes. Full results and source-payload characterization are in
  [`tools/bench/results/2026-08-01_standalone_query_api.md`](tools/bench/results/2026-08-01_standalone_query_api.md).
- Release validation passed the full Rust workspace, all 41 CLI sections
  (including randomized/crash/recovery suites), Python decoder checks, and a
  native libSQL `0.9.30` local connection exercising a bound ID lookup,
  catalog join, `TAF1`, and `TLF1`.
- Session 6 is complete on the TimelessMetrics
  `feat/standalone-query-api-adoption` branch. Cached exact raw, aggregate, and
  latest reads use the `series_id` constraint; scalar aggregate and wide latest
  use TAF1/TLF1 when detected through `pragma_module_list`, with the original
  row statements retained as compatibility fallbacks.
- At 12,000 series, the public TimelessMetrics scalar aggregate fell from
  38.833ms to 12.695ms median and latest fell from 37.488ms to 12.332ms in a
  controlled same-code row-versus-frame comparison. TAF1 reduced sampled peak
  process memory from 13.33MB to 9.21MB; TLF1 increased it from 8.24MB to
  10.78MB despite reducing transport bytes, so that tradeoff is recorded
  explicitly rather than treated as a universal allocation win.
- The adapter added strict dirty-CPU NIF decoders and semantic tests comparing
  frame and row routes, selected-ID reads against label supersets, malformed
  frames, restart/reopen, and the existing read barrier. No extension contract
  was changed for an Elixir-only result shape.

### Session 0: contracts and planner spike

Deliverables:

- Record starting revisions and extend the direct query benchmark with chunk
  reads, selected-series count, SQLite rows, bytes returned, median, and p95.
- Prototype a `series_id = ?` constraint on one throwaway or test-only TVF.
- Verify literal, bound-parameter, and correlated-join plans with
  `EXPLAIN QUERY PLAN`.
- Confirm the constraint remains usable with every required hidden argument
  bound through function-call syntax.
- Decide whether the existing TVFs or explicit `*_by_id` companions provide a
  reliable public contract. Record the decision in this file before shipping.
- Freeze `TAF1` and `TLF1`, including NaN/NULL, integer count, empty result,
  maximum size, malformed input, and future-version behavior.
- Add tiny Rust decoder fixtures before publishing either envelope.

Exit gate:

- The selected-ID plan reads no unrelated series or chunks.
- A catalog-driven join is deterministic across the supported SQLite and
  libSQL versions, or the plan explicitly chooses companion TVFs instead.
- Row/frame semantic oracles and exact byte fixtures are written down.

Performance numbers are diagnostic only. There is no Rust-engine parity gate.

### Session 1: first-class `series_id` reads

Implement a shared `best_index`/`xFilter` path rather than nine slightly
different copies.

Requirements:

- Push one usable `series_id = ?` constraint into every per-series metrics TVF
  listed above and into the base metrics virtual table.
- Intersect the ID with table, metric, and matcher arguments before chunk
  reads; never let an ID bypass caller-visible selection semantics.
- Fetch catalog metadata once for the selected ID and serialize labels only if
  SQLite projects them.
- Set realistic estimated cost and row count so catalog-driven joins choose
  the ID lookup as the inner plan.
- Preserve all existing calls and row ordering contracts.
- Treat a nonexistent, pruned, wrong-table, wrong-metric, or matcher-mismatched
  ID as an empty result, not as an error or a cross-table lookup.
- Pin SQLite coercion behavior for NULL, REAL, TEXT, negative, and overflowing
  ID values.

Tests:

- direct CLI queries for every affected TVF
- literal, prepared-parameter, and correlated-join selection
- equality plus conflicting metric/filter predicates
- empty/missing IDs, restart, rollback, retention, and two connections
- `EXPLAIN QUERY PLAN` assertions where stable and engine counters everywhere
- differential equality against each TVF's existing metric/matcher result

Exit gate:

- The public SQL corpus proves the feature without TimelessMetrics.
- One-ID queries perform zero unrelated chunk reads.
- Full workspace and CLI suites pass without changing old expected output.

### Session 2: callback-safe batch primitives

Add core primitives analogous to `query_range_batch_by_id`:

- `query_aggregate_summary_batch_by_id`
- `query_latest_batch_by_id`

Each primitive must hold one transition guard, snapshot the relevant chunk
index once, use ordered `read_chunks` calls where that preserves the existing
algorithm, include buffered points, and retain input ID order including empty
entries.

The goal is a reusable engine operation, not parallelism inside a SQLite
callback. Do not use Rayon while the callback owns the host connection.

Tests:

- batch output versus the existing by-ID oracle for randomized IDs and bounds
- duplicates and repeated IDs in the input list
- partial/full chunks, buffered data, NaNs, duplicate timestamps, and empty
  ranges
- restart, rollback, compaction, retention, and mixed persisted/buffered state
- injected store errors and mismatched batch-read lengths

Exit gate:

- Batch and repeated by-ID calls are semantically identical.
- The APIs are independently usable by future Rust callers.
- No callback re-entrancy or transaction-visibility behavior changes.

### Session 3: aggregate frame

Deliverables:

- Register `timeless_aggregate_frame` as an additive eponymous-only TVF.
- Reuse the exact matcher compiler and catalog visibility rules of
  `timeless_aggregate`.
- Execute the Session 2 batch summary primitive and encode one checked `TAF1`
  result.
- Keep the existing row TVF unchanged and prove decoded frame parity for
  `avg`, `sum`, `min`, `max`, and `count`.
- Provide small Rust and Python decoder examples; document that application
  code must reject unknown versions rather than guessing.

Tests:

- exact byte fixture and malformed/truncated/overflow envelopes
- all aggregates, empty matches/ranges, NaNs, negative timestamps, and restart
- transaction commit/rollback and two-connection visibility
- row/frame parity with shuffled catalog insertion order
- direct SQLite/libSQL CLI test independent of TimelessMetrics

Exit gate:

- The frame is a documented public compatibility contract.
- A 12K-series query crosses SQLite once and returns the same logical values as
  the row TVF.
- No raw point is materialized above the extension.

### Session 4: latest frame

Deliverables:

- Register `timeless_latest_frame` with the same arguments and inclusive bounds
  as `timeless_latest`.
- Reuse the Session 2 latest batch primitive and encode one checked `TLF1`
  result.
- Preserve newest-point selection and stable duplicate-timestamp tie behavior.
- Add Rust and Python decoder examples and cross-language fixtures.

Tests and exit gate mirror Session 3, with additional cases for bounded latest,
persisted newest-value metadata, old chunks without that metadata, duplicate
maximum timestamps, and buffered-versus-persisted ties.

### Session 5: standalone hardening and release gate

Deliverables:

- Update `README.md`, `docs/GUIDE.md`, and `docs/QUERIES.md` with ordinary SQL,
  prepared-statement, and catalog-join examples.
- Document when to choose rows, per-series batches, or a whole-result frame.
- Document series-ID lifetime, table scope, backup/reopen behavior, and the
  fact that IDs are not portable between independently created databases.
- Add a feature-detection recipe using SQLite's module catalog rather than
  requiring a version string comparison.
- Exercise the APIs through the loadable extension, bundled rusqlite tests,
  libSQL, and `sqld` where the repository's existing harness supports them.
- Run workspace tests, CLI tests, randomized oracles, transaction/recovery
  tests, and the direct query benchmark.
- Save final measurements as API characterization, not a release promise.

Release gate:

- Every new feature is usable without TimelessMetrics.
- Old SQL APIs and on-disk/replication-visible formats are unchanged.
- New envelopes have checked arithmetic, malformed-input tests, and at least
  two independent decoders consuming the same fixtures.
- Multi-connection visibility, rollback, reopen, backup, compaction, and
  retention tests pass.
- Documentation explains the relational row interface before the packed
  interface.

### Session 6: optional TimelessMetrics adoption

Only after Sessions 0 through 5 are complete:

- Use the `series_id` constraint for cached exact raw, exact aggregate, and
  exact-latest reads where it simplifies the adapter.
- Evaluate `TAF1` for multi-series scalar aggregation and `TLF1` for wide latest
  reads.
- Keep the old row statements as compatibility fallbacks while the pinned
  extension version is optional.
- Re-run semantic, allocation, and latency tests, but do not change either
  public extension contract to satisfy an Elixir-only result shape.

This session may be skipped entirely if the adapter becomes throwaway work for
the imminent Rust data-plane POC. The direct extension is complete at the end
of Session 5.

## Per-session working agreement

Every implementation session must:

1. Start from a clean tree and record the starting revision.
2. Add the direct SQL oracle before making the optimized route the default.
3. Preserve transaction visibility and the writer/read gate.
4. Test a second connection and reopen whenever catalog or chunks are read.
5. Use versioned formats, checked arithmetic, and loud malformed-data errors.
6. Measure chunks read and bytes returned in addition to time.
7. Run formatting, workspace tests, CLI tests, and a diff review.
8. Update the completed checkbox, measurements, and remaining risks here.

## Stop conditions

- Do not publish an API whose only consumer is Exqlite or a TimelessMetrics NIF.
- Do not move PromQL policy into the extension.
- Do not duplicate all query TVFs with `*_by_id` names unless Session 0 proves
  output-column constraint pushdown unreliable.
- Do not add `IN` or arbitrary ID-list syntax before single-ID relational
  composition is correct and documented.
- Do not remove row-oriented SQL in favor of opaque blobs.
- Do not change the shadow schema or replication-visible formats for a query
  envelope.
- Do not trade read-after-write correctness for a lower p95.
- Do not let this work grow into the Rust telemetry process POC; finish and
  release the standalone extension surface first.
