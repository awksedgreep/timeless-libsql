# Metrics read/query performance plan

This plan improves the reusable `timeless-libsql` metrics query surface first,
then makes `timeless_metrics` a thin planner and result adapter over it. Work is
split into sessions that can be implemented, reviewed, and benchmarked
independently.

## Objective

Close the read-performance gap with the filesystem Rust block engine without
moving PromQL policy into the storage layer or weakening correctness. Direct
SQLite, libSQL, and `sqld` users must benefit from every new kernel; the Elixir
integration must use the same public SQL interfaces available to everyone else.

The final release gate remains the one in the TimelessMetrics storage migration
plan: representative query p95 must be within 20% of the Rust engine before
`:libsql` becomes the default.

## Fixed boundary

`timeless-libsql` owns:

- series selection and safe label-filter pushdown
- chunk pruning, decompression, ordering, and duplicate preservation
- explicitly requested mechanical reductions
- raw, grid, window, latest, scalar-aggregate, and rollup result encodings
- transaction visibility and multi-connection catalog coherence

The caller owns:

- PromQL lookback defaults, staleness, extrapolation, and counter policy
- cross-series PromQL operators and vector matching
- application transforms and alert semantics
- public API result shaping and fallback selection

Every accelerated query must retain a raw fallback. A kernel is eligible only
when its timestamp bounds, empty-series behavior, ordering, floating-point math,
and label semantics can be pinned independently of PromQL.

## Baseline: 2026-07-31

All measurements below used the current paired branches, disk-backed stores,
22 BEAM schedulers, sequential engine runs, and the same host.

### Embedded write/read benchmark

The large workload populated 10K series with 10M points, then ran 10-second
steady phases. The steady batch size was 100 points.

| Measurement | Rust blocks | libSQL | Current gap |
|---|---:|---:|---:|
| Cold population, 10K-point calls | 978K points/s | 1.28M points/s | libSQL 1.31x faster |
| Single-point writes | 516K points/s | 109K points/s | Rust 4.73x faster |
| Single writes, 22 producers | 431K points/s | 127K points/s | Rust 3.39x faster |
| 100-point batches | 1.21M points/s | 1.41M points/s | libSQL 1.17x faster |
| Flush and compression | 3.287s | 1.102s | libSQL 2.98x faster |
| Storage | 2.350 bytes/point | 1.586 bytes/point | libSQL 32.5% smaller |
| First 1,003-point read after flush | under 1ms | 53ms | catalog-refresh gap |
| Warm 1,003-point read p95, 100 runs | 55us | 217us | Rust 3.95x faster |

### Public-API fan-out benchmark

This workload used one metric, 12K series, four labels per series, 60 points per
series, and 720K total points. Every timed query was warmed first.

| Query | Result | Rust median | libSQL median | Current gap |
|---|---:|---:|---:|---:|
| Exact series | 60 points | 0.130ms | 0.287ms | Rust 2.21x faster |
| Narrow fan-out | 188 series / 11,280 points | 1.041ms | 3.728ms | Rust 3.58x faster |
| Full raw fan-out | 12K series / 720K points | 43.651ms | 372.787ms | Rust 8.54x faster |
| Scalar average | 12K values from 720K points | 28.852ms | 449.270ms | Rust 15.57x faster |

These numbers are the comparison baseline, not permanent benchmark claims.
Record CPU governor, build profile, SQLite version, filesystem, dataset seed,
and raw samples in every saved result.

## Principal causes to verify

1. `query_aggregate_multi` materializes every raw sample in Elixir even though
   `timeless-core` already has chunk-aware scalar aggregation.
2. `latest` and `latest_multi` read complete histories and select the newest
   point above SQLite.
3. A daily rollup read executes `timeless_rollup` six times and repeatedly
   decodes the same labels and buckets.
4. Wide raw reads cross Rust -> SQLite -> Exqlite -> Elixir as one blob per
   series, then allocate one tuple per point. The output is inherently large,
   but copies and repeated label work may not be.
5. The first reader after a large flush refreshes authoritative catalog state.
   The generation fast path helps later queries but not this publication event.
6. Every read currently performs a writer GenServer barrier. It is required for
   read-after-write correctness, but its steady-state cost has not been isolated.
7. TimelessMetrics pushes down only safe, non-empty equality matchers. That is
   correct today; broader pushdown requires dialect and duplicate-matcher parity.

Do not optimize based on this list alone. Session 0 must attribute time and
allocation before changing an API.

## Target public SQL surface

The names and schemas below are provisional until Session 0 records an API
decision. Once documented and released, they are compatibility contracts.

### Scalar aggregate

```sql
SELECT series_id, labels, value
FROM timeless_aggregate(
  'metrics', 'cpu_usage', '{"env":"prod"}', :start, :stop, 'avg');
```

- Operations: `avg`, `sum`, `min`, `max`, and `count` initially.
- Bounds use the same inclusive raw-range contract as `timeless_raw`.
- Empty series produce no row.
- Use chunk metadata for fully covered chunks and decode boundary chunks only.
- Pin and document accumulation order, NaN handling, and comparison rules before
  publishing the API; do not substitute SQLite `AVG()` silently.

### Latest point

```sql
SELECT series_id, labels, ts, value
FROM timeless_latest(
  'metrics', 'cpu_usage', '{"env":"prod"}', :start, :stop);
```

- Return at most one row per matched series.
- Select the greatest timestamp in the inclusive range.
- Pin duplicate-timestamp tie behavior with an oracle before implementation.
- Search newest candidate chunks first; do not decode complete histories.

### Packed rollup rows

```sql
SELECT series_id, labels, buckets
FROM timeless_rollup_batches(
  'metrics', 'cpu_usage', '{"room":"lab"}', 86400, :start, :stop);
```

- Return every stored field (`avg`, `min`, `max`, `count`, `sum`, `last`) in one
  versioned, little-endian columnar blob per series.
- Keep the row-oriented `timeless_rollup` interface for compatibility.
- Document byte layout, size limits, malformed-data errors, and forward-version
  behavior beside `timeless_raw_batches`.

### Existing grid and window kernels

`timeless_grid` and `timeless_window` remain the public mechanical kernels.
TimelessMetrics may use them only where a differential oracle proves their grid,
window, fill, timestamp, and aggregation behavior matches its requested API.
PromQL counter extrapolation and staleness remain above the boundary.

## Sessions

- [x] Session 0: reproducible benchmark and attribution harness
- [x] Session 1: native scalar aggregates
- [x] Session 2: native latest queries
- [x] Session 3: bucketed query-kernel adoption
- [x] Session 4: bundled rollup reads
- [x] Session 5: wide raw transport
- [x] Session 6: catalog publication and first-query latency
- [x] Session 7: matcher and discovery pushdown
- [x] Session 8: integration, soak, and default-engine decision

Session 0 is mandatory before optimization. Sessions 1 and 2 are the first
implementation priorities. Sessions 3 through 7 may be reordered only when the
attribution data identifies a higher-impact dependency; record the reason here.

### Session 0: reproducible benchmark and attribution harness

Deliverables:

- Add a direct-extension benchmark covering exact raw, narrow fan-out, full
  fan-out, scalar aggregate, latest, grid, window, rollup, and first query after
  publication/reopen.
- Extend the TimelessMetrics public-API benchmark so `:rust` and `:libsql` run
  identical datasets and query shapes from one script.
- Capture median, p95, min/max, returned rows/points, allocations where
  practical, database bytes, and the unrounded first-query latency.
- Add opt-in spans/counters for catalog refresh, candidate selection, chunk
  reads, decompression, kernel execution, blob packing, SQLite stepping, label
  JSON decoding, point decoding, and BEAM result shaping.
- Measure the read barrier separately with zero pending writes, one pending
  transaction, and concurrent readers.
- Save the baseline result in each repository with exact commands and revision
  identifiers.

Exit gate:

- At least 90% of full-fan-out and aggregate wall time is attributed to named
  stages, or the unaccounted portion is explicitly recorded as SQLite/FFI cost.
- Repeated-process variance is reported; no optimization phase starts from a
  single-run anecdote.

#### Session 0 progress: 2026-07-31

Implemented the first attribution slice:

- `tools/bench/src/bin/query_read.rs` now drives the public loadable-extension
  SQL surface directly with a deterministic 12K-series/720K-point dataset.
- `timeless_metrics/bench/engine_query_bench.exs` runs the same public API
  workload against either engine and isolates the no-pending barrier, prepared
  SQLite fetch, packed decode, scalar reduction, result sort, and the combined
  fetch/decode/shape path.
- Exact commands, revisions, machine state, raw samples, database bytes, result
  size, and fresh-process ranges are saved in
  `tools/bench/results/2026-07-31_query_read_baseline.md` and the paired
  TimelessMetrics benchmark record.

The combined fixed-reader fetch/decode/shape median is 373.073ms versus
372.787ms through the public libSQL API, accounting for effectively all of the
wide-read wall time. The writer barrier is 9us median with no pending writes.
The direct extension returns and scans the same 720K packed points in 99.975ms;
the remaining cost is dominated by the Exqlite/BEAM boundary and eager tuple
materialization, including nonlinear allocation/GC cost while fetched blobs
and decoded tuples coexist. This validates native aggregate and latest kernels
as the first implementation work: they prevent most raw points from crossing
that boundary.

Session 0 remains open. Still required are cumulative allocation counters,
extension-internal spans, reopen timing, pending-write and concurrent-reader
barrier cases, and a controlled-governor repeat run. The current `powersave`
governor produced a 43.651-71.483ms fresh-process range for the Rust wide-read
median, so relative release claims must wait for that controlled run.

### Session 1: native scalar aggregates

Implement `timeless_aggregate` over the existing `timeless-core` scalar
aggregation machinery, promoting a narrow callback-safe by-series primitive as
needed, with matcher filtering before chunk reads. Avoid Rayon inside SQLite
callbacks; parallelism belongs across independent connections or in
callback-safe code only.

Tests:

- Naive point-scan oracle for every aggregate, partial chunk bounds, empty
  ranges, out-of-order writes, duplicates, negative timestamps, and restart.
- Transaction commit/rollback and two-connection visibility.
- CLI coverage proving the API works for users without TimelessMetrics.
- Bit-exact comparison for count/min/max and any floating operation whose pinned
  contract is bit-exact; otherwise record and test the explicit ULP/tolerance
  contract for sum/avg.

TimelessMetrics integration:

- Route scalar `query_aggregate` and `query_aggregate_multi` operations to the
  TVF when the operation is supported.
- Retain raw fallback for `first`, `last`, `rate`, transforms requiring raw
  points, and any semantic mismatch.
- Prepare one persistent aggregate statement per reader connection.

Exit gate:

- Direct aggregate is at least 5x faster than materializing the 720K-point raw
  fallback.
- TimelessMetrics' 12K-series scalar average is no more than 2x the Rust-engine
  median, with identical results.
- Exact and raw-query performance does not regress by more than 5%.

Completed 2026-07-31. `timeless_aggregate` exposes `avg`, `sum`, `min`,
`max`, and integer-exact `count` through public SQL. It uses a new sequential,
callback-safe per-series `AggregateSummary`; fully covered chunks use metadata
and boundary chunks decode. Direct SQL tests pin inclusive bounds, empty
omission, duplicates, negative timestamps, buffered data, rollback, two
connections, reopen, matcher behavior, argument errors, and the NaN contract.
Sum/avg use the documented chunk-local accumulation order; comparisons to a
flat scan allow normal floating-point rounding rather than promising a false
bit-exact contract.

The direct 720K-point average fell from 96.995-109.284ms to
14.133-16.180ms across three fresh processes (6.38-6.86x). The public
TimelessMetrics result fell from 449.270ms to 34.659-36.558ms (12.29-12.96x)
and is 1.32-1.39x the latest 26.346ms Rust median. Exact raw remained
0.269-0.277ms versus the 0.287ms baseline. The adapter prepares the aggregate
statement per reader, selects only series id/value, reuses immutable labels
from ETS, and keeps callers sticky to a prepared reader while distributing
independent processes. Full results are in
`tools/bench/results/2026-07-31_native_aggregate.md` and the paired
TimelessMetrics result record.

### Session 2: native latest queries

Add an engine primitive and `timeless_latest` TVF that walks chunk metadata from
newest to oldest and stops as soon as the winning point is known. Include
buffered data and preserve transaction visibility.

Tests:

- Empty series/ranges, buffered-only points, newest point on a chunk boundary,
  out-of-order points, duplicate timestamps, compaction, reopen, and retention.
- Matcher oracle and multi-connection publication tests.

TimelessMetrics integration:

- Replace full-history implementations of `latest` and `latest_multi`.
- Keep output ordering and omission behavior identical to the Rust engine.

Exit gate:

- At least 10x faster than raw-history materialization on a long-history
  dataset.
- Public-API median no more than 2x Rust for exact and 12K-series latest reads.

Completed 2026-07-31. `timeless_latest` now uses a sequential callback-safe
per-series primitive that ranks candidate chunks by their newest possible
timestamp, stops once older chunks cannot affect the winner, and includes the
live buffer. Duplicate maximum timestamps retain the first point in stable raw
engine order. New SQLite chunks persist that point as nullable metadata, so
normal unbounded latest reads avoid PCO decompression; legacy databases are
upgraded additively and old chunks retain the exact decode fallback until
compaction.

Direct SQL tests cover buffered data, rollback, inclusive and reversed bounds,
out-of-order and duplicate writes across flushes, matchers, empty omission,
compaction, retention, cross-connection publication, reopen, and legacy-schema
migration. TimelessMetrics prepares one latest statement per reader, projects
only series id/timestamp/value, reuses the immutable ETS label cache, and no
longer materializes raw histories in `latest` or `latest_multi`.

On the long-history 2K-series × 600-point workload, direct latest fell from
27.659ms to 1.858ms (14.89x). On the standard 12K × 60 public workload,
TimelessMetrics libSQL latest fell from the 408-499ms baseline to
35.782-39.285ms and beat the matching Rust engine's 50.708ms median by
1.29-1.42x. Exact latest measured 0.248ms versus Rust's 0.128ms (1.94x).
Full results are in `tools/bench/results/2026-07-31_native_latest.md` and the
paired TimelessMetrics result record.

### Session 3: bucketed query-kernel adoption

Build a differential matrix between TimelessMetrics aggregation buckets and
`timeless_window`/`timeless_grid` covering alignment, inclusive/exclusive bounds,
empty windows, fill mode, sparse data, duplicate timestamps, and transforms.

- Bind only proven-equivalent `avg`, `sum`, `min`, `max`, and `count` cases.
- Keep `rate`, `increase`, PromQL extrapolation, staleness, and unsupported
  transforms on the semantic layer unless a mechanical sub-result can be used
  safely.
- Add packed grid/window variants only if Session 0 shows SQLite row crossings
  are material; do not add formats speculatively.

Exit gate:

- VictoriaMetrics differential corpus remains 182/182.
- Supported bucketed workloads improve at least 3x over raw materialization.
- Unsupported cases demonstrably take the raw fallback.

Completed 2026-07-31. TimelessMetrics' inclusive buckets aligned to `from`
are exactly the kernel's half-open `(t - width, t]` windows when evaluated at
`bucket_start + width - 1`; the adapter uses that mapping only when the entire
inclusive range contains complete buckets. Partial terminal buckets,
`first`/`last`/`rate`, invalid or over-cap grids, and other semantic mismatches
remain on the raw path. The differential test covers negative timestamps,
edges, duplicate timestamps, sparse and empty series, buffered plus flushed
data, complex matchers, transforms, and integer count.

Attribution showed the row TVF spending 259ms to cross 72K SQLite rows into
BEAM, so the measured need justified a public `timeless_window_batches` API.
It emits one `TWB1` timestamp/validity/value blob per series and retains the
row-oriented TVF unchanged. A callback-safe batch core call and batched SQLite
shadow-row read reduced the packed extension fetch to 96.516ms in the final
20-sample public run. End-to-end bucketed average is 127.532ms median / 135.006ms
p95, down 3.88x from the saved 495.455ms raw baseline and only 1.06x / 1.09x the
matching Rust engine's 119.937ms / 124.265ms. Direct SQL packed materialization
is 67.796ms versus 108.825ms for row-TVF `COUNT(*)` on the same 72K buckets.

All exit gates passed: unsupported partial-bucket and rate carry-in cases have
observable raw-fallback tests, the full VictoriaMetrics corpus remains 182/182,
and workspace, CLI/oracle/crash, and TimelessMetrics suites pass. Full results
are in `tools/bench/results/2026-07-31_native_bucketed.md` and the paired
TimelessMetrics record.

### Session 4: bundled rollup reads — complete 2026-07-31

Implement `timeless_rollup_batches` and replace the adapter's six independent
rollup queries with one prepared statement and one decode pass.

Tests:

- Row-oriented and packed interfaces decode to identical buckets.
- Count remains integer-exact; floating fields preserve their stored bits.
- Tier retention, restart recovery, malformed blobs, and unknown versions.

Exit gate:

- At least 3x lower latency than six row-oriented calls on 1K+ rollup buckets.
- No change to on-disk rollup representation or replication semantics.

Completed with the public `timeless_rollup_batches` TVF and `TRB1` columnar
envelope. It returns timestamp, exact `u64` count, derived avg, stored
sum/min/max, last timestamp, and last value in one blob per series. The core
batch primitive holds one transition guard and uses the store's ordered batch
read; `timeless_rollup` remains unchanged. TimelessMetrics now keeps one
prepared packed statement per reader and performs one barrier, query, exact
label check, and decode instead of six of each.

The direct extension workload (12K series, 60K settled buckets, 10 samples)
measured 107.741ms median / 114.492ms p95 packed versus 1,002.150ms /
1,027.747ms for six materialized row queries: **9.30x** lower median latency.
The public TimelessMetrics exact-series workload (1,200 daily buckets, 15
samples) measured 0.767ms / 0.849ms versus 14.744ms / 16.982ms: **19.22x**
lower median latency. Row/packed parity, restart, exact counts, malformed and
unknown blobs, and the existing tier-retention path are covered without any
on-disk or replication-visible format change. Results are recorded in the
paired `2026-07-31_packed_rollups.md` benchmark notes.

### Session 5: wide raw transport

Use Session 0 attribution to optimize `timeless_raw_batches` without changing
its result contract first.

Candidates, in measurement order:

1. Remove intermediate point vectors or duplicate sorts during blob packing.
2. Reuse canonical label JSON and avoid decoding labels used only for exact
   comparison.
3. Honor SQLite projection information when columns are genuinely unused.
4. Reuse output buffers with bounded capacity and checked size arithmetic.
5. Evaluate a versioned multi-series columnar blob only if per-row SQLite and
   Exqlite overhead is dominant; retain the existing TVF for normal SQL use.
6. Evaluate a lazy/streaming TimelessMetrics API separately. Do not silently
   change the eager public API to win a benchmark.

Exit gate:

- Full 12K-series/720K-point public read falls from 369ms to 120ms or less as an
  interim target, with byte-identical timestamps and values.
- Memory remains bounded by a documented multiple of returned payload size.
- Exact and narrow queries do not regress by more than 5%.

Completed 2026-07-31. The original per-series path remains public, while the
additive `timeless_raw_frame` TVF transports all non-empty series in one `TRF1`
columnar blob. The core performs one ordered shadow-row read under one
transition guard; the TimelessMetrics NIF validates the frame and constructs
the final public series maps without an intermediate decoded result.

The final 10-sample public workload measured 113.142ms median / 113.881ms p95,
down from the Session 5 starting median of 344.221ms (3.04x) and the original
372.787ms baseline (3.29x). Exact and narrow improved from 0.348/3.632ms to
0.283/1.978ms. A sampled fresh worker peaked 120,596,408 bytes above baseline
for a 12,959,022-byte external result (9.306x), enforced by the benchmark's
10x bound. Direct SQLite frame materialization measured 63.636ms versus
74.416ms for per-series batches. Slice value bits, timestamps, buffered and
persisted parity, omission, reopen, malformed/unknown envelopes, and checked
size arithmetic are covered. Details are in
`tools/bench/results/2026-07-31_raw_frame.md` and the paired TimelessMetrics
record.

### Session 6: catalog publication and first-query latency

Instrument and then remove avoidable full authoritative-state reloads after a
large flush, compaction, rollup, or prune.

Investigate in this order:

- share already-published engine state across connections in one process
- distinguish append-only generation changes from rewrite/delete generations
- incrementally load new series and chunks when sound
- add a small transactional change journal only if generation counters cannot
  prove an incremental refresh safe
- move unavoidable refresh work to the publishing maintenance command rather
  than the first reader, without exposing uncommitted state

Exit gate:

- First 1,003-point read after the large flush is 5ms or less.
- Warm generation checks add no more than 10us p95.
- Kill/reopen, rollback, compaction, prune, and two-connection tests prove that
  no reader observes stale or uncommitted catalog state.

Completed 2026-07-31. The shared engine already held every local committed
mutation, but its cached authoritative generation remained stale, so the next
reader redundantly reloaded the full series, raw-chunk, and rollup catalogs.
The metrics virtual table now captures the final generation during SQLite
`xSync` and publishes it only from `xCommit`; `xRollback` discards it and
external-process commits still force the conservative full refresh.

The direct first exact read after flush fell from 44.730ms to a 0.187ms median
across five fresh processes (0.175–0.228ms, **239x faster**). The isolated
generation `SELECT` measured 0us p95 / 2us max at microsecond timer resolution.
Through TimelessMetrics, the same public read fell from the recorded 54.818ms
baseline to 2.891ms, clearing the 5ms gate. Reload-counting core tests and CLI
coverage pin commit, rollback, compaction, prune, two live connections,
external-process invalidation, and reopen. Results are recorded in
`tools/bench/results/2026-07-31_catalog_publication.md` and the paired
TimelessMetrics result.

### Session 7: matcher and discovery pushdown

Translate TimelessMetrics matchers into the extension's matcher JSON only after
a pinned semantic comparison covers:

- absent labels as empty strings
- empty equality
- duplicate matchers on one key
- anchored regexes and invalid patterns
- regex dialect differences between Elixir and Rust
- negative equality and negative regex

Use a hybrid plan when a matcher cannot be represented exactly: push a safe
candidate subset down, then post-filter labels before reading points whenever
possible. Apply the same planner to discovery queries.

Exit gate:

- Complete matcher differential suite passes.
- Selective regex/negative workloads avoid unrelated chunk reads.
- No semantic fallback is removed.

Completed 2026-07-31. TimelessMetrics now sends every provably portable
equality, empty-equality, negative, regex, and negative-regex predicate to the
extension. Duplicate keys retain their complete Elixir AND as a residual while
one necessary predicate may narrow candidates. PCRE-only and
dialect-sensitive expressions stay above the boundary; invalid patterns keep
the existing empty-result behavior without reading storage.

The public one-series regex read fell from 116.944ms to 3.083ms median
(**37.93x**) and filtered discovery fell from 52.427ms to 2.431ms
(**21.57x**). The optimized libSQL path is 24.06x faster than the current Rust
block-engine adapter for that regex read. Direct SQL measured 4.947ms for the
same raw shape, 0.731ms for filtered series discovery, and 0.903ms for filtered
label values. A 6K-series negative query took 57.008ms versus 109.695ms for the
12K-series wide read, demonstrating that unrelated chunks no longer cross the
boundary.

`timeless_series(tbl, metric, filter)` and
`timeless_label_values(tbl, metric, key, filter)` are additive public discovery
forms. Differential tests pin absent/empty labels, duplicates, invalid regexes,
PCRE lookahead, Unicode dot behavior, rollback, two connections, and reopen;
all unsafe cases retain post-filter fallback. Results are recorded in
`tools/bench/results/2026-07-31_matcher_discovery_pushdown.md` and the paired
TimelessMetrics result.

### Session 8: integration, soak, and default-engine decision

- Run `cargo test --workspace`, the full CLI suite, randomized query oracles,
  transaction/recovery tests, the full Elixir suite, and the 182-case
  VictoriaMetrics differential corpus.
- Run mixed write/read soak tests with writer restarts, compaction, rollup,
  retention, backup, and multiple readers.
- Re-run the stored benchmark protocol at least five fresh-process times per
  engine and report distributions, not only the best run.
- Update public SQL documentation and examples for every new TVF.
- Pin the extension dependency to an immutable merged revision.

Final performance gate:

- Representative exact, narrow, wide, aggregate, latest, window, and rollup p95
  values are within 20% of Rust, or the default remains `:rust` with the failing
  shapes documented.
- Batch ingest and storage size retain their current advantages within 10%.
- No query optimization changes the public TimelessMetrics result corpus.

Changing the default engine is a separate, easy-to-revert commit after this
session passes. It is not part of an optimization patch.

#### Session 8 progress: 2026-07-31

The five-process query distribution, large write/storage repeat, packed-rollup
distribution, and mixed soak are recorded in the paired TimelessMetrics
`bench/results/2026-07-31_integration_release_gate.md`. Batch throughput and
storage retain their gate advantages, but exact, narrow, wide, scalar, and
exact-latest p95 miss the fixed 20% query bound. The release decision is
therefore to keep `:rust` as the default and leave `:libsql` opt-in.

The soak exposed one correctness bug after an intra-transaction flush: the
shared engine could reference a shadow chunk row visible only to the writer
connection. The extension now gates engine materialization with short read
permits and returns a retryable busy-style conflict to other connections while
a write transaction is active. Unit tests, direct CLI section 39, and a pooled
TimelessMetrics regression pin commit, rollback, and retry behavior. The
post-fix 30-second soak passed 392,512 writes, 23,496 oracle reads, concurrent
maintenance/backups, a forced writer restart, reopen, and backup verification
with zero escaped read transients. `cargo test --workspace`, all 39 CLI
sections, and all 477 Elixir tests pass.

Session 8 is complete. TimelessMetrics pins merged revision
`09aa46e94185c8380d5a16a6efda353b62e0083a`; its unpacked Hex package builds
the wrapper with `cargo check --locked` without a sibling checkout, and all 477
Elixir tests pass against the pin. Further read-p95 work is optional
extension-first optimization, not unfinished migration work. The rollout keeps
`:rust` as the TimelessMetrics default for the next release or two while
`:libsql` ships as the supported forward path.

The extension-first follow-on is captured in
[`STANDALONE_QUERY_PLAN.md`](STANDALONE_QUERY_PLAN.md). It is gated on direct
SQLite/libSQL value rather than TimelessMetrics benchmark parity and is planned
before the separate Rust telemetry data-plane POC.

## Per-session checklist

Every implementation session must:

1. Record the starting revisions and benchmark command.
2. Add the naive/differential oracle before the optimized route becomes default.
3. Test direct SQL usage independently of TimelessMetrics.
4. Test restart, rollback, and a second connection when storage state is read.
5. Compare allocations and bytes returned as well as latency.
6. Run formatting and diff checks in both repositories.
7. Update this file with completed work, measurements, and remaining risks.

## Stop conditions

- Do not move PromQL semantics into the extension to improve a benchmark.
- Do not add a packed format without versioning, checked arithmetic, malformed
  input tests, and a row-oriented SQL alternative.
- Do not trade read-after-write visibility for latency.
- Do not benchmark Rust and libSQL concurrently on the same host.
- Do not change the 20% release gate after seeing results; record a deliberate
  product decision separately if the eager Elixir API imposes an irreducible
  transport floor.
