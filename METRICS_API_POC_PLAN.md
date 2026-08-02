# Rust metrics API POC plan

Status: Sessions 0-5 complete; Session 6 ready on
`poc/rust-telemetry-data-plane` (2026-08-01)

This POC tests the process boundary, not a new metrics storage engine. The
server will use the existing `timeless_metrics` virtual table, its public batch
formats and query TVFs, and the storage/query behavior already adopted by
`timeless_metrics`. The completed logs POC is the implementation reference,
but metrics semantics and wire formats will not be hidden behind a premature
generic API abstraction.

## Boundary decision

```text
Prometheus / VictoriaMetrics / Grafana
                  |
                  v
      timeless-metrics-api (Rust POC)
 HTTP parse, bounded admission, query planning,
 response encoding, cancellation, API telemetry
                  |
                  v
      public timeless-libsql SQL surfaces
                  |
                  v
       metrics.db (one runtime owner)

Phoenix / LiveView / Canvas / Stack (Elixir)
 product state, sessions, dashboards, cluster control
                  |
        HTTP or Unix-domain socket
                  v
      timeless-metrics-api
```

The POC binary is deliberately named `timeless-metrics-api`. `timelessd` is a
possible product name after at least two signal implementations prove which
server machinery is genuinely common. There will be no generic `timeless-api`
crate that obscures metrics-specific behavior before that evidence exists.

## Fixed storage and durability contracts

- Load the existing extension and create/connect the existing
  `timeless_metrics` virtual table. Do not reproduce its engine, series
  catalog, rollups, retention, chunk layout, codecs, or transaction journal.
- Feed named batch `0x01`, resolved-series batch `0x02`, or Prometheus text to
  the existing hidden-column ingest surface. Never insert one SQL row per
  point on a production benchmark path.
- Preserve the engine-owned automatic flush threshold of 4,096 points per
  series. HTTP request, parser batch, and writer transaction boundaries do not
  become storage flush boundaries.
- Preserve queryability of buffered points and the existing explicit `flush`,
  `compact`, `rollup`, and `prune` commands.
- Start with the established metrics maintenance cadence and schema. Any
  budget or policy improvement belongs in the extension and must help direct
  SQLite/libSQL users, not just this server.
- Use one SQLite writer. Reader count is a measured deployment decision, not
  CPU-count folklore; sweep 1/2/4/8 after the functional path is exact.
- The Rust process is the sole owner of `metrics.db` while it runs. Baseline
  Elixir and Rust servers use separate copied/fresh databases.

## Lessons carried forward from the logs POC

1. **Baseline the real design before writing server code.** Compare the Rust
   API first with `TimelessMetrics.HTTP` using `engine: :libsql`; that isolates
   the HTTP/runtime boundary. Keep the default Rust block engine as a useful
   secondary product comparison, not the primary API-boundary control.
2. **Do not build storage to obtain a quick benchmark.** The server reaches
   SQLite only through released extension surfaces. Missing behavior is first
   classified as extension, reusable query crate, or API responsibility.
3. **Keep batching native.** Parse a complete request, encode the established
   columnar batch, and admit it to one bounded writer queue. Do not invent a
   point-count threshold, compress requests individually, or flush at request
   boundaries. Group separate requests only if a controlled benchmark later
   proves a host-transaction benefit without changing durability semantics.
4. **Admission is not completion.** Report admitted and SQLite-completed
   points, queue depth and oldest age, final drain, errors, and explicit flush
   completion. A `204` retains the current asynchronous admission meaning.
5. **Fix the lowest reusable layer.** Writer fairness, bounded reads, native
   scalar/latest/window operations, packed result frames, and compaction
   policy stay in the extension/core when direct users can benefit. The API
   only selects and schedules those public capabilities.
6. **Memory is a release gate.** Measure Linux process HWM as well as latency.
   Reject designs that retain database-sized payload or result copies even if
   they are faster.
7. **Maintenance needs separate accounting.** Measure ingest with maintenance
   deferred, then measure maintenance under load and drain-to-zero. Report
   logical live payload and physical SQLite file high-water separately;
   optimize is not vacuum.
8. **Concurrency follows evidence.** The logs sweep selected two readers and
   rejected an admission controller because its queue was already healthy.
   Metrics must run its own sweep rather than inherit either the old CPU clamp
   or the answer two by assumption.
9. **Parity precedes optimization.** Every accepted route gets black-box
   request/response fixtures, storage-result oracles, malformed-input cases,
   restart tests, and mixed read/write coverage before tuning.
10. **Keep the POC honest and small.** No auth implementation, cluster
    membership, backup product workflow, alert state, scrape scheduling,
    Canvas rewrite, or generic three-signal framework until the data-plane
    slice proves itself.

## Measurement contract

Use deterministic request bodies and query sequences. Every comparison records:

- admitted and SQLite-completed points/s;
- writer queue batches/points, oldest age, and final drain time;
- request parse, batch encode, queue wait, SQLite statement, and transaction
  completion time;
- write and query p50/p95/p99 plus errors by route/query shape;
- candidate series/chunks, payload bytes, decoded points, and returned points;
- buffered/durable points, raw/compressed/rollup chunks and logical bytes;
- compaction/rollup/prune duration and source/output bytes or points;
- SQLite file size, freelist/high-water behavior, and process HWM;
- exact HTTP response parity or documented compatibility normalization;
- cancellation and child-process crash/restart behavior.

The pinned matrix must include no-query, one-query-worker, and two-query-worker
HTTP modes; one/direct extension mode; a maintenance-under-load mode; and a
drain-to-zero phase. Report current Elixir+libSQL, Rust API+libSQL, and—where it
helps product decisions—the current Rust block engine on the same host.

## Session 0 — Pin the control implementation

- [x] Freeze representative request/response fixtures for health, Victoria
      JSON-line ingest, Prometheus text ingest, native latest/range/export,
      label names/values, and series discovery.
- [x] Add deterministic HTTP workload controls and completed-work reporting to
      the existing metrics harness rather than creating a benchmark-only
      storage path.
- [x] Run fresh-process baselines for Elixir HTTP with `engine: :libsql` and
      `engine: :rust`, including HWM and maintenance-deferred storage stats.
- [x] Record the exact current behavior for malformed and partially valid
      Prometheus/VM bodies, timestamp units, NaN/Inf, duplicate series, empty
      results, inclusive range edges, and asynchronous `204` admission.

Exit criterion: the control is reproducible and request admission cannot be
mistaken for SQLite completion.

Result: complete. See
`timeless_metrics/bench/results/2026-08-01_metrics_api_session0.md`. The final
fixed step completed 777.0–792.7K points/s on Elixir+libSQL and 806.6–819.1K
points/s on the Rust block engine across zero/one/two query-worker runs. All
runs drained to queue zero with no request/query errors. The pinned black-box
contract passes on both engines, and completion-aware health/flush controls now
prevent asynchronous `204` admission from being reported as stored work.

## Session 1 — Descriptive server shell and storage contract

- [x] Create `poc/timeless-metrics-api` as a separate Rust workspace/binary,
      reusing proven logs POC worker patterns without copying logs semantics.
- [x] Start one writer and a configurable reader pool over one database; load
      the existing extension and use the current metrics table/schema.
- [x] Implement only `GET /health`, `GET /select/metrics/stats`, and an explicit
      ordered flush barrier.
- [x] Expose admitted/completed/queued counters, API phase timers, extension
      stats, database file/freelist bytes, and actual index units.
- [x] Pin shutdown, restart, invalid configuration, and sole-database-owner
      behavior. No auth or product routes.

Exit criterion: an extension-backed test proves buffered points, the exact
4,096-per-series automatic flush contract, explicit flush, restart recovery,
and no host storage implementation.

Result: complete. The test uncovered one extension bug: metrics queued a
partition at 4,096 points but no vtab ingest surface drained the pending queue.
The fix calls the existing engine operation after Tier 1, named/resolved batch,
and Prometheus ingest, preserving a no-store-write fast path below threshold
and benefiting every direct SQLite/libSQL user. The API test proves 4,095
buffered -> one chunk at 4,096 -> explicit tail flush -> 4,106 points after
reopen, ordered completion counters, and advisory sole ownership. See
`poc/timeless-metrics-api/README.md` and
`../timeless_metrics/bench/results/2026-08-01_metrics_api_session1.md`.

## Session 2 — Native batched ingest

- [x] Implement `POST /api/v1/import/prometheus` by passing the complete body
      through the extension's existing Prometheus ingest path.
- [x] Implement `POST /api/v1/import` by parsing Victoria JSON lines once and
      encoding one established named batch `0x01` per admitted request.
- [x] Keep a durable series-id cache and resolved batch `0x02` deferred until a
      later measured optimization; named batch remains the correctness floor.
- [x] Preserve partial/malformed body semantics from Session 0 and enforce the
      existing HTTP body limit before unbounded allocation.
- [x] Benchmark parse, encode, statement, queue, completion, and HWM. Do not
      add request compression or cross-request grouping without evidence.

Exit criterion: byte/semantic ingest parity, exact persisted points after
flush/reopen, bounded admission, and no per-point SQL path.

Result: complete. The extension-backed contract admits partial and all-invalid
Prometheus/VictoriaMetrics requests with the established empty `204`, persists
four valid points, reports eight rejected inputs, rejects 10 MiB + 1 before
admission, drains exactly five requests through flush, and recovers all four
points after reopen. One rejected Prometheus body begins with a reserved batch
version and proves the HTTP route cannot switch hidden-column protocols.
Prometheus bodies remain reference-counted and unparsed by the API;
VictoriaMetrics bodies are parsed once into one named-columnar batch.

Two fresh no-query Prometheus runs completed 855.2–855.6K points/s with 448us
write p95, a 7–9ms final queue age, zero errors, and 178,888–180,016KiB HWM.
That is 9.7% faster than Session 0's 779.9K Elixir+libSQL control with about 52%
less HWM. Two VictoriaMetrics named-batch runs completed 613.2–620.9K points/s
with 2.78–2.87ms p95, single-digit final queue depth, zero errors, and
178,164–179,460KiB HWM. The latter is the evidence for measuring resolved batch
`0x02` later, not grounds for changing the Session 2 storage contract.

Profiling also caught and removed two per-request `timeless_stats` scans that
initially reduced the Prometheus path to 179.0K points/s. Point completion now
uses SQLite's existing `last_insert_rowid`; rejected-line totals and fused
parse/resolve/buffer time remain cumulative extension stats available to direct
SQLite/libSQL users. See
`poc/timeless-metrics-api/README.md` and
`../timeless_metrics/bench/results/2026-08-01_metrics_api_session2.md`.

## Session 3 — Mechanical reads and discovery

- [x] Implement native exact latest, range/export, label names/values, and
      series routes from `timeless_latest[_frame]`, `timeless_raw_frame`,
      `timeless_series`, and `timeless_label_values`.
- [x] Decode packed frames directly into final response serializers; do not
      recreate Rust -> SQLite rows -> host objects -> JSON expansion.
- [x] Preserve matcher, timestamp, ordering, empty-result, NaN, and response
      envelope semantics against Session 0 fixtures.
- [x] Keep row-TVF fallbacks where feature discovery says an older extension
      lacks a frame; never infer capability from a version string.

Exit criterion: exact route parity with bounded memory, and direct extension
users retain every accelerated primitive used by the server.

Result: complete. The Rust reader pool now serves native latest, inclusive raw
export, native-window/common aggregation with raw-frame fallback for partial
and first/last/rate shapes, label names/values, native series, and repeated
Prometheus selector discovery. It probes `pragma_module_list` for packed
capabilities and retains row-TVF latest/raw fallbacks. `TLF1`, `TRF1`, and
`TWB1` envelopes are validated; raw/window columns remain borrowed views over
one SQLite blob while the reader writes the final JSON body, avoiding duplicate
point vectors.

The extension-backed fixture pins GET/POST merging, exact and multi-series
envelopes, inclusive bounds, empty export, native and partial range paths,
metric/regex/duplicate/missing-label matcher behavior, malformed selectors,
explicit PromQL deferral, read telemetry, and exact reopen results. A fixed
400,000-point comparison returned identical response byte counts for all seven
mechanical shapes. Rust p95 was 1.8-3.0x slower for exact latest/range/export
but remained 0.56-0.75ms; it was 6.7x faster for label names, 4.9x faster for
metric series, and modestly faster for label values/exact selector discovery.
Same-lifecycle HWM was 56,948KiB versus 244,796KiB for Elixir+libSQL.

Under the deterministic 4,000-series mixed workload, Rust completed 866.6K
points/s with 10.08ms query p95 and 180,716KiB HWM. Two Elixir+libSQL controls
completed 604.8-782.5K points/s with 70.26-93.71ms p95 and 457,128-460,152KiB
HWM; both logged the pre-existing label-cache miss race even though their
measured client windows reported zero query errors. The Rust run had no
read/write error, drained to zero, and reported 25.25MB of packed query frames
for only 684.7KB of response bodies. Full method and discarded-harness notes
are in
`../timeless_metrics/bench/results/2026-08-01_metrics_api_session3.md`.

## Session 4 — First PromQL vertical slice

- [x] Port only the parser/evaluator subset required by a pinned vertical
      slice: vector selector plus one range/window aggregate already expressible
      through public raw/window surfaces.
- [x] Return final Prometheus vector/matrix envelopes from Rust and compare
      them to the VictoriaMetrics differential corpus cases in scope.
- [x] Keep unsupported expressions explicit; do not silently fall back across
      the process boundary or shuttle per-series data through Elixir.
- [x] Separate storage-independent PromQL code from route code so logs/traces
      do not inherit metrics semantics.

Exit criterion: one realistic PromQL range request is socket-to-response Rust,
bit/semantic compatible, cancellable, and free of BEAM/NIF data transport.

Result: complete. A storage-independent parser emits only selector and
`avg_over_time(selector[window])` plans. Exact selectors use one public `TRF1`
raw frame plus a linear lookback sweep; window averages lower directly to
public `timeless_window_batches`. Both `/api/v1` and `/prometheus/api/v1`
instant/range routes write the final Prometheus vector/matrix bytes in Rust.
Unsupported functions, operators, aggregations, and name patterns return an
explicit Prometheus `bad_data` response; no request falls back to Elixir.

The extension-backed contract pins strict `(T-window,T]` boundaries, 300-second
selector staleness, grid timestamps, duplicate matcher AND semantics, exact
`__name__`, GET/POST precedence, duration steps, RFC3339/float timestamps,
errors, telemetry, cancellation, and reopen. Dropping a 4,000-series,
11,000-step request cancels SQLite/host work and leaves the sole reader
immediately reusable. A fresh-process socket differential matched the
Elixir+libSQL control on all six selector/window range and instant shapes over
400,000 points.

Rust p95 was 2.66x faster for the 100-series selector, 1.45x faster for exact
window average, and 4.06x faster for the 100-series window average. Its exact
selector range and two instant shapes were 1.42-1.95x slower but remained
0.71-1.12ms p95. Same-lifecycle HWM was 58,272KiB versus 315,596KiB for the
control. Full method and results are in
`../timeless_metrics/bench/results/2026-08-01_metrics_api_session4.md`.

## Session 5 — Elixir control-plane client and isolation

- [x] Add a thin opt-in Elixir client adapter using loopback HTTP or a Unix
      socket; it owns no telemetry database connection.
- [x] Switch one non-product Canvas/query call behind configuration and prove
      identical public results.
- [x] Supervise the OS child and prove Rust crash/restart does not crash the
      BEAM, expose partial responses, or corrupt flushed data.
- [x] Keep sessions, authorization policy, alerts, annotations, scrapes,
      dashboards, and cluster administration in Elixir.

Exit criterion: the proposed process/control-plane boundary works in practice,
not only as a standalone benchmark.

Result: complete. `timeless_ui` now optionally supervises the Rust executable
through an Erlang port and resolves a loopback endpoint through a stateless Req
client which never opens the telemetry database. The configured Canvas data
source switches only `metric_range/5`; all other callbacks remain delegated to
the Elixir source. Complete-body and NDJSON validation prevent transport or
parse truncation from becoming partial graph data.

The release-artifact gate flushes two points, rejects a second database owner,
sends `SIGKILL` to the Rust OS pid, observes the OTP child restart without the
UI supervisor exiting, and returns the exact flushed millisecond points after
reopen. It also caught and fixed normal OTP shutdown orphaning the Rust child:
the owner now sends `SIGTERM`, the server drains through its graceful flush
path, the port child is reaped, and an admitted unflushed tail recovers on the
next reopen. Eight focused tests pass. An interleaved
five-round, 2,500-sample-per-path, 600-point loopback comparison measured
933us p95 with a pinned URL and 927us through the supervised readiness lookup;
SIGKILL-to-ready was 20.08ms. See
`../timeless_metrics/bench/results/2026-08-01_metrics_api_session5.md`.

## Session 6 — Scheduling, maintenance, and final verdict

- [ ] Sweep 1/2/4/8 SQLite readers after functional fixes.
- [ ] Measure no-query and mixed ingest, every included read shape,
      maintenance under load, drain-to-zero, file high-water/reuse, and HWM.
- [ ] Add API admission fairness or cross-request transaction grouping only if
      queue, completion, and writer-wait evidence identifies a real problem.
- [ ] Run the full Elixir/libSQL parity, extension workspace/CLI/oracle/crash,
      HTTP fixture, and child-restart gates.
- [ ] Record a keep/reject decision and the exact next signal boundary.

Exit criterion: a process-boundary decision based on completed work, exact
compatibility, bounded tail latency/memory, operational isolation, and honest
physical storage behavior.

## Explicitly deferred

- full PromQL parity and all Prometheus/Victoria product endpoints;
- authentication implementation or tenant policy;
- alerts, annotations, scrape scheduling, charts, forecasting, and backup UI;
- remote libSQL replication or cluster membership;
- a unified logs/metrics/traces server or generic route framework;
- changing the default TimelessMetrics storage engine;
- automatic vacuum policy. Measure page reuse and explicit vacuum separately.
