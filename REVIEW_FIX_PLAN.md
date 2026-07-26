# Correctness Review Remediation Plan

This file tracks the race-condition and correctness work found during the
2026-07-26 review. The project is still an early POC, so this plan deliberately
targets existing behavioral defects rather than feature completeness.

## Working Agreement

Use a strict red-green-refactor loop for every item:

1. Add the smallest deterministic regression test that demonstrates the defect.
2. Run that test and record the expected failure in the session log below.
3. Implement only the fix for that item.
4. Run the targeted test until green.
5. Run the relevant crate tests, then the full workspace and CLI suites.
6. Commit the test and fix together. Do not leave a knowingly failing test on
   the shared branch between sessions.

For concurrency tests, use barriers/channels or explicit process coordination.
Do not rely on sleeps to make a race "likely." Tests must prove the required
ordering before allowing the competing operation to continue.

## Baseline

Recorded before remediation:

- [x] `cargo test --workspace --all-targets`: 82 tests passed.
- [x] `./tests/cli.sh`: all sections passed, including 150,000 oracle
  operations, five kill-9 rounds, and the two-connection test.
- [ ] `cargo fmt --all -- --check`: currently fails because of existing
  formatting drift.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`: currently fails
  on four codec style lints.
- [x] Review-specific reproductions captured in the review conversation.

The formatting and Clippy failures are not prerequisites for the correctness
work, but should be cleaned in an isolated commit so new warnings are visible.

## Test Layout

Keep focused regressions fast enough to run during each red-green cycle:

- Pure engine concurrency and failure injection:
  `crates/timeless-core/tests/` or the existing engine test modules.
- SQLite transaction, schema, and timestamp semantics:
  a new `tests/correctness.sh`.
- Multi-process behavior:
  a new `tests/multiprocess.py`, called by `tests/correctness.sh`.
- The long oracle/crash suite remains in `tests/cli.sh`.

`tests/correctness.sh` should accept or discover
`target/release/libtimeless_ext.so`, create a fresh temporary directory, and
remove it on exit. Each regression should be runnable independently by a
section name or environment filter so later sessions do not need the full
suite for one change.

## Ordered Remediation

### R1: Statement Atomicity and Savepoints

Severity: critical. This is first because later schema and registry changes
must be able to rely on SQLite rollback semantics.

RED tests:

- [x] A metrics `INSERT ... SELECT` whose second row is invalid leaves no row
  from the failed statement, both before and after a later flush/reopen.
- [x] Equivalent failed multi-row inserts for logs and traces leave no rows.
- [x] `SAVEPOINT; INSERT; ROLLBACK TO; RELEASE; COMMIT` removes the rolled-back
  rows for all three virtual tables.
- [x] A failed statement that crosses the logs/traces auto-flush threshold
  leaves neither buffered rows nor dangling block/index locations.
- [x] Maintenance commands executed after a savepoint roll back to the exact
  pre-savepoint buffer, index, and shadow-table state.

Implementation:

- [x] Instrument a temporary callback-order test to establish exactly which
  `xSavepoint`, `xRelease`, and `xRollbackTo` calls SQLite makes for explicit
  savepoints and failed multi-row statements.
- [x] Add savepoint hooks to the virtual-table registration layer. Rusqlite
  0.40.1 does not expose them through `TransactionVTab`, so use a small,
  reviewed local wrapper/patch rather than inferring statement boundaries in
  `xUpdate`.
- [x] Change each engine journal from one transaction snapshot to a stack of
  checkpoints keyed by SQLite's savepoint number.
- [x] Make rollback-to restore buffer marks/saved entries and reverse
  index additions/removals without ending the outer transaction.
- [x] Make release discard or merge only the released checkpoint.
- [x] Preserve the writer gate until the outermost commit/rollback.

Acceptance:

- [x] Every RED test is green.
- [x] Existing whole-transaction rollback tests remain green.
- [x] Failed SQL statements cannot become durable through a later flush.

### R2: Database-Authoritative Metrics Registry

Severity: critical. Fixes multi-process ID collision, stale process state, and
the unsafe "start fresh" recovery behavior.

RED tests:

- [x] Two separately launched OS processes connect before either writes,
  then serially flush different new metrics. Reopen returns one correctly
  named point for each metric and distinct authoritative series IDs.
- [x] A long-lived process sees chunks committed by another process without
  reconnecting.
- [x] Two processes resolving the same new `(name, labels)` converge on one
  series ID.
- [x] Missing or corrupt registry metadata with existing chunks fails closed;
  it must not allocate IDs from 1 or relabel old chunks.
- [x] Existing databases migrate or reopen without losing series identity.

Implementation:

- [x] Replace the replace-whole-registry blob as the source of truth with a
  shadow `_series` table. Prefer `UNIQUE(name, canonical_labels)` so hash
  collisions cannot define identity.
- [x] Allocate/resolve new IDs through SQLite in the caller's transaction.
  Keep the in-memory resolve cache only as a verified acceleration layer.
- [x] Add an authoritative way for a metrics query to discover chunks
  committed by other processes. The POC refreshes from the store at query
  boundaries; a range lookup or persisted generation can optimize this later.
- [x] Define and test migration from the current `series_registry` blob.
- [x] Change recovery APIs to return an error when authoritative identity
  cannot be loaded. Never combine a fresh registry with existing chunks.
- [x] Keep the process registry as a cache/coordination optimization only;
  correctness must not depend on all writers sharing one address space.

Acceptance:

- [x] Multi-process tests are deterministic and green for repeated runs.
- [x] No process can overwrite another process's series identity.
- [x] A stale process cannot omit externally committed chunks.

### R3: Attached-Database Schema Qualification

Severity: high.

RED tests:

- [x] Creating metrics, logs, and traces in `aux` creates every shadow table
  and index in `aux`, with no same-named object in `main`.
- [x] The same virtual-table name can exist independently in `main` and
  `aux`.
- [x] Insert, query, flush, optimize/compact, prune, reopen, backup, and drop
  operate on the selected schema only.
- [x] Schema and table names containing quotes remain safe.

Implementation:

- [x] Carry the xCreate/xConnect `database_name` into every shadow store.
- [x] Centralize identifier qualification and quoting as
  `"schema"."object"`; do not assemble variants independently in three stores.
- [x] Generate SQLite-correct qualified `CREATE INDEX` and `PRAGMA` syntax.
- [x] Include schema identity in any store metadata or instance identity
  introduced by R2/R4.

Acceptance:

- [x] Inspecting both `sqlite_schema` tables shows complete isolation.
- [x] Detaching or copying `aux` leaves a self-contained telemetry database.

### R4: Transactional DROP and Recreate Identity

Severity: high. Depends on authoritative metadata from R2 and qualification
from R3.

RED tests:

- [x] `BEGIN; DROP TABLE; ROLLBACK` preserves committed buffered and flushed
  rows on the same connection.
- [x] The same rollback with a second connection already attached preserves
  one shared engine, not split state.
- [x] Committed DROP followed by CREATE gets a fresh engine with no old
  buffered rows.
- [x] Failed shadow-table DDL does not remove the live registry entry.

Implementation:

- [x] Stop treating entry removal at the start of `xDestroy` as committed.
- [x] Give each virtual table a persisted instance ID created transactionally
  with its shadow schema, and include it in the process registry key.
- [x] On DROP rollback, restored metadata resolves to the original instance.
  On committed DROP/recreate, the new instance ID forces a new engine.
- [x] Let dead weak registry entries be swept lazily; do not invalidate live
  state before SQLite commits the schema change.

Acceptance:

- [x] DROP obeys the same commit/rollback behavior as an ordinary SQLite
  table for all three modules.

### R5: Atomic Query/Flush/Compaction Publication

Severity: high.

RED tests:

- [x] A barrier-controlled metrics query paused after its index snapshot,
  while flush drains/publishes, still returns the point exactly once.
- [x] Equivalent logs and traces tests pause after candidate lookup and
  cannot miss the pre-existing buffered entry during flush.
- [x] Queries holding old locations while compact/prune runs either complete
  from a pinned generation or retry; they never return missing-row/file
  errors.
- [x] Run each schedule repeatedly under both memory stores and the
  filesystem store where applicable.

Implementation:

- [x] Introduce one generation/transition synchronization primitive per
  engine. Query must pin a consistent buffer/index/store generation for the
  complete scan; flush, compact/optimize, and prune publish under exclusive
  transition ownership.
- [x] Keep store replacement data alive until every reader of the old
  generation has released it, or make readers detect a generation change and
  retry from a fresh snapshot.
- [x] Document a single lock order covering transaction journal, transition
  guard, buffer/partitions, index, and store callbacks.
- [x] Verify that holding the transition guard across re-entrant SQLite reads
  cannot create a connection/gate lock cycle.

Acceptance:

- [x] Every forced interleaving returns a complete, duplicate-free snapshot.
- [x] Concurrent maintenance produces no transient read errors.

### R6: Filesystem Compaction Error Safety

Severity: high for `FsStore`.

RED tests:

- [x] A failed old-file deletion keeps the compaction manifest and does not
  let old and replacement chunks both reappear after restart.
- [x] A failed pending-to-final rename never deletes the corresponding old
  chunk.
- [x] Recovery retries a partially completed manifest and removes it only
  after every required operation succeeds.
- [x] Malformed/truncated manifests fail closed with an actionable error.

Implementation:

- [x] Make compaction recovery return `Result`; propagate it through a
  fallible store/engine constructor instead of logging and continuing.
- [x] Treat missing files idempotently only when the rest of the manifest
  proves the operation already completed.
- [x] Propagate normal-path deletion errors and retain the manifest for retry.
- [x] Add file and directory `sync_all` calls at the durability boundaries
  claimed by the manifest protocol.
- [x] Use a narrow test-only deletion failpoint because portable temporary-file
  tricks cannot inject a deletion error while preserving the old file; no
  production filesystem abstraction was needed.

Acceptance:

- [x] Every injected failure leaves either the complete old generation or the
  complete new generation recoverable, never both visible and never neither.

### R7: Stats Lock-Order Deadlock

Severity: medium.

RED tests:

- [x] Barrier-controlled `stats()` concurrent with flush deadlocks under the
  old ordering and completes under a bounded test timeout after the fix.
- [x] Cover both `BlockEngine` and `SpanBlockEngine`.

Implementation:

- [x] Compute index-derived values in a short scope, release the index lock,
  then read the buffered count.
- [x] Audit every multi-lock method against the lock order documented in R5.
- [x] Evaluate a debug-only lock-order assertion; do not add one because scoped
  guards plus deterministic tests cover the only inversion without adding lock
  instrumentation to production paths.

Acceptance:

- [x] No public method acquires index then buffer when a mutation path can
  acquire buffer then index.

### R8: Timestamp Extremes and NULL Constraints

Severity: medium.

RED tests:

- [x] Insert `i64::MIN` and `i64::MAX`; an unconstrained scan returns both
  for metrics, logs, and traces before and after flush/reopen.
- [x] Explicit inclusive extreme bounds return both.
- [x] `WHERE ts >= NULL`, `ts <= NULL`, and a pushed equality `= NULL`
  (`name`, or `level` for logs) return zero rows, not an error, for every module.
- [x] Strict `<`/`>` edge constraints remain correct with SQLite rechecking.

Implementation:

- [x] Use the full `i64::MIN..=i64::MAX` range; remove unused sentinel
  avoidance unless arithmetic actually requires it.
- [x] Decode every pushed nullable constraint as `Option<T>`. A NULL
  constraint marks the plan impossible and returns an empty result.
- [x] Keep `omit=false` for widened strict-bound handling.

Acceptance:

- [x] Virtual-table results match a plain SQLite table for the complete i64
  timestamp domain and NULL predicates.

## Final Verification Gate

Run after every item and once more before declaring the plan complete:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo build --release -p timeless-ext
./tests/correctness.sh
./tests/cli.sh
```

Also run targeted concurrency tests under a repetition loop or their test
harness equivalent. A race test is not accepted merely because it passed once.

## Session Log

Add one row whenever work starts, stops, or changes direction. Keep notes
short and link commits when available.

| Date | Item | State | Evidence / next step |
|---|---|---|---|
| 2026-07-26 | Baseline | complete | Review finished; existing suites green; remediation plan created. |
| 2026-07-26 | R1 | complete | RED: metrics leaked row 1 of a failed two-row insert. GREEN: 88 workspace tests, correctness suite, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | R2 | complete | RED: two preconnected processes relabeled both chunks as the second metric. GREEN: three multi-process rounds, 88 workspace tests, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | R3 | complete | RED: aux vtabs created shadows in main. GREEN: schema isolation/lifecycle/quoted-name tests, 88 workspace tests, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | R4 | complete | RED: DROP rollback lost committed buffered rows. GREEN: rollback/recreate/failed-DDL tests for all modules, 88 workspace tests, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | Performance audit | complete | Intel 185H, isolated `0645d99` vs R1-R4 tree: metrics Tier 2 -20.0% raw/-17.1% normalized and range query +54.3%; logs/traces/codecs stayed within noise or improved. See RESULTS.md. |
| 2026-07-26 | R5 | complete | RED: all signals missed buffered rows during flush; log/trace readers hit deleted block IDs during optimize. GREEN: 8 deterministic schedules x20 rounds, 96 workspace tests, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | R6 | complete | RED: all four recovery/error cases failed. GREEN: 5 deterministic filesystem cases x20 rounds, fallible engine-open coverage, 102 workspace tests, all R1-R4 suites, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | R7 | complete | RED: logs and traces each deadlocked within the forced `index -> buffer` / `buffer -> index` cycle. GREEN: 2 deterministic schedules x20 rounds, 104 workspace tests, all R1-R4 suites, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | R8 | complete | RED: 9 unconstrained endpoint mismatches, 18 one-sided strict-bound mismatches, and 6 metrics NULL-bound errors; explicit inclusive bounds already passed. GREEN: plain-table parity across buffered/flushed/reopened states x20 rounds, 104 all-target workspace tests, scoped strict Clippy, all R1-R4 suites, 150k-op oracle, and five crash rounds. |
| 2026-07-26 | Performance checkpoint | complete | Intel 185H, R1-R4 vs R1-R8: normalized metrics Tier 1 -0.7% and Tier 2 +2.6%; logs/traces ingest and maintenance stayed within noise. Three >15% timing outliers were not repeatable within the paired runs. See RESULTS.md. |
| 2026-07-26 | Perf remediation | complete | M5 Pro isolation corrected the Tier 2 attribution (first-touch catalog + savepoint wiring, not resolver locks). Fixes: catalog-generation skip for refresh (query back to parity, Tier 1 parity) and bulk series resolver (first-touch halved; found + fixed a re-entrant xSavepoint deadlock — no engine lock may be held across re-entrant store SQL). GREEN: 95 workspace tests, r1-r8 suites, 150k-op oracle, 5 crash rounds, 77/77 cli.sh. Tier 2 -11% vs master remains, documented in RESULTS.md. |

Allowed states: `red test`, `implementing`, `targeted green`, `full green`,
`blocked`, `complete`.

## Completion Checklist

- [x] R1 statement atomicity and savepoints
- [x] R2 database-authoritative metrics registry
- [x] R3 attached-database schema qualification
- [x] R4 transactional DROP/recreate identity
- [x] R5 atomic query/maintenance publication
- [x] R6 filesystem compaction error safety
- [x] R7 stats lock-order deadlock
- [x] R8 timestamp extremes and NULL constraints
- [ ] Formatting and strict Clippy clean
- [ ] Full verification gate green
