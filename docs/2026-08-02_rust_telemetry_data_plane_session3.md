# Rust telemetry data plane — Session 3 result

Date: 2026-08-02  
Branch: `release/rust-telemetry-data-plane`  
Starting heads: extension `6eb405a3a736`; metrics `fed307c91333`; logs
`aeb311377da5`; traces `3763a0c58abf`  
Outcome: pass

Session 3 turned the Session 2 candidate writers into one automatic,
crash-safe release-startup gate per signal. The gates prepare exactly one
validated libSQL target or return one closed, actionable failure. They do not
silently start a legacy owner, select an unverified candidate, delete source
data, or implement any target storage detail outside the public extension.

Production adapters and the Rust process owners have deliberately not been
switched yet. Sessions 4 and 5 consume these gates before spawning the signal
owner and then change the default. Keeping that activation separate avoids a
half-promoted state in which storage is converted but authentication,
lifecycle, and adapter routing are not release-ready.

## Exact startup state machine

`TimelessMetrics.ReleaseStartup`, `TimelessLogs.ReleaseStartup`, and
`TimelessTraces.ReleaseStartup` expose `detect/2`, `prepare/2`, `stats/2`, and
digest-confirmed `cleanup_legacy/3`. Detection classifies every root as one of
the following and never guesses between owners:

| State | Meaning | Startup action |
|---|---|---|
| `fresh` | No recognized source, candidate, or target | Create the public signal vtab, sync it, and reopen it |
| `valid_libsql` | One compatible native target, with no retained source | Serve only after capability, schema, integrity, and signal checks |
| `legacy` | One complete recognized legacy generation | Fence it and automatically stage, validate, seal, and cut over |
| `resumable_migration` | A compatible journaled candidate matches the immutable source manifest | Revalidate committed work and resume its durable cursor |
| `completed_cutover` | The canonical target contains a valid cutover record tied to the retained source | Serve the target and report the rollback source |
| `incompatible_version` | Extension, data ABI, schema, or journal is newer/incompatible | Fail closed with the exact version/capability error |
| `corruption` | Header, integrity, vtab identity, payload, manifest, journal, checkpoint, or source pair is invalid | Fail closed without repair or fallback |
| `ambiguous_dual_store` | Two unlinked targets or legacy generations could own the signal | Fail closed and require operator resolution |

Zero-byte unrelated placeholders are ignored. Recognized symlinks, truncated
SQLite files, wrong-signal virtual tables, missing indexed blocks, mixed old
snapshot/SQLite generations, future ledgers, future journals, and source drift
are explicit failures. The logs and traces detector validates every referenced
legacy payload in a bounded one-block pass; it does not declare readiness from
index metadata alone.

Detection and migration parity scans use SQLite read-only connections. A
regression fingerprints schema, journal/event rows, and target records before
and after repeated detection and stats calls for all three signals. SQLite can
still create or retire transient WAL shared-memory bookkeeping, or checkpoint
already-committed WAL frames when the last reader changes; this physical
layout behavior is not reported as logical mutation. The immutable legacy
manifest remains byte/hash/mtime identical throughout every tested attempt.

## Ownership, resume, and cutover

Every mutating startup action first holds an OS-backed exclusive SQLite owner
transaction in `.timeless-migration/<signal>/owner.db`. A second process fails
immediately. Legacy SQLite sources are also held exclusively for the entire
conversion; the logs/traces bounded readers support exclusive mode directly,
and metrics retains a separate source-owner connection while its Rust cursor
reads blocks. A missing extension capability fails before target creation.

The Session 2 candidate remains side by side. Every restart checks the current
source manifest and semantically revalidates all committed target work before
using the journal cursor. Finalization then uses only public extension
operations:

1. flush the authoritative partial batch;
2. optimize/compact, produce metrics rollups, and checkpoint;
3. close and cold-validate counts, identities, timestamp ordering/ranges,
   float bits, labels, log metadata/order, trace relationships, and all rich
   span columns;
4. write and checkpoint a versioned `cutover_ready` record containing the
   signal, generation, exact source manifest, digest, and retained-source bit;
5. close, verify source stability again, atomically rename on the same
   filesystem, and fsync both relevant parent directories; and
6. reopen through the detector and require `completed_cutover` before
   readiness.

The cutover record is inside the candidate before rename. A crash after rename
therefore recognizes the completed target without depending on a later marker
write. Failpoints cover fresh creation, every migration transaction boundary,
before/after seal, before rename, after rename/before fsync, after source-parent
fsync, after final fsync, cleanup, and process death after seal. The returned
failure state is re-detected after each failpoint instead of being guessed.

## Rollback source and cleanup

Legacy data is never renamed, modified, or automatically removed. A completed
target reports `source_retained: true` and the exact manifest digest. The old
release can reopen the retained source after failed migration and after
successful cutover.

Cleanup is a separate operator action requiring that digest. It rechecks the
target marker, current source manifest, owner fencing, regular-file paths, and
every streaming SHA-256 before removing only manifest-listed files. It marks
the target `source_retained: false` only after deletion and a checkpoint.
Interrupted cleanup is never silently resumed by startup; the operator must
rerun the same explicit command. No code removes an unlisted path or follows a
symlink.

## Readiness and observations

`stats/2` exposes signal/state/readiness, phase, durable cursor, completed and
total records, percentage, retries/checkpoints, last checkpoint, measured
durable migration rate, ETA, source bytes, candidate/WAL/physical bytes, and
process HWM. Only `valid_libsql` and `completed_cutover` are ready.

The threshold-crossing measurements from Session 2 remain the migration
capacity baseline: 134,530 metrics points/s at 6.80 MiB migration RSS delta;
23,890 log records/s at 39.11 MiB; and 19,097 rich spans/s at 64.13 MiB. Session
3 adds state inspection, exclusive fencing, a final seal, directory fsyncs,
and one final cold semantic reopen. Session 7 will measure full production
startup migration rate, tail latency, WAL behavior, and sustained HWM instead
of manufacturing a small-fixture speed claim here.

## Regressions discovered and fixed

1. Candidate checkpoint validation reopened databases with ordinary
   read/write handles. It now loads the extension through dedicated read-only
   handles, while final phase/marker updates remain deliberately read/write.
2. The metrics migration child ignored an explicit extension path and could
   accidentally load the old bundled extension depending on test order. The
   path is now propagated through the writer and every reader; isolated tests
   no longer depend on ambient application state.
3. A non-empty metrics source entirely inside an unsettled hourly bucket has
   no valid persisted rollup chunk. The validator no longer invents one; a
   separate threshold-crossing fixture still proves exact persisted hourly
   count/min/max/last values.
4. A former SQLite owner can leave a transient zero-byte WAL. Source manifests
   now retain non-empty WAL data and exclude only the empty transient, avoiding
   a false source-drift failure after an untrappable process death.
5. Failpoint responses always claimed `resumable_migration`, even after the
   atomic rename had made the observable state `completed_cutover`. Responses
   now rerun the detector and report the actual state.
6. Initial physical-byte regression assertions treated SQLite WAL-to-main
   checkpoint movement as data mutation. The durable invariant is now tested
   directly: exact schema, journal/event history, target records, and immutable
   legacy source contents must not change during detection.

No failed optimization remains in the tree.

## Validation evidence

Passing gates:

```bash
cd timeless_metrics
TIMELESS_EXT_PATH=../timeless-libsql/target/release/libtimeless_ext.so mix test
mix format --check-formatted
mix compile --warnings-as-errors

cd ../timeless_logs
mix test
mix format --check-formatted
mix compile --warnings-as-errors

cd ../timeless_traces
mix test
mix format --check-formatted
mix compile --warnings-as-errors
```

Results:

- metrics: 489 tests passed;
- logs: 226 tests passed;
- traces: 196 tests passed; and
- all three formatting, warnings-as-errors, and whitespace gates passed.

The startup suites pin all eight states, extension/schema/journal downgrade
refusal, every recognized legacy generation, missing/corrupt payloads,
ambiguous targets, active owners, insufficient space and disk-full propagation,
logical checkpoint immutability, process kill/restart, every startup filesystem
boundary, semantic reopen, retained-source reopen, digest refusal, interrupted
cleanup, and exact post-cutover records.

## Exit criterion

Pass. Every startup gate returns one validated ready libSQL path or one
actionable closed state. Interrupted attempts retain a byte-identical legacy
source that the previous release can open. Atomic cutover is recognized from
the pre-rename marker after process death, and no startup path deletes the
rollback source.

Session 4 must invoke this gate before spawning each Rust signal owner and
surface its readiness/stats through Phoenix. Session 5 changes adapters and
defaults only after that process/auth boundary is complete.
