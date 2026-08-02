# Rust telemetry data plane — Session 2 result

Date: 2026-08-02  
Branch: `release/rust-telemetry-data-plane`  
Starting extension head: `ecc32eb`  
Outcome: pass

Session 2 added immutable, bounded legacy readers and restartable candidate
writers for metrics, logs, and traces. Migration writes exclusively through
the public `timeless-libsql` virtual tables and maintenance commands. It does
not select a production store, rename data, delete data, or run during
startup; those ownership and cutover responsibilities remain Session 3.

## Delivered migration contract

Each signal stages a side-by-side candidate below
`.timeless-migration/<signal>/` and records progress in a versioned SQLite
journal. Before the first write it validates the source generation, extension
capability/data ABI, schema, paths, file types, source sizes, and available
disk. The source manifest includes every durable input file and deliberately
excludes SQLite's transient shared-memory file.

Every full public batch and its cursor, record count, digest state, and event
are committed in one target transaction. A crash before that transaction
leaves no imported records; a crash after it resumes at the committed cursor.
Final partial batches use the extension's public flush operation in the same
transaction. Phase transitions are journal transactions too. Injected
failpoints cover before-batch, disk-full, after-batch/before-journal,
after-journal/before-commit, and after-committed-checkpoint boundaries.

The writers retain the extension's authoritative behavior:

- metrics uses resolved public batches capped at 4,096 points per series,
  then public flush, compact, rollup, and checkpoint commands;
- logs uses rich-v1 public batches capped at 8,192 entries, followed by
  public flush, optimize, and checkpoint; and
- traces uses rich-span-v1 public batches capped at 8,192 spans, followed by
  public flush, optimize, and checkpoint.

No importer knows a block layout, compression codec, index layout, rollup
table, or private write path in the target. The promoted extension capability
handshake is mandatory. An old bundled metrics extension without that
handshake fails closed; replacing that packaged artifact is intentionally a
later release-artifact gate.

## Immutable legacy readers

Metrics now has a separate read-only Rust resource. It never invokes recovery,
removes temporary files, or accepts a mutation NIF. It rejects symlinks,
non-regular files, recovery artifacts, corrupt catalog/chunk data, and unknown
formats. The cursor is stable across overlapping chunks and duplicate points:
timestamp, float bits, relative path, byte offset, and ordinal. Exact series
identity is resolved before reading, so a label subset cannot copy points from
a different legacy series. A page is capped at 4,096 points and its working
set is one chunk plus the bounded cursor heap.

Logs and traces recognize both released legacy families without calling the
existing mutating upgrader:

- read-only SQLite index schema generations 1 and 2; and
- safe snapshot plus read-only `disk_log` generations, including inline
  snapshot blocks and delete/compact replay.

They validate block paths, stored and decoded lengths, count, metadata, and
codec before yielding one block. Raw, zstd, and historical OpenZL blocks are
supported. Stored blocks are capped at 64 MiB, decoded blocks at 256 MiB, and
pages at 8,192 records. Source file hashes, sizes, and mtimes are checked again
after validation.

## Exact cold validation

After the last record, each candidate is flushed, maintained, checkpointed,
closed, and reopened without the writer. SQLite integrity, schema/capability,
count, timestamp range, deterministic identity digest, and source manifest
must all agree before the journal can enter `verified`.

Signal-specific oracles additionally prove:

- metrics catalog identities, empty series, duplicate multiplicity, IEEE-754
  float bits including negative zero, raw points, and generated hourly
  rollups (count/min/max/last and persisted rollup chunks);
- logs microsecond timestamps, full severity vocabulary, typed nested
  metadata including null, equal-timestamp product order, and exact cold
  query contents; and
- traces IDs, parents and relationships, nanosecond start/duration, every
  kind/status, status descriptions, typed span/resource/scope attributes,
  events, and complete rich-span columns.

An absent trace service remains absent from extension service/operation
discovery. The stored empty value renders as `unknown` at the Jaeger response
boundary, matching the released product without changing discovery/filter
semantics.

## Measured development fixture

These are one local Linux development run, not production capacity claims.
Each fixture crosses the authoritative batch threshold. Rate is completed,
durable, cold-validated work. RSS delta is the maximum RSS observed during the
migration minus the immediately preceding baseline; process HWM is the VM's
lifetime high-water mark and can include earlier compilation/tests.

| Signal | Durable work | Rate | Scan | Public write | Maintenance | Checkpoint | Migration RSS delta | Source | Candidate logical | Final physical | Peak candidate physical | WAL |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Metrics | 8,195 points | 134,530 points/s | 2.38 ms | 6.31 ms | compact 1.66 ms; rollup 0.06 ms | 0.22 ms | 6.80 MiB | 312,734 B | 442,368 B | 475,136 B | 1,476,680 B | 0 B |
| Logs | 8,193 records | 23,890 records/s | 39.77 ms | 97.49 ms | optimize 43.01 ms | 0.93 ms | 39.11 MiB | 1,817,228 B | 1,179,648 B | 1,212,416 B | 1,493,088 B | 0 B |
| Traces | 8,193 spans | 19,097 spans/s | 66.83 ms | 149.56 ms | optimize 42.55 ms | 1.96 ms | 64.13 MiB | 5,413,937 B | 4,407,296 B | 4,440,064 B | 4,407,296 B | 0 B |

The traces fixture is intentionally rich and therefore the largest source and
working set. Boundedness is principally enforced by record pages and validated
block caps; Session 7's production soak gate will establish long-running RSS
under realistic source sizes.

## Discovered regressions and disposition

1. The first metrics reader matched labels as a subset and could copy a strict
   superset series. Exact catalog identity is now mandatory and regressed.
2. A timestamp-only metrics cursor lost duplicates across overlapping chunks.
   The stable physical/ordinal cursor now preserves multiplicity and float
   bits across pages.
3. Reusing the normal metrics opener could run recovery or remove temporary
   files. The migration reader is a separate strict read-only resource and
   rejects recovery state instead.
4. A full authoritative batch followed by a partial tail exposed a transaction
   boundary error around flush. Batch, tail flush, and journal behavior are
   now pinned at every crash point.
5. The old metrics-bundled extension has no capability function. Migration
   now refuses it rather than guessing compatibility. Release packaging must
   install the promoted extension before default cutover.
6. The initial logs check used a nonexistent `public_batches` capability key.
   It now consumes the released `signals.<signal>.batches` document.
7. Elixir `nil` is not directly JSON-encodable by Erlang's JSON API. Candidate
   writers recursively normalize it to JSON null and validate every nested
   value before admitting a batch.
8. Rich legacy spans can contain an end time inconsistent with start plus
   duration. The public target stores duration, so migration now fails closed
   with the span identity instead of dropping that fidelity.
9. Encoding an absent service as the string `unknown` changed service
   discovery and filtering. The public rich batch now preserves absence as an
   empty stored service; the extension omits it from postings/catalogs and the
   Jaeger renderer alone supplies `unknown`.
10. Historical OpenZL log blocks already collapsed critical, alert, and
    emergency into the old error partition. Those original severities are
    irrecoverable. Migration honestly preserves the source's query-visible
    `error` value; it does not fabricate fidelity.
11. SQLite WAL/shared-memory sidecars can disappear asynchronously when the
    former owner closes. Tests now wait for owner shutdown, and manifests omit
    only the transient shared-memory file. Session 3 must fence the owner and
    settle/checkpoint a legacy SQLite source before taking its manifest.

No failed optimization remains in the tree.

## Validation evidence

Passing gates:

```bash
cargo fmt --all -- --check
cargo test -p timeless-core
cargo test -p timeless-ext
cargo build --release -p timeless-ext

cd servers
TIMELESS_EXT_PATH="$PWD/../target/release/libtimeless_ext.so" \
  cargo test -p timeless-traces-api

cd timeless_metrics
TIMELESS_EXT_PATH="../timeless-libsql/target/release/libtimeless_ext.so" mix test

cd timeless_logs && mix test
cd timeless_traces && mix test
```

Results:

- metrics Rust reader: 35 tests passed; complete application: 484 passed;
- logs application: 218 passed;
- traces application: 190 passed;
- extension: 70 core unit tests, 36 extension unit tests, and all selected
  integration/doc tests passed; and
- promoted traces server: 23 unit/integration contracts passed against the
  rebuilt release extension.

The crash suites verify zero journal progress before a failed transaction,
exact 4,096/8,192 committed progress after the post-commit failpoint, retry
counts, final counts/digests, cold query results, zero final WAL, and unchanged
source snapshots. Insufficient-space preflight creates no candidate, and
injected disk-full leaves no admitted batch.

## Exit criterion

Pass. Deterministic edge and threshold-crossing fixtures for every recognized
legacy generation migrate through public extension interfaces with bounded
pages and caps. Sources remain byte/mtime identical. Every injected batch
crash resumes without loss or duplication; insufficient disk and disk-full
fail closed; and full signal-specific semantic oracles pass after public
maintenance, checkpoint, close, and cold reopen.

Automatic state detection, exclusive legacy-owner settlement, cutover,
readiness/progress integration, cleanup, and rollback are deliberately absent
until Session 3. A Session 2 candidate is never selected as production data.
