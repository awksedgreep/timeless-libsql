# Testing and benchmark runbook

This is the canonical entry point for validating `timeless-libsql`. Run all
commands from the repository root unless a section says otherwise. The
[query release report](docs/QUERY_RELEASE_REPORT.md) records the most recent
complete result; this file defines how to reproduce it.

## Requirements

- Rust 1.95 or newer and Cargo.
- `sqlite3` 3.34.1 or newer with loadable extensions enabled.
- A C compiler and CMake for bundled SQLite and compression dependencies.
- Docker only for refreshing the pinned Prometheus, VictoriaMetrics, and
  VictoriaLogs oracles.
- Linux for the complete signal-process fault and RSS-watermark gate. Core,
  extension, SQL, and most API tests also run on macOS.

## Scratch storage and cleanup

Test and benchmark scratch data is temporary by default. Rust `tempfile`
owners and shell `trap` handlers remove databases, WAL/SHM files, and server
logs on both success and ordinary failure. This matters on systems where
`/tmp` is tmpfs: a leaked benchmark database consumes memory or swap, not just
disk space.

The query evidence runner also removes failed captures by default. Pass
`--keep-failed` only when you need its diagnostic database and server logs;
the error prints their retained path. The production gate removes its
generated scratch tree, but an explicit `--data-dir NEW_DIRECTORY` is
operator-owned and retained. General benchmarks remove their databases; pass
`--keep-dir NEW_DIRECTORY` to `bench`, `bench-logs`, or `bench-traces` only
when you intend to inspect them afterward. Prefer a disk-backed path outside
`/tmp` for retained or large runs.

Result files named by `--output` and database paths explicitly supplied by a
caller are also caller-owned. Remove them when their evidence is no longer
needed. A successful test must never silently retain an internally selected
scratch path.

On macOS, `/usr/bin/sqlite3` disables extension loading. Install SQLite with
Homebrew and put its `bin` directory first in `PATH` before running the shell
suites.

## Tooling migration status

The extension, API, query-contract, SQL-equivalence, fixture, crash-workload,
and persistent-host test logic is Rust. Shell files only orchestrate the
SQLite CLI and Rust binaries.

The following older utilities are still Python and are being replaced in
bounded sessions. Their replacement commands and this inventory must change
in the same commit:

- `servers/crates/timeless-metrics-api/bench_shell.py`;
- `servers/crates/timeless-traces-api/bench/*.py`; and
- `tools/bench/session6_log_compaction.py`.

Do not describe the complete repository as Python-free until
`git ls-files '*.py'` returns no paths and the final command below passes:

```sh
test -z "$(git ls-files '*.py')"
```

## Sixty-second smoke test

```sh
cargo build --release -p timeless-ext --locked
sqlite3 /tmp/timeless-smoke.db \
  ".load target/release/libtimeless_ext" \
  "CREATE VIRTUAL TABLE m USING timeless_metrics;
   INSERT INTO m(name, ts, value) VALUES ('cpu', 1, 42.5);
   INSERT INTO m(m) VALUES ('flush');
   SELECT * FROM m;"
```

The final command prints `cpu|1|42.5|{}`.

## Complete local correctness gate

The order matters. Build the current extension before running API contracts
so a stale `target/debug` or `target/release` library cannot masquerade as the
current source.

### 1. Extension and engine workspace

```sh
cargo fmt --all -- --check
cargo build --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' \
  cargo doc --workspace --no-deps --locked
```

Do **not** run `cargo test --workspace --all-targets` at the repository root.
That option overrides the standalone `dbhealth-ext` test-harness boundary and
tries to link two loadable extensions that intentionally export the same
SQLite entry-point name. `cargo test --workspace` is the supported root test;
dbhealth is validated separately below. Root Clippy may use `--all-targets`
because it checks rather than links the conflicting test binary.

What lives where:
- `crates/timeless-core/tests/roundtrip.rs` — write → query-before-flush → flush →
  aggregate → shutdown → cold-recovery lifecycle
- `crates/timeless-core/tests/compression_honesty.rs` — every point of 1M verified
  bit-exact after recovery; bytes measured from disk, not bookkeeping
- `crates/timeless-core/tests/store_seam.rs` — FsStore/ChunkStore seam + recovery
- `crates/timeless-core/tests/txn_journal.rs` — rollback of buffers, intra-txn
  flushes, optimize-in-txn
- `crates/timeless-core/tests/dup_min_ts.rs` — the (series,min_ts) shadowing
  regression (found by the oracle, fixed here and upstream)
- unit tests inside `src/blocks/` and `src/spans/` — codecs, partitioned
  flush, term pruning read-count proofs, projection/late-materialization, and
  merge caps

### 2. Release extension and signal APIs

Build identities are part of the test contract:

```sh
build_commit="$(git rev-parse HEAD)"

TIMELESS_BUILD_COMMIT="$build_commit" \
  cargo build --release -p timeless-ext --locked

TIMELESS_BUILD_COMMIT="$build_commit" \
  cargo build --release --workspace \
    --manifest-path servers/Cargo.toml --locked

cargo fmt --manifest-path servers/Cargo.toml --all -- --check

TIMELESS_EXT_PATH="$PWD/target/release/libtimeless_ext.so" \
TIMELESS_EXT_TEST_PATH="$PWD/target/release/libtimeless_ext.so" \
  cargo test --workspace --manifest-path servers/Cargo.toml \
    --locked -- --include-ignored

cargo clippy --workspace --all-targets \
  --manifest-path servers/Cargo.toml --locked -- -D warnings

RUSTDOCFLAGS='-D warnings' \
  cargo doc --workspace --manifest-path servers/Cargo.toml \
    --no-deps --locked
```

Use `libtimeless_ext.dylib` instead of `.so` on macOS. The
`--include-ignored` run is intentional: it enables the metrics and logs
contracts that require the real release extension. It also covers trace,
OTLP, Jaeger, rich-field fidelity, shutdown, backup, queue, cancellation, and
8,192-span batching behavior.

### 3. Rust query harness and executable documentation

```sh
cargo test --manifest-path tools/query-harness/Cargo.toml --locked
cargo clippy --manifest-path tools/query-harness/Cargo.toml \
  --all-targets --locked -- -D warnings

cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- contracts
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- oracle validate
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- sql \
  --extension "$PWD/target/release/libtimeless_ext.so"
```

These commands validate matrix IDs and states, shipped-row test references,
documentation links and inventories, immutable oracle pins, release evidence,
and all public SQL equivalents. The SQL command currently executes 135
recipes and 173 statements through the real extension.

### 4. SQLite CLI, transactions, crashes, and dbhealth

```sh
tests/cli.sh
tests/dbhealth.sh

for section in r1 r2 r3 r4 r8 logs-rich; do
  TIMELESS_EXT="$PWD/target/release/libtimeless_ext.so" \
    tests/correctness.sh "$section"
done
```

`tests/cli.sh` is the comprehensive 45-section direct-SQL gate. It includes
150,000 randomized operations, five random-timing `SIGKILL` recoveries,
transaction/savepoint rollback, cold reopen, storage/index consistency, all
three signals, public stats and query surfaces, and the SQL cookbook. Run
`tests/crash.sh target/release/libtimeless_ext.so` only when you want the
five-round crash subset independently.

#### The crash suite

In those five rounds, the Rust query harness owns an unreaped `sqlite3` child
running a long ingest with periodic flushes and watermark logging, sends
`SIGKILL` to that exact child at a random moment, reaps it, reopens, then
asserts `PRAGMA integrity_check` is clean, all flushed watermarks are present,
and no `_terms`/`_trace_blocks`/`_duration_bounds`/`_attribute_blooms` row
dangles. Every current trace block must also have valid duration bounds and
exact configured-attribute results. The durability contract being proven is:
**flushed = durable, buffered = lost, never corrupt.**

`tests/dbhealth.sh` builds and loads `libdbhealth_ext` separately. Never build
`timeless-ext` and `dbhealth-ext` with two `-p` arguments in one Cargo
invocation; their feature variants are deliberately separate products.

### 5. Embedded Rust and direct libSQL

```sh
cargo run --locked -p timeless-ext \
  --no-default-features --features embedded --example embedded

cargo run --manifest-path tools/libsql-check/Cargo.toml --locked -- \
  target/release/libtimeless_ext.so
```

The first command statically registers the production telemetry modules in a
Rust host. The second uses libSQL 0.9.30 directly, loads the release extension
on multiple connections, verifies all three signals, closes the database, and
repeats exact reads after reopen.

## Pinned upstream semantic oracles

Normal development validates checked-in fixtures without network access via
`oracle validate`. Refreshing the actual upstream evidence is explicit,
requires Docker and network access, and uses immutable image digests:

```sh
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- oracle probe
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- oracle prometheus-smoke
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- oracle prometheus-api
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- oracle victoria-metrics-api
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml \
  --locked -- oracle victoria-logs-api
```

These commands own and remove uniquely named temporary containers. They may
refresh checked fixture files, so review `git diff` afterward. See
[the oracle contract](docs/QUERY_ORACLES.md) before changing a pin.

## Query performance evidence

Evidence capture requires a clean worktree and exact matching extension and
server build identities:

```sh
build_commit="$(git rev-parse HEAD)"
TIMELESS_BUILD_COMMIT="$build_commit" \
  cargo build --release -p timeless-ext --locked
TIMELESS_BUILD_COMMIT="$build_commit" \
  cargo build --release --manifest-path servers/Cargo.toml \
    -p timeless-metrics-api -p timeless-logs-api --locked

cargo run --release --manifest-path tools/query-harness/Cargo.toml \
  --locked -- evidence \
  --output "/tmp/timeless-query-evidence.json"
```

The JSON records durable completed work, p50/p95/p99, cardinality, public
storage work, response and extension bytes, cancellation behavior, physical
storage, and RSS HWM. Do not commit a new evidence artifact without updating
the owning matrix rows and findings in the same session.
Use `--keep-failed` only for a failure you are actively diagnosing.

## Production fault and soak gates

The Rust query harness owns the production process, fault, and soak gate:

```sh
cargo run --release --manifest-path tools/query-harness/Cargo.toml \
  --locked -- production \
  --mode short \
  --output /tmp/timeless-production-short.json

cargo run --release --manifest-path tools/query-harness/Cargo.toml \
  --locked -- production \
  --mode release \
  --output /tmp/timeless-production-two-hour.json
```

Short mode defaults to 120 seconds. Release mode requires at least two hours
per concurrently running signal. Both exercise durable writes and queries,
backups, cancellation/disconnect storms, startup descriptor and disk faults,
graceful and abnormal restarts, storage/WAL/resource watermarks, and final
durability barriers. The runner fails the command when the report verdict is
not `passed` and preserves a caller-supplied `--data-dir` for diagnosis.

## Native package validation

The detached Rust release tool builds and validates one native candidate:

```sh
cargo run --release --manifest-path tools/release-tool/Cargo.toml \
  --locked -- \
  --target x86_64-unknown-linux-gnu \
  --output /tmp/timeless-dist
```

This is a local candidate build, not a tag or publication. The command checks
the native target, locked builds, exact binary/extension identity,
deterministic archive metadata, inner and outer checksums, manifest, SPDX
SBOM, license notices, and an isolated install/remove drill with data and
configuration sentinels. See [ARTIFACTS.md](docs/ARTIFACTS.md).

## Benchmarks

The maintained general extension benchmarks already live in the detached
Rust `tools/bench` crate:

```sh
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin bench -- target/release/libtimeless_ext.so
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin bench-logs -- target/release/libtimeless_ext.so
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin bench-traces -- target/release/libtimeless_ext.so
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin bench-codec
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so
```

### Trace duration-pruning evidence

The Rust query harness can measure a copied pre-extrema trace database before
and after public optimize backfill. It deliberately mutates the supplied copy;
never point it at the only copy of a database:

```sh
cp --reflink=auto /path/to/legacy-traces.db /path/to/scratch-traces.db
cargo run --release --manifest-path tools/query-harness/Cargo.toml --locked -- \
  gate trace-duration-evidence \
  --extension target/release/libtimeless_ext.so \
  --database /path/to/scratch-traces.db \
  --table traces --service payments \
  --minimum-duration-ns 9000000000000000000 \
  --iterations 50 --warmup 5 --wal
```

Choose a service present in the fixture and a duration above its maximum. The
command fails if the legacy phase does not actually decode candidate blocks,
if the query returns a row, if optimize leaves an unknown block, or if the
post-backfill phase considers/decodes a persisted block. Its JSON reports
p50/p95/p99, cardinality, candidate/payload/decoded work, logical and physical
storage, optimize backfill time/bytes/entries, and process RSS HWM. This is a
same-fixture storage optimization measurement, not a Victoria/Jaeger
competitive benchmark.

### Trace query enhancement baseline

The Rust query harness can create a temporary deterministic rich-span database,
start the release trace server, measure broad Jaeger reads, stop it cleanly,
and then measure `timeless_trace_buckets` in a fresh direct-SQL process:

```sh
cargo build --release
cargo build --release --manifest-path servers/Cargo.toml --bin timeless-traces-api
cargo build --release --manifest-path tools/query-harness/Cargo.toml
tools/query-harness/target/release/timeless-query-harness trace-baseline \
  --output docs/evidence/local_trace_query_baseline.json
```

Session 7's opt-in trace-attribute A/B uses separate fresh processes at the
same clean commit. Omit the flag for the public JSON1/write-path control and
include it for the two configured `/count` and `/bool` fields:

```sh
tools/query-harness/target/release/timeless-query-harness trace-baseline \
  --output docs/evidence/local_trace_attributes_unindexed.json
tools/query-harness/target/release/timeless-query-harness trace-baseline \
  --attribute-indexes \
  --output docs/evidence/local_trace_attributes_indexed.json
```

Both reports include insert/optimize time, file/WAL state before optimize and
checkpoint, builder/query HWM, and JSON1 attribute controls. The indexed run
also measures the hidden-filter candidate. Compare equal commits and fixtures;
the command rejects dirty tracked source or stale extension/server identities.

Run from a clean tracked commit: the command rejects stale extension or server
build identities. The default workload is 16 public 8,192-span batches, 20
measured iterations, and three warmups. It cleans its temporary database and
server log on both success and ordinary failure. Pass `--retain-on-failure`
only when you deliberately need a failed fixture for diagnosis; remove it
afterward. The output records latency tails, cardinality/result bytes,
extension work, durable fixture construction, storage/WAL/freelist state, and
fresh-server RSS/HWM for each Jaeger shape, and isolated direct-SQL RSS HWM.
It also profiles service, operation-name, kind, and status postings over one-
and four-time-box windows and fails if any query reads a time-disjoint block.
The direct-SQL child additionally compares the readable two-consumer trace
summary CTE with the published single-scan conditional-aggregate recipe,
asserting exact retained-row cardinality, result bytes, public extension work,
and process RSS HWM.

Datasets are generated by a deterministic PRNG (`src/datasets.rs`) — same
data every run, on every machine, so numbers are comparable across hosts.

The general signal benchmarks clean their scratch databases by default. Add
`--keep-dir /disk/backed/new-directory` after the extension path when an
inspection copy is genuinely required.

Several historical API and maintenance benchmark drivers are still Python;
they are named in the migration inventory above. Their Rust ports must retain
the same fixture, completed-work barrier, percentile method, storage counters,
and RSS accounting before the Python originals are removed.

For comparable numbers:

1. use release builds;
2. run twice and report the second run;
3. close connections before reporting main-file size;
4. separate admission from flush/optimize durability time;
5. report logical and physical storage plus WAL/SHM; and
6. identify the exact commit, host, fixture, and result cardinality.

## Test ownership and automatic execution

This repository has no scheduled, branch-push, or pull-request test trigger.
Manual workflows run only when explicitly dispatched; release builds run only
for `v*` tags. This runbook is authoritative for local execution. Adding or
changing an automatic trigger requires a separate explicit owner request.
