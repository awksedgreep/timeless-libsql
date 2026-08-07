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

- `tools/production_gate.py`: production fault and soak gate;
- `tools/package_release.py`: deterministic native artifact packager;
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

## Production fault and soak gates

Until its Rust replacement lands, the production process/fault runner remains
the explicitly listed Python exception:

```sh
python3 tools/production_gate.py \
  --mode short \
  --output /tmp/timeless-production-short.json

python3 tools/production_gate.py \
  --mode release \
  --output /tmp/timeless-production-two-hour.json
```

Short mode defaults to 120 seconds. Release mode requires at least two hours
per concurrently running signal. Both exercise durable writes and queries,
backups, cancellation/disconnect storms, startup descriptor and disk faults,
graceful and abnormal restarts, storage/WAL/resource watermarks, and final
durability barriers. The Rust port must preserve the JSON schema and all
failure gates before this section changes commands.

## Native package validation

Until its Rust replacement lands, candidate packaging remains the second
explicit Python exception:

```sh
python3 tools/package_release.py \
  --target x86_64-unknown-linux-gnu \
  --output /tmp/timeless-dist
```

This is a local candidate build, not a tag or publication. The replacement
must preserve deterministic archives, inner and outer checksums, exact build
identity, manifest, SPDX SBOM, license notices, install/remove behavior, and
data/config preservation. See [ARTIFACTS.md](docs/ARTIFACTS.md).

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
