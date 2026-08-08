# Timeless query harness

This private Rust binary owns the repository's query-documentation contracts,
pinned upstream-oracle refreshes, public SQL recipe execution, and reproducible
query evidence capture. It is a detached Cargo workspace so query tooling does
not become part of the extension or signal-server dependency graphs.

The repository-wide command order, prerequisites, and distinction between
local, live-oracle, evidence, fault/soak, embedding, packaging, and benchmark
gates are maintained in [TESTING.md](../../TESTING.md).

Run every unit and negative-path regression:

```bash
cargo test --manifest-path tools/query-harness/Cargo.toml --locked
cargo clippy --manifest-path tools/query-harness/Cargo.toml \
  --all-targets --locked -- -D warnings
```

Validate checked-in contracts without network access:

```bash
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- contracts
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle validate
```

Execute every fenced recipe in `docs/QUERY_SQL_EQUIVALENTS.md` against the
real public extension:

```bash
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- \
  sql --extension target/release/libtimeless_ext.so
```

Run the two-minute production fault gate against release builds:

```bash
cargo run --release --manifest-path tools/query-harness/Cargo.toml \
  --locked -- production \
  --mode short \
  --output /tmp/timeless-production-short.json
```

The SQL runner links to the host SQLite library because it tests the same
loadable-extension ABI and standard SQLite math surface used by the CLI. The
host must provide SQLite 3.34.1 or newer with extension loading enabled; math
recipes additionally require the standard SQLite math functions documented by
the cookbook.

Commands that deliberately contact pinned containers or run release binaries
are documented in `docs/QUERY_ORACLES.md` and `docs/QUERY_EVIDENCE.md`.
Evidence capture fails on a dirty worktree and on any extension or binary whose
embedded build commit differs from `HEAD`.

The modules have narrow responsibilities:

- `contracts` parses matrices and documentation, validates legal states and
  ownership, checks local links, and proves shipped-row test references;
- `oracle` validates immutable pins, owns temporary containers, writes the
  deterministic raw-Snappy Remote Write fixture, and compares exact API cases;
- `sql_equivalents` extracts and executes public SQL recipes and checks their
  semantic boundary fixtures; and
- `evidence` owns signal processes, fixture admission and durability barriers,
  query latency/counter/cardinality measurements, cancellation contracts,
  storage accounting, and RSS HWM capture; and
- `production` owns the completion-aware mixed-signal fault and soak gate,
  process generations, backups, startup/storage/resource faults, durable
  completion, and its stable JSON report; and
- `gate` replaces language-specific test drivers with Rust binary fixtures,
  persistent SQLite hosts, packed-frame decoders, focused correctness cases,
  crash-workload generation, and dbhealth lifecycle checks used by the shell
  release gate.

Add unit coverage for parser and failure behavior here. Product semantics must
still be pinned through the real extension and signal-server contract tests;
this tool never substitutes an in-memory model for those paths.
