# Timeless signal data-plane servers

This is the release workspace for the three signal-specific Rust executables:

- `timeless-metrics-api`
- `timeless-logs-api`
- `timeless-traces-api`

The complete public route, configuration, authentication, lifecycle, backup,
and error inventory is the
[Rust signal server API reference](../docs/SERVER_API_REFERENCE.md). Artifact
pairing and replacement are defined by the
[compatibility statement](../docs/COMPATIBILITY.md) and
[upgrade guide](../docs/UPGRADE.md).

The servers own HTTP scheduling, bounded admission, query/response work,
maintenance wake-ups, and process lifecycle. They do not implement telemetry
storage. Every write, flush, optimize, rollup, retention, and query operation
crosses a public virtual-table or SQL surface from `libtimeless_ext`.

The server workspace is deliberately separate from the extension workspace.
rusqlite forbids its mutually exclusive `loadable_extension` (the extension)
and `load_extension` (the host executables) features in one Cargo dependency
graph. Keeping two checked workspaces preserves dynamic extension loading and
the public ABI instead of privately linking storage into the servers.

## Build and validate

From the repository root:

The canonical ordering and complete local, oracle, fault, embedding, and
packaging gate inventory is in [TESTING.md](../TESTING.md). The commands below
are the signal-server subset.

```bash
cargo build --release -p timeless-ext
cargo build --release --workspace --manifest-path servers/Cargo.toml

TIMELESS_EXT_PATH="$PWD/target/release/libtimeless_ext.so" \
TIMELESS_EXT_TEST_PATH="$PWD/target/release/libtimeless_ext.so" \
  cargo test --workspace --manifest-path servers/Cargo.toml -- --include-ignored

cargo clippy --workspace --all-targets --manifest-path servers/Cargo.toml -- -D warnings
```

Each executable supports `--version`, returning machine-readable build
identity. On normal startup it acquires its signal-specific owner lease, loads
the extension, validates `timeless_capabilities()`, refuses an incompatible
data ABI/server/extension version, preflights the database schema ledger, and
only then initializes its signal vtab. Listener binding occurs after all of
those checks.

The first additive database ledger is `_timeless_schema_migrations`. A
pre-ledger vtab database is version 0 and is upgraded idempotently to version 1
by its writer. Readers require the current ledger. A future schema or mismatched
data ABI fails before a vtab or initialization PRAGMA is created or changed.

TCP defaults are loopback-only:

| signal | default |
|---|---|
| metrics | `127.0.0.1:19439` |
| logs | `127.0.0.1:19429` |
| traces | `127.0.0.1:19449` |

Auth: **disabled** unless `TIMELESS_AUTH_MODE=required` — a fresh binary
starts open with zero configuration, like comparable telemetry servers.

All three expose `/live`, `/ready`, `/health`, and signal-specific stats. A
SIGINT or SIGTERM stops admission, drains accepted HTTP work, stops maintenance,
flushes through the public extension command, closes/reaps SQLite workers, and
releases the owner lease. Optional authentication and production limits are
available in the current release servers (opt-in via
`TIMELESS_AUTH_MODE=required`). Startup capability/schema fencing occurs before
listener binding; coordinated backup and complete WAL checkpointing use the
sole writer connection.
