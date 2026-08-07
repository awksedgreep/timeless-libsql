# Embedded Rust guide

`timeless-ext` can register the production metrics, logs, traces, and query
modules directly on a `rusqlite::Connection`. This path runs entirely inside
the application process: it starts no HTTP server, BEAM process, NIF, or
sidecar, and it does not require the Timeless UI.

The canonical SQL schemas, commands, formats, and limits remain the
[SQLite extension API](SQL_API_REFERENCE.md). Embedding changes how the
modules are registered, not what they store.

## Link the embedded feature

The loadable-extension and linked-host builds use different rusqlite SQLite
API modes. Select exactly one:

- default features build `libtimeless_ext.so` or `.dylib` for SQLite/libSQL
  `load_extension`;
- `default-features = false, features = ["embedded"]` links the registration
  API into a Rust host.

For a sibling checkout:

```toml
[dependencies]
rusqlite = "0.40.1"
timeless-ext = {
  path = "../timeless-libsql/crates/timeless-ext",
  default-features = false,
  features = ["embedded"]
}
```

The host and `timeless-ext` must resolve one compatible rusqlite 0.40 package
because `register_telemetry` accepts that package's `Connection` type. Do not
enable both `entrypoints` and `embedded`; the crate rejects that configuration
at compile time. A future published crate version should replace the path with
an exact compatible package requirement, not a floating Git branch.

The checked dependency and feature inventory is source-derived:

<!-- public-embedding-contract:start -->

| Contract key | Current value |
|---|---|
| `timeless_ext_embedded_feature` | `embedded` |
| `timeless_ext_loadable_feature` | `entrypoints` |
| `rusqlite_version` | `0.40.1` |
| `direct_libsql_gate_version` | `0.9.30` |
| `static_example` | `crates/timeless-ext/examples/embedded.rs` |
| `dynamic_libsql_gate` | `tools/libsql-check/src/main.rs` |

<!-- public-embedding-contract:end -->

## Register and use the public modules

This repository contains an executable, fidelity-checking example:

```sh
cargo run -p timeless-ext \
  --no-default-features --features embedded --example embedded
```

The minimal registration sequence is:

```rust
use rusqlite::Connection;

fn open(path: &str) -> rusqlite::Result<Connection> {
    let connection = Connection::open(path)?;
    timeless_ext::register_telemetry(&connection)?;
    Ok(connection)
}
```

Call `register_telemetry` once on every newly opened connection before that
connection prepares Timeless SQL. Then probe `timeless_capabilities()` and
reject an unsupported data ABI or a missing module/format needed by the host.
The call installs exactly the three production storage modules, their public
query modules, and the capability scalar. It does not install the
compatibility-only `timeless_spike` module or dbhealth.

To embed database health collection too, explicitly call
`timeless_ext::register_dbhealth(&connection)` and follow the separate
[dbhealth lifecycle](DBHEALTH.md). It is not implicitly part of telemetry
registration.

## Connection and ownership model

Connections in one process that open the same database and virtual-table name
share one Timeless engine. A bounded table-scoped writer gate serializes
writes. Readers on another connection receive a retryable busy-style error
while a write transaction has private shadow state; treat it like
`SQLITE_BUSY`. Registration is still per connection because SQLite's module
catalog is per connection.

One process must own writes to a database. The embedded API does not acquire
the signal-server advisory lease and cannot fence an unrelated process that
ignores application ownership. Do not point an embedded writer, `sqld`, and a
Timeless Rust signal server at the same writable file concurrently.

The host owns SQLite configuration. Set journal, synchronous, busy timeout,
connection count, and filesystem policy before serving traffic. The Timeless
signal servers use WAL and a sole ordered writer; a custom host must choose
and test its own equivalent policy.

## Transactions and durability

Timeless writes participate in the caller's SQLite transaction. A rollback
restores both buffered state and shadow-table changes made by a flush or
maintenance command in that transaction. Use ordinary `BEGIN`, `COMMIT`, and
`ROLLBACK`; never write private shadow tables.

Buffered rows are queryable but are not a process-crash durability promise.
The durable boundary is a successful public `flush` command:

```sql
INSERT INTO metrics(metrics) VALUES ('flush');
INSERT INTO logs(logs)       VALUES ('flush');
INSERT INTO traces(traces)   VALUES ('flush');
```

The authoritative automatic thresholds are 4,096 points per metric series,
8,192 log entries, and 8,192 spans. A graceful embedded shutdown must stop
producers, wait for admitted work, flush every created Timeless table, commit,
perform the host's required WAL checkpoint, close readers, and finally close
the writer. SIGKILL can lose the unflushed tail but cannot turn it into a
claimed successful durable write.

## Maintenance and retention

The virtual tables are passive. They do not start a scheduler in an embedded
host. Run the public commands on the owning writer at a cadence appropriate
for the application:

```sql
INSERT INTO metrics(metrics) VALUES ('compact');
INSERT INTO metrics(metrics) VALUES ('rollup');
INSERT INTO logs(logs)       VALUES ('optimize:65536');
INSERT INTO traces(traces)   VALUES ('optimize:65536');
```

Creation-time retention is applied at the storage maintenance boundaries
documented in the SQL reference. Explicit `prune:<timestamp>` remains an
operator decision and uses each table's persisted timestamp unit. Query work
limits are also caller responsibilities; bind `max_work_points` or
`max_work_entries` and bound returned rows/result bytes before exposing SQL to
untrusted clients.

## Backup, restore, and replication boundary

For an online backup, stop admission to the owning writer, flush all signal
tables, commit, and use SQLite's online-backup API from that same coordinated
owner. For an offline copy, stop all owners and preserve the database together
with any outstanding WAL state, or require a complete checkpoint first. A
bare hot copy of only the main database is not a backup.

All durable Timeless catalog, index, and compressed block state is stored in
the host database. A host-supported SQLite/libSQL replication mechanism can
therefore replicate committed Timeless state without a Timeless-specific
protocol. It cannot replicate the in-process buffer, and every reader that
queries the virtual tables must register/load a compatible extension. File
format compatibility does not make arbitrary hot-file copying safe; use the
host's documented replication or backup mechanism and validate semantic reads
after restore. See [compatibility](COMPATIBILITY.md) and
[upgrade/rollback](UPGRADE.md) before replacing either side.

## Direct libSQL embedding gate

The separate Rust gate uses libSQL's native local API rather than rusqlite:

```sh
cargo build --release -p timeless-ext
cargo run --manifest-path tools/libsql-check/Cargo.toml --locked -- \
  target/release/libtimeless_ext.so
```

It loads the release artifact on two libSQL connections, negotiates the public
capability document, writes and flushes all three signals, verifies packed
metrics queries, eight-level severity and typed nested metadata, complete
rich-span fields, closes the database, and repeats the reads after reopen.
The gate is pinned by `tools/libsql-check/Cargo.lock`; it does not exercise or
claim `timeless_spike` as a production surface.
