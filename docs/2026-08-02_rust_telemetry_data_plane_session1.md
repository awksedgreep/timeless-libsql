# Rust telemetry data plane — Session 1 result

Date: 2026-08-02  
Branch: `release/rust-telemetry-data-plane`  
Starting head: `876cf68b17a7ed9fd83f344f10ae1e85975159b2`  
Outcome: pass

Session 1 promoted the three completed POC crates into versioned release
server locations, made extension/database compatibility machine-readable, and
closed the last known logs fidelity gap before any legacy data is converted.
No server gained a private storage path.

## Delivered contract

`SELECT timeless_capabilities()` returns one deterministic JSON capability
document naming:

- extension semantic version, data ABI, minimum server version, and build
  commit/target/profile;
- public metrics named-v0/resolved-v1 batches, seconds, 4,096-point
  per-series batching, and stored rollups;
- public logs flat-v0/rich-v1 batches, millisecond/microsecond table modes,
  exact severity, typed metadata, and 8,192-entry batching; and
- public traces span-v0/rich-span-v1 batches, nanoseconds, rich-span fidelity,
  and 8,192-span batching.

Every signal writer validates this document and the database ledger before
initialization. `_timeless_schema_migrations` is an ordinary additive product
ledger, not storage. Missing ledgers upgrade idempotently from schema 0 to 1.
Future versions, mismatched data ABIs, old extensions without the handshake,
and extensions requiring a newer server fail before vtab initialization or
listener binding. The exact extension build remains separately checked by the
existing signal schema/batch oracles.

The server source is now under `servers/crates/` with one lockfile and version
`0.3.0`. The extension and servers are intentionally separate Cargo
workspaces: Cargo rejects `rusqlite`'s mutually exclusive
`loadable_extension` and `load_extension` features in one dependency graph.
An attempted dual-version workaround also failed Cargo's one-native-`sqlite3`
links rule. It was fully reverted. The checked split keeps the ABI public and
builds both roles without privately linking the extension.

Only neutral code moved into `timeless-api-common`: capability/schema
negotiation, owner leases, loopback validation, build identity, SIGINT/SIGTERM,
and the identical maintenance-task lifecycle. Signal routes, payloads,
queries, and storage commands remain separate.

All three executables now have:

- loopback-only TCP configuration validation and their measured ports;
- `--version` machine-readable build identity;
- `/live`, `/ready`, and `/health` with build identity;
- an exclusive signal/database lease before SQLite opens;
- bounded writer/reader topology validation;
- admission-before-shutdown fencing, graceful public flush, worker join/reap,
  and lease release; and
- release-workspace debug/release builds and retained benchmark/contract files.

The active `timeless_ui` metrics boundary gate/benchmark and
`timeless_traces_dashboard` traces boundary gate/benchmark now resolve the
promoted `servers/target/release/` executables. Historical POC result documents
retain their original paths because those paths describe the measured commits.

## Exact logs v1 fidelity

The original public batch v0 and codecs 1/2/4/5 remain readable with their
documented millisecond/four-level/flat-string defaults. Additive batch v1
(`0x02`) and codecs 6/7 preserve:

- epoch microseconds for product-created tables (`timestamp_unit='us'`);
- debug, info, notice, warning/warn, error, critical, alert, and emergency;
- canonical typed JSON objects, including nested arrays/maps, booleans,
  numbers, and nulls; and
- the released product's equal-timestamp comparator: message, exact severity,
  canonical metadata, then stable source order.

The existing four partitions remain a coarse storage/index optimization. Rich
severity predicates decode a candidate family when necessary; they never
confuse error with critical/alert/emergency. Legacy blocks can still answer an
exact four-level predicate from partition metadata. Severity grouping now
returns the exact product severity. Retention and the one-hour merge cap scale
with the table's persisted timestamp unit.

Direct SQL coverage inserts v0 and v1 batches into a microsecond table, flushes,
optimizes, closes the connection, reopens cold, and checks typed JSON, exact
severities, timestamp value, grouping, filtering, ordering, and capability
metadata. The HTTP contract independently proves the extension-owned 8,192
threshold remains authoritative.

## Discovered regressions and disposition

1. Moving a Jaeger test changed the relative rich-fixture path. The test
   failed immediately; the path now derives from `CARGO_MANIFEST_DIR` and the
   full rich-span oracle passes.
2. The broad SQL golden encoded block/insertion order for equal log
   timestamps. The product comparator intentionally changed that output. The
   golden now pins canonical product order across flushed and buffered rows.
3. Exact `error` predicates initially decoded legacy error blocks along with
   rich blocks. Correctness was intact, but direct-user work regressed. The
   engine now uses metadata only when the block codec proves the original
   four-level vocabulary; rich codecs still decode. Unit, CLI-stat, and HTTP
   rich-codec regressions pin both sides.
4. Rust 1.97 added strict Clippy diagnostics to pre-existing code. Mechanical
   warnings were corrected; established tuple-shaped public query contracts
   retain narrowly documented type/argument-count allowances. Both workspaces
   pass `-D warnings`.

No failed optimization remains in the tree.

## Validation evidence

Host:

```text
Linux ohm 7.1.3-arch1-2 x86_64
rustc 1.97.0 (2d8144b78 2026-07-07)
cargo 1.97.0 (c980f4866 2026-06-30)
SQLite 3.53.3
```

Passing commands:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release -p timeless-ext

cargo fmt --all --manifest-path servers/Cargo.toml
cargo clippy --workspace --all-targets --manifest-path servers/Cargo.toml -- -D warnings
cargo build --release --workspace --manifest-path servers/Cargo.toml

TIMELESS_EXT_PATH="$PWD/target/release/libtimeless_ext.so" \
TIMELESS_EXT_TEST_PATH="$PWD/target/release/libtimeless_ext.so" \
  cargo test --workspace --manifest-path servers/Cargo.toml -- --include-ignored

for section in r1 r2 r3 r4 r8 logs-rich; do
  TIMELESS_EXT="$PWD/target/release/libtimeless_ext.so" \
    ./tests/correctness.sh "$section"
done
./tests/cli.sh
```

Results:

- extension workspace: 70 core unit tests, 36 extension unit tests, every
  integration/doc test passed;
- server workspace: common 3, logs 12, metrics 22, traces 23 unit/integration
  contracts passed (including ignored real-extension contracts);
- SQL CLI: all 44 sections passed, including three deterministic 50,000-op
  plain-table oracle seeds and five kill-9 recoveries;
- rich logs: public v0/v1 cold-reopen oracle passed;
- rich traces: exact fixture/OTLP/Jaeger/dashboard and crash fidelity passed;
- future-schema refusal passed independently for all three signal servers;
  and
- strict Clippy passed in both workspaces.

The moved executable-path consumers also passed `timeless_ui` `mix precommit`
(77 tests) and `timeless_traces_dashboard` format plus test (11 tests).

The validation builds were 5.7 MiB for the extension and 5.4/4.0/4.4 MiB for
metrics/logs/traces. Those are local validation artifacts, not Session 6
release artifacts. Their embedded build head is the Session 0 starting head;
packaging will rebuild from the committed release head with deterministic
provenance, checksums, and SBOMs.

Session 1 deliberately did not claim a new performance result. Route/query
logic and authoritative thresholds are unchanged except for logs fidelity and
startup negotiation. The completed POC benchmark artifacts remain the pinned
performance baseline until the fixed harness is rerun after migration, auth,
and default adapters.

## Exit criterion

Pass. Packaged-path debug/release builds, formatting, strict Clippy, complete
SQL/oracle/crash tests, all three real-extension API contracts, v0/v1 logs,
rich logs, and rich spans are green. Every binary refuses future schema before
initialization/listen. Every target write/maintenance/query path remains a
public extension surface. Session 2 may begin with bounded immutable legacy
readers and the exact migration writer.
