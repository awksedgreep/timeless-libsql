# Compatibility statement

This document defines what `timeless-libsql` means by compatible. It covers
the loadable SQLite/libSQL extension, the separately packaged `dbhealth`
extension, the three Rust signal servers, public SQL/batch/frame contracts,
and databases containing Timeless virtual tables. Language parity is defined
separately by the [query feature maps](QUERY_FEATURES.md).

## Current contract generations

This marked inventory is checked against both Cargo workspaces and the Rust
constants that enforce the handshake.

<!-- public-compatibility-versions:start -->

| Contract key | Current value | Meaning |
|---|---:|---|
| `extension_workspace` | `0.4.1` | Current tagged extension/core/dbhealth source line. |
| `server_workspace` | `0.4.1` | Current tagged metrics/logs/traces server source line. |
| `extension_data_abi` | `1` | Stored/public telemetry data compatibility generation. |
| `sql_surface_version` | `1` | Advertised public SQL inventory generation. |
| `extension_minimum_server` | `0.4.0` | Oldest server accepted by the current extension document. |
| `server_data_schema` | `1` | Additive `_timeless_schema_migrations` ledger generation. |
| `server_required_data_abi` | `1` | Data ABI required by all current signal servers. |
| `server_minimum_extension` | `0.4.0` | Oldest extension semantic version considered by current servers. |

<!-- public-compatibility-versions:end -->

The capability document, not the semantic version alone, decides whether two
artifacts can run together. A server requires all of the following before it
creates/connects a signal virtual table or binds a listener:

1. `timeless_capabilities()` exists and decodes.
2. `data_abi` is exactly 1.
3. The extension semantic version is at least the server floor.
4. The server semantic version is at least the extension floor.
5. The required signal and batch generation are advertised.
6. Every public query work guard needed by that server exists.
7. The database schema ledger is not newer and has the expected data ABI.

Unknown/additive capability object members and array entries must be
tolerated. Missing required members fail closed.

## Version policy

The project is pre-1.0. A minor version selects a public compatibility line;
a patch version fixes behavior without intentionally removing a documented
surface. Extension and Rust server workspaces advance together whenever their
minimum pairing changes. The changelog calls a source version “unreleased”
until a matching tag is reachable from `main`. A tag and Cargo metadata prove
source identity, not that a complete native artifact set was published. The
[artifact guide](ARTIFACTS.md) records that separate status.

These independent versions must not be conflated:

- Cargo semantic version identifies source/artifacts.
- `data_abi` identifies persisted/public data compatibility.
- `sql_surface_version` identifies the advertised inventory schema.
- each batch and packed frame carries—or, for `raw-series-v0`, externally
  receives—its own format generation.
- the server data-schema ledger identifies additive server-owned metadata,
  not a private replacement storage engine.
- PromQL/MetricsQL/LogsQL matrix rows identify language compatibility.

## Supported artifact pairings

| Extension | Rust server | Verdict | Reason |
|---|---|---|---|
| current `0.4.x` | matching current `0.4.x` | supported when the full capability preflight passes | Both sides require data ABI 1 and the same release floor. |
| tagged `v0.3.0` | `0.4.x` | unsupported | The tag predates `timeless_capabilities()` and the release-server query guards. |
| `0.4.x` | tagged/pre-release `0.3.x` server | unsupported | The current extension declares a `0.4.0` server floor; an old server cannot prove it understands the current API/resource contract. |
| any version with a different `data_abi` | current servers | unsupported | The server refuses before virtual-table initialization. |
| current extension with a future-schema database | current servers | unsupported/fail closed | The server does not downgrade or mutate a future ledger. |
| current extension loaded directly by SQLite/libSQL | no Timeless server; host-owned | supported | The host must negotiate capabilities and own maintenance, backup, concurrency, and limits. |
| current `timeless-ext` linked into a Rust host with only the `embedded` feature | no Timeless server; embedded host-owned | supported | The host registers every connection and owns the same maintenance, backup, concurrency, and limit duties; `entrypoints` and `embedded` are mutually exclusive build modes. |

The `minimum_*_version` fields are necessary but not sufficient. An artifact
at the numeric floor can still be rejected for a missing signal batch or
query-surface capability.

## Database and storage compatibility

The public contract is the virtual-table schema, batch formats, command
behavior, query TVFs/scalars, capability document, and versioned packed
frames. Shadow-table names, columns, and physical codecs are private. Never
copy selected shadow tables, query them from a server, or use them as a
migration API.

Within data ABI 1:

- previously published batch versions and block codecs remain readable;
- a new incompatible batch or result encoding receives a new version/magic;
- additive SQL modules, hidden inputs, capability members, stats keys, and
  frame formats are allowed;
- existing frame magics and batch version bytes never change meaning;
- metrics/logs/traces timestamp units are persisted/negotiated as documented;
- rich logs do not fabricate severities already collapsed by an older source;
- rich traces do not fabricate fields absent from an older span format; and
- trace duration extrema are additive private metadata: legacy blocks without
  a metadata row decode exactly, current writers publish both bounds atomically,
  and public optimize backfills them without changing the stored span payload;
- trace attribute equality is opt-in immutable table metadata: configured
  current writers publish fixed-size filter rows atomically, a missing legacy
  row decodes and rechecks exactly, corrupt filter metadata fails closed, and
  existing tables without an allowlist remain readable but reject the hidden
  predicate;
- transaction, rollback, flush, optimize/compact, rollup, retention, and
  authoritative batching remain extension-owned.

A pre-ledger database is server schema 0. The current writer adds only the
idempotent ledger-v1 record after extension and database preflight; readers
require that record. This ledger does not convert an external Rust block
directory. The signal-server binaries contain no private legacy block reader.
An owning higher-order product must perform any non-SQLite conversion through
the public batch/SQL interfaces before selecting the database.

## Query-language compatibility

Prometheus is the stable PromQL oracle, VictoriaMetrics is the MetricsQL
oracle, and VictoriaLogs is the LogsQL oracle at the immutable versions in
[the oracle manifest](QUERY_ORACLES.md). “Supported” means the individual
matrix row is shipped and its pinned semantic/API evidence passes. It does not
mean every feature of the upstream product.

Intentional differences—strict unknown-parameter handling, typed log values,
resource ceilings, unsupported distributed partial results, and deferred
stream/native-histogram models—are recorded per row. Unsupported syntax fails
explicitly and never delegates to Elixir, Rocket, another server, or private
storage.

## Platform and SQLite/libSQL boundary

The release-tool target inventory covers x86-64/AArch64 Linux GNU and
x86-64/AArch64 macOS. For `v0.4.1`, all four native build, identity,
install/remove, and upload jobs passed, followed by the complete outer
checksum job. Those archives are retained as Actions artifacts until
2026-11-06; the workflow did not create a permanent GitHub Release. Do not
conflate a checked platform matrix with a permanent publication channel.

The extension uses SQLite's loadable-extension ABI and stores ordinary SQLite
pages/WAL records. A host may use SQLite backup, libSQL replication, or sqld
around the complete database. Timeless does not claim that copying only
virtual rows or shadow-table subsets creates a compatible replica.

See the [upgrade guide](UPGRADE.md) before replacing artifacts, the
[SQL API reference](SQL_API_REFERENCE.md) for the exact public surface, and
the [embedded Rust](EMBEDDED_RUST.md), [sqld](SQLD.md), and
[artifact](ARTIFACTS.md) guides for each host boundary.
