# Upgrade and rollback guide

This guide covers upgrades of a SQLite/libSQL database already containing
Timeless virtual tables and upgrades of the three Rust signal servers. It does
not turn the server binaries into readers for an unrelated Rust block-store
directory; that conversion belongs to the higher-order product that owns the
legacy format and must write through the public Timeless batch/SQL contracts.

The current tagged source line is `0.4.1`. Its four native package jobs and
complete outer-checksum gate passed. The resulting archives are authenticated
GitHub Actions artifacts retained until 2026-11-06, not permanent GitHub
Release assets. Build the matching extension and servers from source or obtain
the complete matching workflow set described in the
[artifact guide](ARTIFACTS.md); never mix one retained candidate with another
version. The earlier `v0.3.0` tag predates the capability handshake and is not
a valid extension peer for a `0.4.x` server.

## Invariants

- Upgrade the telemetry extension and all deployed Rust signal binaries as
  one compatibility set.
- Stop the current owner before replacing a database or extension artifact.
- Keep the pre-upgrade database and its coordinated backup until rollback is
  no longer required.
- Never copy, delete, rename, or selectively import private shadow tables.
- Never test a downgrade against the only copy of production data.
- A green `--version` string is not enough; require the complete capability,
  database-ledger, readiness, and semantic checks below.

Opening an older `timeless_traces` table with this extension adds a private
duration-extrema side table. Existing payloads and indexes are not
rewritten during startup. Unknown blocks remain exactly queryable through the
decode fallback; schedule `INSERT INTO traces(traces) VALUES ('optimize')` (or
the bounded `optimize:<max source spans>` form) to populate them. Observe
`duration_unknown_blocks` reaching zero through `timeless_stats('traces')`.
Keep the pre-upgrade backup: an older extension is not expected to understand
the additive private schema even though stored block payloads retain their
codec and data-ABI meaning.

Starting with `0.5.0`, rich-logs `optimize()` writes template-compressed
message blocks (codec byte 8). Nothing is rewritten at open — the new codec
appears only as background or scheduled optimize re-encodes rich blocks — but
once it has, a pre-`0.5.0` extension refuses those blocks with a loud decode
error naming the unknown codec; it never returns partial or flattened rows.
Rolling back the extension after a `0.5.0` optimize therefore requires
restoring the pre-upgrade backup (or re-importing through the public batch
contracts with the older binary). Upgrade every binary that opens the same
database file as one set before allowing optimize to run, exactly as the
invariants above require.

## 1. Inventory the current installation

Record the current artifact identities and complete database paths. For a
current server binary:

```sh
timeless-metrics-api --version
timeless-logs-api --version
timeless-traces-api --version
```

For an extension that already exposes the handshake:

```sh
sqlite3 /absolute/path/telemetry.db <<'SQL'
.load /absolute/path/libtimeless_ext
.mode json
SELECT timeless_capabilities() AS capabilities;
SELECT name, sql
  FROM sqlite_schema
 WHERE name = '_timeless_schema_migrations';
SQL
```

An old extension may fail the first statement with “no such function.” That
is an identified old artifact, not permission to skip preflight in the new
deployment. If the inventory query reports the ledger table, record its rows
with `SELECT * FROM _timeless_schema_migrations ORDER BY signal, version`.

Also record:

- database, `-wal`, and `-shm` paths and sizes;
- signal table names and creation SQL from `sqlite_schema`;
- configured timestamp units, retention, and log index keys;
- current row/series/time-range counts obtained through public tables/TVFs;
- artifact checksums and the deployed configuration/policy files.

## 2. Drain and create the rollback point

For a Rust signal server, stop producers, call its authenticated flush route,
then use its verified backup route while it still owns the database. The
backup operation flushes, performs signal maintenance, requires a complete
WAL checkpoint, uses SQLite's online-backup API, validates the result, fsyncs,
and publishes without overwrite. Exact request/response behavior is in the
[server API reference](SERVER_API_REFERENCE.md#flush-backup-restore-and-wal).

Then stop the server normally and wait for it to exit. SIGINT/SIGTERM drains
accepted HTTP requests, places a final flush behind admitted writes,
checkpoints WAL with `TRUNCATE`, joins workers, and releases its owner lease.
Do not use SIGKILL as an upgrade procedure.

For a directly embedded host, stop every writer and either use SQLite's
online-backup API before shutdown or perform a coordinated complete WAL
checkpoint before copying the entire database. A bare copy of the main file
while WAL frames are outstanding is not a rollback point.

## 3. Preflight the new artifacts on a copy

Verify checksums, place the new extension and matching signal binaries beside
the old artifacts, and retain the old files for rollback. Do not overwrite the
only known-good copy.

Load the new extension against a copy of the database:

```sh
sqlite3 /absolute/path/telemetry-upgrade-check.db <<'SQL'
.load /absolute/path/libtimeless_ext
.mode json
SELECT timeless_capabilities() AS capabilities;
PRAGMA quick_check;
SELECT name, sql
  FROM sqlite_schema
 WHERE type = 'table' AND sql LIKE '%USING timeless_%'
 ORDER BY name;
SQL
```

Require extension version `0.4.x`, `data_abi=1`,
`sql_surface_version=1`, `minimum_server_version>=0.4.0`, the expected signal
batch generations, and every query work guard required by the intended
server. The canonical field inventory is in the
[SQL API reference](SQL_API_REFERENCE.md#capability-and-version-handshake).

Run representative public queries on the copy for every signal and compare
counts, identities, timestamp extrema, float bits where relevant, labels,
severity/metadata, trace relationships, and rich-span fields with the
pre-upgrade inventory. Flush/maintain/checkpoint, close SQLite, reopen cold,
and repeat the semantic checks. Do not treat `quick_check` alone as semantic
parity.

## 4. Replace and start in dependency order

1. Confirm the old owner is stopped and no process holds the signal lease.
2. Install the new extension and matching server binaries atomically or under
   a deployment-level maintenance lock.
3. Start one signal owner with its existing database path and policy.
4. Let the writer preflight the extension and database, initialize/connect the
   public virtual table, and add the idempotent schema-ledger v1 row when the
   database is pre-ledger schema 0.
5. Require `/live`, authenticated `/ready`, and signal stats to report the
   expected build, data ABI, table, queue, and storage state.
6. Resume producers only after readiness succeeds.
7. Repeat for each independently owned signal database/process.

Startup must fail closed for a missing capability function, extension below
`0.4.0`, server below the extension's floor, wrong data ABI, missing rich batch
generation, required query guard absence, future schema ledger, incompatible
timestamp/retention policy, corruption, or another owner lease.

## 5. Post-upgrade verification

After admitting a bounded test batch:

- call the ordered flush barrier and require completed watermarks;
- execute exact latest/range metrics queries and at least one applicable
  PromQL expression;
- execute a bounded log read/count and verify all expected typed fields;
- fetch a complete trace and verify parent relationships and rich-span fields;
- inspect public stats for failures, queue growth, decoded work, WAL/checkpoint
  state, and storage accounting;
- for upgraded traces, require `duration_unknown_blocks=0` after the planned
  optimize backfill and verify an impossible duration filter reports zero
  candidate blocks and decoded spans;
- when a newly created trace table declares `attribute_indexes`, verify
  `attribute_index_fields`, `attribute_bloom_rows`, and
  `attribute_bloom_bytes`, then compare one hidden-filter result with its
  public JSON1 control. Existing tables do not acquire an allowlist merely by
  opening them; changing fields is a side-by-side public row/batch migration;
- restart normally and repeat the reads cold; and
- retain the rollback backup and old artifacts for the documented support
  window.

Do not silently route an unsupported query to another process during this
verification.

## Rollback

Rollback is backup restoration, not an in-place binary downgrade:

1. Stop producers and the new signal owner normally.
2. Preserve the failed/new database, WAL, SHM, logs, and build identities for
   diagnosis.
3. Restore the coordinated pre-upgrade backup to a new path or replace the
   stopped database using an atomic operator-controlled operation.
4. Restore the previous extension and server binaries as the same known-good
   set.
5. Start against the restored database, require its normal compatibility
   checks/readiness, and run the pre-upgrade semantic oracle.
6. Resume producers only after verification.

Never point an older server at a database already mutated by a newer server
and call that rollback, even when `data_abi` has not changed. The older server
may not understand additive schema, batch, query, limit, or lifecycle
contracts. Restore the matching backup.

## Source-state table

| Detected source | Action |
|---|---|
| Fresh database | Create the public signal vtab with the desired timestamp/index/retention options; record schema ledger v1 through the writer. |
| Current SQLite/libSQL database and matching `0.4.x` extension | Start normally after backup; full handshake and schema preflight remain mandatory. |
| Pre-ledger SQLite telemetry database | Back up, preflight with the new extension on a copy, then allow the new writer to add ledger v1 idempotently. |
| Database created by tagged `v0.3.0` | Replace the extension with `0.4.x` before starting a release server; validate all signals on a copy because the old tag has no capability document. |
| Future ledger or different data ABI | Stop. Use a compatible newer binary or an explicit versioned migration; do not mutate/downgrade. |
| Corrupt database or failed semantic parity | Stop and retain both source and candidate; restore/continue the old deployment. |
| External legacy Rust block store | Use the owning higher-order library's explicit converter. These signal binaries do not read it and must not recreate its storage format. |
| Ambiguous multiple candidate/production databases | Stop and require operator selection; never choose by newest mtime or largest file. |

## SQLite/libSQL, WAL, backup, and replication

All durable Timeless state, including compressed blocks, indexes, rollups, and
private metadata, is stored in the containing SQLite database and WAL. Use a
whole-database SQLite/libSQL backup or replication mechanism. Timeless does
not add a second replication protocol and does not claim that an arbitrary
file copy made during active WAL writes is valid.

Replication compatibility does not replace application-level verification:
open a replica with the same extension generation, require the capability
handshake, and compare public semantic queries after the host reports the
expected replication/checkpoint boundary.
