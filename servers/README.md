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

## Running standalone (no control plane, no Elixir)

Each binary takes the extension, the database, and an optional listen
address as positional arguments:

```bash
./timeless-metrics-api ./libtimeless_ext.so ./metrics.db            # loopback default
TIMELESS_ALLOW_NON_LOOPBACK=1 \
  ./timeless-metrics-api ./libtimeless_ext.so ./metrics.db 0.0.0.0:19439
```

Non-loopback binding always requires the explicit
`TIMELESS_ALLOW_NON_LOOPBACK=1` — inside a container, where `0.0.0.0` is the
only address reachable from the host, both go together and the container's
port publishing decides actual exposure. On a bare host the alternative is
keeping the loopback default behind a reverse proxy. Either way, the moment
a server becomes network-reachable is the moment to consider
`TIMELESS_AUTH_MODE=required` or `TIMELESS_ADMIN_KEY` (see the
[API reference](../docs/SERVER_API_REFERENCE.md)).

### Where to put the database

Keep each signal database on a filesystem that overwrites pages in place:
ext4, xfs, or a copy-on-write filesystem with copy-on-write turned off for
the data directory. The servers rewrite 4 KB pages all over a multi-gigabyte
file every few seconds. On btrfs with copy-on-write (its default, often with
`compress=zstd`) every rewritten page becomes its own extent, and under
maintenance load the kernel throttles and stalls the writer. On one
700,000-series deployment `metrics.db` was 3.0 GB in 714,304 extents, the
writer was blocked for 7–10 seconds at a time in `balance_dirty_pages` and up
to 3.9 seconds in `fsync`, and queries waited behind it. With the same files
copied into a no-copy-on-write directory, the longest maintenance step fell
to under a second (#139). ZFS and other copy-on-write filesystems, and
container volumes that sit on one, behave the same way to some degree.

On btrfs, copy-on-write is turned off per directory, and only for files
created afterwards:

```bash
mkdir /srv/timeless-data
chattr +C /srv/timeless-data        # must be empty; new files inherit NOCOW
lsattr -d /srv/timeless-data        # shows a 'C'
```

For an existing deployment, stop the servers, create a new directory with
`+C`, copy the files in with `cp -a --reflink=never` (a reflink copy would
share the old copy-on-write extents), verify, and swap the directory in. The
trade-off: btrfs neither compresses nor checksums NOCOW files, and a snapshot
makes the snapshotted extents copy-on-write again until they are next
rewritten.

To check a running deployment:

```bash
findmnt -no FSTYPE,OPTIONS -T /srv/timeless-data   # filesystem and mount options
lsattr /srv/timeless-data/metrics.db               # 'C' on a NOCOW file
filefrag /srv/timeless-data/metrics.db             # extents: thousands at most, not hundreds of thousands
```

Signs of this problem: a writer thread in state `D`, with
`balance_dirty_pages` or `btrfs_sync_file` in
`/proc/<pid>/task/<tid>/stack`; a `compact_step_max_ns` of several seconds;
and read retries or timeouts that come and go with maintenance.

For rootless Podman or Docker named volumes, the directory to prepare is the
volume's `_data` directory (`podman volume inspect <name>`), before the first
start; or bind-mount a prepared directory instead.

### Sharing a database with the extension

The servers load the same `libtimeless_ext` and speak the same on-disk
format as an application embedding the extension directly. Each database must
have **one writing owner**. A shared lease rejects a second participating
signal server, including one using a symlink or a different signal. Embedded
applications do not acquire this lease and must be stopped before handing
writing ownership to a server. The supported topologies:

1. **The server owns the database.** Applications ingest and query over
   HTTP. The default topology, and the reason the binaries exist.
2. **The application owns the database** through the embedded extension.
   There is no HTTP surface; start a server against that file only after
   the application has stopped writing; this handoff is the operator's responsibility.
3. **Split reads.** The server owns writes; other processes open the same
   file read-only and query through the extension directly. WAL gives them
   safe snapshot reads, minus the server's not-yet-flushed buffer
   (typically the last few seconds).

Two independent writing owners on one file are unsupported. The servers resolve
symlinks before opening SQLite and reject hard-linked database files on Linux
and macOS because those aliases can use different WAL paths. Keep database
paths and lease files in place while a server is running. Upgrade every
participating server to obtain the same alias protection.

All three expose `/live`, `/ready`, `/health`, and signal-specific stats. A
SIGINT or SIGTERM stops admission, drains accepted HTTP work, stops maintenance,
flushes through the public extension command, closes/reaps SQLite workers, and
releases the owner lease. Optional authentication and production limits are
available in the current release servers (opt-in via
`TIMELESS_AUTH_MODE=required`). Startup capability/schema fencing occurs before
listener binding; coordinated backup and complete WAL checkpointing use the
sole writer connection.
