# Standalone telemetry data-plane POC

The `poc/rust-data-plane` branch proves a signal-neutral Rust HTTP/data-plane
process using the public `timeless-ext` embedding API. Logs are the first
workload; the daemon must not depend on Elixir or on `timeless_logs` internals.

The detailed session plan, frozen main-branch baselines, compatibility target,
and acceptance criteria live in the paired `timeless_logs` branch at
`notes/rust_data_plane_poc_2026-08-01.md`.

## Crate boundary

- `timeless-api`: protocol parsing and response models that can be reused by a
  daemon, tests, or another Rust embedding.
- `timelessd`: the standalone process. It owns sockets, admission control,
  SQLite connections, extension loading, lifecycle, and configuration.
- `timeless-ext`: remains the SQLite/libSQL storage and query extension. POC
  improvements that benefit direct database users belong here rather than in
  daemon-only code.

The API crate may know telemetry signals; it must not know Phoenix. The daemon
may compose signals; it must not become a control-plane database. The extension
must remain usable without the daemon.

`timelessd` is a deliberately detached Cargo workspace. A SQLite extension uses
rusqlite's `loadable_extension` ABI, while its host uses the mutually exclusive
`load_extension` ABI (and bundles SQLite). Building both in one Cargo dependency
graph would be invalid; the separation records a real deployable boundary.

## First vertical slice

The first executable slice is deliberately small:

1. Open a SQLite database and load the distributable `libtimeless_ext` shared
   library, exactly as a direct SQLite/libSQL host does.
2. Create a `timeless_logs` virtual table with configured index keys.
3. Serve `/health`.
4. Accept VictoriaLogs-compatible NDJSON at `/insert/jsonline`.
5. Serve the currently supported LogsQL subset at `/select/logsql/query`.
6. Flush on graceful shutdown and prove restart durability.

HTTP compatibility and mixed-load performance are measured by the paired logs
repository. Later signals reuse the process shell; they do not fork separate
servers or create new Rust/Elixir crossings.

The daemon keeps the extension's `message_index=none` default. Trigram message
indexing remains an explicit storage tradeoff for direct users; silently paying
its write and disk cost would make the POC incomparable with the Elixir donor.
