# timelessd POC

`timelessd` is the standalone Rust telemetry data plane. This branch starts
with VictoriaLogs-compatible log ingest/query while keeping the process shell
signal-neutral.

It is a detached Cargo workspace because the host and guest sides of SQLite
require mutually exclusive rusqlite features:

- `timeless-ext`: `loadable_extension`
- `timelessd`: bundled SQLite plus `load_extension`

Build and run from the `timeless-libsql` root:

```console
cargo build --release -p timeless-ext
cargo build --release --manifest-path crates/timelessd/Cargo.toml

crates/timelessd/target/release/timelessd \
  --listen 127.0.0.1:9428 \
  --db /var/lib/timeless/timeless.db \
  --extension target/release/libtimeless_ext.so
```

Environment equivalents are `TIMELESSD_LISTEN`, `TIMELESSD_DATABASE`,
`TIMELESSD_EXTENSION`, `TIMELESSD_INDEX_KEYS`, `TIMELESSD_READ_WORKERS`, and
`TIMELESSD_BEARER_TOKEN`.

Current routes:

- `GET /health`
- `POST /insert/jsonline`
- `GET|POST /select/logsql/query`
- `GET /api/v1/flush`

The default table indexes `service,app,node` metadata. The optional extension
trigram message index is intentionally not enabled by default; it is a useful
explicit read/write/disk tradeoff, not a free setting.

Tests require the debug extension artifact because they deliberately load the
real shared library:

```console
cargo build -p timeless-ext
cargo test --manifest-path crates/timelessd/Cargo.toml --offline
```

The paired compatibility plan and frozen donor baseline live in
`timeless_logs/notes/rust_data_plane_poc_2026-08-01.md`.
