# Timeless logs storage POC

This is the clean, storage-only gate for a future Rust logs data plane. It has
no HTTP server, auth, routing, or daemon configuration.

The worker owns one SQLite connection and deliberately models the existing
service lifecycle:

1. A bounded channel and entry-credit pool acknowledge admission, not
   durability. Credits remain charged until raw flush, so dequeuing work does
   not disguise storage backlog.
2. Producer batches use the logs vtab batch blob; entries remain queryable in
   the extension buffer.
3. An aggregate 1,000-entry threshold or low-volume timer writes raw blocks.
4. Background maintenance uses bounded `optimize:<entries>` commands. It does
   not compress on ingest and does not drain an unbounded backlog in one call.
5. Graceful shutdown flushes the remaining tail as raw.

Run the executable gate after building the extension:

```bash
cargo build -p timeless-ext --release
cargo run --manifest-path tools/logs-poc/Cargo.toml --release --bin logs-lifecycle -- \
  target/release/libtimeless_ext.so
```

It verifies exact total, level, indexed metadata, and message queries while
data is buffered, raw, mixed raw/compressed, fully compressed, cold-reopened,
and reopened after a graceful tail flush. The printed table is storage
lifecycle evidence, not an API performance benchmark.
