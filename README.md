# timeless-libsql

**Compressed metrics, logs, and traces inside any SQLite or libSQL database —
one loadable extension, three virtual tables.** Think "FTS5 for telemetry."

```sql
.load ./libtimeless_ext
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
CREATE VIRTUAL TABLE logs    USING timeless_logs(index_keys='service,path,status');
CREATE VIRTUAL TABLE traces  USING timeless_traces;

INSERT INTO metrics(name, ts, value, labels)
  VALUES ('cpu_usage', 1753000000, 42.5, '{"host":"pvm1"}');
INSERT INTO metrics(metrics) VALUES ('flush');   -- FTS5-style command idiom

SELECT * FROM logs   WHERE service='payments' AND level='error' AND ts > :t0;
SELECT * FROM traces WHERE trace_id = x'4bf92f3577b34da6a3ce929d0e0e4736';

-- Prometheus scrape bodies are just another blob:
-- curl -s target:9100/metrics -o s.prom
INSERT INTO metrics(metrics) VALUES (readfile('s.prom'));
```

Chunks/blocks are compressed (pco + adaptive columnar encoding) and stored in
shadow tables **inside the same database file** — so transactions, backup,
and libSQL replication come from the host, while compression, pruning, and
pushdown come from the engines. Works in sqlite3, the libsql crate, and
self-hosted `sqld` (`--extensions-path`, sha256 trusted.lst; SQL over HTTP).

## Numbers (1M points/entries/spans, measured, lossless-verified)

| signal | vs plain SQLite rows | headline |
|---|---|---|
| metrics | 6.4x (hostile data) – 200x (friendly) smaller | 18M pts/s batch ingest |
| logs | **13.5x** smaller | `level=error` 2x faster than a plain-table scan |
| traces | 4.2x smaller (vs plain table *with* a trace_id index) | `status='error'` 10.8x faster |

Full methodology, honest asterisks, and known limits: [RESULTS.md](RESULTS.md).
Design history and decision log: [PLAN.md](PLAN.md).
Running the suites and benchmarks yourself: [TESTING.md](TESTING.md).

## Status

**Experimental.** Built as a rapid POC (2026-07); the engine lineage is
production (extracted from [timeless_metrics](https://github.com/awksedgreep/timeless_metrics)'
Rust core), and the harness is serious — randomized property testing against
plain-table oracles, kill -9 crash rounds, transaction rollback tests — but
the extension itself is days old. Durability contract: flushed = durable,
buffered = lost on crash, never corrupt.

## Build & test

```sh
cargo build --release -p timeless-ext     # -> target/release/libtimeless_ext.so
./tests/cli.sh                            # 21 sections: round-trips, pushdown,
                                          # rollback, crash, oracle, prometheus
cd tools/bench && cargo run --release --bin bench -- ../../target/release/libtimeless_ext.so
```

Workspace: `timeless-core` (engines: pco chunk store, columnar block store —
no SQLite dependency), `timeless-codec` (typed column encoders with adaptive
strategy selection), `timeless-ext` (the loadable extension: vtabs + shadow
stores).

## License

MIT
