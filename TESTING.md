# Testing & Benchmarking Guide

Everything here assumes a release build of the extension first:

```sh
cargo build --release -p timeless-ext
# artifact: target/release/libtimeless_ext.so   (.dylib on macOS)
```

## The 60-second smoke test

```sh
sqlite3 /tmp/t.db \
  ".load target/release/libtimeless_ext" \
  "CREATE VIRTUAL TABLE m USING timeless_metrics;
   INSERT INTO m(name, ts, value) VALUES ('cpu', 1, 42.5);
   INSERT INTO m(m) VALUES ('flush');
   SELECT * FROM m;"
```

Prints `cpu|1|42.5|{}`. If `.load` fails on macOS, see the macOS section —
it's almost certainly Apple's system sqlite3, not the build.

## Test suites

### Rust tests (fast, no extension involved)

```sh
cargo test -p timeless-codec        # column encoders: exactness, adaptive
                                    # strategy selection, hostile inputs
cargo test -p timeless-core        # engines: round-trip, recovery, txn
                                    # journal, compression honesty (bit-exact
                                    # 1M-point verification), dup-min_ts
                                    # regression, block/span engines
```

What lives where:
- `timeless-core/tests/roundtrip.rs` — write → query-before-flush → flush →
  aggregate → shutdown → cold-recovery lifecycle
- `timeless-core/tests/compression_honesty.rs` — every point of 1M verified
  bit-exact after recovery; bytes measured from disk, not bookkeeping
- `timeless-core/tests/store_seam.rs` — FsStore/ChunkStore seam + recovery
- `timeless-core/tests/txn_journal.rs` — rollback of buffers, intra-txn
  flushes, optimize-in-txn
- `timeless-core/tests/dup_min_ts.rs` — the (series,min_ts) shadowing
  regression (found by the oracle, fixed here and upstream)
- unit tests inside `src/blocks/` and `src/spans/` — codecs, partitioned
  flush, term pruning read-count proofs, projection/late-materialization, and
  merge caps

### The CLI suite (the real integration harness)

```sh
./tests/cli.sh          # 45 sections, a few minutes; needs sqlite3 + Rust/Cargo
```

The extension is a cdylib, so end-to-end behavior is tested through the
sqlite3 CLI plus persistent Rust SQLite hosts: vtab lifecycles for all three
signals, pushdown proofs (with `EXPLAIN QUERY PLAN` assertions), reopen
recovery, prune/optimize/compact, append-only enforcement, transaction
rollback (including auto-flush inside `BEGIN`), Tier 2 batch blobs,
malformed-input rejection, Prometheus ingest, rich trace row/batch fidelity
at the exact 8,192-span threshold, multi-connection and multi-process sharing,
and the oracle and crash suites. The standalone `tools/query-harness` crate
builds binary fixtures, drives cases that require more than one live SQLite
connection or process, and decodes packed public frames. The release gate
neither imports nor executes Python. Every section prints `PASS:`; the script
exits nonzero on the first failure.

### The oracle (randomized property testing)

```sh
cd tools/bench
cargo run --release --bin oracle -- ../../target/release/libtimeless_ext.so
# or replay specific seeds:
cargo run --release --bin oracle -- ../../target/release/libtimeless_ext.so 42 1337
```

~50k randomized operations per seed across all three vtabs and mirrored
plain tables in the same database — inserts, flush/optimize/compact at
random points, every pushdown plan family, explicit transactions with
rollback, prunes. Every query result is compared against the plain-table
mirror, order-insensitive, floats by bit pattern. On mismatch it prints the
seed and op index — rerun with that seed to reproduce deterministically.
This harness found a real engine bug (chunk-index shadowing); trust it.

### The crash suite

```sh
./tests/crash.sh target/release/libtimeless_ext.so
```

Five rounds: the Rust query harness owns an unreaped `sqlite3` child running a
long ingest with periodic flushes and watermark logging, sends SIGKILL to that
exact child at a random moment, reaps it, reopens, then asserts
`PRAGMA integrity_check` is clean, all flushed watermarks are present, and
no `_terms`/`_trace_blocks`/`_duration_bounds`/`_attribute_blooms` row
dangles; every current trace block also has valid duration bounds and exact
configured-attribute results. The durability contract being proven:
**flushed = durable, buffered = lost, never corrupt.**

## Benchmarks

All live in `tools/bench` (standalone crate, bundled SQLite — no system
sqlite3 needed, works identically on macOS). Run from `tools/bench/`:

| binary | args | measures | runtime |
|---|---|---|---|
| `bench` | `<path-to-.so>` | metrics: plain vs Tier 1 vs Tier 2 ingest rates, file sizes, query timings, bit-exact spot checks | ~1 min |
| `bench-logs` | `<path-to-.so>` | logs: 1M realistic entries, ingest, file size vs plain, level/service/LIKE query timings vs plain-table oracle counts | ~1 min |
| `bench-traces` | `<path-to-.so>` | traces: 1M spans, size vs plain-table-WITH-trace_id-index, trace lookup / status / service+range timings | ~2 min |
| `bench-codec` | none | codec-level bake-off on identical blocks (no SQLite): per-column bytes and encode/decode MB/s across codec generations | ~1 min |
| `oracle` | `<path-to-.so> [seeds]` | correctness, not speed (above) | ~10 s |

```sh
cd tools/bench
cargo run --release --bin bench        -- ../../target/release/libtimeless_ext.so
cargo run --release --bin bench-logs   -- ../../target/release/libtimeless_ext.so
cargo run --release --bin bench-traces -- ../../target/release/libtimeless_ext.so
cargo run --release --bin bench-codec
```

### Trace duration-pruning evidence

The Rust query harness can measure a copied pre-extrema trace database before
and after public optimize backfill. It deliberately mutates the supplied copy;
never point it at the only copy of a database:

```sh
cp --reflink=auto /path/to/legacy-traces.db /path/to/scratch-traces.db
cargo run --release --manifest-path tools/query-harness/Cargo.toml --locked -- \
  gate trace-duration-evidence \
  --extension target/release/libtimeless_ext.so \
  --database /path/to/scratch-traces.db \
  --table traces --service payments \
  --minimum-duration-ns 9000000000000000000 \
  --iterations 50 --warmup 5 --wal
```

Choose a service present in the fixture and a duration above its maximum. The
command fails if the legacy phase does not actually decode candidate blocks,
if the query returns a row, if optimize leaves an unknown block, or if the
post-backfill phase considers/decodes a persisted block. Its JSON reports
p50/p95/p99, cardinality, candidate/payload/decoded work, logical and physical
storage, optimize backfill time/bytes/entries, and process RSS HWM. This is a
same-fixture storage optimization measurement, not a Victoria/Jaeger
competitive benchmark.

### Trace query enhancement baseline

The Rust query harness can create a temporary deterministic rich-span database,
start the release trace server, measure broad Jaeger reads, stop it cleanly,
and then measure `timeless_trace_buckets` in a fresh direct-SQL process:

```sh
cargo build --release
cargo build --release --manifest-path servers/Cargo.toml --bin timeless-traces-api
cargo build --release --manifest-path tools/query-harness/Cargo.toml
tools/query-harness/target/release/timeless-query-harness trace-baseline \
  --output docs/evidence/local_trace_query_baseline.json
```

Session 7's opt-in trace-attribute A/B uses separate fresh processes at the
same clean commit. Omit the flag for the public JSON1/write-path control and
include it for the two configured `/count` and `/bool` fields:

```sh
tools/query-harness/target/release/timeless-query-harness trace-baseline \
  --output docs/evidence/local_trace_attributes_unindexed.json
tools/query-harness/target/release/timeless-query-harness trace-baseline \
  --attribute-indexes \
  --output docs/evidence/local_trace_attributes_indexed.json
```

Both reports include insert/optimize time, file/WAL state before optimize and
checkpoint, builder/query HWM, and JSON1 attribute controls. The indexed run
also measures the hidden-filter candidate. Compare equal commits and fixtures;
the command rejects dirty tracked source or stale extension/server identities.

Run from a clean tracked commit: the command rejects stale extension or server
build identities. The default workload is 16 public 8,192-span batches, 20
measured iterations, and three warmups. It cleans its temporary database and
server log on both success and ordinary failure. Pass `--retain-on-failure`
only when you deliberately need a failed fixture for diagnosis; remove it
afterward. The output records latency tails, cardinality/result bytes,
extension work, durable fixture construction, storage/WAL/freelist state, and
fresh-server RSS/HWM for each Jaeger shape, and isolated direct-SQL RSS HWM.
It also profiles service, operation-name, kind, and status postings over one-
and four-time-box windows and fails if any query reads a time-disjoint block.
The direct-SQL child additionally compares the readable two-consumer trace
summary CTE with the published single-scan conditional-aggregate recipe,
asserting exact retained-row cardinality, result bytes, public extension work,
and process RSS HWM.

Datasets are generated by a deterministic PRNG (`src/datasets.rs`) — same
data every run, on every machine, so numbers are comparable across hosts.

### Rules for fair numbers

1. **Release builds only.** Debug-build numbers are meaningless (5–20x off).
2. **File sizes are measured after connection close** (WAL folded in). The
   bench binaries do this; if you measure manually, close first.
3. **Run everything twice**, quote the second run. First runs pay page-cache
   and allocator warmup; the recorded reference numbers treat ±15% between
   runs as noise (query timings under 10ms are especially jittery).
4. **Ingest rates**: `bench` measures insert time only (encode excluded for
   Tier 2 — the blob is built outside the clock, since a real client
   prepares batches off the hot path). Flush/optimize are reported
   separately because they're paid at flush cadence, not per point.
5. **Compression ratios depend on data shape more than anything.** The
   generators are deliberately hostile (ms-jitter timestamps, per-point
   noise, random IDs). Friendly data (regular scrape intervals, slow-moving
   gauges) compresses 10–30x better. Both are real; quote which one you ran.

Reference numbers from the original dev machine (Linux x86, i7-class) are in
[RESULTS.md](RESULTS.md) — compare yours against those tables.

## Two extensions, one build caveat

`libtimeless_ext` and `libdbhealth_ext` share source but are separate
products with disjoint entry points (dbhealth builds timeless-ext with
`default-features = false`). Build them in separate invocations — or
just `cargo build --release`, which is fine. The one spelling to avoid
is selecting both with `-p` in a single command
(`cargo build -p timeless-ext -p dbhealth-ext`): the two feature
variants of the dual-crate-type timeless-ext under fat LTO trip rustc's
"failed to load bitcode" issue. `tests/dbhealth.sh` builds its .so the
right way.

## macOS / Apple Silicon notes

- **Apple's system `/usr/bin/sqlite3` disables extension loading** — `.load`
  fails with "not authorized" no matter what you do. `brew install sqlite`
  and use `$(brew --prefix sqlite)/bin/sqlite3`, or skip the CLI entirely:
  the bench binaries and oracle bundle their own SQLite and work everywhere.
- The artifact is `libtimeless_ext.dylib`, not `.so`.
- `tests/cli.sh` needs the brew sqlite3 first in PATH; `tests/crash.sh` and
  the oracle care only about the extension path you pass.
- Interesting comparisons to record on Apple Silicon: Tier 2 ingest rate
  (memory-bandwidth-bound) and the decode MB/s lines from `bench-codec`
  (pco and zstd both scale with wide cores).
