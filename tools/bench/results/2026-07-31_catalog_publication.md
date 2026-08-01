# Catalog publication and first-query latency — 2026-07-31

This is the direct-extension result for Session 6 of
[`QUERY_PERFORMANCE_PLAN.md`](../../../QUERY_PERFORMANCE_PLAN.md). It measures
the public loadable-extension SQL surface rather than calling
`timeless-core` directly.

## Cause and change

The shared in-process engine already contained the effects of a successful
local flush, compaction, rollup build, or prune. The shadow store also advanced
its transactional `(max_series_id, chunk_generation)` token, but the engine's
cached token remained stale. The next reader therefore reloaded the complete
series catalog, raw-chunk index, and rollup index merely to rediscover the
state already present in memory.

The metrics virtual table now captures the authoritative token in SQLite's
`xSync` prepare phase, after its final mutation and while the write transaction
still excludes other writers. `xCommit` publishes that token to the shared
engine before making its journal inactive; `xRollback` discards it. A mutation
from another process still produces a mismatch and takes the original full,
always-correct refresh path.

This changes neither the database format nor the public SQL contract.

## Reproduction

Starting revision: `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, with the query work uncommitted.

```sh
cargo build --release -p timeless-ext
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so \
  --series 12000 --points 60 --runs 10
```

Five additional fresh processes used `--runs 1` for the first-query gate.
Environment: Linux 7.1.3 x86-64, Intel Core Ultra 9 185H, Rust/Cargo 1.97.0,
bundled SQLite 3.53.2, and a `/tmp` benchmark database on `tmpfs`.

## Result

| Measurement | Before | After | Change |
|---|---:|---:|---:|
| First exact read after flush, direct extension | 44.730ms | 0.187ms fresh-process median | 239x faster |
| Fresh-process first-read range | — | 0.175–0.228ms | below the 5ms gate |
| Cached generation `SELECT`, p95 / max | — | 0 / 2us | below the 10us gate |
| Full empty TVF query, median / p95 | — | 19–21 / 23–25us | conservative end-to-end no-op |

The five first-read samples were `175`, `193`, `187`, `179`, and `228`us.
The generation-only timer has one-microsecond resolution; its p95 rounded to
zero, while the largest observed sample was 2us. `warm_refresh_noop` includes
TVF dispatch, argument decoding, the generation read, and empty candidate
selection, so it is intentionally broader than the generation-check exit gate.

The final ten-run process measured warm exact raw at 37us median / 46us p95.
The publication change removes the one-time reload; it does not add a new warm
query stage.

## Correctness coverage

- Core tests count authoritative reloads and prove that a locally published
  generation skips them while an external generation change does not.
- A captured token followed by rollback is never published.
- CLI coverage uses two live connections and exercises commit, rollback after
  prepared flush/prune work, compaction row replacement, prune, a separate
  SQLite process, and reopen.
- Existing crash/recovery and transaction-journal suites retain the
  conservative reload path whenever a token cannot prove coherence.

The paired public TimelessMetrics result is recorded in
`timeless_metrics/bench/results/2026-07-31_catalog_publication.md`.
