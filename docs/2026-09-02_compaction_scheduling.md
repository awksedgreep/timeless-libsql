# Incremental compaction: hot-path cost and block-span smearing (2026-09-02)

<!-- document-status: session-evidence -->

Branch `perf/p5-compaction-scheduling`, from `7beca58` (v0.7.8).

Host: Apple M1 Max (8P+2E, Asahi Arch 7.1.6, `schedutil` governor), 62 GB,
rustc 1.98.0, bundled SQLite 3.53.4. Release builds. Per
[TESTING.md](../TESTING.md), each benchmark ran twice and the second run is
quoted. Datasets are the deterministic ones in `tools/bench/src/datasets.rs`,
so the fixture is identical across every row below.

## What prompted this

`2b6a353` (2026-08-11, "Auto-optimize rides the flush path") made a budgeted
optimize pass ride `flush()`. A commit-by-commit rebuild of the logs ingest
path on this host isolates it as the only step that moves:

| commit | date | change | 1M-entry Tier 1 insert |
|---|---|---|---:|
| `2016a88` | 07-26 | F1-F6 capstone | 1.34 s |
| `29bda0f` | 08-01 | Bound logs compaction amplification | 1.42 s |
| `c686744` | 08-08 | CLP template message compression | 1.73 s |
| `e37a5f5` | 08-11 | Per-connection engine pins | 1.72 s |
| **`2b6a353`** | **08-11** | **Auto-optimize rides the flush path** | **3.37 s** |
| `c9ef3d3` | 08-15 | LogsQL work-cap | 3.39 s |
| `7beca58` | 08-30 | v0.7.8 | 3.38 s |

Most of that is moved work, not new work: logs `optimize` fell 1529 ms -> 264 ms
and traces 1884 ms -> 83 ms over the same window. Counting insert plus explicit
optimize, the shipped path costs 3.73 s against 3.39 s for the interval path
alone — so the urgent path also did ~10% MORE total work and produced a worse
layout.

## It is in the hot path

`LogEngine::push()` (`blocks/engine.rs`), called from vtab `xUpdate` per
inserted row, auto-flushes at `flush_threshold` (8192). `flush()` calls
`maybe_auto_optimize()`, which calls `optimize_inner()`, which takes
`transition_write()` and decodes, re-encodes and swaps blocks before the
`INSERT` statement returns. There is no background thread on this path.

Measured with `sqlite3 .timer on`, 48 x 8192-row inserts:

```
1:11ms  2:10ms  3:9ms  4:29ms  5:8ms  6:8ms  7:8ms  8:27ms  ...  48:28ms
```

Every 4th statement stalls, because the urgent trigger fires at 32,768 raw
entries and each flush adds 8,192. One row insert in ~32k pays a full
compaction pass inside the host's write transaction.

## The layout regression

Not file fragmentation: both databases show `freelist_count = 0` and near
identical page counts (2205 vs 2174 x 4 KiB). The cause is block ts-span
smearing. `plan_compressed_segment` sorts candidates by entry count to pair
similar-sized tiers for the 2x growth rule — correct for amplification, blind
to time. Merging two blocks that sit far apart yields a block spanning the
union INCLUDING the gap, and range pruning pays that width on every
overlapping query.

| | shipped | with span guard |
|---|---:|---:|
| blocks | 170 | 215 |
| block mean ts span | 198,935 ms | **55,777 ms** |
| block max ts span | 2,948,717 ms | **98,635 ms** |
| blocks over the 8,192 target | 15 | **0** |
| candidate blocks for the range query | 5 | 11 |
| service+level+range | 15.9 ms | **6.9 ms** |
| `optimize` | 263.8 ms | **29.6 ms** |
| storage | 9.03 B/entry | 9.09 B/entry |

Candidate count rises while time falls: the guard trades a few wide blocks for
more narrow ones, and narrow blocks are cheaper to decode than the 10,170-entry,
49-minute blocks they replace. The shipped max block span sits at 82% of the
1 h `merge_max_ts_span` the logs vtab passes — and that cap exists for
retention correctness, not pruning, so nothing bounded width before this.

### Tuning `merge_span_growth_limit`

| limit | blocks | mean span | max span | over target | storage | query |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 215 | 55,777 | 98,635 | 0 | 9.09 | 6.9 ms |
| **2** | **215** | **55,777** | **98,635** | **0** | **9.09** | **6.9 ms** |
| 3 | 215 | 55,777 | 98,635 | 0 | 9.09 | 6.9 ms |
| 4 | 199 | 82,996 | 2,162,381 | 5 | 9.07 | 10.5 ms |
| 8 | 172 | 181,763 | 2,948,717 | 14 | 9.04 | 15.8 ms |
| unbounded | 170 | 198,935 | 2,948,717 | 15 | 9.03 | 15.9 ms |

A step function: 1-3 reach the same layout, 4 lets welds back in. The default
of 2 sits mid-plateau rather than on the cliff edge.

## Two changes measured and rejected

**Hysteresis on the urgent trigger** (drain 4 budgets per urgent pass instead
of 1, so the backlog clears its own threshold). Predicted fewer passes for the
same work; measured worse on both axes — Tier 1 ingest 0.29M -> 0.23-0.26M
entries/s, Tier 2 0.51M -> 0.30-0.33M, and the range query 6.9 ms -> 9.2-9.7 ms.
A larger budget admits more compressed-merge groups per pass, which is both
more work and more widening. Storage did improve (9.09 -> 8.98 B/entry), so
there is a real storage/pruning tension here worth a deliberate decision.
Reverted.

**Removing the duplicated planning pass.** `maybe_auto_optimize` runs the full
`plan_optimize` via `optimize_backlog()`, then `optimize_inner` plans again
under the write transition. Replacing the first with a cheap metadata-only
candidate check measured identical (0.29-0.30M entries/s, same storage, same
query). It would change `optimize_count` semantics on a released stats surface
— no-op auto passes would start counting as optimize passes — so it was
reverted rather than shipped for no gain. The earlier O(index) urgency-probe
hypothesis was tested the same way and is also wrong: scanning the index on
every flush and skipping the scan entirely measure the same.

## Controls

Traces and metrics are byte-identical to baseline; neither engine was touched.

| control | baseline | branch |
|---|---|---|
| traces vtab ingest / storage | 0.17M spans/s, 34.94 B/span | 0.17M spans/s, 34.94 B/span |
| traces service+range (pushdown) | 24.8 ms | 24.8 ms |
| metrics Tier 1 / Tier 2 | 1.31M / 16.63M pts/s | 1.30M / 16.61M pts/s |
| metrics name+range / full scan | 4.5 ms / 173.8 ms | 4.6 ms / 176.0 ms |

Hot-path stall profile after the change: p50 9 ms, p90 21 ms, max 22 ms
(shipped: p50 8 ms, stalls 27-29 ms). The stall FREQUENCY is unchanged — every
4th flush — because the trigger was not touched. Only its cost fell.

## Gates run

`cargo fmt --all --check`, `cargo test --workspace --locked` (251 tests),
`cargo clippy --workspace --all-targets --locked -- -D warnings`,
`RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked`,
`cargo test --manifest-path servers/Cargo.toml` (311 tests), `tests/cli.sh`
(47 sections, including the 150,000-op randomized gate, five SIGKILL crash
recoveries, and section 43's size-tiered/bounded optimize),
`tests/correctness.sh` r1/r2/r3/r4/r8/logs-rich, and `tests/dbhealth.sh`. All
pass.

## Reconfirmed at v0.8.0 (2026-09-04)

Every number above was measured at `7beca58` (the v0.7.9 baseline). v0.8.0
landed the observability-schema surface, `prom_text`, admission/auth work, and
changes to `blocks/codec.rs`, `spans/codec.rs` and `rollup.rs` — none of which
touch the merge planner, but all of which sit under these benchmarks. Re-run on
the same host at `f1d5071`, same method, second runs quoted:

| logs | v0.7.9 | v0.8.0 |
|---|---:|---:|
| Tier 1 / Tier 2 ingest | 0.29M / 0.50M entries/s | 0.29M / 0.50M entries/s |
| service+level+range | 6.9 ms | 6.8 ms |
| `optimize` | 29.9 ms | 30.0 ms |
| level=error count | 26.5 ms | 26.8 ms |
| `count(*)` cold reopen | 486.8 ms | 492.5 ms |
| `LIKE '%timeout%'` | 606.2 ms | 621.3 ms |
| `timeless_log_buckets` | 176.9 ms | 177.8 ms |
| storage | 9.09 B/entry | 9.10 B/entry |

The block layout is byte-identical — 215 blocks, 55,777 ms mean span, 98,635 ms
max span, 0 over target — so the codec changes did not alter block formation and
the merge guard still holds its layout.

| traces | v0.7.9 | v0.8.0 |
|---|---:|---:|
| vtab / batch-v0 ingest | 0.17M / 0.27M spans/s | 0.17M / 0.27M spans/s |
| trace_id point lookup | 0.265 ms | 0.245 ms |
| status='error' count | 1.5 ms | 1.4 ms |
| service+range (pushdown) | 24.8 ms | 25.4 ms |
| `optimize` | 82.7 ms | 87.1 ms |
| storage | 34.94 B/span | 34.95 B/span |

| metrics | v0.7.9 | v0.8.0 |
|---|---:|---:|
| plain / Tier 1 / Tier 2 | 3.26M / 1.30M / 16.61M pts/s | 3.26M / 1.28M / 16.73M pts/s |
| name+range / full scan | 4.6 ms / 176.0 ms | 4.5 ms / 176.3 ms |
| `timeless_grid` TVF | 2.1 ms | 2.1 ms |
| rollup build / tier read | 121.7 ms / 3.2 ms | 121.9 ms / 3.2 ms |
| storage | 8.348 B/pt | 8.352 B/pt |

Everything sits inside the repository's noise band. The two largest movers are
logs `LIKE` (+2.5%) and traces `optimize` (+5.3%); neither is repeatable enough
across the paired runs to call a regression, and both are on paths the merge
guard does not touch.

## Not done here

- **Traces keeps the unguarded merge path.** `SpanEngine` has no
  closed-window exemption, so the same guard could strand blocks
  permanently; its block max span is 2,967,397,838,096 ns (~49 min, the same
  signature) but its queries never regressed, so there is no evidence
  justifying the risk yet. Porting the closed-window rule first would make
  the guard safe there too.
- **Getting compaction off the write path entirely.** The repo already has
  two templates: `health_vtab.rs` spawns a per-(file, table) scheduler thread
  that opens its own connection, sets a 5 s busy timeout, issues work through
  the front door as ordinary SQL, skips `.sqld/` paths, and gives up after
  five consecutive errors; and the signal servers run
  `wal_checkpoint(TRUNCATE)` every 300 s through their writer queue. The
  `optimize:<max_entries>` command those would drive is already public.
  `2b6a353`'s premise that "the extension has no timer of its own" was
  already false when written — the dbhealth scheduler shipped in v0.2.0 on
  07-28. This is the change that would actually remove the stalls rather than
  shrink them, and it deserves its own branch.
