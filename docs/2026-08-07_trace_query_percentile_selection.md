# Trace bucket exact-percentile selection experiment

Date: 2026-08-07

Session: 3 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)

Selection candidate: `5c74b5c3c61db4d196b26d1bcb9e46ae5a606ba7`

Candidate evidence: [`2026-08-07_trace_query_percentile_selection.json`](evidence/2026-08-07_trace_query_percentile_selection.json)

Candidate SHA-256: `fe78e041bc1b8e5145539958e7858038190152cd776db9518304807b26aabef8`

Full-sort control: `3830a5f8b29363809d4365316a4d8438842b16b4`

Control evidence: [`2026-08-07_trace_query_percentile_selection_control.json`](evidence/2026-08-07_trace_query_percentile_selection_control.json)

Control SHA-256: `414699725d6b559a3d4b12443e24ab1559c43b0b6f9005b0b7c3cf57c5477535`

## Experiment

The candidate replaced each bucket's complete unstable duration sort with
three progressively smaller in-place `select_nth_unstable` partitions. It used
integer nearest-rank indexes and checked SQLite cancellation before, between,
and after selection. The retained storage shape was unchanged: one duration
vector per `(bucket_ts, service)` and no rich-span rowset.

The control was built in a detached clean worktree at the exact Session 2 head.
Both clean release builds ran the same deterministic 131,072-rich-span fixture,
16 public 8,192-span batches, 48 optimized blocks, three warmups, and 20 measured
iterations. Candidate and control artifacts retain complete query work,
cardinality, response, storage, durability, and HWM evidence.

## Exact A/B result

| direct-SQL shape | full-sort p50/p95/p99 | selection p50/p95/p99 | result |
|---|---:|---:|---|
| all 16 time boxes | 17.209 / 17.438 / 18.368 ms | 17.167 / 17.776 / 18.200 ms | p50 0.2% faster; p95 1.9% slower; p99 0.9% faster |
| one time box | 1.277 / 1.507 / 1.565 ms | 1.285 / 1.363 / 1.365 ms | p50 0.6% slower; tail movement was not repeatable |

An immediately adjacent repeat reversed the apparent tail advantage: the
selection and control all-box p95 values were 20.521 and 20.150 ms, while the
one-box p95 values were 1.473 and 1.338 ms. Earlier Session 2 evidence measured
the identical full-sort path at 22.509 ms p95. The broad p50 remained close in
the contemporaneous checked pair, and the extension decode timer moved with
wall time even though it ends before percentile finalization. These facts make
the tail changes host/runtime variance rather than a defensible selection win.

Every run emitted exactly 16 broad rows/131,072 spans and one narrow row/8,192
spans. Candidate and control considered and read the same blocks, decoded the
same four columns and bytes, materialized the same scalar values, and produced
the same response bytes. Candidate direct-SQL HWM was 15,340 KiB versus 15,164
KiB for the control, a noise-level 176 KiB increase rather than a memory win.

Public batch admission and optimize varied by less than 7% across the repeated
runs with no percentile-path write change. The checked artifacts have identical
logical payload, physical database size, WAL/checkpoint state, block count,
duration coverage, index rows, timestamp range, and exact reopen count.

## Retained regressions

The selection implementation was reverted, but the experiment found useful
coverage gaps that remain fixed:

- the Rust `rich-traces` real-extension gate compares public
  `timeless_trace_buckets` results with an independent sorted oracle for empty,
  singleton, duplicate-heavy, ordered, reverse-ordered, and deterministic
  8,192-value buckets;
- those cases run against mixed buffered/persisted state, after public flush
  and optimize, and after cold reopen;
- percentile finalization checks cancellation before and after the bounded
  in-place sort, including a buffer-only regression whose cancellation cannot
  be observed during block decode; and
- fixed p50/p95/p99 ranks use integer nearest-rank arithmetic, preserving exact
  duplicate and boundary-cardinality behavior without floating-point indexes.

The existing `query_total_ns` counter measures the projected public row stream
through its final decode and deliberately does not include the TVF's subsequent
bucket aggregation/finalization. End-to-end direct-SQL latency remains the
authority for this experiment; the narrower counter is retained as a storage
work diagnostic and is not relabeled as bucket wall time.

## Verdict

Session 3 passes via its required negative path. Exact selection is correct and
bounded, but it does not materially improve the already projection-optimized
bucket query. The production engine therefore retains the simpler full sort.
No storage format, public SQL schema, batch, codec, index, transaction,
retention, compression, optimize, migration, or Rust API behavior changes.
Session 4 may proceed from the Session 2 implementation plus the stronger
percentile and cancellation regressions.
