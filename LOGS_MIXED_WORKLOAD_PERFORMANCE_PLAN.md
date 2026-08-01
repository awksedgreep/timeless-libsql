# Logs mixed-workload performance plan

Status: active on `poc/rust-logs-api-v3` (2026-08-01)

This plan improves the existing `timeless_logs` virtual table and the Rust
logs API without replacing or reproducing the storage engine. Work is split
into independently measurable sessions so each change can be kept or reverted
on evidence.

## Fixed contracts

- The extension owns the 8,192-entry automatic flush threshold.
- HTTP requests continue to enter through logs batch-blob v0 and
  `INSERT INTO logs(logs) VALUES (?1)`.
- Flush remains raw-first; compression and merging remain maintenance work.
- Buffered entries remain queryable before flush.
- SQLite transaction, rollback, recovery, and cross-connection semantics
  remain exact.
- Level-pure block pruning remains enabled unless a controlled benchmark
  disproves its value.
- Improvements should benefit direct SQLite/libSQL users wherever the useful
  boundary is inside the extension.

## What the current evidence says

- Direct logs batch-blob ingestion, including automatic raw flushes, has
  measured about 1.32M entries/s. The mixed HTTP POC kept low admission p99
  through 76.3K entries/s and filled its writer queue at the 124K step. These
  are different workloads, but the gap shows that HTTP/query concurrency is a
  larger immediate constraint than raw block encoding alone.
- Level-pure blocks changed the 1M-entry `level=error` query from 356.3ms to
  22.9ms (15.6x faster) and improved the measured compression ratio. The
  partitioning feature is valuable; its current comparison sort may not be.
- A query currently holds both the extension read permit and the engine
  transition read guard while it reads and decodes blocks, filters entries,
  and sorts the complete result. The cursor also renders metadata JSON before
  releasing its read permit.
- A writer waiting for existing readers is not visible to new readers, so new
  read permits can barge ahead of it.
- `ORDER BY ts ... LIMIT ...` is not pushed into the virtual table. A request
  for the latest 100 rows can materialize and sort every matching row before
  SQLite discards all but 100.
- `optimize` can repeatedly recompress a small compressed tail as new raw
  blocks arrive. A full 1M-entry logs optimization has measured about 1.37s,
  so maintenance pause time and rewrite amplification must be explicit.

## Current level partitioning, precisely

An automatic or explicit flush groups the buffer by the four possible levels
and writes one raw block for every level present. Therefore an 8,192-entry
flush normally produces:

- one block if the entire buffer has one level;
- two or three blocks if only those levels are present;
- four blocks when debug, info, warning, and error all occur.

Four is likely for a large mixed buffer, although production configurations
that suppress debug commonly produce three. The blocks are not equal-sized:
their entry counts follow the traffic's severity distribution.

Optimize does not blindly create four blocks. It compresses and merges blocks
inside separate level partitions, targets approximately 8,192 entries per
output block, and never merges different levels. Over a large optimized data
set, block counts therefore follow the number of entries per level plus at
most one small tail per level.

`level:error` already consults the `_terms` posting list and reads only blocks
carrying `level:error`. The expected reduction is distribution-dependent, not
always 4x: uniform levels approach 4x; a 5% error population can approach 20x
entry avoidance; an info-heavy query saves less. The measured realistic error
query improved 15.6x.

The persisted block header is exactly 38 bytes:

```text
version:1 | codec:1 | entry_count:4 | ts_min:8 | ts_max:8 |
four column lengths:16
```

It has no reserved severity byte. Batch-blob v0 has reserved framing bytes,
but that is the ingest wire format, not the persisted block format. A durable
severity tag would require a new block-format version or a new `_blocks`
metadata column. Today the in-memory partition tag is derived during recovery
from the existing `level:*` posting lists. Because normal queries already use
those posting lists before reading payloads, a header tag is expected to help
recovery bookkeeping rather than steady-state query I/O. It should not be
added without a recovery benchmark proving material value.

## Measurement contract

Before optimization work, add a deterministic workload and report both API
and storage completion. Every comparison must record:

- admitted entries/s;
- extension-completed entries/s and final drain time;
- writer queue depth and oldest queued-batch age;
- write p50/p95/p99;
- query p50/p95/p99 by query shape;
- read-permit wait and hold time;
- candidate blocks, payload blocks read, decoded entries, and returned rows;
- raw/compressed block counts and bytes;
- optimize duration, blocks/entries rewritten, and ingest pause time;
- peak memory;
- exact result parity against the current implementation.

Run at least these modes over the same pinned data:

1. direct extension ingestion with no queries;
2. HTTP ingestion with no queries;
3. HTTP ingestion with one query worker;
4. HTTP ingestion with two query workers;
5. a drain-to-zero phase after producers stop.

Admission throughput must never be presented as durable or completed
ingestion throughput.

## Session 1 — Instrument and pin the baseline

- [x] Make the HTTP workload deterministic through a recorded seed or fixture.
- [x] Add completed-ingestion, queue-age, and final-drain reporting.
- [x] Measure engine phases separately: batch parse, metadata normalization,
      buffer append, flush partition/order work, term extraction, raw encode,
      shadow-table write, and transaction completion. The current SQLite
      statement timer includes blob decode, append/flush, and autocommit; the
      extension counters split the material flush phases from that total.
- [x] Measure query permit wait/hold time and materialized-versus-returned rows.
- [x] Record no-query and mixed-load baselines using the measurement contract.

Pinned results are recorded in
`../timeless_logs/bench/results/2026-08-01_rust_logs_api_session1.md`. Peak RSS
was not sampled in these short attribution runs and remains mandatory for the
final Session 7 matrix.

Exit criterion: the bottleneck attribution is repeatable, and admitted work
cannot be mistaken for completed SQLite ingestion.

## Session 2 — Writer fairness and shorter permit lifetime

- [x] Track waiting writers in `WriterGate`.
- [x] Once a writer is waiting, prevent later readers from barging ahead;
      return the existing retryable busy-style result.
- [x] Release the virtual-table read permit immediately after engine result
      materialization, before metadata JSON rendering.
- [x] Add concurrency tests proving no missing rows, stale block locations,
      starvation, deadlock, or transaction-visibility regression.
- [x] Repeat the one- and two-reader mixed workloads.

The pinned Session 2 comparison is appended to
`../timeless_logs/bench/results/2026-08-01_rust_logs_api_session1.md`.

Exit criterion: writers make bounded progress under continuous query load,
with exact query results and no new HTTP errors after retry.

## Session 3 — Shrink the engine read critical section

- [x] Separate protected snapshot work from CPU-only query work.
- [x] Under the transition guard, resolve candidate locations, obtain the
      payload ownership needed to survive optimize/prune, and snapshot the
      matching buffer generation.
- [x] Release the guard before safe decoding, per-entry filtering, and final
      result ordering.
- [x] Measure the extra memory of owned payload snapshots. If it is excessive,
      evaluate generation pins or deferred block reclamation rather than
      accepting unbounded duplication.
- [x] Preserve rollback, prune, optimize, and cross-connection publication
      tests under forced interleavings.

The first implementation owned every candidate payload and was rejected after
one query retained 304,952,873 bytes. The final SQLite path uses the host
SELECT's stable database snapshot: it retains block locations, releases both
guards, then streams one old row version at a time. Stores without snapshot
isolation retain the conservative owned-payload fallback. Forced tests prove
flush, optimize, and prune can publish after snapshot while the exact captured
generation remains readable.

Exit criterion: query CPU no longer blocks the writer, and snapshot ownership
does not accumulate payload bytes on SQLite. Whole-result materialization is
measured separately and was the blocking memory issue taken into Session 4.

## Session 4 — Push bounded query intent into the extension

- [x] Teach `xBestIndex`/`xFilter` to accept `LIMIT` and `OFFSET`.
- [x] Consume a supported single-column `ORDER BY ts ASC|DESC` only when the
      engine guarantees that order exactly.
- [x] Add ascending/descending bounded query execution. Traverse blocks in
      time order and stop when remaining block bounds cannot displace the
      current top `limit + offset` matches.
- [x] Keep SQLite rechecks until every pushed constraint has exact semantics.
- [x] Add tests for overlapping block ranges, duplicate timestamps, buffered
      entries, sparse matches, offsets, and both orders.

Exit criterion: a latest-100 query materializes approximately the rows it
needs rather than the complete time-window result, with byte-for-byte API
parity.

The extension now claims SQLite's special LIMIT/OFFSET constraints only for a
single exact `ORDER BY ts ASC|DESC` plan. It returns the ordered
`LIMIT + OFFSET` prefix, leaves LIMIT/OFFSET and row predicates for SQLite to
recheck, and declines the bounded plan for strict timestamp inequalities,
message LIKE, duplicate predicates, or any unsupported filter. This makes
partial pushdown conservative: a row that SQLite might reject can never
displace a valid row from the bounded window.

The engine keeps a bounded heap with deterministic equal-timestamp ordering.
Candidate blocks are traversed by `ts_min` ascending or `ts_max` descending;
once the next block's bound cannot displace the worst retained row, the rest
are skipped without payload reads or decoding. Exact tests cover overlapping
ranges, duplicate timestamps across blocks and the buffer, sparse metadata
matches, zero and oversized limits, offsets, both directions, and the planner
fallbacks. The complete Rust workspace, CLI/oracle/crash suite, and the
extension-backed 8,192-entry API test pass.

On the final two-reader database (3,109,000 raw entries), an isolated
latest-100 query took 77.91ms inside the engine. It returned 100 rows, decoded
141,000 rather than 3,109,000 entries, read 20.45MiB, and skipped 1,424 of
1,492 candidate blocks. The remaining 68 blocks overlap the newest timestamp
in this deliberately dense 21-second benchmark; their metadata bounds cannot
soundly exclude them.

The pinned mixed workload remained write-healthy (458.8K completed entries/s
with one reader and 467.3K with two, zero queue at every boundary), while
query p99 improved from 2.40s to 1.83s and from 2.09s to 1.95s respectively.
The bounded calls returned no more than 100 rows each and skipped 8,087 blocks
across 92 one-reader calls and 13,415 across 179 two-reader calls.

Overall process HWM was still 5.66GiB with one reader and 6.84GiB with two.
This does not come from the bounded result sets: the workload intentionally
executes one unbounded native-count candidate scan and one SQLite-rechecked
substring scan in every five queries. Those shapes still materialized
millions of `LogEntry` values and dominate allocator high-water behavior.
Session 4's limited-row exit criterion is met; Session 5 is now the explicit
whole-workload embedded-memory gate.

## Session 5 — Native exact filtering and counts

- [ ] Move exact message matching below the virtual-table boundary after
      trigram candidate pruning, without weakening SQLite `LIKE` semantics.
- [ ] Add a native filtered-count path that uses level-pure block metadata
      whenever a complete block is provably inside the query and decodes only
      boundary or non-exact blocks.
- [ ] Use native count and bounded query paths from the Rust API without
      creating an API-only storage behavior.
- [ ] Benchmark error count, service+level count, substring, and latest-tail
      queries independently.

Exit criterion: direct SQLite/libSQL callers and the API share the same faster
query implementation and exact oracle counts.

## Session 6 — Reduce compaction amplification

- [ ] Report raw compression separately from compressed-block merging.
- [ ] Measure how often entries in a small tail are rewritten across repeated
      optimize calls.
- [ ] Evaluate size-tiered merging or a minimum output-fill rule so a raw
      block is compressed once and compressed tails merge only when the
      resulting block is sufficiently full.
- [ ] Evaluate incremental optimize work budgets based on raw backlog/bytes,
      not an arbitrary unconditional timer interval.
- [ ] Preserve the one-hour merge-span cap, level partitioning, atomic swaps,
      retention behavior, and recovery compatibility.

Exit criterion: lower bytes rewritten per ingested byte and bounded optimize
pause time without a material query or compression-ratio regression.

## Session 7 — API scheduling and final comparison

- [ ] Sweep one, two, four, and eight SQLite readers after extension fixes.
- [ ] If necessary, apply API query admission fairness using observable writer
      backlog; do not add a second storage buffer or flush threshold.
- [ ] Consider grouping multiple already-admitted SQLite statements into one
      host transaction only as a separately measured API optimization. It must
      not change the extension's 8,192-entry buffer contract.
- [ ] Run the full pinned matrix and compare completed ingestion, query
      latency, optimize pauses, disk size, and memory with the Elixir API.

Exit criterion: a production-boundary decision based on completed work and
bounded tail latency, not queue admission alone.

## Deferred decision gate — flush partition implementation

Do not change partitioning as part of Sessions 1–7 without discussing the
measurements first. Benchmark these alternatives in isolation:

1. current stable sort by `(level, ts)`;
2. an in-place linear four-way level partition with no raw timestamp sort;
3. four persistent in-memory level buffers, only if the linear partition
   remains material and transaction journaling can stay simple.

The likely candidate is the linear partition: the raw codec accepts unordered
timestamps, optimize already sorts merged entries by timestamp, and queries
sort final results. Acceptance still requires:

- exact query/order parity before and after flush/optimize/recovery;
- unchanged rollback and savepoint behavior;
- unchanged level-pruning effectiveness;
- no material optimized-size regression;
- a repeatable completed-ingestion improvement.

Separately benchmark recovery with the existing four `_terms` probes before
considering a persisted severity field. Do not version the block header or
alter the shadow schema merely to duplicate information already present in
the posting index.
