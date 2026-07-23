# Bug: chunk index shadowing when two chunks share (series_id, min_ts)

**Status:** fixed here 2026-07-22 — donor fix ported to `crates/timeless-core/src/engine.rs` (key widened to `(PartitionKey, min_ts, chunk_seq)`; regression tests in `crates/timeless-core/tests/dup_min_ts.rs`; oracle now generates duplicate metric timestamps) — **fixed upstream** in timeless_metrics on 2026-07-22
(`native/tms_engine/src/lib.rs`, unreleased v6.1.3; see
`~/Documents/elixir/timeless/timeless_metrics/BUG_chunk_index_min_ts_shadowing.md`)
**Component:** `crates/timeless-core/src/engine.rs` (metrics engine only —
logs/traces have no such key)
**Severity:** data invisibility (silent), low probability in steady-state
scraping, higher under backfill / duplicate-timestamp ingest
**Found:** 2026-07-22, via the oracle property harness in this repo
(`tools/bench/src/oracle.rs`), run against this engine copy. Already noted
as a known engine limit in `RESULTS.md` (~line 133) and worked around in
the oracle generator; this report exists so the limit gets *fixed* rather
than permanently avoided.

## Summary

The chunk index is a `BTreeMap` keyed by `(PartitionKey, min_ts)`
(`engine.rs:413`). `BTreeMap::insert` on an existing key **replaces** the
value. If two chunks for the same series have the same `min_ts`, the
second insert silently shadows the first: the earlier chunk's points
become unqueryable even though its data remains in the chunk store.
`rebuild_index` (`engine.rs:1942`) has the same collision, so restart
does not repair it — whichever chunk the store scan yields last wins.

The txn journal is already *aware* of the edge: `index_insert_new`
(`engine.rs:690`) journals the old meta on a silent overwrite so rollback
can restore it. That preserves rollback correctness but not visibility —
the shadowed chunk is still invisible to every query on the committed
timeline.

## How it can happen

1. **Backfill / duplicate timestamps across flush boundaries** — two
   flush cycles each produce a chunk for series S whose earliest point
   has the same timestamp (re-ingested overlapping export, retried
   batches, clients that clamp/floor timestamps).
2. **Same-second scrapes with second-resolution timestamps** — the vtab
   timestamp column is epoch seconds, so this is easy to hit from SQL.
3. **Compaction landing on an occupied key** — a merged chunk keyed at a
   `min_ts` equal to a remaining chunk's `min_ts` for the same series.

## Reproduction sketch

```text
resolve series S
write_point(S, ts=100, v=1.0); flush_all()      # chunk A: min_ts=100
write_point(S, ts=100, v=2.0)                   # duplicate ts (backfill)
write_point(S, ts=200, v=3.0); flush_all()      # chunk B: min_ts=100 → shadows A
query_range(S, 0, 1000)                         # returns B's points only
```

The oracle harness reproduces this immediately if you remove its
strictly-increasing-per-series timestamp constraint
(`tools/bench/src/oracle.rs:29-35`).

## The fix (ported from timeless_metrics — apply the same shape)

Widen the key with a per-engine monotonic sequence:

```rust
/// In-memory only — restart recovery re-assigns fresh values.
type ChunkKey = (PartitionKey, i64, u64);

chunk_seq: AtomicU64,   // on Engine, init 0
fn next_chunk_seq(&self) -> u64 { self.chunk_seq.fetch_add(1, Ordering::Relaxed) }
```

The seq needs no persistence: every removal site derives its keys by
iterating the index, never by reconstructing them, so uniqueness within
one engine lifetime is sufficient. Collisions become impossible; range
scans stay cheap.

Sites in this copy (fewer than upstream — the centralized insert path
helps):

- `engine.rs:413` — index declaration, plus `index_read`/`index_write`
  signatures (523/527)
- `engine.rs:690` `index_insert_new` — THE insert path for all flush
  routes; add the seq here. The journal-the-overwrite band-aid inside it
  can then be deleted (overwrites can no longer happen).
- `engine.rs:506/509` — txn journal types `added: HashSet<...>` and
  `removed: Vec<(..., ChunkMeta)>` carry the index key; widen both, and
  the rollback replay at ~670 follows mechanically.
- `engine.rs:1527-1583` — compaction: `removed` set, and the
  remove/insert swap (collect full keys when scanning candidates, as
  upstream does).
- `engine.rs:1698, 1771` — query-side
  `range((pk, i64::MIN)..)` → `range((pk, i64::MIN, u64::MIN)..)`, and
  the `take_while` destructuring.
- `engine.rs:1900-1916` — retention `to_remove` key type.
- `engine.rs:1942` `rebuild_index` — insert with `next_chunk_seq()` per
  scanned chunk.

Upstream commit (timeless_metrics, same change against the pre-seam
code) is a working reference diff, including a regression test
`duplicate_min_ts_chunks_do_not_shadow` covering in-memory + restart.

## Follow-ups once fixed

- Remove the oracle generator's strictly-increasing constraint (or make
  duplicate timestamps a generated case) so the property harness
  actually exercises this — that constraint exists solely to dodge this
  bug (`tools/bench/src/oracle.rs:29-35`).
- Update the known-limits entry in `RESULTS.md` (~line 133).
- The txn-journal interaction deserves one dedicated test: flush two
  same-min_ts chunks inside a txn, roll back, verify both index entries
  vanish and re-flush works.
