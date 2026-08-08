# Trace duration block pruning — 2026-08-07

## Outcome

`timeless_traces` now records exact inclusive duration extrema for each new
persisted span block. A `duration_ns` lower or upper bound can therefore reject
an impossible block before reading or decoding its compressed payload. Exact
per-span filtering remains authoritative for every surviving block.

The optimization belongs in the extension because it avoids a storage read
and benefits direct SQLite/libSQL, embedded Rust, the Rust traces API, and
Jaeger callers equally. It does not add TraceQL syntax or change the public
trace table schema, rich-span codec, 8,192-span batch, time-boxing, trace-id
index, compression, retention, or transaction model.

## Storage and compatibility design

The private `<table>_duration_bounds` table contains one small row per current
block:

```sql
CREATE TABLE <table>_duration_bounds (
  block_id INTEGER PRIMARY KEY,
  duration_min INTEGER NOT NULL,
  duration_max INTEGER NOT NULL,
  CHECK(duration_min <= duration_max)
) WITHOUT ROWID;
```

The physical name and schema are private implementation details. Direct users
negotiate the public `duration_block_pruning` capability and inspect
`duration_bounded_blocks` / `duration_unknown_blocks` through
`timeless_stats('<table>')`; they must not access the side table.

- New flushes and optimize rewrites publish the payload, trace/term indexes,
  and duration row in the caller's SQLite transaction.
- Opening an older database creates only the empty side table. A missing row
  means unknown, so the block is decoded and filtered exactly.
- Public `optimize` backfills unknown blocks one block at a time, validates the
  declared entry count, and updates only the small metadata row. The positive
  `optimize:<max source spans>` budget also bounds this work.
- Rollback removes new or backfilled metadata with the host transaction.
- Optimize/prune deletes duration rows before their corresponding blocks.
- Orphaned or inverted metadata fails closed with an actionable corruption
  error on connection/reopen.

An initial implementation placed the two extrema beside each compressed BLOB
in `<table>_blocks`. Although correct and fast, updating 42 rows copied roughly
27.5 MB into WAL because SQLite rewrote payload-sized records. That
implementation was reverted before commit. The side table reduced the same
backfill to one 4 KiB database page and 4,120 additional WAL bytes.

## Exact same-fixture evidence

The maintained Rust command documented in [TESTING.md](../TESTING.md) ran in
WAL mode against a copied legacy POC database containing 960,570 spans in 42
compressed blocks (26,238,576 logical payload bytes). The query selected the
real public table, constrained `service='api'`, used an impossible inclusive
duration lower bound, projected every rich-span field, ordered by
`start_ts DESC, span_id DESC`, and used `LIMIT 20`. Each phase ran 3 warmups
and 20 measured requests.

| Measurement | Legacy exact fallback | After public optimize |
|---|---:|---:|
| p50 | 485.671 ms | 0.030 ms |
| p95 | 536.720 ms | 0.040 ms |
| p99 | 536.720 ms | 0.040 ms |
| candidate/payload blocks, 20 queries | 840 | 0 |
| decoded spans, 20 queries | 19,211,400 | 0 |
| payload bytes read, 20 queries | 524,771,520 | 0 |
| returned rows | 0 | 0 |
| process RSS HWM | 55,676 KiB | 55,676 KiB |

Backfill covered all 42 blocks and 960,570 spans in 519.005 ms, spending
514.348 ms in block read/decode/metadata computation. Before optimize the main,
WAL, and SHM files totaled 35,848,368 bytes. Immediately afterward they
totaled 35,852,488 bytes. A truncate checkpoint left a 35,794,944-byte main
database plus the 32,768-byte SHM file and a zero-byte WAL. Results, block
count, payload bytes, and RSS HWM were unchanged.

This is a controlled before/after storage-pruning measurement on one fixture,
not a VictoriaTraces or Jaeger competitive benchmark. The very large speedup
applies when duration extrema can reject persisted blocks. Broad or matching
duration predicates still pay normal trace-id, term, time, payload, and exact
row-filtering costs.

## Regression boundary

The checked-in gates cover:

- inclusive lower/upper boundaries, mixed and unknown blocks, impossible and
  inverted intervals, exact results, and query-work counters;
- fresh flush and optimize metadata, legacy open, bounded backfill, repeated
  optimize, rollback, delete/prune consistency, and payload identity;
- real-extension schema creation/drop, direct SQL filtering, cold reopen,
  corrupt metadata rejection, public stats, and capability negotiation;
- complete rich-span projection and unchanged Rust/HTTP/Jaeger query paths;
- the maintained large-fixture latency, work, WAL/checkpoint, storage, and HWM
  evidence command.

## Operator procedure

No special migration command is required. After installing a compatible
extension, open a copy first, confirm the capability, then observe coverage:

```sql
SELECT json_extract(
  timeless_capabilities(),
  '$.signals.traces.duration_block_pruning.version'
);

SELECT key, value
  FROM timeless_stats('traces')
 WHERE key IN ('duration_bounded_blocks', 'duration_unknown_blocks');

INSERT INTO traces(traces) VALUES ('optimize');
```

The database remains exact before the optimize. Run the maintenance command
when its one-time read/decode work is acceptable, repeat a bounded form if
needed, and wait for `duration_unknown_blocks = 0` before expecting complete
duration pruning. Preserve the pre-upgrade backup; never manipulate private
shadow tables.
