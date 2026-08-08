# Trace posting-list time-window profile

Date: 2026-08-07

Session: 4 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)

Measured source: `93a83ca31b97470d4e39f50a39f411e64454d8e2`

Evidence: [`2026-08-07_trace_query_posting_windows.json`](evidence/2026-08-07_trace_query_posting_windows.json)

Evidence SHA-256: `b9df7ffd00a2c750d1789114e24b697420bf25b38fd0dd164655049c7004a6f4`

## Existing boundary

Trace postings already participate in time pruning before payload access. The
SQLite store intersects the `(term, block_id)` primary-key postings for every
service, operation name, kind, and status predicate. It then joins those block
IDs to block metadata and applies inclusive `ts_min`/`ts_max` overlap (plus
duration extrema) before the engine receives candidate locations. Returned
locations are ordered by block start time. No server or benchmark reads the
private tables directly; the evidence uses public trace rows and cumulative
`timeless_stats` work counters.

The retained layout intentionally does not duplicate time columns into every
posting row. Session 4 first had to prove that this omission caused repeated
payload reads for time-disjoint candidates before paying that write, index,
WAL, migration, validation, and maintenance cost.

## Release profile

The deterministic release fixture contains 131,072 rich spans admitted as 16
public 8,192-span batches. Public optimize leaves 48 compressed blocks: one
block per status partition per time box. Every profiled request ran 20 measured
iterations after three warmups in an isolated direct-SQL process.

| predicate | one-box candidates / 48 | one-box p95 | four-box candidates / 48 | four-box p95 |
|---|---:|---:|---:|---:|
| `service='bench'` | 3 | 1.531 ms | 12 | 5.186 ms |
| `name='GET /baseline'` | 3 | 0.447 ms | 12 | 1.693 ms |
| `kind='internal'` | 3 | 0.269 ms | 12 | 1.086 ms |
| `status='error'` | 1 | 0.291 ms | 4 | 1.528 ms |

Candidate and payload-block counts are identical. The one-box service, name,
and kind queries prune 45 of 48 blocks before payload access; their four-box
queries prune 36. Status queries prune still further because flush and optimize
already maintain status-pure blocks. Decoded-span and matched-row counts are
exact for every shape. The harness fails if a future plan reads even one block
outside these theoretical overlapping partitions.

The isolated process HWM is 15,048 KiB. The unchanged fixture contains 576 term
rows and 49,152 trace-index rows, reports 2,523,136 index bytes and 666,844
logical payload bytes, and closes with a 68,165,632-byte database and zero WAL
or SHM bytes. Admission measured 217,111 durable spans/s and optimize measured
394.447 ms in this run. Those values are preserved as complete evidence, but
they are not claimed as write changes because Session 4 changed no storage or
write path.

## What a later experiment would need to prove

A term-local bound can outperform the current block-time join only when all of
the following are true:

1. the query time window overlaps a block;
2. the block contains the requested term somewhere; and
3. every occurrence of that term lies outside the requested subrange.

That requires temporal clustering within an otherwise overlapping block. The
measured service, name, and kind values span each relevant block, while a status
term's range is already the range of its status-pure block. Adding block bounds
to posting rows would therefore reject nothing measured here. Reordering the
posting key by block time could reduce metadata-row work at much larger block
counts, but it still could not reduce the payload candidates recorded here.

Reconsider term-local extrema only with production-shaped evidence that counts
these within-block false positives and shows material narrow and broad wins.
Any proposal must then measure additional bytes per posting, insert and optimize
CPU, WAL/checkpoint behavior, migration of nullable legacy rows, corruption,
transactions and rollback, retention deletes, and old-format reopen. Missing
bounds must remain a conservative exact fallback.

## Verdict

Session 4 passes by stopping before its prototype gate. The hypothesized
repeated decompression of time-disjoint posting candidates is absent from the
observed workload, and the existing block-time join already provides the
candidate reduction a duplicated block-time posting layout would offer. The
Rust evidence harness and exact work assertions are retained. No prototype had
to be reverted, and no storage schema, posting row, block format, batch, codec,
transaction, retention, optimize, migration, public SQL, or Rust API behavior
changed. Session 5 may proceed from the unchanged production layout.
