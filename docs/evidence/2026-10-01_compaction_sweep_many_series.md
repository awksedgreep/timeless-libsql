# Compaction sweeps over many short-lived series — 2026-10-01

Issues: [#92](https://github.com/awksedgreep/timeless-libsql/issues/92),
[#93](https://github.com/awksedgreep/timeless-libsql/issues/93)

## What was reported

A metrics plane written to by one `timeless_beam_acct` collector for 13.6
hours (2,500 series a tick at 10 s, 38,810 series in all, the stack's
retention and rollup ladder) held 3.3 million raw-tier chunks of 3.8 points,
had never merged a compressed chunk, kept one thread at a core, took 2 GB,
and answered a PromQL question with `storage is temporarily busy`.

## Why

Three things, each of which the other two made worse:

1. Every `compact-step` was given `i64::MAX` as its cutoff, so a raw chunk
   flushed between two steps was eligible at once. The sweep found fresh
   raw chunks at every step, never reported that it was done, compressed
   each few points by themselves, and ran into the next sweep.
2. Every step planned the sweep afresh: a walk of the whole chunk index,
   cloning the metadata of every eligible chunk, of which it then took 64
   series' worth. At 605 steps a sweep over 3.3 million chunks that is two
   billion visits a sweep. The bounded rollup cycle, which shares the
   sweep's steps, walked the index for its series list at every step too.
3. Compressed chunks merged only when a group held at least half the
   32K-point target. A series of thirty-point chunks needs 546 of them for
   that; a process's series never gets there.

## What changed

- The metrics server passes the time a sweep began as the cutoff of every
  step (`compact-step:<series>:<points>:<bytes>:<cutoff>`); a chunk whose
  newest sample is at or past it waits for the next sweep.
- The engine plans a sweep once for its cutoff and steps through the plan;
  a group whose chunks retention has taken is passed over; a sweep found
  empty is not planned again until a chunk has been added. The rollup
  cycle reads its series once, at its start.
- Compressed chunks merge size-tiered by count: smallest first, a chunk
  joining a run only while it is no larger than what the run holds, and a
  run of four or more merged (two, for a series not written in the last
  hour). A large chunk is never rewritten for a small arrival.

## Method

A scratch plane on this host with the stack's settings sped up tenfold:
`TIMELESS_METRICS_FLUSH_INTERVAL_SECS=1`,
`TIMELESS_METRICS_COMPACT_INTERVAL_SECS=10`,
`TIMELESS_METRICS_RAW_RETENTION_SECS=604800`,
`TIMELESS_METRICS_ROLLUPS=1h@30d,1d@365d,30d@forever`. A writer imported
2,000 series every second over `/api/v1/import/prometheus`: 1,500 steady and
500 "process" series of which a tenth were replaced by new ones each tick
(about fifty new series a second, some sixty times the collector's rate).
Every thirty seconds it read `/select/metrics/stats`, the process's CPU time
and resident size from `/proc`, and timed one PromQL instant query over every
series of the host with a three-second lookback. Eight minutes a run. The
writer is `harness.py` in the session scratch directory; the numbers are
from the second runs.

### Before (release `eb2ed46`, 0.8.6)

| minute | series | raw-tier chunks | points a chunk | merge steps | sweeps done | CPU of a core | RSS | `{host="h"}` at 3 s lookback |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0.5 | 3,430 | 28,758 | 2.09 | 0 | 2 | 14.6% | 51 MB | 200 2096 in 80 ms |
| 2.1 | 7,847 | 144,001 | 1.71 | 0 | 5 | 47.6% | 151 MB | 200 2095 in 70 ms |
| 4.1 | 13,721 | 239,588 | 2.06 | 0 | 5 | 67.7% | 235 MB | 200 2092 in 124 ms |
| 6.2 | 19,614 | 304,265 | 2.44 | 0 | 5 | 76.1% | 346 MB | 200 2097 in 1960 ms |

The five sweeps finished in the first minute; the sixth ran to the end of
the measurement.

### After (this change)

| minute | series | raw-tier chunks | points a chunk | merge steps | sweeps done | CPU of a core | RSS | `{host="h"}` at 3 s lookback |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0.5 | 3,429 | 11,189 | 5.36 | 0 | 2 | 6.7% | 37 MB | 200 2097 in 21 ms |
| 2.1 | 7,850 | 28,348 | 8.68 | 3,062 | 11 | 9.9% | 59 MB | 200 2094 in 24 ms |
| 4.1 | 13,780 | 52,643 | 9.38 | 7,653 | 23 | 16.0% | 83 MB | 200 2097 in 39 ms |
| 6.2 | 19,675 | 70,497 | 10.53 | 15,472 | 32 | 24.2% | 128 MB | 200 2098 in 59 ms |

Chunks a quarter of what they were at the same point and still merging,
sweeps ending (thirty-seven in eight minutes against a ten-second
interval), a third of the CPU, less than half the memory, and the query a
tenth of the time, with no refusal. What CPU still grows with is the number
of series ever seen, which the writer drives at some sixty times the
collector's rate: the rollup cycle reads every series whose first bucket
has not settled, and no hourly bucket settles in an eight-minute run.

## Gates

`cargo test -p timeless-core` (221), the metrics server crate with the
release extension and ignored tests included (192), Clippy and rustdoc with
warnings denied in both workspaces, and formatting. The engine's contracts
were extended: small chunks merge by count in tiers, a sweep is planned once
and leaves what is newer than its cutoff, a drained sweep is not planned
again until a chunk is added, a stale planned group is passed over, and an
ended series' pieces are put together once; the server's: a scheduled sweep
is planned once and leaves what is newer than its cutoff.

## Reads during a sweep (#94)

A read is several statements, each admitted by the writer gate only while
no writer holds or waits for it, and begun again from the first when one is
refused. A sweep whose steps follow each other ten milliseconds apart
refuses such a read at every step for as long as it runs. The server now
waits between two steps, for up to half a second, while a read is in
flight.

Same plane settings as above, 4,000 series a second (a tenth of them new),
and one PromQL instant query over the host's series every 100 ms for three
minutes, `reads.py` beside the writer:

| | without the yield | with it |
|---|---:|---:|
| reads | 814 | 1,178 |
| refused `storage is temporarily busy` | 3 | 0 |
| p50 / p99 / max | 36 ms / 2,666 ms / 5,025 ms | 46 ms / 167 ms / 221 ms |
| read retries inside the server | 10,332 | 5,626 |
| sweeps completed | 13 | 11 |
| yields | — | 971, 35.5 s in all |

What remains of the question is the shape of a read: a permit a statement,
not a permit a read, so that a long read on a store with a long sweep is
still begun again whenever the next step is faster than it.

## Two days on: the walks that were left (2026-10-03)

A plane on the build above, fed by `timeless_beam_acct` for two days with
the stack's settings, held 139,484 series, 241,841 raw-tier chunks of 185
points and 191,160 rollup chunks. Its sweeps ended, 582 of them on
schedule, and it still took a quarter of a core: 3,300 steps a sweep at
22 ms a step, twelve hours of sweeping in forty-eight.

Three walks of the whole chunk index were still made at every step. The
server read the extension's statistics before and after each step, to say
what the step had done, which only a traced sweep uses; and the bounded
rollup cycle found the newest sample in the store again. And the steps
were as many as they were because a rollup step stopped at 64 groups
looked at, of 418,000, nearly all with nothing to roll up.

The server now reads the counters only for a traced sweep; the rollup
cycle keeps the newest sample of its start; a rollup step writes at most
its budget of chunks and looks at up to 64 times as many groups to find
them.

Two copies of a read-only snapshot of that store, no writes, a sweep every
twenty seconds, two minutes each:

| | before (`cfb48b6`) | after |
|---|---:|---:|
| CPU of a core | 41.4% | 2.3% |
| sweeps completed | 0 (the first had not ended) | 5 |
| steps a sweep | about 3,300 | 103 |
| time in steps | all of it | 2.6 s |
| longest step | — | 100 ms |
