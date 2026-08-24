# timeless-demogen

Synthetic telemetry for timeless demos and screencasts: tens of thousands to
hundreds of thousands of metric series, millions of log entries, and millions
of spans — realistic-looking, mutually correlated, deterministic, and
generated **from inside a sqlite3 session** if you want.

Everything here is detached from the parent workspace on purpose (same as
`tools/bench`): building or shipping timeless never touches this code, and
the generator talks to a stock `libtimeless_ext` artifact through the same
public Tier 2 batch surface any producer would use. Three crates:

- `core/` — the pure generator (fleet model, blob encoders, drivers; no deps)
- `ext/` — **`libtimeless_demogen.so`**, a loadable extension exposing
  `timeless_demo(...)` as a SQL function
- `.` — `timeless-demogen`, a CLI front end for scripted seeding

## The in-shell demo (the screencast flow)

```sh
cargo build --release -p timeless-ext          # at the repo root, once
cd tools/demogen/ext && cargo build --release  # the demo module
```

```text
$ sqlite3 demo.db
sqlite> .load ./target/release/libtimeless_ext
sqlite> .load ./tools/demogen/ext/target/release/libtimeless_demogen
sqlite> PRAGMA auto_vacuum=INCREMENTAL;   -- BEFORE anything writes the file
sqlite> PRAGMA journal_mode=WAL;
sqlite> .timer on
sqlite> SELECT timeless_demo('seed', 'medium');
```

Progress streams to the terminal while it runs; the query result is the
ingest summary plus the storage report — for the medium profile on a
desktop, 593 MB of raw telemetry stored as 53.8 MB of data blocks (11.0x)
in a 76.9 MB file, seeded in ~16 s. Then explore in place:

```sql
SELECT count(*) FROM timeless_series('metrics');

-- error ramp: the incident jumps out of a 5-minute bucket count
SELECT bucket_ts, n FROM timeless_log_buckets(
  'logs','level','{"service":"auth","level":"error"}', :start, :stop, 300000);

-- indexed metadata columns read like a table
SELECT ts, level, message FROM logs
 WHERE service='auth' AND level='error' ORDER BY ts DESC LIMIT 20;

-- slowest failing traces in the incident window
SELECT lower(hex(trace_id)), duration_ns/1000000 AS ms FROM spans
 WHERE status='error' AND start_ts BETWEEN :start_ns AND :stop_ns
 ORDER BY duration_ns DESC LIMIT 10;

-- log -> trace pivot: error logs carry a real trace_id half the time
SELECT json_extract(metadata,'$.trace_id') FROM logs
 WHERE level='error' AND json_extract(metadata,'$.trace_id') IS NOT NULL LIMIT 5;
```

Keep it moving live (state persists in a tiny `demogen_state` table, so
walks and counters continue across calls and sessions):

```sql
SELECT timeless_demo('tick', 30);     -- instantly backfill the last 30s
SELECT timeless_demo('follow', 60);   -- real-time appends + flush every ~2s
SELECT timeless_demo('report');       -- compression/storage report, any time
SELECT timeless_demo('info');         -- built-in cheat sheet
```

## Bring your own tables

Name the table you want filled and the generator fills exactly that one:

```sql
CREATE VIRTUAL TABLE web_server_metrics USING timeless_metrics;
SELECT timeless_demo('seed','small','web_server_metrics');
--> seeded profile 'small' (seed 42) into metrics (web_server_metrics)
```

Comma-separate to name more than one, at most one table per signal:

```sql
SELECT timeless_demo('seed','small','my_metrics, app_logs');
```

Named tables must already exist and be timeless vtables — your names, your
creation arguments (`retention`, `index_keys`, `attribute_indexes`), and
nothing created or guessed on your behalf. Unknown names, ordinary tables,
and two tables claiming the same signal are all rejected explicitly.

The seed integer and the table list are told apart by type, so
`('seed','small',7)` and `('seed','small',7,'my_metrics')` both do what they
look like.

**Name nothing and the tables are inferred**: any timeless vtables already in
the database are used as-is, and a database with none gets the full
three-signal demo created for it. That is what the screencast relies on — it
declares its own three tables, then seeds.

**Seeding refuses a table that already holds data.** These tables are
append-only, so synthetic telemetry mixed into real data cannot be taken back
out. Seed into an empty table or a scratch file.

Seeding only some signals is fine. Logs without traces means those error logs
carry no `trace_id` — there are no spans to point at — and the compression
report simply omits the signals that have no table.

## The compression report

The report never lets bookkeeping muddy the compression story. Raw bytes
are counted at generation time and mean *the logical rows as the public
surface returns them* — for a metric sample 16 B (ts + value), for a log
entry ts + level + message + metadata, for a span the ids, kind/status,
timings, and every string field. Stored and index bytes come from the
engine's own public `timeless_stats` counters (`bytes_on_disk`,
`index_bytes`): data block payload only. Before measuring, the WAL is
checkpointed back into the main file and freed pages are vacuumed, so the
file line is the real on-disk size — and if free pages can't be returned
(auto_vacuum wasn't set in time) they're called out as reclaimable rather
than blended in.

```text
storage (raw = logical rows as queried; stored/index = engine block bytes on disk):
  metrics      8,553,600 samples  raw    137 MB -> stored   10.7 MB  ( 12.8x, 1.3 B/sample)  index 6.6 MB
  logs         2,000,000 entries  raw    217 MB -> stored   16.9 MB  ( 12.8x, 8.5 B/entry)  index 0.16 MB
  traces         959,694 spans    raw    239 MB -> stored   26.2 MB  (  9.1x, 27.3 B/span)  index 7.8 MB
  total    raw 593 MB -> stored 53.8 MB (11.0x); indexes 14.6 MB
  file     76.9 MB (data + indexes + btree overhead)
```

Generated metric values are quantized the way real collectors quantize
them (percentages to 0.1, byte gauges to pages, counters to integers) —
full-precision float noise is something no exporter emits, and it would
misrepresent the codec on production-shaped data.

`follow` is the one to pair with a traces live tail or a dashboard reading
the same file from another process (hence the WAL pragma above).

## Profiles

| profile | series | log entries | spans | window | seed time* |
|---|---|---|---|---|---|
| `small`  | ~4k   | 200k | ~200k | 30 min | ~2 s |
| `medium` (default) | ~35k  | 2M   | ~1M   | 60 min | ~16 s |
| `large`  | ~245k | 5M   | ~3M   | 60 min | minutes |

\* measured on a desktop; seeding doubles as an ingest-rate demo (~9M metric
samples/s, ~190k spans/s through the public batch surface).

## What the data looks like

- A fleet of services (`api`, `web`, `auth`, `billing`, …) × pods, each pod
  exporting a Prometheus-style catalog: cpu/memory/disk/net system metrics,
  `http_requests_total{path,status}`, latency sum/count, cache and queue
  gauges. Counters climb, cpu random-walks, gauges breathe on an hourly
  sinusoid. Cardinality is `services × pods × (14 + 7 × paths)`.
- Logs are request-shaped with `service`, `path`, `status` metadata (the
  vtab is created with `index_keys='service,path,status'`).
- Traces are 5–20 span call chains across services; error spans carry
  exception events and status messages (rich blob v2).
- **One incident is baked in**: for the middle third of the window, one
  service (`auth`) spikes cpu, its 500s jump ~150×, latency inflates, logs
  turn error-heavy, and traces slow down and fail.
- Everything is a pure function of the seed (timestamps anchor to "now" at
  seed time), so a rehearsed take replays identically.

## The CLI

Same generator, batch ergonomics — useful for scripting or very large seeds:

```sh
cd tools/demogen && cargo build --release
./target/release/timeless-demogen seed --db demo.db --profile large
./target/release/timeless-demogen seed --db demo.db --follow   # seed, then tail
./target/release/timeless-demogen live --db demo.db            # append forever
```

Every profile knob has a flag (`--services`, `--pods`, `--paths`,
`--minutes`, `--step-secs`, `--logs`, `--traces`, `--seed`, live-mode
rates); `--help` lists them.

`--tables` works the same as naming targets in SQL — same resolution, same
rules, same refusals, because both front ends share `core/src/tables.rs`:

```sh
./target/release/timeless-demogen seed --db demo.db --tables web_server_metrics
./target/release/timeless-demogen live --db demo.db --tables web_server_metrics
```

Without `--tables` the CLI uses any timeless vtables already in the file and
creates the three defaults if there are none. Seeding a table that already
holds data is refused. `--append` remains a separate, file-level guard:
seeding a database path that already exists needs it.

## Notes

- Load order: `libtimeless_ext` first, then `libtimeless_demogen`.
- Timestamps: metrics and logs are unix **millis**, traces unix **nanos**,
  matching `docs/SQL_API_REFERENCE.md`.
- The `ext/` crate must stay separate from the CLI crate: rusqlite's
  `loadable_extension` build (calls routed through the host's api-routines
  table, no SQLite linked) is incompatible with the CLI's `bundled` build.
  Both share `core/`, which has no dependencies at all.
