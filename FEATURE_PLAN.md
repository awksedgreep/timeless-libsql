# Feature Plan — post-0.1.1 functionality

Six features agreed 2026-07-26, ordered into sessions. Check items off as
they land; add a session-log row per work session (the REVIEW_FIX_PLAN.md
discipline — it worked).

Sequencing: F1 → F2 are the operability package and F1 builds the
engine-resolution helpers every later TVF needs. F3 (rollups) is the long
pole and consumes F2's retention machinery. F4/F5/F6 are independent of
F3 and of each other — reorder freely if priorities shift.

- [ ] **F1** series catalog + stats TVFs
- [ ] **F2** automated retention (table argument)
- [ ] **F3** rollup ladder (downsampling)
- [ ] **F4** Q2 kernels for logs & traces
- [ ] **F5** batch blob ingest for logs & traces
- [ ] **F6** trigram index for log message search

## Working agreement

Red-green per item: smallest failing test first, then the fix, then the
full gate. No sleeps in concurrency tests. Numbers quoted from second
benchmark runs, interleaved A/B when comparing builds (session-to-session
drift exceeds most effect sizes — proven twice in RESULTS.md).

The verification gate, after every feature:

```sh
cargo test --workspace --all-targets
cargo build --release -p timeless-ext
./tests/correctness.sh r1 && ./tests/correctness.sh r2   # + r3/r4/r8 when touched
./tests/cli.sh
cd tools/bench && cargo run --release --bin oracle -- ../../target/release/libtimeless_ext.dylib
./tests/crash.sh target/release/libtimeless_ext.dylib    # when the write/flush path changed
```

## Cross-cutting invariants (violate any of these and R1-R8 unravels)

1. **The lock hazard.** No engine lock (journal mutex, series/index
   RwLock, transition guard) may be held across re-entrant store SQL.
   Multi-row DML opens a SQLite statement journal, which fires xSavepoint
   back into THIS engine's txn_savepoint → self-deadlock. Pattern:
   snapshot under lock → drop locks → store call → re-take locks →
   record. (Found live in `resolve_series_batch`; see engine.rs docs.)
2. **Rayon-free vtab paths.** Worker threads have no DbGuard binding —
   any engine method reachable from a vtab callback must be sequential
   (`*_by_id` kernel variants, `collect_metric` loop). Parallel variants
   are for embedded callers only and must say so in their doc comment.
3. **Generation coverage.** Any NEW store mutation that changes what
   `load_series()`/`scan()` return must bump `chunk_gen` (or be covered
   by the `_series` max-id watermark) in the same transaction, or another
   process's refresh will skip a reload it needed. Grep
   `bump_chunk_generation` before adding a mutating store method.
4. **Journal coverage.** Any NEW engine in-memory mutation reachable
   inside a transaction needs undo in the R1/R5 journal (frames, not the
   pre-savepoint single snapshot). If flush/compact/prune learn new
   tricks, tests/cli.sh section 6* and the oracle's txn ops must cover
   them.
5. **Passivity.** No background threads, no wall-clock policy decisions
   in the engine. Time-based behavior keys off DATA time (max ingested
   ts), which is deterministic, unit-agnostic, and replay-safe.
6. **The waist.** Kernels/TVFs are semantics-free reductions, bit-exact
   against naive evaluation (or, where pre-aggregation makes bit-parity
   impossible — F3 — the aggregate definitions are documented exactly and
   property-tested against naive bucket math). Anything referee-sensitive
   stays above the waist.
7. **ts units.** metrics = epoch seconds, logs = ms, traces = ns. The
   engine treats ts as opaque i64; only argument DOCUMENTATION mentions
   units. `prune:`/retention/rollup arguments are in the table's unit.
8. **macOS testing.** `.load` needs brew sqlite3; artifact is `.dylib`
   (cli.sh wants the `libtimeless_ext.so` symlink); profilers lie about
   inlined frames — trust interleaved A/B deltas, not samples.

---

## F1 — Series catalog + stats TVFs

**Goal.** Cheap SQL answers to "what series exist?" (today: full
decompress scan) and "how big/healthy is this table?" (today: nothing).
`timeless_series` completes the query waist: it is `list_metrics()` for
SQL callers.

**SQL surface.**

```sql
SELECT * FROM timeless_series('metrics');
-- name TEXT, labels TEXT (canonical JSON), series_id INTEGER,
-- min_ts INTEGER, max_ts INTEGER, points INTEGER, chunks INTEGER,
-- buffered INTEGER          (metrics tables only)

SELECT * FROM timeless_stats('metrics');   -- also logs/traces tables
-- key TEXT, value            (k/v rows: module, buffered_points|entries,
--                             chunks|blocks, bytes_on_disk, series|terms,
--                             ts_min, ts_max, flush_threshold, ...)
```

**Design notes.**

- `timeless_series` reads the in-memory registry + chunk index under
  `transition_read` — no chunk decompression. New engine method
  `series_overview() -> Vec<SeriesOverview>` (per series: fold ChunkMeta
  min/max/point_count from the index; buffered length from partitions).
- `timeless_stats` dispatches on module type. Detect by shadow tables
  (`_chunks` → metrics; `_trace_blocks` → traces; `_blocks` w/o
  `_trace_blocks` → logs), then resolve through the registry. Engines
  already expose `info()`/`stats()`/`storage_stats()` (R7 fixed the
  stats lock order — do not reorder).
- **The reusable piece:** generalize `MetricsTab::shared_engine_for`
  into per-module helpers (`logs_vtab::shared_engine_for`,
  `traces_vtab::shared_engine_for`) mirroring each xConnect tail. F4
  needs these; build them here.
- TVF plumbing pattern is already in query_tvf.rs (hidden-arg columns,
  bitmask best_index, materializing cursor) — extend that file.

**Implementation.**

- [ ] `Engine::series_overview()` under `transition_read` (+ unit test)
- [ ] stats k/v surface for all three engines (existing info/stats
      methods; add whatever is missing, keeping the R7 lock order)
- [ ] `logs_vtab::shared_engine_for` / `traces_vtab::shared_engine_for`
- [ ] module-type detection by shadow-table probe (one `sqlite_schema`
      query on the caller's connection)
- [ ] `timeless_series` + `timeless_stats` TVFs in query_tvf.rs
- [ ] refresh before reads (generation check makes this cheap)

**Verification.**

- [ ] cli.sh section: catalog matches inserted series (incl. buffered
      only, flushed-only, and post-prune states); stats keys sane for all
      three modules; unknown table errors cleanly
- [ ] core test: overview vs naive registry+index walk
- [ ] R4 interplay: catalog correct after DROP/recreate on same name

**Acceptance:** `timeless_series` on the 1M-point bench db returns 1000
rows in low single-digit ms with zero chunk reads. All suites green.

---

## F2 — Automated retention (table argument)

**Goal.** Kill the unbounded-growth footgun without violating passivity:
retention applied opportunistically during maintenance the engine already
performs, inside the caller's transaction.

**SQL surface.**

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics(retention='30d');
CREATE VIRTUAL TABLE logs    USING timeless_logs(index_keys='service', retention='7d');
CREATE VIRTUAL TABLE traces  USING timeless_traces(retention='72h');
```

`retention=` accepts `<n>[s|m|h|d]` (or bare integer = table ts units).
Suffixes convert via the module's documented unit (metrics s, logs ms,
traces ns). Stored in `_meta['retention']` at create, loaded at connect
(the index_keys pattern in logs_vtab).

**Design notes.**

- **Data-time, not wall-time** (invariant 5): cutoff =
  `max_ts_ever_ingested - retention`. Deterministic, replay-safe, works
  for backfills. Track the high-water mark in the engine (recovered as
  max ts_max over chunks at connect; advanced by writes).
- Application points: end of `flush` and `optimize`/`compact` — the
  engine is already mutating inside a transaction there; prune rides the
  same journal + chunk_gen machinery `prune:` already uses (delete path
  bumps chunk_gen — invariant 3 is already satisfied by reuse).
- Manual `prune:<ts>` stays; retention is a floor, not a replacement.
- Do NOT prune on every insert statement's auto-flush — cheap guard:
  skip unless cutoff advanced past the last applied cutoff by ≥ one
  retention/16 slice (avoids per-flush delete scans).

**Implementation.**

- [ ] duration parser + per-module unit conversion (one shared fn,
      unit-tested; rejects nonsense loudly)
- [ ] persist/load `_meta['retention']`; expose in `timeless_stats`
- [ ] engine: high-water-mark tracking + `apply_retention()` called from
      flush/optimize/compact tails, with the advance guard
- [ ] wire the table-arg parsing in all three vtabs' create paths
      (reject unknown args as today)

**Verification.**

- [ ] core test: retention prunes exactly `< cutoff` chunks/blocks;
      high-water mark recovery after reopen; backfill (old ts after new)
      does not re-prune the present
- [ ] cli.sh section: create with retention, ingest two epochs, flush →
      old epoch gone, new epoch intact, `_meta` shows the setting;
      rollback of a flush that pruned restores both (journal coverage —
      invariant 4)
- [ ] oracle run unaffected (it uses tables without retention)

**Acceptance:** a retention'd table under continuous ingest holds
steady-state size across 3+ retention windows (scripted check), with no
measurable ingest regression (interleaved A/B).

---

## F3 — Rollup ladder (downsampling)

**Goal.** "A year of metrics in SQLite": raw resolution ages out while
coarse aggregates survive, per a declared ladder. The `resolution` column
in `_chunks` was reserved for exactly this on day one.

**SQL surface.**

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics(
  retention='14d',                    -- raw tier
  rollups='300s@90d,3600s@2y');       -- resolution@retention, ascending

SELECT labels, ts, value
  FROM timeless_rollup('metrics', 'cpu_usage', NULL, 300, :t0, :t1, 'avg');
--                                              resolution^        agg: avg|sum|min|max|count|last
INSERT INTO metrics(metrics) VALUES ('rollup');   -- manual trigger (maintenance idiom)
```

**Design notes.**

- **Bucket definition (the whole semantic contract, document verbatim):**
  bucket B covers `[B, B + R)` with `B = floor(ts / R) * R` (i64
  Euclidean floor for negative ts). Per bucket per series store
  `(count u64, sum f64, min f64, max f64, last_ts i64, last_val f64)`,
  where sum folds left-to-right in ascending engine ts order and `last`
  is the max-ts sample (engine-order tiebreak). `avg = sum/count` at
  read. This is pre-aggregation: bit-parity with raw folds is
  IMPOSSIBLE for float sum across bucket boundaries and is not claimed —
  instead the property test pins rollup output bit-for-bit against naive
  bucket math over raw samples (invariant 6, amended).
- **Storage:** rollup chunk = new encoding id in the existing `_chunks`
  row shape: `resolution` column set to R, payload = columnar
  (bucket_ts[], count[], sum[], min[], max[], last_ts[], last_val[])
  behind the existing codec seam (delta+pco for ts columns, pco for the
  floats). `ts_min`/`ts_max` = first/last bucket start, `point_count` =
  bucket count. **Migration:** `scan_sql` must now SELECT `resolution`
  (it doesn't today) and `StoredChunk`/`ChunkMeta` must carry it; the
  in-memory index becomes per-resolution
  (`BTreeMap<(pk, resolution, min_ts, seq)>` or a map of maps — decide by
  read-path ergonomics). Old dbs: resolution defaults 0, nothing else
  changes — no data migration.
- **Production:** `'rollup'` runs inside maintenance (explicit command +
  automatically during `compact`/`optimize`): for each ladder tier,
  aggregate from the *finest still-available* source tier (raw preferred)
  for buckets whose source data is complete AND older than a settle
  margin (one bucket width) — avoids rewriting hot buckets. Idempotent:
  re-rolling a bucket replaces its row (replace_chunks machinery — atomic
  swap inside the host txn, crash-safe for free).
- **Retention interplay:** tier k's retention prunes tier k chunks only;
  raw retention must not outrun rollup production — enforce at create:
  raw retention ≥ 2× coarsest settle margin, error otherwise.
- **Read path:** `timeless_rollup` reads ONE explicit tier (plus
  merge-on-read of buckets from raw for the not-yet-rolled tail — flag it
  v1.1 if hairy; v1 may serve rolled buckets only and document the
  settle lag). NO implicit tier substitution in Q1/Q2 surfaces — explicit
  is honest; auto-selection can come later once trusted.
- **Multi-process:** rollup writes are chunk mutations → chunk_gen
  covers refresh (invariant 3). Journal: rollup inside a txn journals
  added/removed index entries exactly like compact (invariant 4).

**Implementation.**

- [ ] schema/index migration: resolution through scan/StoredChunk/
      ChunkMeta/index keys; recovery + refresh; dup-seq safety preserved
- [ ] rollup payload encoding (new encoding id) behind the codec seam
- [ ] bucket aggregation kernel (pure fn over sorted samples → buckets;
      property-test first, alone)
- [ ] `rollups=` arg parsing/persistence; ladder validation
- [ ] 'rollup' command + compact/optimize integration (settle margin,
      idempotent replace)
- [ ] per-tier retention wiring into F2's apply_retention
- [ ] `timeless_rollup` TVF
- [ ] `timeless_stats`/`timeless_series` awareness (per-tier chunk/byte
      counts)

**Verification.**

- [ ] property test: buckets bit-exact vs naive bucket math (dup ts,
      buffered+flushed, negative ts, bucket-boundary samples)
- [ ] lifecycle test: ingest 3 raw windows → rollup → raw pruned →
      rollup queries still answer; reopen + second process agree
- [ ] rollback of a txn containing 'rollup' restores index + rows
      (cli.sh 6-family addition)
- [ ] crash round with rollup in the loop (tests/crash.sh variant)
- [ ] oracle: teach the generator the 'rollup' command with a mirrored
      naive bucket table, or explicitly document exclusion
- [ ] bench: storage at ladder steady-state; rollup query vs raw-scan
      equivalent

**Acceptance:** documented ladder demo — e.g. 30 days of 10s-scrape data
at raw+5m+1h with raw@14d — file size, query timings at each tier, all
suites green. This is a multi-session feature; land the index migration
+ bucket kernel first, alone, before anything writes rollup chunks.

---

## F4 — Q2 kernels for logs & traces

**Goal.** The dominant dashboard shapes evaluated engine-side:
log volume by time bucket (by level / an index key), trace
duration/error stats per service per bucket. Same waist rules, same
verification pattern as timeless_grid/timeless_window.

**SQL surface.**

```sql
SELECT bucket_ts, group_key, n FROM timeless_log_buckets(
  'logs',          -- table
  'level',         -- group_by: 'level' or any declared index key
  '{"service":"api"}',  -- filter: JSON of index-key equalities (NULL = none)
  :t0, :t1, 60000);     -- start, stop, step (ms)

SELECT bucket_ts, service, spans, errors, dur_sum, dur_min, dur_max
  FROM timeless_trace_buckets('traces', NULL, :t0, :t1, 60000000000);
--                                     ^service filter    ^step (ns)
```

Buckets are `[t, t+step)` aligned to `start` (closed-open — matches how
log dashboards bin; DOCUMENT the difference from the metrics kernels'
(t-w, t] windows: grids sample backward, histograms bin forward).
Empty buckets produce no row. No percentiles — quantile sketches are
approximation policy, above the waist (sum/min/max/count enable avg and
alerting math client-side).

**Design notes.**

- Engine kernels on BlockEngine/SpanBlockEngine: iterate blocks
  overlapping the range, pruned by `_terms` posting lists for
  level/index-key/service filters (machinery exists — reuse the LogQuery
  candidate path), count/fold into buckets. Buffered entries merge like
  every read path.
- Rayon rule (invariant 2): kernels sequential; there are no per-series
  partitions here so a single sequential pass per block is natural.
- group_by validity: must be 'level' or a declared index key — error
  otherwise (names the valid set, like the agg error).
- TVF plumbing + engine resolution: F1's helpers.

**Implementation.**

- [ ] BlockEngine::bucket_counts(filter, group_by, start, stop, step)
- [ ] SpanBlockEngine::bucket_stats(service, start, stop, step)
- [ ] naive-reference property tests (core), incl. entries exactly on
      bucket edges and step larger than range
- [ ] timeless_log_buckets / timeless_trace_buckets TVFs
- [ ] cli.sh section vs SQL GROUP BY reference over the raw vtabs
- [ ] bench-logs/bench-traces: bucket query vs raw-scan + client bin

**Acceptance:** bucket queries beat the raw-scan equivalent by ≥5x on
the 1M-entry bench datasets with verified-equal results; suites green.

---

## F5 — Batch blob ingest for logs & traces

**Goal.** Close the ingest gap with metrics (23.8M pts/s vs 1.1M/0.8M
row-at-a-time): columnar batch blobs through each table's hidden command
column. OTLP protobuf is explicitly OUT of v0 (format churn + dependency
weight); the reserved version-byte space keeps the door open.

**Format v0 (logs)** — same philosophy as the metrics blob: dumb,
little-endian, versioned from day one; whole batch validated before ANY
entry is buffered (all-or-nothing, like metrics step 4).

```
0     u8   version = 0x01
1     u8   flags = 0
2     u16  reserved
4     u32  n_entries
8     —    ts[]      n × i64 LE (ms)
—     —    level[]   n × u8 (0..3 — the strict vocabulary, validated)
—     —    message[] n × { u32 len, utf8 }
—     —    metadata[] n × { u32 len, utf8 flat-JSON ('' = {}) }
```

**Format v0 (traces):** header + columnar `trace_id[16]`, `span_id[8]`,
`parent_id[8]` (all-zero = NULL), `name`/`service` as u32-len strings
(v0 keeps it dumb — dictionaries are a v1 measurement question),
`kind[] u8`, `status[] u8` (validated vocab), `start_ts[] i64`,
`duration[] i64`, `attributes[]` len-prefixed flat-JSON.

**Design notes.**

- Dispatch: logs/traces hidden columns currently accept TEXT commands
  only; add BLOB → first byte 0x01 = batch v0, 0x00/0x02–0x08 reserved
  and rejected loudly (the metrics lesson, per-table namespace).
- Engine write: bulk append into block-engine buffers with the clock
  read hoisted per statement (the write_point_at lesson) and auto-flush
  checks amortized per batch, not per entry.
- Same durability contract as rows: buffered until 'flush'.
- Statement-journal hazard check (invariant 1): the ingest itself does
  no store SQL, but auto-flush mid-batch does — confirm no engine lock
  is held across it (mirror how row-path auto-flush behaves today).

**Implementation.**

- [ ] logs blob parser + validator + bulk buffer append (+ bench encoder
      in tools/bench)
- [ ] traces blob parser + validator + bulk append (+ bench encoder)
- [ ] vtab dispatch (TEXT command | BLOB batch) for both modules
- [ ] malformed-blob rejection tests: truncation in every section, bad
      level/kind/status byte, wrong lengths — atomically rejected
- [ ] cli.sh section: python-built tiny blobs round-trip (the section 7
      pattern), rollback of a batch, auto-flush crossing mid-batch
- [ ] bench-logs/bench-traces: Tier 2 rows alongside Tier 1

**Acceptance:** ≥5x row-path ingest for logs and traces on bench data
(target ≥5M entries/s logs), identical query results vs the same data
via Tier 1, suites green.

---

## F6 — Trigram index for log message search

**Goal.** Erase the one benchmark defeat: `message LIKE '%timeout%'` at
357ms vs plain 75ms, because every block decompresses. A trigram posting
index makes substring search a block-pruning problem.

**Why trigrams, not word tokens:** a word-token index cannot soundly
pre-filter substring LIKE ('%timeout%' must match "xtimeouty"). Trigram
containment CAN: every 3-byte window of a matched substring appears in
the message, so blocks missing any pattern trigram are safely skipped.
Recheck stays exact (SQLite re-evaluates LIKE; we never set omit).

**SQL surface.**

```sql
CREATE VIRTUAL TABLE logs USING timeless_logs(
  index_keys='service,path', message_index='trigram');
-- LIKE queries are unchanged — the index is a transparent accelerator:
SELECT * FROM logs WHERE message LIKE '%timeout%' AND ts > :t0;
```

**Design notes.**

- Opt-in via table arg (index costs space — measure and publish the
  overhead). Persisted in `_meta`; connect loads it; tables without it
  behave exactly as today.
- Encode-time: per block, the deduped set of lowercased byte trigrams of
  all messages, stored in `_terms` as `tg:<3 bytes>` rows (the existing
  posting machinery; term pruning already proven by read-count tests in
  blocks/).
- Query-time: extract literal runs from the LIKE pattern (split on
  `%`/`_`, honoring ESCAPE — any run <3 bytes contributes nothing);
  candidate blocks = intersection of postings for every pattern trigram;
  decompress candidates only; SQLite rechecks row-exactly. Case folding:
  index lowercases ASCII; pattern trigrams lowercase too — matches
  SQLite's default ASCII-case-insensitive LIKE. Document the NOCASE and
  non-ASCII caveats (non-ASCII trigrams are byte-trigrams; still sound,
  just conservative).
- best_index: accept SQLITE_INDEX_CONSTRAINT_LIKE on the message column
  (never omit); plan flag routes the pattern to filter.
- Buffered entries: always searched exactly (no index) — same merge
  contract as every read.
- Soundness test is the star: the index may only ever SKIP blocks that
  provably cannot match — property-test candidate sets against
  brute-force block scans over hostile messages (repeats, unicode,
  patterns straddling entry boundaries, `_`/`%`/ESCAPE forms).

**Implementation.**

- [ ] trigram extraction (encode side) + `tg:` postings in _terms
- [ ] `message_index=` arg parse/persist/load
- [ ] LIKE pattern → required-trigram set (ESCAPE-aware, unit-tested
      hard)
- [ ] best_index LIKE handling + pruned read path
- [ ] soundness property test (core) + cli.sh section (results identical
      with/without the index, EXPLAIN-level proof it pruned, plus a
      read-count proof in the blocks test style)
- [ ] bench-logs: LIKE timing + index size overhead with/without

**Acceptance:** `LIKE '%timeout%'` on the 1M-entry bench under 50ms with
byte-identical results to the unindexed path; published size overhead;
suites green.

---

## Session log

| Date | Item | State | Evidence / next step |
|---|---|---|---|
| 2026-07-26 | Plan | complete | This document; F1 next. |
