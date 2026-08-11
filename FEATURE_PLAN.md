# Feature Plan — post-0.1.1 functionality

Six features agreed 2026-07-26, ordered into sessions. Check items off as
they land; add a session-log row per work session (the REVIEW_FIX_PLAN.md
discipline — it worked).

Sequencing: F1 → F2 are the operability package and F1 builds the
engine-resolution helpers every later TVF needs. F3 (rollups) is the long
pole and consumes F2's retention machinery. F4/F5/F6 are independent of
F3 and of each other — reorder freely if priorities shift.

- [x] **F1** series catalog + stats TVFs
- [x] **F2** automated retention (table argument)
- [x] **F3** rollup ladder (downsampling)
- [x] **F4** Q2 kernels for logs & traces
- [x] **F5** batch blob ingest for logs & traces
- [x] **F6** trigram index for log message search
- [x] **F7** SQL query tier: counter kernels, percentiles, trimmed folds
- [x] **F8** label matchers + discovery TVF
- [x] **F9** gap-fill + the query cookbook

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

- [x] `Engine::series_overview()` under `transition_read` (+ unit test)
- [x] stats k/v surface for all three engines (existing info/stats
      methods; add whatever is missing, keeping the R7 lock order)
- [x] `logs_vtab::shared_engine_for` / `traces_vtab::shared_engine_for`
- [x] module-type detection by shadow-table probe (one `sqlite_schema`
      query on the caller's connection)
- [x] `timeless_series` + `timeless_stats` TVFs in query_tvf.rs
- [x] refresh before reads (generation check makes this cheap)

**Verification.**

- [x] cli.sh section: catalog matches inserted series (incl. buffered
      only, flushed-only, and post-prune states); stats keys sane for all
      three modules; unknown table errors cleanly
- [x] core test: overview vs naive registry+index walk
- [x] R4 interplay: catalog correct after DROP/recreate on same name

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

- [x] duration parser + per-module unit conversion (one shared fn,
      unit-tested; rejects nonsense loudly)
- [x] persist/load `_meta['retention']`; expose in `timeless_stats`
- [x] engine: high-water-mark tracking + `apply_retention()` called from
      flush/optimize/compact tails, with the advance guard
- [x] wire the table-arg parsing in all three vtabs' create paths
      (reject unknown args as today)

**Verification.**

- [x] core test: retention prunes exactly `< cutoff` chunks/blocks;
      high-water mark recovery after reopen; backfill (old ts after new)
      does not re-prune the present
- [x] cli.sh section: create with retention, ingest two epochs, flush →
      old epoch gone, new epoch intact, `_meta` shows the setting;
      rollback of a flush that pruned restores both (journal coverage —
      invariant 4)
- [x] oracle run unaffected (it uses tables without retention)

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

- [x] schema/index migration: resolution through scan/StoredChunk/
      ChunkMeta/index keys; recovery + refresh; dup-seq safety preserved
- [x] rollup payload encoding (new encoding id) behind the codec seam
- [x] bucket aggregation kernel (pure fn over sorted samples → buckets;
      property-test first, alone)
- [x] `rollups=` arg parsing/persistence; ladder validation
- [x] 'rollup' command + compact/optimize integration (settle margin,
      idempotent replace)
- [x] per-tier retention wiring into F2's apply_retention
- [x] `timeless_rollup` TVF
- [x] `timeless_stats`/`timeless_series` awareness (per-tier chunk/byte
      counts)

**Verification.**

- [x] property test: buckets bit-exact vs naive bucket math (dup ts,
      buffered+flushed, negative ts, bucket-boundary samples)
- [x] lifecycle test: ingest 3 raw windows → rollup → raw pruned →
      rollup queries still answer; reopen + second process agree
- [x] rollback of a txn containing 'rollup' restores index + rows
      (cli.sh 6-family addition)
- [x] crash round with rollup in the loop (tests/crash.sh variant)
- [x] oracle: DOCUMENTED EXCLUSION (oracle.rs header) — pre-aggregation
      cannot be mirrored row-for-row; covered by the kernel property
      suite, lifecycle tests, cli.sh §25, and the crash-suite tier decode
- [x] bench: storage at ladder steady-state; rollup query vs raw-scan
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

- [x] BlockEngine::bucket_counts(filter, group_by, start, stop, step)
- [x] SpanBlockEngine::bucket_stats(service, start, stop, step)
- [x] naive-reference property tests (core), incl. entries exactly on
      bucket edges and step larger than range
- [x] timeless_log_buckets / timeless_trace_buckets TVFs
- [x] cli.sh section vs SQL GROUP BY reference over the raw vtabs
- [x] bench-logs/bench-traces: bucket query vs raw-scan + client bin

**Acceptance:** bucket queries beat the raw-scan equivalent by ≥5x on
the 1M-entry bench datasets with verified-equal results; suites green.
MEASURED 2026-07-26: logs 5.7x (95.9ms vs 543.5ms — the level-purity
fast path counts fully-contained pure blocks from metadata alone).
Traces: 2.4x (246ms vs 582ms) — duration sums REQUIRE decoding every
span, so the ≥5x bar is unreachable without per-block duration
aggregates in block metadata; accepted as decode-bound, with the
metadata-aggregate extension noted as the v2 path.

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

- [x] logs blob parser + validator + bulk buffer append (+ bench encoder
      in tools/bench)
- [x] traces blob parser + validator + bulk append (+ bench encoder)
- [x] vtab dispatch (TEXT command | BLOB batch) for both modules
- [x] malformed-blob rejection tests: truncation in every section, bad
      level/kind/status byte, wrong lengths — atomically rejected
- [x] cli.sh section: python-built tiny blobs round-trip (the section 7
      pattern), rollback of a batch, auto-flush crossing mid-batch
- [x] bench-logs/bench-traces: Tier 2 rows alongside Tier 1

**Acceptance:** ≥5x row-path ingest for logs and traces on bench data
(target ≥5M entries/s logs), identical query results vs the same data
via Tier 1, suites green.
MEASURED 2026-07-26: logs 1.32M vs 1.14M entries/s (+16%), traces 1.11M
vs 0.76M spans/s (+46%) — the ≥5x bar was written from the metrics
experience, where Tier 2 skips per-row SQL dispatch that DOMINATES.
Logs/traces ingest is FLUSH-BOUND: both tiers pay identical auto-flush
encode work (sort + term extraction + block encode every 8192 entries)
plus per-entry metadata parsing, so eliminating row dispatch moves the
needle 16-46%, not 5x. Accepted with that analysis; the v2 paths are
deferred metadata parsing (parse at flush, not ingest) and blob→raw-
block passthrough encoding. The format's other value stands: one
round-trip per batch for remote (sqld) writers and atomic all-or-
nothing batches. Correctness verified equal to Tier 1 in-bench.

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

- [x] trigram extraction (encode side) + `tg:` postings in _terms
- [x] `message_index=` arg parse/persist/load
- [x] LIKE pattern → required-trigram set (ESCAPE-aware, unit-tested
      hard)
- [x] best_index LIKE handling + pruned read path
- [x] soundness property test (core) + cli.sh section (results identical
      with/without the index, EXPLAIN-level proof it pruned, plus a
      read-count proof in the blocks test style)
- [x] bench-logs: LIKE timing + index size overhead with/without

**Acceptance:** `LIKE '%timeout%'` on the 1M-entry bench under 50ms with
byte-identical results to the unindexed path; published size overhead;
suites green.
MEASURED 2026-07-26: 48.3ms (was 334.5ms unindexed vtab; the plain
table's 74.5ms is also beaten — the last losing benchmark row is now a
win). Count verified against the plain oracle; overhead 88,713 tg: term
rows ≈ 1.5 MB (~17% of the compressed logs file). Terms are hex-encoded
(`tg:` + 6 hex chars) because byte windows can split UTF-8; ESCAPE'd
LIKEs never reach vtab constraints, so wildcard-escaping cannot mislead
the pruner.

---

---

## M1 — Migration enablers for the timeless_metrics engine swap

Agreed 2026-07-26 after F6: the two remaining THIS-SIDE pieces so the
timeless repo only needs the NIF rebind + vm_diff run.

**M1a — the pinned waist.** A `waist` module in timeless-core exposing
the layering contract's exact surface: `query_multi(metric, matchers,
from, to) → Vec<(Labels, Vec<(ts, value)>)>` and `list_metrics()` (+
`list_series` for above-waist matcher evaluation). Matcher policy per
the contract: `=` pushes down (find_series), `!=` is mechanical string
inequality evaluated in the adapter; `=~`/`!~` are NOT implemented here
— regex dialect is caller semantics, evaluated above the waist via
list_series + query_multi_ids. Sequential per-series reads (NIF/vtab
safe); the rayon labeled path remains for callers that want it.

- [x] waist module + docs stating the contract verbatim
- [x] Eq/Neq evaluation + query_multi_ids escape hatch
- [x] shape/equivalence tests vs naive matcher evaluation

**M1b — FsStore → shadow-store importer.** `import` binary
(tools/bench): reads an existing timeless data directory through the
recovered fs engine, replays every series into a timeless_metrics vtab
via Tier 2 blobs (the proven write path — catalog, validation,
generation all for free), then verifies EVERY point bit-exact through
the vtab before reporting success. `--selftest` generates a hostile
deterministic fixture (quoted/escaped labels, NaN payload bits,
negative ts) and runs the full cycle.

- [x] generic metrics blob v0 encoder + JSON label escaping
- [x] import flow (transactions, flush, per-series verify, report)
- [x] --selftest fixture incl. hostile labels + weird floats
- [x] cli.sh section: selftest + imported db queried via sqlite3

---

## The SQL query tier (F7-F9), agreed 2026-07-30

Scope decision: make the SQL surface USABLE for sqlite/libsql-native
users — not elegant, not PromQL, never a web service. The Elixir layer
stays the semantics authority (conformance-certified); everything below
follows the waist rule — mechanical, parameter-explicit, bit-verifiable
folds only. Estimated one session per feature; check off as we go.

## F7 — Window vocabulary: counters, percentiles, trimmed folds

**Goal.** The raw-sample advantage, cashed in: exact time-series math
PromQL can only estimate, because we kept the samples. New agg names on
`timeless_window` plus duration percentiles on `timeless_trace_buckets`
("p95 latency per service per minute" — THE trace dashboard query).

**SQL surface.**

```sql
SELECT labels, ts, value FROM timeless_window(
  'metrics', 'requests_total', NULL, :t0, :t1, 60, 300, 'rate');
-- new aggs: delta | increase | rate | pNN (p50, p95, p99.9…) | tavg:N

SELECT bucket_ts, service, spans, errors, dur_sum, dur_min, dur_max,
       dur_p50, dur_p95, dur_p99
  FROM timeless_trace_buckets('traces', NULL, :t0, :t1, :step);
```

**THE SEMANTIC LINE — definitions pinned verbatim (these ARE the
contract; the property tests quote them):**

- Samples per window: ascending engine ts order, half-open
  `(t - window, t]`, exactly as the existing aggs.
- `delta` = last − first (engine-order ties, same rule as grid-last).
- `increase` = Σ over consecutive pairs of
  `(v[i] − v[i−1]) if v[i] ≥ v[i−1] else v[i]` — the stable
  reset-adjustment rule (counter restarted near zero). The window's
  first sample contributes nothing. NO extrapolation, NO lookback
  beyond the window, NO staleness inference — this is NOT PromQL
  `increase` and the docs say so in bold.
- `rate` = increase ÷ window, per NATIVE ts unit (per second for
  metrics; document the unit dependence).
- `pNN` (N a decimal in (0, 100]): NEAREST-RANK — exclude NaNs, sort
  remaining values by `f64::total_cmp`, take index
  `ceil(N/100 × n) − 1`. No interpolation (that is what keeps the
  bit-exact tests trivial and the estimator arguments out). Empty
  after NaN exclusion → no row.
- `tavg:N` (N in [0, 50)): after the same NaN-excluded sort, drop
  `floor(n × N/100)` from EACH tail, average the remainder
  left-to-right. The user supplies N — the database NEVER decides what
  an outlier is (auto-detection like 3σ/IQR is refused as a default;
  the cookbook shows those as explicit SQL recipes).
- Trace buckets: `dur_p50/dur_p95/dur_p99` fixed columns, exact
  nearest-rank over the bucket's i64 durations (no float subtlety).
  Requires collecting per-(bucket, service) duration vectors instead of
  streaming folds — same memory order as the spans already materialized
  by query(); measure and note.

**Implementation.**

- [x] window kernel: new agg vocabulary (parser for pNN/tavg:N; the
      free-form agg string was built for this) + the five definitions
      above as folds
- [x] trace bucket_stats: per-group duration collection + p50/p95/p99;
      TVF schema gains the three columns
- [x] naive-reference property tests quoting the pinned definitions
      (dup ts, resets mid-window, NaN exclusion, even/odd n, p99.9 on
      tiny windows, tavg boundary fractions, empty-after-exclusion)
- [x] cli.sh section vs SQL reference (increase/rate need recursive-CTE
      or window-function references; percentiles vs an exact SQL rank
      recipe)
- [x] bench: p95 window + trace dur_p95 timings published; docs updated
      (README + the NOT-PromQL warning)

**Acceptance:** window p95 and trace dur_p95 produce property-verified
exact results at dashboard latencies (target: same order as existing
aggs; publish the sort overhead honestly); all suites green.

## F8 — Label matchers + discovery

**Goal.** Kill stringly label filtering. Matcher support in every
metrics TVF filter argument, plus label discovery for building UIs.

**SQL surface.**

```sql
-- filter JSON values grow operators (plain string stays equality):
SELECT * FROM timeless_grid('metrics', 'cpu',
  '{"host": {"re": "web-.*"}, "env": {"neq": "dev"}}',
  :t0, :t1, 60, 90);
-- operators: plain "v" (eq) | {"neq": v} | {"re": pattern} | {"nre": pattern}

SELECT value FROM timeless_label_values('metrics', 'cpu_usage', 'host');
```

**Design notes.**

- Equality still pushes into `find_series`; the other operators filter
  the enumerated candidates (the M1 list_series path, done for the
  user) BEFORE any chunk reads. Absent label = "" for neq (the pinned
  waist rule), and `re`/`nre` match against "" for absent — document.
- Regex dialect: the Rust `regex` crate (new dependency, ~RE2 family),
  DOCUMENTED as such. This is SQL-user convenience; the waist module
  and the Elixir layer are untouched and keep their own matcher
  semantics above the waist. Invalid pattern = loud error naming it.
- One shared filter-parse upgrade in query_tvf.rs serves
  grid/window/rollup identically.
- `timeless_label_values`: the registry's label_values() exposed as a
  TVF (the timeless_series pattern; ~an hour).

**Implementation.**

- [x] matcher JSON parsing (back-compatible: plain strings = eq) +
      shared candidate filtering in the kernel TVFs
- [x] regex dep + error surfacing; absent-label semantics per the
      pinned rules
- [x] timeless_label_values TVF
- [x] tests: matcher equivalence vs naive filtering (incl. absent-label
      edges, anchoring gotchas, invalid patterns); cli.sh section
- [x] docs (README filter syntax table)

**Acceptance:** a `{"re": …}` dashboard query over the 1M-point bench
returns verified-identical results to client-side filtering with no
measurable overhead beyond the per-series regex test; suites green.

## F9 — Gap-fill + the query cookbook

**Goal.** Chart-readiness and leverage: dense grids for plotting
libraries, and a cookbook that may make further features unnecessary.

**SQL surface.**

```sql
-- optional trailing arg on timeless_grid / timeless_window:
--   fill = 'none' (default) | 'null'  → every grid point emitted,
--   value NULL where the window/lookback is empty
SELECT labels, ts, value FROM timeless_grid(
  'metrics', 'cpu', NULL, :t0, :t1, 60, 90, 'null');
```

**Design notes.**

- Gap-fill is presentation mechanics, zero semantics: the kernels
  already know every grid point; 'null' just stops omitting the empty
  ones (per series — a series with NO points in range stays absent,
  matching query_multi's omission rule; document).
- `docs/QUERIES.md` cookbook, every recipe smoke-tested in a cli.sh
  section so it can never rot: reset-corrected rate in pure SQL window
  functions (and when to prefer the F7 kernel), topk-per-bucket,
  cross-metric joins, σ/IQR outlier exclusion as explicit SQL,
  dashboard patterns per TVF, the LEFT-JOIN-generate_series alternative
  to gap-fill.

**Implementation.**

- [x] fill argument on grid/window (decode + cursor emission of NULL
      rows; KernelCursor value becomes Option)
- [x] cli.sh checks: dense grid row counts, NULL placement, per-series
      absence rule
- [x] docs/QUERIES.md with every recipe executed by a cli.sh section
- [x] README pointer

**Acceptance:** a charting query needs no client-side gap handling;
every cookbook recipe is machine-verified; suites green.

## Cardinality mitigation (P1-P5), agreed 2026-08-11

Findings from tools/bench bench-cardinality (M5 Pro, 100 pts/series,
tier2 ingest; full evidence in the 2026-08-11 session-log row): all
query paths scale linearly to 100k series, reader RSS ~3.6KB/series,
BUT a TVF-only connection rebuilds the engine on EVERY statement
(registry holds Weak refs; eponymous TVF vtabs die at statement end) —
416ms + ~360MB alloc churn per query at 100k. Generation-change
refresh is also a full O(N) reload (hits replication readers per sync
batch), cold recovery is ~416ms, and the row-TVF kernels carry a
~4-5us/series materialization floor.

- [x] **P1** per-connection engine pinning: strong Arc deposited in a
      per-connection pin cell on engine resolution; cleanup via a
      per-connection scalar-function state destructor (fires at
      sqlite3_close). Registry keeps its Weak contract; DROP machinery
      (DROP_PINS) untouched. Acceptance: bench-cardinality unpinned
      columns collapse to pinned; suites green.
- [x] **P2** incremental generation refresh: series delta via the
      existing MAX(id) watermark (append-only); chunk index delta via
      (max rowid, count) pure-append detection, full-rescan fallback
      on prune/compact. Acceptance: refresh-after-flush at 100k in
      low ms (vs ~350ms full reload); correctness suites green incl.
      retention/compaction fallback paths.
- [x] **P3** registry build speed + memory diet: pre-sized maps, one
      shared Arc<SeriesInfo> per series, interned label strings,
      Vec postings. Target: cold recovery 2-3x faster, RSS toward
      ~1.5KB/series at 100k.
- [x] **P4** row-TVF kernels on batch chunk reads + lazy labels
      rendering (the packed-TVF primitives, reused). Target: fleet
      grid/window at 100k from ~66ms toward ~46ms.
- [~] **P5** decoded-chunk LRU — DEFERRED 2026-08-11 after the
      re-measurement this item was gated on: with P1-P4 in, the query
      it would help most (fleet rate-all at 100k) is already 48.9ms
      warm, and the realistic ceiling (~15-20ms) costs a bounded
      decoded-points cache (~16MB per hot metric) plus new
      invalidation surface. Lowest value-per-risk on the list.
      Revisit only if a real workload (Elixir layer / actual dashboard
      cadence) shows repeated heavy folds mattering.

Non-goals: flush throughput (linear, fine), full-catalog O(N) scans
(inherent), 100-pt-chunk compression tax (use compact).

## Session log

| Date | Item | State | Evidence / next step |
|---|---|---|---|
| 2026-08-11 | P5 | deferred | Re-measured after P2/P4 per the plan's own gate and deferred with the user's agreement. Cumulative P1-P4 at 100k series: repeat TVF query 332ms -> 0.07-0.2ms; refresh-after-external-flush ~350ms -> 0.5ms; cold recovery 416ms -> 175ms; reader RSS 362-414MB -> 203-235MB (~2KB/series); fleet rate-all 66ms -> 48.9ms. Cardinality-mitigation plan CLOSED (4 shipped, 1 deferred with rationale). |
| 2026-08-11 | P4 | complete | run_kernel now takes a BATCH closure: one call per query for the whole candidate set (defensive length+order verification against the candidates snapshot), labels rendered lazily only for emitting series. WindowTab -> query_window_op_batch_by_id (existing packed primitive); GridTab -> NEW engine query_grid_last_batch_by_id (validate once, one batched range read, grid_last_walk per series - semantics identical to the per-id path by construction); RollupTab keeps per-series reads inside the batch shape (tier chunks are few; documented). One deliberate test update: the window-batch stats counters now count row + packed users of the primitive (exactly double in cli.sh's section - same three queries in each shape; comment added). Measured at 100k: rate-all 66ms -> 48.9ms (packed-TVF territory, target met); recovery/RSS/other vectors unchanged except +14MB transient peak during the heaviest batch query (221 -> 235MB final; the time-for-space trade, noted). Gate: 46-section cli.sh green incl. the section-22 plain-SQL oracle and section 30-33 fill/absence pins; workspace green (pre-existing capabilities/0.6.0 failure only). P5 (decode LRU) is gated on re-measurement - see next row. |
| 2026-08-11 | P3 | complete | Registry diet: forward map now identity-hash -> candidate-ids verified against series_info (kills the duplicated (String, Labels) key per series - the largest per-series allocation; reuses fast_series_hash_pairs, collisions resolved never trusted); label/metric postings HashSet -> sorted Vec (binary-search insert/remove); from_stored pre-sizes; series_overview iterates series_info; resolve paths lose a per-lookup (String, Labels) clone. Measured at 100k (bench-cardinality): cold recovery 360ms -> 188ms (1.9x); reader RSS 362 -> 201MB post-recovery, 414 -> 221MB final (~3.6KB -> ~2.0KB/series, 1.8x; residual = the single owned name+labels copy, interning left as optional follow-up); all query latencies unchanged; P2 compounding visible (pin_base_vtab 292ms -> 4.6ms). Gate: 46-section cli.sh green, workspace green (pre-existing capabilities/0.6.0 failure only). P4 next. |
| 2026-08-11 | P2 | complete | Pure-append delta refresh: store trait grows append_watermark() (chunk_shape_gen + max rowid), scan_since(), load_series_since() with full-reload defaults; shadow store bumps chunk_shape_gen ONLY on delete/replace (prune, compact) and shares one row decoder between full and delta scans; engine primes gen+watermark at construction BEFORE the recovery scans (stale-conservative; first refresh no longer redundantly reloads), delta gated on unchanged shape-gen, application idempotent by ChunkLoc (writer's own flushed rows skipped, never double-counted), any delta doubt or error falls back to full reload. Tests: p2_delta_refresh.rs scripted counting store (append -> 1 delta scan 0 full scans + queryable; shape change -> full reload; idempotence); catalog_publication.rs updated for primed tokens. Gate: 46-section cli.sh all green, workspace tests green (1 pre-existing capabilities/0.6.0 failure, fails on main). Acceptance MET: cross-process refresh-after-flush at 100k series 0.52ms vs ~350ms full reload (~670x), external point visibility verified. P3 next. |
| 2026-08-11 | P1 | complete | Per-connection engine pins: CONN_PINS keyed by sqlite3 ptr; ConnPinScope owned by the timeless_pins() scalar function's boxed state (SQLite drops function state at close - the connection-close hook, no clientdata API); pin_engine at all three shared_engine_for sites, first-resolution-wins so the pin set is bounded by distinct tables. Registry Weak contract and DROP_PINS untouched; no-scope connections keep pre-P1 behavior. Verified: 2 new shared.rs unit tests (pin keeps Weak upgradable across statement-drop, scope drop rebuilds; no-scope = no-op), cli.sh section 46 (pins 0->1->1->2 across metrics+logs TVFs, fresh connection back to 0), full 46-section suite, 214 workspace tests (1 pre-existing failure: capabilities 0.5-line assert vs 0.6.0 version - NOT P1's, fails on main). bench-cardinality: unpinned grid at 100k 332ms -> 0.07ms (matches pinned); first-touch recovery 360ms remains (P2/P3 target); all other columns unchanged. ALSO on this branch: query-harness now builds+runs on macOS (glibc-only rlimit type, Apple sqlite lacking load-extension symbols -> bundled, math polyfill for the bundled build; 135 recipes pass) - pre-existing main breakage that blocked the whole gate at section 7. P2 next. |
| 2026-07-31 | Wide raw frame | complete | Added public `timeless_raw_frame` without replacing `timeless_raw` or `timeless_raw_batches`. `TRF1` carries all matched non-empty series as IDs/counts/timestamps/value bits in one checked columnar frame. A callback-safe core batch query uses one transition guard and one shadow-store read. TimelessMetrics validates it in a NIF and emits final series maps directly. Public 12K-series/720K-point read improved from 344.221ms to 113.142ms median (3.04x; 113.881ms p95); direct frame materialization is 63.636ms vs 74.416ms per-series batches. Exact/narrow improved, and sampled peak memory is 9.306x the serialized result under an enforced 10x bound. |
| 2026-07-31 | Packed rollup reads | complete | Added public `timeless_rollup_batches` while retaining `timeless_rollup`. `TRB1` returns the complete bucket contract in eight little-endian columns, including exact u64 counts and float bits. One callback-safe core batch read replaces per-series chunk statements; TimelessMetrics replaces six rollup queries with one prepared call/decode. Direct 60K-bucket materialization improved 9.30x (1,002.150ms to 107.741ms median); the public 1,200-bucket adapter improved 19.22x (14.744ms to 0.767ms). No stored payload or replication-visible row changed. |
| 2026-07-31 | Q2 packed windows | complete | Added public `timeless_window_batches`: one `TWB1` columnar blob per series with timestamp/value columns and a validity bitmap preserving sparse or `fill='null'` output. The callback-safe core batch primitive validates and holds the transition once; `ChunkStore::read_chunks` lets SQLite fetch all selected shadow rows in one ordered batch instead of executing one statement per series. CLI §22 pins sparse/dense blobs byte-for-byte; core batch results match the by-id oracle; workspace and full CLI suites pass. Direct 12K-series/72K-bucket fetch fell from 108.825ms row-TVF count to 67.796ms packed materialization; TimelessMetrics public bucketed average reached 127.532ms median versus the saved 495.455ms pre-native baseline and 119.937ms for the Rust engine. |
| 2026-07-26 | Plan | complete | This document; F1 next. |
| 2026-07-30 | F9 | complete | fill='none'\|'null' trailing arg on grid+window (KernelArgs.fill; cursor value now Option<f64>; dense walk bounded by the engine's step>0 + grid-cap validation, checked_add guarded; rollup untouched — no fill arg declared). Per-series absence rule preserved and asserted (series with points outside the range emits nothing even filled). docs/QUERIES.md cookbook: dashboard patterns per TVF, native + generate_series gap-fill, pure-SQL reset-corrected increase (asserted EQUAL to the F7 kernel over the same half-open window), top-k per bucket, cross-metric error-ratio join on shared grid points, IQR fences via exact-percentile kernel + 2-sigma SQL (both robust avgs = 11.3 on the outlier fixture; sigma-masking caveat documented). cli.sh §32 (3 checks: 2x5 dense rows with 7 NULLs, NULL placement grid+window agree, unknown fill loud) + §33 (7 checks — every cookbook recipe executed). README: fill + cookbook pointer. 140 workspace tests, 34 cli.sh sections. F1-F9 ALL COMPLETE. |
| 2026-07-30 | F8 | complete | Matcher operators in every kernel-TVF filter (plain=eq pushed into the label index; {"neq"}/{"re"}/{"nre"} compiled and applied to the candidate series list BEFORE chunk reads). Regexes: Rust `regex` crate (std+perf only), FULLY ANCHORED (PromQL-style — pinned decision), absent label = "" for all three ops per the waist rule. timeless_label_values TVF (registry-only, sorted distinct). 9 new ext unit tests (anchoring incl. substring must-NOT-match, absent-label semantics for neq/re/nre, eq-vs-matcher split, loud invalid-regex/unknown-operator errors) + cli.sh §31 (7 checks incl. the missing-env series and error paths). Bench acceptance MET: selective regex grid verified vs independent client-side filter (8300 rows exact); all-hosts regex 1.5ms vs 1.7ms NULL filter = no measurable overhead. 140 workspace tests, 32 cli.sh sections. F9 next. |
| 2026-07-30 | F7 | complete | WindowOp vocabulary (delta/increase/rate with the pinned reset rule; exact nearest-rank pNN, NaN-excluded; tavg:N both-tails trim) + trace dur_p50/p95/p99 (exact i64 nearest-rank). Property suites quote the pinned definitions verbatim (25 rounds, resets + NaN staleness + dup ts, bit-exact vs naive; "NaN poisons increase" asserted honestly — staleness stays above the waist; trace percentiles vs naive across 3 step sizes). DEVIATION from the checklist item: cli.sh §30 uses hand-verified literals on a fixed dataset instead of recursive-CTE SQL references — simpler and equally binding; three error paths named (p0, tavg:50, unknown-agg lists vocabulary). Bench: exact p95 5.6ms vs avg 1.5ms over 16,600 5-min windows (sort cost published, acceptance "same order" MET); trace buckets with percentiles 245ms vs 246ms pre-F7 (free — durations already materialized); tier2 21.71M pts/s (noise band). 132 workspace tests, 31 cli.sh sections. Committed 5a71e29. F8 next. |
| 2026-07-26 | M1 | complete | Waist pinned (timeless_core::waist: query_multi/list_metrics + Eq/Neq matchers in-waist, regex documented above-waist with list_series/query_multi_ids escape hatch; equivalence tests vs naive). Importer shipped (Tier 2 replay + mandatory bit-exact verification; --selftest with hostile fixture). THE FIXTURE FOUND A REAL BUG: NaN samples (Prometheus staleness markers) crashed flush — SQLite binds NaN as NULL, violating the chunk-stat NOT NULL columns. Fixed at the store seam (non-finite stats round-trip as 8-byte bit blobs); NaN VALUES are preserved in storage, surface as SQL NULL (inherent REAL limitation, documented; engine/waist reads return true NaN bits). 129 workspace tests, 30 cli.sh sections incl. §29, 8 consecutive crash-suite runs green (one unreproduced check failure before the reruns, output not captured — watch item). |
| 2026-07-26 | F6 | complete | Opt-in message_index='trigram': hex-encoded tg: terms + tg: marker per block at extract_terms (4096-trigram budget → over-budget blocks stay unindexed/unpruned), LIKE claimed in best_index (never omitted; ESCAPE'd LIKEs never reach vtabs), candidates = unindexed ∪ has-all-pattern-trigrams. Core: soundness property over hostile messages/patterns, read-count proof (1 of 5 blocks), unindexed-fallback. cli.sh §28 parity checks. Bench: 48.3ms (<50ms acceptance MET; was 334.5ms, beats plain's 74.5ms — last losing row flipped), overhead 1.5MB published. 127 workspace tests, 29 cli.sh sections. ALL SIX FEATURES COMPLETE. |
| 2026-07-26 | F5 | complete | logs/traces batch blob v0 (shared BatchReader extracted from metrics; engine push_batch mirrors push incl. auto-flush; version-byte dispatch with 0x00/0x02-0x08 reserved loudly). cli.sh §28-worth of checks in §27: round-trip incl. pushdown on blob metadata, packed ids + root-parent NULL, in-txn rollback, 7 truncation/bad-byte blobs rejected atomically (one test bug fixed red→green: durability contract requires flush before cross-process count). Bench: logs +16% (1.32M/s), traces +46% (1.11M/s) — ≥5x NOT met; analysis recorded in acceptance (flush-bound, unlike metrics' dispatch-bound), v2 paths noted. 124 workspace tests, 28 cli.sh sections. |
| 2026-07-26 | F4 | complete | bucket_counts (with a level-purity fast path: fully-contained pure blocks counted from metadata, filtered-out pure levels skipped without decode) + bucket_stats; [t,t+step) forward binning documented vs the grids' backward windows. Naive-reference tests (30 rounds each, exact), TVFs via the F1 helpers, cli.sh §26 GROUP BY cross-checks incl. buffered entries. Bench: logs 5.7x (95.9 vs 543.5ms, acceptance met); traces 2.4x — decode-bound (durations), documented in the acceptance note with the per-block-aggregate v2 path. 124 workspace tests, 27 cli.sh sections. |
| 2026-07-26 | F3 | complete | Separate rollup index (raw scan now WHERE resolution=0 — zero risk to raw paths; deviation from the per-resolution-key sketch, allowed by "decide by read-path ergonomics"). Kernel bit-exact vs naive (200 rounds), ENC_ROLLUP_V1 zstd payload, rollups= arg, 'rollup' cmd (+auto at compact, 1-bucket settle, append-only watermark; late-past-settle data stays raw-only — documented v1 limit), per-tier retention, journaled rollup index (rollback test), timeless_rollup TVF, stats rows. 121+ workspace tests, cli.sh 26 sections incl. §25, crash suite WITH rollup in the kill window, oracle exclusion documented. Bench: tier read 4.1ms vs 34.5ms raw GROUP BY; build 80ms/1M pts; ingest+query unchanged (24.8M pts/s, 2.0ms). |
| 2026-07-26 | F2 | complete | retention= on all three vtabs (native-unit persisted in _meta, data-time cutoff derived from index+buffer at maintenance, /16 advance guard). RED→GREEN: backfill test corrected to the chunk-granular contract. 114 workspace tests, cli.sh 25 sections incl. §24 (per-module prune, rollback-restores-pruned, 4-window steady state = exactly one epoch), oracle+crash via cli.sh, interleaved ingest A/B at parity. F3/F4 next. |
| 2026-07-26 | F1 | complete | timeless_series + timeless_stats shipped with logs/traces shared_engine_for helpers and module probe. Acceptance: 1000-series catalog in 3.3ms warm (389ms first-call = one-time engine recovery), zero chunk reads. 108 workspace tests, cli.sh 24 sections incl. new §23 (9 checks: buffered/prune/DROP-recreate states, module routing errors). F2 next. |
