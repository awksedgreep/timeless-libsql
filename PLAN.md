# timeless-libsql — Hero POC Project Plan

**Pitch:** VictoriaMetrics-class compression with a SQL interface, inside any
SQLite/libSQL database, one `.load` away. "FTS5 for telemetry," starting with
metrics. Port the *techniques* proven in timeless_metrics (pco columnar chunk
compression, batch-first write path, series registry, block pruning) into a
loadable extension — not a port of the timeless architecture.

**Prior art check (2026-07-21):** nobody has done domain-aware compressed
telemetry storage as a SQLite or libSQL extension. Nearest neighbors:
sqlite-zstd (generic row compression), sqlite3-partitioner (partitioning only),
ProxySQL embedded TSDB (plain rows). TimescaleDB compressed hypertables are the
only proven instance of this pattern, on Postgres. The lane is empty.

---

## Hero POC success criteria

The demo that makes people go "whoa":

1. `sqlite3` CLI: `.load ./libtimeless_metrics` →
   `CREATE VIRTUAL TABLE metrics USING timeless_metrics;` → insert → query. It
   is Just A Table.
2. TSBS-style dataset (~100M points): vtab database file is **20–40x smaller**
   than the same data in a plain SQLite table, with equal-or-better range-query
   latency.
3. Blob-batch ingestion sustains **≥8M pts/sec** (Tier 2 interface, below).
4. Flourish: same `.so` loaded into self-hosted `sqld` via
   `--extensions-path`, queried over HTTP. libSQL replication of a telemetry
   db demonstrated (embedded replica pulls the metrics db).

## Non-goals (POC)

- PromQL, rollups, retention, alerting, scraping (stays in timeless_metrics /
  future work)
- Label-based pushdown beyond metric name + time range (labels stored + returned,
  filterable by SQLite above the vtab; label posting-list pushdown is v2)
- ~~Multi-connection concurrent writers (single writer assumed; see Risk
  R4)~~ FIXED in Session 10: process-global engine registry + per-table
  writer gate (see Risk R4 entry); concurrent writers now serialize with
  busy-style errors instead of corrupting split state
- Durability stronger than timeless_metrics today (in-memory buffer until chunk
  flush; raw staging shadow table is a v2 decision with write-amp tradeoff)

Logs and traces are NOT non-goals — they are Phase 2 (Sessions 5–6), gated on
the metrics POC proving the vtab skeleton. Their designs are specified below so
weekend sessions can flow straight into them.

---

## Architecture

```
                     ┌─────────────────────────────────────────┐
 SQL (any client)    │  timeless-ext (cdylib, loadable ext)    │
 INSERT / SELECT ───▶│  vtab module: xCreate/xConnect,         │
 'ingest' blob cmd   │  xUpdate, xBestIndex/xFilter, commands  │
                     ├─────────────────────────────────────────┤
                     │  timeless-core (pure Rust, no SQLite,   │
                     │  no rustler): Engine, SeriesRegistry,   │
                     │  PartitionBuffer, pco chunk codec       │
                     ├─────────────────────────────────────────┤
                     │  ChunkStore trait (metrics) /            │
                     │  BlockStore (logs+traces, Phase 2)       │
                     │   ├─ FsStore  (timeless_metrics keeps)  │
                     │   └─ ShadowTableStore (blobs via host   │
                     │      connection, prepared stmts)        │
                     └─────────────────────────────────────────┘
                                SQLite / libSQL / sqld
```

Key properties carried over from timeless_metrics:
- points queryable before AND after flush (merged buffer + chunk reads)
- batch-first write path
- chunk metadata prunes reads (series_id + ts_min/ts_max)
- WAL is NOT the bottleneck: at 16M pts/sec the engine emits only ~10–30MB/s
  of pco-compressed chunk blobs. The entire perf problem is the row-at-a-time
  SQL call path — solved by interface tiers, not storage heroics.

## Ingestion interface tiers

| Tier | Interface | Expected rate | Scope |
|------|-----------|---------------|-------|
| 1 | `INSERT INTO metrics(name, ts, value, labels) VALUES ...` (prepared, big txns) | ~0.5–2M pts/s (measure in spike!) | POC — compatibility floor |
| 2 | `INSERT INTO metrics(metrics, batch) VALUES ('ingest', :blob)` — packed columnar blob, FTS5 command idiom | target ≥8M pts/s | POC — hero benchmark runs on this |
| 3 | `sqlite3_bind_pointer` / carray-style zero-copy (in-process only) | ~native 16M pts/s | Deferred unless Tier 2 disappoints |

Durability semantics MUST be identical across tiers (same engine buffers, same
flush contract).

### Batch blob format v0 (public contract — version it from day one)

```
offset  size  field
0       1     format version (0x01)
1       1     flags (reserved, 0)
2       2     reserved
4       4     u32 LE  n_series_entries
8       4     u32 LE  n_points
12      —     series table: n_series_entries × { u32 LE name_len, name utf8,
              u32 LE labels_len, labels utf8 (JSON) }
—       —     per-point series index: n_points × u32 LE (into series table)
—       —     timestamps: n_points × i64 LE (unix seconds — match timeless)
—       —     values: n_points × f64 LE
```

Columnar (all series refs, then all ts, then all values) so the engine ingests
near-memcpy. Alignment: don't assume — read LE explicitly. Keep it dumb; this
is not protobuf territory.

### Command surface (FTS5 idiom)

- `INSERT INTO metrics(metrics) VALUES ('flush')` — force chunk flush
- `INSERT INTO metrics(metrics, batch) VALUES ('ingest', ?)` — Tier 2
- (later: 'optimize' for merge compaction, 'stats')

## Shadow tables (created by xCreate)

```sql
CREATE TABLE IF NOT EXISTS "<name>_series" (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  labels      TEXT NOT NULL DEFAULT '{}',   -- JSON
  labels_hash BLOB NOT NULL,
  UNIQUE(name, labels_hash)
);
CREATE TABLE IF NOT EXISTS "<name>_chunks" (
  id          INTEGER PRIMARY KEY,
  series_id   INTEGER NOT NULL,
  ts_min      INTEGER NOT NULL,
  ts_max      INTEGER NOT NULL,
  point_count INTEGER NOT NULL,
  codec       INTEGER NOT NULL,             -- 1 = pco-v<x>
  resolution  INTEGER NOT NULL DEFAULT 0,   -- 0=raw; rollup ladder is v2
  data        BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS "<name>_chunks_series_ts"
  ON "<name>_chunks"(series_id, ts_min);
CREATE TABLE IF NOT EXISTS "<name>_meta" (k TEXT PRIMARY KEY, v);
  -- schema_version, engine config
```

xConnect: rebuild in-memory series registry + chunk metadata index by scanning
`_series` and `_chunks` metadata columns (NOT the blobs). Adapt the engine's
existing restart-recovery logic.

## Declared vtab schema

```sql
CREATE TABLE x(name TEXT, ts INTEGER, value REAL, labels TEXT HIDDEN, ...);
```

xBestIndex pushdown for POC: `name = ?` (equality) and `ts >= / <= / BETWEEN`.
Everything else filtered by SQLite above us (correct, just not pruned).

---

## Phase 2 — logs store (`timeless_logs` vtab)

Donor design: timeless_logs (Elixir) — blocks + inverted term index +
two-tier raw→compressed compaction, ~12.8x with OpenZL columnar split. Unlike
metrics there is no Rust donor code: this is a fresh Rust implementation of a
proven design. Reference: `~/Documents/elixir/timeless/timeless_logs/docs/
architecture.md` (write path, columnar format, selective term indexing,
compaction + merge compaction).

```sql
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,path,status');
-- declared schema: x(ts INTEGER, level TEXT, message TEXT, metadata TEXT)
```

Shadow tables:
```sql
"<name>_blocks"  (id INTEGER PK, ts_min, ts_max, entry_count, byte_size,
                  codec INTEGER,  -- 1=raw 2=zstd-columnar 3=openzl-columnar
                  data BLOB)
"<name>_terms"   (term TEXT, block_id INTEGER,
                  PRIMARY KEY(term, block_id)) WITHOUT ROWID
"<name>_meta"    (k TEXT PRIMARY KEY, v)
```

Design decisions (ported from timeless_logs):
- **Columnar split before compression**: timestamps (i64 LE), levels (u8),
  length-prefixed messages, batched metadata — each column compressed
  independently. Port the exact layout from timeless_logs storage.md.
- **Two-tier blocks**: inserts accumulate in engine buffer → flush writes a
  raw block row → 'optimize' command compacts raw→compressed and merges small
  blocks (bigger dictionary window). Compaction = one transaction (insert new
  block + terms, delete old rows) — atomic swap free of charge.
- **Selective term indexing**: only `level:` terms plus keys named in
  `index_keys=` vtab arg join the posting list. Identifier-like metadata stays
  scan-only. This is the lesson that keeps the index small — encode it as a
  schema-level contract.
- **Compression codec**: v1 = zstd columnar split (pure-ish Rust via `zstd`
  crate, links everywhere). OpenZL (C++) is a stretch goal AFTER confirming it
  static-links cleanly into a loadable .so (Risk R7). Codec column means both
  coexist; ratio target ≥10x either way. Third option on the table: a
  purpose-built pure-Rust block codec — see "Codec strategy" below; the
  Session 5 benchmark is the decision point.
- Pushdown (xBestIndex): `level = ?`, indexed `metadata` key equality
  (term posting-list intersection in SQL), `ts` range. `message LIKE` is NOT
  pruned but IS executed inside the module (decompress candidate block →
  substring scan) — never materialize non-matching rows.
- Tier 2 ingest: `INSERT INTO logs(logs, batch) VALUES ('ingest', ?)` —
  batch format v0-logs: header + columnar {ts[], level[], msgs, metadata}.

## Phase 2 — trace store (`timeless_traces` vtab)

Donor design: timeless_traces (Elixir) — same block skeleton as logs plus a
trace index with packed 16-byte trace IDs, ~10x compression. Reference:
`~/Documents/elixir/timeless/timeless_traces/docs/architecture.md`.

```sql
CREATE VIRTUAL TABLE traces USING timeless_traces;
-- declared schema: x(trace_id BLOB, span_id BLOB, parent_span_id BLOB,
--                    name TEXT, service TEXT, kind TEXT, status TEXT,
--                    start_ts INTEGER, duration_ns INTEGER, attributes TEXT)
```

Shadow tables: `_blocks`, `_terms`, `_meta` identical to logs, plus:
```sql
"<name>_trace_blocks" (trace_id BLOB, block_id INTEGER,
                       PRIMARY KEY(trace_id, block_id)) WITHOUT ROWID
```

Design decisions:
- **Packed binary trace IDs** (16-byte BLOBs) in the trace index — the
  timeless_traces lesson; no hex text anywhere in storage.
- The hero query: `SELECT * FROM traces WHERE trace_id = x'...'` →
  `_trace_blocks` lookup → decompress only blocks containing that trace.
  This pushdown is the whole point of the trace vtab; get it into xBestIndex
  with a high-priority index plan.
- Terms: `service:`, `kind:`, `status:`, `name:` — same posting-list table
  and intersection pushdown as logs.
- Timestamps are **nanoseconds** here (OTel convention; logs are µs, metrics
  s) — the shared block-store code must not assume a unit. Store unit in
  `_meta`.
- Tier 2 ingest: same command idiom, batch format v0-traces (columnar spans).
- Reuse: logs and traces share the block store, term index, compaction, and
  command plumbing in `timeless-core` — the traces vtab should be mostly
  configuration + the trace_blocks index over the logs skeleton. timeless
  proved this: the two Elixir libraries are near-clones.

---

## Pruning & retention (design now, implement v2 — one schema cost today)

Block-granular deletes are a structural advantage: one DELETE row = one whole
compressed block (~2000 entries) or chunk, found via the ts_max metadata that
already exists for query pruning. ~1000x fewer row touches than row-store
retention (cf. Home Assistant recorder-purge pain). Worth a demo line.

Decisions:
- **Single db file, freelist reuse — NOT partitioned ATTACH dbs.** SQLite
  DELETE frees pages but never shrinks the file; at steady-state ingest that's
  fine (file plateaus ≈ retention window + slack; freed pages recycle).
  Set `PRAGMA auto_vacuum = INCREMENTAL` at xCreate (must precede growth);
  trim slack with small `incremental_vacuum(N)` during maintenance. NEVER
  full VACUUM (whole-file rewrite, ingest stall). ATTACH partitioning would
  recover drop-the-file semantics but breaks the vtab model + sqld story —
  rejected.
- **No daemon → prune on command + opportunistically.** `INSERT INTO
  t(t) VALUES('prune')` command; auto-enforce during 'flush'/'optimize' when
  `retention='30d'` vtab arg is set. Batch deletes to bound the WAL.
- **Same-transaction index cleanup**: deleting a block removes its `_terms` /
  `_trace_blocks` rows atomically. Posting lists never dangle.
- **HARD RULE — cap merged-block time span** (e.g. 1h): merge compaction must
  never produce blocks straddling retention boundaries, or old data becomes
  unprunable until the whole block expires. Make it a rule, not an emergent
  property of ts_min grouping. Expensive to retrofit — adopt in Session 5.
- **Metrics resolution ladder is v2, but costs one column NOW**:
  `resolution INTEGER DEFAULT 0` on `_chunks` (0=raw) so rollup chunks (1m/1h
  aggregates) land in the same table later with per-resolution retention, no
  schema migration.
- **Replication bonus**: retention deletes replicate — embedded replicas prune
  in lockstep with the primary, zero read-side config.

---

## Codec strategy — the OpenZL question (decided by data, not taste)

Three candidate codecs for logs/traces blocks. NOT porting OpenZL to Rust —
it's a 100k-line actively-evolving framework with a versioned frame format;
a faithful port is months of work plus a permanent upstream-tracking
treadmill. The realistic options:

1. **zstd columnar** (v1, always ships): per-column zstd over our split.
   Pure-Rust-friendly, links everywhere, respectable ratio.
2. **OpenZL via `openzl-sys` FFI crate** (stretch): thin -sys + safe wrapper,
   days not months; ex_openzl build knowledge transfers directly. One call per
   block → FFI overhead irrelevant. C++ contained in one reusable crate.
3. **`timeless-codec` — purpose-built pure-Rust block codec** (the
   "openzl-lite" option): our columnar split + per-column transforms —
   delta/pco for timestamp columns, dictionary-assisted zstd for messages,
   plain zstd for metadata/attributes. Key insight: the columnar split (the
   biggest win) is OUR code, applied before OpenZL ever sees data — OpenZL's
   *marginal* gain over a good per-column pipeline is unmeasured. Standalone
   value: publishable crate, no C++ anywhere, WASM-capable, and the only
   codec shape that could survive an upstream PR into libSQL core (C
   codebase — a C++ framework dep is ~disqualifying there).

**Decision rule (Session 5 benchmark, same blocks, three ways):**
- OpenZL margin over best pure-Rust pipeline < ~1.5x → drop OpenZL in Rust
  contexts; invest the difference in timeless-codec transforms.
- Margin ≥ ~2x → keep OpenZL via -sys crate as the premium codec; revisit
  only if WASM/upstreaming ambitions materialize.
- In between → ship both behind the codec byte and let deployments choose
  (size-sensitive vs dependency-sensitive).

The codec byte in `_blocks` already reserves slots, so this is not an
architectural decision — blocks with different codecs coexist in one table.
Nothing blocks on this choice.

**DECIDED 2026-07-22 (post-Session 6):** `timeless-codec` becomes a real crate.
- API guardrail: the public unit is the TYPED COLUMN ENCODER (i64/f64/
  low-card-string/high-card-string/blob + validity bitmaps + adaptive
  selection + block framing) — NOT LogEntry/SpanEntry. Logs = 4 fixed
  columns, spans = 10, the v3 generic table = any STRICT schema. Scope name
  is positioning, not a technical ceiling.
- FSST demoted to bake-off checkbox (owner's prior tests were poor + our
  access pattern decompresses whole blocks, so FSST's random-access edge is
  never collected while its ratio deficit vs concatenated-column zstd is
  always paid). Expected menu: pco (numerics/ts), dictionary+RLE (low-card),
  zstd±trained-dict (messages/high-card).
- Compose existing crates (pco, maybe vortex-alp) — originality lives in
  selection policy + framing + form factor (tiny, no Arrow, no C++, WASM).
  Prior art: Vortex is BtrBlocks-in-Rust for the Arrow world; its encodings
  are consumable à la carte; its format/deps are not our form factor.
- STRATEGIC ARC (opt-in, later, owner's call): publish timeless-core +
  timeless-codec to crates.io → Elixir timeless_logs/traces can become thin
  rustler NIFs over the same crates (the tms_engine shape), deleting the
  ex_openzl C++ build from the Elixir distribution. Evidence needed first:
  same-data bake-off vs OpenZL (we're at 12.2x zstd-columnar vs their 12.8x
  OpenZL on DIFFERENT datasets — suggestive, not proven).
- First task next session: extract crates/timeless-codec by deduping
  blocks/codec.rs + spans/codec.rs (zstd helpers + header framing).

**DONE 2026-07-22 (Session 7):** `crates/timeless-codec` exists and shipped.
- API guardrail held: public unit = typed column encoders (encode_i64 /
  encode_f64 / encode_str / encode_u8 / encode_fixed_bytes + framed
  ColumnEnc, adaptive strategy selection by sampled trial encode), no
  LogEntry/SpanEntry anywhere in the crate. Shared zstd helpers +
  bounds-checked Reader moved there; the blocks/spans copies deleted.
- Codec 4 ("adaptive columnar v1") added to blocks/ and spans/:
  optimize() writes it, decode speaks 1/2/4, raw flush stays 1, codec 3
  still reserved for OpenZL. Codec 2 is legacy-decodable forever (unit
  tests keep encoding it via the retained encoder path).
- Bake-off numbers + verdict: see the Session 5 checklist entry. Verdict
  short form: codec 4 default (traces -5.3% ≥ 5% gate; logs -3.4%;
  decode FASTER both datasets; no query regression >20% — level=error
  24.2ms vs 22.9 recorded = noise-level, trace lookup 3.36ms vs 3.9).
- Strategy menu behaved as predicted: dictionary fired on services AND
  names (every group), delta+pco beat delta+zstd on ms-jitter log ts and
  on ns start_ts/durations (every group), RLE collapsed the partition-
  pure level/status columns to 5 bytes; messages stayed concat+zstd
  (unique ids → distinct ratio ~1), kind stayed zstd (shuffled u8s, RLE
  loses), ids stayed plain zstd (random = irreducible). FSST never
  entered (owner decision); metadata/attributes stay serialized+zstd
  (per-key columns = future format revision).

**DONE 2026-07-22 (Session 8):** codec 5 ("adaptive columnar v2") —
metadata/attributes SHREDDED into per-key columns. optimize() writes 5;
decode speaks 1/2/4/5; raw flush stays 1; codec 3 still reserved.
- Layout (blocks/codec.rs encode_pairs_column, SHARED with spans):
  strategy byte per block — SHREDDED if distinct keys ≤ 64, else LEGACY
  (the codec-2/4 bytes verbatim, so key-explosion blocks never regress).
  SHREDDED = [u16 n_keys][sorted len-prefixed keys] then per key:
  presence bitmap (ceil(n/8) bytes, timeless-codec bitmap helpers) +
  DENSE values through encode_str — each key's values get their own
  adaptive dictionary/concat pick. No outer zstd over the shredded
  region (values already compressed per key; keys table + bitmaps tiny).
  Canonical (sorted+deduped) pairs are guaranteed by push() in both
  engines; the encoder VERIFIES and falls back to LEGACY otherwise.
- bench-codec (codec 4 vs 5, same 8192-entry partition-pure groups):
  logs metadata -20.9% (3,596,129 → 2,845,453 B over 125 groups; rep
  group 29,630 → 23,297 = -21.4%), logs total **-8.1%** (9.21 → 8.46
  MB). Traces attributes +4.2% (821,762 → 856,384 B) → total +0.13%:
  the 2-key OTel-ish attribute schema is always-present, so the two
  all-ones bitmaps (2KB/block) buy nothing the interleaved zstd form
  didn't already have — the shred pays off with MORE keys and SPARSER
  keys (the logs shape), not fewer. Decode FASTER both datasets (logs
  558 → 619 MB/s, traces 628 → 697 MB/s); encode -14% logs / -3%
  traces, paid at optimize() time only.
- End-to-end after the switch: logs file 9.65 → 8.93 MB (12.5x →
  **13.5x** vs plain), traces 35.8 → 35.9 MB (4.3x, unchanged).
  Queries within noise of recorded: level=error 20.8–23.8ms (rec 24.2),
  svc+level+range 7.2–8.4ms (rec 7.4), trace lookup 3.41ms (rec 3.36),
  status=error 4.3ms (rec 4.1) — none near the 20% gate.
- Decision rule (≥3% total on either dataset, no >20% query
  regression): **PASS on logs → codec 5 is the optimize() default.**

---

## Source-of-truth references (already verified — don't re-derive)

### timeless_metrics engine — the donor code
`~/Documents/elixir/timeless/timeless_metrics/native/tms_engine/src/lib.rs`
(~3,500 lines, single file)
- Lines ~1–2445: pure engine — extraction target. Structs: `PartitionKey`(42),
  `SeriesInfo`(48), `PartitionBuffer`(53), `ChunkMeta`(81), `SeriesRegistry`(99),
  `Engine`(442, impl at 501–2288), `CompressedPartition`(477)
- Line ~2448 onward: 22 `#[rustler::nif]` wrappers + `EngineResource`
  (ResourceArc shim, line 467–475) — NOT extracted; these define the API the
  vtab needs (~8 of them)
- Deps: `pco = "1"`, `dashmap = "6"`, `rayon = "1"` (all pure Rust — keep);
  `rustler = "0.37"` (stays behind in tms_engine)
- Watch for stray `rustler::Binary`/`Term`/`atoms!` usage inside engine
  internals during extraction — expect a few hours of type substitution

### timeless_logs / timeless_traces — the donor designs (Elixir, no Rust donor)
- `~/Documents/elixir/timeless/timeless_logs/docs/architecture.md` — write
  path, ETS term index (maps to `_terms` table), columnar OpenZL format,
  selective metadata indexing policy, compaction + merge compaction triggers
- `~/Documents/elixir/timeless/timeless_logs/docs/storage.md` — exact columnar
  layout to port (ts i64 LE / level u8 / length-prefixed messages / batched
  metadata), measured ~12.8x
- `~/Documents/elixir/timeless/timeless_traces/docs/architecture.md` — trace
  index w/ packed 16-byte IDs, span terms, ns timestamps, ~10x
- Their snapshot+disk_log durability machinery does NOT port — SQLite
  transactions replace it entirely (that's half the pitch)

### libSQL source (cloned at `~/Documents/rust/libsql`)
- sqld extension loading: `libsql-server/src/config.rs:131`
  (`validate_extensions`) — `--extensions-path <dir>`, dir contains
  `trusted.lst`, lines are `<sha256> <filename>`, verified at startup
- Per-connection load: `libsql-server/src/connection/connection_core.rs:119`
  (rusqlite `LoadExtensionGuard` + `load_extension`) — loaded into EVERY
  connection (see Risk R4)
- Embedded API: `libsql/src/local/connection.rs:428`
  (`enable_load_extension` / `load_extension`)
- Native-vtab precedent/template: `libsql-sqlite3/src/vectorvtab.c` (+ 11 more
  vector*.c files) — the upstream-path model
- sqld hosts connections via rusqlite → testing against rusqlite ≈ testing
  against sqld's host stack

### Rust vtab plumbing candidates (spike decides)
- `rusqlite` with `loadable_extension` + `vtab` features (has `UpdateVTab`
  trait for writes)
- `sqlite-loadable-rs` (Alex Garcia — sqlite-vec lineage)
- Fallback if both disappoint: thin C shim for module registration, Rust does
  the work

---

## Weekend session plan

### Session 1 — Day-0 spike + scaffold — ✅ COMPLETE 2026-07-22, ALL GATES GREEN

- [x] `git init`, workspace scaffold: `crates/timeless-core`,
      `crates/timeless-ext` (cdylib), `tools/libsql-check` (detached)
- [x] Spike A — writable vtab via rusqlite 0.40.1 (`vtab` +
      `loadable_extension` features). `Module::update_module_with_tx()`,
      `UpdateVTab` + `TransactionVTab` (xBegin/xCommit/xRollback exist in
      rusqlite natively → risk R5 has library support!). No C shim needed.
- [x] Spike B — re-entrancy works exactly as hoped:
      `Connection::from_handle(raw_db)` inside callbacks; xCreate makes the
      shadow table, xUpdate inserts, cursor reads back. BEGIN/ROLLBACK
      correctly discards vtab writes (shadow writes ride the host txn — the
      atomicity claim, proven). Reopen (xConnect) + DROP (xDestroy cleanup)
      verified.
- [x] Spike C — measured on 1M rows via generate_series, release build:
      plain table ~16M rows/s; vtab naive (re-prepare per row) ~1.0M/s;
      vtab with held host Connection + prepare_cached ~3.4M/s.
      **Tier 1 ceiling = ~3.4M pts/s** (better than the 0.5–2M estimate).
      Lesson recorded: hold the host Connection in the vtab struct, use
      prepare_cached, pre-format SQL strings.
- [x] libsql parity — same .so loads via `libsql` crate 0.9
      (`load_extension_enable` + `load_extension`, core features only);
      full vtab lifecycle passed. `tools/libsql-check` is the harness.
- [x] **Gate: PASSED.** API facts: Inserts/Updates args include the two rowid
      slots — columns start at index 2. Entry points exported:
      sqlite3_extension_init + sqlite3_timelessext_init +
      sqlite3_timeless_ext_init (filename-derived name ambiguity covered).

### Session 2 — Core extraction + storage seam (≈1–1.5 days) — IN PROGRESS
- [x] Carved lines 1–2443 of tms_engine `lib.rs` into `timeless-core`
      (2026-07-22). Rustler coupling was ONLY the imports + atoms mod +
      EngineResource block (grep-verified, deleted). Two helpers lived below
      the boundary and were copied in: `read_exact_at`, `partition_vec_memory`.
      Engine half includes the pure Prometheus text parser (future scrape
      compat). Public API pub-ified per the 22 NIF call sites.
- [x] DECISION: Elixir repo's tms_engine left untouched (no path dep into an
      experimental repo from a published hex package). Rewiring it onto a
      published timeless-core crate is post-POC.
- [x] Round-trip acceptance tests green: write → pre-flush query → flush →
      post-flush query → aggregate → shutdown → restart-recovery from disk.
      Compression measured: 1M-point drifty gauge = 0.133 bytes/point
      (~120x vs 16B raw; pco best case, TSBS will be honest).
- [x] `ChunkStore` trait threaded through the engine (subagent refactor,
      2026-07-22): `store/mod.rs` (trait, ChunkLoc enum File|Row, EncodedChunk,
      StoredChunk, ChunkBytes) + `store/fs.rs` (FsStore — ALL fs machinery
      moved: PCO1/PCB1 formats, pending/manifest compaction recovery, TTL file
      cache, dir scan). Engine holds Box<dyn ChunkStore>; zero fs:: in
      engine.rs. `replace_chunks` gained an on_committed callback to preserve
      pre-refactor ordering (manifest → rename → index swap → delete).
      SQLite-backend risk notes recorded by the refactor: `_chunks` MUST use
      INTEGER PRIMARY KEY (vacuum moves bare rowids under ChunkLoc::Row!);
      keep storage_stats O(1); registry blob rewrite = write amplification;
      read_chunk wants one contiguous buffer per chunk.
- [x] Compression honesty tests (user challenged the 120x number — verdict:
      REAL but best-case). Every point verified bit-exact after cold recovery,
      bytes measured from actual files: periodic sawtooth 0.133 B/pt (120x);
      hostile random walk (ms-jitter timestamps, 4-decimal noise) 3.9 B/pt
      (4x lossless). Real telemetry lands between; TSBS in Session 4 is the
      quotable number.
- [x] Implement `ShadowTableStore` in timeless-ext: prepared statements against
      `_chunks`/`_series` on the host connection (→ rolls into Session 3)
- [x] Unit tests for round-trip: write chunks through ShadowTableStore, recover
      registry + metadata from tables (→ rolls into Session 3)

### Session 3 — The vtab, end to end — ✅ CORE COMPLETE 2026-07-22
- [x] ShadowTableStore (timeless-ext/src/shadow_store.rs): ChunkStore over the
      host connection. Mutex<HostHandle> for rayon-thread safety; two blob
      columns (ts_data/val_data); id INTEGER PRIMARY KEY (vacuum-safe);
      replace_chunks rides the host transaction (NO manifest — SQLite provides
      the atomicity). resolution column reserved.
- [x] timeless_metrics vtab (metrics_vtab.rs): runtime schema with hidden
      command column; commands flush/compact/prune:<ts>; append-only (DELETE/
      UPDATE rejected with clear errors); hand-rolled flat-JSON labels parser
      (no serde); best_index bitmask name-eq + ts-range pushdown.
- [x] tests/cli.sh: 8 sections all PASS (round-trip, append-only, spike
      regression, pushdown, reopen recovery, prune, compact, rollback caveat).
- [x] Hero smoke (1M pts, synthetic-friendly): plain SQLite table db 46.7MB
      vs vtab db 233KB = ~200x smaller file; Tier 1 SQL ingest 2.8M pts/s;
      flush 26ms → 1 chunk; count(*) through vtab = 1M. (TSBS honesty run
      still owed in Session 4.)
- [!] **LESSON (recorded): rayon deadlock trap.** Engine methods using
      par_iter (query_range_labeled, query_aggregate_labeled) MUST NOT be
      called from vtab callbacks: rayon workers re-enter SQLite on the host
      connection whose mutex the host thread holds inside xFilter → deadlock.
      Cursor uses sequential query_range_by_id per series. Fix candidates for
      later: sequential variants in timeless-core, or rayon-off engine mode.
- [x] DONE 2026-07-22 (Session 9 hardening — see that session's block):
      plain-table oracle property test (tools/bench `oracle`, 3 seeds ×
      50k ops, cli.sh section 19); kill -9 crash test (tests/crash.sh,
      cli.sh section 20); R5 real transaction rollback (journal in all
      three engines, TransactionVTab wired — risk register updated).
- [x] DONE 2026-07-22 (Session 10): R4 global engine registry — one
      engine per (db file, table) per process, thread-local connection
      routing, per-table writer gate (see that session's block and the
      risk register R4 entry)

### Session 4 — Tier 2 + hero benchmark + sqld flourish (≈1–1.5 days)
- [x] Batch blob format v0 encoder (bench-side) + decoder (ext-side);
      'ingest' wired to resolve_series_batch + write_batch_raw. DEVIATION
      from the "('ingest', :blob)" sketch above: the hidden command column
      is overloaded by TYPE instead — TEXT = command, BLOB = v0 batch
      (`INSERT INTO metrics(metrics) VALUES (:blob)`). Unambiguous, zero
      schema change, one less string compare on the hot path. Decoder
      validates the WHOLE blob (incl. every series index) before writing
      anything; malformed batch = hard error, nothing stored. cli.sh
      sections 7 + 8 cover exact round-trip, truncation, bad index.
- [x] Bench harness: tools/bench (standalone crate, bundled rusqlite),
      TSBS-style deterministic workload (100 hosts × 10 metrics × 1000
      pts = 1M, 10s cadence + ms jitter, per-kind value shapes), three-way
      plain vs Tier 1 vs Tier 2, query timings + bit-exact spot check
- [x] Tier 2 ≥8M pts/s: MET — ~17–18M pts/s ingest (1M pts in ~56ms via
      10×100k blobs, single txn); Tier 1 ~1.7–2M pts/s; plain baseline
      ~3.6–4M rows/s. File 8.28MB vs plain 52.6MB (6.4x smaller,
      8.3 B/pt) on honest jittered TSBS data; flush 177ms; count/range
      queries verified after reopen (numbers from 2026-07-22 run)
- [x] sqld demo 2026-07-22: sqld built from cloned source; trusted.lst
      sha256 verified; vtab created + ingested + flushed via HTTP request 1;
      SEPARATE request (fresh pooled connection → xConnect recovery) returned
      rows with pushdown in 0.19ms. Legacy JSON endpoint (POST /).
- [ ] Stretch: embedded replica of the metrics db syncing to a second node
- [x] RESULTS.md written — all four success criteria met (compression
      criterion with honest asterisk: 6.4x on deliberately-hostile TSBS
      variant, 200x friendly; see RESULTS.md)
- [x] Post-POC (2026-07-22): Prometheus text ingest shipped. The hidden
      BLOB column now sub-dispatches on its FIRST byte: 0x01 = batch v0
      (unchanged), 0x00/0x02–0x08 = reserved future batch versions (loud
      "unknown blob format" error, never mis-parsed as text), anything
      else = Prometheus text exposition body → engine.ingest_prometheus.
      SQL surface unchanged: INSERT INTO metrics(metrics) VALUES
      (readfile('scrape')). UNIT DECISION: tables store EPOCH SECONDS —
      the engine normalizes explicit prom ms timestamps (/1000), so the
      wall-clock default for timestamp-less samples is passed in seconds
      to keep every body internally consistent. Partial success (some
      samples + some malformed/NaN lines) succeeds silently, like a real
      Prometheus scrape; only 0-samples-with-errors bodies fail. The
      scraping LOOP stays external by design (cron/curl/Elixir) — the
      vtab is passive. cli.sh section 18 + timeless-core
      tests/prom_ingest.rs pin the semantics.

### Session 5 — Logs vtab (Phase 2, ≈1.5–2 days) — ✅ CORE COMPLETE 2026-07-22
Gated on: metrics POC green (vtab skeleton, command idiom, shadow-table store
all proven — logs reuses every one of them).

- [x] `timeless-core`: generic block store module (`src/blocks/`: LogEntry,
      BlockEngine, BlockStore trait mirroring ChunkStore, MemBlockStore for
      tests), unit-agnostic timestamps (`merge_max_ts_span` is a config
      param in ts units; the vtab passes 3_600_000 ms). 11 unit tests green
      incl. read-count proof that term pruning skips blocks.
- [x] Columnar split codec: ported the timeless_logs layout (ts delta+zstd /
      level u8 / u32-len-prefixed messages / len-prefixed metadata pairs,
      each column independently zstd-7); codec byte reserves OpenZL slot
      (1=raw framing, 2=zstd-columnar, 3=reserved)
- [x] `timeless_logs` vtab: xCreate with `index_keys=` arg (persisted in
      _meta, read back at xConnect — never trusts replayed args), xUpdate
      row insert, 'flush' + 'optimize' commands. DESIGN IMPROVEMENT: one
      HIDDEN TEXT column per index key, so `WHERE service='api'` is plain
      column equality (pushed to posting lists) and `SELECT service` works.
- [ ] 'ingest' blob batch for logs (Tier 2) — DEFERRED; hidden column
      dispatches by type already (BLOB reserved, clear error for now)
- [x] xBestIndex: level/term equality + ts range → posting-list INTERSECT
      in SQL. Deviation: `message LIKE` left ABOVE the vtab (SQLite filters
      rows we materialize; candidate blocks still pruned by the other
      constraints). In-module substring scan = later optimization.
- [x] Compaction as single-transaction atomic swap (one replace_blocks call
      rides the host txn; terms swapped in the same operation); merge
      compaction with HARD time-span cap on merged blocks (retention
      boundary rule — unit test proves the cap splits merges)
- [x] 'prune:<ts>' command; same-transaction terms cleanup (cli.sh section
      11 asserts _terms count drops with _blocks); batched deletes
- [ ] `retention=` vtab arg (auto-prune during flush/optimize) — deferred
      with Tier 2; 'prune:<ts>' covers the POC story
- [x] Oracle test vs plain table (bench-logs cross-checks all query counts);
      compression ratio benchmark vs plain SQLite log table: **11.2x
      smaller (10.7 B/entry vs 120.3), target ≥10x MET** on 1M realistic
      entries. NOTE: file shrink needs host-side `PRAGMA auto_vacuum =
      INCREMENTAL` before growth + stepped `PRAGMA incremental_vacuum`
      after 'optimize' (xCreate's pragma attempt is too late — the CREATE
      statement already allocated pages). Not yet compared vs
      timeless_logs itself.
- [x] **Level-term weakness — FIXED 2026-07-22.** The original bench
      measured `level=error` at 356.3ms/1M entries, SLOWER than a plain
      table scan (45ms): 8192-entry flush blocks were level-MIXED, so
      with a 70%-info workload every block carried every `level:` term
      and the posting-list intersection pruned nothing. Fix:
      LEVEL-PARTITIONED flush — the buffer is grouped by level and one
      level-PURE raw block per level present is written (≤4 per flush),
      so each block emits exactly ONE `level:` term and the existing
      query_terms intersection prunes perfectly; optimize() merges only
      WITHIN a level partition (merge_max_ts_span cap unchanged, applied
      per partition), and pre-existing mixed blocks form their own
      partition that never merges with pure ones. The partition tag is
      IN-MEMORY ONLY (no shadow-schema change, codec unchanged):
      recovery re-derives it from the `level:` posting lists — a block
      listed under exactly one `level:` term is pure, ≥2 is mixed (four
      metadata-only query_terms calls at xConnect). Friction fixes rode
      along: BlockStore::put_blocks (batch insert; ShadowBlockStore does
      one lock/prepared loop per flush) and query_terms now returns
      (BlockLoc, BlockMeta). New bench-logs numbers (same 1M workload):
      level=error 356.3 → 22.9 ms (15.6x, now ~2x faster than plain);
      service+level+range 102.8 → 7.9 ms (13x); LIKE '%timeout%'
      554.5 → 572.6 ms (~unchanged, expected — no indexed constraint to
      prune with); file 10.7MB/11.2x → 9.9MB/12.2x smaller (9.86
      B/entry — level-homogeneous blocks compress BETTER, e.g. the
      messages column groups per-level templates). Metrics bench
      unaffected (tier2 18.4M pts/s, bit-exact checks OK).
- [x] **Codec bake-off — DONE 2026-07-22 (Session 7)**, two ways not
      three (OpenZL untouched per the DECIDED block; codec 3 stays
      reserved): codec 2 (zstd columnar) vs codec 4 (adaptive columnar
      v1 = timeless-codec typed column encoders) over the SAME
      1M-entry logs + 960,570-span traces workloads, cut into
      8192-entry level/status-pure groups (bench-codec, shared
      `datasets` module with bench-logs/bench-traces). Result: logs
      -3.4% total (ts column -38.7% via delta+pco, level -44% via RLE;
      messages/metadata unchanged — they dominate), traces **-5.3%**
      total (names -37.8% + services -12.3% via dictionary, start_ts
      -10.3% + durations -9.9% via delta+pco, status -44% via RLE).
      Decision rule (≥5% on either dataset, no >20% query regression):
      **PASS on traces → codec 4 is the optimize() default.**
      End-to-end after the switch: logs file 9.9→9.65 MB (12.2→12.5x),
      traces 37.3→35.8 MB (4.2→4.3x); queries level=error 24.2ms,
      svc+level+range 7.4ms, trace lookup 3.36ms, status=error 4.1ms —
      all within noise of or better than the recorded numbers. Decode
      throughput IMPROVED (logs 587→630 MB/s, traces 605→767 MB/s);
      encode is the only cost (traces 174→125 MB/s, paid at optimize
      time only, and raw flush stays codec 1).
- [ ] Stretch: OpenZL static-link spike (R7) via `openzl-sys` wrapper crate —
      if clean, add codec 3 and include in bake-off

### Session 6 — Traces vtab (Phase 2, ≈1 day) — ✅ CORE COMPLETE 2026-07-22
Gated on: Session 5 (shares the entire block-store skeleton).

- [x] DESIGN DECISION — parallel `spans/` module in timeless-core, NOT a
      genericized BlockEngine: the trace index changes the STORE CONTRACT
      itself (blocks carry trace-id row sets; the store answers
      query_trace), so a generic BlockStore<Aux> would have rippled through
      every existing store impl and the four hand-written test wrapper
      stores — violating the "logs behavior must not change" gate for zero
      logs benefit. spans/ mirrors blocks/ line-for-line where logic is
      identical (deliberately diff-able) and SHARES the real primitives:
      BlockLoc/BlockMeta, codec constants, zstd helpers + bounds-checked
      Reader (blocks/codec.rs, now pub(crate)). Logs untouched: all
      Session 5 tests pass byte-identical, bench-logs regression unchanged
      (12.2x, level=error 22.9ms).
- [x] `_trace_blocks` packed-ID index (16-byte BLOBs, (trace_id, block_id)
      WITHOUT ROWID PK, deduped per block); trace_id equality is the
      top-priority xBestIndex plan (cost 10 vs 1e3 terms / 1e6 scan;
      cli.sh proves the planner picks "VIRTUAL TABLE INDEX 1"). Rows are
      created/deleted in the SAME operation as their blocks everywhere
      (flush/optimize/prune) — never-dangle extended to the trace index,
      cli.sh section 16 asserts it. NOTE: the trace_id constraint sets
      omit=1 (unique among all our vtabs) so `WHERE trace_id = '<hex>'`
      TEXT works — SQLite's own re-check would reject BLOB=TEXT; our
      filter applies exact per-span equality after parsing both forms.
- [x] Span columnar layout: 10 columns (trace 16B / span 8B / parent
      presence+8B / name u16-len / service u16-len / kind u8 / status u8 /
      start_ts i64 delta / duration i64 / attributes like logs metadata),
      ns timestamps ('ts_unit'='ns' recorded in _meta), same RAW→ZSTD
      two-tier + codec byte. Terms: service:/kind:/status:/name: ALWAYS —
      no index_keys arg (span dimensions are OTel-conventional
      low-cardinality enums/bounded sets; the open-ended stuff lives in
      scan-only attributes). Partition dimension = STATUS (unset/ok/error)
      — the Session 5 level fix applied from day one: status-pure blocks,
      merge only within partition, recovery re-derives the tag from
      status: posting lists. 11 new core unit tests incl. read-count proof
      that a trace query reads ONLY blocks containing the trace.
- [ ] 'ingest' batch format v0-traces (Tier 2) — DEFERRED, same status as
      logs Tier 2 (hidden column dispatches by type; BLOB reserved with a
      clear error)
- [x] Oracle + ratio benchmark (bench-traces, 960,570 spans = 100k traces
      × 5–20 spans, indexed-plain baseline — the fair fight): vtab
      37.3MB vs plain+idx 155.2MB = **4.2x smaller** (38.8 B/span vs
      161.6). Target was ~10x — honest miss with a clear reason: this
      workload is ~24 B/span of irreducible entropy (random 8B span_id +
      8B parent_id + log-normal ns durations ≈ incompressible); block
      payloads are already at 28.3 B/span. Text-heavier spans (more
      attributes) would widen the ratio. Queries (cold): trace_id point
      lookup 3.9ms avg vs 0.005ms plain-indexed (expected: 1–2 block
      decompressions vs a b-tree probe; interactive either way);
      status='error' count 4.6ms vs 49.7ms plain (10.8x — the status
      partition earning its keep); service+range 118ms vs 51ms (term
      candidates decompress; acceptable, optimize later). Correctness: all
      counts match the plain oracle, 3 random spans bit-exact through the
      vtab (all 10 columns), one full-trace span set identical in both
      stores. cli.sh sections 13–17 (14 new checks) all pass.
- [x] Demo 2026-07-22: three signals in ONE sqld database over HTTP —
      metric by name, log by service pushdown, trace by trace_id (hex
      round-trip). The original pitch, running.

### Session 9 — Hardening: R5 rollback, oracle, crash test — ✅ COMPLETE 2026-07-22

- [x] **R5 real transaction rollback in all three engines** (metrics
      Engine, BlockEngine, SpanBlockEngine): transaction journal keyed
      off xBegin/xCommit/xRollback (TransactionVTab wired in all three
      vtabs). Full details in the risk register R5 entry. Empirical
      fact recorded: SQLite calls xBegin per WRITE STATEMENT in
      autocommit and once per explicit BEGIN (plus one lone xCommit at
      CREATE VIRTUAL TABLE — commit on an inactive journal is a no-op),
      so txn_begin is deliberately O(active partitions)/O(1) with
      capacity-retaining reuse. Design choice: NO commands are refused
      inside transactions — flush/compact/optimize/prune are all fully
      journaled (the "restore removed entries" branch works because
      SQLite rollback restores deleted rows under their original
      rowids). 11 new core unit tests (blocks 6, spans 4 + metrics
      integration file tests/txn_journal.rs) + cli.sh sections 6/6b/6c
      rewritten from "rollback caveat WARNING" to hard assertions
      (buffered rollback, intra-txn flush rollback with buffer restore,
      auto-flush-in-txn for logs/traces, optimize-in-txn, dangling-row
      joins, integrity_check, reopen).
- [x] **Plain-table oracle property test** (tools/bench src/oracle.rs,
      cli.sh section 19): one db, three vtabs + three mirrored plain
      tables; splitmix64-seeded generator drives 50k ops/seed (85%
      inserts, commands, pushdown queries of every plan family,
      explicit txns that rollback or commit — half with intra-txn
      flush — and mirrored prune-alls); every query op compares
      canonicalized (sorted, float-by-bits) result sets; mismatch
      prints seed + op index for exact replay. 3 fixed seeds run in
      ~9s. The oracle's own first catch was a bug in ITS generator
      (shared prune cutoff across signals with different ts units) —
      the harness works. Generator constraint recorded: metric ts are
      strictly increasing per series because the chunk index is keyed
      (series, min_ts) — duplicate-min_ts chunks would shadow each
      other (pre-existing engine limit, now in RESULTS known-limits).
- [x] **kill -9 crash test** (tests/crash.sh, cli.sh section 20): 5
      iterations of killing a live 3000-round ingest (BEGIN; 30 rows
      over 3 signals; flush ×3; COMMIT; watermark print; optimize/
      compact every 25 rounds) at a random 0.1–0.8s, then reopen and
      assert: integrity_check ok; vtabs recover and FULLY decode;
      count ≥ last-watermark counts (flushed = durable); zero dangling
      _terms/_trace_blocks rows and zero term-less blocks (never-dangle
      through crashes); pushdown queries answer. Documented contract:
      flushed = durable, buffered = lost, never corrupt.
- [x] Regression sweep: timeless-core 55 tests + timeless-codec 16
      green; bench unchanged (tier2 16.8M pts/s, tier1 1.73M, bit-exact
      spot checks OK); cli.sh 47 PASS lines across 20 sections.

### Session 10 — Hardening: R4 shared engine registry — ✅ COMPLETE 2026-07-22

- [x] **Process-global engine registry** (timeless-ext/src/shared.rs):
      `static REGISTRY: Mutex<HashMap<RegistryKey, Weak<dyn Any+Send+Sync>>>`
      keyed by (canonical db file path via sqlite3_db_filename(db,
      database_name) — handles ATTACH aliases, canonicalize with
      missing-file parent fallback, table name). Empty filename
      (`:memory:`/temp) → per-connection Private key (db handle
      address) — each :memory: db is private to its connection, sharing
      would be corruption. xCreate/xConnect upgrade-or-build under the
      registry mutex (held across construction so two racing pooled
      connections can't build two engines); xDisconnect drops the Arc
      (Weak values: registry never keeps an engine alive — buffered =
      lost with the process, unchanged); xDestroy removes the entry AND
      drops shadow tables; dead Weaks swept lazily on every access.
      Type-erased values (one registry, three engine types), downcast
      checked with a loud error.
- [x] **Thread-local connection routing**: `CURRENT_DB:
      Cell<*mut sqlite3>` + RAII `DbGuard` (saves/restores previous —
      nest-safe, panic-safe) bound by every callback that can reach a
      store (connect/create, insert incl. commands, begin/commit/
      rollback, cursor filter, destroy). All three shadow stores lost
      their `Mutex<HostHandle>` (and the `unsafe impl Send`) — they are
      Strings-only now; every op fetches the CALLING connection via
      `shared::current_conn()` (correct transaction context per
      connection, no cross-connection mutex re-entry). Unbound thread →
      hard error naming the rayon lesson (the permanent guard).
- [x] **WriterGate** (per SharedEngine): `Mutex<Option<usize>>` holder
      token (conn_id = raw db pointer as usize) + Condvar, 5s bounded
      wait, re-entrant for the same connection, released only by the
      holder. Acquired in xBegin (NOT the first insert — DELIBERATE
      DEVIATION from the R4 sketch: SQLite fires xBegin on connection B
      before B's first xUpdate, and txn_begin() RESETS the engine
      journal, so gating xBegin is the only placement that keeps the
      journal provably single-writer; still lazy — xBegin only fires
      for transactions that WRITE the vtab). commit/rollback are
      holder-only (the lone xCommit at CREATE VIRTUAL TABLE must not
      close another connection's journal) and release after closing
      the journal; Drop on the vtab releases defensively.
- [x] **Empirical fact recorded** (VDBE bytecode, cli.sh 21): stock
      SQLite executes OP_Transaction (file write lock) BEFORE OP_VBegin,
      so a second writer hits SQLITE_BUSY before reaching the gate —
      the gate is defense-in-depth on stock SQLite and the ACTIVE
      journal protection under concurrent-writer hosts (libsql BEGIN
      CONCURRENT). Its timeout path is unit-tested in Rust (9 new
      shared.rs tests: gate block/timeout/re-entrancy/stray-release,
      registry share/isolate/sweep/type-mismatch, DbGuard nesting,
      unbound-thread error).
- [x] cli.sh section 21 (python3 sqlite3, TWO connections in ONE
      process): (a) A insert+flush → B sees rows WITHOUT reopen;
      (b) A buffered insert → B sees it too (shared-buffer semantics,
      asserted on purpose); (c) A BEGIN+insert → B's write fails
      bounded with a lock error (~2s busy_timeout); (d) A COMMIT → B
      retry succeeds; (e) DROP + recreate on A → both connections sane.
- [x] Semantics documented (shared.rs module docs + RESULTS.md
      known-limits): shared buffer = dirty reads of buffered points
      across connections (accepted telemetry semantics); flushed data
      remains transactional; sharp edge: another connection querying
      DURING a foreign uncommitted intra-txn flush can hit a row-read
      error until that txn commits (bounded, busy-like; single
      statement window in autocommit).
- [x] Regression sweep: 71 workspace Rust tests green (62 prior + 9
      shared.rs); cli.sh 21 sections ALL PASS (oracle + crash
      unchanged); bench unchanged (tier2 17.6M pts/s, tier1 1.75M,
      6.4x; logs 13.5x, level=error 21ms; traces 4.3x, trace lookup
      3.1ms — all within noise of Session 8/9 numbers).

---

## Risk register

- **R1 — writable-vtab support in Rust wrappers is under-traveled.**
  Mitigation: Session 1 gate; C-shim fallback (+~2 days).
- **R2 — re-entrant shadow-table SQL from vtab callbacks.** Supported (FTS5
  does exactly this) but subtle. Mitigation: Spike B before any real code.
- **R3 — rustler types entangled in engine internals.** Looked clean in
  inventory (rustler confined to lines 2448+ and EngineResource). Budget a few
  hours of type substitution.
- **R4 — sqld loads the ext into EVERY connection. FIXED 2026-07-22
  (Session 10).** One vtab instance per connection over shared shadow
  tables no longer means split state: a process-global registry
  (extensions are one shared lib per process, so a `static` works)
  hands every connection the SAME engine, keyed by (canonical db file
  path via sqlite3_db_filename, table name) — :memory:/temp fall back
  to per-connection keys. Store SQL is routed to the CALLING
  connection through a thread-local RAII binding (the shadow stores
  hold no connection at all anymore — the old Mutex<HostHandle> and
  its unsafe Send impl are gone), and write transactions are
  serialized per table by a WriterGate (holder = connection id,
  acquired at xBegin so the engine-global R5 journal stays
  single-writer, 5s bounded wait → busy-style error). Accepted
  semantics: one shared buffer per table = cross-connection dirty
  reads of buffered (pre-durable) points; flushed data remains
  transactional. Full design + deadlock analysis in
  crates/timeless-ext/src/shared.rs; proven by cli.sh section 21
  (two connections, one process) + 9 shared.rs unit tests.
- **R5 — transaction semantics of in-memory buffers. FIXED 2026-07-22
  (Session 9).** Each engine now keeps a TRANSACTION JOURNAL activated by
  xBegin (SQLite fires it before the first write of every transaction —
  verified empirically: once per statement in autocommit, once per
  explicit BEGIN; SELECTs never). Journaled: buffer marks (rollback
  truncates txn-era points), pre-txn buffered data drained by an
  intra-txn flush (RESTORED on rollback — its chunk/block rows roll back
  with the host txn), index additions (removed on rollback — no dangling
  locs) and index removals with metas (restored verbatim on rollback —
  SQLite's page-level undo brings the deleted rows back under the same
  rowids, partition tags ride along in the journaled IndexEntry). ALL
  commands (flush/compact/optimize/prune) work inside explicit
  transactions and roll back fully; add-then-remove within one txn
  cancels (nothing resurrects). Cheap by construction: txn_begin is
  O(active partitions) marks (metrics) / one usize (blocks, spans) into
  capacity-retaining collections — it is on the autocommit per-statement
  path. Remaining documented limits: SAVEPOINT-granular rollback is not
  implemented (rusqlite wires xBegin/xSync/xCommit/xRollback, not
  xSavepoint — whole-transaction rollback only); series NAMES registered
  during a rolled-back txn stay registered in memory (harmless empty
  series); the journal presumes a transactional store (the vtab's shadow
  stores — over FsStore the txn_* API must simply not be used).
- **R6 — pco chunk granularity vs SQLite page model.** Prior art warning: bit-
  level appends into page storage kill throughput. Our design never does this —
  chunks are compressed complete, stored whole as blobs. Keep it that way.
- **R7 — OpenZL is C++ and must static-link into a loadable .so** (logs/traces
  headline ratio depends on it: 12.8x OpenZL vs ~lower with zstd-only).
  Mitigation: zstd columnar is the v1 codec and is already respectable; OpenZL
  is a gated stretch spike in Session 5 (as an `openzl-sys` crate). Never
  block a session on it. Escape hatch: the pure-Rust `timeless-codec` option
  ("Codec strategy" section) may retire this risk entirely if the bake-off
  shows OpenZL's marginal gain is small.
- **R8 — three vtabs, one extension or three?** Ship ONE .so exporting all
  three modules (`timeless_metrics`, `timeless_logs`, `timeless_traces`) —
  one trusted.lst entry, one artifact, shared core. Revisit only if size or
  codec deps force a split.

## Estimate

3–4 weekend-days solo for the polished metrics hero POC (Sessions 1–4);
minimal insert-query-compress demo after Sessions 1–3. Phase 2 adds ~2.5–3
days: logs (1.5–2, it builds the shared block skeleton) then traces (~1,
mostly reuse). Working sessions with Claude Code should compress calendar
time meaningfully — extraction and vtab boilerplate parallelize well.

## Open questions (decide during, not before)

- Crate/repo name: `timeless-libsql`? `timeless-sqlite`? (artifact name
  `libtimeless_metrics.so` either way)
- timestamps: engine uses unix seconds; keep for POC, but batch format v1
  probably wants ms/ns flag for logs/traces later
- labels: JSON TEXT for POC; interning/posting-list pushdown is v2
- upstream ambition: loadable-first is decided; revisit native
  (vectorvtab.c-style) only after the POC has numbers
- if timeless-codec wins the bake-off: spin it out as its own published crate?
  (standalone value — telemetry block codec, no C++, WASM-capable; also the
  only codec shape viable for an upstream libSQL PR)
- **Edge telemetry / chunk shipping** (discussed 2026-07-21): routers that run
  containers (Mikrotik/RouterOS, ARM64) host a tiny collector + local libsql
  db with this extension; upstream transport = the `_chunks`/`_blocks` blobs
  themselves (insert into central sqld via Hrana or batch command — no
  recompression, chunks ARE the wire format). Wins: cross-sample compression
  beats per-scrape push by 10–100x on constrained backhaul; store-and-forward
  free from retention-bounded local storage; locally SQL-queryable during WAN
  outage. Zero-code alternative: central node holds embedded replicas of
  router dbs, libSQL delta sync ships (already-compressed) WAL frames.
  Ties into the planned mktxp-replacement Mikrotik collector — it could write
  local libsql instead of pushing raw samples. Great RESULTS.md demo #2 if the
  POC lands.
- **v3 north star — generic `timeless_table` vtab** (discussed 2026-07-21):
  arbitrary append-only time-keyed STRICT schemas with per-column type-aware
  codecs. SQLite has only 5 storage classes, all supportable: INTEGER
  (delta/pco/RLE/bitmap), REAL (pco/ALP), low-card TEXT (dictionary+RLE),
  high-card TEXT (FSST or dict-zstd), BLOB (zstd floor; JSONB shredding +
  F32_BLOB byte-stream-split as v2 cleverness), NULL via per-column validity
  bitmaps. Require STRICT semantics (dynamic typing is the one real hazard;
  ANY = type-tag stream fallback). Generalize ts_min/ts_max to per-column
  min/max ZONE MAPS on every block (free pruning on any range predicate);
  `index_keys=` generalizes to opt-in posting lists for any low-card column.
  DESIGN CONSEQUENCE FOR NOW: build timeless-codec as *typed column encoders*
  with adaptive codec choice by sampling at flush (BtrBlocks approach) —
  logs/traces then become schema presets of the generic engine, not bespoke
  code. Prior art: Parquet encodings, ClickHouse codecs, DuckDB lightweight
  compression, BtrBlocks paper.
