#!/usr/bin/env bash
# End-to-end tests for the timeless_metrics vtab, driven through the
# sqlite3 CLI (the extension is a cdylib — the CLI *is* the test harness,
# same approach the Session 1 spike used).
#
# Sections:
#   1. create + insert + select, 'flush' command, shadow-table sanity
#   1b. append-only enforcement (DELETE must fail with a clear error)
#   1c. spike module regression (timeless_spike still registers and works)
#   2. name + ts range pushdown
#   3. reopen recovery (new process; ShadowTableStore.scan rebuilds index)
#   4. 'prune:<ts>' retention command
#   5. 'compact' command (chunk merge through replace_chunks)
#   6. metrics transaction rollback (R5 FIXED: buffered inserts,
#      intra-txn flush with chunk-row rollback + buffer restore,
#      auto-queue rebuild, integrity_check, reopen)
#   6b. logs transaction rollback (real auto-flush inside the txn,
#       optimize-in-txn, no dangling _terms rows)
#   6c. traces transaction rollback (auto-flush + _trace_blocks rows
#       vanish with their blocks; never-dangle through rollback)
#   7. Tier 2 batch blob ingest (format v0; blob in the hidden column)
#   8. malformed batch blobs rejected atomically (truncation, bad index)
#   9. timeless_logs round-trip (metadata + index-key columns, pre/post
#      flush exactness, optimize codec transition, reopen recovery)
#   10. logs pushdown proof (service+level constraints, _terms contents)
#   11. logs prune removes blocks AND their term rows
#   12. logs append-only enforcement + level/command validation
#   13. timeless_traces round-trip (hex + blob ids in, BLOBs out,
#       status-partitioned flush, optimize, reopen recovery)
#   14. trace_id pushdown proof (_trace_blocks contents + the planner
#       choosing the trace-index plan, visible as VIRTUAL TABLE INDEX 1)
#   15. traces status/service pushdown
#   16. traces prune removes blocks AND _terms AND _trace_blocks rows
#   17. traces append-only + kind/status/id-length validation
#   18. Prometheus text ingest (BLOB dispatch on first byte: 0x01 = batch
#       v0, reserved 0x00/0x02–0x08 = loud error, else exposition text;
#       ms timestamps normalized to EPOCH SECONDS; partial success)
#   19. plain-table oracle property test (tools/bench oracle: 3 seeds x
#       50k randomized ops, vtab results must equal a mirrored plain
#       table after every query; prints seed+op for replay on mismatch)
#   20. kill -9 crash test (tests/crash.sh: 5 random-timing kills of a
#       live ingest; integrity_check, index-join invariants, and the
#       flushed-= -durable watermark contract on reopen)
#   21. R4 shared engine: TWO connections in ONE process (python3
#       sqlite3) — flushed + buffered data visible across connections
#       without reopen, writer-gate busy timeout, retry after commit,
#       drop/recreate sanity
#   22. Q2 reduction-kernel TVFs (timeless_grid / timeless_window /
#       timeless_window_batches):
#       results identical to a plain-SQL evaluator over the raw vtab
#       (recursive-CTE grid + correlated subqueries), buffered-point
#       visibility, label filter, arg validation, TVF-first recovery
#
# NOTE on durability semantics being tested: points buffered but NOT
# flushed before the process exits are lost — that is the accepted POC
# contract, so every section flushes before relying on a reopen.
# ROLLBACK, however, is now REAL (R5 fixed): sections 6/6b/6c assert
# that rolled-back buffered writes AND rolled-back intra-txn flushes
# leave no trace, in memory or on disk.
# Section 34 covers the timeless_metrics embedding waist: durable series-id
# resolution, resolved batch v1, and matcher-aware timeless_raw reads.
# Section 35 covers the chunk-aware timeless_aggregate TVF, including direct
# SQL oracles, integer count, buffered/rollback visibility, and reopen.
# Section 36 covers the newest-first timeless_latest TVF: inclusive bounds,
# duplicate ties, matcher/empty omission, transaction and cross-connection
# visibility, compaction, retention, and reopen.
# Section 37 covers authoritative catalog-generation publication: commit,
# rollback, compact, prune, external-process invalidation, two live
# connections, and reopen.
# Section 38 covers matcher-aware catalog discovery: regex/negative/absent
# semantics, filtered label values and raw reads, rollback, two connections,
# direct-SQL errors, and reopen.
# Section 39 pins the cross-connection transaction visibility gate: an
# intra-transaction flush must report a retryable busy conflict to another
# connection, never expose an index location whose shadow row is invisible.
# Section 45 executes the public statements documented in
# docs/QUERY_SQL_EQUIVALENTS.md so the direct SQLite/libSQL recipes cannot
# silently drift from the extension.
# Section 40 makes durable metrics series IDs first-class read constraints on
# the base vtab and every per-series query TVF, including parameterized joins.
# Section 41 independently decodes TAF1/TLF1 aggregate/latest result frames and
# proves row parity, NaN/NULL handling, empty omission, and live publication.
# Section 42 covers exact native log substring search plus scalar native count:
# bounded ordering, ASCII/Unicode case folding, metadata-only fast paths,
# exact decode fallbacks, buffered rows, zero results, and work counters.
# Section 43 pins size-tiered log optimization, rewrite amplification,
# bounded optimize commands, actionable/deferred backlog, and phase telemetry.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXT="$ROOT/target/release/libtimeless_ext.so"

echo "== building extension (release) =="
cargo build -p timeless-ext --release --manifest-path "$ROOT/Cargo.toml"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
DB="$TMP/metrics_test.db"

FAILURES=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILURES=$((FAILURES + 1)); }
check_eq() { # check_eq <label> <got> <expected>
  if [[ "$2" == "$3" ]]; then
    pass "$1"
  else
    fail "$1"
    echo "--- expected ---"; printf '%s\n' "$3"
    echo "--- got ---"; printf '%s\n' "$2"
  fi
}

# ---------------------------------------------------------------------------
echo "== section 1: create, insert, select, flush, shadow tables =="
# One invocation: buffered (pre-flush) rows must already be queryable,
# then identical after flush, and the flush must land 2 chunks (one per
# series: cpu has 2 points, mem has 1) totalling 3 points.
got=$(sqlite3 "$DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
INSERT INTO metrics(name, ts, value, labels) VALUES ('cpu', 100, 1.5, '{"host":"a"}');
INSERT INTO metrics(name, ts, value, labels) VALUES ('cpu', 200, 2.5, '{"host":"a"}');
INSERT INTO metrics(name, ts, value, labels) VALUES ('mem', 150, 3.0, '{"host":"b"}');
SELECT 'pre', name, ts, value, labels FROM metrics ORDER BY ts, name;
INSERT INTO metrics(metrics) VALUES ('flush');
SELECT 'post', name, ts, value, labels FROM metrics ORDER BY ts, name;
SELECT 'chunks', COUNT(*), SUM(point_count) FROM metrics_chunks;
SELECT 'series', COUNT(*) FROM metrics_series;
SELECT 'legacy_registry', COUNT(*) FROM metrics_meta WHERE k = 'series_registry';
SQL
)
expected='pre|cpu|100|1.5|{"host":"a"}
pre|mem|150|3.0|{"host":"b"}
pre|cpu|200|2.5|{"host":"a"}
post|cpu|100|1.5|{"host":"a"}
post|mem|150|3.0|{"host":"b"}
post|cpu|200|2.5|{"host":"a"}
chunks|2|3
series|2
legacy_registry|0'
check_eq "insert/select/flush round-trip + shadow tables" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 1b: append-only enforcement =="
if err=$(sqlite3 "$DB" ".load $EXT" "DELETE FROM metrics WHERE name='cpu';" 2>&1); then
  fail "DELETE should be rejected (got success: $err)"
elif [[ "$err" == *append-only* ]]; then
  pass "DELETE rejected with append-only error"
else
  fail "DELETE rejected but with unexpected message: $err"
fi

# ---------------------------------------------------------------------------
echo "== section 1c: spike module still registered and working =="
got=$(sqlite3 "$TMP/spike.db" <<SQL
.load $EXT
CREATE VIRTUAL TABLE s USING timeless_spike;
INSERT INTO s(ts, value) VALUES (1, 2.5);
SELECT ts, value FROM s;
SQL
)
check_eq "spike vtab round-trip" "$got" "1|2.5"

# ---------------------------------------------------------------------------
echo "== section 2: name + ts range pushdown =="
# New process: also exercises xConnect recovery implicitly. BETWEEN
# becomes ts>= and ts<= constraints; name= is the equality constraint.
got=$(sqlite3 "$DB" <<SQL
.load $EXT
SELECT name, ts, value FROM metrics WHERE name = 'cpu' AND ts BETWEEN 150 AND 250;
SQL
)
check_eq "WHERE name='cpu' AND ts BETWEEN 150 AND 250" "$got" "cpu|200|2.5"

# ---------------------------------------------------------------------------
echo "== section 3: reopen recovery (flushed data survives a new process) =="
got=$(sqlite3 "$DB" <<SQL
.load $EXT
SELECT name, ts, value, labels FROM metrics ORDER BY ts, name;
SQL
)
expected='cpu|100|1.5|{"host":"a"}
mem|150|3.0|{"host":"b"}
cpu|200|2.5|{"host":"a"}'
check_eq "recovery via ShadowTableStore.scan" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 4: prune command deletes old chunks =="
# Two flushes so 'disk' gets two chunks (old + new); prune:1000000 must
# drop every chunk whose max_ts < 1000000 — that is disk-old AND the
# cpu/mem chunks from section 1 (ts 100..200). Whole-chunk deletes: this
# is the block-granular retention story from PLAN.md.
got=$(sqlite3 "$DB" <<SQL
.load $EXT
INSERT INTO metrics(name, ts, value) VALUES ('disk', 1000, 1.0);
INSERT INTO metrics(metrics) VALUES ('flush');
INSERT INTO metrics(name, ts, value) VALUES ('disk', 2000000, 2.0);
INSERT INTO metrics(metrics) VALUES ('flush');
SELECT 'before_chunks', COUNT(*) FROM metrics_chunks;
INSERT INTO metrics(metrics) VALUES ('prune:1000000');
SELECT 'after_chunks', COUNT(*) FROM metrics_chunks;
SELECT 'after_data', name, ts, value FROM metrics ORDER BY ts, name;
SQL
)
expected='before_chunks|4
after_chunks|1
after_data|disk|2000000|2.0'
check_eq "prune:1000000 removes expired chunks + rows" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 5: compact command merges chunks =="
# Two flushes give 'net' two small pco chunks; 'compact' (POC cutoff =
# i64::MAX) must merge them into one via ShadowTableStore.replace_chunks,
# with the data unchanged afterwards.
got=$(sqlite3 "$DB" <<SQL
.load $EXT
INSERT INTO metrics(name, ts, value) VALUES ('net', 3000000, 1.0);
INSERT INTO metrics(metrics) VALUES ('flush');
INSERT INTO metrics(name, ts, value) VALUES ('net', 3000010, 2.0);
INSERT INTO metrics(metrics) VALUES ('flush');
SELECT 'net_chunks_before', COUNT(*) FROM metrics_chunks WHERE ts_min >= 3000000;
INSERT INTO metrics(metrics) VALUES ('compact');
SELECT 'net_chunks_after', COUNT(*) FROM metrics_chunks WHERE ts_min >= 3000000;
SELECT 'net_data', ts, value FROM metrics WHERE name = 'net' ORDER BY ts;
SQL
)
expected='net_chunks_before|2
net_chunks_after|1
net_data|3000000|1.0
net_data|3000010|2.0'
check_eq "compact merges 2 chunks into 1, data intact" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 6: metrics transaction rollback (R5 — real semantics) =="
# PLAN.md risk R5 is FIXED: the engines keep a transaction journal
# activated by xBegin (which SQLite fires before the first write of
# EVERY transaction — one per statement in autocommit, one per explicit
# BEGIN). ROLLBACK must:
#   - discard points buffered during the txn (pre- AND post-reopen),
#   - undo intra-txn 'flush' completely: the chunk ROWS roll back with
#     the host txn, the journal removes their index entries (no
#     dangling locs), and pre-txn buffered points the flush drained are
#     RESTORED to the buffer (they came from committed statements!),
#   - leave the db bit-happy (PRAGMA integrity_check) with no orphan
#     index state (queries return exactly the pre-txn data).
RBDB="$TMP/rollback_metrics.db"
got=$(sqlite3 "$RBDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
INSERT INTO metrics(name, ts, value) VALUES ('base', 100, 1.0);
INSERT INTO metrics(metrics) VALUES ('flush');
INSERT INTO metrics(name, ts, value) VALUES ('base', 200, 2.0);
SELECT 'pre', COUNT(*) FROM metrics;
BEGIN;
INSERT INTO metrics(name, ts, value) VALUES ('rb', 300, 9.9);
SELECT 'in_txn', COUNT(*) FROM metrics;
ROLLBACK;
SELECT 'post', COUNT(*), (SELECT COUNT(*) FROM metrics WHERE name='rb') FROM metrics;
BEGIN;
INSERT INTO metrics(name, ts, value) VALUES ('rb2', 400, 4.4);
INSERT INTO metrics(metrics) VALUES ('flush');
SELECT 'chunks_in_txn', COUNT(*) FROM metrics_chunks;
ROLLBACK;
SELECT 'chunks_post', COUNT(*) FROM metrics_chunks;
SELECT 'rows_post', name, ts, value FROM metrics ORDER BY ts;
BEGIN;
INSERT INTO metrics(name, ts, value) SELECT 'big', 1000 + value, 0.5 FROM generate_series(1, 5000);
ROLLBACK;
SELECT 'big_post', COUNT(*) FROM metrics WHERE name = 'big';
PRAGMA integrity_check;
INSERT INTO metrics(metrics) VALUES ('flush');
SQL
)
# pre: base@100 (flushed) + base@200 (buffered) = 2.
# chunks_in_txn: baseline chunk + intra-txn flush of base@200 and
# rb2@400 (one chunk per series) = 3; back to 1 after ROLLBACK.
# rows_post: base@100 from the chunk, base@200 RESTORED to the buffer.
# big: 5000 points cross the 4096 auto-queue threshold inside the txn —
# all gone after ROLLBACK (and the flush queue must be rebuilt, which
# the final committed 'flush' exercises).
expected='pre|2
in_txn|3
post|2|0
chunks_in_txn|3
chunks_post|1
rows_post|base|100|1.0
rows_post|base|200|2.0
big_post|0
ok'
check_eq "metrics rollback: buffered + intra-txn flush + auto-queue" "$got" "$expected"

# Reopen in a NEW process: rolled-back data must not resurface, the
# restored-and-then-flushed base@200 must be durable.
got=$(sqlite3 "$RBDB" <<SQL
.load $EXT
SELECT COUNT(*), (SELECT COUNT(*) FROM metrics WHERE name IN ('rb','rb2','big')) FROM metrics;
PRAGMA integrity_check;
SQL
)
check_eq "metrics rollback state survives reopen" "$got" "2|0
ok"

# ---------------------------------------------------------------------------
echo "== section 6b: logs transaction rollback (incl. real auto-flush) =="
# The logs engine AUTO-FLUSHES inside push() at 8192 buffered entries,
# so a big INSERT...SELECT inside an explicit txn writes real block +
# term rows mid-transaction. ROLLBACK must remove them (rows roll back,
# journal drops the index entries), restore the pre-txn buffered entry
# the auto-flush drained, and leave zero orphan index state.
RLDB="$TMP/rollback_logs.db"
got=$(sqlite3 "$RLDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');
INSERT INTO logs(ts, level, message, service) VALUES (1000, 'info', 'keep-flushed', 'api');
INSERT INTO logs(logs) VALUES ('flush');
INSERT INTO logs(ts, level, message, service) VALUES (2000, 'error', 'keep-buffered', 'web');
SELECT 'pre', COUNT(*), (SELECT COUNT(*) FROM logs_blocks), (SELECT COUNT(*) FROM logs_terms) FROM logs;
BEGIN;
INSERT INTO logs(ts, level, message) SELECT 10000 + value, 'info', 'bulk-' || value FROM generate_series(1, 9000);
SELECT 'in_txn', (SELECT COUNT(*) FROM logs_blocks) > 1, COUNT(*) FROM logs;
ROLLBACK;
SELECT 'post', COUNT(*), (SELECT COUNT(*) FROM logs_blocks), (SELECT COUNT(*) FROM logs_terms) FROM logs;
SELECT 'rows', ts, level, message FROM logs ORDER BY ts;
BEGIN;
INSERT INTO logs(logs) VALUES ('flush');
INSERT INTO logs(logs) VALUES ('optimize');
SELECT 'opt_in_txn', COUNT(*) FILTER (WHERE codec != 1) FROM logs_blocks;
ROLLBACK;
SELECT 'opt_post', (SELECT COUNT(*) FROM logs_blocks), (SELECT COUNT(*) FILTER (WHERE codec = 1) FROM logs_blocks), (SELECT COUNT(*) FROM logs_terms);
SELECT 'final', COUNT(*) FROM logs;
PRAGMA integrity_check;
INSERT INTO logs(logs) VALUES ('flush');
SQL
)
# pre: 2 rows; 1 raw block (info: level:info + service:api = 2 terms).
# in_txn: at 8192 buffered the auto-flush fired (blocks > 1 → 1) and
# all 9002 rows are visible (blocks + remaining buffer).
# post: blocks/terms back to 1/2, keep-buffered RESTORED to the buffer.
# opt txn: flush + optimize inside one rolled-back txn (add-then-remove
# journal dedup): everything back to the single raw pre-txn block.
expected='pre|2|1|2
in_txn|1|9002
post|2|1|2
rows|1000|info|keep-flushed
rows|2000|error|keep-buffered
opt_in_txn|2
opt_post|1|1|2
final|2
ok'
check_eq "logs rollback: auto-flush + optimize in txn fully undone" "$got" "$expected"

got=$(sqlite3 "$RLDB" <<SQL
.load $EXT
SELECT COUNT(*), (SELECT COUNT(*) FROM logs WHERE message LIKE 'bulk-%') FROM logs;
SELECT t.term FROM logs_terms t LEFT JOIN logs_blocks b ON t.block_id = b.id WHERE b.id IS NULL;
PRAGMA integrity_check;
SQL
)
check_eq "logs rollback state survives reopen, no dangling terms" "$got" "2|0
ok"

# ---------------------------------------------------------------------------
echo "== section 6c: traces transaction rollback (incl. _trace_blocks) =="
# Same story as logs plus the trace index: rows in _trace_blocks are
# created in the same operation as their blocks, so ROLLBACK must take
# them away together — never-dangle holds THROUGH rollback.
RTDB="$TMP/rollback_traces.db"
got=$(sqlite3 "$RTDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE traces USING timeless_traces;
INSERT INTO traces(trace_id, span_id, name, service, status, start_ts) VALUES (x'11111111111111111111111111111111', x'0000000000000001', 'keep-flushed', 'api', 'ok', 1000);
INSERT INTO traces(traces) VALUES ('flush');
INSERT INTO traces(trace_id, span_id, name, service, status, start_ts) VALUES (x'22222222222222222222222222222222', x'0000000000000002', 'keep-buffered', 'web', 'error', 2000);
SELECT 'pre', COUNT(*), (SELECT COUNT(*) FROM traces_blocks), (SELECT COUNT(*) FROM traces_terms), (SELECT COUNT(*) FROM traces_trace_blocks) FROM traces;
BEGIN;
INSERT INTO traces(trace_id, span_id, name, service, start_ts) SELECT randomblob(16), randomblob(8), 'bulk', 'svc', 10000 + value FROM generate_series(1, 9000);
SELECT 'in_txn', (SELECT COUNT(*) FROM traces_blocks) > 1, (SELECT COUNT(*) FROM traces_trace_blocks) > 1, COUNT(*) FROM traces;
ROLLBACK;
SELECT 'post', COUNT(*), (SELECT COUNT(*) FROM traces_blocks), (SELECT COUNT(*) FROM traces_terms), (SELECT COUNT(*) FROM traces_trace_blocks) FROM traces;
SELECT 'rows', name, status, start_ts FROM traces ORDER BY start_ts;
PRAGMA integrity_check;
INSERT INTO traces(traces) VALUES ('flush');
SQL
)
# pre: 2 spans; 1 ok-pure raw block (4 terms: kind/name/service/status)
# + 1 trace row. in_txn: auto-flush at 8192 wrote blocks + trace rows.
# post: everything back — 1 block / 4 terms / 1 trace row — and the
# pre-txn buffered error span RESTORED.
expected='pre|2|1|6|1
in_txn|1|1|9002
post|2|1|6|1
rows|keep-flushed|ok|1000
rows|keep-buffered|error|2000
ok'
check_eq "traces rollback: auto-flush + trace-index rows fully undone" "$got" "$expected"

got=$(sqlite3 "$RTDB" <<SQL
.load $EXT
SELECT COUNT(*), (SELECT COUNT(*) FROM traces WHERE name = 'bulk') FROM traces;
SELECT hex(tb.trace_id) FROM traces_trace_blocks tb LEFT JOIN traces_blocks b ON tb.block_id = b.id WHERE b.id IS NULL;
SELECT t.term FROM traces_terms t LEFT JOIN traces_blocks b ON t.block_id = b.id WHERE b.id IS NULL;
PRAGMA integrity_check;
SQL
)
check_eq "traces rollback state survives reopen, no dangling index rows" "$got" "2|0
ok"

# ---------------------------------------------------------------------------
echo "== section 7: Tier 2 batch blob ingest (format v0) =="
# The hidden command column is overloaded by TYPE: TEXT = command, BLOB =
# batch-blob-v0 ingest. Build a tiny 3-point blob with python3 (struct
# packs little-endian with '<'), feed it through readfile(), and verify:
#  - last_insert_rowid() reports the point count (3),
#  - points are queryable IMMEDIATELY (same buffers as Tier 1 — ingest
#    does NOT flush; durability contract is identical across tiers),
#  - after an explicit 'flush' the same rows come back from chunks.
# Series table: cpu with labels, mem with labels_len=0 (= no labels, '{}').
BLOB="$TMP/batch_v0.blob"
python3 - "$BLOB" <<'PY'
import struct, sys
names = [(b'cpu', b'{"host":"a"}'), (b'mem', b'')]
hdr = struct.pack('<BBHII', 1, 0, 0, len(names), 3)   # ver, flags, rsvd, n_series, n_points
series = b''.join(struct.pack('<I', len(n)) + n +
                  struct.pack('<I', len(l)) + l for n, l in names)
idx  = struct.pack('<3I', 0, 0, 1)                    # cpu, cpu, mem
ts   = struct.pack('<3q', 100, 200, 150)
vals = struct.pack('<3d', 1.5, 2.5, 3.25)
open(sys.argv[1], 'wb').write(hdr + series + idx + ts + vals)
PY
T2DB="$TMP/tier2_test.db"
got=$(sqlite3 "$T2DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
INSERT INTO metrics(metrics) VALUES (readfile('$BLOB'));
SELECT 'ingested', last_insert_rowid();
SELECT 'pre', name, ts, value, labels FROM metrics ORDER BY ts, name;
INSERT INTO metrics(metrics) VALUES ('flush');
SELECT 'post', name, ts, value, labels FROM metrics ORDER BY ts, name;
SELECT 'chunks', COUNT(*), SUM(point_count) FROM metrics_chunks;
SQL
)
expected='ingested|3
pre|cpu|100|1.5|{"host":"a"}
pre|mem|150|3.25|{}
pre|cpu|200|2.5|{"host":"a"}
post|cpu|100|1.5|{"host":"a"}
post|mem|150|3.25|{}
post|cpu|200|2.5|{"host":"a"}
chunks|2|3'
check_eq "Tier 2 blob ingest: exact 3-point round-trip" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 8: malformed batch blobs are rejected atomically =="
# The decoder validates the ENTIRE blob (header, series table, column
# lengths, every series index) before writing a single point — a bad
# batch is a hard error and the table must be unchanged afterwards.
BADDB="$TMP/tier2_bad.db"
sqlite3 "$BADDB" ".load $EXT" "CREATE VIRTUAL TABLE metrics USING timeless_metrics;"

# 8a: truncated blob (drop the last 4 bytes of the value column)
python3 - "$BLOB" "$TMP/batch_trunc.blob" <<'PY'
import sys
b = open(sys.argv[1], 'rb').read()
open(sys.argv[2], 'wb').write(b[:-4])
PY
if err=$(sqlite3 "$BADDB" ".load $EXT" \
    "INSERT INTO metrics(metrics) VALUES (readfile('$TMP/batch_trunc.blob'));" 2>&1); then
  fail "truncated blob should be rejected (got success: $err)"
elif [[ "$err" == *truncated* ]]; then
  pass "truncated blob rejected with a truncation error"
else
  fail "truncated blob rejected but with unexpected message: $err"
fi

# 8b: out-of-range series index (1-entry series table, point says index 5)
python3 - "$TMP/batch_oob.blob" <<'PY'
import struct, sys
hdr = struct.pack('<BBHII', 1, 0, 0, 1, 1)
series = struct.pack('<I', 3) + b'cpu' + struct.pack('<I', 0)
body = struct.pack('<I', 5) + struct.pack('<q', 1) + struct.pack('<d', 1.0)
open(sys.argv[1], 'wb').write(hdr + series + body)
PY
if err=$(sqlite3 "$BADDB" ".load $EXT" \
    "INSERT INTO metrics(metrics) VALUES (readfile('$TMP/batch_oob.blob'));" 2>&1); then
  fail "out-of-range series index should be rejected (got success: $err)"
elif [[ "$err" == *"out of range"* ]]; then
  pass "out-of-range series index rejected"
else
  fail "out-of-range index rejected but with unexpected message: $err"
fi

# Nothing from either bad batch may have been stored (flush would persist
# any strays, so flush first, then count).
got=$(sqlite3 "$BADDB" ".load $EXT" \
  "INSERT INTO metrics(metrics) VALUES ('flush'); SELECT COUNT(*) FROM metrics;")
check_eq "malformed batches stored nothing" "$got" "0"

# ---------------------------------------------------------------------------
echo "== section 9: timeless_logs round-trip =="
# Fresh db. Covers: index_keys creation arg; metadata as flat JSON; the
# index-key hidden columns as INSERT shorthand (service='web' merges
# into metadata); canonical sorted-JSON metadata output; queryable
# before AND after flush; 'optimize' transitions codec 1 (raw) -> 5
# (adaptive columnar v2 with per-key shredded metadata — the Session 8
# winner; codecs 2 and 4 are legacy formats, still decodable but no
# longer written) with identical rows; SELECT of a hidden index-key
# column surfaces the value from metadata.
LOGDB="$TMP/logs_test.db"
got=$(sqlite3 "$LOGDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,path,status');
INSERT INTO logs(ts, level, message, metadata) VALUES (1000, 'info', 'req done', '{"service":"api","path":"/checkout","status":"200"}');
INSERT INTO logs(ts, level, message, metadata, service) VALUES (2000, 'error', 'boom', '{"path":"/pay"}', 'web');
INSERT INTO logs(ts, level, message) VALUES (1500, 'debug', 'noise');
SELECT 'pre', ts, level, message, metadata FROM logs ORDER BY ts;
INSERT INTO logs(logs) VALUES ('flush');
SELECT 'post', ts, level, message, metadata FROM logs ORDER BY ts;
SELECT 'raw_blocks', COUNT(*) FROM logs_blocks WHERE codec = 1;
INSERT INTO logs(logs) VALUES ('optimize');
SELECT 'codecs', COUNT(*) FILTER (WHERE codec = 1), COUNT(*) FILTER (WHERE codec = 5) FROM logs_blocks;
SELECT 'opt', ts, level, message, metadata FROM logs ORDER BY ts;
SELECT 'svc', ts, COALESCE(service, '-') FROM logs ORDER BY ts;
SQL
)
# Block counts: flush is LEVEL-PARTITIONED (level-term weakness fix) —
# the 3 buffered entries span 3 levels (info/debug/error), so flush
# writes 3 level-pure raw blocks, and optimize compacts each level
# partition separately (never merging across levels): 3 raw -> 3
# codec-5 (adaptive columnar v2, shredded metadata) blocks.
expected='pre|1000|info|req done|{"path":"/checkout","service":"api","status":"200"}
pre|1500|debug|noise|{}
pre|2000|error|boom|{"path":"/pay","service":"web"}
post|1000|info|req done|{"path":"/checkout","service":"api","status":"200"}
post|1500|debug|noise|{}
post|2000|error|boom|{"path":"/pay","service":"web"}
raw_blocks|3
codecs|0|3
opt|1000|info|req done|{"path":"/checkout","service":"api","status":"200"}
opt|1500|debug|noise|{}
opt|2000|error|boom|{"path":"/pay","service":"web"}
svc|1000|api
svc|1500|-
svc|2000|web'
check_eq "logs insert/flush/optimize round-trip" "$got" "$expected"

# Reopen in a NEW process: xConnect must recover the block index via
# scan() and index_keys from _meta (NOT from creation args).
got=$(sqlite3 "$LOGDB" <<SQL
.load $EXT
SELECT ts, level, message, metadata FROM logs ORDER BY ts;
SELECT 'svc2000', service FROM logs WHERE ts = 2000;
SQL
)
expected='1000|info|req done|{"path":"/checkout","service":"api","status":"200"}
1500|debug|noise|{}
2000|error|boom|{"path":"/pay","service":"web"}
svc2000|web'
check_eq "logs reopen recovery (scan + index_keys from _meta)" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 10: logs pushdown proof (terms + hidden-column equality) =="
# The _terms posting list must contain level: terms plus terms for the
# allowlisted keys ONLY (selective indexing), and WHERE service='api'
# + level filters must return exactly the matching rows. message LIKE
# stays a SQLite-side filter but must still be correct.
got=$(sqlite3 "$LOGDB" <<SQL
.load $EXT
SELECT 'term_svc_api', COUNT(*) FROM logs_terms WHERE term = 'service:api';
SELECT 'term_lvl_err', COUNT(*) FROM logs_terms WHERE term = 'level:error';
SELECT 'term_status', COUNT(*) FROM logs_terms WHERE term = 'status:200';
SELECT 'q_svc', ts, level, message FROM logs WHERE service = 'api';
SELECT 'q_svc_lvl', ts, message FROM logs WHERE service = 'web' AND level = 'error';
SELECT 'q_lvl_range', ts, message FROM logs WHERE level = 'error' AND ts >= 1500 AND ts <= 2500;
SELECT 'q_none', COUNT(*) FROM logs WHERE service = 'nope';
SELECT 'q_like', COUNT(*) FROM logs WHERE message LIKE '%boo%';
SQL
)
expected='term_svc_api|1
term_lvl_err|1
term_status|1
q_svc|1000|info|req done
q_svc_lvl|2000|boom
q_lvl_range|2000|boom
q_none|0
q_like|1'
check_eq "service/level/ts pushdown + LIKE above the vtab" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 10b: logs bounded ORDER BY/LIMIT/OFFSET pushdown =="
# Two overlapping flushed ranges, duplicate timestamps, and a buffered tail.
# The bounded path must return the same exact window while retaining only
# LIMIT+OFFSET rows. Equal timestamps use the released product's canonical
# message/severity/metadata comparator regardless of block/insertion order.
# LIKE and strict bounds remain on the conservative unbounded path because
# SQLite still performs those exact checks.
BOUNDEDLOGDB="$TMP/logs_bounded.db"
got=$(sqlite3 "$BOUNDEDLOGDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');
INSERT INTO logs(ts,level,message,service) VALUES
  (10,'info','b0-10','api'),(30,'info','b0-30-a','api'),(30,'info','b0-30-b','web');
INSERT INTO logs(logs) VALUES('flush');
INSERT INTO logs(ts,level,message,service) VALUES
  (5,'info','b1-05','api'),(20,'info','b1-20','web'),
  (30,'info','b1-30','api'),(40,'info','b1-40','api');
INSERT INTO logs(logs) VALUES('flush');
INSERT INTO logs(ts,level,message,service) VALUES
  (0,'info','buf-00','api'),(30,'info','buf-30','api'),(50,'info','buf-50','web');
SELECT 'desc', ts, message FROM logs ORDER BY ts DESC LIMIT 3 OFFSET 2;
SELECT 'asc', ts, message FROM logs ORDER BY ts ASC LIMIT 3 OFFSET 1;
SELECT 'api', ts, message FROM logs WHERE service='api' ORDER BY ts ASC LIMIT 3 OFFSET 1;
SELECT 'like', ts, message FROM logs WHERE message LIKE '%30%' ORDER BY ts DESC LIMIT 2;
SELECT 'strict', ts, message FROM logs WHERE ts > 30 ORDER BY ts ASC LIMIT 2;
SELECT 'profile',
  (SELECT value FROM timeless_stats('logs') WHERE key='query_bounded_count'),
  (SELECT value FROM timeless_stats('logs') WHERE key='query_bounded_requested_entries'),
  (SELECT value FROM timeless_stats('logs') WHERE key='query_bounded_max_entries');
SQL
)
expected='desc|30|b0-30-a
desc|30|b0-30-b
desc|30|b1-30
asc|5|b1-05
asc|10|b0-10
asc|20|b1-20
api|5|b1-05
api|10|b0-10
api|30|b0-30-a
like|30|b0-30-a
like|30|b0-30-b
strict|40|b1-40
strict|50|buf-50
profile|3|13|5'
check_eq "bounded log windows exact; unsafe rechecks stay unbounded" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 11: logs prune removes blocks AND their term rows =="
# Fresh db, two flushes -> two blocks with disjoint ts ranges. Pruning
# between them must delete the old block and its posting-list rows in
# the same operation (posting lists never dangle — PLAN.md rule).
PRUNEDB="$TMP/logs_prune.db"
got=$(sqlite3 "$PRUNEDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');
INSERT INTO logs(ts, level, message, service) VALUES (1000, 'info', 'old-1', 'api');
INSERT INTO logs(ts, level, message, service) VALUES (2000, 'warning', 'old-2', 'web');
INSERT INTO logs(logs) VALUES ('flush');
INSERT INTO logs(ts, level, message, service) VALUES (9000000, 'info', 'new-1', 'api');
INSERT INTO logs(logs) VALUES ('flush');
SELECT 'before', (SELECT COUNT(*) FROM logs_blocks), (SELECT COUNT(*) FROM logs_terms);
INSERT INTO logs(logs) VALUES ('prune:1000000');
SELECT 'after', (SELECT COUNT(*) FROM logs_blocks), (SELECT COUNT(*) FROM logs_terms);
SELECT 'rows', ts, message FROM logs ORDER BY ts;
SQL
)
# before (level-partitioned flush): the first flush spans two levels so
# it writes TWO pure blocks — info block terms = level:info, service:api
# (2) + warning block terms = level:warning, service:web (2); the second
# flush is info-only -> one block, terms = level:info, service:api (2).
# 3 blocks / 6 term rows total (same 6 terms as the pre-partition layout,
# distributed over more, purer blocks).
# after: both old blocks (ts < 1000000) pruned with their term rows;
# only the new block's 2 terms may remain.
expected='before|3|6
after|1|2
rows|9000000|new-1'
check_eq "prune drops expired blocks + their term rows" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 12: logs append-only + validation =="
if err=$(sqlite3 "$LOGDB" ".load $EXT" "DELETE FROM logs WHERE ts = 1000;" 2>&1); then
  fail "logs DELETE should be rejected (got success: $err)"
elif [[ "$err" == *append-only* ]]; then
  pass "logs DELETE rejected with append-only error"
else
  fail "logs DELETE rejected but with unexpected message: $err"
fi

if err=$(sqlite3 "$LOGDB" ".load $EXT" "UPDATE logs SET message = 'x' WHERE ts = 1000;" 2>&1); then
  fail "logs UPDATE should be rejected (got success: $err)"
elif [[ "$err" == *append-only* ]]; then
  pass "logs UPDATE rejected with append-only error"
else
  fail "logs UPDATE rejected but with unexpected message: $err"
fi

# Unknown level names must be rejected loudly (0=debug..3=error only).
if err=$(sqlite3 "$LOGDB" ".load $EXT" \
    "INSERT INTO logs(ts, level, message) VALUES (1, 'fatal', 'x');" 2>&1); then
  fail "level 'fatal' should be rejected (got success: $err)"
elif [[ "$err" == *"unknown log level"* ]]; then
  pass "unknown level rejected with a clear error"
else
  fail "unknown level rejected but with unexpected message: $err"
fi

# Unknown commands too.
if err=$(sqlite3 "$LOGDB" ".load $EXT" \
    "INSERT INTO logs(logs) VALUES ('defrag');" 2>&1); then
  fail "unknown command should be rejected (got success: $err)"
elif [[ "$err" == *"unknown command"* ]]; then
  pass "unknown command rejected with a clear error"
else
  fail "unknown command rejected but with unexpected message: $err"
fi

# ---------------------------------------------------------------------------
echo "== section 13: timeless_traces round-trip =="
# Fresh db. Covers: hex-TEXT ids and BLOB ids both accepted on INSERT
# (ids are ALWAYS returned as BLOBs — hex() for display); kind/status
# TEXT vocabularies; NULL parent (root span); NULL kind/status take the
# OTel defaults (internal/unset); canonical sorted-JSON attributes;
# queryable before AND after flush; STATUS-partitioned flush (3
# statuses buffered -> 3 status-pure raw blocks); 'optimize'
# transitions codec 1 -> 5 (adaptive columnar v2, shredded attributes;
# codecs 2/4 = legacy, still decodable) per partition with identical
# rows.
TRACEDB="$TMP/traces_test.db"
got=$(sqlite3 "$TRACEDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE traces USING timeless_traces;
INSERT INTO traces(trace_id, span_id, parent_span_id, name, service, kind, status, start_ts, duration_ns, attributes)
  VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '1111111111111111', NULL, 'GET /checkout', 'api', 'server', 'ok', 1000, 5000, '{"http.status":"200","http.method":"GET"}');
INSERT INTO traces(trace_id, span_id, parent_span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES (x'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', x'2222222222222222', x'1111111111111111', 'db.query', 'db', 'client', 'error', 2000, 700);
INSERT INTO traces(trace_id, span_id, name, service, start_ts)
  VALUES (x'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', x'3333333333333333', 'cache.get', 'cache', 1500);
SELECT 'pre', hex(trace_id), hex(span_id), CASE WHEN parent_span_id IS NULL THEN '-' ELSE hex(parent_span_id) END, name, service, kind, status, start_ts, duration_ns, attributes FROM traces ORDER BY start_ts;
INSERT INTO traces(traces) VALUES ('flush');
SELECT 'post', hex(trace_id), hex(span_id), CASE WHEN parent_span_id IS NULL THEN '-' ELSE hex(parent_span_id) END, name, service, kind, status, start_ts, duration_ns, attributes FROM traces ORDER BY start_ts;
SELECT 'raw_blocks', COUNT(*) FROM traces_blocks WHERE codec = 1;
SELECT 'ts_unit', v FROM traces_meta WHERE k = 'ts_unit';
INSERT INTO traces(traces) VALUES ('optimize:8192');
SELECT 'codecs', COUNT(*) FILTER (WHERE codec = 1), COUNT(*) FILTER (WHERE codec = 5) FROM traces_blocks;
SELECT 'maint',
       (SELECT value FROM timeless_stats('traces') WHERE key='optimize_budgeted_count'),
       (SELECT value FROM timeless_stats('traces') WHERE key='optimize_budget_entries'),
       (SELECT value FROM timeless_stats('traces') WHERE key='optimize_raw_blocks'),
       (SELECT value FROM timeless_stats('traces') WHERE key='optimize_raw_entries'),
       (SELECT value FROM timeless_stats('traces') WHERE key='optimize_pending_raw_entries'),
       (SELECT value FROM timeless_stats('traces') WHERE key='optimize_merge_deferred_entries');
SELECT 'opt', hex(trace_id), name, kind, status FROM traces ORDER BY start_ts;
SQL
)
# Block counts: the 3 buffered spans span 3 statuses (ok/unset/error),
# so the status-partitioned flush writes 3 status-pure raw blocks and
# optimize compacts each partition separately: 3 raw -> 3 codec-5
# (adaptive columnar v2, shredded attributes) blocks.
expected='pre|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|1111111111111111|-|GET /checkout|api|server|ok|1000|5000|{"http.method":"GET","http.status":"200"}
pre|BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB|3333333333333333|-|cache.get|cache|internal|unset|1500|0|{}
pre|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|2222222222222222|1111111111111111|db.query|db|client|error|2000|700|{}
post|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|1111111111111111|-|GET /checkout|api|server|ok|1000|5000|{"http.method":"GET","http.status":"200"}
post|BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB|3333333333333333|-|cache.get|cache|internal|unset|1500|0|{}
post|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|2222222222222222|1111111111111111|db.query|db|client|error|2000|700|{}
raw_blocks|3
ts_unit|ns
codecs|0|3
maint|1|8192|3|3|0|3
opt|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|GET /checkout|server|ok
opt|BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB|cache.get|internal|unset
opt|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|db.query|client|error'
check_eq "traces insert/flush/optimize round-trip (hex + blob ids)" "$got" "$expected"

# Reopen in a NEW process: xConnect recovers the block index via scan()
# and status partitions via the status: posting lists.
got=$(sqlite3 "$TRACEDB" <<SQL
.load $EXT
SELECT hex(trace_id), name, status, start_ts FROM traces ORDER BY start_ts;
SELECT 'by_trace', name FROM traces WHERE trace_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' ORDER BY start_ts;
SQL
)
expected='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|GET /checkout|ok|1000
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB|cache.get|unset|1500
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|db.query|error|2000
by_trace|GET /checkout
by_trace|db.query'
check_eq "traces reopen recovery (scan + partition re-derivation)" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 14: trace_id pushdown proof =="
# Two proofs:
#  a) the _trace_blocks index holds PACKED 16-byte rows (dedup per
#     block: trace A has spans in 2 blocks -> 2 rows, trace B in 1);
#  b) the PLANNER picks the trace plan: best_index claims trace_id
#     equality as idx_num bit 1 with cost ~10, which EXPLAIN QUERY PLAN
#     prints as "VIRTUAL TABLE INDEX 1:". A hex-TEXT trace_id works in
#     WHERE too (the filter parses both forms).
got=$(sqlite3 "$TRACEDB" <<SQL
.load $EXT
SELECT 'rows', COUNT(*), SUM(LENGTH(trace_id)) FROM traces_trace_blocks;
SELECT 'trace_a', COUNT(*) FROM traces_trace_blocks WHERE trace_id = x'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
SELECT 'q_blob', name FROM traces WHERE trace_id = x'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ORDER BY start_ts;
SELECT 'q_hex', COUNT(*) FROM traces WHERE trace_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
SELECT 'q_miss', COUNT(*) FROM traces WHERE trace_id = x'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC';
SQL
)
# 3 status-pure blocks: trace A is in the ok block AND the error block
# (2 rows), trace B in the unset block (1 row) -> 3 rows, 16 bytes each.
expected='rows|3|48
trace_a|2
q_blob|GET /checkout
q_blob|db.query
q_hex|2
q_miss|0'
check_eq "_trace_blocks packed rows + trace_id lookups (blob + hex)" "$got" "$expected"

plan=$(sqlite3 "$TRACEDB" ".load $EXT" \
  "EXPLAIN QUERY PLAN SELECT * FROM traces WHERE trace_id = x'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';")
if [[ "$plan" == *"VIRTUAL TABLE INDEX 1:"* ]]; then
  pass "planner chose the trace-index plan (idx_num 1)"
else
  fail "unexpected query plan for trace_id equality: $plan"
fi

# ---------------------------------------------------------------------------
echo "== section 15: traces status/service pushdown =="
# The _terms posting list must carry all four term families, and
# status/service/kind/name equality + ts range must return exactly the
# matching spans (posting-list intersection happens in SQL; SQLite
# re-checks above us).
got=$(sqlite3 "$TRACEDB" <<SQL
.load $EXT
SELECT 'term_status_err', COUNT(*) FROM traces_terms WHERE term = 'status:error';
SELECT 'term_svc_api', COUNT(*) FROM traces_terms WHERE term = 'service:api';
SELECT 'term_kind_server', COUNT(*) FROM traces_terms WHERE term = 'kind:server';
SELECT 'term_name', COUNT(*) FROM traces_terms WHERE term = 'name:db.query';
SELECT 'q_status', name FROM traces WHERE status = 'error';
SELECT 'q_svc', name FROM traces WHERE service = 'api';
SELECT 'q_kind', name FROM traces WHERE kind = 'client';
SELECT 'q_name', service FROM traces WHERE name = 'cache.get';
SELECT 'q_combo', COUNT(*) FROM traces WHERE service = 'db' AND status = 'error' AND start_ts >= 1500 AND start_ts <= 2500;
SELECT 'q_none', COUNT(*) FROM traces WHERE service = 'nope';
SQL
)
expected='term_status_err|1
term_svc_api|1
term_kind_server|1
term_name|1
q_status|db.query
q_svc|GET /checkout
q_kind|db.query
q_name|cache
q_combo|1
q_none|0'
check_eq "traces status/service/kind/name pushdown" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 16: traces prune removes blocks + terms + trace rows =="
# Fresh db, two flushes -> blocks with disjoint ts ranges. Pruning
# between them must delete the old blocks and BOTH kinds of index rows
# in the same operation (posting lists AND the trace index never
# dangle — the PLAN.md rule extended to _trace_blocks).
TPRUNEDB="$TMP/traces_prune.db"
got=$(sqlite3 "$TPRUNEDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE traces USING timeless_traces;
INSERT INTO traces(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES (x'11111111111111111111111111111111', x'0000000000000001', 'old-op', 'api', 'server', 'ok', 1000, 10);
INSERT INTO traces(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES (x'22222222222222222222222222222222', x'0000000000000002', 'old-op', 'web', 'server', 'error', 2000, 10);
INSERT INTO traces(traces) VALUES ('flush');
INSERT INTO traces(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES (x'33333333333333333333333333333333', x'0000000000000003', 'new-op', 'api', 'server', 'ok', 9000000, 10);
INSERT INTO traces(traces) VALUES ('flush');
SELECT 'before', (SELECT COUNT(*) FROM traces_blocks), (SELECT COUNT(*) FROM traces_terms), (SELECT COUNT(*) FROM traces_trace_blocks);
INSERT INTO traces(traces) VALUES ('prune:1000000');
SELECT 'after', (SELECT COUNT(*) FROM traces_blocks), (SELECT COUNT(*) FROM traces_terms), (SELECT COUNT(*) FROM traces_trace_blocks);
SELECT 'rows', hex(trace_id), name FROM traces ORDER BY start_ts;
SELECT 'gone', COUNT(*) FROM traces WHERE trace_id = x'11111111111111111111111111111111';
SQL
)
# before: flush 1 spans two statuses -> 2 pure blocks (ok: 4 terms
# kind/name/service/status, error: 4 terms) + flush 2 -> 1 block
# (4 terms) = 3 blocks / 12 term rows / 3 trace rows.
# after: both old blocks pruned with ALL their index rows; the new
# block keeps 4 terms + 1 trace row.
expected='before|3|18|3
after|1|6|1
rows|33333333333333333333333333333333|new-op
gone|0'
check_eq "traces prune drops blocks + terms + trace-index rows" "$got" "$expected"

# ---------------------------------------------------------------------------
echo "== section 17: traces append-only + validation =="
if err=$(sqlite3 "$TRACEDB" ".load $EXT" "DELETE FROM traces WHERE service = 'api';" 2>&1); then
  fail "traces DELETE should be rejected (got success: $err)"
elif [[ "$err" == *append-only* ]]; then
  pass "traces DELETE rejected with append-only error"
else
  fail "traces DELETE rejected but with unexpected message: $err"
fi

if err=$(sqlite3 "$TRACEDB" ".load $EXT" "UPDATE traces SET name = 'x' WHERE start_ts = 1000;" 2>&1); then
  fail "traces UPDATE should be rejected (got success: $err)"
elif [[ "$err" == *append-only* ]]; then
  pass "traces UPDATE rejected with append-only error"
else
  fail "traces UPDATE rejected but with unexpected message: $err"
fi

# Unknown kind/status vocabularies rejected loudly.
if err=$(sqlite3 "$TRACEDB" ".load $EXT" \
    "INSERT INTO traces(trace_id, span_id, name, service, kind, start_ts) VALUES (x'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD', x'0000000000000004', 'op', 's', 'span', 1);" 2>&1); then
  fail "kind 'span' should be rejected (got success: $err)"
elif [[ "$err" == *"unknown span kind"* ]]; then
  pass "unknown kind rejected with a clear error"
else
  fail "unknown kind rejected but with unexpected message: $err"
fi

if err=$(sqlite3 "$TRACEDB" ".load $EXT" \
    "INSERT INTO traces(trace_id, span_id, name, service, status, start_ts) VALUES (x'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD', x'0000000000000004', 'op', 's', 'failed', 1);" 2>&1); then
  fail "status 'failed' should be rejected (got success: $err)"
elif [[ "$err" == *"unknown span status"* ]]; then
  pass "unknown status rejected with a clear error"
else
  fail "unknown status rejected but with unexpected message: $err"
fi

# Wrong id lengths: 15-byte blob and odd-length hex both rejected.
if err=$(sqlite3 "$TRACEDB" ".load $EXT" \
    "INSERT INTO traces(trace_id, span_id, name, service, start_ts) VALUES (x'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDD', x'0000000000000004', 'op', 's', 1);" 2>&1); then
  fail "15-byte trace_id should be rejected (got success: $err)"
elif [[ "$err" == *"expected exactly 16"* ]]; then
  pass "15-byte trace_id BLOB rejected"
else
  fail "short trace_id rejected but with unexpected message: $err"
fi

if err=$(sqlite3 "$TRACEDB" ".load $EXT" \
    "INSERT INTO traces(trace_id, span_id, name, service, start_ts) VALUES ('abc', x'0000000000000004', 'op', 's', 1);" 2>&1); then
  fail "3-char hex trace_id should be rejected (got success: $err)"
elif [[ "$err" == *"not a 32-char hex string"* ]]; then
  pass "short hex trace_id TEXT rejected"
else
  fail "short hex trace_id rejected but with unexpected message: $err"
fi

if err=$(sqlite3 "$TRACEDB" ".load $EXT" \
    "INSERT INTO traces(trace_id, span_id, name, service, start_ts) VALUES (x'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD', x'00000000000000', 'op', 's', 1);" 2>&1); then
  fail "7-byte span_id should be rejected (got success: $err)"
elif [[ "$err" == *"expected exactly 8"* ]]; then
  pass "7-byte span_id BLOB rejected"
else
  fail "short span_id rejected but with unexpected message: $err"
fi

# Unknown commands too.
if err=$(sqlite3 "$TRACEDB" ".load $EXT" \
    "INSERT INTO traces(traces) VALUES ('defrag');" 2>&1); then
  fail "unknown command should be rejected (got success: $err)"
elif [[ "$err" == *"unknown command"* ]]; then
  pass "unknown command rejected with a clear error"
else
  fail "unknown command rejected but with unexpected message: $err"
fi

# ---------------------------------------------------------------------------
echo "== section 18: Prometheus text ingest =="
# The hidden BLOB payload now sub-dispatches on its FIRST BYTE:
#   0x01            → batch blob v0 (section 7 semantics, unchanged)
#   0x00, 0x02–0x08 → reserved future batch versions → loud error
#   anything else   → Prometheus text exposition body
# TIMESTAMP UNIT (documented in metrics_vtab.rs module docs): the table
# stores EPOCH SECONDS. Explicit prom timestamps are MILLISECONDS on the
# wire and the engine normalizes them (/1000); samples without a
# timestamp get the CURRENT WALL CLOCK in seconds. Fixture covers:
# HELP/TYPE comments (free), a bare counter (no labels, no ts), a
# labeled gauge with an explicit ms ts, a histogram-style multi-label
# line (no ts), one malformed line (counted as an error), and one valid NaN
# sample. Partial success succeeds silently, like a Prometheus scrape.
PROMBODY="$TMP/scrape.prom"
cat > "$PROMBODY" <<'PROM'
# HELP http_requests_total Total HTTP requests.
# TYPE http_requests_total counter
http_requests_total 1027
node_temp_celsius{sensor="cpu0",host="pvm1"} 42.5 1753000000123
http_request_duration_seconds_bucket{le="0.5",method="GET",code="200"} 129389
this line is definitely not prometheus !!!
bad_metric NaN
PROM
PROMDB="$TMP/prom_test.db"
got=$(sqlite3 "$PROMDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
INSERT INTO metrics(metrics) VALUES (readfile('$PROMBODY'));
SELECT 'ingested', last_insert_rowid();
INSERT INTO metrics(metrics) VALUES ('flush');
SELECT 'total', COUNT(*) FROM metrics;
SELECT 'temp', name, ts, value, labels FROM metrics WHERE name = 'node_temp_celsius';
SELECT 'bucket', value, labels FROM metrics WHERE name = 'http_request_duration_seconds_bucket';
SELECT 'default_shared', COUNT(DISTINCT ts) FROM metrics WHERE name != 'node_temp_celsius';
SELECT 'default_sane', COUNT(*) FROM metrics WHERE name != 'node_temp_celsius'
  AND ts BETWEEN 1750000000 AND 4000000000;
SQL
)
# ingested = 4 samples (rowid = count; the malformed line is an error, not a
# sample, and NOT fatal). The NaN sample is stored bit-exact but ordinary
# SQLite REAL projection is NULL; packed public frames preserve its bits.
# 'temp' proves the explicit ms ts came out as
# SECONDS (1753000000123 ms → 1753000000 s) and labels round-trip in
# canonical sorted-JSON. 'bucket' pins one exact multi-label value.
# 'default_shared' = 1: both no-ts samples got the SAME default (one
# wall-clock read per body). 'default_sane': that default is epoch
# SECONDS in a sane range — 1750000000 ≈ mid-2025 < now, and the 4e9
# upper bound would be violated by a milliseconds default (~1.79e12),
# so this asserts the unit, not just "recent".
expected='ingested|4
total|4
temp|node_temp_celsius|1753000000|42.5|{"host":"pvm1","sensor":"cpu0"}
bucket|129389.0|{"code":"200","le":"0.5","method":"GET"}
default_shared|1
default_sane|3'
check_eq "prometheus body: count, ms→s ts, labels, shared seconds default" "$got" "$expected"

# Batch v0 still works THROUGH THE SAME DISPATCH into the same table
# (regression: section 7's blob starts with 0x01 and must keep taking
# the batch path, not the text path). 3 more points → 7 total. Flushed
# at the end so they survive into the new-process check below (the POC
# durability contract: unflushed buffers die with the process).
got=$(sqlite3 "$PROMDB" <<SQL
.load $EXT
INSERT INTO metrics(metrics) VALUES (readfile('$BLOB'));
SELECT 'batch_rowid', last_insert_rowid();
SELECT 'total', COUNT(*) FROM metrics;
INSERT INTO metrics(metrics) VALUES ('flush');
SQL
)
expected='batch_rowid|3
total|7'
check_eq "batch v0 blob still dispatches to the batch path" "$got" "$expected"

# An all-garbage body parses as prometheus text but yields 0 samples +
# errors → must be rejected (that payload was not exposition text).
printf 'not prometheus at all\nstill not prometheus\n' > "$TMP/garbage.prom"
if err=$(sqlite3 "$PROMDB" ".load $EXT" \
    "INSERT INTO metrics(metrics) VALUES (readfile('$TMP/garbage.prom'));" 2>&1); then
  fail "all-garbage prometheus body should be rejected (got success: $err)"
elif [[ "$err" == *"0 samples ingested"* ]]; then
  pass "all-garbage body rejected with '0 samples ingested'"
else
  fail "all-garbage body rejected but with unexpected message: $err"
fi

# A reserved version byte (0x05) must fail LOUDLY — a future batch
# format fed to this build must never be mis-parsed as text.
printf '\x05future batch format' > "$TMP/v5.blob"
if err=$(sqlite3 "$PROMDB" ".load $EXT" \
    "INSERT INTO metrics(metrics) VALUES (readfile('$TMP/v5.blob'));" 2>&1); then
  fail "version byte 0x05 should be rejected (got success: $err)"
elif [[ "$err" == *"unknown blob format: version byte 0x05"* ]]; then
  pass "reserved version byte 0x05 rejected with a clear error"
else
  fail "0x05 blob rejected but with unexpected message: $err"
fi

# Zero-length blob: no first byte to dispatch on → clear error.
if err=$(sqlite3 "$PROMDB" ".load $EXT" \
    "INSERT INTO metrics(metrics) VALUES (x'');" 2>&1); then
  fail "empty blob should be rejected (got success: $err)"
elif [[ "$err" == *"empty blob"* ]]; then
  pass "empty blob rejected with a clear error"
else
  fail "empty blob rejected but with unexpected message: $err"
fi

# Nothing from the rejected payloads may have been stored.
got=$(sqlite3 "$PROMDB" ".load $EXT" \
  "INSERT INTO metrics(metrics) VALUES ('flush'); SELECT COUNT(*) FROM metrics;")
check_eq "rejected payloads stored nothing" "$got" "7"

# ---------------------------------------------------------------------------
echo "== section 19: plain-table oracle property test (3 seeds) =="
# tools/bench/src/oracle.rs: a seeded PRNG drives ~50k ops per seed
# (inserts / flush / optimize / compact / queries / explicit txns with
# rollback / prune-all) against the three vtabs AND mirrored plain
# tables in one db; after every query op the result sets must match
# exactly (order-insensitive, floats by bit pattern). A failure prints
# the seed + op index — replay with:  oracle <ext.so> <seed>
if (cd "$ROOT/tools/bench" && cargo run --release --quiet --bin oracle -- "$EXT"); then
  pass "oracle: 3 seeds, 50k ops each, vtab == plain table throughout"
else
  fail "oracle property test found a divergence (seed/op printed above)"
fi

# ---------------------------------------------------------------------------
echo "== section 20: kill -9 crash test =="
# tests/crash.sh: repeatedly kill -9 a live ingest+flush workload, then
# reopen and verify integrity + the flushed-data durability contract.
if "$ROOT/tests/crash.sh" "$EXT"; then
  pass "crash test: 5 kill -9 iterations, integrity + watermarks held"
else
  fail "crash test failed (see output above)"
fi

# ---------------------------------------------------------------------------
echo "== section 21: R4 shared engine — two connections, one process =="
# The sqld reality: the extension is loaded into EVERY pooled connection
# and each connection xConnects its own vtab instance over the same
# shadow tables. shared.rs must make them share ONE engine (registry
# keyed by canonical db path + table), route store SQL to the calling
# connection, and serialize writers with the 5s-bounded gate.
#
# The sqlite3 CLI is one connection per process, so this section drives
# TWO connections in ONE process through python3's sqlite3 module (which
# links the same system libsqlite3 and supports enable_load_extension —
# verified: if your python was built without it, this section fails
# loudly at load_extension, not silently).
#
# Checks:
#  (a) A inserts + flushes; B sees the rows WITHOUT reopening — under
#      the old per-connection engines B's index snapshot (taken at its
#      earlier xConnect) would have been stale/empty.
#  (b) A inserts with NO flush; B sees the BUFFERED point too — the
#      documented shared-buffer semantics (dirty reads of buffered
#      telemetry; flushed data stays transactional).
#  (c) A holds BEGIN + INSERT (writer gate held); B's INSERT must fail
#      BOUNDED with a busy-style error. Empirical fact (VDBE bytecode:
#      OP_Transaction runs before OP_VBegin): stock SQLite takes the
#      FILE write lock before the vtab's xBegin, so B collides with
#      SQLITE_BUSY ("database is locked") before it can reach our
#      gate — the gate's own 5s timeout path is therefore covered by
#      Rust unit tests in shared.rs (it is the active protection only
#      under concurrent-writer hosts like libsql BEGIN CONCURRENT).
#      Here we assert the user-visible contract: a second writer fails
#      bounded, with a lock error, and never hangs or corrupts.
#  (d) A COMMITs; B's retried INSERT succeeds (gate released).
#  (e) A DROPs and recreates the table; both connections stay sane
#      (registry entry removed at xDestroy, fresh engine after).
DB21="$TMP/multiconn.db"
py_out=$(python3 - "$EXT" "$DB21" <<'PYEOF'
import sqlite3, sys, time

ext, db = sys.argv[1], sys.argv[2]

def connect():
    c = sqlite3.connect(db, timeout=30)
    c.isolation_level = None  # autocommit; explicit BEGIN issued by hand
    c.enable_load_extension(True)
    c.load_extension(ext)
    return c

A = connect()
B = connect()

A.execute("CREATE VIRTUAL TABLE m USING timeless_metrics")
# Force B's xConnect NOW, before any data exists: a stale second engine
# would snapshot emptiness here and never see A's work.
assert B.execute("SELECT count(*) FROM m").fetchone()[0] == 0

# (a) flushed data crosses connections without a reopen
A.execute("INSERT INTO m(name, ts, value) VALUES ('cpu', 100, 1.5)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
print("a", B.execute("SELECT name, ts, value FROM m").fetchall())
print("a_agg", B.execute(
    "SELECT value FROM timeless_aggregate('m','cpu',NULL,0,500,'sum')"
).fetchone()[0])

# (b) BUFFERED (unflushed) data is visible too: one shared buffer.
# This is the accepted telemetry semantics, asserted on purpose so a
# future change to it is a deliberate decision, not an accident.
A.execute("INSERT INTO m(name, ts, value) VALUES ('cpu', 200, 2.5)")
print("b", B.execute("SELECT count(*) FROM m WHERE name = 'cpu'").fetchone()[0])
print("b_agg", B.execute(
    "SELECT value FROM timeless_aggregate('m','cpu',NULL,0,500,'sum')"
).fetchone()[0])

# (c) A's open write txn locks B's writes out, BOUNDED. On stock
# SQLite the failure is SQLite's own "database is locked" (file write
# lock precedes xBegin — see the section comment); our gate message
# ("locked by another connection") is accepted too, for hosts where
# the vtab gate is reached first. Either way: bounded, never a hang.
B.execute("PRAGMA busy_timeout = 2000")
A.execute("BEGIN")
A.execute("INSERT INTO m(name, ts, value) VALUES ('cpu', 300, 3.5)")
t0 = time.time()
try:
    B.execute("INSERT INTO m(name, ts, value) VALUES ('mem', 300, 9.0)")
    print("c UNEXPECTED-SUCCESS")
except sqlite3.OperationalError as e:
    dt = time.time() - t0
    if "locked" in str(e) and 1.5 <= dt <= 20.0:
        print("c busy-after-bounded-wait")
    else:
        print("c UNEXPECTED", repr(str(e)), round(dt, 1))
B.execute("PRAGMA busy_timeout = 30000")

# (d) commit releases the gate; B's retry succeeds
A.execute("COMMIT")
B.execute("INSERT INTO m(name, ts, value) VALUES ('mem', 300, 9.0)")
# 4 = ts100 (flushed) + ts200/ts300 (A, buffered) + mem300 (B, buffered):
# A reading B's buffered point is the same shared-buffer semantics.
print("d", A.execute("SELECT count(*) FROM m").fetchone()[0])

# (e) DROP on A (xDestroy: shadow tables dropped, registry entry
# removed), recreate, then BOTH connections use the fresh engine.
A.execute("DROP TABLE m")
A.execute("CREATE VIRTUAL TABLE m USING timeless_metrics")
B.execute("INSERT INTO m(name, ts, value) VALUES ('disk', 400, 7.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
print("e", A.execute("SELECT name, ts, value FROM m").fetchall(),
      B.execute("SELECT count(*) FROM m").fetchone()[0])

A.close(); B.close()
PYEOF
) || { fail "section 21 python driver crashed"; py_out=""; }
check_eq "(a) B sees A's flushed rows without reopen" \
  "$(grep '^a ' <<<"$py_out")" "a [('cpu', 100, 1.5)]"
check_eq "(a aggregate) B reduces A's flushed rows without reopen" \
  "$(grep '^a_agg ' <<<"$py_out")" "a_agg 1.5"
check_eq "(b) B sees A's BUFFERED point (shared-buffer semantics)" \
  "$(grep '^b ' <<<"$py_out")" "b 2"
check_eq "(b aggregate) B reduces A's buffered point" \
  "$(grep '^b_agg ' <<<"$py_out")" "b_agg 4.0"
check_eq "(c) second writer fails BOUNDED with a lock error (gate unit-tested in shared.rs)" \
  "$(grep '^c ' <<<"$py_out")" "c busy-after-bounded-wait"
check_eq "(d) B's retry succeeds after A commits" \
  "$(grep '^d ' <<<"$py_out")" "d 4"
check_eq "(e) drop + recreate: both connections sane on the new engine" \
  "$(grep '^e ' <<<"$py_out")" "e [('disk', 400, 7.0)] 1"

# ---------------------------------------------------------------------------
echo "== section 22: Q2 reduction-kernel TVFs (timeless_grid / timeless_window) =="
# The kernels are only allowed to exist because a dumb evaluator over the
# raw vtab must agree with them exactly (PLAN.md "Query interface tiers").
# Here the dumb evaluator is plain SQL over the SAME vtab: a recursive-CTE
# grid plus correlated subqueries. Values are dyadic (x.0/x.5/x.25) so
# float sums are order-independent and text-compare exactly. The dataset
# deliberately mixes flushed points with a buffered one (ts=131) — TVF
# queries must see the buffer like vtab queries do.
Q2DB="$TMP/q2_tvf.db"
got=$(sqlite3 "$Q2DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m USING timeless_metrics;
INSERT INTO m(name, ts, value, labels) VALUES
  ('cpu', 100, 1.0,  '{"host":"a"}'), ('cpu', 110, 2.0,  '{"host":"a"}'),
  ('cpu', 125, 3.0,  '{"host":"a"}'),
  ('cpu', 105, 10.5, '{"host":"b"}'), ('cpu', 122, 20.25,'{"host":"b"}'),
  ('mem', 108, 7.0,  '{"host":"a"}');
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(name, ts, value, labels) VALUES ('cpu', 131, 4.0, '{"host":"a"}');

.print -- grid vs SQL reference (start 100 stop 140 step 10 lookback 15)
SELECT 'tvf', labels, ts, value
  FROM timeless_grid('m', 'cpu', NULL, 100, 140, 10, 15) ORDER BY labels, ts;
WITH RECURSIVE g(t) AS (SELECT 100 UNION ALL SELECT t+10 FROM g WHERE t+10 <= 140),
  h(labels) AS (SELECT DISTINCT labels FROM m WHERE name='cpu')
SELECT 'ref', h.labels, g.t,
  (SELECT value FROM m WHERE name='cpu' AND labels=h.labels
    AND ts <= g.t AND ts > g.t - 15 ORDER BY ts DESC LIMIT 1) AS v
FROM h, g WHERE v IS NOT NULL ORDER BY h.labels, g.t;

.print -- window aggs vs SQL reference (start 100 stop 140 step 20 window 30)
SELECT 'tvf', 'sum', labels, ts, value
  FROM timeless_window('m', 'cpu', NULL, 100, 140, 20, 30, 'sum') ORDER BY labels, ts;
WITH RECURSIVE g(t) AS (SELECT 100 UNION ALL SELECT t+20 FROM g WHERE t+20 <= 140),
  h(labels) AS (SELECT DISTINCT labels FROM m WHERE name='cpu')
SELECT 'ref', 'sum', h.labels, g.t,
  (SELECT SUM(value) FROM m WHERE name='cpu' AND labels=h.labels
    AND ts <= g.t AND ts > g.t - 30) AS v
FROM h, g WHERE v IS NOT NULL ORDER BY h.labels, g.t;
SELECT 'tvf', 'count', labels, ts, value
  FROM timeless_window('m', 'cpu', NULL, 100, 140, 20, 30, 'count') ORDER BY labels, ts;
WITH RECURSIVE g(t) AS (SELECT 100 UNION ALL SELECT t+20 FROM g WHERE t+20 <= 140),
  h(labels) AS (SELECT DISTINCT labels FROM m WHERE name='cpu')
SELECT 'ref', 'count', h.labels, g.t,
  (SELECT CAST(COUNT(value) AS REAL) FROM m WHERE name='cpu' AND labels=h.labels
    AND ts <= g.t AND ts > g.t - 30) AS v
FROM h, g WHERE v > 0 ORDER BY h.labels, g.t;
SELECT 'tvf', 'min', labels, ts, value
  FROM timeless_window('m', 'cpu', NULL, 100, 140, 20, 30, 'min') ORDER BY labels, ts;
WITH RECURSIVE g(t) AS (SELECT 100 UNION ALL SELECT t+20 FROM g WHERE t+20 <= 140),
  h(labels) AS (SELECT DISTINCT labels FROM m WHERE name='cpu')
SELECT 'ref', 'min', h.labels, g.t,
  (SELECT MIN(value) FROM m WHERE name='cpu' AND labels=h.labels
    AND ts <= g.t AND ts > g.t - 30) AS v
FROM h, g WHERE v IS NOT NULL ORDER BY h.labels, g.t;

.print -- packed window batches: sparse and dense/null forms
SELECT 'batch_sparse', labels, hex(buckets)
  FROM timeless_window_batches('m', 'cpu', NULL, 100, 140, 20, 30, 'sum')
  ORDER BY labels;
SELECT 'batch_dense', labels, hex(buckets)
  FROM timeless_window_batches('m', 'cpu', NULL, 100, 140, 20, 30, 'sum', 'null')
  ORDER BY labels;
SELECT 'batch_limited', COUNT(*)
  FROM timeless_window_batches('m', 'cpu', NULL, 100, 140, 20, 30, 'sum', NULL, 6);
SELECT 'window_stats',
  (SELECT value FROM timeless_stats('m') WHERE key='window_batch_query_count'),
  (SELECT value FROM timeless_stats('m') WHERE key='window_batch_query_series_considered'),
  (SELECT value FROM timeless_stats('m') WHERE key='window_batch_query_candidate_chunks'),
  (SELECT value FROM timeless_stats('m') WHERE key='window_batch_query_decoded_points'),
  (SELECT value FROM timeless_stats('m') WHERE key='window_batch_query_buffered_points_considered'),
  (SELECT value FROM timeless_stats('m') WHERE key='window_batch_query_returned_points'),
  (SELECT value > 0 FROM timeless_stats('m') WHERE key='window_batch_query_payload_bytes_read'),
  (SELECT value > 0 FROM timeless_stats('m') WHERE key='window_batch_query_total_ns');

.print -- label filter + metric isolation
SELECT 'filtered', labels, ts, value
  FROM timeless_grid('m', 'cpu', '{"host":"b"}', 100, 140, 10, 15) ORDER BY ts;
SELECT 'mem', labels, ts, value
  FROM timeless_grid('m', 'mem', NULL, 100, 140, 10, 15) ORDER BY ts;
SQL
)
# The TVF and reference halves must be identical modulo the tvf/ref tag.
tvf_grid=$(grep '^tvf|{' <<<"$got" | sed 's/^tvf|//')
ref_grid=$(grep '^ref|{' <<<"$got" | sed 's/^ref|//')
check_eq "grid-last TVF == SQL reference over the raw vtab (incl. buffered ts=131)" \
  "$tvf_grid" "$ref_grid"
for agg in sum count min; do
  tvf_w=$(grep "^tvf|$agg|" <<<"$got" | sed 's/^tvf|//')
  ref_w=$(grep "^ref|$agg|" <<<"$got" | sed 's/^ref|//')
  if [[ -z "$tvf_w" ]]; then
    fail "window $agg TVF produced no rows"
  else
    check_eq "window $agg TVF == SQL reference" "$tvf_w" "$ref_w"
  fi
done
check_eq "packed sparse window blobs preserve timestamps and value bits" \
  "$(grep '^batch_sparse|' <<<"$got")" \
'batch_sparse|{"host":"a"}|5457423103000000640000000000000078000000000000008C0000000000000007000000000000F03F00000000000008400000000000001C40
batch_sparse|{"host":"b"}|545742310200000078000000000000008C000000000000000300000000000025400000000000403440'
check_eq "packed dense window blobs mark empty grid points in the validity bitmap" \
  "$(grep '^batch_dense|' <<<"$got")" \
'batch_dense|{"host":"a"}|5457423103000000640000000000000078000000000000008C0000000000000007000000000000F03F00000000000008400000000000001C40
batch_dense|{"host":"b"}|5457423103000000640000000000000078000000000000008C0000000000000006000000000000000000000000000025400000000000403440'
check_eq "packed window max_work_points is inclusive" \
  "$(grep '^batch_limited|' <<<"$got")" 'batch_limited|2'
check_eq "packed window stats expose series, chunks, decode, buffer, result, bytes, and time" \
  "$(grep '^window_stats|' <<<"$got")" 'window_stats|3|6|6|15|3|15|1|1'
check_eq "label filter restricts to host b" \
  "$(grep '^filtered|' <<<"$got")" \
'filtered|{"host":"b"}|110|10.5
filtered|{"host":"b"}|130|20.25'
check_eq "metric isolation (mem grid sees only mem's ts=108 sample: hits at t=110 and t=120)" \
  "$(grep '^mem|' <<<"$got")" \
'mem|{"host":"a"}|110|7.0
mem|{"host":"a"}|120|7.0'

# Error contract: helpful messages, not silent wrong answers.
err=$(sqlite3 "$Q2DB" ".load $EXT" \
  "SELECT * FROM timeless_grid('m', 'cpu', NULL, 0, 100, 10);" 2>&1 || true)
check_eq "missing required arg names the argument" \
  "$(grep -c 'missing required argument.*lookback' <<<"$err")" "1"
err=$(sqlite3 "$Q2DB" ".load $EXT" \
  "SELECT * FROM timeless_grid('m', 'cpu', NULL, 0, 100, 0, 10);" 2>&1 || true)
check_eq "step 0 rejected by the kernel" \
  "$(grep -c 'step must be positive' <<<"$err")" "1"
err=$(sqlite3 "$Q2DB" ".load $EXT" \
  "SELECT * FROM timeless_grid('nope', 'cpu', NULL, 0, 100, 10, 10);" 2>&1 || true)
check_eq "unknown table fails with a no-such-table error" \
  "$(grep -c 'no such table' <<<"$err")" "1"
err=$(sqlite3 "$Q2DB" ".load $EXT" \
  "SELECT * FROM timeless_window_batches('m', 'cpu', NULL, 100, 140, 20, 30, 'bogus');" 2>&1 || true)
check_eq "packed window errors name their own public module" \
  "$(grep -c 'timeless_window_batches: unknown agg' <<<"$err")" "1"
err=$(sqlite3 "$Q2DB" ".load $EXT" \
  "SELECT * FROM timeless_window_batches('m','cpu',NULL,140,140,20,100,'sum',NULL,4);" 2>&1 || true)
check_eq "packed window max_work_points rejects excess input before decode" \
  "$(grep -c 'work point limit 4 exceeded.*candidate input points: 5' <<<"$err")" "1"

# Fresh-connection recovery: the TVF must build the engine itself (no
# prior vtab query on this connection) and still see flushed data.
got=$(sqlite3 "$Q2DB" ".load $EXT" \
  "SELECT labels, ts, value FROM timeless_grid('m', 'cpu', '{\"host\":\"a\"}', 100, 130, 10, 15) ORDER BY ts;")
check_eq "fresh connection: TVF-first access recovers the engine" "$got" \
'{"host":"a"}|100|1.0
{"host":"a"}|110|2.0
{"host":"a"}|120|2.0
{"host":"a"}|130|3.0'

# ---------------------------------------------------------------------------
echo "== section 23: F1 catalog + stats TVFs (timeless_series / timeless_stats) =="
F1DB="$TMP/f1_tvf.db"
got=$(sqlite3 "$F1DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m USING timeless_metrics;
CREATE VIRTUAL TABLE l USING timeless_logs(index_keys='service');
CREATE VIRTUAL TABLE t USING timeless_traces;
INSERT INTO m(name, ts, value, labels) VALUES
  ('cpu', 100, 1.0, '{"host":"a"}'), ('cpu', 200, 2.0, '{"host":"a"}'),
  ('cpu', 150, 9.0, '{"host":"b"}');
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(name, ts, value, labels) VALUES ('cpu', 250, 3.0, '{"host":"a"}');
INSERT INTO l(ts, level, message, metadata) VALUES (1000, 'error', 'boom', '{"service":"api"}');
INSERT INTO t(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES ('4bf92f3577b34da6a3ce929d0e0e4736', '00f067aa0ba902b7', 'op', 'api', 'server', 'ok', 5000, 10);
.print -- catalog: buffered ts=250 must extend max_ts, points/buffered split correct
SELECT 'cat', name, labels, min_ts, max_ts, points, chunks, buffered FROM timeless_series('m');
.print -- catalog count matches DISTINCT over the raw vtab
SELECT 'catcount', (SELECT COUNT(*) FROM timeless_series('m')) =
                   (SELECT COUNT(DISTINCT name || labels) FROM m);
.print -- stats: module row + a few load-bearing keys per module type
SELECT 'sm', key, value FROM timeless_stats('m')
 WHERE key IN ('module','series','buffered_points','disk_points');
SELECT 'sl', key, value FROM timeless_stats('l')
 WHERE key IN ('module','blocks','buffered_entries','terms');
SELECT 'st', key, value FROM timeless_stats('t')
 WHERE key IN ('module','buffered_spans','disk_spans','total_spans','ts_min',
               'query_count','query_cancelled','query_candidate_blocks','query_payload_blocks_read',
               'query_decoded_spans','query_matched_spans','query_returned_spans');
.print -- prune reflected in catalog
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(m) VALUES ('prune:160');
SELECT 'postprune', name, labels, min_ts, max_ts FROM timeless_series('m') ORDER BY labels;
SQL
)
check_eq "catalog rows (buffered extends max_ts; per-series split)" \
  "$(grep '^cat|' <<<"$got")" \
'cat|cpu|{"host":"a"}|100|250|3|1|1
cat|cpu|{"host":"b"}|150|150|1|1|0'
check_eq "catalog count == DISTINCT series over raw vtab" \
  "$(grep '^catcount|' <<<"$got")" "catcount|1"
check_eq "metrics stats keys" "$(grep '^sm|' <<<"$got")" \
'sm|module|timeless_metrics
sm|series|2
sm|disk_points|3
sm|buffered_points|1'
check_eq "logs stats keys" "$(grep '^sl|' <<<"$got")" \
'sl|module|timeless_logs
sl|blocks|0
sl|buffered_entries|1
sl|terms|0'
check_eq "traces stats keys" "$(grep '^st|' <<<"$got")" \
'st|module|timeless_traces
st|buffered_spans|1
st|disk_spans|0
st|total_spans|1
st|ts_min|5000
st|query_count|0
st|query_cancelled|0
st|query_candidate_blocks|0
st|query_payload_blocks_read|0
st|query_decoded_spans|0
st|query_matched_spans|0
st|query_returned_spans|0'
# prune is CHUNK-granular: cpu-a's {100,200} chunk straddles the cutoff
# and survives whole; cpu-b's chunk dies, leaving an empty cataloged
# series (empty series persist — documented limit).
check_eq "prune reflected in catalog (chunk-granular; emptied series has NULL range)" \
  "$(grep '^postprune|' <<<"$got")" \
'postprune|cpu|{"host":"a"}|100|250
postprune|cpu|{"host":"b"}||'
err=$(sqlite3 "$F1DB" ".load $EXT" "SELECT * FROM timeless_series('l');" 2>&1 || true)
check_eq "timeless_series on a logs table names the right module" \
  "$(grep -c 'timeless_logs table' <<<"$err")" "1"
err=$(sqlite3 "$F1DB" ".load $EXT" "SELECT * FROM timeless_stats('nope');" 2>&1 || true)
check_eq "stats on unknown table: clean error" \
  "$(grep -c 'not a timeless virtual table' <<<"$err")" "1"
# R4 interplay: committed DROP + recreate must show a FRESH catalog.
got=$(sqlite3 "$F1DB" <<SQL
.load $EXT
DROP TABLE m;
CREATE VIRTUAL TABLE m USING timeless_metrics;
INSERT INTO m(name, ts, value) VALUES ('disk', 999, 7.0);
SELECT 'fresh', name, min_ts, max_ts, points, buffered FROM timeless_series('m');
SQL
)
check_eq "catalog after committed DROP/recreate is fresh (R4)" \
  "$(grep '^fresh|' <<<"$got")" "fresh|disk|999|999|1|1"

# ---------------------------------------------------------------------------
echo "== section 24: F2 automated retention (table argument) =="
F2DB="$TMP/f2_retention.db"
got=$(sqlite3 "$F2DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m USING timeless_metrics(retention='100s');
CREATE VIRTUAL TABLE l USING timeless_logs(index_keys='service', retention=1000);
CREATE VIRTUAL TABLE t USING timeless_traces(retention='1m');
SELECT 'ret', 'm', value FROM timeless_stats('m') WHERE key='retention';
SELECT 'ret', 'l', value FROM timeless_stats('l') WHERE key='retention';
SELECT 'ret', 't', value FROM timeless_stats('t') WHERE key='retention';
.print -- metrics: epoch 2 flush prunes epoch 1 (cutoff 1210-100=1110)
INSERT INTO m(name, ts, value) VALUES ('cpu', 1000, 1.0), ('cpu', 1010, 2.0);
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(name, ts, value) VALUES ('cpu', 1200, 3.0), ('cpu', 1210, 4.0);
INSERT INTO m(m) VALUES ('flush');
SELECT 'mrows', ts FROM m ORDER BY ts;
.print -- logs: same shape in ms (retention 1000 native)
INSERT INTO l(ts, level, message) VALUES (5000, 'info', 'old');
INSERT INTO l(l) VALUES ('flush');
INSERT INTO l(ts, level, message) VALUES (7000, 'info', 'new');
INSERT INTO l(l) VALUES ('flush');
SELECT 'lrows', ts, message FROM l ORDER BY ts;
.print -- traces: retention 1m = 60e9 ns
INSERT INTO t(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES ('00000000000000000000000000000001', '0000000000000001', 'old', 's', 'server', 'ok', 1000000000, 5);
INSERT INTO t(t) VALUES ('flush');
INSERT INTO t(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES ('00000000000000000000000000000002', '0000000000000002', 'new', 's', 'server', 'ok', 200000000000, 5);
INSERT INTO t(t) VALUES ('flush');
SELECT 'trows', name FROM t ORDER BY start_ts;
.print -- rollback: a flush whose retention pruned must restore the pruned chunk
INSERT INTO m(name, ts, value) VALUES ('cpu', 1400, 5.0);
BEGIN;
INSERT INTO m(name, ts, value) VALUES ('cpu', 1600, 6.0);
INSERT INTO m(m) VALUES ('flush');
ROLLBACK;
SELECT 'postrb', ts FROM m ORDER BY ts;
SQL
)
check_eq "retention persisted per module (native units: 100s, 1000ms, 60e9ns)" \
  "$(grep '^ret|' <<<"$got")" \
'ret|m|100
ret|l|1000
ret|t|60000000000'
check_eq "metrics: epoch-2 flush auto-prunes epoch 1" \
  "$(grep '^mrows|' <<<"$got")" \
'mrows|1200
mrows|1210'
check_eq "logs: epoch-2 flush auto-prunes epoch 1" \
  "$(grep '^lrows|' <<<"$got")" "lrows|7000|new"
check_eq "traces: epoch-2 flush auto-prunes epoch 1" \
  "$(grep '^trows|' <<<"$got")" "trows|new"
# The rolled-back txn's flush pruned nothing new (guard) but drained the
# 1400 buffer point; rollback must restore it as buffered, and 1600 must
# vanish. Chunks 1200/1210 remain.
check_eq "rollback restores buffered point drained by the rolled-back flush" \
  "$(grep '^postrb|' <<<"$got")" \
'postrb|1200
postrb|1210
postrb|1400'
# Steady state (acceptance): 4 retention windows of continuous ingest —
# point count must not grow past ~2 epochs' worth.
got=$(sqlite3 "$F2DB" <<SQL
.load $EXT
INSERT INTO m(name, ts, value) SELECT 'cpu', 2000 + value, 1.0 FROM generate_series(0, 49);
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(name, ts, value) SELECT 'cpu', 2200 + value, 1.0 FROM generate_series(0, 49);
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(name, ts, value) SELECT 'cpu', 2400 + value, 1.0 FROM generate_series(0, 49);
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(name, ts, value) SELECT 'cpu', 2600 + value, 1.0 FROM generate_series(0, 49);
INSERT INTO m(m) VALUES ('flush');
SELECT 'steady', COUNT(*), MIN(ts) >= 2400, MAX(ts) FROM m;
SQL
)
# Epochs are 200 apart with retention 100: each flush's cutoff excludes
# every prior epoch, so steady state is exactly one epoch (50 points).
check_eq "steady state across 4 retention windows (only the newest epoch survives)" \
  "$(grep '^steady|' <<<"$got")" "steady|50|1|2649"

# ---------------------------------------------------------------------------
echo "== section 25: F3 rollup ladder (timeless_rollup + 'rollup' command) =="
F3DB="$TMP/f3_rollup.db"
got=$(sqlite3 "$F3DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m USING timeless_metrics(rollups='60s@0,300s@0');
INSERT INTO m(name, ts, value, labels)
  SELECT 'cpu', 1000 + value * 10, value * 1.0, '{"host":"a"}' FROM generate_series(0, 99);
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(m) VALUES ('rollup');
.print -- 60s tier count-per-bucket vs SQL reference over raw (settled buckets only)
SELECT 'tvf', ts, value FROM timeless_rollup('m', 'cpu', NULL, 60, 0, 99999, 'count') ORDER BY ts;
WITH b(t) AS (SELECT DISTINCT (ts / 60) * 60 FROM m WHERE ts <= 1919)
SELECT 'ref', b.t, CAST((SELECT COUNT(*) FROM m WHERE ts >= b.t AND ts < b.t + 60) AS REAL)
FROM b WHERE b.t + 59 <= 1930 ORDER BY b.t;
.print -- avg agg vs SQL for one whole bucket
SELECT 'avg300', value FROM timeless_rollup('m', 'cpu', NULL, 300, 1200, 1200, 'avg');
SELECT 'refavg', AVG(value) FROM m WHERE ts >= 1200 AND ts < 1500;
.print -- rollback: 'rollup' inside a txn fully undone (rows AND index)
BEGIN;
INSERT INTO m(name, ts, value, labels)
  SELECT 'cpu', 3000 + value * 10, 1.0, '{"host":"a"}' FROM generate_series(0, 49);
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(m) VALUES ('rollup');
ROLLBACK;
SELECT 'postrb', COUNT(*) FROM timeless_rollup('m', 'cpu', NULL, 60, 2000, 99999, 'count');
SELECT 'postrb_rows', COUNT(*) FROM m_chunks WHERE resolution > 0 AND ts_min >= 2000;
SELECT 'stats', key, value FROM timeless_stats('m') WHERE key IN ('rollup_tiers','rollup_chunks');
SQL
)
tvf_rows=$(grep '^tvf|' <<<"$got" | sed 's/^tvf|//')
ref_rows=$(grep '^ref|' <<<"$got" | sed 's/^ref|//')
if [[ -z "$tvf_rows" ]]; then
  fail "rollup TVF produced no rows"
else
  check_eq "60s rollup counts == SQL reference over raw (settled only)" "$tvf_rows" "$ref_rows"
fi
check_eq "300s avg bucket == SQL AVG over the same raw window" \
  "$(grep -c '^avg300|34.5$' <<<"$got")$(grep -c '^refavg|34.5$' <<<"$got")" "11"
check_eq "rolled-back 'rollup' leaves no index entries or rows" \
  "$(grep -E '^postrb\|' <<<"$got")
$(grep -E '^postrb_rows\|' <<<"$got")" \
'postrb|0
postrb_rows|0'
check_eq "stats expose ladder + rollup chunk count" \
  "$(grep '^stats|' <<<"$got")" \
'stats|rollup_tiers|60:0,300:0
stats|rollup_chunks|2'
# Reopen recovery: fresh process sees the rolled buckets.
got=$(sqlite3 "$F3DB" ".load $EXT" \
  "SELECT COUNT(*) FROM timeless_rollup('m', 'cpu', NULL, 60, 0, 99999, 'count');")
check_eq "reopen: rollup index recovered from shadow rows" "$got" "16"
# The packed public API returns the complete stored bucket contract once per
# series. Decode it independently and compare every exposed aggregate with the
# long-lived row TVF; this also pins the version and exact payload length.
packed=$(python3 - "$EXT" "$F3DB" <<'PY'
import sqlite3, struct, sys

ext, db = sys.argv[1:]
conn = sqlite3.connect(db)
conn.enable_load_extension(True)
conn.load_extension(ext)
conn.enable_load_extension(False)
sid, labels, blob = conn.execute(
    "SELECT series_id, labels, buckets FROM timeless_rollup_batches("
    "'m', 'cpu', NULL, 60, 0, 99999)"
).fetchone()
assert blob[:4] == b"TRB1"
n, = struct.unpack_from("<I", blob, 4)
assert len(blob) == 8 + n * 64
columns = [blob[8 + i*n*8:8 + (i+1)*n*8] for i in range(8)]
timestamps = list(struct.unpack(f"<{n}q", columns[0]))
counts = list(struct.unpack(f"<{n}Q", columns[1]))
avg, sums, mins, maxs = [list(struct.unpack(f"<{n}d", col)) for col in columns[2:6]]
last_ts = list(struct.unpack(f"<{n}q", columns[6]))
last = list(struct.unpack(f"<{n}d", columns[7]))
packed_values = {
    "avg": avg, "sum": sums, "min": mins, "max": maxs, "last": last
}
for agg, values in packed_values.items():
    rows = conn.execute(
        "SELECT ts, value FROM timeless_rollup('m','cpu',NULL,60,0,99999,?) ORDER BY ts",
        (agg,),
    ).fetchall()
    assert [row[0] for row in rows] == timestamps
    assert [struct.pack("<d", row[1]) for row in rows] == [struct.pack("<d", v) for v in values]
count_rows = conn.execute(
    "SELECT ts, value FROM timeless_rollup('m','cpu',NULL,60,0,99999,'count') ORDER BY ts"
).fetchall()
assert [row[0] for row in count_rows] == timestamps
assert [int(row[1]) for row in count_rows] == counts
assert all(bucket <= sample < bucket + 60 for bucket, sample in zip(timestamps, last_ts))
print(f"packed|{sid}|{labels}|{n}|ok")
PY
)
check_eq "packed rollup blob == all six row aggregates" "$packed" \
  'packed|1|{"host":"a"}|16|ok'
# Error paths.
err=$(sqlite3 "$F3DB" ".load $EXT" \
  "SELECT * FROM timeless_rollup('m', 'cpu', NULL, 60, 0, 1, 'median');" 2>&1 || true)
check_eq "unknown rollup agg is named" "$(grep -c 'unknown agg' <<<"$err")" "1"
err=$(sqlite3 "$TMP/f3_bad.db" ".load $EXT" \
  "CREATE VIRTUAL TABLE b USING timeless_metrics(rollups='1h@0,5m@0');" 2>&1 || true)
check_eq "descending ladder rejected at CREATE" "$(grep -c 'must ascend' <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 26: F4 bucket kernels (timeless_log_buckets / timeless_trace_buckets) =="
F4DB="$TMP/f4_buckets.db"
got=$(sqlite3 "$F4DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE l USING timeless_logs(index_keys='service');
CREATE VIRTUAL TABLE t USING timeless_traces;
INSERT INTO l(ts, level, message, metadata)
  SELECT 1000 + value * 37, CASE value % 4 WHEN 3 THEN 'error' ELSE 'info' END,
         'e' || value, '{"service":"svc' || (value % 3) || '"}'
  FROM generate_series(0, 199);
INSERT INTO l(l) VALUES ('flush');
INSERT INTO l(ts, level, message, metadata) VALUES (9000, 'error', 'buffered', '{"service":"svc0"}');
.print -- kernel vs SQL GROUP BY over the raw vtab (incl. the buffered entry)
SELECT 'k', bucket_ts, group_key, n
  FROM timeless_log_buckets('l', 'level', NULL, 1000, 9999, 500) ORDER BY 2, 3;
SELECT 'r', 1000 + ((ts - 1000) / 500) * 500, level, COUNT(*)
  FROM l WHERE ts BETWEEN 1000 AND 9999 GROUP BY 2, 3 ORDER BY 2, 3;
.print -- group_by index key + filter
SELECT 'kf', bucket_ts, group_key, n
  FROM timeless_log_buckets('l', 'service', '{"level":"error"}', 1000, 9999, 4000) ORDER BY 2, 3;
SELECT 'rf', 1000 + ((ts - 1000) / 4000) * 4000, service, COUNT(*)
  FROM l WHERE level = 'error' AND ts BETWEEN 1000 AND 9999 GROUP BY 2, 3 ORDER BY 2, 3;
INSERT INTO t(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  SELECT printf('%032x', value + 1), printf('%016x', value + 1), 'op',
         'svc' || (value % 2), 'server',
         CASE value % 5 WHEN 0 THEN 'error' ELSE 'ok' END,
         1000000 + value * 1000, 100 + value
  FROM generate_series(0, 99);
INSERT INTO t(t) VALUES ('flush');
SELECT 'tk', bucket_ts, service, spans, errors, dur_sum
  FROM timeless_trace_buckets('t', NULL, 1000000, 1099999, 50000) ORDER BY 2, 3;
SELECT 'tr', 1000000 + ((start_ts - 1000000) / 50000) * 50000, service,
       COUNT(*), SUM(status = 'error'), SUM(duration_ns)
  FROM t WHERE start_ts BETWEEN 1000000 AND 1099999 GROUP BY 2, 3 ORDER BY 2, 3;
SQL
)
k=$(grep '^k|' <<<"$got" | sed 's/^k|//'); r=$(grep '^r|' <<<"$got" | sed 's/^r|//')
if [[ -z "$k" ]]; then fail "log bucket kernel returned no rows"; else
  check_eq "log buckets by level == SQL GROUP BY (incl. buffered)" "$k" "$r"; fi
kf=$(grep '^kf|' <<<"$got" | sed 's/^kf|//'); rf=$(grep '^rf|' <<<"$got" | sed 's/^rf|//')
if [[ -z "$kf" ]]; then fail "filtered log bucket kernel returned no rows"; else
  check_eq "log buckets by index key + level filter == SQL GROUP BY" "$kf" "$rf"; fi
tk=$(grep '^tk|' <<<"$got" | sed 's/^tk|//'); tr_=$(grep '^tr|' <<<"$got" | sed 's/^tr|//')
if [[ -z "$tk" ]]; then fail "trace bucket kernel returned no rows"; else
  check_eq "trace buckets == SQL GROUP BY (spans/errors/dur_sum)" "$tk" "$tr_"; fi
err=$(sqlite3 "$F4DB" ".load $EXT" \
  "SELECT * FROM timeless_log_buckets('l', 'path', NULL, 0, 10, 1);" 2>&1 || true)
check_eq "undeclared group key names the valid set" \
  "$(grep -c "expected 'level' or a declared index key" <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 27: F5 batch blob ingest (logs + traces v0) =="
F5DB="$TMP/f5_batch.db"
LBLOB="$TMP/f5_logs.blob"; TBLOB="$TMP/f5_traces.blob"
python3 - "$LBLOB" "$TBLOB" <<'PY'
import struct, sys
lp, tp = sys.argv[1], sys.argv[2]
# logs v0: 3 entries
blob = struct.pack('<BBHI', 1, 0, 0, 3)
blob += b''.join(struct.pack('<q', t) for t in [1000, 1050, 2000])
blob += bytes([1, 3, 1])
for m in [b'hello', b'boom', b'world']:
    blob += struct.pack('<I', len(m)) + m
for m in [b'{"service":"api"}', b'', b'{"service":"web"}']:
    blob += struct.pack('<I', len(m)) + m
open(lp, 'wb').write(blob)
# traces v0: 2 spans (one root, one child; ok + error)
b = struct.pack('<BBHI', 1, 0, 0, 2)
b += bytes([1]*16) + bytes([2]*16)
b += bytes([3]*8) + bytes([4]*8)
b += bytes(8) + bytes([5]*8)
for n in [b'op1', b'op2']: b += struct.pack('<I', len(n)) + n
for s in [b'api', b'web']: b += struct.pack('<I', len(s)) + s
b += bytes([1, 2]) + bytes([1, 2])
b += struct.pack('<qq', 5000, 6000) + struct.pack('<qq', 100, 200)
for a in [b'{"k":"v"}', b'']: b += struct.pack('<I', len(a)) + a
open(tp, 'wb').write(b)
PY
got=$(sqlite3 "$F5DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE l USING timeless_logs(index_keys='service');
CREATE VIRTUAL TABLE t USING timeless_traces;
INSERT INTO l(l) VALUES (readfile('$LBLOB'));
SELECT 'lrid', last_insert_rowid();
SELECT 'l', ts, level, message, metadata FROM l ORDER BY ts;
SELECT 'lpush', COUNT(*) FROM l WHERE service = 'api';
INSERT INTO t(t) VALUES (readfile('$TBLOB'));
SELECT 't', hex(trace_id), name, service, kind, status, start_ts, duration_ns,
       parent_span_id IS NULL FROM t ORDER BY start_ts;
.print -- rollback: a batch inside a txn fully undone
BEGIN;
INSERT INTO l(l) VALUES (readfile('$LBLOB'));
ROLLBACK;
SELECT 'postrb', COUNT(*) FROM l;
INSERT INTO l(l) VALUES ('flush');
SQL
)
check_eq "logs blob round-trip (rowid = count, pushdown works on blob metadata)" \
  "$(grep -E '^(lrid|lpush)\|' <<<"$got")" \
'lrid|3
lpush|1'
check_eq "logs blob rows decode exactly" "$(grep '^l|' <<<"$got")" \
'l|1000|info|hello|{"service":"api"}
l|1050|error|boom|{}
l|2000|info|world|{"service":"web"}'
check_eq "traces blob rows decode exactly (packed ids, root parent NULL)" \
  "$(grep '^t|' <<<"$got")" \
't|01010101010101010101010101010101|op1|api|server|ok|5000|100|1
t|02020202020202020202020202020202|op2|web|client|error|6000|200|0'
check_eq "rolled-back batch leaves nothing" "$(grep '^postrb|' <<<"$got")" "postrb|3"
# Malformed rejection: truncate at every section boundary + bad bytes.
python3 - "$LBLOB" "$TMP" <<'PY'
import sys
blob = open(sys.argv[1], 'rb').read()
tmp = sys.argv[2]
for i, cut in enumerate([3, 7, 20, 33, 40, len(blob) - 1]):
    open(f"{tmp}/f5_cut{i}.blob", 'wb').write(blob[:cut])
bad = bytearray(blob); bad[8 + 24] = 9  # level byte -> 9
open(f"{tmp}/f5_badlevel.blob", 'wb').write(bytes(bad))
PY
rejected=0
for f in "$TMP"/f5_cut*.blob "$TMP/f5_badlevel.blob"; do
  if sqlite3 "$F5DB" ".load $EXT" "INSERT INTO l(l) VALUES (readfile('$f'));" 2>/dev/null; then
    fail "malformed blob $(basename "$f") was ACCEPTED"
  else
    rejected=$((rejected + 1))
  fi
done
check_eq "all 7 malformed blobs rejected" "$rejected" "7"
count=$(sqlite3 "$F5DB" ".load $EXT" "SELECT COUNT(*) FROM l;")
check_eq "rejections were atomic (count unchanged)" "$count" "3"

echo "== section 27b: rich traces v1 fidelity and lifecycle =="
if python3 "$ROOT/tests/traces_rich.py" "$EXT" "$TMP/traces_rich.db" > "$TMP/traces_rich.log" 2>&1; then
  pass "$(cat "$TMP/traces_rich.log")"
else
  fail "rich traces regression: $(tail -3 "$TMP/traces_rich.log")"
fi

# ---------------------------------------------------------------------------
echo "== section 28: F6 trigram message index =="
F6DB="$TMP/f6_trigram.db"
got=$(sqlite3 "$F6DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE lt USING timeless_logs(index_keys='service', message_index='trigram');
CREATE VIRTUAL TABLE ln USING timeless_logs(index_keys='service');
INSERT INTO lt(ts, level, message)
  SELECT value, 'info', CASE value % 7 WHEN 0 THEN 'connection timeout to db ' || value
                                       WHEN 3 THEN 'TIMEOUT waiting ' || value
                                       ELSE 'ordinary message ' || value END
  FROM generate_series(1, 500);
INSERT INTO ln(ts, level, message)
  SELECT value, 'info', CASE value % 7 WHEN 0 THEN 'connection timeout to db ' || value
                                       WHEN 3 THEN 'TIMEOUT waiting ' || value
                                       ELSE 'ordinary message ' || value END
  FROM generate_series(1, 500);
INSERT INTO lt(lt) VALUES ('flush');
INSERT INTO ln(ln) VALUES ('flush');
INSERT INTO lt(ts, level, message) VALUES (600, 'info', 'buffered timeout tail');
INSERT INTO ln(ts, level, message) VALUES (600, 'info', 'buffered timeout tail');
.print -- identical results with and without the index, several patterns
SELECT 'p1', (SELECT COUNT(*) FROM lt WHERE message LIKE '%timeout%') =
             (SELECT COUNT(*) FROM ln WHERE message LIKE '%timeout%'),
             (SELECT COUNT(*) FROM lt WHERE message LIKE '%timeout%');
SELECT 'p2', (SELECT COUNT(*) FROM lt WHERE message LIKE '%time%out%') =
             (SELECT COUNT(*) FROM ln WHERE message LIKE '%time%out%');
SELECT 'p3', (SELECT COUNT(*) FROM lt WHERE message LIKE 'TIMEOUT_wait%') =
             (SELECT COUNT(*) FROM ln WHERE message LIKE 'TIMEOUT_wait%');
SELECT 'p4', (SELECT COUNT(*) FROM lt WHERE message LIKE '%zzznope%') =
             (SELECT COUNT(*) FROM ln WHERE message LIKE '%zzznope%');
.print -- index artifacts: tg terms present only on the indexed table
SELECT 'tg', (SELECT COUNT(*) FROM lt_terms WHERE term >= 'tg:' AND term < 'tg;') > 0,
             (SELECT COUNT(*) FROM ln_terms WHERE term >= 'tg:' AND term < 'tg;');
SELECT 'meta', (SELECT v FROM lt_meta WHERE k='message_index'),
               (SELECT COUNT(*) FROM ln_meta WHERE k='message_index');
SQL
)
check_eq "trigram vs unindexed: identical LIKE results (incl. buffered + case fold)"   "$(grep -E '^p[0-9]\|' <<<"$got")" 'p1|1|144
p2|1
p3|1
p4|1'
check_eq "tg terms only where declared; setting persisted"   "$(grep -E '^(tg|meta)\|' <<<"$got")" 'tg|1|0
meta|trigram|0'
err=$(sqlite3 "$F6DB" ".load $EXT"   "CREATE VIRTUAL TABLE bad USING timeless_logs(message_index='btree');" 2>&1 || true)
check_eq "invalid message_index value rejected"   "$(grep -c "expected 'trigram' or 'none'" <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 29: M1b fs-store importer (selftest incl. NaN + hostile labels) =="
# The selftest builds a hostile FsStore fixture (escaped/quoted labels,
# NaN payload bits — the case that found the shadow-store NaN stat bug —
# duplicate ts, a multi-blob series), imports it through Tier 2 blobs,
# and verifies every point. It exits nonzero on ANY mismatch.
(cd "$ROOT/tools/bench" && cargo build --release --bin import >/dev/null 2>&1)
if "$ROOT/tools/bench/target/release/import" --selftest "$EXT" "$TMP/import_st" > "$TMP/import.log" 2>&1; then
  pass "importer selftest (bit-exact incl. NaN + hostile labels)"
else
  fail "importer selftest: $(tail -2 "$TMP/import.log")"
fi
got=$(sqlite3 "$TMP/import_st/imported.db" ".load $EXT" \
  "SELECT COUNT(*), (SELECT COUNT(*) FROM timeless_series('metrics')) FROM metrics;")
check_eq "imported db independently queryable (rows + catalog)" "$got" "150015|4"

# ---------------------------------------------------------------------------
echo "== section 30: F7 window vocabulary (counters, percentiles, trimmed folds) =="
# Hand-verified literal expectations on a tiny dataset (the pinned
# definitions from FEATURE_PLAN F7, computed by hand in the comments):
#   req: 100 @10, 150 @20, 40 @30 (reset), 90 @40
#     delta(0,40] = 90 - 100 = -10
#     increase    = +50, reset->+40, +50 = 140      (NOT PromQL: no extrapolation)
#     rate        = 140 / 40 = 3.5 per native unit
#   lat: 5, 100, 7, 6, 900  ->  sorted [5,6,7,100,900]
#     p50 = rank ceil(2.5)=3 -> 7;  p95 -> rank 5 -> 900
#     tavg:20 drops 1 each end -> avg(6,7,100) = 37.666...
F7DB="$TMP/f7_window.db"
got=$(sqlite3 "$F7DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m USING timeless_metrics;
CREATE VIRTUAL TABLE t USING timeless_traces;
INSERT INTO m(name, ts, value) VALUES
  ('req', 10, 100.0), ('req', 20, 150.0), ('req', 30, 40.0), ('req', 40, 90.0),
  ('lat', 10, 5.0), ('lat', 20, 100.0), ('lat', 30, 7.0), ('lat', 40, 6.0), ('lat', 50, 900.0);
INSERT INTO m(m) VALUES ('flush');
SELECT 'delta', value FROM timeless_window('m','req',NULL,40,40,10,40,'delta');
SELECT 'incr', value FROM timeless_window('m','req',NULL,40,40,10,40,'increase');
SELECT 'rate', value FROM timeless_window('m','req',NULL,40,40,10,40,'rate');
SELECT 'p50', value FROM timeless_window('m','lat',NULL,50,50,10,50,'p50');
SELECT 'p95', value FROM timeless_window('m','lat',NULL,50,50,10,50,'p95');
SELECT 'tavg', ROUND(value, 4) FROM timeless_window('m','lat',NULL,50,50,10,50,'tavg:20');
INSERT INTO t(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  SELECT printf('%032x', value), printf('%016x', value), 'op', 'api', 'server', 'ok',
         1000, value * 100 FROM generate_series(1, 100);
INSERT INTO t(t) VALUES ('flush');
SELECT 'tp', dur_p50, dur_p95, dur_p99 FROM timeless_trace_buckets('t', NULL, 0, 2000, 5000);
SQL
)
check_eq "counter kernels (delta / reset-adjusted increase / rate)" \
  "$(grep -E '^(delta|incr|rate)\|' <<<"$got")" \
'delta|-10.0
incr|140.0
rate|3.5'
check_eq "exact nearest-rank percentiles + trimmed mean" \
  "$(grep -E '^(p50|p95|tavg)\|' <<<"$got")" \
'p50|7.0
p95|900.0
tavg|37.6667'
check_eq "trace duration percentiles (100 durations 100..10000)" \
  "$(grep '^tp|' <<<"$got")" "tp|5000|9500|9900"
err=$(sqlite3 "$F7DB" ".load $EXT" \
  "SELECT * FROM timeless_window('m','lat',NULL,0,1,1,1,'p0');" 2>&1 || true)
check_eq "p0 rejected (percentile range named)" \
  "$(grep -c 'percentile must be in (0, 100]' <<<"$err")" "1"
err=$(sqlite3 "$F7DB" ".load $EXT" \
  "SELECT * FROM timeless_window('m','lat',NULL,0,1,1,1,'tavg:50');" 2>&1 || true)
check_eq "tavg:50 rejected (trim range named)" \
  "$(grep -c 'trim fraction must be in' <<<"$err")" "1"
err=$(sqlite3 "$F7DB" ".load $EXT" \
  "SELECT * FROM timeless_window('m','lat',NULL,0,1,1,1,'median');" 2>&1 || true)
check_eq "unknown agg lists the full vocabulary" \
  "$(grep -c 'delta, increase, rate, pNN' <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 31: F8 label matchers + timeless_label_values =="
# Series: cpu{host=web-1,env=prod}, cpu{host=web-2,env=dev},
#         cpu{host=db-1,env=prod}, cpu{host=api-1} (env ABSENT).
# Matcher rules under test: regex fully anchored (PromQL-style),
# absent label matches as "" for neq/re/nre.
F8DB="$TMP/f8_matchers.db"
got=$(sqlite3 "$F8DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m USING timeless_metrics;
INSERT INTO m(name, labels, ts, value) VALUES
  ('cpu', '{"host":"web-1","env":"prod"}', 10, 1.0),
  ('cpu', '{"host":"web-2","env":"dev"}',  10, 2.0),
  ('cpu', '{"host":"db-1","env":"prod"}',  10, 3.0),
  ('cpu', '{"host":"api-1"}',              10, 4.0);
INSERT INTO m(m) VALUES ('flush');
SELECT 're', value FROM timeless_grid('m','cpu','{"host":{"re":"web-.*"}}',10,10,10,10) ORDER BY value;
SELECT 'anchor', COUNT(*) FROM timeless_grid('m','cpu','{"host":{"re":"eb-"}}',10,10,10,10);
SELECT 'neq', value FROM timeless_grid('m','cpu','{"env":{"neq":"prod"}}',10,10,10,10) ORDER BY value;
SELECT 'nre', value FROM timeless_grid('m','cpu','{"env":{"nre":".+"}}',10,10,10,10) ORDER BY value;
SELECT 'mix', value FROM timeless_grid('m','cpu','{"env":"prod","host":{"nre":"db-.*"}}',10,10,10,10) ORDER BY value;
SELECT 'lv', value FROM timeless_label_values('m','cpu','host');
SELECT 'lv_env', value FROM timeless_label_values('m','cpu','env');
SELECT 'lv_none', COUNT(*) FROM timeless_label_values('m','cpu','rack');
SQL
)
check_eq "anchored re selects exactly the web hosts" \
  "$(grep -E '^(re|anchor)\|' <<<"$got")" \
're|1.0
re|2.0
anchor|0'
check_eq "neq/nre treat absent env as empty string" \
  "$(grep -E '^(neq|nre)\|' <<<"$got")" \
'neq|2.0
neq|4.0
nre|4.0'
check_eq "eq pushdown + matcher AND compose" \
  "$(grep '^mix|' <<<"$got")" "mix|1.0"
check_eq "label_values: sorted distinct, absent-key empty" \
  "$(grep -E '^lv' <<<"$got")" \
'lv|api-1
lv|db-1
lv|web-1
lv|web-2
lv_env|dev
lv_env|prod
lv_none|0'
err=$(sqlite3 "$F8DB" ".load $EXT" \
  "SELECT * FROM timeless_grid('m','cpu','{\"host\":{\"re\":\"[\"}}',0,1,1,1);" 2>&1 || true)
check_eq "invalid regex is a loud error naming pattern and label" \
  "$(grep -c 'invalid regex' <<<"$err")" "1"
err=$(sqlite3 "$F8DB" ".load $EXT" \
  "SELECT * FROM timeless_grid('m','cpu','{\"host\":{\"like\":\"x\"}}',0,1,1,1);" 2>&1 || true)
check_eq "unknown operator names the valid set" \
  "$(grep -c 'valid operators: neq, re, nre' <<<"$err")" "1"
err=$(sqlite3 "$F8DB" ".load $EXT" \
  "SELECT * FROM timeless_label_values('m','cpu');" 2>&1 || true)
check_eq "label_values missing arg lists the call shape" \
  "$(grep -c 'tbl, metric, key' <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 32: F9 gap-fill (fill='null' on grid/window) =="
# Series a: points @10, @30; b: @10 only; c: @1000 (outside range).
# Grid 10..50 step 10 lookback 10 -> 5 grid points; windows (t-10, t].
#   a: 10=1.0, 20=NULL, 30=3.0, 40=NULL, 50=NULL
#   b: 10=5.0, rest NULL       c: NO rows (per-series absence rule)
F9DB="$TMP/f9_fill.db"
got=$(sqlite3 "$F9DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m9 USING timeless_metrics;
INSERT INTO m9(name, labels, ts, value) VALUES
  ('cpu', '{"host":"a"}', 10, 1.0), ('cpu', '{"host":"a"}', 30, 3.0),
  ('cpu', '{"host":"b"}', 10, 5.0),
  ('cpu', '{"host":"c"}', 1000, 9.0);
INSERT INTO m9(m9) VALUES ('flush');
SELECT 'cnt', COUNT(*), SUM(value IS NULL) FROM timeless_grid('m9','cpu',NULL,10,50,10,10,'null');
SELECT 'a', ts, COALESCE(value, '-') FROM timeless_grid('m9','cpu','{"host":"a"}',10,50,10,10,'null') ORDER BY ts;
SELECT 'absent', COUNT(*) FROM timeless_grid('m9','cpu','{"host":"c"}',10,50,10,10,'null');
SELECT 'sparse', COUNT(*) FROM timeless_grid('m9','cpu',NULL,10,50,10,10);
SELECT 'w', ts, COALESCE(value, '-') FROM timeless_window('m9','cpu','{"host":"a"}',10,50,10,10,'avg','null') ORDER BY ts;
SQL
)
check_eq "dense grid: 2 series x 5 points, 7 NULLs; series c absent; sparse default unchanged" \
  "$(grep -E '^(cnt|absent|sparse)\|' <<<"$got")" \
'cnt|10|7
absent|0
sparse|3'
check_eq "NULL placement per grid point (grid + window agree)" \
  "$(grep -E '^(a|w)\|' <<<"$got")" \
'a|10|1.0
a|20|-
a|30|3.0
a|40|-
a|50|-
w|10|1.0
w|20|-
w|30|3.0
w|40|-
w|50|-'
err=$(sqlite3 "$F9DB" ".load $EXT" \
  "SELECT * FROM timeless_grid('m9','cpu',NULL,10,50,10,10,'zero');" 2>&1 || true)
check_eq "unknown fill value rejected loudly" \
  "$(grep -c "fill must be 'none' or 'null'" <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 33: docs/QUERIES.md cookbook (every recipe executed) =="
# The cookbook's fixed dataset; expectations computed by hand in
# docs/QUERIES.md terms. If a recipe in the doc drifts from what the
# extension does, this section fails.
CKDB="$TMP/cookbook.db"
got=$(sqlite3 "$CKDB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE ck USING timeless_metrics;
INSERT INTO ck(name, labels, ts, value) VALUES
  ('req', '{"host":"a"}', 0, 0.0), ('req', '{"host":"a"}', 10, 10.0),
  ('req', '{"host":"a"}', 20, 30.0), ('req', '{"host":"a"}', 30, 5.0),
  ('req', '{"host":"a"}', 40, 25.0),
  ('cpu', '{"host":"a"}', 0, 1.0),  ('cpu', '{"host":"a"}', 60, 10.0),
  ('cpu', '{"host":"b"}', 0, 2.0),  ('cpu', '{"host":"b"}', 60, 20.0),
  ('cpu', '{"host":"c"}', 0, 3.0),  ('cpu', '{"host":"c"}', 60, 15.0),
  ('errors',   '{"host":"a"}', 60, 5.0),
  ('requests', '{"host":"a"}', 60, 50.0),
  ('arith_lhs', '{"host":"a"}', 60, 8.0),
  ('arith_rhs', '{"host":"a"}', 60, 2.0),
  ('set_lhs', '{"host":"a"}', 60, 10.0),
  ('set_lhs', '{"host":"b"}', 60, 20.0),
  ('set_lhs', '{"host":"c"}', 60, 30.0),
  ('set_rhs', '{"host":"a"}', 60, 100.0),
  ('set_rhs', '{"host":"b"}', 60, 200.0),
  ('set_rhs', '{"host":"d"}', 60, 400.0),
  ('matching_lhs', '{"host":"a","shared":"x","zone":"east"}', 60, 8.0),
  ('matching_rhs', '{"host":"a","shared":"x","zone":"west"}', 60, 2.0),
  ('group_many_lhs', '{"host":"a","pod":"p1"}', 60, 8.0),
  ('group_many_lhs', '{"host":"a","pod":"p2"}', 60, 9.0),
  ('group_one_rhs', '{"host":"a","team":"core"}', 60, 2.0),
  ('group_one_lhs', '{"host":"a","team":"core"}', 60, 8.0),
  ('group_many_rhs', '{"host":"a","pod":"p1"}', 60, 2.0),
  ('group_many_rhs', '{"host":"a","pod":"p2"}', 60, 3.0),
  ('lat', '{}', 0, 10.0), ('lat', '{}', 10, 11.0), ('lat', '{}', 20, 12.0),
  ('lat', '{}', 30, 13.0), ('lat', '{}', 40, 10.0), ('lat', '{}', 50, 11.0),
  ('lat', '{}', 60, 12.0), ('lat', '{}', 70, 13.0), ('lat', '{}', 80, 10.0),
  ('lat', '{}', 90, 11.0), ('lat', '{}', 100, 1000.0);
INSERT INTO ck(ck) VALUES ('flush');

-- recipe: reset-corrected increase in pure SQL over (0, 40] ...
WITH s AS (
  SELECT ts, value,
         LAG(value) OVER (PARTITION BY labels ORDER BY ts) AS prev
    FROM ck WHERE name = 'req' AND ts > 0 AND ts <= 40
)
SELECT 'sql_incr', SUM(CASE WHEN prev IS NULL THEN 0
                            WHEN value >= prev THEN value - prev
                            ELSE value END) FROM s;
-- ... must equal the F7 kernel over the same window
SELECT 'kern_incr', value FROM timeless_window('ck','req',NULL,40,40,10,40,'increase');

-- recipe: top-k per bucket (top 2 hosts by avg cpu per minute)
WITH b AS (
  SELECT labels, (ts / 60) * 60 AS bucket_ts, AVG(value) AS v
    FROM ck WHERE name = 'cpu' AND ts >= 0 AND ts <= 60
   GROUP BY labels, bucket_ts
),
r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY bucket_ts ORDER BY v DESC) AS rn
    FROM b
)
SELECT 'topk', bucket_ts, labels, v FROM r WHERE rn <= 2 ORDER BY bucket_ts, rn;

-- recipe: cross-metric join (error ratio on shared grid points)
SELECT 'ratio', e.ts, e.value / r.value
  FROM timeless_grid('ck','errors',NULL,0,60,60,60) e
  JOIN timeless_grid('ck','requests',NULL,0,60,60,60) r
    ON r.labels = e.labels AND r.ts = e.ts;

-- recipe: IQR fences from the exact-percentile kernel
WITH fences AS (
  SELECT (SELECT value FROM timeless_window('ck','lat',NULL,100,100,1,101,'p25')) AS q1,
         (SELECT value FROM timeless_window('ck','lat',NULL,100,100,1,101,'p75')) AS q3
)
SELECT 'iqr', ROUND(AVG(value), 4)
  FROM ck, fences
 WHERE name = 'lat' AND ts > -1 AND ts <= 100
   AND value BETWEEN q1 - 1.5 * (q3 - q1) AND q3 + 1.5 * (q3 - q1);

-- recipe: 2-sigma exclusion in plain SQL
WITH stats AS (
  SELECT AVG(value) AS mu,
         sqrt(AVG(value * value) - AVG(value) * AVG(value)) AS sigma
    FROM ck WHERE name = 'lat' AND ts > -1 AND ts <= 100
)
SELECT 'sigma', ROUND(AVG(value), 4)
  FROM ck, stats
 WHERE name = 'lat' AND ts > -1 AND ts <= 100
   AND ABS(value - mu) <= 2 * sigma;

-- recipe: portable gap-fill (generate_series LEFT JOIN) ...
SELECT 'gs', gs.value, COALESCE(g.value, '-')
  FROM generate_series(0, 120, 30) gs
  LEFT JOIN timeless_grid('ck','cpu','{"host":"a"}',0,120,30,30) g
    ON g.ts = gs.value ORDER BY gs.value;
-- ... must equal the native fill='null' emission
SELECT 'nf', ts, COALESCE(value, '-')
  FROM timeless_grid('ck','cpu','{"host":"a"}',0,120,30,30,'null') ORDER BY ts;

-- recipe: discovery
SELECT 'lv', value FROM timeless_label_values('ck','cpu','host');

-- SQL-PROM-006: root range selector at T=60 over (0,60]
SELECT 'range_selector', labels, ts, value
  FROM timeless_raw('ck','cpu','{"host":"a"}',0,60)
 WHERE ts > 0 AND ts <= 60
 ORDER BY labels, ts;
-- SQL-PROM-007: bounded packed foundations
SELECT 'bounded_raw', length(frame) > 0
  FROM timeless_raw_frame('ck','cpu','{"host":"a"}',0,60,2);
SELECT 'bounded_window', COUNT(*)
  FROM timeless_window_batches(
    'ck','cpu','{"host":"a"}',0,60,60,60,'avg',NULL,2);
-- SQL-PROM-010: unary minus over the bounded public selector grid
SELECT 'unary_minus', labels, ts, -value
  FROM timeless_grid('ck','cpu','{"host":"a"}',0,60,60,60)
 ORDER BY labels, ts;
-- SQL-PROM-004: all arithmetic operations with default exact-label matching
WITH lhs AS (
  SELECT labels, ts, value
    FROM timeless_grid('ck','arith_lhs',NULL,60,60,60,60)
), rhs AS (
  SELECT labels, ts, value
    FROM timeless_grid('ck','arith_rhs',NULL,60,60,60,60)
)
SELECT 'arithmetic', lhs.labels, lhs.ts,
       lhs.value + rhs.value,
       lhs.value - rhs.value,
       lhs.value * rhs.value,
       lhs.value / rhs.value,
       lhs.value % rhs.value,
       pow(lhs.value, rhs.value)
  FROM lhs JOIN rhs USING(labels, ts);
-- SQL-PROM-011: comparison filter and bool over the public grid
SELECT 'comparison_filter', labels, ts, value
  FROM timeless_grid('ck','cpu','{"host":"a"}',0,60,60,60)
 WHERE value > 5
 ORDER BY labels, ts;
SELECT 'comparison_bool', labels, ts, CAST(value > 5 AS REAL)
  FROM timeless_grid('ck','cpu','{"host":"a"}',0,60,60,60)
 ORDER BY labels, ts;
-- SQL-PROM-012: step-local set membership over public grids
WITH lhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','set_lhs',NULL,60,60,60,60)
), rhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','set_rhs',NULL,60,60,60,60)
)
SELECT 'set_and',lhs.labels,lhs.ts,lhs.value FROM lhs
 WHERE EXISTS (SELECT 1 FROM rhs WHERE rhs.labels=lhs.labels AND rhs.ts=lhs.ts)
 ORDER BY lhs.labels,lhs.ts;
WITH lhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','set_lhs',NULL,60,60,60,60)
), rhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','set_rhs',NULL,60,60,60,60)
)
SELECT 'set_unless',lhs.labels,lhs.ts,lhs.value FROM lhs
 WHERE NOT EXISTS (SELECT 1 FROM rhs WHERE rhs.labels=lhs.labels AND rhs.ts=lhs.ts)
 ORDER BY lhs.labels,lhs.ts;
WITH lhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','set_lhs',NULL,60,60,60,60)
), rhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','set_rhs',NULL,60,60,60,60)
)
SELECT 'set_or',labels,ts,value FROM (
  SELECT labels,ts,value FROM lhs
  UNION ALL
  SELECT rhs.labels,rhs.ts,rhs.value FROM rhs
   WHERE NOT EXISTS (
     SELECT 1 FROM lhs WHERE lhs.labels=rhs.labels AND lhs.ts=rhs.ts
   )
) ORDER BY labels,ts;
-- SQL-PROM-013: explicit on/ignoring label keys over public grids
WITH lhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','matching_lhs',NULL,60,60,60,60)
), rhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','matching_rhs',NULL,60,60,60,60)
)
SELECT 'matching_on',
       json_object('host',COALESCE(json_extract(lhs.labels,'$.host'),'')),
       lhs.ts,lhs.value+rhs.value
  FROM lhs JOIN rhs
    ON rhs.ts=lhs.ts
   AND COALESCE(json_extract(rhs.labels,'$.host'),'') =
       COALESCE(json_extract(lhs.labels,'$.host'),'')
 ORDER BY 2,3;
WITH lhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','matching_lhs',NULL,60,60,60,60)
), rhs AS (
  SELECT labels,ts,value FROM timeless_grid('ck','matching_rhs',NULL,60,60,60,60)
)
SELECT 'matching_ignoring',json_remove(lhs.labels,'$.zone'),
       lhs.ts,lhs.value+rhs.value
  FROM lhs JOIN rhs
    ON rhs.ts=lhs.ts
   AND json_remove(rhs.labels,'$.zone')=json_remove(lhs.labels,'$.zone')
 ORDER BY 2,3;
-- SQL-PROM-014: explicit group_left/group_right many/one joins
WITH many AS (
  SELECT labels,ts,value FROM timeless_grid('ck','group_many_lhs',NULL,60,60,60,60)
), one AS (
  SELECT labels,ts,value FROM timeless_grid('ck','group_one_rhs',NULL,60,60,60,60)
)
SELECT 'group_left',
       json_set(many.labels,'$.team',json_extract(one.labels,'$.team')),
       many.ts,many.value+one.value
  FROM many JOIN one
    ON one.ts=many.ts
   AND COALESCE(json_extract(one.labels,'$.host'),'') =
       COALESCE(json_extract(many.labels,'$.host'),'')
 ORDER BY 2,3;
WITH one AS (
  SELECT labels,ts,value FROM timeless_grid('ck','group_one_lhs',NULL,60,60,60,60)
), many AS (
  SELECT labels,ts,value FROM timeless_grid('ck','group_many_rhs',NULL,60,60,60,60)
)
SELECT 'group_right',
       json_set(many.labels,'$.team',json_extract(one.labels,'$.team')),
       many.ts,one.value-many.value
  FROM one JOIN many
    ON one.ts=many.ts
   AND COALESCE(json_extract(one.labels,'$.host'),'') =
       COALESCE(json_extract(many.labels,'$.host'),'')
 ORDER BY 2,3;
WITH one AS (
  SELECT labels,ts FROM timeless_grid('ck','group_one_rhs',NULL,60,60,60,60)
)
SELECT 'group_duplicate_count',COUNT(*) FROM (
  SELECT COALESCE(json_extract(labels,'$.host'),'') key,ts
    FROM one GROUP BY key,ts HAVING COUNT(*)>1
);
SQL
)
check_eq "pure-SQL reset-corrected increase == F7 kernel (45 over (0,40])" \
  "$(grep -E '^(sql_incr|kern_incr)\|' <<<"$got")" \
'sql_incr|45.0
kern_incr|45.0'
check_eq "top-2 hosts per bucket" \
  "$(grep '^topk|' <<<"$got")" \
'topk|0|{"host":"c"}|3.0
topk|0|{"host":"b"}|2.0
topk|60|{"host":"b"}|20.0
topk|60|{"host":"c"}|15.0'
check_eq "cross-metric error ratio on shared grid" \
  "$(grep '^ratio|' <<<"$got")" "ratio|60|0.1"
check_eq "IQR fences and 2-sigma both exclude the outlier (robust avg 11.3)" \
  "$(grep -E '^(iqr|sigma)\|' <<<"$got")" \
'iqr|11.3
sigma|11.3'
check_eq "generate_series LEFT JOIN == native fill='null'" \
  "$(grep '^gs|' <<<"$got" | sed 's/^gs|//')" \
  "$(grep '^nf|' <<<"$got" | sed 's/^nf|//')"
check_eq "gap-fill shape itself (0=1.0, 60=10.0, rest NULL)" \
  "$(grep '^nf|' <<<"$got")" \
'nf|0|1.0
nf|30|-
nf|60|10.0
nf|90|-
nf|120|-'
check_eq "label discovery (cookbook)" \
  "$(grep '^lv|' <<<"$got")" \
'lv|a
lv|b
lv|c'
check_eq "SQL-PROM-006 range selector uses exact (T-W,T] bounds" \
  "$(grep '^range_selector|' <<<"$got")" \
'range_selector|{"host":"a"}|60|10.0'
check_eq "SQL-PROM-007 bounds packed raw and window storage work" \
  "$(grep -E '^bounded_(raw|window)\|' <<<"$got")" \
$'bounded_raw|1\nbounded_window|1'
check_eq "SQL-PROM-010 negates a bounded public selector grid" \
  "$(grep '^unary_minus|' <<<"$got")" \
$'unary_minus|{"host":"a"}|0|-1.0\nunary_minus|{"host":"a"}|60|-10.0'
check_eq "SQL-PROM-004 executes every arithmetic operator with exact-label matching" \
  "$(grep '^arithmetic|' <<<"$got")" \
'arithmetic|{"host":"a"}|60|10.0|6.0|16.0|4.0|0.0|64.0'
check_eq "SQL-PROM-011 filters or maps comparisons over a public grid" \
  "$(grep -E '^comparison_(filter|bool)\|' <<<"$got")" \
$'comparison_filter|{"host":"a"}|60|10.0\ncomparison_bool|{"host":"a"}|0|0.0\ncomparison_bool|{"host":"a"}|60|1.0'
check_eq "SQL-PROM-012 executes step-local many-to-many set membership" \
  "$(grep -E '^set_(and|unless|or)\|' <<<"$got")" \
$'set_and|{"host":"a"}|60|10.0\nset_and|{"host":"b"}|60|20.0\nset_unless|{"host":"c"}|60|30.0\nset_or|{"host":"a"}|60|10.0\nset_or|{"host":"b"}|60|20.0\nset_or|{"host":"c"}|60|30.0\nset_or|{"host":"d"}|60|400.0'
check_eq "SQL-PROM-013 executes on/ignoring keys and result projection" \
  "$(grep -E '^matching_(on|ignoring)\|' <<<"$got")" \
$'matching_on|{"host":"a"}|60|10.0\nmatching_ignoring|{"host":"a","shared":"x"}|60|10.0'
check_eq "SQL-PROM-014 executes both group directions after uniqueness preflight" \
  "$(grep -E '^group_(left|right|duplicate_count)\|' <<<"$got")" \
$'group_left|{"host":"a","pod":"p1","team":"core"}|60|10.0\ngroup_left|{"host":"a","pod":"p2","team":"core"}|60|11.0\ngroup_right|{"host":"a","pod":"p1","team":"core"}|60|6.0\ngroup_right|{"host":"a","pod":"p2","team":"core"}|60|5.0\ngroup_duplicate_count|0'

# ---------------------------------------------------------------------------
echo "== section 34: embedding waist + resolved-series batch =="
E34DB="$TMP/embed34.db"
E34BLOB="$TMP/resolved_v1.blob"
python3 - "$E34BLOB" <<'PY'
import struct, sys
out = bytearray(struct.pack('<BBHI', 2, 0, 0, 3))
out += struct.pack('<qqq', 1, 2, 1)
out += struct.pack('<qqq', 10, 10, 20)
out += struct.pack('<ddd', 1.5, 2.5, 3.5)
open(sys.argv[1], 'wb').write(out)
PY
got=$(sqlite3 "$E34DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE em USING timeless_metrics;
INSERT INTO em(em,name,labels) VALUES ('resolve','cpu','{"host":"web-1","env":"prod"}');
SELECT 'sid1', last_insert_rowid();
INSERT INTO em(em,name,labels) VALUES ('resolve','cpu','{"host":"db-1"}');
SELECT 'sid2', last_insert_rowid();
INSERT INTO em(em) VALUES (readfile('$E34BLOB'));
INSERT INTO em(em) VALUES ('flush');
SELECT 'raw', series_id, labels, ts, value
  FROM timeless_raw('em','cpu','{"host":{"re":"web-.*"}}',0,30)
 ORDER BY ts;
SELECT 'raw_batch', series_id, length(points), hex(substr(points,1,4))
  FROM timeless_raw_batches('em','cpu','{"host":{"re":"web-.*"}}',0,30);
SELECT 'raw_frame', length(frame), hex(substr(frame,1,16))
  FROM timeless_raw_frame('em','cpu','{"host":{"re":"web-.*"}}',0,30);
SELECT 'raw_frame_limit', length(frame)
  FROM timeless_raw_frame('em','cpu','{"host":{"re":"web-.*"}}',0,30,2);
SELECT 'empty_frame', COUNT(*)
  FROM timeless_raw_frame('em','cpu','{"host":"missing"}',0,30);
SELECT 'raw_profile',
       MAX(CASE WHEN key='raw_batch_query_count' THEN value END),
       MAX(CASE WHEN key='raw_batch_query_series_considered' THEN value END),
       MAX(CASE WHEN key='raw_batch_query_candidate_chunks' THEN value END),
       MAX(CASE WHEN key='raw_batch_query_payload_bytes_read' THEN value > 0 END),
       MAX(CASE WHEN key='raw_batch_query_decoded_points' THEN value END),
       MAX(CASE WHEN key='raw_batch_query_returned_points' THEN value END)
  FROM timeless_stats('em');
BEGIN;
INSERT INTO em(em) VALUES (readfile('$E34BLOB'));
SELECT 'raw_frame_tx', length(frame)
  FROM timeless_raw_frame('em','cpu','{"host":{"re":"web-.*"}}',0,30,4);
ROLLBACK;
SELECT 'raw_frame_rollback', length(frame)
  FROM timeless_raw_frame('em','cpu','{"host":{"re":"web-.*"}}',0,30,2);
SELECT 'catalog', name, labels, series_id FROM timeless_series('em') ORDER BY series_id;
SQL
)
check_eq "resolve command returns durable catalog ids" \
  "$(grep '^sid' <<<"$got")" $'sid1|1\nsid2|2'
check_eq "resolved batch + matcher-aware raw waist" \
  "$(grep '^raw|' <<<"$got")" \
'raw|1|{"env":"prod","host":"web-1"}|10|1.5
raw|1|{"env":"prod","host":"web-1"}|20|3.5'
check_eq "raw batch emits one packed blob per series" \
  "$(grep '^raw_batch|' <<<"$got")" 'raw_batch|1|36|02000000'
check_eq "raw frame emits one versioned columnar blob for all non-empty series" \
  "$(grep -E '^(raw_frame|raw_frame_limit|empty_frame)\|' <<<"$got")" \
$'raw_frame|60|54524631010000000200000000000000\nraw_frame_limit|60\nempty_frame|0'
check_eq "raw frame work limits preserve transaction and rollback visibility" \
  "$(grep -E '^raw_frame_(tx|rollback)\|' <<<"$got")" \
$'raw_frame_tx|92\nraw_frame_rollback|60'
check_eq "packed raw stats expose candidates, decode, bytes, and returned work" \
  "$(grep '^raw_profile|' <<<"$got")" \
  'raw_profile|4|3|3|1|6|6'
check_eq "resolved empty-series catalog is queryable" \
  "$(grep '^catalog|' <<<"$got")" \
'catalog|cpu|{"env":"prod","host":"web-1"}|1
catalog|cpu|{"host":"db-1"}|2'

if err=$(sqlite3 "$E34DB" ".load $EXT" \
  "SELECT length(frame) FROM timeless_raw_frame('em','cpu','{\"host\":{\"re\":\"web-.*\"}}',0,30,1);" 2>&1); then
  fail "raw frame max_work_points should reject excess candidate work"
elif [[ "$err" == *"work point limit 1 exceeded (candidate points: 2)"* ]]; then
  pass "raw frame max_work_points rejects before excess decode"
else
  fail "raw frame max_work_points returned the wrong error"
  printf '%s\n' "$err"
fi

got=$(sqlite3 "$E34DB" <<SQL
.load $EXT
SELECT 'reopen_frame', length(frame), hex(substr(frame,1,16))
  FROM timeless_raw_frame('em','cpu',NULL,0,30);
SELECT 'reopen_limited_frame', length(frame)
  FROM timeless_raw_frame('em','cpu',NULL,0,30,3);
SQL
)
check_eq "raw frame survives a new-process reopen" "$got" \
  $'reopen_frame|88|54524631020000000300000000000000\nreopen_limited_frame|88'

for invalid_limit in 0 -1 NULL 1.5 "'bad'"; do
  err=$(sqlite3 "$E34DB" ".load $EXT" \
    "SELECT length(frame) FROM timeless_raw_frame('em','cpu',NULL,0,30,$invalid_limit);" 2>&1 || true)
  if [[ "$err" == *"max_work_points"* ]]; then
    pass "raw frame rejects invalid max_work_points=$invalid_limit"
  else
    fail "raw frame accepted or misreported max_work_points=$invalid_limit"
    printf '%s\n' "$err"
  fi
done

# ---------------------------------------------------------------------------
echo "== section 35: native scalar aggregate TVF =="
E35DB="$TMP/aggregate35.db"
E35NAN="$TMP/aggregate35_nan.blob"
python3 - "$E35NAN" <<'PY'
import math, struct, sys
series = [(b'nan_metric', b'{"host":"mixed"}'),
          (b'nan_metric', b'{"host":"all-nan"}')]
header = struct.pack('<BBHII', 1, 0, 0, len(series), 5)
table = b''.join(struct.pack('<I', len(name)) + name +
                 struct.pack('<I', len(labels)) + labels
                 for name, labels in series)
indexes = struct.pack('<5I', 0, 0, 0, 1, 1)
timestamps = struct.pack('<5q', 0, 1, 2, 0, 1)
values = struct.pack('<5d', math.nan, 2.0, 4.0, math.nan, math.nan)
open(sys.argv[1], 'wb').write(header + table + indexes + timestamps + values)
PY
got=$(sqlite3 "$E35DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE ag USING timeless_metrics;
INSERT INTO ag(ag,name,labels) VALUES ('resolve','cpu','{"host":"empty"}');
INSERT INTO ag(name, labels, ts, value) VALUES
  ('cpu', '{"host":"a","env":"prod"}', -10, 1.0),
  ('cpu', '{"host":"a","env":"prod"}',  10, 3.0),
  ('cpu', '{"host":"a","env":"prod"}',   0, 2.0),
  ('cpu', '{"host":"a","env":"prod"}',  10, 5.0),
  ('cpu', '{"host":"b","env":"dev"}',    0, -4.0),
  ('cpu', '{"host":"b","env":"dev"}',   20, 6.0);
INSERT INTO ag(ag) VALUES (readfile('$E35NAN'));
SELECT 'buffered', labels, typeof(value), value
  FROM timeless_aggregate('ag','cpu',NULL,-10,20,'count') ORDER BY labels;
BEGIN;
INSERT INTO ag(name, labels, ts, value)
  VALUES ('cpu', '{"host":"a","env":"prod"}', 30, 100.0);
SELECT 'txn', value FROM timeless_aggregate('ag','cpu','{"host":"a"}',-10,30,'sum');
ROLLBACK;
SELECT 'rollback', value FROM timeless_aggregate('ag','cpu','{"host":"a"}',-10,30,'sum');
INSERT INTO ag(ag) VALUES ('flush');
SELECT 'native', 'avg', labels, value
  FROM timeless_aggregate('ag','cpu',NULL,-10,20,'avg')
UNION ALL
SELECT 'native', 'sum', labels, value
  FROM timeless_aggregate('ag','cpu',NULL,-10,20,'sum')
UNION ALL
SELECT 'native', 'min', labels, value
  FROM timeless_aggregate('ag','cpu',NULL,-10,20,'min')
UNION ALL
SELECT 'native', 'max', labels, value
  FROM timeless_aggregate('ag','cpu',NULL,-10,20,'max')
UNION ALL
SELECT 'native', 'count', labels, value
  FROM timeless_aggregate('ag','cpu',NULL,-10,20,'count')
ORDER BY 2, 3;
SELECT 'partial', labels, value
  FROM timeless_aggregate('ag','cpu','{"host":"a"}',0,10,'sum');
SELECT 'regex', labels, value
  FROM timeless_aggregate('ag','cpu','{"env":{"neq":"prod"}}',-10,20,'max');
SELECT 'empty_range', COUNT(*)
  FROM timeless_aggregate('ag','cpu',NULL,100,200,'avg');
SELECT 'empty_series', COUNT(*)
  FROM timeless_aggregate('ag','cpu','{"host":"empty"}',-10,20,'avg');

WITH expected AS (
  SELECT labels, AVG(value) AS value FROM ag
   WHERE name='cpu' AND ts BETWEEN -10 AND 20 GROUP BY labels
), actual AS (
  SELECT labels, value FROM timeless_aggregate('ag','cpu',NULL,-10,20,'avg')
), delta AS (
  SELECT * FROM expected EXCEPT SELECT * FROM actual
  UNION ALL
  SELECT * FROM actual EXCEPT SELECT * FROM expected
)
SELECT 'oracle_avg', COUNT(*) FROM delta;
WITH expected AS (
  SELECT labels, SUM(value) AS value FROM ag
   WHERE name='cpu' AND ts BETWEEN -10 AND 20 GROUP BY labels
), actual AS (
  SELECT labels, value FROM timeless_aggregate('ag','cpu',NULL,-10,20,'sum')
), delta AS (
  SELECT * FROM expected EXCEPT SELECT * FROM actual
  UNION ALL
  SELECT * FROM actual EXCEPT SELECT * FROM expected
)
SELECT 'oracle_sum', COUNT(*) FROM delta;
SELECT 'nan', 'avg', labels, value IS NULL, COALESCE(value, '-')
  FROM timeless_aggregate('ag','nan_metric',NULL,0,2,'avg')
UNION ALL
SELECT 'nan', 'sum', labels, value IS NULL, COALESCE(value, '-')
  FROM timeless_aggregate('ag','nan_metric',NULL,0,2,'sum')
UNION ALL
SELECT 'nan', 'min', labels, value IS NULL, COALESCE(value, '-')
  FROM timeless_aggregate('ag','nan_metric',NULL,0,2,'min')
UNION ALL
SELECT 'nan', 'max', labels, value IS NULL, COALESCE(value, '-')
  FROM timeless_aggregate('ag','nan_metric',NULL,0,2,'max')
UNION ALL
SELECT 'nan', 'count', labels, value IS NULL, COALESCE(value, '-')
  FROM timeless_aggregate('ag','nan_metric',NULL,0,2,'count')
ORDER BY 2, 3;
SQL
)
check_eq "aggregate sees buffered rows and count stays SQLite INTEGER" \
  "$(grep '^buffered|' <<<"$got")" \
'buffered|{"env":"dev","host":"b"}|integer|2
buffered|{"env":"prod","host":"a"}|integer|4'
check_eq "aggregate transaction visibility rolls back exactly" \
  "$(grep -E '^(txn|rollback)\|' <<<"$got")" \
$'txn|111.0\nrollback|11.0'
check_eq "all native scalar operations and duplicate timestamps" \
  "$(grep '^native|' <<<"$got")" \
'native|avg|{"env":"dev","host":"b"}|1.0
native|avg|{"env":"prod","host":"a"}|2.75
native|count|{"env":"dev","host":"b"}|2
native|count|{"env":"prod","host":"a"}|4
native|max|{"env":"dev","host":"b"}|6.0
native|max|{"env":"prod","host":"a"}|5.0
native|min|{"env":"dev","host":"b"}|-4.0
native|min|{"env":"prod","host":"a"}|1.0
native|sum|{"env":"dev","host":"b"}|2.0
native|sum|{"env":"prod","host":"a"}|11.0'
check_eq "aggregate bounds, matcher, and empty omission" \
  "$(grep -E '^(partial|regex|empty_)' <<<"$got")" \
'partial|{"env":"prod","host":"a"}|10.0
regex|{"env":"dev","host":"b"}|6.0
empty_range|0
empty_series|0'
check_eq "aggregate matches flat SQL avg/sum oracle on the pinned dataset" \
  "$(grep '^oracle_' <<<"$got")" $'oracle_avg|0\noracle_sum|0'
check_eq "aggregate NaN contract: count includes, sum/avg propagate, min/max ignore" \
  "$(grep '^nan|' <<<"$got")" \
'nan|avg|{"host":"all-nan"}|1|-
nan|avg|{"host":"mixed"}|1|-
nan|count|{"host":"all-nan"}|0|2
nan|count|{"host":"mixed"}|0|3
nan|max|{"host":"all-nan"}|1|-
nan|max|{"host":"mixed"}|0|4.0
nan|min|{"host":"all-nan"}|1|-
nan|min|{"host":"mixed"}|0|2.0
nan|sum|{"host":"all-nan"}|1|-
nan|sum|{"host":"mixed"}|1|-'

got=$(sqlite3 "$E35DB" <<SQL
.load $EXT
SELECT 'reopen', labels, typeof(value), value
  FROM timeless_aggregate('ag','cpu',NULL,-10,20,'count') ORDER BY labels;
SQL
)
check_eq "aggregate survives a new-process reopen" "$got" \
'reopen|{"env":"dev","host":"b"}|integer|2
reopen|{"env":"prod","host":"a"}|integer|4'

err=$(sqlite3 "$E35DB" ".load $EXT" \
  "SELECT * FROM timeless_aggregate('ag','cpu',NULL,0,10,'median');" 2>&1 || true)
check_eq "unknown aggregate rejected with the supported set" \
  "$(grep -c 'expected one of: avg, sum, min, max, count' <<<"$err")" "1"
err=$(sqlite3 "$E35DB" ".load $EXT" \
  "SELECT * FROM timeless_aggregate('ag','cpu',NULL,0,10);" 2>&1 || true)
check_eq "missing aggregate argument reports the call shape" \
  "$(grep -c 'tbl, metric, filter, start, stop, agg' <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 36: newest-first latest-point TVF =="
E36DB="$TMP/latest36.db"
got=$(sqlite3 "$E36DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE latest USING timeless_metrics;
INSERT INTO latest(latest,name,labels) VALUES ('resolve','cpu','{"host":"empty"}');
INSERT INTO latest(name, labels, ts, value) VALUES
  ('cpu', '{"host":"a","env":"prod"}', 10, 1.0),
  ('cpu', '{"host":"a","env":"prod"}', 30, 3.0),
  ('cpu', '{"host":"b","env":"dev"}',  20, 2.0);
SELECT 'buffered', labels, ts, value
  FROM timeless_latest('latest','cpu',NULL,0,100) ORDER BY labels;
BEGIN;
INSERT INTO latest(name, labels, ts, value)
  VALUES ('cpu', '{"host":"a","env":"prod"}', 40, 9.0);
SELECT 'txn', ts, value
  FROM timeless_latest('latest','cpu','{"host":"a"}',0,100);
ROLLBACK;
SELECT 'rollback', ts, value
  FROM timeless_latest('latest','cpu','{"host":"a"}',0,100);
INSERT INTO latest(latest) VALUES ('flush');

-- A later-created chunk with a smaller min_ts sorts first in engine order;
-- its first ts=30 duplicate is therefore the pinned winner.
INSERT INTO latest(name, labels, ts, value) VALUES
  ('cpu', '{"host":"a","env":"prod"}', 30, 4.0),
  ('cpu', '{"host":"a","env":"prod"}',  5, 0.5);
INSERT INTO latest(latest) VALUES ('flush');
INSERT INTO latest(name, labels, ts, value) VALUES
  ('cpu', '{"host":"a","env":"prod"}', 30, 5.0),
  ('cpu', '{"host":"a","env":"prod"}', 40, 6.0);
SELECT 'tie', labels, ts, value
  FROM timeless_latest('latest','cpu','{"host":"a"}',0,30);
SELECT 'newest', labels, ts, value
  FROM timeless_latest('latest','cpu','{"host":"a"}',0,100);
SELECT 'bounded', labels, ts, value
  FROM timeless_latest('latest','cpu','{"host":"a"}',6,29);
SELECT 'matcher', labels, ts, value
  FROM timeless_latest('latest','cpu','{"env":{"neq":"prod"}}',0,100);
SELECT 'empty_range', COUNT(*)
  FROM timeless_latest('latest','cpu',NULL,100,200);
SELECT 'reverse_range', COUNT(*)
  FROM timeless_latest('latest','cpu',NULL,10,9);
SELECT 'empty_series', COUNT(*)
  FROM timeless_latest('latest','cpu','{"host":"empty"}',0,100);
INSERT INTO latest(latest) VALUES ('flush');
INSERT INTO latest(latest) VALUES ('compact');
SELECT 'compact', labels, ts, value
  FROM timeless_latest('latest','cpu','{"host":"a"}',0,100);
SQL
)
check_eq "latest sees buffered rows and transaction rollback exactly" \
  "$(grep -E '^(buffered|txn|rollback)\|' <<<"$got")" \
'buffered|{"env":"dev","host":"b"}|20|2.0
buffered|{"env":"prod","host":"a"}|30|3.0
txn|40|9.0
rollback|30|3.0'
check_eq "latest preserves duplicate ties, bounds, matchers, and omission" \
  "$(grep -E '^(tie|newest|bounded|matcher|empty_|reverse_)' <<<"$got")" \
'tie|{"env":"prod","host":"a"}|30|4.0
newest|{"env":"prod","host":"a"}|40|6.0
bounded|{"env":"prod","host":"a"}|10|1.0
matcher|{"env":"dev","host":"b"}|20|2.0
empty_range|0
reverse_range|0
empty_series|0'
check_eq "latest survives compaction" "$(grep '^compact|' <<<"$got")" \
  'compact|{"env":"prod","host":"a"}|40|6.0'

got=$(sqlite3 "$E36DB" <<SQL
.load $EXT
SELECT 'reopen', labels, ts, value
  FROM timeless_latest('latest','cpu',NULL,0,100) ORDER BY labels;
SQL
)
check_eq "latest survives a new-process reopen" "$got" \
'reopen|{"env":"dev","host":"b"}|20|2.0
reopen|{"env":"prod","host":"a"}|40|6.0'

# Simulate a database created before max_ts_val existed. Reopen must add the
# nullable column and old rows must take the exact decode fallback.
sqlite3 "$E36DB" "ALTER TABLE latest_chunks DROP COLUMN max_ts_val;"
got=$(sqlite3 "$E36DB" <<SQL
.load $EXT
SELECT 'migrated', labels, ts, value
  FROM timeless_latest('latest','cpu',NULL,0,100) ORDER BY labels;
SELECT 'column', COUNT(*) FROM pragma_table_info('latest_chunks') WHERE name='max_ts_val';
SQL
)
check_eq "latest migrates legacy schema and decodes legacy chunks" "$got" \
'migrated|{"env":"dev","host":"b"}|20|2.0
migrated|{"env":"prod","host":"a"}|40|6.0
column|1'

E36RET="$TMP/latest36_retention.db"
got=$(sqlite3 "$E36RET" <<SQL
.load $EXT
CREATE VIRTUAL TABLE r USING timeless_metrics(retention='10s');
INSERT INTO r(name,labels,ts,value) VALUES ('cpu','{"host":"a"}',0,1.0);
INSERT INTO r(r) VALUES ('flush');
INSERT INTO r(name,labels,ts,value) VALUES ('cpu','{"host":"a"}',100,2.0);
INSERT INTO r(r) VALUES ('flush');
SELECT 'retention', ts, value FROM timeless_latest('r','cpu',NULL,0,200);
SELECT 'pruned', COUNT(*) FROM timeless_latest('r','cpu',NULL,0,50);
SQL
)
check_eq "latest follows automatic retention" "$got" $'retention|100|2.0\npruned|0'

E36PUB="$TMP/latest36_publication.db"
got=$(EXT_PATH="$EXT" DB_PATH="$E36PUB" python3 - <<'PY'
import os, sqlite3

db = os.environ["DB_PATH"]
ext = os.environ["EXT_PATH"]
c1 = sqlite3.connect(db, timeout=10)
c2 = sqlite3.connect(db, timeout=10)
for conn in (c1, c2):
    conn.enable_load_extension(True)
    conn.load_extension(ext)
c1.execute("CREATE VIRTUAL TABLE m USING timeless_metrics")
c1.commit()
c1.execute("INSERT INTO m(name,labels,ts,value) VALUES ('cpu','{\"host\":\"a\"}',10,1.0)")
# Publish the transaction but deliberately do not flush: B must see the shared,
# pre-durable buffer without ever seeing transaction-private shadow rows.
c1.commit()
row = c2.execute(
    "SELECT labels,ts,value FROM timeless_latest('m','cpu',NULL,0,100)"
).fetchone()
print("publication|%s|%d|%.1f" % row)
c1.close()
c2.close()
PY
)
check_eq "latest publishes committed buffered writes across live connections" "$got" \
  'publication|{"host":"a"}|10|1.0'

err=$(sqlite3 "$E36DB" ".load $EXT" \
  "SELECT * FROM timeless_latest('latest','cpu',NULL,0);" 2>&1 || true)
check_eq "missing latest argument reports the call shape" \
  "$(grep -c 'tbl, metric, filter, start, stop' <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 37: authoritative catalog publication and invalidation =="
E37DB="$TMP/catalog37.db"
got=$(EXT_PATH="$EXT" DB_PATH="$E37DB" python3 - <<'PY'
import os, sqlite3, subprocess

db = os.environ["DB_PATH"]
ext = os.environ["EXT_PATH"]

def connect():
    conn = sqlite3.connect(db, timeout=10)
    conn.isolation_level = None
    conn.enable_load_extension(True)
    conn.load_extension(ext)
    return conn

def rows(conn, name=None):
    if name is None:
        return conn.execute(
            "SELECT name,ts,value FROM m ORDER BY name,ts,value"
        ).fetchall()
    return conn.execute(
        "SELECT name,ts,value FROM m WHERE name=? ORDER BY ts,value", (name,)
    ).fetchall()

A = connect()
B = connect()
A.execute("CREATE VIRTUAL TABLE m USING timeless_metrics")
# Establish B's committed empty generation before A publishes anything.
assert B.execute("SELECT count(*) FROM timeless_series('m')").fetchone()[0] == 0

A.execute("INSERT INTO m(name,ts,value) VALUES ('cpu',10,1.0)")
A.execute("INSERT INTO m(name,ts,value) VALUES ('cpu',20,2.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
print("commit", rows(B, "cpu"))

# Capture occurs in xSync, but rollback must never publish that token. This
# transaction also mutates both series buffers and the durable chunk catalog.
A.execute("BEGIN")
A.execute("INSERT INTO m(name,ts,value) VALUES ('cpu',30,99.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
A.execute("INSERT INTO m(m) VALUES ('prune:25')")
A.execute("ROLLBACK")
print("rollback", rows(B, "cpu"))

# Compaction rewrites rowids while preserving the shared engine's exact view.
A.execute("INSERT INTO m(name,ts,value) VALUES ('cpu',30,3.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
A.execute("INSERT INTO m(m) VALUES ('compact')")
print("compact", rows(B, "cpu"), B.execute("SELECT count(*) FROM m_chunks").fetchone()[0])

# Prune deletes an independent old-series chunk and publishes the new token.
A.execute("INSERT INTO m(name,ts,value) VALUES ('old',1,8.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
A.execute("INSERT INTO m(m) VALUES ('prune:5')")
print("prune", rows(B))

# A different process has a different engine registry. Its committed token
# must invalidate this process's published fast path and force a safe reload.
sql = ".load %s\nINSERT INTO m(name,ts,value) VALUES ('mem',40,4.0);\nINSERT INTO m(m) VALUES ('flush');\n" % ext
subprocess.run(["sqlite3", db], input=sql, text=True, check=True,
               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
print("external", rows(B))

A.close()
B.close()
C = connect()
print("reopen", rows(C), C.execute("SELECT count(*) FROM m_chunks").fetchone()[0])
C.close()
PY
) || { fail "section 37 python driver crashed"; got=""; }
check_eq "catalog commit publishes to an already-open reader" \
  "$(grep '^commit ' <<<"$got")" \
  "commit [('cpu', 10, 1.0), ('cpu', 20, 2.0)]"
check_eq "catalog rollback discards the prepared generation" \
  "$(grep '^rollback ' <<<"$got")" \
  "rollback [('cpu', 10, 1.0), ('cpu', 20, 2.0)]"
check_eq "catalog compaction publication preserves rows and swaps chunks" \
  "$(grep '^compact ' <<<"$got")" \
  "compact [('cpu', 10, 1.0), ('cpu', 20, 2.0), ('cpu', 30, 3.0)] 1"
check_eq "catalog prune publication removes only the old chunk" \
  "$(grep '^prune ' <<<"$got")" \
  "prune [('cpu', 10, 1.0), ('cpu', 20, 2.0), ('cpu', 30, 3.0)]"
check_eq "external-process generation invalidates the local fast path" \
  "$(grep '^external ' <<<"$got")" \
  "external [('cpu', 10, 1.0), ('cpu', 20, 2.0), ('cpu', 30, 3.0), ('mem', 40, 4.0)]"
check_eq "catalog publication state survives reopen" \
  "$(grep '^reopen ' <<<"$got")" \
  "reopen [('cpu', 10, 1.0), ('cpu', 20, 2.0), ('cpu', 30, 3.0), ('mem', 40, 4.0)] 2"

# ---------------------------------------------------------------------------
echo "== section 38: matcher and discovery pushdown =="
E38DB="$TMP/matcher38.db"
got=$(EXT_PATH="$EXT" DB_PATH="$E38DB" python3 - <<'PY'
import os, sqlite3

db = os.environ["DB_PATH"]
ext = os.environ["EXT_PATH"]

def connect():
    conn = sqlite3.connect(db, timeout=10)
    conn.isolation_level = None
    conn.enable_load_extension(True)
    conn.load_extension(ext)
    return conn

def values(rows):
    return "|".join(row[0] for row in rows)

A = connect()
B = connect()
A.execute("CREATE VIRTUAL TABLE m USING timeless_metrics")
A.executemany("INSERT INTO m(name,labels,ts,value) VALUES ('cpu',?,?,?)", [
    ('{"code":"a","env":"prod","host":"web-1"}', 10, 1.0),
    ('{"code":"é","env":"dev","host":"web-2"}', 10, 2.0),
    ('{"host":"db-1"}', 10, 3.0),
    ('{"env":"","host":"empty"}', 10, 4.0),
])

# The second live connection sees buffered catalog rows through matcher-aware
# discovery before any flush.
print("buffered|" + values(B.execute(
    "SELECT labels FROM timeless_series('m','cpu','{\"host\":{\"re\":\"web-.*\"}}') ORDER BY labels"
)))
A.execute("INSERT INTO m(m) VALUES ('flush')")

print("combined|" + values(B.execute(
    "SELECT labels FROM timeless_series('m','cpu','{\"host\":{\"re\":\"web-.*\"},\"env\":{\"neq\":\"dev\"}}') ORDER BY labels"
)))
print("absent|" + values(B.execute(
    "SELECT labels FROM timeless_series('m','cpu','{\"env\":{\"re\":\"\"}}') ORDER BY labels"
)))
print("label_values|" + values(B.execute(
    "SELECT value FROM timeless_label_values('m','cpu','host','{\"env\":{\"neq\":\"dev\"}}')"
)))
print("raw|%d" % B.execute(
    "SELECT count(*) FROM timeless_raw_batches('m','cpu','{\"host\":{\"re\":\"web-.*\"},\"env\":{\"neq\":\"dev\"}}',0,20)"
).fetchone()[0])

A.execute("BEGIN")
A.execute("INSERT INTO m(name,labels,ts,value) VALUES ('cpu','{\"host\":\"tmp\"}',20,9.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
A.execute("ROLLBACK")
print("rollback|%d" % B.execute(
    "SELECT count(*) FROM timeless_series('m','cpu','{\"host\":\"tmp\"}')"
).fetchone()[0])

A.close()
B.close()
C = connect()
print("reopen|%d" % C.execute(
    "SELECT count(*) FROM timeless_series('m','cpu','{\"host\":{\"re\":\"web-.*\"}}')"
).fetchone()[0])
C.close()
PY
) || { fail "section 38 python driver crashed"; got=""; }
check_eq "filtered discovery sees buffered rows across live connections" \
  "$(grep '^buffered|' <<<"$got")" \
  'buffered|{"code":"a","env":"prod","host":"web-1"}|{"code":"é","env":"dev","host":"web-2"}'
check_eq "regex and negative matchers combine before catalog rows cross SQL" \
  "$(grep '^combined|' <<<"$got")" \
  'combined|{"code":"a","env":"prod","host":"web-1"}'
check_eq "absent labels match the empty-string discovery contract" \
  "$(grep '^absent|' <<<"$got")" \
  $'absent|{"env":"","host":"empty"}|{"host":"db-1"}'
check_eq "filtered label values omit absent keys and stay sorted" \
  "$(grep '^label_values|' <<<"$got")" 'label_values|db-1|empty|web-1'
check_eq "selective matchers reach raw chunks and rollback/reopen stay exact" \
  "$(grep -E '^(raw|rollback|reopen)\|' <<<"$got")" \
  $'raw|1\nrollback|0\nreopen|2'

err=$(sqlite3 "$E38DB" ".load $EXT" \
  "SELECT * FROM timeless_series('m','cpu','{\"host\":{\"re\":\"[\"}}');" 2>&1 || true)
check_eq "direct discovery rejects an invalid regex with label context" \
  "$(grep -c 'invalid regex.*host' <<<"$err")" "1"

# ---------------------------------------------------------------------------
echo "== section 39: reader gate hides transaction-private chunk rows =="
E39DB="$TMP/read_gate39.db"
got=$(EXT_PATH="$EXT" DB_PATH="$E39DB" python3 - <<'PY'
import os, sqlite3

db = os.environ["DB_PATH"]
ext = os.environ["EXT_PATH"]

def connect():
    conn = sqlite3.connect(db, timeout=10)
    conn.isolation_level = None
    conn.enable_load_extension(True)
    conn.load_extension(ext)
    return conn

def rows(conn):
    return conn.execute(
        "SELECT ts,value FROM timeless_raw('m','cpu',NULL,0,100) ORDER BY ts,value"
    ).fetchall()

A = connect()
B = connect()
A.execute("CREATE VIRTUAL TABLE m USING timeless_metrics")
A.execute("INSERT INTO m(name,ts,value) VALUES ('cpu',10,1.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
assert rows(B) == [(10, 1.0)]

# The shared engine now indexes row 20, but only A's transaction can see its
# shadow-table row. Before the read gate, B followed that location and failed
# with `chunk row ... Query returned no rows`.
A.execute("BEGIN")
A.execute("INSERT INTO m(name,ts,value) VALUES ('cpu',20,2.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
try:
    rows(B)
    print("during|UNEXPECTED-SUCCESS")
except sqlite3.OperationalError as error:
    print("during|" + str(error))
A.execute("ROLLBACK")
print("rollback|" + repr(rows(B)))

A.execute("BEGIN")
A.execute("INSERT INTO m(name,ts,value) VALUES ('cpu',30,3.0)")
A.execute("INSERT INTO m(m) VALUES ('flush')")
A.execute("COMMIT")
print("commit|" + repr(rows(B)))

A.close()
B.close()
PY
) || { fail "section 39 python driver crashed"; got=""; }
check_eq "reader reports a retryable transaction conflict, never a dangling row" \
  "$(grep -c '^during|.*active write transaction.*retry.*SQLITE_BUSY' <<<"$got")" "1"
check_eq "rollback restores the reader's committed chunk view" \
  "$(grep '^rollback|' <<<"$got")" \
  'rollback|[(10, 1.0)]'
check_eq "commit publishes the replacement view before the reader retries" \
  "$(grep '^commit|' <<<"$got")" \
  'commit|[(10, 1.0), (30, 3.0)]'

# ---------------------------------------------------------------------------
echo "== section 40: durable series-id read constraints =="
E40DB="$TMP/series_id40.db"
sqlite3 "$E40DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE m USING timeless_metrics(rollups='10s@0');
INSERT INTO m(name,labels,ts,value) VALUES
  ('cpu','{"host":"a"}',0,1.0),
  ('cpu','{"host":"a"}',10,3.0),
  ('cpu','{"host":"a"}',20,5.0),
  ('cpu','{"host":"b"}',0,9.0),
  ('cpu','{"host":"b"}',10,11.0),
  ('mem','{"host":"a"}',0,99.0);
INSERT INTO m(m) VALUES ('flush');
INSERT INTO m(m) VALUES ('rollup');
SQL

got=$(EXT_PATH="$EXT" DB_PATH="$E40DB" python3 - <<'PY'
import os, sqlite3

db = os.environ["DB_PATH"]
ext = os.environ["EXT_PATH"]
conn = sqlite3.connect(db)
conn.enable_load_extension(True)
conn.load_extension(ext)
conn.enable_load_extension(False)

sid = conn.execute(
    "SELECT series_id FROM timeless_series('m','cpu','{\"host\":\"a\"}')"
).fetchone()[0]
assert sid == 1

checks = [
    (
        "series",
        "SELECT name,labels,min_ts,max_ts,points FROM timeless_series('m') "
        "WHERE series_id=?",
        "SELECT name,labels,min_ts,max_ts,points FROM timeless_series("
        "'m','cpu','{\"host\":\"a\"}')",
    ),
    (
        "base",
        "SELECT ts,value FROM m WHERE series_id=? ORDER BY ts,value",
        "SELECT ts,value FROM m WHERE name='cpu' AND labels='{\"host\":\"a\"}' "
        "ORDER BY ts,value",
    ),
    (
        "raw",
        "SELECT ts,value FROM timeless_raw('m','cpu',NULL,0,30) "
        "WHERE series_id=? ORDER BY ts,value",
        "SELECT ts,value FROM timeless_raw('m','cpu','{\"host\":\"a\"}',0,30) "
        "ORDER BY ts,value",
    ),
    (
        "raw_batches",
        "SELECT points FROM timeless_raw_batches('m','cpu',NULL,0,30) "
        "WHERE series_id=?",
        "SELECT points FROM timeless_raw_batches('m','cpu','{\"host\":\"a\"}',0,30)",
    ),
    (
        "aggregate",
        "SELECT value FROM timeless_aggregate('m','cpu',NULL,0,30,'avg') "
        "WHERE series_id=?",
        "SELECT value FROM timeless_aggregate('m','cpu','{\"host\":\"a\"}',0,30,'avg')",
    ),
    (
        "latest",
        "SELECT ts,value FROM timeless_latest('m','cpu',NULL,0,30) WHERE series_id=?",
        "SELECT ts,value FROM timeless_latest('m','cpu','{\"host\":\"a\"}',0,30)",
    ),
    (
        "grid",
        "SELECT ts,value FROM timeless_grid('m','cpu',NULL,0,20,10,10) "
        "WHERE series_id=? ORDER BY ts",
        "SELECT ts,value FROM timeless_grid('m','cpu','{\"host\":\"a\"}',0,20,10,10) "
        "ORDER BY ts",
    ),
    (
        "window",
        "SELECT ts,value FROM timeless_window('m','cpu',NULL,0,20,10,10,'avg') "
        "WHERE series_id=? ORDER BY ts",
        "SELECT ts,value FROM timeless_window("
        "'m','cpu','{\"host\":\"a\"}',0,20,10,10,'avg') ORDER BY ts",
    ),
    (
        "window_batches",
        "SELECT buckets FROM timeless_window_batches("
        "'m','cpu',NULL,0,20,10,10,'avg') WHERE series_id=?",
        "SELECT buckets FROM timeless_window_batches("
        "'m','cpu','{\"host\":\"a\"}',0,20,10,10,'avg')",
    ),
    (
        "rollup",
        "SELECT ts,value FROM timeless_rollup('m','cpu',NULL,10,0,100,'avg') "
        "WHERE series_id=? ORDER BY ts",
        "SELECT ts,value FROM timeless_rollup("
        "'m','cpu','{\"host\":\"a\"}',10,0,100,'avg') ORDER BY ts",
    ),
    (
        "rollup_batches",
        "SELECT buckets FROM timeless_rollup_batches('m','cpu',NULL,10,0,100) "
        "WHERE series_id=?",
        "SELECT buckets FROM timeless_rollup_batches("
        "'m','cpu','{\"host\":\"a\"}',10,0,100)",
    ),
]
for name, by_id, by_labels in checks:
    selected = conn.execute(by_id, (sid,)).fetchall()
    expected = conn.execute(by_labels).fetchall()
    assert selected == expected, (name, selected, expected)
    print(f"parity|{name}|{len(selected)}")

# INTEGER affinity is part of the SQL contract once xBestIndex omits the
# original predicate. Non-integral, NULL, and malformed values match nothing.
coercions = [
    (1, 1), (1.0, 1), ("1", 1), ("+1.0e0", 1),
    (1.5, 0), ("1x", 0), (None, 0), (sqlite3.Binary(b"1"), 0),
]
for value, expected in coercions:
    count = conn.execute(
        "SELECT count(*) FROM timeless_aggregate("
        "'m','cpu',NULL,0,30,'avg') WHERE series_id=?",
        (value,),
    ).fetchone()[0]
    assert count == expected, (value, count, expected)
print("affinity|ok")

assert conn.execute(
    "SELECT count(*) FROM timeless_aggregate("
    "'m','cpu','{\"host\":\"b\"}',0,30,'avg') WHERE series_id=?",
    (sid,),
).fetchone()[0] == 0
assert conn.execute(
    "SELECT count(*) FROM timeless_aggregate("
    "'m','mem',NULL,0,30,'avg') WHERE series_id=?",
    (sid,),
).fetchone()[0] == 0
assert conn.execute(
    "SELECT count(*) FROM timeless_aggregate("
    "'m','cpu',NULL,0,30,'avg') WHERE series_id=999999"
).fetchone()[0] == 0
print("intersection|ok")

joined = conn.execute(
    "SELECT s.series_id,q.value "
    "FROM timeless_series('m','cpu',NULL) AS s "
    "JOIN timeless_aggregate('m','cpu',NULL,0,30,'avg') AS q "
    "ON q.series_id=s.series_id ORDER BY s.series_id"
).fetchall()
assert joined == [(1, 3.0), (2, 10.0)]
base_joined = conn.execute(
    "SELECT a.series_id FROM m AS a JOIN m AS b "
    "ON b.series_id=a.series_id WHERE a.ts=0 AND b.ts=10 "
    "ORDER BY a.series_id"
).fetchall()
assert base_joined == [(1,), (2,)], base_joined
plan = conn.execute(
    "EXPLAIN QUERY PLAN SELECT s.series_id,q.value "
    "FROM timeless_series('m','cpu',NULL) AS s "
    "JOIN timeless_aggregate('m','cpu',NULL,0,30,'avg') AS q "
    "ON q.series_id=s.series_id"
).fetchall()
assert any("107374" in row[3] for row in plan), plan
print("join|ok")

# Corrupt only series 2's raw payload. A selected series-1 scan still works;
# an unselected scan reaches the unrelated chunk and fails to decode it.
conn.execute("UPDATE m_chunks SET ts_data=x'00' WHERE series_id=2 AND resolution=0")
conn.commit()
assert conn.execute(
    "SELECT count(*) FROM timeless_raw('m','cpu',NULL,0,30) WHERE series_id=1"
).fetchone()[0] == 3
try:
    conn.execute(
        "SELECT count(*) FROM timeless_raw('m','cpu',NULL,0,30)"
    ).fetchone()
    raise AssertionError("broad scan unexpectedly ignored the corrupt unrelated chunk")
except sqlite3.DatabaseError as error:
    message = str(error).lower()
    assert (
        "decode" in message or "payload" in message or
        "insufficientdata" in message or "out of bounds" in message
    ), error
print("pruning|ok")
conn.close()
PY
) || { fail "section 40 series-id driver crashed"; got=""; }

check_eq "series-id output constraint matches every label-selected query surface" \
  "$(grep '^parity|' <<<"$got")" \
'parity|series|1
parity|base|3
parity|raw|3
parity|raw_batches|1
parity|aggregate|1
parity|latest|1
parity|grid|3
parity|window|3
parity|window_batches|1
parity|rollup|1
parity|rollup_batches|1'
check_eq "series-id affinity, intersections, parameterized join, and chunk pruning" \
  "$(grep -E '^(affinity|intersection|join|pruning)\|' <<<"$got")" \
$'affinity|ok\nintersection|ok\njoin|ok\npruning|ok'

# ---------------------------------------------------------------------------
echo "== section 41: packed aggregate/latest result frames =="
E41DB="$TMP/frames41.db"
got=$(PYTHONDONTWRITEBYTECODE=1 EXT_PATH="$EXT" EXAMPLES_PATH="$ROOT/examples" AGG_DB="$E35DB" \
  LATEST_DB="$E36DB" FRAME_DB="$E41DB" python3 - <<'PY'
import os, sqlite3, sys

sys.path.insert(0, os.environ["EXAMPLES_PATH"])
from query_frames import decode_aggregate_frame, decode_latest_frame

ext = os.environ["EXT_PATH"]

def connect(path):
    conn = sqlite3.connect(path, timeout=10)
    conn.enable_load_extension(True)
    conn.load_extension(ext)
    conn.enable_load_extension(False)
    return conn

agg = connect(os.environ["AGG_DB"])
modules = agg.execute(
    "SELECT name FROM pragma_module_list WHERE name IN "
    "('timeless_aggregate_frame','timeless_latest_frame') ORDER BY name"
).fetchall()
assert modules == [("timeless_aggregate_frame",), ("timeless_latest_frame",)]
print("frame_detection|ok")
for kind, name in enumerate(("avg", "sum", "min", "max", "count")):
    blob, = agg.execute(
        "SELECT frame FROM timeless_aggregate_frame("
        "'ag',?,NULL,-10,20,?)",
        ("nan_metric" if name != "count" else "cpu", name),
    ).fetchone()
    decoded_kind, decoded = decode_aggregate_frame(blob)
    assert decoded_kind == kind
    metric = "nan_metric" if name != "count" else "cpu"
    expected = agg.execute(
        "SELECT series_id,value FROM timeless_aggregate("
        "'ag',?,NULL,-10,20,?) ORDER BY series_id",
        (metric, name),
    ).fetchall()
    assert sorted(decoded) == expected, (name, decoded, expected)
assert agg.execute(
    "SELECT count(*) FROM timeless_aggregate_frame("
    "'ag','cpu','{\"host\":\"missing\"}',0,20,'avg')"
).fetchone()[0] == 0
print("aggregate_frames|5|ok")
agg.close()

latest = connect(os.environ["LATEST_DB"])
blob, = latest.execute(
    "SELECT frame FROM timeless_latest_frame('latest','cpu',NULL,0,100)"
).fetchone()
decoded = sorted(decode_latest_frame(blob))
expected = latest.execute(
    "SELECT series_id,ts,value FROM timeless_latest("
    "'latest','cpu',NULL,0,100) ORDER BY series_id"
).fetchall()
assert decoded == expected, (decoded, expected)
assert latest.execute(
    "SELECT count(*) FROM timeless_latest_frame("
    "'latest','cpu','{\"host\":\"missing\"}',0,100)"
).fetchone()[0] == 0
print("latest_frame|ok")
latest.close()

# Two live connections prove that frames retain the extension's publication
# and transaction gate rather than opening a private storage path.
frame_db = os.environ["FRAME_DB"]
a = connect(frame_db)
b = connect(frame_db)
a.execute("CREATE VIRTUAL TABLE m USING timeless_metrics")
a.commit()
a.execute(
    "INSERT INTO m(name,labels,ts,value) VALUES "
    "('cpu','{\"host\":\"a\"}',10,1.0)"
)
a.commit()
first = decode_latest_frame(b.execute(
    "SELECT frame FROM timeless_latest_frame('m','cpu',NULL,0,100)"
).fetchone()[0])
assert first == [(1, 10, 1.0)]
a.execute("BEGIN")
a.execute(
    "INSERT INTO m(name,labels,ts,value) VALUES "
    "('cpu','{\"host\":\"a\"}',20,2.0)"
)
try:
    b.execute(
        "SELECT frame FROM timeless_latest_frame('m','cpu',NULL,0,100)"
    ).fetchone()
    raise AssertionError("frame read escaped an active write transaction")
except sqlite3.OperationalError as error:
    assert "active write transaction" in str(error)
a.commit()
second = decode_latest_frame(b.execute(
    "SELECT frame FROM timeless_latest_frame('m','cpu',NULL,0,100)"
).fetchone()[0])
assert second == [(1, 20, 2.0)]
print("frame_publication|ok")
a.close()
b.close()
PY
) || { fail "section 41 frame driver crashed"; got=""; }

check_eq "independent TAF1/TLF1 decoders match rows and preserve publication" \
  "$got" \
$'frame_detection|ok\naggregate_frames|5|ok\nlatest_frame|ok\nframe_publication|ok'

# ---------------------------------------------------------------------------
echo "== section 42: exact log contains + native scalar count =="
LOG_NATIVE_DB="$TMP/log_native.db"
got=$(sqlite3 "$LOG_NATIVE_DB" <<SQL
.load $EXT
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service', message_index='trigram');
INSERT INTO logs(ts,level,message,metadata) VALUES
  (100,'info','routine one','{"service":"api"}'),
  (101,'info','routine two','{"service":"api"}'),
  (102,'info','routine three','{"service":"api"}'),
  (200,'error','request failed','{"service":"api"}'),
  (201,'error','TIMEOUT waiting for db','{"service":"db"}'),
  (202,'error','CafÉ timeout','{"service":"db"}');
INSERT INTO logs(logs) VALUES('flush');
INSERT INTO logs(ts,level,message,metadata) VALUES
  (300,'error','buffer timeout','{"service":"db"}');
SELECT 'contains', group_concat(ts, ',') FROM (
  SELECT ts FROM logs
  WHERE message_contains='TiMeOuT'
  ORDER BY ts DESC LIMIT 2
);
SELECT 'unicode', group_concat(ts, ',') FROM (
  SELECT ts FROM logs
  WHERE message_contains='café'
  ORDER BY ts
);
SELECT 'all', n FROM timeless_log_count('logs');
SELECT 'error', n FROM timeless_log_count('logs', '{"level":"error"}');
SELECT 'db', n FROM timeless_log_count('logs', '{"service":"db"}');
SELECT 'message', n FROM timeless_log_count('logs', NULL, 'TIMEOUT');
SELECT 'boundary', n FROM timeless_log_count(
  'logs', '{"level":"error"}', NULL, 201, 300
);
SELECT 'zero', n FROM timeless_log_count('logs', NULL, 'absent');
SELECT 'stats',
  max(CASE WHEN key='query_bounded_count' THEN value END),
  max(CASE WHEN key='native_count_count' THEN value END),
  max(CASE WHEN key='native_count_metadata_blocks' THEN value END),
  max(CASE WHEN key='native_count_metadata_entries' THEN value END),
  max(CASE WHEN key='native_count_decoded_blocks' THEN value END),
  max(CASE WHEN key='native_count_decoded_entries' THEN value END)
FROM timeless_stats('logs');
SQL
) || { fail "section 42 native log query crashed"; got=""; }

check_eq "exact log contains and native count share bounded engine primitives" \
  "$got" \
$'contains|300,202\nunicode|202\nall|7\nerror|4\ndb|3\nmessage|3\nboundary|3\nzero|0\nstats|1|6|3|9|3|9'

plan=$(sqlite3 "$LOG_NATIVE_DB" ".load $EXT" \
  "EXPLAIN QUERY PLAN SELECT ts FROM logs WHERE message_contains='timeout' ORDER BY ts DESC LIMIT 2;")
if [[ "$plan" == *"bounded-ts-desc"* ]]; then
  pass "exact message_contains participates in bounded newest-first planning"
else
  fail "exact message_contains participates in bounded newest-first planning"
  echo "$plan"
fi

err=$(sqlite3 "$TMP/log_native_bad.db" ".load $EXT" \
  "CREATE VIRTUAL TABLE bad USING timeless_logs(index_keys='message_contains');" 2>&1 || true)
if [[ "$err" == *"collides with a built-in column name"* ]]; then
  pass "message_contains is reserved from dynamic index-key columns"
else
  fail "message_contains is reserved from dynamic index-key columns"
  echo "$err"
fi

# ---------------------------------------------------------------------------
echo "== section 43: size-tiered and bounded logs optimize =="
LOG_OPT_DB="$TMP/log_optimize.db"
got=$(python3 - "$EXT" "$LOG_OPT_DB" <<'PY'
import sqlite3
import struct
import sys

extension, database = sys.argv[1:]
db = sqlite3.connect(database)
db.enable_load_extension(True)
db.load_extension(extension)
db.enable_load_extension(False)

def batch(start, count, step=1):
    timestamps = [start + i * step for i in range(count)]
    messages = [f"request {ts}".encode() for ts in timestamps]
    metadata = [b'{"service":"api"}'] * count
    out = bytearray(b'\x01\x00\x00\x00')
    out += struct.pack('<I', count)
    out += b''.join(struct.pack('<q', ts) for ts in timestamps)
    out += b'\x01' * count
    for values in (messages, metadata):
        for value in values:
            out += struct.pack('<I', len(value)) + value
    return bytes(out)

def stats(table):
    return dict(db.execute(
        "SELECT key, CAST(value AS INTEGER) FROM timeless_stats(?)", (table,)
    ))

db.execute("CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service')")
for cycle in range(40):
    db.execute("INSERT INTO logs(logs) VALUES (?)", (batch(cycle * 256, 256),))
    db.execute("INSERT INTO logs(logs) VALUES ('flush')")
    db.execute("INSERT INTO logs(logs) VALUES ('optimize')")
    db.commit()

s = stats('logs')
assert db.execute("SELECT n FROM timeless_log_count('logs')").fetchone()[0] == 10_240
assert s['optimize_raw_entries'] == 10_240
assert s['optimize_merge_entries'] == 12_288
assert s['optimize_blocks_removed'] == 73
assert s['optimize_blocks_written'] == 42
assert s['blocks'] == 9
assert s['optimize_pending_raw_entries'] == 0
assert s['optimize_merge_ready_entries'] == 0
assert s['optimize_merge_deferred_blocks'] == 8
assert s['optimize_merge_deferred_entries'] == 2_048
rewritten = s['optimize_raw_entries'] + s['optimize_merge_entries']
print(f"amplification|{rewritten / 10_240:.3f}|{s['blocks']}|"
      f"{s['optimize_merge_deferred_blocks']}|{s['optimize_merge_deferred_entries']}")

db.execute("CREATE VIRTUAL TABLE budgeted USING timeless_logs")
for cycle in range(4):
    # More than the one-hour merge-span cap makes four independent raw groups.
    db.execute(
        "INSERT INTO budgeted(budgeted) VALUES (?)",
        (batch(cycle * 3_600_001, 256),),
    )
    db.execute("INSERT INTO budgeted(budgeted) VALUES ('flush')")
db.execute("INSERT INTO budgeted(budgeted) VALUES ('optimize:512')")
db.commit()
first = stats('budgeted')
assert first['raw_blocks'] == 2
assert first['optimize_raw_entries'] == 512
assert first['optimize_budgeted_count'] == 1
assert first['optimize_budget_entries'] == 512
assert first['optimize_budget_limited_count'] == 1
db.execute("INSERT INTO budgeted(budgeted) VALUES ('optimize:512')")
db.commit()
second = stats('budgeted')
assert second['raw_blocks'] == 0
assert second['optimize_raw_entries'] == 1_024
assert db.execute("SELECT count(*) FROM budgeted").fetchone()[0] == 1_024
try:
    db.execute("INSERT INTO budgeted(budgeted) VALUES ('optimize:0')")
    raise AssertionError('zero optimize budget unexpectedly accepted')
except sqlite3.OperationalError as error:
    assert 'budget must be positive' in str(error)
print(f"budget|{first['raw_blocks']}|{first['optimize_raw_entries']}|"
      f"{first['optimize_budget_limited_count']}|{second['optimize_raw_entries']}|"
      f"{second['raw_blocks']}")
db.close()
PY
) || { fail "section 43 optimize driver crashed"; got=""; }

check_eq "size-tiered optimize bounds rewrites and direct callers can budget work" \
  "$got" \
$'amplification|2.200|9|8|2048\nbudget|2|512|1|1024|0'

# ---------------------------------------------------------------------------
echo "== section 44: streaming and metadata-native traces reads =="
TRACE_READ_DB="$TMP/trace_reads.db"
got=$(python3 - "$EXT" "$TRACE_READ_DB" <<'PY'
import sqlite3
import sys

extension, database = sys.argv[1:]
db = sqlite3.connect(database)
db.enable_load_extension(True)
db.load_extension(extension)
db.enable_load_extension(False)
db.execute("CREATE VIRTUAL TABLE traces USING timeless_traces")
for number in range(12):
    service = 'api' if number != 0 else 'worker'
    operation = 'GET /items' if service == 'api' else 'tick'
    duration = 5_000 if number == 5 else number + 1
    db.execute(
        "INSERT INTO traces(trace_id,span_id,name,service,kind,status,start_ts,"
        "duration_ns,attributes,events,resource,instrumentation_scope) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (number.to_bytes(16, 'big'), number.to_bytes(8, 'big'), operation,
         service, 'server', 'ok', number, duration, '{}', '[]', '{}', '{}'),
    )
    if number in (3, 7, 11):
        db.execute("INSERT INTO traces(traces) VALUES ('flush')")
db.commit()

services = ','.join(row[0] for row in db.execute(
    "SELECT value FROM timeless_trace_services('traces') ORDER BY value"
))
operations = ','.join(row[0] for row in db.execute(
    "SELECT value FROM timeless_trace_operations('traces','api') ORDER BY value"
))
newest = ','.join(str(row[0]) for row in db.execute(
    "SELECT start_ts FROM traces WHERE service='api' "
    "ORDER BY start_ts DESC,span_id DESC LIMIT 2 OFFSET 1"
))
duration = ','.join(str(row[0]) for row in db.execute(
    "SELECT start_ts FROM traces WHERE service='api' AND duration_ns>=1000 "
    "ORDER BY start_ts DESC,span_id DESC LIMIT 2"
))
stats = dict(db.execute(
    "SELECT key, CAST(value AS INTEGER) FROM timeless_stats('traces')"
))
assert services == 'api,worker'
assert operations == 'GET /items'
assert newest == '10,9'
assert duration == '5'
assert stats['discovery_count'] == 2
assert stats['query_bounded_count'] == 2
assert stats['query_bounded_requested_spans'] == 5
assert stats['query_bounded_max_spans'] == 3
assert stats['query_stable_location_snapshots'] == 2
assert stats['query_snapshot_payload_max_bytes'] == 0
assert stats['query_blocks_skipped_by_bound'] >= 2
assert db.execute(
    "SELECT count(*) FROM traces_terms WHERE term='operations:'"
).fetchone()[0] == 3
plan = ' '.join(row[3] for row in db.execute(
    "EXPLAIN QUERY PLAN SELECT start_ts FROM traces WHERE service='api' "
    "ORDER BY start_ts DESC,span_id DESC LIMIT 2 OFFSET 1"
))
assert 'bounded-ts-desc-offset' in plan
assert db.execute("SELECT count(*) FROM traces").fetchone()[0] == 12
after = dict(db.execute(
    "SELECT key, CAST(value AS INTEGER) FROM timeless_stats('traces')"
))
assert after['query_count'] == 3
assert after['query_snapshot_payload_max_bytes'] == 0
print(f"catalog|{services}|{operations}")
print(f"bounded|{newest}|{duration}|{stats['query_blocks_skipped_by_bound']}")
print(f"stream|{after['query_count']}|{after['query_stable_location_snapshots']}|"
      f"{after['query_snapshot_payload_max_bytes']}")
db.close()
PY
) || { fail "section 44 traces read driver crashed"; got=""; }

check_eq "traces expose bounded streaming reads and native discovery" \
  "$got" \
$'catalog|api,worker|GET /items\nbounded|10,9|5|2\nstream|3|3|0'

# ---------------------------------------------------------------------------
echo "== section 45: documented query-language SQL equivalents =="
SQL_EQ_DB="$TMP/query_sql_equivalents.db"
got=$(python3 - "$EXT" "$SQL_EQ_DB" <<'PY'
import math
import sqlite3
import sys

extension, database = sys.argv[1:]
db = sqlite3.connect(database)
db.enable_load_extension(True)
db.load_extension(extension)
db.enable_load_extension(False)
db.execute("CREATE VIRTUAL TABLE metrics USING timeless_metrics")
db.execute(
    "CREATE VIRTUAL TABLE logs USING "
    "timeless_logs(index_keys='service,host,path,status')"
)
db.executemany(
    "INSERT INTO metrics(name,labels,ts,value) VALUES(?,?,?,?)",
    [
        ('cpu', '{"host":"web-1","service":"api"}', 100, 10.0),
        ('cpu', '{"host":"web-2","service":"api"}', 100, 20.0),
        ('cpu', '{"host":"web-1","service":"api"}', 110, 30.0),
        ('avg_precision', '{}', 10, 1e16),
        ('avg_precision', '{}', 20, 1.0),
        ('avg_precision', '{}', 30, -1e16),
        ('avg_overflow', '{}', 20, sys.float_info.max),
        ('avg_overflow', '{}', 30, sys.float_info.max),
        ('min_window', '{}', 10, 5.0),
        ('min_window', '{}', 20, 3.0),
        ('min_window', '{}', 30, 4.0),
        ('min_zero', '{}', 20, 0.0),
        ('min_zero', '{}', 30, -0.0),
        ('max_window', '{}', 10, 5.0),
        ('max_window', '{}', 20, 3.0),
        ('max_window', '{}', 30, 4.0),
        ('max_zero', '{}', 20, -0.0),
        ('max_zero', '{}', 30, 0.0),
        ('rate_counter', '{"case":"steady"}', 10, 100.0),
        ('rate_counter', '{"case":"steady"}', 30, 300.0),
        ('rate_counter', '{"case":"steady"}', 50, 500.0),
        ('rate_counter', '{"case":"reset"}', 10, 100.0),
        ('rate_counter', '{"case":"reset"}', 30, 150.0),
        ('rate_counter', '{"case":"reset"}', 50, 20.0),
        ('changes_metric', '{"case":"repeated"}', 10, 1.0),
        ('changes_metric', '{"case":"repeated"}', 20, 1.0),
        ('changes_metric', '{"case":"repeated"}', 30, 2.0),
        ('changes_metric', '{"case":"repeated"}', 40, 2.0),
        ('changes_metric', '{"case":"repeated"}', 50, 1.0),
        ('changes_metric', '{"case":"constant"}', 20, 7.0),
        ('changes_metric', '{"case":"constant"}', 40, 7.0),
        ('changes_metric', '{"case":"singleton"}', 50, 5.0),
        ('abs_metric', '{"case":"negative"}', 100, -3.0),
        ('abs_metric', '{"case":"negative_inf"}', 100, -math.inf),
        ('abs_metric', '{"case":"negative_zero"}', 100, -0.0),
        ('abs_metric', '{"case":"positive"}', 100, 2.0),
        ('round_metric', '{"case":"negative"}', 100, -1.6),
        ('round_metric', '{"case":"negative_zero"}', 100, -0.0),
        ('round_metric', '{"case":"positive"}', 100, 1.6),
        ('clamp_metric', '{"case":"below"}', 100, -3.0),
        ('clamp_metric', '{"case":"inside"}', 100, 2.0),
        ('clamp_metric', '{"case":"above"}', 100, 8.0),
        ('errors_total', '{"host":"web-1"}', 100, 2.0),
        ('requests_total', '{"host":"web-1"}', 100, 10.0),
    ],
)
db.execute("INSERT INTO metrics(metrics) VALUES ('flush')")
db.executemany(
    "INSERT INTO logs(ts,level,message,metadata) VALUES(?,?,?,?)",
    [
        (1000, 'error', 'request timeout',
         '{"service":"api","host":"web-1","deployment":'
         '{"region":"us-east"},"duration_ms":12}'),
        (2000, 'info', 'request ok',
         '{"service":"api","host":"web-2","deployment":'
         '{"region":"us-west"},"duration_ms":4}'),
    ],
)
db.execute("INSERT INTO logs(logs) VALUES ('flush')")
db.commit()

instant = db.execute(
    "SELECT labels,ts,value FROM timeless_grid("
    "'metrics',:metric,:filter_json,:at,:at,1,:lookback) ORDER BY labels",
    {
        'metric': 'cpu',
        'filter_json': None,
        'at': 110,
        'lookback': 20,
    },
).fetchall()
assert len(instant) == 2

absolute = db.execute(
    "SELECT labels,ts,CASE WHEN value=0.0 THEN 0.0 ELSE abs(value) END "
    "FROM timeless_grid("
    "'metrics',:metric,:filter_json,:start,:end,:step,:lookback) "
    "ORDER BY labels,ts",
    {
        'metric': 'abs_metric',
        'filter_json': None,
        'start': 100,
        'end': 100,
        'step': 1,
        'lookback': 1,
    },
).fetchall()
assert absolute == [
    ('{"case":"negative"}', 100, 3.0),
    ('{"case":"negative_inf"}', 100, math.inf),
    ('{"case":"negative_zero"}', 100, 0.0),
    ('{"case":"positive"}', 100, 2.0),
]
assert math.copysign(1.0, absolute[2][2]) == 1.0

rounding = db.execute(
    "WITH parameter(nearest) AS (SELECT CAST(:nearest AS REAL)),"
    " selected AS (SELECT labels,ts,value,1.0/nearest AS inverse"
    " FROM timeless_grid("
    "'metrics',:metric,:filter_json,:start,:end,:step,:lookback),parameter)"
    " SELECT labels,ts,ceil(value),floor(value),"
    " floor(value*inverse+0.5)/inverse FROM selected ORDER BY labels,ts",
    {
        'metric': 'round_metric',
        'filter_json': None,
        'start': 100,
        'end': 100,
        'step': 1,
        'lookback': 1,
        'nearest': 0.5,
    },
).fetchall()
assert rounding == [
    ('{"case":"negative"}', 100, -1.0, -2.0, -1.5),
    ('{"case":"negative_zero"}', 100, -0.0, -0.0, 0.0),
    ('{"case":"positive"}', 100, 2.0, 1.0, 1.5),
]
assert math.copysign(1.0, rounding[1][2]) == -1.0
assert math.copysign(1.0, rounding[1][3]) == -1.0
assert math.copysign(1.0, rounding[1][4]) == 1.0

clamped = db.execute(
    "WITH parameters(minimum,maximum) AS ("
    " SELECT CAST(:minimum AS REAL),CAST(:maximum AS REAL)),"
    " selected AS (SELECT labels,ts,value,minimum,maximum"
    " FROM timeless_grid("
    "'metrics',:metric,:filter_json,:start,:end,:step,:lookback),parameters)"
    " SELECT labels,ts,CASE WHEN value<=minimum THEN minimum"
    " WHEN value>=maximum THEN maximum ELSE value END"
    " FROM selected WHERE minimum<=maximum ORDER BY labels,ts",
    {
        'metric': 'clamp_metric',
        'filter_json': None,
        'start': 100,
        'end': 100,
        'step': 1,
        'lookback': 1,
        'minimum': 0.0,
        'maximum': 5.0,
    },
).fetchall()
assert clamped == [
    ('{"case":"above"}', 100, 5.0),
    ('{"case":"below"}', 100, 0.0),
    ('{"case":"inside"}', 100, 2.0),
]
inverted_clamp = db.execute(
    "WITH parameters(minimum,maximum) AS (SELECT 5.0,0.0)"
    " SELECT labels FROM timeless_grid("
    "'metrics','clamp_metric',NULL,100,100,1,1),parameters"
    " WHERE minimum<=maximum"
).fetchall()
assert inverted_clamp == []

offset = db.execute(
    "SELECT labels,ts+:offset AS outer_ts,value FROM timeless_grid("
    "'metrics',:metric,:filter_json,:start-:offset,:end-:offset,:step,:lookback) "
    "ORDER BY labels,outer_ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
        'offset': 10,
    },
).fetchall()
assert offset == [
    ('{"host":"web-1","service":"api"}', 110, 10.0),
    ('{"host":"web-2","service":"api"}', 110, 20.0),
]

fixed = db.execute(
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "), selected AS ("
    " SELECT labels,value FROM timeless_grid("
    "  'metrics',:metric,:filter_json,:anchor-:offset,:anchor-:offset,1,:lookback)"
    ") SELECT selected.labels,evaluation.ts,selected.value"
    " FROM selected CROSS JOIN evaluation"
    " ORDER BY selected.labels,evaluation.ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
        'anchor': 110,
        'offset': 10,
    },
).fetchall()
assert fixed == [
    ('{"host":"web-1","service":"api"}', 100, 10.0),
    ('{"host":"web-1","service":"api"}', 110, 10.0),
    ('{"host":"web-2","service":"api"}', 100, 20.0),
    ('{"host":"web-2","service":"api"}', 110, 20.0),
]

subquery = db.execute(
    "WITH timing AS ("
    " SELECT :at-:offset effective_at,:window window,:resolution resolution"
    "), bounds AS ("
    " SELECT effective_at-window lower,effective_at,resolution,"
    " ((effective_at-window)%resolution+resolution)%resolution lower_mod,"
    " (effective_at%resolution+resolution)%resolution upper_mod FROM timing"
    "), aligned AS ("
    " SELECT lower-lower_mod+resolution first_ts,"
    " effective_at-upper_mod last_ts,resolution FROM bounds"
    ") SELECT labels,ts,value FROM timeless_grid("
    " 'metrics',:metric,:filter_json,(SELECT first_ts FROM aligned),"
    " (SELECT last_ts FROM aligned),(SELECT resolution FROM aligned),:lookback)"
    " WHERE (SELECT first_ts<=last_ts FROM aligned) ORDER BY labels,ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'at': 125,
        'offset': 0,
        'window': 30,
        'resolution': 10,
        'lookback': 30,
    },
).fetchall()
assert subquery == [
    ('{"host":"web-1","service":"api"}', 100, 10.0),
    ('{"host":"web-1","service":"api"}', 110, 30.0),
    ('{"host":"web-1","service":"api"}', 120, 30.0),
    ('{"host":"web-2","service":"api"}', 100, 20.0),
    ('{"host":"web-2","service":"api"}', 110, 20.0),
    ('{"host":"web-2","service":"api"}', 120, 20.0),
]

window = db.execute(
    "SELECT labels,ts,value FROM timeless_window("
    "'metrics',:metric,:filter_json,:start,:end,:step,:window,'avg') "
    "ORDER BY labels,ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'window': 20,
    },
).fetchall()
assert len(window) == 4

precision_avg = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','avg_precision',NULL,30,30,1,30,'avg')"
).fetchone()[0]
assert precision_avg == 1.0 / 3.0
overflow_avg = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','avg_overflow',NULL,30,30,1,20,'avg')"
).fetchone()[0]
assert overflow_avg == sys.float_info.max

precision_sum = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','avg_precision',NULL,30,30,1,30,'sum')"
).fetchone()[0]
assert precision_sum == 1.0
overflow_sum = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','avg_overflow',NULL,30,30,1,20,'sum')"
).fetchone()[0]
assert math.isinf(overflow_sum) and overflow_sum > 0
sample_count = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','avg_precision',NULL,30,30,1,30,'count')"
).fetchone()[0]
assert sample_count == 3.0
sample_presence = db.execute(
    "SELECT CAST(value>0 AS REAL) FROM timeless_window("
    "'metrics','avg_precision',NULL,30,30,1,30,'count')"
).fetchone()[0]
assert sample_presence == 1.0
empty_presence = db.execute(
    "SELECT CAST(value>0 AS REAL) FROM timeless_window("
    "'metrics','avg_precision',NULL,1000,1000,1,30,'count')"
).fetchall()
assert empty_presence == []
window_quantile = db.execute(
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.value"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    " WHERE raw.value IS NOT NULL"
    "),ranked AS ("
    " SELECT *,ROW_NUMBER() OVER (PARTITION BY series_id,ts ORDER BY value)-1 value_index,"
    " COUNT(*) OVER (PARTITION BY series_id,ts) value_count FROM selected"
    "),positions AS ("
    " SELECT DISTINCT series_id,labels,ts,value_count,"
    " CAST(:q AS REAL)*(value_count-1) rank FROM ranked"
    "),bounds AS ("
    " SELECT *,CAST(rank AS INTEGER) lower_index,"
    " MIN(CAST(rank AS INTEGER)+1,value_count-1) upper_index FROM positions"
    ") SELECT bounds.labels,bounds.ts,"
    " lower.value*(1.0-(bounds.rank-bounds.lower_index))"
    " +upper.value*(bounds.rank-bounds.lower_index)"
    " FROM bounds JOIN ranked lower ON lower.series_id=bounds.series_id"
    " AND lower.ts=bounds.ts AND lower.value_index=bounds.lower_index"
    " JOIN ranked upper ON upper.series_id=bounds.series_id"
    " AND upper.ts=bounds.ts AND upper.value_index=bounds.upper_index"
    " ORDER BY bounds.labels,bounds.ts",
    {
        'metric': 'min_window',
        'filter_json': None,
        'start': 30,
        'end': 30,
        'step': 1,
        'window': 20,
        'q': 0.5,
    },
).fetchall()
assert window_quantile == [('{}', 30, 3.5)]
window_dispersion_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,"
    " ROW_NUMBER() OVER (PARTITION BY raw.series_id,evaluation.ts"
    " ORDER BY raw.ts) sample_number,"
    " COUNT(*) OVER (PARTITION BY raw.series_id,evaluation.ts) sample_count,"
    " raw.value FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    " WHERE raw.value IS NOT NULL"
    "),moments(series_id,labels,ts,sample_number,sample_count,n,mean,m2) AS ("
    " SELECT series_id,labels,ts,sample_number,sample_count,1.0,value,0.0"
    " FROM selected WHERE sample_number=1"
    " UNION ALL SELECT next.series_id,next.labels,next.ts,next.sample_number,"
    " next.sample_count,moments.n+1.0,"
    " moments.mean+(next.value-moments.mean)/(moments.n+1.0),"
    " moments.m2+(next.value-moments.mean)*(next.value-(moments.mean"
    " +(next.value-moments.mean)/(moments.n+1.0)))"
    " FROM moments JOIN selected next ON next.series_id=moments.series_id"
    " AND next.ts=moments.ts AND next.sample_number=moments.sample_number+1"
    ") SELECT labels,ts,CASE WHEN :variance_only THEN m2/n ELSE SQRT(m2/n) END"
    " FROM moments WHERE sample_number=sample_count ORDER BY labels,ts"
)
dispersion_params = {
    'metric': 'min_window',
    'filter_json': None,
    'start': 30,
    'end': 30,
    'step': 1,
    'window': 20,
}
window_stddev = db.execute(
    window_dispersion_sql,
    {**dispersion_params, 'variance_only': False},
).fetchall()
assert window_stddev == [('{}', 30, 0.5)]
window_stdvar = db.execute(
    window_dispersion_sql,
    {**dispersion_params, 'variance_only': True},
).fetchall()
assert window_stdvar == [('{}', 30, 0.25)]

rate_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.ts sample_ts,raw.value,"
    " LAG(raw.value) OVER (PARTITION BY raw.series_id,evaluation.ts"
    " ORDER BY raw.ts) previous_value,"
    " ROW_NUMBER() OVER (PARTITION BY raw.series_id,evaluation.ts"
    " ORDER BY raw.ts) sample_number,"
    " COUNT(*) OVER (PARTITION BY raw.series_id,evaluation.ts) sample_count"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),folded AS ("
    " SELECT series_id,labels,ts,MAX(sample_count) sample_count,"
    " MIN(sample_ts) first_ts,MAX(sample_ts) last_ts,"
    " MAX(CASE WHEN sample_number=1 THEN value END) first_value,"
    " MAX(CASE WHEN sample_number=sample_count THEN value END) last_value,"
    " SUM(CASE WHEN sample_number>1 AND value<previous_value"
    " THEN previous_value ELSE 0.0 END) reset_correction"
    " FROM selected GROUP BY series_id,labels,ts"
    " HAVING MAX(sample_count)>=2 AND MAX(sample_ts)>MIN(sample_ts)"
    "),intervals AS ("
    " SELECT *,last_value-first_value+reset_correction counter_delta,"
    " (last_ts-first_ts)*1.0 sampled_interval,"
    " (last_ts-first_ts)*1.0/(sample_count-1) average_interval"
    " FROM folded"
    "),edges AS ("
    " SELECT *,CASE WHEN first_ts-(ts-:window)>=average_interval*1.1"
    " THEN average_interval/2.0 ELSE first_ts-(ts-:window) END start_duration,"
    " CASE WHEN ts-last_ts>=average_interval*1.1"
    " THEN average_interval/2.0 ELSE ts-last_ts END end_duration"
    " FROM intervals"
    ") SELECT labels,ts,counter_delta*((sampled_interval+"
    " CASE WHEN counter_delta>0 AND first_value>=0"
    " THEN MIN(start_duration,sampled_interval*first_value/counter_delta)"
    " ELSE start_duration END+end_duration)/sampled_interval)/:window value"
    " FROM edges ORDER BY labels,ts"
)
rate_values = db.execute(
    rate_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert rate_values == [
    ('{"case":"reset"}', 60, 1.75),
    ('{"case":"steady"}', 60, 10.0),
]

irate_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),ranked AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.ts sample_ts,raw.value,"
    " ROW_NUMBER() OVER (PARTITION BY raw.series_id,evaluation.ts"
    " ORDER BY raw.ts DESC) recency"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),final_pair AS ("
    " SELECT series_id,labels,ts,"
    " MAX(CASE WHEN recency=1 THEN sample_ts END) last_ts,"
    " MAX(CASE WHEN recency=1 THEN value END) last_value,"
    " MAX(CASE WHEN recency=2 THEN sample_ts END) previous_ts,"
    " MAX(CASE WHEN recency=2 THEN value END) previous_value"
    " FROM ranked WHERE recency<=2 GROUP BY series_id,labels,ts"
    ") SELECT labels,ts,CASE WHEN last_value<previous_value THEN last_value"
    " ELSE last_value-previous_value END/(last_ts-previous_ts) value"
    " FROM final_pair WHERE previous_ts IS NOT NULL AND last_ts>previous_ts"
    " ORDER BY labels,ts"
)
irate_values = db.execute(
    irate_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert irate_values == [
    ('{"case":"reset"}', 60, 1.0),
    ('{"case":"steady"}', 60, 10.0),
]

increase_sql = rate_sql.replace(")/:window value", ") value")
assert increase_sql != rate_sql
increase_values = db.execute(
    increase_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert increase_values == [
    ('{"case":"reset"}', 60, 105.0),
    ('{"case":"steady"}', 60, 600.0),
]

delta_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.ts sample_ts,raw.value,"
    " ROW_NUMBER() OVER (PARTITION BY raw.series_id,evaluation.ts"
    " ORDER BY raw.ts) sample_number,"
    " COUNT(*) OVER (PARTITION BY raw.series_id,evaluation.ts) sample_count"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),folded AS ("
    " SELECT series_id,labels,ts,MAX(sample_count) sample_count,"
    " MIN(sample_ts) first_ts,MAX(sample_ts) last_ts,"
    " MAX(CASE WHEN sample_number=1 THEN value END) first_value,"
    " MAX(CASE WHEN sample_number=sample_count THEN value END) last_value"
    " FROM selected GROUP BY series_id,labels,ts"
    " HAVING MAX(sample_count)>=2 AND MAX(sample_ts)>MIN(sample_ts)"
    "),intervals AS ("
    " SELECT *,last_value-first_value gauge_delta,"
    " (last_ts-first_ts)*1.0 sampled_interval,"
    " (last_ts-first_ts)*1.0/(sample_count-1) average_interval"
    " FROM folded"
    "),edges AS ("
    " SELECT *,CASE WHEN first_ts-(ts-:window)>=average_interval*1.1"
    " THEN average_interval/2.0 ELSE first_ts-(ts-:window) END start_duration,"
    " CASE WHEN ts-last_ts>=average_interval*1.1"
    " THEN average_interval/2.0 ELSE ts-last_ts END end_duration"
    " FROM intervals"
    ") SELECT labels,ts,gauge_delta*"
    " (sampled_interval+start_duration+end_duration)/sampled_interval value"
    " FROM edges ORDER BY labels,ts"
)
delta_values = db.execute(
    delta_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert delta_values == [
    ('{"case":"reset"}', 60, -120.0),
    ('{"case":"steady"}', 60, 600.0),
]

idelta_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),ranked AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.ts sample_ts,raw.value,"
    " ROW_NUMBER() OVER (PARTITION BY raw.series_id,evaluation.ts"
    " ORDER BY raw.ts DESC) recency"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),final_pair AS ("
    " SELECT series_id,labels,ts,"
    " MAX(CASE WHEN recency=1 THEN sample_ts END) last_ts,"
    " MAX(CASE WHEN recency=1 THEN value END) last_value,"
    " MAX(CASE WHEN recency=2 THEN sample_ts END) previous_ts,"
    " MAX(CASE WHEN recency=2 THEN value END) previous_value"
    " FROM ranked WHERE recency<=2 GROUP BY series_id,labels,ts"
    ") SELECT labels,ts,last_value-previous_value value FROM final_pair"
    " WHERE previous_ts IS NOT NULL AND last_ts>previous_ts"
    " ORDER BY labels,ts"
)
idelta_values = db.execute(
    idelta_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert idelta_values == [
    ('{"case":"reset"}', 60, -130.0),
    ('{"case":"steady"}', 60, 200.0),
]

deriv_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.ts sample_ts,raw.value,"
    " MIN(raw.ts) OVER (PARTITION BY raw.series_id,evaluation.ts) first_sample_ts"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),centered AS ("
    " SELECT series_id,labels,ts,(sample_ts-first_sample_ts)*1.0 x,value y"
    " FROM selected"
    "),statistics AS ("
    " SELECT series_id,labels,ts,COUNT(*)*1.0 n,MIN(y) min_y,MAX(y) max_y,"
    " SUM(x) sum_x,SUM(y) sum_y,SUM(x*y) sum_xy,SUM(x*x) sum_x2"
    " FROM centered GROUP BY series_id,labels,ts"
    ") SELECT labels,ts,CASE WHEN min_y=max_y THEN 0.0"
    " ELSE (sum_xy-sum_x*sum_y/n)/(sum_x2-sum_x*sum_x/n) END value"
    " FROM statistics WHERE n>=2 ORDER BY labels,ts"
)
deriv_values = db.execute(
    deriv_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert deriv_values == [
    ('{"case":"reset"}', 60, -2.0),
    ('{"case":"steady"}', 60, 10.0),
]

predict_linear_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,"
    " (raw.ts-evaluation.ts)*1.0 x,raw.value y"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),statistics AS ("
    " SELECT series_id,labels,ts,COUNT(*)*1.0 n,MIN(y) min_y,MAX(y) max_y,"
    " SUM(x) sum_x,SUM(y) sum_y,SUM(x*y) sum_xy,SUM(x*x) sum_x2"
    " FROM selected GROUP BY series_id,labels,ts"
    "),coefficients AS ("
    " SELECT *,CASE WHEN min_y=max_y THEN 0.0"
    " ELSE (sum_xy-sum_x*sum_y/n)/(sum_x2-sum_x*sum_x/n) END slope"
    " FROM statistics WHERE n>=2"
    ") SELECT labels,ts,slope*:horizon+(sum_y/n-slope*sum_x/n) value"
    " FROM coefficients ORDER BY labels,ts"
)
predict_linear_values = db.execute(
    predict_linear_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
        'horizon': 10.0,
    },
).fetchall()
assert predict_linear_values == [
    ('{"case":"reset"}', 60, 10.0),
    ('{"case":"steady"}', 60, 700.0),
]

changes_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.ts sample_ts,raw.value"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),sequenced AS ("
    " SELECT *,ROW_NUMBER() OVER (PARTITION BY series_id,ts ORDER BY sample_ts)"
    " sample_number,LAG(value) OVER (PARTITION BY series_id,ts ORDER BY sample_ts)"
    " previous_value FROM selected"
    ") SELECT labels,ts,SUM(CASE WHEN sample_number=1 THEN 0"
    " WHEN (value IS NULL)<>(previous_value IS NULL) THEN 1"
    " WHEN value!=previous_value THEN 1 ELSE 0 END) value"
    " FROM sequenced GROUP BY series_id,labels,ts ORDER BY labels,ts"
)
changes_values = db.execute(
    changes_sql,
    {
        'metric': 'changes_metric',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert changes_values == [
    ('{"case":"constant"}', 60, 0),
    ('{"case":"repeated"}', 60, 2),
    ('{"case":"singleton"}', 60, 0),
]

resets_sql = (
    "WITH RECURSIVE evaluation(ts) AS ("
    " SELECT :start UNION ALL SELECT ts+:step FROM evaluation"
    " WHERE ts+:step<=:end"
    "),selected AS ("
    " SELECT raw.series_id,raw.labels,evaluation.ts,raw.ts sample_ts,raw.value"
    " FROM evaluation JOIN timeless_raw("
    " 'metrics',:metric,:filter_json,:start-:window,:end) raw"
    " ON raw.ts>evaluation.ts-:window AND raw.ts<=evaluation.ts"
    "),sequenced AS ("
    " SELECT *,ROW_NUMBER() OVER (PARTITION BY series_id,ts ORDER BY sample_ts)"
    " sample_number,LAG(value) OVER (PARTITION BY series_id,ts ORDER BY sample_ts)"
    " previous_value FROM selected"
    ") SELECT labels,ts,SUM(CASE WHEN sample_number>1 AND value<previous_value"
    " THEN 1 ELSE 0 END) value FROM sequenced"
    " GROUP BY series_id,labels,ts ORDER BY labels,ts"
)
resets_values = db.execute(
    resets_sql,
    {
        'metric': 'rate_counter',
        'filter_json': None,
        'start': 60,
        'end': 60,
        'step': 1,
        'window': 60,
    },
).fetchall()
assert resets_values == [
    ('{"case":"reset"}', 60, 1),
    ('{"case":"steady"}', 60, 0),
]

minimum = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','min_window',NULL,30,30,1,20,'min')"
).fetchone()[0]
assert minimum == 3.0
first_zero = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','min_zero',NULL,30,30,1,20,'min')"
).fetchone()[0]
assert first_zero == 0.0 and math.copysign(1.0, first_zero) == 1.0

maximum = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','max_window',NULL,30,30,1,20,'max')"
).fetchone()[0]
assert maximum == 4.0
reverse_zero = db.execute(
    "SELECT value FROM timeless_window("
    "'metrics','max_zero',NULL,30,30,1,20,'max')"
).fetchone()[0]
assert reverse_zero == 0.0 and math.copysign(1.0, reverse_zero) == -1.0
last_value = db.execute(
    "SELECT value FROM timeless_grid("
    "'metrics','min_window',NULL,30,30,1,20)"
).fetchone()[0]
assert last_value == 4.0
last_zero = db.execute(
    "SELECT value FROM timeless_grid("
    "'metrics','min_zero',NULL,30,30,1,20)"
).fetchone()[0]
assert last_zero == 0.0 and math.copysign(1.0, last_zero) == -1.0

cross_sum = db.execute(
    "WITH selected AS ("
    " SELECT ts,json_extract(labels,'$.service') service,value"
    " FROM timeless_grid('metrics','cpu',NULL,100,110,10,20)"
    ") SELECT service,ts,SUM(value) FROM selected"
    " GROUP BY service,ts ORDER BY service,ts"
).fetchall()
assert cross_sum == [('api', 100, 30.0), ('api', 110, 50.0)]

cross_avg = db.execute(
    "WITH selected AS ("
    " SELECT ts,COALESCE(json_extract(labels,'$.service'),'') service,value"
    " FROM timeless_grid('metrics',:metric,:filter_json,:start,:end,:step,:lookback)"
    ") SELECT service,ts,AVG(value) FROM selected"
    " GROUP BY service,ts ORDER BY service,ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
    },
).fetchall()
assert cross_avg == [('api', 100, 15.0), ('api', 110, 25.0)]

cross_extrema = db.execute(
    "WITH selected AS ("
    " SELECT ts,COALESCE(json_extract(labels,'$.service'),'') service,value"
    " FROM timeless_grid('metrics',:metric,:filter_json,:start,:end,:step,:lookback)"
    ") SELECT service,ts,MIN(value),MAX(value) FROM selected"
    " GROUP BY service,ts ORDER BY service,ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
    },
).fetchall()
assert cross_extrema == [
    ('api', 100, 10.0, 20.0),
    ('api', 110, 20.0, 30.0),
]

cross_count_group = db.execute(
    "WITH selected AS ("
    " SELECT ts,COALESCE(json_extract(labels,'$.service'),'') service"
    " FROM timeless_grid('metrics',:metric,:filter_json,:start,:end,:step,:lookback)"
    ") SELECT service,ts,COUNT(*),1 FROM selected"
    " GROUP BY service,ts ORDER BY service,ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
    },
).fetchall()
assert cross_count_group == [
    ('api', 100, 2, 1),
    ('api', 110, 2, 1),
]

cross_dispersion = db.execute(
    "WITH selected AS ("
    " SELECT ts,COALESCE(json_extract(labels,'$.service'),'') service,value"
    " FROM timeless_grid('metrics',:metric,:filter_json,:start,:end,:step,:lookback)"
    "), moments AS ("
    " SELECT service,ts,AVG(value) mean,AVG(value*value) second_moment"
    " FROM selected GROUP BY service,ts"
    ") SELECT service,ts,MAX(second_moment-mean*mean,0.0),"
    " SQRT(MAX(second_moment-mean*mean,0.0)) FROM moments"
    " ORDER BY service,ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
    },
).fetchall()
assert cross_dispersion == [
    ('api', 100, 25.0, 5.0),
    ('api', 110, 25.0, 5.0),
]

cross_quantile = db.execute(
    "WITH selected AS ("
    " SELECT ts,COALESCE(json_extract(labels,'$.service'),'') service,value"
    " FROM timeless_grid('metrics',:metric,:filter_json,:start,:end,:step,:lookback)"
    " WHERE value IS NOT NULL"
    "), ranked AS ("
    " SELECT service,ts,value,ROW_NUMBER() OVER ("
    "  PARTITION BY service,ts ORDER BY value)-1 value_index,"
    " COUNT(*) OVER (PARTITION BY service,ts) value_count FROM selected"
    "), positions AS ("
    " SELECT DISTINCT service,ts,value_count,CAST(:q AS REAL)*(value_count-1) rank"
    " FROM ranked"
    "), bounds AS ("
    " SELECT *,CAST(rank AS INTEGER) lower_index,"
    " MIN(CAST(rank AS INTEGER)+1,value_count-1) upper_index FROM positions"
    ") SELECT bounds.service,bounds.ts,"
    " lower.value*(1.0-(bounds.rank-bounds.lower_index))"
    " +upper.value*(bounds.rank-bounds.lower_index)"
    " FROM bounds JOIN ranked lower ON lower.service=bounds.service"
    " AND lower.ts=bounds.ts AND lower.value_index=bounds.lower_index"
    " JOIN ranked upper ON upper.service=bounds.service"
    " AND upper.ts=bounds.ts AND upper.value_index=bounds.upper_index"
    " ORDER BY bounds.service,bounds.ts",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
        'q': 0.5,
    },
).fetchall()
assert cross_quantile == [('api', 100, 15.0), ('api', 110, 25.0)]

cross_values = db.execute(
    "WITH selected AS ("
    " SELECT ts,COALESCE(json_extract(labels,'$.service'),'') service,value"
    " FROM timeless_grid('metrics',:metric,:filter_json,:start,:end,:step,:lookback)"
    ") SELECT service,ts,value,COUNT(*) FROM selected"
    " GROUP BY service,ts,value ORDER BY service,ts,value",
    {
        'metric': 'cpu',
        'filter_json': None,
        'start': 100,
        'end': 110,
        'step': 10,
        'lookback': 20,
    },
).fetchall()
assert cross_values == [
    ('api', 100, 10.0, 1),
    ('api', 100, 20.0, 1),
    ('api', 110, 20.0, 1),
    ('api', 110, 30.0, 1),
]

ratio = db.execute(
    "WITH errors AS ("
    " SELECT ts,labels,json_extract(labels,'$.host') host,value"
    " FROM timeless_grid('metrics','errors_total',NULL,100,100,1,20)"
    "), requests AS ("
    " SELECT ts,labels,json_extract(labels,'$.host') host,value"
    " FROM timeless_grid('metrics','requests_total',NULL,100,100,1,20)"
    ") SELECT e.value/r.value FROM errors e JOIN requests r"
    " ON r.ts=e.ts AND r.host=e.host"
).fetchone()[0]
assert ratio == 0.2

top = db.execute(
    "WITH selected AS ("
    " SELECT labels,ts,value"
    " FROM timeless_grid('metrics','cpu',NULL,100,110,10,20)"
    "), ranked AS ("
    " SELECT *,ROW_NUMBER() OVER ("
    "  PARTITION BY ts ORDER BY value DESC,labels) rank FROM selected"
    ") SELECT labels,ts,value FROM ranked WHERE rank<=1"
    " ORDER BY ts,value DESC,labels"
).fetchall()
assert [row[2] for row in top] == [20.0, 30.0]

bottom = db.execute(
    "WITH selected AS ("
    " SELECT labels,ts,value"
    " FROM timeless_grid('metrics','cpu',NULL,100,110,10,20)"
    "), ranked AS ("
    " SELECT *,ROW_NUMBER() OVER ("
    "  PARTITION BY ts ORDER BY value ASC,labels) rank FROM selected"
    ") SELECT labels,ts,value FROM ranked WHERE rank<=1"
    " ORDER BY ts,value ASC,labels"
).fetchall()
assert [row[2] for row in bottom] == [10.0, 20.0]

bounded = db.execute(
    "SELECT ts,level,message,metadata FROM logs"
    " WHERE ts>=:start_ms AND ts<=:end_ms"
    " AND level=:level AND service=:service"
    " ORDER BY ts DESC LIMIT :limit OFFSET :offset",
    {
        'start_ms': 1000,
        'end_ms': 2000,
        'level': 'error',
        'service': 'api',
        'limit': 10,
        'offset': 0,
    },
).fetchall()
assert len(bounded) == 1 and bounded[0][0] == 1000

substring = db.execute(
    "SELECT ts FROM logs WHERE message_contains=:needle ORDER BY ts DESC",
    {'needle': 'TIMEOUT'},
).fetchall()
assert substring == [(1000,)]

count = db.execute(
    "SELECT n FROM timeless_log_count("
    "'logs',:filter_json,:message_contains,:start_ms,:end_ms)",
    {
        'filter_json': '{"level":"error","service":"api"}',
        'message_contains': None,
        'start_ms': 1000,
        'end_ms': 2000,
    },
).fetchone()[0]
assert count == 1

values = db.execute(
    "SELECT value FROM timeless_log_values("
    "'logs',:field,:filter_json,:message_contains,:start_ms,:end_ms,"
    ":max_values) ORDER BY value",
    {
        'field': 'host',
        'filter_json': None,
        'message_contains': None,
        'start_ms': 1000,
        'end_ms': 2000,
        'max_values': 100,
    },
).fetchall()
assert values == [('web-1',), ('web-2',)]

metadata = db.execute(
    "SELECT ts FROM logs"
    " WHERE json_extract(metadata,'$.deployment.region')=:region",
    {'region': 'us-east'},
).fetchall()
assert metadata == [(1000,)]

buckets = db.execute(
    "SELECT bucket_ts,group_key,n FROM timeless_log_buckets("
    "'logs',:group_key,:filter_json,:start_ms,:end_ms,:step_ms)"
    " ORDER BY bucket_ts,group_key",
    {
        'group_key': 'level',
        'filter_json': None,
        'start_ms': 1000,
        'end_ms': 2000,
        'step_ms': 1000,
    },
).fetchall()
assert sum(row[2] for row in buckets) == 2

print(f"prom|{len(instant)}|{len(window)}|{len(top)}|{ratio:.1f}|"
      f"{len(offset)}|{len(fixed)}|{len(subquery)}")
print(f"logs|{len(bounded)}|{len(substring)}|{count}|{len(values)}|"
      f"{len(metadata)}|{sum(row[2] for row in buckets)}")
db.close()
PY
) || { fail "section 45 SQL equivalents driver crashed"; got=""; }

check_eq "documented SQL equivalents execute through the public extension" \
  "$got" \
$'prom|2|4|2|0.2|2|4|6\nlogs|1|1|1|2|1|2'

# ---------------------------------------------------------------------------
echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "ALL SECTIONS PASSED"
else
  echo "$FAILURES CHECK(S) FAILED"
  exit 1
fi
