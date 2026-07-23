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
#   6. rollback caveat (documented POC behavior, WARNING not failure)
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
#
# NOTE on durability semantics being tested: points buffered but NOT
# flushed before the process exits are lost — that is the accepted POC
# contract, so every section flushes before relying on a reopen.

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
SELECT 'registry', COUNT(*) FROM metrics_meta WHERE k = 'series_registry';
SQL
)
expected='pre|cpu|100|1.5|{"host":"a"}
pre|mem|150|3.0|{"host":"b"}
pre|cpu|200|2.5|{"host":"a"}
post|cpu|100|1.5|{"host":"a"}
post|mem|150|3.0|{"host":"b"}
post|cpu|200|2.5|{"host":"a"}
chunks|2|3
registry|1'
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
echo "== section 6: rollback caveat (documented POC behavior) =="
# A data-row INSERT only touches engine memory (partition buffers), never
# SQL — so ROLLBACK has nothing to undo and the buffered point remains
# visible until the process exits (it is never flushed, so it does NOT
# survive reopen). Accepted POC limitation, tracked as PLAN.md risk R5.
got=$(sqlite3 "$DB" <<SQL
.load $EXT
BEGIN;
INSERT INTO metrics(name, ts, value) VALUES ('rb', 5555, 9.9);
ROLLBACK;
SELECT 'rbcount', COUNT(*) FROM metrics WHERE name = 'rb';
SQL
)
if [[ "$got" == "rbcount|1" ]]; then
  pass "rollback behavior is the documented one"
  echo "WARNING: buffered (unflushed) writes survive ROLLBACK — accepted POC"
  echo "WARNING: limitation (PLAN.md R5); they vanish when the process exits."
elif [[ "$got" == "rbcount|0" ]]; then
  pass "rollback discarded the buffered point (better than documented)"
  echo "NOTE: update the R5 notes — rollback currently discards buffered writes."
else
  fail "unexpected rollback result: $got"
fi
# Confirm the un-flushed 'rb' point did NOT become durable:
got=$(sqlite3 "$DB" ".load $EXT" "SELECT COUNT(*) FROM metrics WHERE name = 'rb';")
check_eq "unflushed point lost on reopen (expected POC semantics)" "$got" "0"

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
INSERT INTO traces(traces) VALUES ('optimize');
SELECT 'codecs', COUNT(*) FILTER (WHERE codec = 1), COUNT(*) FILTER (WHERE codec = 5) FROM traces_blocks;
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
expected='before|3|12|3
after|1|4|1
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
# line (no ts), one malformed line and one NaN line (each counted as an
# error, neither fatal — partial success succeeds silently, like a real
# Prometheus server scrape).
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
# ingested = 3 samples (rowid = count; the 2 bad lines are errors, not
# samples, and NOT fatal). 'temp' proves the explicit ms ts came out as
# SECONDS (1753000000123 ms → 1753000000 s) and labels round-trip in
# canonical sorted-JSON. 'bucket' pins one exact multi-label value.
# 'default_shared' = 1: both no-ts samples got the SAME default (one
# wall-clock read per body). 'default_sane': that default is epoch
# SECONDS in a sane range — 1750000000 ≈ mid-2025 < now, and the 4e9
# upper bound would be violated by a milliseconds default (~1.79e12),
# so this asserts the unit, not just "recent".
expected='ingested|3
total|3
temp|node_temp_celsius|1753000000|42.5|{"host":"pvm1","sensor":"cpu0"}
bucket|129389.0|{"code":"200","le":"0.5","method":"GET"}
default_shared|1
default_sane|2'
check_eq "prometheus body: count, ms→s ts, labels, shared seconds default" "$got" "$expected"

# Batch v0 still works THROUGH THE SAME DISPATCH into the same table
# (regression: section 7's blob starts with 0x01 and must keep taking
# the batch path, not the text path). 3 more points → 6 total. Flushed
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
total|6'
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
check_eq "rejected payloads stored nothing" "$got" "6"

# ---------------------------------------------------------------------------
echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "ALL SECTIONS PASSED"
else
  echo "$FAILURES CHECK(S) FAILED"
  exit 1
fi
