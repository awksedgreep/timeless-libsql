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
echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "ALL SECTIONS PASSED"
else
  echo "$FAILURES CHECK(S) FAILED"
  exit 1
fi
