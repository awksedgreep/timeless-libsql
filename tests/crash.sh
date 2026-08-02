#!/usr/bin/env bash
# kill -9 crash test (hardening session, Job 3). Invoked from cli.sh
# section 20, or standalone:  tests/crash.sh <path-to-libtimeless_ext.so>
#
# WHAT DURABILITY IS PROMISED (the contract these assertions pin down):
#   - FLUSHED = DURABLE. A 'flush' command writes chunk/block rows
#     through the host connection; once the enclosing SQLite transaction
#     commits, that data survives anything SQLite itself survives —
#     including kill -9 mid-write (journal/WAL recovery on next open).
#   - BUFFERED = LOST. Points/entries/spans still in engine memory die
#     with the process. That is the same deal every buffering TSDB
#     offers, and it is stated in PLAN.md and RESULTS.md.
#   - NEVER CORRUPT. Whatever moment the process dies, reopening must
#     give integrity_check == ok, a working vtab (xConnect recovery from
#     shadow-table metadata), and internally-consistent indexes: every
#     _terms / _trace_blocks row joins to an existing _blocks row (the
#     never-dangle rule holds THROUGH crashes because index rows ride
#     the same transaction as their blocks).
#
# Mechanics: each iteration spawns a sqlite3 process running a long
# ingest script against a FRESH db. The script loops rounds of
# BEGIN; <inserts into all three vtabs>; flush x3; COMMIT; and prints a
# watermark line "WM <round>" AFTER the commit — so every watermark in
# the log corresponds to durably-committed data. We kill -9 at a random
# 0.1–0.8s, reopen, and assert count(vtab) >= watermark * rows-per-round
# for each signal. (>= not ==: a partial NEXT round may have committed
# after the last watermark hit the log; stdout buffering can only make
# the recorded watermark LOWER than the durable truth, never higher, so
# the assertion is sound.)
#
# Round sizing note: rows/round stays far below the 8192 auto-flush
# thresholds so the explicit per-round 'flush' is the only persistence
# event — that keeps "durable rows per watermark" exactly computable.

set -euo pipefail

EXT="${1:?usage: crash.sh <path-to-libtimeless_ext.so>}"
ITERATIONS=5
# 3000 rounds ≈ several seconds of wall time on tmpfs — the kill at
# 0.1–0.8s reliably lands MID-FLIGHT (a 200-round script finished in
# ~250ms and every "crash" was actually a clean exit; watermarks that
# always equal ROUNDS mean the test tested nothing).
ROUNDS=3000
M_PER_ROUND=10   # metric points per round
L_PER_ROUND=10   # log entries per round
T_PER_ROUND=10   # spans per round

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

FAILURES=0
pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; FAILURES=$((FAILURES + 1)); }

# ── Generate the ingest script ONCE (pure function of the constants;
# python3 because 3000 rounds × ~33 statements = ~100k lines, which a
# bash echo loop generates too slowly). Values/timestamps are
# deterministic per (round, i) so any surviving prefix is predictable;
# only WHERE the kill lands is racy — by design.
INGEST="$TMP/ingest.sql"
python3 - "$EXT" "$ROUNDS" "$M_PER_ROUND" "$L_PER_ROUND" "$T_PER_ROUND" > "$INGEST" <<'PY'
import sys
ext, rounds, m_n, l_n, t_n = sys.argv[1], *map(int, sys.argv[2:6])
w = sys.stdout.write
w(f".load {ext}\n")
w("CREATE VIRTUAL TABLE metrics USING timeless_metrics(rollups='60s@0');\n")
w("CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');\n")
w("CREATE VIRTUAL TABLE traces USING timeless_traces;\n")
for r in range(1, rounds + 1):
    w("BEGIN;\n")
    for i in range(m_n):
        w(f"INSERT INTO metrics(name, ts, value, labels) VALUES ('m{i % 3}', {1700000000 + r * 100 + i}, {r}.{i}, '{{\"host\":\"h{i % 2}\"}}');\n")
    for i in range(l_n):
        lvl = "error" if i % 4 == 3 else "info"
        w(f"INSERT INTO logs(ts, level, message, service) VALUES ({1700000000000 + r * 1000 + i}, '{lvl}', 'round {r} entry {i}', 'svc{i % 2}');\n")
    for i in range(t_n):
        st = "error" if i % 5 == 0 else "ok"
        ts = 1700000000000000000 + r * 1000000 + i
        desc = "crash-path" if st == "error" else ""
        w(f'''INSERT INTO traces(trace_id, span_id, name, service, status, start_ts, attributes, status_description, events, resource, instrumentation_scope)
VALUES (x'{r * 31 + i % 7:032x}', x'{r * 1000 + i:016x}', 'op{i % 3}', 'must-not-win', '{st}', {ts},
'{{"bool":true,"count":{i},"service.name":"s{i % 2}"}}', '{desc}',
'[{{"attributes":{{"attempt":{i},"fatal":false}},"name":"checkpoint","timestamp":{ts + 1}}}]',
'{{"deployment.environment":"crash","service.name":"resource-must-not-win"}}',
'{{"attributes":{{"debug":false}},"name":"crash-lib","version":"1.0"}}');
''')
    w("INSERT INTO metrics(metrics) VALUES ('flush');\n")
    w("INSERT INTO logs(logs) VALUES ('flush');\n")
    w("INSERT INTO traces(traces) VALUES ('flush');\n")
    # Sprinkle maintenance into the crash window too: compaction must
    # be exactly as crash-safe as flush (same host-transaction ride).
    if r % 25 == 0:
        w("INSERT INTO logs(logs) VALUES ('optimize');\n")
        w("INSERT INTO traces(traces) VALUES ('optimize');\n")
        w("INSERT INTO metrics(metrics) VALUES ('compact');\n")
        w("INSERT INTO metrics(metrics) VALUES ('rollup');\n")
    w("COMMIT;\n")
    w(f"SELECT 'WM {r}';\n")
PY

for ((iter = 1; iter <= ITERATIONS; iter++)); do
  DB="$TMP/crash_$iter.db"
  LOG="$TMP/crash_$iter.log"
  rm -f "$DB" "$DB-journal" "$DB-wal"

  sqlite3 "$DB" < "$INGEST" > "$LOG" 2>/dev/null &
  PID=$!
  # Random 100–800 ms of life. $RANDOM is fine here: the KILL TIMING is
  # supposed to be arbitrary; determinism lives in the ingest script.
  SLEEP_MS=$((100 + RANDOM % 701))
  sleep "0.$(printf '%03d' "$SLEEP_MS")"
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true

  WM=$(grep -o 'WM [0-9]*' "$LOG" | tail -1 | cut -d' ' -f2 || true)
  WM=${WM:-0}
  echo "iteration $iter: killed after ${SLEEP_MS}ms, last committed watermark: round $WM"

  # 1. Physical integrity: SQLite's own crash recovery (rollback
  #    journal replay) must leave a clean file. No extension needed.
  ic=$(sqlite3 "$DB" "PRAGMA integrity_check;")
  if [[ "$ic" == "ok" ]]; then
    pass "integrity_check ok"
  else
    fail "integrity_check: $ic"
  fi

  # 2. The vtabs must reopen (xConnect recovery from shadow metadata)
  #    and full counts must succeed — count(*) decodes every chunk and
  #    block, so this doubles as "every stored payload is readable".
  #
  #    RACE, OBSERVED TWICE (2026-07-26 uncaptured, 2026-07-30 captured):
  #    a kill in the first ~100ms can land BETWEEN the three autocommit
  #    CREATE VIRTUAL TABLE statements, so a later table legitimately
  #    does not exist at reopen. That is correct durability behavior
  #    (the CREATE never committed), not a failure — it only ever
  #    happens with WM=0, when nothing is owed. Detect it and pass.
  counts=$(sqlite3 "$DB" ".load $EXT" \
    "SELECT (SELECT COUNT(*) FROM metrics) || '|' || (SELECT COUNT(*) FROM logs) || '|' || (SELECT COUNT(*) FROM traces);" 2>&1) || {
    if [[ "$WM" == "0" && "$counts" == *"no such table"* ]]; then
      pass "kill landed before schema commit (WM=0, partial CREATEs rolled back cleanly)"
      continue
    fi
    fail "reopen/count failed: $counts"
    continue
  }
  IFS='|' read -r mc lc tc <<< "$counts"
  pass "reopen + full decode: metrics=$mc logs=$lc traces=$tc"

  # 2a. Generation-2 rich columns must remain typed JSON after a kill,
  #     including blocks that optimize may have rewritten.
  rich=$(sqlite3 "$DB" ".load $EXT" \
    "SELECT COUNT(*) FROM traces
       WHERE service IN ('s0','s1')
         AND json_valid(attributes) AND json_type(attributes, '$.bool')='true'
         AND json_valid(events) AND json_type(events)='array'
         AND json_valid(resource) AND json_type(resource)='object'
         AND json_valid(instrumentation_scope) AND json_type(instrumentation_scope)='object';" 2>&1) || {
    fail "rich trace fidelity query failed: $rich"
    continue
  }
  if [[ "$rich" == "$tc" ]]; then
    pass "all $tc decoded traces retain rich typed JSON post-crash"
  else
    fail "rich trace fidelity count $rich != trace count $tc"
  fi

  # 2b. F3: every surviving rollup chunk must decode (count forces a
  #     full read of the 60s tier); errors here mean a torn rollup row
  #     escaped the transaction, which must be impossible.
  rc=$(sqlite3 "$DB" ".load $EXT" \
    "SELECT COUNT(*) FROM timeless_rollup('metrics', 'm0', NULL, 60, 0, 9999999999, 'count');" 2>&1) || {
    fail "rollup reopen/decode failed: $rc"
    continue
  }
  pass "rollup tier readable post-crash ($rc buckets for m0)"

  # 3. Durability floor: everything flushed at the last watermark must
  #    be present (>=, see header). If WM=0 the kill landed before the
  #    first commit — nothing is owed.
  ok=1
  ((mc >= WM * M_PER_ROUND)) || { ok=0; fail "metrics count $mc < watermark $((WM * M_PER_ROUND))"; }
  ((lc >= WM * L_PER_ROUND)) || { ok=0; fail "logs count $lc < watermark $((WM * L_PER_ROUND))"; }
  ((tc >= WM * T_PER_ROUND)) || { ok=0; fail "traces count $tc < watermark $((WM * T_PER_ROUND))"; }
  ((ok)) && pass "flushed-before-kill data present (>= watermark counts)"

  # 4. Index-consistency invariants, in SQL (the never-dangle rule):
  #    every _terms / _trace_blocks row joins to a _blocks row, every
  #    block is reachable from at least one term (blocks always carry a
  #    level:/status: term), and every chunk row decodes (covered by 2).
  inv=$(sqlite3 "$DB" <<'SQL'
SELECT
  (SELECT COUNT(*) FROM logs_terms t LEFT JOIN logs_blocks b ON t.block_id = b.id WHERE b.id IS NULL)
  || '|' ||
  (SELECT COUNT(*) FROM traces_terms t LEFT JOIN traces_blocks b ON t.block_id = b.id WHERE b.id IS NULL)
  || '|' ||
  (SELECT COUNT(*) FROM traces_trace_blocks tb LEFT JOIN traces_blocks b ON tb.block_id = b.id WHERE b.id IS NULL)
  || '|' ||
  (SELECT COUNT(*) FROM logs_blocks b WHERE NOT EXISTS (SELECT 1 FROM logs_terms t WHERE t.block_id = b.id))
  || '|' ||
  (SELECT COUNT(*) FROM traces_blocks b WHERE NOT EXISTS (SELECT 1 FROM traces_terms t WHERE t.block_id = b.id));
SQL
)
  if [[ "$inv" == "0|0|0|0|0" ]]; then
    pass "no dangling _terms/_trace_blocks rows, no term-less blocks"
  else
    fail "index invariant violated (dangling/orphan counts: $inv)"
  fi

  # 5. A pushdown query on each signal still works post-crash (the
  #    recovered posting lists / trace index answer, not just scans).
  q=$(sqlite3 "$DB" ".load $EXT" \
    "SELECT (SELECT COUNT(*) FROM metrics WHERE name='m0') >= 0
        AND (SELECT COUNT(*) FROM logs WHERE level='error') >= 0
        AND (SELECT COUNT(*) FROM traces WHERE status='error') >= 0;" 2>&1) || {
    fail "post-crash pushdown query failed: $q"
    continue
  }
  [[ "$q" == "1" ]] && pass "pushdown queries answer post-crash"
done

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "CRASH TEST PASSED ($ITERATIONS iterations)"
else
  echo "CRASH TEST: $FAILURES check(s) failed"
  exit 1
fi
