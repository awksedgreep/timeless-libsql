#!/usr/bin/env bash
# Focused correctness regressions from REVIEW_FIX_PLAN.md.
#
# Usage:
#   ./tests/correctness.sh [r1|r2|r3|r4|r8]
#
# TIMELESS_EXT may point at an already-built extension. Otherwise this script
# builds the release cdylib before running the selected section.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXT="${TIMELESS_EXT:-$ROOT/target/release/libtimeless_ext.so}"
SECTION="${1:-r1}"

case "$SECTION" in
  r1|r2|r3|r4|r8) ;;
  *)
    echo "unknown correctness section: $SECTION" >&2
    exit 2
    ;;
esac

if [[ -z "${TIMELESS_EXT:-}" ]]; then
  cargo build -p timeless-ext --release --manifest-path "$ROOT/Cargo.toml"
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

if [[ "$SECTION" == "r2" ]]; then
  echo "== R2: database-authoritative metrics catalog =="
  python3 "$ROOT/tests/r2_multiprocess.py" "$EXT" "$TMP"
  exit
fi

if [[ "$SECTION" == "r3" ]]; then
  echo "== R3: attached-database schema qualification =="
  python3 "$ROOT/tests/r3_attached.py" "$EXT" "$TMP"
  exit
fi

if [[ "$SECTION" == "r4" ]]; then
  echo "== R4: transactional DROP and recreate identity =="
  python3 "$ROOT/tests/r4_drop.py" "$EXT" "$TMP"
  exit
fi

if [[ "$SECTION" == "r8" ]]; then
  echo "== R8: timestamp extremes and NULL constraints =="
  python3 "$ROOT/tests/r8_extremes.py" "$EXT" "$TMP"
  exit
fi

echo "== R1: statement atomicity and savepoints =="
python3 - "$EXT" "$TMP/r1.db" <<'PY'
import sqlite3
import sys

extension, db_path = sys.argv[1:]


def connect():
    db = sqlite3.connect(db_path, isolation_level=None)
    db.enable_load_extension(True)
    db.load_extension(extension)
    return db


def scalar(db, sql):
    return db.execute(sql).fetchone()[0]


def expect_error(db, sql, label):
    try:
        db.execute(sql)
    except sqlite3.DatabaseError:
        return
    raise AssertionError(f"{label}: statement unexpectedly succeeded")


db = connect()
db.executescript(
    """
    CREATE VIRTUAL TABLE metrics USING timeless_metrics;
    CREATE VIRTUAL TABLE logs USING timeless_logs;
    CREATE VIRTUAL TABLE traces USING timeless_traces;
    """
)

# Metrics used to mark a partition as flushable at 4,096 points without ever
# draining the engine's pending queue. Pin the public threshold itself: 4,095
# remains queryable in memory, and the next point creates exactly one durable
# raw chunk without any host-issued `flush` command.
db.execute("CREATE VIRTUAL TABLE metrics_threshold USING timeless_metrics")
db.execute(
    """
    WITH RECURSIVE seq(n) AS (
      VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < 4095
    )
    INSERT INTO metrics_threshold(name, ts, value)
    SELECT 'threshold_metric', 100000 + n, n FROM seq
    """
)


def metric_stat(key):
    return db.execute(
        "SELECT CAST(value AS INTEGER) FROM timeless_stats('metrics_threshold') "
        "WHERE key = ?",
        (key,),
    ).fetchone()[0]


assert metric_stat("buffered_points") == 4095
assert metric_stat("disk_points") == 0
assert scalar(db, "SELECT COUNT(*) FROM metrics_threshold_chunks") == 0
db.execute(
    "INSERT INTO metrics_threshold(name, ts, value) "
    "VALUES ('threshold_metric', 104096, 4096)"
)
assert metric_stat("buffered_points") == 0
assert metric_stat("disk_points") == 4096
assert scalar(db, "SELECT COUNT(*) FROM metrics_threshold_chunks") == 1

# A statement that fails after its first xUpdate must roll back that first
# in-memory mutation while leaving the explicit outer transaction usable.
statement_cases = [
    (
        "metrics",
        """
        INSERT INTO metrics(name, ts, value)
        VALUES ('stmt_bad', 10, 1.0), ('stmt_bad', 11, NULL)
        """,
        "SELECT COUNT(*) FROM metrics WHERE name = 'stmt_bad'",
    ),
    (
        "logs",
        """
        INSERT INTO logs(ts, level, message)
        VALUES (10, 'info', 'stmt_bad'), (11, 'fatal', 'stmt_bad')
        """,
        "SELECT COUNT(*) FROM logs WHERE message = 'stmt_bad'",
    ),
    (
        "traces",
        """
        INSERT INTO traces(trace_id, span_id, name, service, start_ts)
        VALUES
          (zeroblob(16), zeroblob(8), 'stmt_bad', 'svc', 10),
          (x'01', zeroblob(8), 'stmt_bad', 'svc', 11)
        """,
        "SELECT COUNT(*) FROM traces WHERE name = 'stmt_bad'",
    ),
]

for table, sql, count_sql in statement_cases:
    db.execute("BEGIN")
    expect_error(db, sql, f"{table} failed multi-row insert")
    assert db.in_transaction, f"{table}: failed statement ended outer transaction"
    assert scalar(db, count_sql) == 0, (
        f"{table}: first row from failed statement leaked in memory"
    )
    db.execute("COMMIT")

# The same statement-level rollback must work when the failed statement owns
# the outer autocommit transaction rather than running inside an explicit one.
for table, sql, count_sql in statement_cases:
    sql = sql.replace("stmt_bad", "autocommit_bad")
    count_sql = count_sql.replace("stmt_bad", "autocommit_bad")
    expect_error(db, sql, f"{table} failed autocommit multi-row insert")
    assert not db.in_transaction, f"{table}: failed autocommit statement stayed active"
    assert scalar(db, count_sql) == 0, (
        f"{table}: autocommit statement leaked its first row"
    )

# Force the failed statement across the logs/traces auto-flush threshold.
# The flush drains data that predates the statement too, so rollback must both
# remove the statement's new block locations and restore the old buffer.
threshold_cases = [
    (
        "logs",
        """
        WITH RECURSIVE seq(n) AS (
          VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < 8200
        )
        INSERT INTO logs(ts, level, message)
        SELECT 10000 + n,
               CASE WHEN n = 8200 THEN 'fatal' ELSE 'info' END,
               'threshold_bad'
        FROM seq
        """,
        "SELECT COUNT(*) FROM logs WHERE message = 'threshold_bad'",
        "SELECT COUNT(*) FROM logs_blocks",
    ),
    (
        "traces",
        """
        WITH RECURSIVE seq(n) AS (
          VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < 8200
        )
        INSERT INTO traces(trace_id, span_id, name, service, start_ts)
        SELECT CASE WHEN n = 8200 THEN x'01' ELSE zeroblob(16) END,
               zeroblob(8),
               'threshold_bad', 'svc', 10000 + n
        FROM seq
        """,
        "SELECT COUNT(*) FROM traces WHERE name = 'threshold_bad'",
        "SELECT COUNT(*) FROM traces_blocks",
    ),
]

for table, sql, count_sql, block_count_sql in threshold_cases:
    db.execute("BEGIN")
    expect_error(db, sql, f"{table} failed auto-flushing insert")
    assert scalar(db, count_sql) == 0, (
        f"{table}: rows leaked after failed auto-flushing statement"
    )
    assert scalar(db, block_count_sql) == 0, (
        f"{table}: block locations leaked after failed auto-flushing statement"
    )
    db.execute("COMMIT")

# ROLLBACK TO must restore the checkpoint without discarding work performed
# before the savepoint or ending the outer transaction.
savepoint_cases = [
    (
        "metrics",
        """
        INSERT INTO metrics(name, ts, value)
        VALUES ('save_outer', 20, 2.0)
        """,
        """
        INSERT INTO metrics(name, ts, value)
        VALUES ('save_inner', 21, 3.0)
        """,
        "SELECT COUNT(*) FROM metrics WHERE name = 'save_outer'",
        "SELECT COUNT(*) FROM metrics WHERE name = 'save_inner'",
    ),
    (
        "logs",
        """
        INSERT INTO logs(ts, level, message)
        VALUES (20, 'info', 'save_outer')
        """,
        """
        INSERT INTO logs(ts, level, message)
        VALUES (21, 'error', 'save_inner')
        """,
        "SELECT COUNT(*) FROM logs WHERE message = 'save_outer'",
        "SELECT COUNT(*) FROM logs WHERE message = 'save_inner'",
    ),
    (
        "traces",
        """
        INSERT INTO traces(trace_id, span_id, name, service, start_ts)
        VALUES (x'11111111111111111111111111111111',
                x'1111111111111111', 'save_outer', 'svc', 20)
        """,
        """
        INSERT INTO traces(trace_id, span_id, name, service, start_ts)
        VALUES (x'22222222222222222222222222222222',
                x'2222222222222222', 'save_inner', 'svc', 21)
        """,
        "SELECT COUNT(*) FROM traces WHERE name = 'save_outer'",
        "SELECT COUNT(*) FROM traces WHERE name = 'save_inner'",
    ),
]

for table, outer_sql, inner_sql, outer_count, inner_count in savepoint_cases:
    db.execute("BEGIN")
    db.execute(outer_sql)
    db.execute(f"SAVEPOINT {table}_sp")
    db.execute(inner_sql)
    db.execute(f"ROLLBACK TO {table}_sp")
    db.execute(f"RELEASE {table}_sp")
    assert db.in_transaction, f"{table}: ROLLBACK TO ended outer transaction"
    db.execute("COMMIT")
    assert scalar(db, outer_count) == 1, f"{table}: pre-savepoint row was lost"
    assert scalar(db, inner_count) == 0, f"{table}: savepoint row leaked in memory"

# SQLite may open the vtable transaction only after a savepoint already
# exists. It must replay that active savepoint immediately after xBegin.
boundary_cases = [
    (
        "metrics",
        "INSERT INTO metrics(name, ts, value) VALUES ('boundary_row', 30, 3.0)",
        "SELECT COUNT(*) FROM metrics WHERE name = 'boundary_row'",
    ),
    (
        "logs",
        "INSERT INTO logs(ts, level, message) VALUES (30, 'info', 'boundary_row')",
        "SELECT COUNT(*) FROM logs WHERE message = 'boundary_row'",
    ),
    (
        "traces",
        """
        INSERT INTO traces(trace_id, span_id, name, service, start_ts)
        VALUES (x'30303030303030303030303030303030',
                x'3030303030303030', 'boundary_row', 'svc', 30)
        """,
        "SELECT COUNT(*) FROM traces WHERE name = 'boundary_row'",
    ),
]

for table, insert_sql, count_sql in boundary_cases:
    db.execute("BEGIN")
    db.execute(f"SAVEPOINT {table}_before_begin")
    db.execute(insert_sql.replace("boundary_row", "late_participant"))
    db.execute(f"ROLLBACK TO {table}_before_begin")
    db.execute(f"RELEASE {table}_before_begin")
    db.execute("COMMIT")
    assert scalar(db, count_sql.replace("boundary_row", "late_participant")) == 0, (
        f"{table}: savepoint created before xBegin did not roll back"
    )

# Releasing a savepoint commits it only into its parent transaction. A later
# outer rollback must still undo the released frame.
for table, insert_sql, count_sql in boundary_cases:
    db.execute("BEGIN")
    db.execute(f"SAVEPOINT {table}_released")
    db.execute(insert_sql.replace("boundary_row", "released_then_rolled_back"))
    db.execute(f"RELEASE {table}_released")
    db.execute("ROLLBACK")
    assert scalar(
        db, count_sql.replace("boundary_row", "released_then_rolled_back")
    ) == 0, f"{table}: released savepoint escaped outer rollback"

# Maintenance mutates buffers, engine indexes, and shadow tables together.
# Rollback-to must restore the precise pre-savepoint generation while keeping
# the outer transaction's buffered row.
db.executescript(
    """
    CREATE VIRTUAL TABLE metrics_maint USING timeless_metrics;
    CREATE VIRTUAL TABLE logs_maint USING timeless_logs;
    CREATE VIRTUAL TABLE traces_maint USING timeless_traces;

    INSERT INTO metrics_maint(name, ts, value) VALUES ('old', 1, 1.0);
    INSERT INTO metrics_maint(metrics_maint) VALUES ('flush');
    INSERT INTO metrics_maint(name, ts, value) VALUES ('buffered', 2, 2.0);

    INSERT INTO logs_maint(ts, level, message) VALUES (1, 'info', 'old');
    INSERT INTO logs_maint(logs_maint) VALUES ('flush');
    INSERT INTO logs_maint(ts, level, message) VALUES (2, 'info', 'buffered');

    INSERT INTO traces_maint(trace_id, span_id, name, service, start_ts)
    VALUES (x'01010101010101010101010101010101',
            x'0101010101010101', 'old', 'svc', 1);
    INSERT INTO traces_maint(traces_maint) VALUES ('flush');
    INSERT INTO traces_maint(trace_id, span_id, name, service, start_ts)
    VALUES (x'02020202020202020202020202020202',
            x'0202020202020202', 'buffered', 'svc', 2);
    """
)

maintenance_cases = [
    (
        "metrics_maint",
        "INSERT INTO metrics_maint(name, ts, value) VALUES ('outer', 3, 3.0)",
        [
            "INSERT INTO metrics_maint(metrics_maint) VALUES ('flush')",
            "INSERT INTO metrics_maint(metrics_maint) VALUES ('compact')",
            "INSERT INTO metrics_maint(metrics_maint) VALUES ('prune:1000')",
        ],
        "SELECT COUNT(*) FROM metrics_maint",
        "SELECT COUNT(*) FROM metrics_maint_chunks",
    ),
    (
        "logs_maint",
        "INSERT INTO logs_maint(ts, level, message) VALUES (3, 'info', 'outer')",
        [
            "INSERT INTO logs_maint(logs_maint) VALUES ('flush')",
            "INSERT INTO logs_maint(logs_maint) VALUES ('optimize')",
            "INSERT INTO logs_maint(logs_maint) VALUES ('prune:1000')",
        ],
        "SELECT COUNT(*) FROM logs_maint",
        "SELECT COUNT(*) FROM logs_maint_blocks",
    ),
    (
        "traces_maint",
        """
        INSERT INTO traces_maint(trace_id, span_id, name, service, start_ts)
        VALUES (x'03030303030303030303030303030303',
                x'0303030303030303', 'outer', 'svc', 3)
        """,
        [
            "INSERT INTO traces_maint(traces_maint) VALUES ('flush')",
            "INSERT INTO traces_maint(traces_maint) VALUES ('optimize')",
            "INSERT INTO traces_maint(traces_maint) VALUES ('prune:1000')",
        ],
        "SELECT COUNT(*) FROM traces_maint",
        "SELECT COUNT(*) FROM traces_maint_blocks",
    ),
]

for table, outer_sql, commands, row_count_sql, block_count_sql in maintenance_cases:
    db.execute("BEGIN")
    db.execute(outer_sql)
    db.execute(f"SAVEPOINT {table}_maintenance")
    for command in commands:
        db.execute(command)
    assert scalar(db, row_count_sql) == 0, f"{table}: prune did not exercise rollback"
    db.execute(f"ROLLBACK TO {table}_maintenance")
    db.execute(f"RELEASE {table}_maintenance")
    db.execute("COMMIT")
    assert scalar(db, row_count_sql) == 3, (
        f"{table}: maintenance rollback did not restore all rows"
    )
    assert scalar(db, block_count_sql) == 1, (
        f"{table}: maintenance rollback did not restore shadow generation"
    )

# A later committed flush must not make either class of rolled-back row durable.
db.execute("INSERT INTO metrics(metrics) VALUES ('flush')")
db.execute("INSERT INTO logs(logs) VALUES ('flush')")
db.execute("INSERT INTO traces(traces) VALUES ('flush')")
db.execute("INSERT INTO metrics_maint(metrics_maint) VALUES ('flush')")
db.execute("INSERT INTO logs_maint(logs_maint) VALUES ('flush')")
db.execute("INSERT INTO traces_maint(traces_maint) VALUES ('flush')")
db.close()

db = connect()
for table, _, count_sql in statement_cases:
    assert scalar(db, count_sql) == 0, (
        f"{table}: failed-statement row became durable after flush/reopen"
    )
    assert scalar(db, count_sql.replace("stmt_bad", "autocommit_bad")) == 0, (
        f"{table}: failed autocommit row became durable after flush/reopen"
    )
for table, _, count_sql, _ in threshold_cases:
    assert scalar(db, count_sql) == 0, (
        f"{table}: failed threshold row became durable after flush/reopen"
    )
for table, _, _, outer_count, inner_count in savepoint_cases:
    assert scalar(db, outer_count) == 1, f"{table}: outer row was not durable"
    assert scalar(db, inner_count) == 0, (
        f"{table}: savepoint row became durable after flush/reopen"
    )
for table, _, count_sql in boundary_cases:
    for tag in ("late_participant", "released_then_rolled_back"):
        assert scalar(db, count_sql.replace("boundary_row", tag)) == 0, (
            f"{table}: boundary savepoint row {tag!r} became durable"
        )
for table, _, _, row_count_sql, _ in maintenance_cases:
    assert scalar(db, row_count_sql) == 3, (
        f"{table}: maintenance rollback state changed after flush/reopen"
    )
db.close()

print("PASS: statement, savepoint, auto-flush, and maintenance rollback are atomic")
PY
