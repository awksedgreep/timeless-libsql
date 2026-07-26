#!/usr/bin/env python3
"""Transactional DROP/recreate regressions for the process engine registry."""

import os
import sqlite3
import sys


SIGNALS = ("metrics", "logs", "traces")


def connect(extension, db_path):
    db = sqlite3.connect(db_path, isolation_level=None, timeout=10)
    db.enable_load_extension(True)
    db.load_extension(extension)
    return db


def scalar(db, sql, params=()):
    return db.execute(sql, params).fetchone()[0]


def create_tables(db):
    db.executescript(
        """
        CREATE VIRTUAL TABLE metrics USING timeless_metrics;
        CREATE VIRTUAL TABLE logs USING timeless_logs;
        CREATE VIRTUAL TABLE traces USING timeless_traces;
        """
    )


def command(db, table, value):
    db.execute(f'INSERT INTO "{table}"("{table}") VALUES(?)', (value,))


def flush_all(db):
    for table in SIGNALS:
        command(db, table, "flush")


def insert_rows(db, suffix, ts):
    db.execute(
        "INSERT INTO metrics(name, ts, value) VALUES(?, ?, ?)",
        (f"metric_{suffix}", ts, float(ts)),
    )
    db.execute(
        "INSERT INTO logs(ts, level, message) VALUES(?, 'info', ?)",
        (ts, f"log_{suffix}"),
    )
    byte = b"\x11" if suffix.startswith("flushed") else b"\x22"
    db.execute(
        "INSERT INTO traces(trace_id, span_id, name, service, start_ts) "
        "VALUES(?, ?, ?, 'r4', ?)",
        (byte * 16, byte * 8, f"trace_{suffix}", ts),
    )


def counts(db):
    return (
        scalar(db, "SELECT COUNT(*) FROM metrics"),
        scalar(db, "SELECT COUNT(*) FROM logs"),
        scalar(db, "SELECT COUNT(*) FROM traces"),
    )


def instance_ids(db):
    ids = []
    for table in SIGNALS:
        row = db.execute(
            f"SELECT v FROM {table}_meta WHERE k='instance_id'"
        ).fetchone()
        ids.append(None if row is None else row[0])
    return tuple(ids)


def drop_all(db):
    for table in SIGNALS:
        db.execute(f'DROP TABLE "{table}"')


def test_drop_rollback_preserves_buffered_state(extension, tmp):
    db_path = os.path.join(tmp, "rollback.db")
    db = connect(extension, db_path)
    create_tables(db)
    insert_rows(db, "flushed", 10)
    flush_all(db)
    insert_rows(db, "buffered", 20)
    before_ids = instance_ids(db)
    assert counts(db) == (2, 2, 2)

    db.execute("BEGIN")
    drop_all(db)
    db.execute("ROLLBACK")

    assert instance_ids(db) == before_ids
    assert counts(db) == (2, 2, 2), "DROP rollback lost committed buffered rows"
    flush_all(db)
    db.close()

    reopened = connect(extension, db_path)
    assert counts(reopened) == (2, 2, 2)
    reopened.close()


def test_drop_rollback_keeps_preconnected_engine_shared(extension, tmp):
    db_path = os.path.join(tmp, "shared.db")
    first = connect(extension, db_path)
    create_tables(first)
    second = connect(extension, db_path)
    assert counts(second) == (0, 0, 0)

    insert_rows(first, "buffered_a", 30)
    assert counts(second) == (1, 1, 1)
    first.execute("BEGIN")
    drop_all(first)
    first.execute("ROLLBACK")

    assert counts(second) == (1, 1, 1)
    insert_rows(second, "buffered_b", 40)
    assert counts(first) == (2, 2, 2)
    first.close()
    second.close()


def test_committed_drop_recreate_gets_fresh_identity(extension, tmp):
    db_path = os.path.join(tmp, "recreate.db")
    first = connect(extension, db_path)
    create_tables(first)
    insert_rows(first, "old_buffered", 50)
    old_ids = instance_ids(first)

    first.execute("BEGIN")
    drop_all(first)
    first.execute("COMMIT")
    create_tables(first)
    new_ids = instance_ids(first)
    assert all(old != new for old, new in zip(old_ids, new_ids)), (
        old_ids,
        new_ids,
    )
    assert counts(first) == (0, 0, 0)
    second = connect(extension, db_path)
    assert counts(second) == (0, 0, 0), "old engine leaked through recreate"
    first.close()
    second.close()


def test_failed_destroy_does_not_split_registry(extension, tmp):
    db_path = os.path.join(tmp, "failed-destroy.db")
    first = connect(extension, db_path)
    first.execute("CREATE VIRTUAL TABLE metrics USING timeless_metrics")
    second = connect(extension, db_path)
    assert scalar(second, "SELECT COUNT(*) FROM metrics") == 0
    first.execute(
        "INSERT INTO metrics(name, ts, value) VALUES('buffered_before_failure', 60, 1)"
    )

    denied = {"seen": False}

    def authorizer(action, arg1, _arg2, _database, _trigger):
        if action == sqlite3.SQLITE_DROP_TABLE and arg1 == "metrics_chunks":
            denied["seen"] = True
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    first.set_authorizer(authorizer)
    try:
        first.execute("DROP TABLE metrics")
    except sqlite3.DatabaseError:
        pass
    else:
        raise AssertionError("authorizer-forced shadow DROP unexpectedly succeeded")
    finally:
        first.set_authorizer(None)
    assert denied["seen"], "failure occurred before xDestroy attempted shadow DDL"
    assert scalar(first, "SELECT COUNT(*) FROM metrics") == 1

    # A third xConnect must find the still-live shared engine. Before R4,
    # xDestroy removed its registry entry before the denied DDL, so this
    # connection constructed a split engine and omitted the buffered row.
    third = connect(extension, db_path)
    assert scalar(third, "SELECT COUNT(*) FROM metrics") == 1
    third.execute(
        "INSERT INTO metrics(name, ts, value) VALUES('after_failure', 61, 2)"
    )
    assert scalar(second, "SELECT COUNT(*) FROM metrics") == 2
    first.close()
    second.close()
    third.close()


def test_instance_id_migration_and_validation(extension, tmp):
    db_path = os.path.join(tmp, "instance-migration.db")
    db = connect(extension, db_path)
    create_tables(db)
    insert_rows(db, "flushed_migration", 70)
    flush_all(db)
    db.close()

    plain = sqlite3.connect(db_path, isolation_level=None)
    for table in SIGNALS:
        plain.execute(f"DELETE FROM {table}_meta WHERE k='instance_id'")
    plain.close()

    migrated = connect(extension, db_path)
    assert counts(migrated) == (1, 1, 1)
    assert all(
        isinstance(instance_id, bytes) and len(instance_id) == 16
        for instance_id in instance_ids(migrated)
    )
    migrated.close()

    plain = sqlite3.connect(db_path, isolation_level=None)
    plain.execute(
        "UPDATE metrics_meta SET v=x'01' WHERE k='instance_id'"
    )
    plain.close()
    corrupt = connect(extension, db_path)
    try:
        scalar(corrupt, "SELECT COUNT(*) FROM metrics")
    except sqlite3.DatabaseError:
        pass
    else:
        raise AssertionError("corrupt instance_id reopened successfully")
    corrupt.close()


def main():
    extension, tmp = sys.argv[1:]
    tests = (
        test_drop_rollback_preserves_buffered_state,
        test_drop_rollback_keeps_preconnected_engine_shared,
        test_committed_drop_recreate_gets_fresh_identity,
        test_failed_destroy_does_not_split_registry,
        test_instance_id_migration_and_validation,
    )
    for test in tests:
        test(extension, tmp)
        print(f"PASS {test.__name__}")


if __name__ == "__main__":
    main()
