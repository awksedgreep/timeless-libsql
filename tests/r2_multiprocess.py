#!/usr/bin/env python3
"""Deterministic multi-process regressions for the metrics catalog."""

import multiprocessing
import os
import sqlite3
import struct
import sys
import traceback


def connect(extension, db_path):
    db = sqlite3.connect(db_path, isolation_level=None, timeout=10)
    db.enable_load_extension(True)
    db.load_extension(extension)
    return db


def initialize(extension, db_path):
    db = connect(extension, db_path)
    db.execute("CREATE VIRTUAL TABLE metrics USING timeless_metrics")
    db.close()


def worker(extension, db_path, control):
    try:
        db = connect(extension, db_path)
        # Force xConnect and engine recovery before telling the parent that
        # this process is ready.
        db.execute("SELECT COUNT(*) FROM metrics").fetchone()
        control.send(("ready", None))
        while True:
            command, payload = control.recv()
            if command == "stop":
                db.close()
                control.send(("ok", None))
                return
            if command == "write":
                for name, ts, value, labels in payload:
                    db.execute(
                        "INSERT INTO metrics(name, ts, value, labels) "
                        "VALUES (?, ?, ?, ?)",
                        (name, ts, value, labels),
                    )
                db.execute("INSERT INTO metrics(metrics) VALUES ('flush')")
                control.send(("ok", None))
                continue
            if command == "counts":
                names = payload
                result = {
                    name: db.execute(
                        "SELECT COUNT(*) FROM metrics WHERE name = ?", (name,)
                    ).fetchone()[0]
                    for name in names
                }
                control.send(("ok", result))
                continue
            raise AssertionError(f"unknown worker command: {command}")
    except Exception:
        control.send(("error", traceback.format_exc()))


def start_worker(ctx, extension, db_path, expect_ready=True):
    parent, child = ctx.Pipe()
    process = ctx.Process(target=worker, args=(extension, db_path, child))
    process.start()
    status, payload = parent.recv()
    if expect_ready:
        assert status == "ready", payload
    return process, parent, status, payload


def ask(process, control, command, payload=None):
    assert process.is_alive(), f"worker exited before {command}"
    control.send((command, payload))
    status, result = control.recv()
    assert status == "ok", result
    return result


def stop(process, control):
    if process.is_alive():
        ask(process, control, "stop")
    process.join(10)
    assert process.exitcode == 0


def direct_rows(db_path, sql):
    db = sqlite3.connect(db_path, isolation_level=None)
    try:
        return db.execute(sql).fetchall()
    finally:
        db.close()


def encode_legacy_registry(entries):
    out = bytearray(struct.pack(">I", len(entries)))
    for series_id, name, labels in entries:
        name_bytes = name.encode()
        out.extend(struct.pack(">qH", series_id, len(name_bytes)))
        out.extend(name_bytes)
        out.extend(struct.pack(">H", len(labels)))
        for key, value in sorted(labels.items()):
            key_bytes = key.encode()
            value_bytes = value.encode()
            out.extend(struct.pack(">H", len(key_bytes)))
            out.extend(key_bytes)
            out.extend(struct.pack(">H", len(value_bytes)))
            out.extend(value_bytes)
    return bytes(out)


def test_distinct_series_ids(ctx, extension, tmp):
    db_path = os.path.join(tmp, "distinct.db")
    initialize(extension, db_path)
    process_a, control_a, _, _ = start_worker(ctx, extension, db_path)
    process_b, control_b, _, _ = start_worker(ctx, extension, db_path)

    ask(process_a, control_a, "write", [("from_a", 10, 1.0, "{}")])
    ask(process_b, control_b, "write", [("from_b", 20, 2.0, "{}")])
    stop(process_a, control_a)
    stop(process_b, control_b)

    reader, control, _, _ = start_worker(ctx, extension, db_path)
    counts = ask(reader, control, "counts", ["from_a", "from_b"])
    stop(reader, control)
    assert counts == {"from_a": 1, "from_b": 1}, counts

    series_ids = direct_rows(
        db_path,
        "SELECT series_id, COUNT(*) FROM metrics_chunks GROUP BY series_id",
    )
    assert len(series_ids) == 2, series_ids


def test_same_identity_converges(ctx, extension, tmp):
    db_path = os.path.join(tmp, "same.db")
    initialize(extension, db_path)
    process_a, control_a, _, _ = start_worker(ctx, extension, db_path)
    process_b, control_b, _, _ = start_worker(ctx, extension, db_path)

    ask(
        process_a,
        control_a,
        "write",
        [
            ("prefix", 10, 1.0, "{}"),
            ("shared", 11, 2.0, '{"host":"a"}'),
        ],
    )
    ask(
        process_b,
        control_b,
        "write",
        [("shared", 12, 3.0, '{"host":"a"}')],
    )
    stop(process_a, control_a)
    stop(process_b, control_b)

    reader, control, _, _ = start_worker(ctx, extension, db_path)
    counts = ask(reader, control, "counts", ["prefix", "shared"])
    stop(reader, control)
    assert counts == {"prefix": 1, "shared": 2}, counts

    shared_ids = direct_rows(
        db_path,
        """
        SELECT DISTINCT c.series_id
        FROM metrics_chunks AS c
        JOIN metrics_series AS s ON s.id = c.series_id
        WHERE s.name = 'shared'
        """,
    )
    assert len(shared_ids) == 1, shared_ids


def test_long_lived_reader_refreshes(ctx, extension, tmp):
    db_path = os.path.join(tmp, "refresh.db")
    initialize(extension, db_path)
    reader, reader_control, _, _ = start_worker(ctx, extension, db_path)
    writer, writer_control, _, _ = start_worker(ctx, extension, db_path)

    assert ask(reader, reader_control, "counts", ["external"]) == {"external": 0}
    ask(writer, writer_control, "write", [("external", 30, 4.0, "{}")])
    assert ask(reader, reader_control, "counts", ["external"]) == {"external": 1}

    stop(reader, reader_control)
    stop(writer, writer_control)


def test_corrupt_legacy_registry_fails_closed(ctx, extension, tmp):
    db_path = os.path.join(tmp, "corrupt.db")
    initialize(extension, db_path)
    writer, control, _, _ = start_worker(ctx, extension, db_path)
    ask(writer, control, "write", [("existing", 40, 5.0, "{}")])
    stop(writer, control)

    db = sqlite3.connect(db_path, isolation_level=None)
    db.execute("DROP TABLE IF EXISTS metrics_series")
    db.execute(
        "UPDATE metrics_meta SET v = ? WHERE k = 'series_registry'",
        (b"\x00",),
    )
    db.close()

    process, control, status, payload = start_worker(
        ctx, extension, db_path, expect_ready=False
    )
    process.join(10)
    assert status == "error", (
        "existing chunks with no authoritative catalog and a corrupt legacy "
        f"registry reopened successfully: {status}, {payload}"
    )


def test_legacy_registry_migrates(ctx, extension, tmp):
    db_path = os.path.join(tmp, "legacy.db")
    initialize(extension, db_path)
    writer, control, _, _ = start_worker(ctx, extension, db_path)
    ask(
        writer,
        control,
        "write",
        [("legacy_metric", 50, 6.0, '{"host":"legacy"}')],
    )
    stop(writer, control)

    series_id = direct_rows(
        db_path, "SELECT DISTINCT series_id FROM metrics_chunks"
    )[0][0]
    blob = encode_legacy_registry(
        [(series_id, "legacy_metric", {"host": "legacy"})]
    )
    db = sqlite3.connect(db_path, isolation_level=None)
    db.execute("DROP TABLE IF EXISTS metrics_series")
    db.execute(
        "INSERT OR REPLACE INTO metrics_meta(k, v) VALUES('series_registry', ?)",
        (blob,),
    )
    db.close()

    reader, control, _, _ = start_worker(ctx, extension, db_path)
    assert ask(reader, control, "counts", ["legacy_metric"]) == {
        "legacy_metric": 1
    }
    stop(reader, control)
    rows = direct_rows(
        db_path,
        "SELECT id, name FROM metrics_series WHERE name = 'legacy_metric'",
    )
    assert rows == [(series_id, "legacy_metric")], rows


def test_catalog_rows_follow_transactions(_ctx, extension, tmp):
    db_path = os.path.join(tmp, "catalog_tx.db")
    initialize(extension, db_path)
    db = connect(extension, db_path)

    db.execute("BEGIN")
    db.execute(
        "INSERT INTO metrics(name, ts, value, labels) VALUES('rolled', 60, 7, '{}')"
    )
    db.execute("ROLLBACK")

    # Do not query between rollback and reuse: that would refresh and clear
    # caches, masking a stale ID retained by xRollback.
    db.execute(
        "INSERT INTO metrics(name, ts, value, labels) "
        "VALUES('replacement', 61, 8, '{}')"
    )
    db.execute(
        "INSERT INTO metrics(name, ts, value, labels) VALUES('rolled', 62, 9, '{}')"
    )
    db.execute("INSERT INTO metrics(metrics) VALUES('flush')")

    db.execute("BEGIN")
    db.execute("SAVEPOINT catalog_sp")
    db.execute(
        "INSERT INTO metrics(name, ts, value, labels) "
        "VALUES('savepoint_rolled', 63, 10, '{}')"
    )
    db.execute("ROLLBACK TO catalog_sp")
    db.execute("RELEASE catalog_sp")
    db.execute(
        "INSERT INTO metrics(name, ts, value, labels) "
        "VALUES('savepoint_rolled', 64, 11, '{}')"
    )
    db.execute("COMMIT")
    db.execute("INSERT INTO metrics(metrics) VALUES('flush')")

    counts = {
        name: db.execute(
            "SELECT COUNT(*) FROM metrics WHERE name = ?", (name,)
        ).fetchone()[0]
        for name in ("replacement", "rolled", "savepoint_rolled")
    }
    assert counts == {
        "replacement": 1,
        "rolled": 1,
        "savepoint_rolled": 1,
    }, counts
    ids = db.execute(
        "SELECT id FROM metrics_series "
        "WHERE name IN ('replacement', 'rolled', 'savepoint_rolled')"
    ).fetchall()
    assert len(set(ids)) == 3, ids
    db.close()


def main():
    extension, tmp = sys.argv[1:]
    ctx = multiprocessing.get_context("spawn")
    tests = [
        test_distinct_series_ids,
        test_same_identity_converges,
        test_long_lived_reader_refreshes,
        test_corrupt_legacy_registry_fails_closed,
        test_legacy_registry_migrates,
        test_catalog_rows_follow_transactions,
    ]
    for round_number in range(3):
        round_tmp = os.path.join(tmp, f"round-{round_number}")
        os.mkdir(round_tmp)
        for test in tests:
            test(ctx, extension, round_tmp)
            print(f"PASS round {round_number + 1}: {test.__name__}")


if __name__ == "__main__":
    main()
