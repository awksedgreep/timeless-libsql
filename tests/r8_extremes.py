#!/usr/bin/env python3
"""R8 parity tests for i64 timestamp extremes and NULL constraints."""

import sqlite3
import sys
import unittest
from pathlib import Path


EXTENSION = sys.argv[1]
TEMP_DIR = Path(sys.argv[2])
I64_MIN = -(1 << 63)
I64_MAX = (1 << 63) - 1

SIGNALS = {
    "metrics": {
        "time": "ts",
        "equality": "name",
        "select": "name, ts, value",
    },
    "logs": {
        "time": "ts",
        "equality": "level",
        "select": "ts, level, message",
    },
    "traces": {
        "time": "start_ts",
        "equality": "name",
        "select": "trace_id, span_id, name, service, start_ts",
    },
}


class TimestampParity(unittest.TestCase):
    def setUp(self):
        self.db_path = TEMP_DIR / f"{self._testMethodName}.db"
        self.db = self.connect()
        self.db.executescript(
            """
            CREATE VIRTUAL TABLE metrics USING timeless_metrics;
            CREATE TABLE plain_metrics(name TEXT, ts INTEGER, value REAL);

            CREATE VIRTUAL TABLE logs USING timeless_logs;
            CREATE TABLE plain_logs(ts INTEGER, level TEXT, message TEXT);

            CREATE VIRTUAL TABLE traces USING timeless_traces;
            CREATE TABLE plain_traces(
              trace_id BLOB,
              span_id BLOB,
              name TEXT,
              service TEXT,
              start_ts INTEGER
            );
            """
        )
        metric_rows = [
            ("edge", I64_MIN, -1.0),
            ("edge", I64_MAX, 1.0),
        ]
        self.db.executemany(
            "INSERT INTO metrics(name, ts, value) VALUES (?, ?, ?)", metric_rows
        )
        self.db.executemany(
            "INSERT INTO plain_metrics(name, ts, value) VALUES (?, ?, ?)",
            metric_rows,
        )

        log_rows = [
            (I64_MIN, "info", "min"),
            (I64_MAX, "error", "max"),
        ]
        self.db.executemany(
            "INSERT INTO logs(ts, level, message) VALUES (?, ?, ?)", log_rows
        )
        self.db.executemany(
            "INSERT INTO plain_logs(ts, level, message) VALUES (?, ?, ?)",
            log_rows,
        )

        trace_rows = [
            (bytes([1]) * 16, bytes([1]) * 8, "min", "svc", I64_MIN),
            (bytes([2]) * 16, bytes([2]) * 8, "max", "svc", I64_MAX),
        ]
        self.db.executemany(
            """
            INSERT INTO traces(trace_id, span_id, name, service, start_ts)
            VALUES (?, ?, ?, ?, ?)
            """,
            trace_rows,
        )
        self.db.executemany(
            """
            INSERT INTO plain_traces(trace_id, span_id, name, service, start_ts)
            VALUES (?, ?, ?, ?, ?)
            """,
            trace_rows,
        )

    def tearDown(self):
        self.db.close()

    def connect(self):
        db = sqlite3.connect(self.db_path, isolation_level=None)
        db.enable_load_extension(True)
        db.load_extension(EXTENSION)
        return db

    def reopen(self):
        self.db.close()
        self.db = self.connect()

    def flush(self):
        for signal in SIGNALS:
            self.db.execute(f"INSERT INTO {signal}({signal}) VALUES ('flush')")

    def assert_parity(self, signal, where="", params=()):
        spec = SIGNALS[signal]
        suffix = f" WHERE {where}" if where else ""
        order = f" ORDER BY {spec['time']}"
        actual = self.db.execute(
            f"SELECT {spec['select']} FROM {signal}{suffix}{order}", params
        ).fetchall()
        expected = self.db.execute(
            f"SELECT {spec['select']} FROM plain_{signal}{suffix}{order}", params
        ).fetchall()
        self.assertEqual(actual, expected)

    def assert_all_signals(self, stage, where_for):
        for signal in SIGNALS:
            where, params = where_for(signal)
            with self.subTest(stage=stage, signal=signal, where=where):
                self.assert_parity(signal, where, params)

    def test_unconstrained_scan_includes_both_i64_extremes(self):
        where_for = lambda _signal: ("", ())
        self.assert_all_signals("buffered", where_for)
        self.flush()
        self.assert_all_signals("flushed", where_for)
        self.reopen()
        self.assert_all_signals("reopened", where_for)

    def test_explicit_inclusive_extreme_bounds(self):
        def where_for(signal):
            time_col = SIGNALS[signal]["time"]
            return (f"{time_col} >= ? AND {time_col} <= ?", (I64_MIN, I64_MAX))

        self.assert_all_signals("buffered", where_for)
        self.flush()
        self.assert_all_signals("flushed", where_for)
        self.reopen()
        self.assert_all_signals("reopened", where_for)

    def test_null_predicates_return_no_rows_without_errors(self):
        for stage in ["buffered", "flushed", "reopened"]:
            for signal, spec in SIGNALS.items():
                predicates = [
                    (f"{spec['time']} >= ?", (None,)),
                    (f"{spec['time']} <= ?", (None,)),
                    (f"{spec['equality']} = ?", (None,)),
                ]
                for where, params in predicates:
                    with self.subTest(stage=stage, signal=signal, where=where):
                        self.assert_parity(signal, where, params)
            if stage == "buffered":
                self.flush()
            elif stage == "flushed":
                self.reopen()

    def test_strict_edge_bounds_match_sqlite_rechecking(self):
        for stage in ["buffered", "flushed", "reopened"]:
            for signal, spec in SIGNALS.items():
                predicates = [
                    (f"{spec['time']} > ?", (I64_MIN,)),
                    (f"{spec['time']} < ?", (I64_MAX,)),
                    (f"{spec['time']} > ?", (I64_MAX,)),
                    (f"{spec['time']} < ?", (I64_MIN,)),
                ]
                for where, params in predicates:
                    with self.subTest(stage=stage, signal=signal, where=where):
                        self.assert_parity(signal, where, params)
            if stage == "buffered":
                self.flush()
            elif stage == "flushed":
                self.reopen()


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TimestampParity)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
