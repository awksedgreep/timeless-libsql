#!/usr/bin/env python3
"""Visual tour of the Timeless observability schema (#20): the friendly
views that install themselves alongside every signal table.

Like screencast.py, this drives a real sqlite3 session with simulated
typing so a recording is pristine and identical every take. Unlike the
compression tour, this one needs ONLY libtimeless_ext — no demogen
module — and hand-seeds a handful of rows so every output stays
readable on screen.

    python3 tools/demogen/schema_screencast.py tour.db     # rehearsal
    python3 tools/demogen/schema_screencast.py tour.db --cast schema.cast

`--cast` records an asciinema-v3 file directly — no asciinema install
needed, and the file plays on asciinema.org, in asciinema, or converts
to GIF with agg (cargo install --git https://github.com/asciinema/agg):

    agg --cols 120 --rows 32 schema.cast schema-demo.gif

Run from the repo root with the extension built (see QUICKSTART.md).
"""

import fcntl
import json
import os
import pty
import random
import select
import struct
import sys
import termios
import time

ARGS = [arg for arg in sys.argv[1:] if arg != "--cast"]
CAST_PATH = sys.argv[sys.argv.index("--cast") + 1] if "--cast" in sys.argv else None
DB = ARGS[0] if ARGS else "schema-tour.db"
PROMPT = b"sqlite> "

# (command, seconds to let the result sit on screen[, typing pace])
#
# Pace 0 pastes the line in one write instead of typing it — boilerplate
# and narration. Queries type at full speed; the dwell time after each is
# the reader's time. Every result below was verified against a real
# database; timestamps are fixed so takes are identical.
SCRIPT = [
    (".load ./target/release/libtimeless_ext", 0.25, 0),
    ("PRAGMA journal_mode=WAL;", 0.3, 0),
    (".mode box", 0.3, 0),
    (".print '-- one CREATE per signal. no extra commands, no installer to run'",
     0.4, 0),
    ("CREATE VIRTUAL TABLE metrics USING timeless_metrics;", 1.0),
    ("CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,host');",
     1.0),
    ("CREATE VIRTUAL TABLE traces USING timeless_traces;", 1.0),
    (".print '-- companions installed themselves, and the inventory "
     "describes every object'", 0.4, 0),
    ("SELECT object_name, object_kind, schema_version "
     "FROM timeless_schema_inventory ORDER BY object_name;", 4.0),
    # ── seed a small, readable incident ─────────────────────────────
    (".print '-- seed a tiny incident: a checkout request whose db call failed'",
     0.4, 0),
    ("INSERT INTO traces (trace_id, span_id, parent_span_id, name, service, "
     "kind, status, start_ts, duration_ns) VALUES "
     "('aaaa0000000000000000000000000000', '0100000000000000', NULL, "
     "'POST /checkout', 'checkout', 'server', 'ok', 1753000000123000000, 8500000);",
     1.4),
    ("INSERT INTO traces (trace_id, span_id, parent_span_id, name, service, "
     "kind, status, start_ts, duration_ns) VALUES "
     "('aaaa0000000000000000000000000000', '0200000000000000', "
     "'0100000000000000', 'SELECT items', 'db', 'client', 'error', "
     "1753000000200000000, 1200000);", 1.4),
    ("INSERT INTO logs(ts, level, message, metadata) VALUES "
     "(1753000000456, 'error', 'payment declined', "
     "'{\"service\":\"payments\",\"retryable\":false}');", 1.2),
    ("INSERT INTO metrics(name, ts, value, labels) VALUES "
     "('checkout_errors', 1753000000, 1.0, '{\"host\":\"web1\"}');", 1.2),
    ("INSERT INTO metrics(name, ts, value, labels) VALUES "
     "('checkout_latency', 1753000000, 0.85, '{\"host\":\"web1\"}');", 1.2),
    ("INSERT INTO metrics(metrics) VALUES ('flush');", 0.8),
    ("INSERT INTO logs(logs) VALUES ('flush');", 0.8),
    ("INSERT INTO traces(traces) VALUES ('flush');", 0.8),
    # ── traces ──────────────────────────────────────────────────────
    (".print '-- spans, with ids you can read and times you can read'",
     0.4, 0),
    ("SELECT trace_id, name, service, status, start_time, duration_ms "
     "FROM timeless_traces_spans;", 3.5),
    (".print '-- one row per trace. root state, service set, and "
     "completeness never overreach'", 0.4, 0),
    ("SELECT trace_id, span_rows, error_rows, root_state, root_name, "
     "services, completeness FROM timeless_traces_summary;", 4.0),
    (".print '-- the error span, straight from its own view'", 0.4, 0),
    ("SELECT name, service, start_time FROM timeless_traces_errors;", 3.0),
    (".print '-- catalogs, for building dropdowns and service maps'",
     0.4, 0),
    ("SELECT service, operation FROM timeless_traces_operations;", 3.0),
    # ── logs ────────────────────────────────────────────────────────
    (".print '-- log entries: receipt-style UTC, message, metadata kept as "
     "typed JSON'", 0.4, 0),
    ("SELECT ts_time, level, message, json_extract(metadata, '$.retryable') "
     "AS retryable FROM timeless_logs_entries;", 3.5),
    (".print '-- what you can filter on fast (declared index keys)'", 0.4, 0),
    ("SELECT field FROM timeless_logs_fields;", 2.5),
    ("SELECT service FROM timeless_logs_services;", 2.5),
    # ── metrics ─────────────────────────────────────────────────────
    (".print '-- newest value per series, in plain words and plain time'",
     0.4, 0),
    ("SELECT name, labels, value, ts_time FROM timeless_metrics_latest;", 3.5),
    (".print '-- and the catalog: what exists, how much of it'", 0.4, 0),
    ("SELECT name, points FROM timeless_metrics_series;", 3.0),
    # ── lifecycle ───────────────────────────────────────────────────
    (".mode list --charlimit 0 --linelimit 0", 0.2, 0),
    (".print '-- dropping a table uninstalls exactly its own objects -- "
     "nothing else is touched'", 0.4, 0),
    ("DROP TABLE traces;", 1.0),
    ("SELECT object_name FROM timeless_schema_inventory "
     "WHERE source_table = 'traces';", 2.5),
    (".print '-- (no rows: the traces companions left with their table)'",
     0.6, 0),
    ("SELECT object_name FROM timeless_schema_inventory "
     "WHERE source_table = 'logs';", 2.5),
    (".print '-- logs companions untouched. that is the whole contract.'",
     0.6, 0),
]


def type_line(fd, line, pace=1.0):
    if pace <= 0:
        os.write(fd, line.encode())
        time.sleep(0.08)
    else:
        for ch in line:
            os.write(fd, ch.encode())
            time.sleep(random.uniform(0.015, 0.045) * pace)
        time.sleep(0.25)
    os.write(fd, b"\r")


# ── built-in asciinema-v3 recorder ────────────────────────────────
# The pty echoes everything we type, so capturing the output stream
# captures the whole session. Each frame gets its own relative
# timestamp, which preserves the pacing exactly as it played.
#
# v3 shape (what agg and asciinema 3.x consume): header with a `term`
# object, and events as {"time", "code", "data"} objects.

CAST_START = None
CAST_FILE = None


def emit(frame: bytes):
    """Write one output frame to stdout and, when recording, to the cast."""
    global CAST_START
    os.write(1, frame)
    if CAST_FILE is not None:
        elapsed = time.monotonic() - CAST_START
        event = {
            "time": round(elapsed, 6),
            "code": "o",
            "data": frame.decode("utf-8", "replace"),
        }
        CAST_FILE.write(json.dumps(event, separators=(",", ":")) + "\n")


def pump_until_prompt(fd):
    """Stream sqlite3 output to our stdout until the prompt returns."""
    tail = b""
    while True:
        r, _, _ = select.select([fd], [], [], 300)
        if not r:
            raise TimeoutError("sqlite3 produced no output for 5 minutes")
        try:
            chunk = os.read(fd, 4096)
        except OSError:
            return False
        if not chunk:
            return False
        emit(chunk)
        tail = (tail + chunk)[-64:]
        if tail.endswith(PROMPT):
            return True


def main():
    global CAST_START, CAST_FILE
    # Pristine takes: a leftover database would fail the CREATEs.
    for suffix in ["", "-wal", "-shm"]:
        try:
            os.remove(DB + suffix)
        except FileNotFoundError:
            pass
    pid, fd = pty.fork()
    if pid == 0:
        os.execvp("sqlite3", ["sqlite3", DB])
    # A pty starts with no size; sqlite3 clips output to its guess. 120x32
    # is also the geometry the recording should use.
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 120, 0, 0))
    if CAST_PATH is not None:
        CAST_START = time.monotonic()
        CAST_FILE = open(CAST_PATH, "w")
        CAST_FILE.write(
            json.dumps(
                {
                    "version": 3,
                    "term": {"cols": 120, "rows": 32},
                    "env": {"TERM": os.environ.get("TERM", "xterm-256color")},
                }
            )
            + "\n"
        )
    try:
        pump_until_prompt(fd)
        for command, dwell, *rest in SCRIPT:
            pace = rest[0] if rest else 1.0
            time.sleep(0.6 if pace else 0.15)
            type_line(fd, command, pace)
            pump_until_prompt(fd)
            time.sleep(dwell)
        time.sleep(1.0)
        os.write(fd, b".quit\r")
        pump_until_prompt(fd)
    finally:
        os.waitpid(pid, 0)
        if CAST_FILE is not None:
            CAST_FILE.close()
            print(f"\ncast written: {CAST_PATH}")


if __name__ == "__main__":
    main()
