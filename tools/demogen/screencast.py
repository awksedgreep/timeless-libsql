#!/usr/bin/env python3
"""Drive the demogen tour through a real sqlite3 session with simulated
typing, so a recording is pristine and identical every take.

    python3 tools/demogen/screencast.py demo.db small     # rehearsal
    asciinema rec -c 'python3 tools/demogen/screencast.py demo.db large' demo.cast

Run from the repo root with both artifacts built (see QUICKSTART.md).
Everything is executed for real — the pauses exist so a viewer can read
each result before the next command starts typing.
"""

import fcntl
import os
import pty
import random
import select
import struct
import sys
import termios
import time

DB = sys.argv[1] if len(sys.argv) > 1 else "demo.db"
PROFILE = sys.argv[2] if len(sys.argv) > 2 else "medium"
PROMPT = b"sqlite> "

# (command, seconds to let the result sit on screen[, typing pace])
#
# Pace 0 pastes the line in one write instead of typing it. The setup lines
# are load/pragma boilerplate nobody needs to watch being typed, and at full
# pace they burned ~15s before the seed even started — long enough to lose a
# viewer before the demo begins. Everything from the seed on types normally.
SCRIPT = [
    (".load ./target/release/libtimeless_ext", 0.25, 0),
    (".load ./tools/demogen/ext/target/release/libtimeless_demogen", 0.25, 0),
    ("PRAGMA auto_vacuum=INCREMENTAL;", 0.2, 0),
    ("PRAGMA journal_mode=WAL;", 0.3, 0),
    (".timer on", 0.2, 0),
    # list mode prints the multi-line seed/report text verbatim; box mode
    # would truncate it into one squashed cell.
    (".mode list --charlimit 0 --linelimit 0", 0.2, 0),
    # Narration. `.print` writes raw text in any output mode; `SELECT 'x';`
    # would box the string and repeat it as a column header. Each caption
    # pastes instantly and the query below it types at full speed, so the
    # typing time doubles as reading time.
    #
    # The three CREATE VIRTUAL TABLE statements are the point of the whole
    # tour: they are the extension's actual public surface, and without them
    # a viewer only sees a function dumping rows into a file. Declaring them
    # here also means the generator fills OUR tables -- our names, our
    # index_keys, our attribute_indexes -- rather than creating its own.
    (".print '-- three virtual tables. this is the entire API surface of the "
     "extension'", 0.4, 0),
    ("CREATE VIRTUAL TABLE metrics USING timeless_metrics(retention='14d');",
     1.2),
    ("CREATE VIRTUAL TABLE logs USING timeless_logs("
     "index_keys='service,path,status');", 1.2),
    ("CREATE VIRTUAL TABLE spans USING timeless_traces("
     "attribute_indexes='[{\"scope\":\"span\",\"path\":\"/http.method\"}]');",
     1.6),
    (".print '-- the generator fills the tables we just declared -- our "
     "names, our indexes'", 0.4, 0),
    (".print '-- seed a synthetic fleet: services x pods, correlated "
     "metrics + logs + traces, one incident'", 0.4, 0),
    (f"SELECT timeless_demo('seed','{PROFILE}');", 6.0),
    (".mode box", 0.5),
    (".print '-- each (name, labels) pair is its own series: the "
     "cardinality the engine has to index'", 0.4, 0),
    ("SELECT count(*) AS series FROM timeless_series('metrics');", 2.5),
    # The incident jumps out of a per-bucket error count. Log timestamps are
    # in the table's persisted unit (ms by default), so /1000 before
    # 'unixepoch'; time-of-day alone keeps the ramp scannable.
    (".print '-- bucket auth errors into 5-minute counts -- nobody told it "
     "when the incident was'", 0.4, 0),
    ("SELECT strftime('%H:%M', bucket_ts/1000, 'unixepoch') AS bucket, n "
     "FROM timeless_log_buckets('logs','level',"
     "'{\"service\":\"auth\",\"level\":\"error\"}',"
     "(SELECT min(min_ts) FROM timeless_series('metrics')),"
     "(SELECT max(max_ts) FROM timeless_series('metrics')), 300000);", 4.0),
    (".print '-- indexed metadata columns read like an ordinary table'",
     0.4, 0),
    ("SELECT datetime(ts/1000,'unixepoch') AS time, message FROM logs "
     "WHERE service='auth' AND level='error' ORDER BY ts DESC LIMIT 5;", 3.5),
    # Log -> trace pivot: error logs carry real trace ids.
    (".print '-- error logs carry the trace_id of the failed request, so "
     "logs and traces correlate'", 0.4, 0),
    ("SELECT json_extract(metadata,'$.trace_id') AS trace_id FROM logs "
     "WHERE level='error' AND json_extract(metadata,'$.trace_id') IS NOT NULL "
     "LIMIT 3;", 3.5),
    # Payoff for declaring the tables ourselves: the attribute index above
    # is one WE asked for, answering a query over generated data.
    (".print '-- and the span attribute index we declared up front answers "
     "this one'", 0.4, 0),
    ("SELECT service, count(*) AS spans FROM spans WHERE attribute_filter="
     "'{\"scope\":\"span\",\"path\":\"/http.method\",\"value\":\"GET\"}' "
     "GROUP BY service ORDER BY spans DESC LIMIT 4;", 3.5),
    (".mode list --charlimit 0 --linelimit 0", 0.2, 0),
    (".print '-- what it cost on disk: raw logical bytes vs engine block "
     "bytes, indexes counted apart'", 0.4, 0),
    ("SELECT timeless_demo('report');", 6.0),
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


def pump_until_prompt(fd):
    """Stream sqlite3 output to our stdout until the prompt returns."""
    tail = b""
    while True:
        r, _, _ = select.select([fd], [], [], 600)
        if not r:
            raise TimeoutError("sqlite3 produced no output for 10 minutes")
        try:
            chunk = os.read(fd, 4096)
        except OSError:
            return False
        if not chunk:
            return False
        os.write(1, chunk)
        tail = (tail + chunk)[-64:]
        if tail.endswith(PROMPT):
            return True


def main():
    pid, fd = pty.fork()
    if pid == 0:
        os.execvp("sqlite3", ["sqlite3", DB])
    # A pty starts with no size; sqlite3 clips output to its guess. 120x32
    # is also the geometry the recording should use.
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 120, 0, 0))
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


if __name__ == "__main__":
    main()
