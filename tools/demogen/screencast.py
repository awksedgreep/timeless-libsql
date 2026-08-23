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

# (command, seconds to let the result sit on screen)
SCRIPT = [
    (".load ./target/release/libtimeless_ext.so", 0.8),
    (".load ./tools/demogen/ext/target/release/libtimeless_demogen.so", 0.8),
    ("PRAGMA auto_vacuum=INCREMENTAL;", 0.6),
    ("PRAGMA journal_mode=WAL;", 0.8),
    (".timer on", 0.8),
    # list mode prints the multi-line seed/report text verbatim; box mode
    # would truncate it into one squashed cell.
    (".mode list --charlimit 0 --linelimit 0", 0.5),
    (f"SELECT timeless_demo('seed','{PROFILE}');", 6.0),
    (".mode box", 0.5),
    ("SELECT count(*) AS series FROM timeless_series('metrics');", 2.5),
    # The incident jumps out of a per-bucket error count.
    ("SELECT bucket_ts, n FROM timeless_log_buckets('logs','level',"
     "'{\"service\":\"auth\",\"level\":\"error\"}',"
     "(SELECT min(min_ts) FROM timeless_series('metrics')),"
     "(SELECT max(max_ts) FROM timeless_series('metrics')), 300000);", 4.0),
    ("SELECT ts, message FROM logs WHERE service='auth' AND level='error' "
     "ORDER BY ts DESC LIMIT 5;", 3.5),
    # Log -> trace pivot: error logs carry real trace ids.
    ("SELECT json_extract(metadata,'$.trace_id') AS trace_id FROM logs "
     "WHERE level='error' AND json_extract(metadata,'$.trace_id') IS NOT NULL "
     "LIMIT 3;", 3.5),
    (".mode list --charlimit 0 --linelimit 0", 0.5),
    ("SELECT timeless_demo('report');", 6.0),
]


def type_line(fd, line):
    for ch in line:
        os.write(fd, ch.encode())
        time.sleep(random.uniform(0.015, 0.045))
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
        for command, dwell in SCRIPT:
            time.sleep(0.6)
            type_line(fd, command)
            pump_until_prompt(fd)
            time.sleep(dwell)
        time.sleep(1.0)
        os.write(fd, b".quit\r")
        pump_until_prompt(fd)
    finally:
        os.waitpid(pid, 0)


if __name__ == "__main__":
    main()
