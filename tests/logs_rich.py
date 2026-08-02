#!/usr/bin/env python3
"""Direct-SQL compatibility oracle for release-grade rich logs."""

import json
import sqlite3
import struct
import sys


extension, database = sys.argv[1:]


def connect():
    db = sqlite3.connect(database, isolation_level=None)
    db.enable_load_extension(True)
    db.load_extension(extension)
    return db


def framed(text):
    encoded = text.encode("utf-8")
    return struct.pack("<I", len(encoded)) + encoded


def rich_batch(entries):
    blob = bytearray(struct.pack("<BBHI", 0x02, 0, 0, len(entries)))
    for entry in entries:
        blob.extend(struct.pack("<q", entry[0]))
    for entry in entries:
        blob.extend(framed(entry[1]))
    for entry in entries:
        blob.extend(framed(entry[2]))
    for entry in entries:
        blob.extend(framed(json.dumps(entry[3], sort_keys=True, separators=(",", ":"))))
    return bytes(blob)


def flat_batch(entries):
    blob = bytearray(struct.pack("<BBHI", 0x01, 0, 0, len(entries)))
    for entry in entries:
        blob.extend(struct.pack("<q", entry[0]))
    blob.extend(entry[1] for entry in entries)
    for entry in entries:
        blob.extend(framed(entry[2]))
    for entry in entries:
        blob.extend(framed(json.dumps(entry[3], sort_keys=True, separators=(",", ":"))))
    return bytes(blob)


db = connect()
capabilities = json.loads(db.execute("SELECT timeless_capabilities()").fetchone()[0])
assert capabilities["data_abi"] == 1
assert "rich-v1" in capabilities["signals"]["logs"]["batches"]
assert capabilities["signals"]["logs"]["authoritative_batch_entries"] == 8192

try:
    db.execute("CREATE VIRTUAL TABLE bad USING timeless_logs(timestamp_unit='seconds')")
except sqlite3.DatabaseError as error:
    assert "expected 'ms' or 'us'" in str(error)
else:
    raise AssertionError("unknown logs timestamp unit was accepted")

db.execute(
    "CREATE VIRTUAL TABLE logs USING timeless_logs("
    "index_keys='service', timestamp_unit='us')"
)
assert db.execute(
    "SELECT CAST(v AS TEXT) FROM logs_meta WHERE k='timestamp_unit'"
).fetchone()[0] == "us"

timestamp = 1_700_000_000_123_456
typed = {
    "array": [1, True, None, {"nested": 2.5}],
    "bool": False,
    "count": 9007199254740991,
    "null": None,
    "service": "api",
}
entries = [
    (timestamp, "notice", "b-message", typed),
    (timestamp, "critical", "a-message", {"service": "api", "code": 503}),
    (timestamp + 1, "emergency", "c-message", {"service": "ops", "fatal": True}),
]
db.execute("INSERT INTO logs(logs) VALUES (?)", (rich_batch(entries),))
assert db.execute("SELECT COUNT(*) FROM logs").fetchone()[0] == 3

# The original v0 public batch remains readable and retains its documented
# four-level/flat-string behavior in a microsecond-capable table.
legacy = [(timestamp + 2, 1, "legacy-info", {"service": "legacy", "status": "ok"})]
db.execute("INSERT INTO logs(logs) VALUES (?)", (flat_batch(legacy),))
assert db.execute("SELECT COUNT(*) FROM logs").fetchone()[0] == 4

db.execute("INSERT INTO logs(logs) VALUES ('flush')")
db.execute("INSERT INTO logs(logs) VALUES ('optimize')")
db.close()

# Reopen cold so the result validates persisted codecs and recovery, not a
# process-local buffer.
db = connect()
rows = db.execute(
    "SELECT ts,level,message,metadata FROM logs ORDER BY ts ASC LIMIT 10"
).fetchall()
assert [row[2] for row in rows] == [
    "a-message",
    "b-message",
    "c-message",
    "legacy-info",
]
assert rows[0][1] == "critical"
assert rows[1][1] == "notice"
assert rows[2][1] == "emergency"
assert rows[3][1] == "info"
assert rows[1][0] == timestamp
assert json.loads(rows[1][3]) == typed
assert rows[1][3] == json.dumps(typed, sort_keys=True, separators=(",", ":"))

assert db.execute(
    "SELECT COUNT(*) FROM logs WHERE level='notice'"
).fetchone()[0] == 1
assert db.execute(
    "SELECT COUNT(*) FROM logs WHERE level='critical'"
).fetchone()[0] == 1
assert db.execute(
    "SELECT COUNT(*) FROM logs WHERE level='error'"
).fetchone()[0] == 0

groups = db.execute(
    "SELECT bucket_ts,group_key,n FROM timeless_log_buckets("
    "'logs','level',NULL,?, ?, 10)",
    (timestamp, timestamp + 9),
).fetchall()
assert {(group, count) for _, group, count in groups} == {
    ("notice", 1),
    ("critical", 1),
    ("emergency", 1),
    ("info", 1),
}

# At equal timestamps both ASC and DESC retain the product's canonical payload
# tie-breaker; only timestamp direction reverses.
asc = db.execute("SELECT message FROM logs ORDER BY ts ASC LIMIT 2").fetchall()
assert asc == [("a-message",), ("b-message",)]
db.close()
