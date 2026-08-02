#!/usr/bin/env python3
"""Session 1 rich-span fidelity and lifecycle regression.

Runs exclusively through the public timeless_traces SQLite extension. It
pins row/batch parity, the authoritative 8,192-span threshold, transaction
semantics, every maintenance operation, cold reopen, and corrupt input.
"""

import json
import os
import sqlite3
import struct
import sys


extension, db_path = sys.argv[1:]


def connect():
    db = sqlite3.connect(db_path, isolation_level=None)
    db.enable_load_extension(True)
    db.load_extension(extension)
    return db


def text_column(values):
    out = bytearray()
    for value in values:
        raw = value.encode()
        out += struct.pack("<I", len(raw)) + raw
    return out


def batch(spans, version=2):
    out = bytearray(struct.pack("<BBHI", version, 0, 0, len(spans)))
    out += b"".join(span["trace_id"] for span in spans)
    out += b"".join(span["span_id"] for span in spans)
    out += b"".join(span.get("parent_span_id") or bytes(8) for span in spans)
    out += text_column([span["name"] for span in spans])
    out += text_column([span["service"] for span in spans])
    out += bytes(span["kind"] for span in spans)
    out += bytes(span["status"] for span in spans)
    out += b"".join(struct.pack("<q", span["start_ts"]) for span in spans)
    out += b"".join(struct.pack("<q", span["duration_ns"]) for span in spans)
    out += text_column([json.dumps(span["attributes"], sort_keys=True, separators=(",", ":")) for span in spans])
    if version == 2:
        out += text_column([span["status_description"] for span in spans])
        out += text_column([json.dumps(span["events"], sort_keys=True, separators=(",", ":")) for span in spans])
        out += text_column([json.dumps(span["resource"], sort_keys=True, separators=(",", ":")) for span in spans])
        out += text_column([json.dumps(span["instrumentation_scope"], sort_keys=True, separators=(",", ":")) for span in spans])
    return bytes(out)


def rich_span(n, start_ts=None):
    return {
        "trace_id": n.to_bytes(16, "big"),
        "span_id": n.to_bytes(8, "big"),
        "parent_span_id": None if n % 2 else max(1, n - 1).to_bytes(8, "big"),
        "name": "GET /rich" if n % 2 else "db.query",
        # Deliberately conflicts: attributes must win service derivation.
        "service": "explicit-must-not-win",
        "kind": n % 5,
        "status": n % 3,
        "status_description": "boom 🚀" if n % 3 == 2 else "",
        "start_ts": start_ts if start_ts is not None else 1_700_000_000_000_000_000 + n,
        "duration_ns": n * 101,
        "attributes": {
            "array": [1, "two", False, None],
            "bool": True,
            "count": n,
            "nested": {"ratio": 1.25, "unicode": "空🔥"},
            "service.name": "rich-service",
        },
        "events": [
            {
                "attributes": {"attempt": n, "fatal": False},
                "name": "exception",
                "timestamp": 1_700_000_000_000_000_000 + n + 1,
            }
        ],
        "resource": {
            "deployment.environment": "test",
            "service.name": "resource-must-not-win",
        },
        "instrumentation_scope": {
            "attributes": {"debug": False},
            "name": "rich-lib",
            "version": "4.5.6",
        },
    }


def session0_contract_fixture():
    """Exact normalized values from rich_trace.otlp.json in timeless_traces."""
    resource = {
        "debug": False,
        "replica": 7,
        "service.name": "contract-svc",
        "service.version": "1.2.3",
    }
    scope = {"name": "contract-lib", "version": "4.5.6"}
    return [
        {
            "trace_id": bytes.fromhex("00112233445566778899aabbccddeeff"),
            "span_id": bytes.fromhex("0102030405060708"),
            "parent_span_id": None,
            "name": "GET /contract",
            "service": "explicit-must-not-win",
            "kind": 1,
            "status": 2,
            "status_description": "contract failure",
            "start_ts": 1_700_000_000_000_000_000,
            "duration_ns": 120_000_000,
            "attributes": {
                "http.method": "GET",
                "http.status_code": 503,
                "retryable": True,
                "score": 0.75,
            },
            "events": [
                {
                    "attributes": {
                        "exception.type": "ContractError",
                        "handled": False,
                    },
                    "name": "exception",
                    "timestamp": 1_700_000_000_040_000_000,
                }
            ],
            "resource": resource,
            "instrumentation_scope": scope,
        },
        {
            "trace_id": bytes.fromhex("00112233445566778899aabbccddeeff"),
            "span_id": bytes.fromhex("1112131415161718"),
            "parent_span_id": bytes.fromhex("0102030405060708"),
            "name": "DB contract",
            "service": "explicit-must-not-win",
            "kind": 2,
            "status": 0,
            "status_description": "",
            "start_ts": 1_700_000_000_020_000_000,
            "duration_ns": 60_000_000,
            "attributes": {"db.system": "libsql", "rows": 3},
            "events": [],
            "resource": resource,
            "instrumentation_scope": scope,
        },
    ]


COLS = (
    "trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,duration_ns,"
    "attributes,status_description,events,resource,instrumentation_scope"
)
INSERT = f"INSERT INTO {{table}}({COLS}) VALUES ({','.join('?' for _ in range(14))})"
SELECT = f"SELECT {COLS} FROM {{table}} ORDER BY start_ts,span_id"


def row_values(span):
    kinds = ["internal", "server", "client", "producer", "consumer"]
    statuses = ["unset", "ok", "error"]
    return (
        span["trace_id"], span["span_id"], span["parent_span_id"], span["name"],
        span["service"], kinds[span["kind"]], statuses[span["status"]],
        span["start_ts"], span["duration_ns"],
        json.dumps(span["attributes"]), span["status_description"],
        json.dumps(span["events"]), json.dumps(span["resource"]),
        json.dumps(span["instrumentation_scope"]),
    )


def semantic_rows(db, table):
    rows = []
    for row in db.execute(SELECT.format(table=table)):
        row = list(row)
        for index in (9, 11, 12, 13):
            row[index] = json.loads(row[index])
        rows.append(tuple(row))
    return rows


def jaeger_type(value):
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "float64"
    return "string"


db = connect()
db.executescript(
    """
    CREATE VIRTUAL TABLE row_spans USING timeless_traces;
    CREATE VIRTUAL TABLE batch_spans USING timeless_traces;
    CREATE VIRTUAL TABLE threshold_spans USING timeless_traces;
    CREATE VIRTUAL TABLE txn_spans USING timeless_traces;
    CREATE VIRTUAL TABLE lifecycle_spans USING timeless_traces;
    CREATE VIRTUAL TABLE corrupt_spans USING timeless_traces;
    CREATE VIRTUAL TABLE v0_spans USING timeless_traces;
    """
)

# Row versus rich batch v1, including service precedence and typed JSON.
fixture = session0_contract_fixture()
for span in fixture:
    db.execute(INSERT.format(table="row_spans"), row_values(span))
db.execute("INSERT INTO batch_spans(batch_spans) VALUES (?)", (batch(fixture),))
assert semantic_rows(db, "row_spans") == semantic_rows(db, "batch_spans")
assert {row[4] for row in semantic_rows(db, "batch_spans")} == {"contract-svc"}
assert db.execute(
    "SELECT COUNT(*) FROM batch_spans WHERE service='contract-svc'"
).fetchone()[0] == len(fixture)
assert db.execute(
    "SELECT COUNT(*) FROM batch_spans WHERE service='explicit-must-not-win'"
).fetchone()[0] == 0
root = semantic_rows(db, "batch_spans")[0]
tags = {key: (jaeger_type(value), value) for key, value in root[9].items()}
tags["otel.status_description"] = ("string", root[10])
assert tags["http.status_code"] == ("int64", 503)
assert tags["retryable"] == ("bool", True)
assert tags["score"] == ("float64", 0.75)
assert tags["otel.status_description"] == ("string", "contract failure")
event = root[11][0]
log_fields = {
    "event": ("string", event["name"]),
    **{
        key: (jaeger_type(value), value)
        for key, value in event["attributes"].items()
    },
}
assert event["timestamp"] // 1000 == 1_700_000_000_040_000
assert log_fields["handled"] == ("bool", False)
assert log_fields["exception.type"] == ("string", "ContractError")
try:
    bad = rich_span(4)
    values = list(row_values(bad))
    values[9] = "[]"  # attributes must be an object, even though this is valid JSON.
    db.execute(INSERT.format(table="row_spans"), values)
    raise AssertionError("wrong trace JSON shape unexpectedly succeeded")
except sqlite3.DatabaseError:
    pass
assert len(semantic_rows(db, "row_spans")) == len(fixture)

# v0 remains accepted and receives documented rich defaults.
legacy = rich_span(90)
legacy["attributes"] = {"legacy": "string-only"}
db.execute("INSERT INTO v0_spans(v0_spans) VALUES (?)", (batch([legacy], version=1),))
v0 = db.execute(
    "SELECT attributes,status_description,events,resource,instrumentation_scope FROM v0_spans"
).fetchone()
assert json.loads(v0[0]) == {"legacy": "string-only"}
assert v0[1:] == ("", "[]", "{}", "{}")

# Exactly 8,191 spans remain buffered; the next admitted span auto-flushes.
threshold = [rich_span(10_000 + n) for n in range(8191)]
db.execute("INSERT INTO threshold_spans(threshold_spans) VALUES (?)", (batch(threshold),))
assert db.execute("SELECT COUNT(*) FROM threshold_spans_blocks").fetchone()[0] == 0
last = rich_span(20_000)
db.execute(INSERT.format(table="threshold_spans"), row_values(last))
assert db.execute("SELECT COUNT(*) FROM threshold_spans_blocks").fetchone()[0] == 3
assert db.execute("SELECT COUNT(*) FROM threshold_spans").fetchone()[0] == 8192

# Failed rich batch and savepoint rollback are all-or-nothing.
before = db.execute("SELECT COUNT(*) FROM txn_spans").fetchone()[0]
try:
    db.execute("INSERT INTO txn_spans(txn_spans) VALUES (?)", (batch(fixture)[:-1],))
    raise AssertionError("truncated v1 batch unexpectedly succeeded")
except sqlite3.DatabaseError:
    pass
assert db.execute("SELECT COUNT(*) FROM txn_spans").fetchone()[0] == before
db.execute("BEGIN")
db.execute(INSERT.format(table="txn_spans"), row_values(rich_span(30_001)))
db.execute("SAVEPOINT rich")
db.execute("INSERT INTO txn_spans(txn_spans) VALUES (?)", (batch([rich_span(30_002)]),))
db.execute("ROLLBACK TO rich")
db.execute("RELEASE rich")
db.execute("COMMIT")
assert [row[1] for row in semantic_rows(db, "txn_spans")] == [(30_001).to_bytes(8, "big")]

# Flush, optimize, prune, and cold reopen retain every rich value.
old = rich_span(40_001, start_ts=100)
keep = rich_span(40_002, start_ts=200)
db.execute("INSERT INTO lifecycle_spans(lifecycle_spans) VALUES (?)", (batch([old, keep]),))
db.execute("INSERT INTO lifecycle_spans(lifecycle_spans) VALUES ('flush')")
db.execute("INSERT INTO lifecycle_spans(lifecycle_spans) VALUES ('optimize')")
assert len(semantic_rows(db, "lifecycle_spans")) == 2
db.execute("INSERT INTO lifecycle_spans(lifecycle_spans) VALUES ('prune:150')")
assert semantic_rows(db, "lifecycle_spans")[0][1] == keep["span_id"]

# A corrupt persisted payload errors rather than panicking or fabricating rows.
victim = rich_span(50_001)
db.execute(INSERT.format(table="corrupt_spans"), row_values(victim))
db.execute("INSERT INTO corrupt_spans(corrupt_spans) VALUES ('flush')")
block_id, payload = db.execute("SELECT id,data FROM corrupt_spans_blocks LIMIT 1").fetchone()
db.execute("UPDATE corrupt_spans_blocks SET data=? WHERE id=?", (payload[:-1], block_id))
try:
    db.execute("SELECT COUNT(*) FROM corrupt_spans").fetchone()
    raise AssertionError("corrupt generation-2 block unexpectedly decoded")
except sqlite3.DatabaseError:
    pass
db.execute("UPDATE corrupt_spans_blocks SET data=? WHERE id=?", (payload, block_id))

# Make both public ingest paths durable and optimized before the cold
# reopen assertion; this is the Session 1 exit fixture.
for table in ("row_spans", "batch_spans"):
    db.execute(f"INSERT INTO {table}({table}) VALUES ('flush')")
    db.execute(f"INSERT INTO {table}({table}) VALUES ('optimize')")
expected_row = semantic_rows(db, "row_spans")
expected_batch = semantic_rows(db, "batch_spans")
expected_lifecycle = semantic_rows(db, "lifecycle_spans")
db.close()

db = connect()
assert semantic_rows(db, "row_spans") == expected_row
assert semantic_rows(db, "batch_spans") == expected_batch
assert semantic_rows(db, "lifecycle_spans") == expected_lifecycle
assert semantic_rows(db, "corrupt_spans")[0][9] == victim["attributes"]
assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
db.close()

print("PASS: rich row/batch fidelity, threshold, transactions, maintenance, corruption, reopen")
