#!/usr/bin/env python3
"""Deterministic repeated-maintenance benchmark for Session 6.

Run the same script against the parent and candidate extensions. Each 2,048
entry arrival is explicitly flushed and optimized, intentionally exercising
the small-tail case more aggressively than the API's normal 30-second timer.
The script prints one JSON record so results can be compared mechanically.
"""

import json
import math
import os
import sqlite3
import struct
import sys
import time


CYCLES = 128
ENTRIES_PER_CYCLE = 2_048
SERVICES = ("api", "web", "worker", "billing", "search")
PATHS = ("/v1/orders", "/v1/users", "/health", "/checkout")


def level(number):
    bucket = number % 20
    if bucket == 0:
        return 3, "error"
    if bucket <= 2:
        return 2, "warning"
    if bucket <= 5:
        return 0, "debug"
    return 1, "info"


def encode_batch(start, count):
    timestamps = [1_700_000_000_000 + (start + offset) * 10 for offset in range(count)]
    levels = []
    messages = []
    metadata = []
    for offset, timestamp in enumerate(timestamps):
        number = start + offset
        level_number, level_name = level(number)
        service = SERVICES[number % len(SERVICES)]
        path = PATHS[number % len(PATHS)]
        duration = (number * 37) % 2_000
        if level_name in ("warning", "error") and number % 7 == 0:
            message = f"request timeout after {duration}ms request_id={number:016x}"
        else:
            message = (
                f"{level_name} request completed in {duration}ms "
                f"request_id={number:016x}"
            )
        levels.append(level_number)
        messages.append(message.encode())
        metadata.append(
            json.dumps(
                {"service": service, "path": path, "status": str(200 + number % 6)},
                separators=(",", ":"),
            ).encode()
        )

    out = bytearray(b"\x01\x00\x00\x00")
    out += struct.pack("<I", count)
    out += b"".join(struct.pack("<q", timestamp) for timestamp in timestamps)
    out += bytes(levels)
    for values in (messages, metadata):
        for value in values:
            out += struct.pack("<I", len(value)) + value
    return bytes(out)


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def query_p95_ms(db, sql, parameters=()):
    samples = []
    expected = None
    for _ in range(51):
        started = time.perf_counter_ns()
        value = db.execute(sql, parameters).fetchall()
        elapsed = (time.perf_counter_ns() - started) / 1_000_000
        if expected is None:
            expected = value
        assert value == expected
        samples.append(elapsed)
    return round(percentile(samples[1:], 0.95), 3)


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: session6_log_compaction.py EXTENSION DATABASE LABEL")
    extension, database, label = sys.argv[1:]
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(database + suffix)
        except FileNotFoundError:
            pass

    db = sqlite3.connect(database, isolation_level=None)
    db.enable_load_extension(True)
    db.load_extension(extension)
    db.enable_load_extension(False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(
        "CREATE VIRTUAL TABLE logs USING "
        "timeless_logs(index_keys='service,path,status')"
    )

    optimize_ms = []
    observed_rewrite_entries = 0
    observed_rewrite_bytes = 0
    ingested_raw_bytes = 0
    for cycle in range(CYCLES):
        start = cycle * ENTRIES_PER_CYCLE
        db.execute("INSERT INTO logs(logs) VALUES (?)", (encode_batch(start, ENTRIES_PER_CYCLE),))
        db.execute("INSERT INTO logs(logs) VALUES ('flush')")
        ingested_raw_bytes += db.execute(
            "SELECT COALESCE(SUM(length(data)),0) FROM logs_blocks WHERE codec=1"
        ).fetchone()[0]
        before = {
            block_id: (entry_count, byte_count)
            for block_id, entry_count, byte_count in db.execute(
                "SELECT id, entry_count, length(data) FROM logs_blocks"
            )
        }
        started = time.perf_counter_ns()
        db.execute("INSERT INTO logs(logs) VALUES ('optimize')")
        optimize_ms.append((time.perf_counter_ns() - started) / 1_000_000)
        after = {row[0] for row in db.execute("SELECT id FROM logs_blocks")}
        for block_id, (entry_count, byte_count) in before.items():
            if block_id not in after:
                observed_rewrite_entries += entry_count
                observed_rewrite_bytes += byte_count

    total_entries = CYCLES * ENTRIES_PER_CYCLE
    block_count, payload_bytes = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(length(data)),0) FROM logs_blocks"
    ).fetchone()
    assert db.execute("SELECT n FROM timeless_log_count('logs')").fetchone()[0] == total_entries
    stats = dict(
        db.execute("SELECT key, CAST(value AS INTEGER) FROM timeless_stats('logs')")
    )
    measured_rewrite_entries = stats.get("optimize_raw_entries", 0) + stats.get(
        "optimize_merge_entries", 0
    )
    measured_rewrite_bytes = stats.get("optimize_raw_input_bytes", 0) + stats.get(
        "optimize_merge_input_bytes", 0
    )
    if measured_rewrite_entries:
        assert measured_rewrite_entries == observed_rewrite_entries
        assert measured_rewrite_bytes == observed_rewrite_bytes
    rewrite_entries = measured_rewrite_entries or observed_rewrite_entries
    rewrite_bytes = measured_rewrite_bytes or observed_rewrite_bytes

    result = {
        "label": label,
        "entries": total_entries,
        "blocks": block_count,
        "payload_bytes": payload_bytes,
        "bytes_per_entry": round(payload_bytes / total_entries, 3),
        "rewrite_entries": rewrite_entries,
        "rewrite_amplification": round(rewrite_entries / total_entries, 3),
        "ingested_raw_bytes": ingested_raw_bytes,
        "rewrite_bytes": rewrite_bytes,
        "byte_rewrite_amplification": round(rewrite_bytes / ingested_raw_bytes, 3),
        "optimize_total_ms": round(sum(optimize_ms), 3),
        "optimize_p50_ms": round(percentile(optimize_ms, 0.50), 3),
        "optimize_p95_ms": round(percentile(optimize_ms, 0.95), 3),
        "optimize_p99_ms": round(percentile(optimize_ms, 0.99), 3),
        "optimize_max_ms": round(max(optimize_ms), 3),
        "error_count_p95_ms": query_p95_ms(
            db, "SELECT n FROM timeless_log_count('logs', '{\"level\":\"error\"}')"
        ),
        "service_count_p95_ms": query_p95_ms(
            db,
            "SELECT n FROM timeless_log_count(" 
            "'logs', '{\"level\":\"info\",\"service\":\"api\"}')",
        ),
        "latest_100_p95_ms": query_p95_ms(
            db, "SELECT ts FROM logs ORDER BY ts DESC LIMIT 100"
        ),
    }
    print(json.dumps(result, sort_keys=True))
    db.close()


if __name__ == "__main__":
    main()
