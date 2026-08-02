#!/usr/bin/env python3
"""Measure repeated small-flush maintenance without inventing block policy.

The server must be started with a short optimize interval. Each cycle admits
one deterministic OTLP request, crosses the public durable flush barrier, then
waits for one ordered API maintenance wake-up. All block decisions and work
counters come back from timeless-libsql through the public stats endpoint.
"""

import argparse
import http.client
import json
import os
import sys
import time
import urllib.parse

sys.dont_write_bytecode = True
import ingest  # noqa: E402


def post(base, payload):
    target = urllib.parse.urlsplit(base)
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=60)
    connection.request(
        "POST",
        "/insert/opentelemetry/v1/traces",
        payload,
        {"content-type": "application/json"},
    )
    response = connection.getresponse()
    body = response.read()
    connection.close()
    if response.status != 200:
        raise RuntimeError(f"ingest failed: HTTP {response.status}: {body!r}")


def wait_for_optimize(base, previous, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stats = ingest.request_json(base + "/select/traces/stats")
        if stats["optimize_count"] > previous:
            return stats
        time.sleep(0.01)
    raise RuntimeError(f"optimize wake-up did not advance past {previous}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:19449")
    parser.add_argument("--cycles", type=int, default=16)
    parser.add_argument("--batch", type=int, default=512)
    parser.add_argument("--server-pid", type=int)
    args = parser.parse_args()
    base = args.url.rstrip("/")
    before = ingest.request_json(base + "/select/traces/stats")
    previous_optimize = before["optimize_count"]
    started = time.perf_counter_ns()
    cycles = []
    for cycle in range(args.cycles):
        payload = ingest.body(2_000 + cycle, 0, args.batch, 1_700_001_000_000_000_000)
        post(base, payload)
        ingest.request_json(base + "/api/v1/flush", "POST")
        stats = wait_for_optimize(base, previous_optimize)
        previous_optimize = stats["optimize_count"]
        cycles.append({
            "cycle": cycle + 1,
            "blocks": stats["blocks"],
            "logical_payload_bytes": stats["bytes_on_disk"],
            "physical_database_bytes": stats["physical_database_bytes"],
            "extension_optimize_total_ns": stats["extension_optimize_total_ns"],
            "extension_rewritten_spans": (
                stats["extension_optimize_raw_entries"]
                + stats["extension_optimize_merge_entries"]
            ),
        })
    after = ingest.request_json(base + "/select/traces/stats")
    report = {
        "cycles": cycles,
        "elapsed_seconds": (time.perf_counter_ns() - started) / 1_000_000_000,
        "offered_spans": args.cycles * args.batch,
        "completed_spans": after["completed_spans"] - before["completed_spans"],
        "final": after,
        "memory": ingest.proc_memory(args.server_pid),
        "database_files": {
            suffix: os.path.getsize(os.environ["TIMELESS_TRACES_DATABASE"] + suffix)
            if os.path.exists(os.environ["TIMELESS_TRACES_DATABASE"] + suffix)
            else 0
            for suffix in ("", "-wal", "-shm")
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["completed_spans"] != report["offered_spans"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
