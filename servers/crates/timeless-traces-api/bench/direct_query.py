#!/usr/bin/env python3
"""Direct SQLite extension counterpart to the fixed HTTP query matrix."""

import argparse
import gc
import json
import os
import sqlite3
import statistics
import time


SPAN_COLUMNS = (
    "trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,"
    "duration_ns,attributes,status_description,events,resource,instrumentation_scope"
)


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def stats(connection):
    return {
        key: value
        for key, value in connection.execute(
            "SELECT key,value FROM timeless_stats('traces')"
        )
    }


def process_memory():
    values = {}
    with open("/proc/self/status", encoding="utf-8") as status:
        for line in status:
            if line.startswith(("VmRSS:", "VmHWM:")):
                key, value, _unit = line.split()
                values[key.rstrip(":").lower() + "_kib"] = int(value)
    return values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--extension", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    connection = sqlite3.connect(args.database)
    connection.enable_load_extension(True)
    connection.load_extension(os.path.abspath(args.extension))
    trace = f"{(1 << 48) + 1:032x}"
    base_ns = 1_700_000_000_000_000_000
    shapes = [
        (
            "services",
            "SELECT value FROM timeless_trace_services('traces') ORDER BY value",
            (),
            max(3, args.repeats // 4),
        ),
        (
            "operations",
            "SELECT value FROM timeless_trace_operations('traces', ?) ORDER BY value",
            ("bench",),
            max(3, args.repeats // 4),
        ),
        (
            "trace_exact",
            f"SELECT {SPAN_COLUMNS} FROM traces WHERE trace_id=? "
            "ORDER BY start_ts,span_id",
            (trace,),
            args.repeats,
        ),
        (
            "search_service_fanout",
            f"SELECT {SPAN_COLUMNS} FROM traces WHERE service=? "
            "ORDER BY start_ts DESC,span_id DESC LIMIT 20",
            ("bench",),
            args.repeats,
        ),
        (
            "search_service_operation",
            f"SELECT {SPAN_COLUMNS} FROM traces WHERE service=? AND name=? "
            "ORDER BY start_ts DESC,span_id DESC LIMIT 20",
            ("bench", "GET /bench"),
            args.repeats,
        ),
        (
            "search_time_selective",
            f"SELECT {SPAN_COLUMNS} FROM traces WHERE service=? "
            "AND start_ts>=? AND start_ts<=? "
            "ORDER BY start_ts DESC,span_id DESC LIMIT 20",
            ("bench", base_ns, base_ns + 100_000),
            args.repeats,
        ),
        (
            "search_duration_miss",
            f"SELECT {SPAN_COLUMNS} FROM traces WHERE service=? AND duration_ns>=? "
            "ORDER BY start_ts DESC,span_id DESC LIMIT 20",
            ("bench", 1_000_000),
            args.repeats,
        ),
    ]
    for _name, sql, parameters, _count in shapes:
        connection.execute(sql, parameters).fetchall()
    before = stats(connection)
    results = {}
    memory_by_shape = {"after_warmup": process_memory()}
    for name, sql, parameters, count in shapes:
        latencies = []
        cardinalities = []
        for _ in range(count):
            started = time.perf_counter_ns()
            rows = connection.execute(sql, parameters).fetchall()
            latencies.append((time.perf_counter_ns() - started) / 1_000_000)
            cardinalities.append(len(rows))
        results[name] = {
            "requests": count,
            "p50_ms": percentile(latencies, 0.50),
            "p95_ms": percentile(latencies, 0.95),
            "p99_ms": percentile(latencies, 0.99),
            "mean_ms": statistics.fmean(latencies),
            "rows": sorted(set(cardinalities)),
        }
        gc.collect()
        memory_by_shape[name] = process_memory()
    after = stats(connection)
    work = {
        key: after[key] - before[key]
        for key in after
        if key.startswith(("query_", "discovery_"))
        and isinstance(after[key], int)
        and isinstance(before.get(key), int)
    }
    report = {
        "fixture": {
            "total_spans": after["total_spans"],
            "blocks": after["blocks"],
            "logical_payload_bytes": after["bytes_on_disk"],
        },
        "routes": results,
        "extension_work_delta": work,
        "process_memory": process_memory(),
        "process_memory_by_shape": memory_by_shape,
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
