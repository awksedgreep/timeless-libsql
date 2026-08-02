#!/usr/bin/env python3
"""Deterministic Jaeger route latency and extension-work benchmark."""

import argparse
import json
import statistics
import time
import urllib.request


def get(url):
    started = time.perf_counter_ns()
    with urllib.request.urlopen(url, timeout=120) as response:
        body = response.read()
        status = response.status
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    if status != 200:
        raise RuntimeError(f"GET {url}: {status}: {body!r}")
    return elapsed_ms, body, json.loads(body)


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def memory(pid):
    output = {}
    if not pid:
        return output
    with open(f"/proc/{pid}/status", encoding="utf-8") as status:
        for line in status:
            if line.startswith(("VmRSS:", "VmHWM:")):
                key, value, _unit = line.split()
                output[key.rstrip(":").lower() + "_kib"] = int(value)
    return output


def stats(base):
    return get(base + "/select/traces/stats")[2]


def delta(after, before, prefix):
    return {
        key: after[key] - before[key]
        for key in after
        if key.startswith(prefix)
        and isinstance(after[key], int)
        and isinstance(before.get(key), int)
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:19449")
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    base = args.url.rstrip("/")
    trace = f"{(1 << 48) + 1:032x}"
    base_us = 1_700_000_000_000_000
    shapes = [
        ("services", "/select/jaeger/api/services", max(3, args.repeats // 4)),
        (
            "operations",
            "/select/jaeger/api/services/bench/operations",
            max(3, args.repeats // 4),
        ),
        ("trace_exact", f"/select/jaeger/api/traces/{trace}", args.repeats),
        (
            "search_service_fanout",
            "/select/jaeger/api/traces?service=bench&limit=20",
            args.repeats,
        ),
        (
            "search_service_operation",
            "/select/jaeger/api/traces?service=bench&operation=GET%20%2Fbench&limit=20",
            args.repeats,
        ),
        (
            "search_time_selective",
            f"/select/jaeger/api/traces?service=bench&start={base_us}&end={base_us + 100}&limit=20",
            args.repeats,
        ),
        (
            "search_duration_miss",
            "/select/jaeger/api/traces?service=bench&minDuration=1ms&limit=20",
            args.repeats,
        ),
    ]
    # Warm every plan once before taking the cumulative-counter baseline.
    for _name, path, _count in shapes:
        get(base + path)
    before = stats(base)
    results = {}
    for name, path, count in shapes:
        latencies = []
        response_bytes = []
        returned_traces = []
        returned_spans = []
        for _ in range(count):
            elapsed, body, parsed = get(base + path)
            latencies.append(elapsed)
            response_bytes.append(len(body))
            traces = parsed["data"]
            returned_traces.append(len(traces))
            returned_spans.append(
                sum(len(trace.get("spans", [])) for trace in traces)
                if traces and isinstance(traces[0], dict)
                else 0
            )
        results[name] = {
            "requests": count,
            "p50_ms": percentile(latencies, 0.50),
            "p95_ms": percentile(latencies, 0.95),
            "p99_ms": percentile(latencies, 0.99),
            "mean_ms": statistics.fmean(latencies),
            "response_bytes": sorted(set(response_bytes)),
            "returned_traces": sorted(set(returned_traces)),
            "returned_spans": sorted(set(returned_spans)),
        }
    after = stats(base)
    report = {
        "fixture": {
            "source": "bench/ingest.py",
            "seed_contract": "workers=16 requests=100 batch=500",
            "total_spans": after["total_spans"],
            "blocks": after["blocks"],
            "raw_blocks": after["raw_blocks"],
            "compressed_blocks": after["compressed_blocks"],
            "logical_payload_bytes": after["bytes_on_disk"],
            "physical_database_bytes": after["physical_database_bytes"],
        },
        "routes": results,
        "api_work_delta": delta(after, before, "api_read_"),
        "extension_work_delta": delta(after, before, "extension_query_"),
        "memory": memory(args.server_pid),
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
