#!/usr/bin/env python3
"""Deterministic completion-aware OTLP JSON ingest benchmark.

The server's 200 response covers its one SQLite statement. This harness still
calls the explicit flush barrier and reports offered time plus drain time so
the number is comparable with the Session 0 Elixir control.
"""

import argparse
import concurrent.futures
import http.client
import json
import os
import statistics
import time
import urllib.parse
import urllib.request


def body(worker, sequence, count, base_ns):
    spans = []
    for offset in range(count):
        trace_ordinal = offset // 5
        identity_base = (worker << 48) | (sequence << 16)
        trace_number = identity_base + trace_ordinal + 1
        number = identity_base + offset + 1
        start = base_ns + sequence * 1_000_000 + offset * 1_000
        spans.append({
            "traceId": f"{trace_number:032x}",
            "spanId": f"{number:016x}",
            "parentSpanId": "" if offset % 5 == 0 else f"{(identity_base + offset - offset % 5 + 1):016x}",
            "name": "GET /bench" if offset % 5 == 0 else "db.query",
            "kind": 2 if offset % 5 == 0 else 3,
            "startTimeUnixNano": str(start),
            "endTimeUnixNano": str(start + 100_000),
            "status": {"code": 0},
            "attributes": [
                {"key": "http.method", "value": {"stringValue": "GET"}},
                {"key": "worker", "value": {"intValue": worker}},
            ],
            "events": [],
        })
    return json.dumps({"resourceSpans": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "bench"}}
        ]},
        "scopeSpans": [{"scope": {"name": "bench"}, "spans": spans}],
    }]}, separators=(",", ":")).encode()


def worker(host, port, number, requests, batch, base_ns):
    connection = http.client.HTTPConnection(host, port, timeout=60)
    bodies = [body(number, sequence, batch, base_ns) for sequence in range(requests)]
    latencies = []
    errors = []
    for payload in bodies:
        started = time.perf_counter_ns()
        try:
            connection.request(
                "POST", "/insert/opentelemetry/v1/traces", payload,
                {"content-type": "application/json"})
            response = connection.getresponse()
            response.read()
            if response.status != 200:
                errors.append(response.status)
        except Exception as error:
            errors.append(repr(error))
            connection.close()
            connection = http.client.HTTPConnection(host, port, timeout=60)
        latencies.append((time.perf_counter_ns() - started) / 1_000_000)
    connection.close()
    return latencies, errors


def request_json(url, method="GET"):
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def percentile(values, fraction):
    if not values:
        return 0
    return values[min(len(values) - 1, int((len(values) - 1) * fraction))]


def proc_memory(pid):
    result = {}
    if not pid:
        return result
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as status:
            for line in status:
                if line.startswith(("VmRSS:", "VmHWM:")):
                    key, value, _unit = line.split()
                    result[key.rstrip(":").lower() + "_kib"] = int(value)
    except FileNotFoundError:
        pass
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:19449")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--server-pid", type=int)
    args = parser.parse_args()
    target = urllib.parse.urlsplit(args.url)
    before = request_json(args.url + "/select/traces/stats")
    base_ns = 1_700_000_000_000_000_000
    started = time.perf_counter_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(
            worker, target.hostname, target.port, number + 1, args.requests,
            args.batch, base_ns) for number in range(args.workers)]
        results = [future.result() for future in futures]
    offered_seconds = (time.perf_counter_ns() - started) / 1_000_000_000
    drain_started = time.perf_counter_ns()
    flush = request_json(args.url + "/api/v1/flush", "POST")
    drain_seconds = (time.perf_counter_ns() - drain_started) / 1_000_000_000
    after = request_json(args.url + "/select/traces/stats")
    latencies = sorted(value for result, _errors in results for value in result)
    errors = [error for _result, failures in results for error in failures]
    completed = after["completed_spans"] - before["completed_spans"]
    report = {
        "workers": args.workers,
        "requests_per_worker": args.requests,
        "batch_spans": args.batch,
        "offered_requests": args.workers * args.requests,
        "offered_spans": args.workers * args.requests * args.batch,
        "completed_spans": completed,
        "failed_spans": after["failed_spans"] - before["failed_spans"],
        "http_errors": errors,
        "offered_seconds": offered_seconds,
        "drain_seconds": drain_seconds,
        "completed_spans_per_second": completed / (offered_seconds + drain_seconds),
        "request_p50_ms": percentile(latencies, 0.50),
        "request_p95_ms": percentile(latencies, 0.95),
        "request_p99_ms": percentile(latencies, 0.99),
        "request_mean_ms": statistics.fmean(latencies),
        "flush": flush,
        "stats": after,
        "memory": proc_memory(args.server_pid),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if errors or completed != report["offered_spans"] or report["failed_spans"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
