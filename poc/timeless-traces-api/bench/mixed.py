#!/usr/bin/env python3
"""Writer fairness under zero, one, and two broad concurrent trace queries."""

import argparse
import concurrent.futures
import json
import sys
import threading
import time
import urllib.parse
import urllib.request

sys.dont_write_bytecode = True
import ingest  # noqa: E402


QUERY_PATH = (
    "/select/jaeger/api/traces?service=bench&minDuration=1ms&limit=20"
)


def query_loop(base, started, stop):
    latencies = []
    errors = []
    started.wait()
    while not stop.is_set():
        begin = time.perf_counter_ns()
        try:
            with urllib.request.urlopen(base + QUERY_PATH, timeout=120) as response:
                body = response.read()
                if response.status != 200:
                    errors.append(f"HTTP {response.status}: {body!r}")
        except Exception as error:  # benchmark evidence retains exact failures
            errors.append(repr(error))
        latencies.append((time.perf_counter_ns() - begin) / 1_000_000)
    return latencies, errors


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return 0
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def counter_delta(after, before, keys):
    return {key: after[key] - before[key] for key in keys}


def phase(args, phase_number, query_workers):
    target = urllib.parse.urlsplit(args.url)
    before = ingest.request_json(args.url + "/select/traces/stats")
    started = threading.Event()
    stop = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.writers + query_workers
    ) as pool:
        queries = [
            pool.submit(query_loop, args.url, started, stop)
            for _ in range(query_workers)
        ]
        base_ns = 1_700_000_100_000_000_000 + phase_number * 1_000_000_000
        writes = [
            pool.submit(
                ingest.worker,
                target.hostname,
                target.port,
                1_000 + phase_number * args.writers + worker,
                args.requests,
                args.batch,
                base_ns,
            )
            for worker in range(args.writers)
        ]
        begin = time.perf_counter_ns()
        started.set()
        write_results = [future.result() for future in writes]
        writer_seconds = (time.perf_counter_ns() - begin) / 1_000_000_000
        stop.set()
        drain_begin = time.perf_counter_ns()
        flush = ingest.request_json(args.url + "/api/v1/flush", "POST")
        durable_seconds = (time.perf_counter_ns() - begin) / 1_000_000_000
        drain_seconds = (time.perf_counter_ns() - drain_begin) / 1_000_000_000
        query_results = [future.result() for future in queries]
    after = ingest.request_json(args.url + "/select/traces/stats")
    write_latencies = sorted(
        latency for latencies, _errors in write_results for latency in latencies
    )
    write_errors = [
        error for _latencies, errors in write_results for error in errors
    ]
    query_latencies = sorted(
        latency for latencies, _errors in query_results for latency in latencies
    )
    query_errors = [
        error for _latencies, errors in query_results for error in errors
    ]
    completed = after["completed_spans"] - before["completed_spans"]
    offered = args.writers * args.requests * args.batch
    result = {
        "query_workers": query_workers,
        "offered_spans": offered,
        "completed_spans": completed,
        "failed_spans": after["failed_spans"] - before["failed_spans"],
        "writer_seconds": writer_seconds,
        "durable_seconds": durable_seconds,
        "drain_seconds": drain_seconds,
        "durable_spans_per_second": completed / durable_seconds,
        "write_p50_ms": percentile(write_latencies, 0.50),
        "write_p95_ms": percentile(write_latencies, 0.95),
        "write_p99_ms": percentile(write_latencies, 0.99),
        "query_requests": len(query_latencies),
        "query_p50_ms": percentile(query_latencies, 0.50),
        "query_p95_ms": percentile(query_latencies, 0.95),
        "query_p99_ms": percentile(query_latencies, 0.99),
        "write_errors": write_errors,
        "query_errors": query_errors,
        "flush": flush,
        "memory": ingest.proc_memory(args.server_pid),
        "gate_delta": counter_delta(
            after,
            before,
            (
                "extension_read_conflicts",
                "extension_read_barge_rejections",
                "extension_writer_wait_count",
                "extension_writer_wait_ns",
                "extension_writer_timeouts",
                "api_read_retries",
            ),
        ),
        "maintenance_delta": counter_delta(
            after,
            before,
            (
                "extension_optimize_count",
                "extension_optimize_total_ns",
                "extension_optimize_raw_entries",
                "extension_optimize_merge_entries",
            ),
        ),
    }
    if completed != offered or result["failed_spans"] or write_errors or query_errors:
        raise RuntimeError(json.dumps(result, sort_keys=True))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:19449")
    parser.add_argument("--writers", type=int, default=8)
    parser.add_argument("--requests", type=int, default=25)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--server-pid", type=int)
    args = parser.parse_args()
    results = [phase(args, index, readers) for index, readers in enumerate((0, 1, 2))]
    print(json.dumps({"phases": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
