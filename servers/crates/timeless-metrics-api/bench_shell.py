#!/usr/bin/env python3
"""Sequential control-route smoke benchmark for the Session 1 shell."""

import argparse
import json
import math
import time
import urllib.request


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:19439")
    parser.add_argument("--requests", type=int, default=2_000)
    parser.add_argument("--flush-requests", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=100)
    args = parser.parse_args()
    if args.requests <= 0 or args.flush_requests <= 0 or args.warmup < 0:
        parser.error("request counts must be positive and warmup non-negative")
    return args


def request(base, method, path):
    req = urllib.request.Request(base + path, method=method)
    started = time.perf_counter_ns()
    with urllib.request.urlopen(req, timeout=10) as response:
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"{method} {path}: HTTP {response.status}")
    elapsed = time.perf_counter_ns() - started
    payload = json.loads(body)
    if payload.get("status") not in (None, "ok"):
        raise RuntimeError(f"{method} {path}: {payload}")
    return elapsed


def percentile(sorted_values, quantile):
    index = max(0, math.ceil(len(sorted_values) * quantile) - 1)
    return sorted_values[index]


def measure(base, method, path, count, warmup):
    for _ in range(warmup):
        request(base, method, path)
    values = [request(base, method, path) for _ in range(count)]
    values.sort()
    total_ns = sum(values)
    print(
        f"{method} {path}: requests={count} errors=0 "
        f"sequential_rps={count / (total_ns / 1e9):.1f} "
        f"p50_us={percentile(values, 0.50) / 1e3:.1f} "
        f"p95_us={percentile(values, 0.95) / 1e3:.1f} "
        f"p99_us={percentile(values, 0.99) / 1e3:.1f}"
    )


def main():
    args = parse_args()
    base = args.url.rstrip("/")
    measure(base, "GET", "/health", args.requests, args.warmup)
    measure(base, "GET", "/select/metrics/stats", args.requests, args.warmup)
    measure(base, "POST", "/api/v1/flush", args.flush_requests, args.warmup)


if __name__ == "__main__":
    main()
