#!/usr/bin/env python3
"""Capture reproducible narrow/wide query and durable-ingest evidence."""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def http(
    base: str,
    path: str,
    data: bytes | None = None,
    content_type: str | None = None,
    timeout: float = 30.0,
) -> tuple[int, bytes, dict[str, str]]:
    headers = {"content-type": content_type} if content_type else {}
    request = urllib.request.Request(base + path, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read(), dict(response.headers.items())
    except urllib.error.HTTPError as error:
        body = error.read()
        raise RuntimeError(f"HTTP {error.code} for {path}: {body[:500]!r}") from error


def wait_live(base: str, process: subprocess.Popen, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited during startup with {process.returncode}")
        try:
            status, _, _ = http(base, "/live", timeout=0.5)
            if status == 200:
                return
        except (OSError, RuntimeError):
            pass
        time.sleep(0.05)
    raise RuntimeError(f"server did not become live at {base}")


def build_identity(binary: Path) -> dict:
    output = subprocess.check_output([str(binary), "--version"], text=True)
    return json.loads(output)


def stats(base: str, path: str) -> dict:
    status, body, _ = http(base, path)
    if status != 200:
        raise RuntimeError(f"stats returned {status}")
    return json.loads(body)


def numeric_delta(before: dict, after: dict) -> dict:
    result: dict[str, int | float] = {}
    for key, value in after.items():
        prior = before.get(key)
        if isinstance(value, (int, float)) and isinstance(prior, (int, float)):
            difference = value - prior
            if difference:
                result[key] = difference
    return result


def percentile(values: list[int], percentile_value: float) -> int:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) * percentile_value + 0.999999)) - 1))
    return ordered[index]


def hwm_kib(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    raise RuntimeError(f"VmHWM is unavailable for pid {pid}")


def measure(
    name: str,
    request: Callable[[], bytes],
    cardinality: Callable[[bytes], int],
    expected: int,
    stats_before: Callable[[], dict],
    iterations: int,
    warmup: int,
) -> dict:
    for _ in range(warmup):
        body = request()
        actual = cardinality(body)
        if actual != expected:
            raise RuntimeError(f"{name} warmup cardinality {actual}, expected {expected}")
    before = stats_before()
    latencies: list[int] = []
    response_bytes = 0
    for _ in range(iterations):
        started = time.perf_counter_ns()
        body = request()
        latencies.append(time.perf_counter_ns() - started)
        response_bytes = len(body)
        actual = cardinality(body)
        if actual != expected:
            raise RuntimeError(f"{name} cardinality {actual}, expected {expected}")
    after = stats_before()
    return {
        "iterations": iterations,
        "warmup": warmup,
        "latency_ns": {
            "min": min(latencies),
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": max(latencies),
        },
        "result_cardinality": expected,
        "response_bytes": response_bytes,
        "stats_delta": numeric_delta(before, after),
    }


class Server:
    def __init__(self, binary: Path, extension: Path, database: Path, env: dict[str, str]):
        self.binary = binary
        self.database = database
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.log_path = database.with_suffix(".server.log")
        self.log = self.log_path.open("wb")
        process_env = os.environ.copy()
        process_env.update(env)
        self.process = subprocess.Popen(
            [str(binary), str(extension), str(database), f"127.0.0.1:{self.port}"],
            env=process_env,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_live(self.base, self.process)
        except Exception:
            self.close(require_clean=False)
            raise

    def close(self, require_clean: bool = True) -> None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()
        if require_clean and self.process.returncode != 0:
            output = self.log_path.read_text(errors="replace")
            raise RuntimeError(f"{self.binary.name} shutdown={self.process.returncode}:\n{output}")


def query_json_cardinality(body: bytes) -> int:
    response = json.loads(body)
    if response.get("status") != "success":
        raise RuntimeError(f"query failed: {response}")
    return len(response["data"]["result"])


def ndjson_cardinality(body: bytes) -> int:
    return sum(1 for line in body.splitlines() if line)


def metrics_evidence(
    root: Path,
    extension: Path,
    binary: Path,
    directory: Path,
    series: int,
    points: int,
    iterations: int,
    warmup: int,
) -> dict:
    server = Server(
        binary,
        extension,
        directory / "metrics.db",
        {
            "TIMELESS_AUTH_MODE": "disabled",
            "TIMELESS_METRICS_FLUSH_INTERVAL_SECS": "3600",
            "TIMELESS_METRICS_COMPACT_INTERVAL_SECS": "3600",
            "TIMELESS_METRICS_RETENTION_INTERVAL_SECS": "3600",
        },
    )
    try:
        base_ts = 1_800_000_000
        timestamps = [(base_ts + point * 10) * 1000 for point in range(points)]
        lines = []
        for index in range(series):
            lines.append(
                json.dumps(
                    {
                        "metric": {
                            "__name__": "query_contract_cpu",
                            "host": f"h{index:04d}",
                            "service": "api" if index % 2 == 0 else "worker",
                        },
                        "values": [float(index + point) for point in range(points)],
                        "timestamps": timestamps,
                    },
                    separators=(",", ":"),
                )
            )
        payload = ("\n".join(lines) + "\n").encode()
        started = time.perf_counter_ns()
        status, _, _ = http(server.base, "/api/v1/import", payload, "application/json")
        admission_ns = time.perf_counter_ns() - started
        if status != 204:
            raise RuntimeError(f"metrics import returned {status}")
        started = time.perf_counter_ns()
        status, flush_body, _ = http(server.base, "/api/v1/flush", b"", "application/json")
        durable_ns = time.perf_counter_ns() - started
        if status != 200:
            raise RuntimeError(f"metrics flush returned {status}: {flush_body!r}")
        after_flush = stats(server.base, "/select/metrics/stats")
        expected_points = series * points
        if after_flush["completed_points"] != expected_points or after_flush["queued_points"] != 0:
            raise RuntimeError(f"metrics durable watermark mismatch: {after_flush}")

        at = base_ts + (points - 1) * 10
        def promql(expression: str) -> bytes:
            query = urllib.parse.urlencode({"query": expression, "time": at})
            return http(server.base, f"/api/v1/query?{query}")[1]

        stat = lambda: stats(server.base, "/select/metrics/stats")
        narrow = measure(
            "metrics-narrow",
            lambda: promql('query_contract_cpu{host="h0000"}'),
            query_json_cardinality,
            1,
            stat,
            iterations,
            warmup,
        )
        wide = measure(
            "metrics-wide",
            lambda: promql("query_contract_cpu"),
            query_json_cardinality,
            series,
            stat,
            iterations,
            warmup,
        )
        range_narrow = measure(
            "metrics-range-vector-narrow",
            lambda: promql('query_contract_cpu{host="h0000"}[5m]'),
            query_json_cardinality,
            1,
            stat,
            iterations,
            warmup,
        )
        range_wide = measure(
            "metrics-range-vector-wide",
            lambda: promql("query_contract_cpu[5m]"),
            query_json_cardinality,
            series,
            stat,
            iterations,
            warmup,
        )
        final_stats = stat()
        return {
            "build": build_identity(binary),
            "fixture": {"series": series, "points_per_series": points, "logical_points": expected_points},
            "ingestion": {
                "wire_bytes": len(payload),
                "admission_ns": admission_ns,
                "durability_barrier_ns": durable_ns,
                "completed_points": after_flush["completed_points"],
                "failed_points": after_flush["failed_points"],
                "queued_points": after_flush["queued_points"],
            },
            "queries": {
                "narrow": narrow,
                "wide": wide,
                "range_vector_narrow": range_narrow,
                "range_vector_wide": range_wide,
            },
            "storage": {
                key: final_stats[key]
                for key in (
                    "bytes_on_disk",
                    "sqlite_index_bytes",
                    "database_file_bytes",
                    "database_wal_bytes",
                    "database_shm_bytes",
                    "physical_database_bytes",
                    "buffer_memory_bytes",
                    "chunks",
                    "series",
                )
            },
            "rss_hwm_kib": hwm_kib(server.process.pid),
            "cancellation": {"cancelled_requests": final_stats["api_read_cancelled"], "contract_test": "session_four_cancels_dropped_promql_requests_and_reuses_the_reader"},
        }
    finally:
        server.close()


def logs_evidence(
    root: Path,
    extension: Path,
    binary: Path,
    directory: Path,
    entries: int,
    iterations: int,
    warmup: int,
) -> dict:
    server = Server(
        binary,
        extension,
        directory / "logs.db",
        {
            "TIMELESS_AUTH_MODE": "disabled",
            "TIMELESS_LOGS_FLUSH_INTERVAL_SECS": "3600",
            "TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS": "3600",
        },
    )
    try:
        severities = ["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"]
        base_ts = 1_800_000_000_000_000
        lines = []
        for index in range(entries):
            lines.append(
                json.dumps(
                    {
                        "_time": base_ts + index,
                        "_msg": f"query contract event {index}",
                        "level": severities[index % len(severities)],
                        "service": "api" if index % 4 == 0 else "worker",
                        "host": f"h{index % 64:02d}",
                        "status": 500 if index % 8 == 4 else 200,
                        "context": {"retry": index % 3 == 0, "attempt": index % 5},
                    },
                    separators=(",", ":"),
                )
            )
        payload = ("\n".join(lines) + "\n").encode()
        started = time.perf_counter_ns()
        status, _, _ = http(server.base, "/insert/jsonline", payload, "application/x-ndjson")
        admission_ns = time.perf_counter_ns() - started
        if status != 204:
            raise RuntimeError(f"logs ingest returned {status}")
        started = time.perf_counter_ns()
        status, flush_body, _ = http(server.base, "/api/v1/flush")
        durable_ns = time.perf_counter_ns() - started
        if status != 200:
            raise RuntimeError(f"logs flush returned {status}: {flush_body!r}")
        after_flush = stats(server.base, "/select/logsql/stats")
        if after_flush["completed_entries"] != entries or after_flush["queued_entries"] != 0:
            raise RuntimeError(f"logs durable watermark mismatch: {after_flush}")

        def logsql(expression: str) -> bytes:
            body = urllib.parse.urlencode({"query": expression}).encode()
            return http(
                server.base,
                "/select/logsql/query",
                body,
                "application/x-www-form-urlencoded",
            )[1]

        stat = lambda: stats(server.base, "/select/logsql/stats")
        narrow_expected = entries // len(severities)
        narrow = measure(
            "logs-narrow",
            lambda: logsql("level:error service:api | limit 10000"),
            ndjson_cardinality,
            narrow_expected,
            stat,
            iterations,
            warmup,
        )
        wide = measure(
            "logs-wide",
            lambda: logsql("* | limit 10000"),
            ndjson_cardinality,
            entries,
            stat,
            iterations,
            warmup,
        )
        final_stats = stat()
        return {
            "build": build_identity(binary),
            "fixture": {"logical_entries": entries, "severities": severities, "typed_nested_metadata": True},
            "ingestion": {
                "wire_bytes": len(payload),
                "admission_ns": admission_ns,
                "durability_barrier_ns": durable_ns,
                "completed_entries": after_flush["completed_entries"],
                "queued_entries": after_flush["queued_entries"],
            },
            "queries": {"narrow": narrow, "wide": wide},
            "storage": {
                key: final_stats[key]
                for key in (
                    "total_bytes",
                    "disk_size",
                    "index_size",
                    "database_file_bytes",
                    "database_wal_bytes",
                    "database_shm_bytes",
                    "physical_database_bytes",
                    "raw_blocks",
                    "compressed_blocks",
                    "buffered_entries",
                )
            },
            "rss_hwm_kib": hwm_kib(server.process.pid),
            "cancellation": {"contract_test": "HTTP disconnect coverage is added per LogsQL evaluator row"},
        }
    finally:
        server.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extension", type=Path, default=Path("target/release/libtimeless_ext.so"))
    parser.add_argument("--metrics-binary", type=Path, default=Path("servers/target/release/timeless-metrics-api"))
    parser.add_argument("--logs-binary", type=Path, default=Path("servers/target/release/timeless-logs-api"))
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--metric-series", type=int, default=512)
    parser.add_argument("--metric-points", type=int, default=32)
    parser.add_argument("--log-entries", type=int, default=8192)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.iterations, args.metric_series, args.metric_points, args.log_entries) <= 0 or args.warmup < 0:
        parser.error("workload sizes and iterations must be positive; warmup must be non-negative")

    root = Path(__file__).resolve().parents[1]
    extension = args.extension.resolve()
    metrics_binary = args.metrics_binary.resolve()
    logs_binary = args.logs_binary.resolve()
    for path in (extension, metrics_binary, logs_binary):
        if not path.is_file():
            parser.error(f"missing release artifact: {path}")

    with tempfile.TemporaryDirectory(prefix="timeless-query-evidence-") as temporary:
        directory = Path(temporary)
        evidence = {
            "schema_version": 1,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
            "host": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "processor": platform.processor(),
            },
            "workload": {
                "iterations": args.iterations,
                "warmup": args.warmup,
                "single_client": True,
                "loopback_http": True,
                "release_build": True,
            },
            "metrics": metrics_evidence(
                root,
                extension,
                metrics_binary,
                directory,
                args.metric_series,
                args.metric_points,
                args.iterations,
                args.warmup,
            ),
            "logs": logs_evidence(
                root,
                extension,
                logs_binary,
                directory,
                args.log_entries,
                args.iterations,
                args.warmup,
            ),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
