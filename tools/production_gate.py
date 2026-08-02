#!/usr/bin/env python3
"""Completion-aware production soak and fault gate for the three release servers.

This runner only uses the public HTTP APIs of the signal-specific Rust
binaries. The binaries in turn use the existing timeless-libsql virtual
tables. An accepted asynchronous write is not counted as durable until the
signal's public flush barrier succeeds. Every planned restart cold-reopens the
same database and checks exact logical counts plus a semantic sentinel.

The short gate accelerates maintenance so CI crosses real flush/optimize/
compact boundaries. The release gate uses production defaults and runs all
three signals together for eight hours; that is also at least two hours for
each individual signal.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import hashlib
import http.client
import json
import math
import os
import pathlib
import resource
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from collections import defaultdict
from typing import Any


# The official 4 Hz x 64-record workload advances event time at 256 records
# per second. This fixed window begins at 2026-08-02T00:00:00Z, stays inside
# the seven-day retention default, and remains behind wall-clock "now" for the
# release run so exact-latest queries are never accidentally empty.
BASE_SECONDS = 1_785_628_800
ORDINALS_PER_SECOND = 256
BASE_MILLISECONDS = BASE_SECONDS * 1_000
BASE_NANOSECONDS = BASE_SECONDS * 1_000_000_000
SIGNALS = ("metrics", "logs", "traces")
COUNT_FIELDS = {"metrics": "total_points", "logs": "total_entries", "traces": "total_spans"}
STATS_PATHS = {
    "metrics": "/select/metrics/stats",
    "logs": "/select/logsql/stats",
    "traces": "/select/traces/stats",
}
FLUSH_METHODS = {"metrics": "POST", "logs": "GET", "traces": "POST"}
QUERY_SHAPES = {
    "metrics": ("exact_latest", "narrow_range", "wide_range", "scalar_avg", "discovery"),
    "logs": ("exact", "narrow", "wide", "scalar_count", "discovery"),
    "traces": ("exact_trace", "narrow_search", "wide_search", "operations", "services"),
}
COUNTER_FIELDS = (
    "checkpoint_count",
    "checkpoint_errors",
    "backup_count",
    "backup_errors",
    "compact_count",
    "compact_errors",
    "optimize_count",
    "optimize_errors",
    "prune_count",
    "prune_errors",
    "scheduled_flush_count",
    "scheduled_flush_errors",
    "api_read_cancelled",
    "extension_query_cancelled",
    "api_read_retries",
    "read_conflicts",
    "extension_read_conflicts",
    "writer_timeouts",
    "extension_writer_timeouts",
)


class GateFailure(RuntimeError):
    pass


def monotonic_ns() -> int:
    return time.perf_counter_ns()


def elapsed_ms(started_ns: int) -> float:
    return (monotonic_ns() - started_ns) / 1_000_000


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def latency_summary(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {"requests": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "mean_ms": 0.0}
    return {
        "requests": len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "mean_ms": statistics.fmean(values),
    }


def linear_slope_per_hour(samples: list[tuple[float, int]], warmup_seconds: float) -> float:
    points = [(seconds, value) for seconds, value in samples if seconds >= warmup_seconds]
    if len(points) < 3:
        return 0.0
    mean_x = statistics.fmean(point[0] for point in points)
    mean_y = statistics.fmean(point[1] for point in points)
    denominator = sum((x - mean_x) ** 2 for x, _ in points)
    if denominator == 0:
        return 0.0
    per_second = sum((x - mean_x) * (y - mean_y) for x, y in points) / denominator
    return per_second * 3_600


def generation_slopes(samples: list[tuple[int, float, int]]) -> dict[str, dict[str, float | int]]:
    grouped: dict[int, list[tuple[float, int]]] = defaultdict(list)
    for generation, elapsed, rss in samples:
        grouped[generation].append((elapsed, rss))
    slopes = {}
    for generation, points in grouped.items():
        first = points[0][0]
        span = points[-1][0] - first
        # Treat every restart as a new allocator lifetime. Otherwise a later
        # low-RSS process could hide growth in an earlier long-lived process.
        warmup = min(span * 0.25, 30 * 60)
        relative = [(elapsed - first, rss) for elapsed, rss in points]
        slopes[str(generation)] = {
            "samples": len(points),
            "span_seconds": span,
            "warmup_seconds": warmup,
            "slope_kib_per_hour": linear_slope_per_hour(relative, warmup),
        }
    return slopes


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def proc_memory(pid: int | None) -> dict[str, int]:
    if pid is None:
        return {}
    values: dict[str, int] = {}
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as status:
            for line in status:
                if line.startswith(("VmRSS:", "VmHWM:")):
                    name, value, _unit = line.split()
                    values[name.rstrip(":").lower() + "_kib"] = int(value)
    except FileNotFoundError:
        pass
    return values


@dataclasses.dataclass
class HttpResult:
    status: int
    body: bytes
    headers: dict[str, str]
    elapsed_ms: float
    request_bytes: int

    def json(self) -> Any:
        return json.loads(self.body)


@dataclasses.dataclass
class Server:
    signal_name: str
    binary: pathlib.Path
    extension: pathlib.Path
    database: pathlib.Path
    port: int
    log_dir: pathlib.Path
    short_maintenance: bool = False
    process: subprocess.Popen[bytes] | None = None
    generation: int = 0
    epoch_stats: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    memory_hwm_kib: int = 0
    log_handle: Any = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.process is not None and self.process.poll() is None else None

    def environment(self) -> dict[str, str]:
        environment = os.environ.copy()
        environment["TIMELESS_AUTH_MODE"] = "disabled"
        if self.short_maintenance:
            if self.signal_name == "metrics":
                environment.update({
                    "TIMELESS_METRICS_FLUSH_INTERVAL_SECS": "2",
                    "TIMELESS_METRICS_COMPACT_INTERVAL_SECS": "5",
                    "TIMELESS_METRICS_RETENTION_INTERVAL_SECS": "15",
                })
            elif self.signal_name == "logs":
                environment.update({
                    "TIMELESS_LOGS_FLUSH_INTERVAL_SECS": "1",
                    "TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS": "5",
                })
            else:
                environment.update({
                    "TIMELESS_TRACES_FLUSH_INTERVAL_SECS": "1",
                    "TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS": "5",
                })
        return environment

    def start(self, *, preexec_fn: Any = None, timeout: float = 30.0) -> None:
        if self.process is not None and self.process.poll() is None:
            raise GateFailure(f"{self.signal_name} server is already running")
        self.generation += 1
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.log_dir / f"{self.signal_name}-{self.port}-g{self.generation}.log"
        self.log_handle = log_path.open("ab", buffering=0)
        self.process = subprocess.Popen(
            [str(self.binary), str(self.extension), str(self.database), f"127.0.0.1:{self.port}"],
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
            env=self.environment(),
            preexec_fn=preexec_fn,
        )
        deadline = time.monotonic() + timeout
        last_error = "not contacted"
        while time.monotonic() < deadline:
            code = self.process.poll()
            if code is not None:
                self._close_log()
                tail = log_path.read_text(errors="replace")[-4_000:]
                raise GateFailure(
                    f"{self.signal_name} exited during startup with {code}: {tail}"
                )
            try:
                result = http_request(self, "GET", "/ready", timeout=1.0)
                if result.status == 200:
                    return
                last_error = f"HTTP {result.status}: {result.body!r}"
            except Exception as error:  # readiness is expected to race startup
                last_error = repr(error)
            time.sleep(0.05)
        self.kill()
        raise GateFailure(f"{self.signal_name} readiness timeout: {last_error}")

    def snapshot_epoch(self) -> None:
        if self.pid is None:
            return
        with contextlib.suppress(Exception):
            self.epoch_stats.append(stats(self))
        memory = proc_memory(self.pid)
        self.memory_hwm_kib = max(self.memory_hwm_kib, memory.get("vmhwm_kib", 0))

    def stop(self, timeout: float = 30.0) -> None:
        if self.process is None:
            return
        process = self.process
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired as error:
                process.kill()
                process.wait(timeout=10)
                self._close_log()
                raise GateFailure(f"{self.signal_name} did not gracefully stop") from error
            if code != 0:
                self._close_log()
                raise GateFailure(f"{self.signal_name} graceful stop exited {code}")
        self._close_log()

    def kill(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self._close_log()

    def _close_log(self) -> None:
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None


@dataclasses.dataclass
class SignalState:
    server: Server
    batch: int
    accepted: int = 0
    next_ordinal: int = 0
    write_latencies: list[float] = dataclasses.field(default_factory=list)
    query_latencies: dict[str, list[float]] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    query_response_bytes_hwm: dict[str, int] = dataclasses.field(
        default_factory=lambda: defaultdict(int)
    )
    query_result_rows_hwm: dict[str, int] = dataclasses.field(
        default_factory=lambda: defaultdict(int)
    )
    ingest_body_bytes_hwm: int = 0
    errors: list[str] = dataclasses.field(default_factory=list)
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    state_lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    rss_samples: list[tuple[int, float, int]] = dataclasses.field(default_factory=list)
    resource_samples: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    max_watermarks: dict[str, int] = dataclasses.field(default_factory=lambda: defaultdict(int))

    @property
    def signal_name(self) -> str:
        return self.server.signal_name


def http_request(
    server: Server,
    method: str,
    path: str,
    body: bytes | str | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 60.0,
) -> HttpResult:
    payload = body.encode() if isinstance(body, str) else body
    started = monotonic_ns()
    connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=timeout)
    try:
        connection.request(method, path, body=payload, headers=headers or {})
        response = connection.getresponse()
        response_body = response.read()
        return HttpResult(
            response.status,
            response_body,
            {key.lower(): value for key, value in response.getheaders()},
            elapsed_ms(started),
            len(payload or b""),
        )
    finally:
        connection.close()


def require_status(result: HttpResult, expected: int, operation: str) -> HttpResult:
    if result.status != expected:
        raise GateFailure(f"{operation}: HTTP {result.status}: {result.body[:2_000]!r}")
    return result


def request_json(
    server: Server,
    method: str,
    path: str,
    body: bytes | str | None = None,
    headers: dict[str, str] | None = None,
    expected: int = 200,
) -> tuple[Any, float]:
    result = require_status(http_request(server, method, path, body, headers), expected, path)
    try:
        return result.json(), result.elapsed_ms
    except json.JSONDecodeError as error:
        raise GateFailure(f"{path}: incomplete/non-JSON response: {result.body[:2_000]!r}") from error


def stats(server: Server) -> dict[str, Any]:
    value, _latency = request_json(server, "GET", STATS_PATHS[server.signal_name])
    if not isinstance(value, dict):
        raise GateFailure(f"{server.signal_name} stats is not an object")
    return value


def flush(server: Server) -> dict[str, Any]:
    value, _latency = request_json(server, FLUSH_METHODS[server.signal_name], "/api/v1/flush")
    return value


def metrics_body(start: int, count: int) -> bytes:
    grouped: dict[int, tuple[list[float], list[int]]] = {
        host: ([], []) for host in range(4)
    }
    for ordinal in range(start, start + count):
        values, timestamps = grouped[ordinal % 4]
        values.append(float(ordinal) + 0.5)
        timestamps.append(BASE_MILLISECONDS + ordinal * 1_000 // ORDINALS_PER_SECOND)
    lines = []
    for host, (values, timestamps) in grouped.items():
        if values:
            lines.append(json.dumps({
                "metric": {"__name__": "release_gate_metric", "host": f"host-{host}"},
                "values": values,
                "timestamps": timestamps,
            }, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode()


def logs_body(start: int, count: int) -> bytes:
    levels = ("debug", "info", "warning", "error")
    lines = []
    for ordinal in range(start, start + count):
        lines.append(json.dumps({
            "_time": BASE_SECONDS + ordinal // ORDINALS_PER_SECOND,
            "_msg": f"release-gate-{ordinal}",
            "level": levels[ordinal % len(levels)],
            "service": "release-gate",
            "host": f"host-{ordinal % 4}",
            "status": 500 if ordinal % 4 == 3 else 200,
            "attempt": ordinal,
            "sampled": ordinal % 2 == 0,
            "nested": {"worker": ordinal % 8},
            "tags": ["soak", f"lane-{ordinal % 4}"],
        }, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode()


def traces_body(start: int, count: int) -> bytes:
    spans = []
    for ordinal in range(start, start + count):
        trace_number = ordinal // 4 + 1
        root_ordinal = (ordinal // 4) * 4
        start_ns = BASE_NANOSECONDS + ordinal * 1_000_000_000 // ORDINALS_PER_SECOND
        spans.append({
            "traceId": f"{trace_number:032x}",
            "spanId": f"{ordinal + 1:016x}",
            "parentSpanId": "" if ordinal % 4 == 0 else f"{root_ordinal + 1:016x}",
            "name": "GET /release-gate" if ordinal % 4 == 0 else "db.query",
            "kind": 2 if ordinal % 4 == 0 else 3,
            "startTimeUnixNano": str(start_ns),
            "endTimeUnixNano": str(start_ns + 500_000 + ordinal % 17),
            "status": {"code": 2 if ordinal % 17 == 0 else 1, "message": "gate"},
            "attributes": [
                {"key": "gate.ordinal", "value": {"intValue": str(ordinal)}},
                {"key": "gate.bool", "value": {"boolValue": ordinal % 2 == 0}},
                {"key": "http.method", "value": {"stringValue": "GET"}},
            ],
            "events": [{
                "timeUnixNano": str(start_ns + 100),
                "name": "gate.event",
                "attributes": [{"key": "event.ordinal", "value": {"intValue": str(ordinal)}}],
            }],
            "links": [],
        })
    return json.dumps({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "release-gate"}},
                {"key": "deployment.environment", "value": {"stringValue": "soak"}},
            ]},
            "scopeSpans": [{
                "scope": {"name": "production-gate", "version": "1"},
                "spans": spans,
            }],
        }],
    }, separators=(",", ":")).encode()


def ingest_result(server: Server, start: int, count: int) -> HttpResult:
    if server.signal_name == "metrics":
        return http_request(
            server, "POST", "/api/v1/import", metrics_body(start, count),
            {"content-type": "application/x-ndjson"},
        )
    if server.signal_name == "logs":
        return http_request(
            server, "POST", "/insert/jsonline", logs_body(start, count),
            {"content-type": "application/x-ndjson"},
        )
    return http_request(
        server, "POST", "/insert/opentelemetry/v1/traces", traces_body(start, count),
        {"content-type": "application/json"},
    )


def expected_ingest_status(signal_name: str) -> int:
    return 204 if signal_name in ("metrics", "logs") else 200


def write_once(state: SignalState) -> None:
    with state.state_lock:
        start = state.next_ordinal
    result = ingest_result(state.server, start, state.batch)
    require_status(result, expected_ingest_status(state.signal_name), f"{state.signal_name} ingest")
    with state.state_lock:
        if start != state.next_ordinal:
            raise GateFailure(f"{state.signal_name} writer ordinal raced")
        state.next_ordinal += state.batch
        state.accepted += state.batch
        state.write_latencies.append(result.elapsed_ms)
        state.ingest_body_bytes_hwm = max(state.ingest_body_bytes_hwm, result.request_bytes)


def validate_ndjson(result: HttpResult, operation: str) -> list[Any]:
    rows = []
    try:
        for line in result.body.splitlines():
            if line:
                rows.append(json.loads(line))
    except json.JSONDecodeError as error:
        raise GateFailure(f"{operation}: partial NDJSON response") from error
    declared = result.headers.get("x-timeless-result-rows")
    if declared is not None and int(declared) != len(rows):
        raise GateFailure(f"{operation}: declared {declared} rows, decoded {len(rows)}")
    return rows


def result_rows(result: HttpResult) -> int:
    declared = result.headers.get("x-timeless-result-rows")
    return int(declared) if declared is not None else 0


def metrics_query(state: SignalState, shape: str) -> tuple[float, int, int]:
    with state.state_lock:
        newest = max(0, state.next_ordinal - 1)
    newest_seconds = BASE_SECONDS + newest // ORDINALS_PER_SECOND
    from_seconds = max(BASE_SECONDS, newest_seconds - 300)
    paths = {
        "exact_latest": "/api/v1/query?metric=release_gate_metric&host=host-0",
        "narrow_range": (
            "/api/v1/query_range?metric=release_gate_metric&host=host-0"
            f"&from={from_seconds}&to={newest_seconds}&step=10&aggregate=avg"
        ),
        "wide_range": (
            "/api/v1/query_range?metric=release_gate_metric"
            f"&from={max(BASE_SECONDS, newest_seconds - 60)}&to={newest_seconds}"
            "&step=10&aggregate=avg"
        ),
        "scalar_avg": (
            "/api/v1/query_range?metric=release_gate_metric&host=host-1"
            f"&from={from_seconds}&to={newest_seconds}&step=300&aggregate=avg"
        ),
        "discovery": "/api/v1/label/host/values?metric=release_gate_metric",
    }
    result = require_status(http_request(state.server, "GET", paths[shape]), 200, shape)
    try:
        json.loads(result.body)
    except json.JSONDecodeError as error:
        raise GateFailure(f"metrics {shape}: partial JSON") from error
    return result.elapsed_ms, len(result.body), result_rows(result)


def logs_query(state: SignalState, shape: str) -> tuple[float, int, int]:
    with state.state_lock:
        newest = max(0, state.next_ordinal - 1)
    newest_seconds = BASE_SECONDS + newest // ORDINALS_PER_SECOND
    paths = {
        "exact": "/select/logsql/query?message=release-gate-0&limit=1&order=asc",
        "narrow": "/select/logsql/query?level=error&service=release-gate&limit=100&order=desc",
        "wide": (
            "/select/logsql/query?service=release-gate&limit=1000&order=desc"
            f"&start={max(BASE_SECONDS, newest_seconds - 4000)}&end={newest_seconds}"
        ),
        "discovery": "/select/logsql/field_values?field=host&service=release-gate&limit=10",
    }
    if shape == "scalar_count":
        result = require_status(
            http_request(
                state.server,
                "POST",
                "/select/logsql/query",
                "query=level%3Aerror+%7C+stats+count%28*%29",
                {"content-type": "application/x-www-form-urlencoded"},
            ),
            200,
            shape,
        )
    else:
        result = require_status(http_request(state.server, "GET", paths[shape]), 200, shape)
    if shape == "discovery":
        json.loads(result.body)
        rows = result_rows(result)
    else:
        rows = len(validate_ndjson(result, f"logs {shape}"))
    return result.elapsed_ms, len(result.body), rows


def traces_query(state: SignalState, shape: str) -> tuple[float, int, int]:
    trace_one = f"{1:032x}"
    paths = {
        "exact_trace": f"/select/jaeger/api/traces/{trace_one}",
        "narrow_search": (
            "/select/jaeger/api/traces?service=release-gate"
            "&operation=GET%20%2Frelease-gate&limit=20"
        ),
        "wide_search": "/select/jaeger/api/traces?service=release-gate&limit=100",
        "operations": "/select/jaeger/api/services/release-gate/operations",
        "services": "/select/jaeger/api/services",
    }
    result = require_status(http_request(state.server, "GET", paths[shape]), 200, shape)
    try:
        value = json.loads(result.body)
    except json.JSONDecodeError as error:
        raise GateFailure(f"traces {shape}: partial JSON") from error
    if not isinstance(value, dict) or "data" not in value:
        raise GateFailure(f"traces {shape}: invalid response shape")
    return result.elapsed_ms, len(result.body), result_rows(result)


def query_once(state: SignalState, shape: str) -> None:
    if state.signal_name == "metrics":
        latency, response_bytes, rows = metrics_query(state, shape)
    elif state.signal_name == "logs":
        latency, response_bytes, rows = logs_query(state, shape)
    else:
        latency, response_bytes, rows = traces_query(state, shape)
    with state.state_lock:
        state.query_latencies[shape].append(latency)
        state.query_response_bytes_hwm[shape] = max(
            state.query_response_bytes_hwm[shape], response_bytes
        )
        state.query_result_rows_hwm[shape] = max(state.query_result_rows_hwm[shape], rows)


def semantic_oracle(server: Server) -> None:
    if server.signal_name == "metrics":
        result = require_status(
            http_request(
                server,
                "GET",
                f"/api/v1/export?metric=release_gate_metric&host=host-0&from={BASE_SECONDS}&to={BASE_SECONDS}",
            ),
            200,
            "metrics sentinel",
        )
        try:
            rows = [json.loads(line) for line in result.body.splitlines() if line]
        except json.JSONDecodeError as error:
            raise GateFailure("metrics sentinel returned partial JSON lines") from error
        values = rows[0].get("values", []) if len(rows) == 1 else []
        timestamps = rows[0].get("timestamps", []) if len(rows) == 1 else []
        # Metrics are native epoch seconds, so several high-rate samples can
        # share the sentinel second. Preserve multiplicity and require the
        # first bit-exact sentinel rather than pretending one JSON line equals
        # one result row (the result header counts points).
        if (
            len(rows) != 1
            or 0.5 not in values
            or BASE_MILLISECONDS not in timestamps
            or len(values) != len(timestamps)
        ):
            raise GateFailure(f"metrics sentinel changed: {rows!r}")
    elif server.signal_name == "logs":
        result = require_status(
            http_request(server, "GET", "/select/logsql/query?message=release-gate-0&limit=2&order=asc"),
            200,
            "logs sentinel",
        )
        rows = validate_ndjson(result, "logs sentinel")
        if len(rows) != 1 or rows[0].get("_msg") != "release-gate-0":
            raise GateFailure(f"logs sentinel changed: {rows!r}")
        if rows[0].get("attempt") != 0 or rows[0].get("nested") != {"worker": 0}:
            raise GateFailure(f"logs rich metadata changed: {rows!r}")
    else:
        trace_id = f"{1:032x}"
        value, _latency = request_json(server, "GET", f"/select/jaeger/api/traces/{trace_id}")
        traces = value.get("data", [])
        if len(traces) != 1 or len(traces[0].get("spans", [])) != 4:
            raise GateFailure(f"trace relationship sentinel changed: {value!r}")
        spans = traces[0]["spans"]
        roots = [span for span in spans if not span.get("references")]
        if len(roots) != 1:
            raise GateFailure(f"trace parent relationships changed: {spans!r}")


def writer_loop(state: SignalState, active: threading.Event, stop: threading.Event, interval: float) -> None:
    deadline = time.monotonic()
    while not stop.is_set():
        if not active.wait(timeout=0.1):
            continue
        deadline += interval
        try:
            with state.lock:
                if active.is_set() and not stop.is_set():
                    write_once(state)
        except Exception as error:
            with state.state_lock:
                state.errors.append(f"writer: {error!r}")
            stop.set()
            return
        stop.wait(max(0.0, deadline - time.monotonic()))


def query_loop(state: SignalState, active: threading.Event, stop: threading.Event, interval: float) -> None:
    shapes = QUERY_SHAPES[state.signal_name]
    number = 0
    deadline = time.monotonic()
    while not stop.is_set():
        if not active.wait(timeout=0.1):
            continue
        deadline += interval
        shape = shapes[number % len(shapes)]
        number += 1
        try:
            with state.lock:
                if active.is_set() and not stop.is_set():
                    query_once(state, shape)
        except Exception as error:
            with state.state_lock:
                state.errors.append(f"query {shape}: {error!r}")
            stop.set()
            return
        stop.wait(max(0.0, deadline - time.monotonic()))


def sample_state(state: SignalState, elapsed: float) -> None:
    with state.lock:
        current = stats(state.server)
        memory = proc_memory(state.server.pid)
    rss = memory.get("vmrss_kib", 0)
    hwm = memory.get("vmhwm_kib", 0)
    state.server.memory_hwm_kib = max(state.server.memory_hwm_kib, hwm)
    state.rss_samples.append((state.server.generation, elapsed, rss))
    keys = (
        COUNT_FIELDS[state.signal_name],
        "database_file_bytes",
        "database_wal_bytes",
        "database_shm_bytes",
        "physical_database_bytes",
        "bytes_on_disk",
        "disk_size",
        "freelist_bytes",
        "buffer_memory_bytes",
        "buffered_points",
        "buffered_entries",
        "buffered_spans",
        "queued_batches",
        "queued_requests",
        "queued_points",
        "queued_entries",
        "queued_spans",
        "queued_body_bytes",
        "in_flight_batches",
        "in_flight_requests",
        "command_queue_capacity_batches",
        "command_queue_capacity_requests",
        "query_snapshot_payload_max_bytes",
        "extension_query_snapshot_payload_max_bytes",
    )
    resource_sample = {"elapsed_seconds": elapsed, "rss_kib": rss, "hwm_kib": hwm}
    for key in keys:
        value = current.get(key)
        if isinstance(value, (int, float)):
            resource_sample[key] = value
            state.max_watermarks[key] = max(state.max_watermarks[key], int(value))
    state.resource_samples.append(resource_sample)


def durable_barrier(state: SignalState) -> dict[str, Any]:
    with state.lock:
        report = flush(state.server)
        current = stats(state.server)
        with state.state_lock:
            expected = state.accepted
        actual = current[COUNT_FIELDS[state.signal_name]]
        if actual != expected:
            raise GateFailure(
                f"{state.signal_name} durable count mismatch: expected {expected}, got {actual}"
            )
        queue_fields = (
            "queued_batches", "queued_requests", "in_flight_batches", "in_flight_requests"
        )
        for field in queue_fields:
            if current.get(field, 0) != 0:
                raise GateFailure(f"{state.signal_name} did not drain {field}: {current[field]}")
        semantic_oracle(state.server)
        return {"flush": report, "stats": current}


def restart_all(
    states: dict[str, SignalState], active: threading.Event, abrupt: bool, events: list[dict[str, Any]], elapsed: float
) -> None:
    active.clear()
    try:
        for signal_name in SIGNALS:
            state = states[signal_name]
            with state.lock:
                before = durable_barrier_unlocked(state)
                state.server.epoch_stats.append(before["stats"])
                if abrupt:
                    state.server.kill()
                else:
                    state.server.stop()
                state.server.start()
                reopened = stats(state.server)
                expected = state.accepted
                actual = reopened[COUNT_FIELDS[signal_name]]
                if actual != expected:
                    raise GateFailure(
                        f"{signal_name} cold restart count mismatch: expected {expected}, got {actual}"
                    )
                semantic_oracle(state.server)
        events.append({
            "elapsed_seconds": elapsed,
            "fault": "sigkill_restart" if abrupt else "graceful_restart",
            "result": "passed",
        })
    finally:
        active.set()


def durable_barrier_unlocked(state: SignalState) -> dict[str, Any]:
    report = flush(state.server)
    current = stats(state.server)
    expected = state.accepted
    actual = current[COUNT_FIELDS[state.signal_name]]
    if actual != expected:
        raise GateFailure(
            f"{state.signal_name} durable count mismatch: expected {expected}, got {actual}"
        )
    semantic_oracle(state.server)
    return {"flush": report, "stats": current}


def slow_and_cancel_storm(states: dict[str, SignalState], events: list[dict[str, Any]], elapsed: float) -> None:
    sockets: list[socket.socket] = []
    for state in states.values():
        server = state.server
        for _ in range(8):
            connection = socket.create_connection(("127.0.0.1", server.port), timeout=2)
            connection.sendall(
                b"POST /insert/jsonline HTTP/1.1\r\nHost: localhost\r\n"
                b"Content-Length: 1000000\r\nContent-Type: application/x-ndjson\r\n\r\n{}"
            )
            sockets.append(connection)
        cancellation_path = {
            "metrics": "/api/v1/query_range?metric=release_gate_metric&from=2000000000&to=2100000000&step=1&aggregate=p95",
            "logs": "/select/logsql/query?service=release-gate&limit=10000&order=desc",
            "traces": "/select/jaeger/api/traces?service=release-gate&limit=10000",
        }[state.signal_name]
        for _ in range(16):
            connection = socket.create_connection(("127.0.0.1", server.port), timeout=2)
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            request = f"GET {cancellation_path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            connection.sendall(request.encode())
            connection.close()
    time.sleep(0.25)
    for connection in sockets:
        with contextlib.suppress(OSError):
            connection.close()
    time.sleep(0.25)
    for state in states.values():
        require_status(http_request(state.server, "GET", "/live"), 200, "post-cancellation liveness")
    events.append({"elapsed_seconds": elapsed, "fault": "slow_disconnect_cancellation_storm", "result": "passed"})


def address_conflict_probe(state: SignalState, root: pathlib.Path) -> None:
    database = root / f"address-conflict-{state.signal_name}.db"
    environment = state.server.environment()
    completed = subprocess.run(
        [
            str(state.server.binary),
            str(state.server.extension),
            str(database),
            f"127.0.0.1:{state.server.port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=environment,
        timeout=30,
        check=False,
    )
    output = completed.stdout.decode(errors="replace")
    if completed.returncode == 0 or "bind" not in output.lower():
        raise GateFailure(
            f"{state.signal_name} address conflict did not fail closed: code={completed.returncode}, output={output!r}"
        )


def backup_overlap_probe(
    state: SignalState, root: pathlib.Path, events: list[dict[str, Any]], elapsed: float
) -> None:
    destination = (root / f"overlap-{state.signal_name}-{len(events)}.db").resolve()
    with state.state_lock:
        before = state.accepted
    result = require_status(
        http_request(
            state.server,
            "POST",
            "/api/v1/backup",
            json.dumps({"destination": str(destination)}),
            {"content-type": "application/json"},
            timeout=180,
        ),
        200,
        f"{state.signal_name} overlap backup",
    )
    report = result.json()
    with state.state_lock:
        after = state.accepted
    digest = sha256(destination)
    refused = http_request(
        state.server,
        "POST",
        "/api/v1/backup",
        json.dumps({"destination": str(destination)}),
        {"content-type": "application/json"},
        timeout=180,
    )
    if refused.status != 500 or sha256(destination) != digest:
        raise GateFailure(f"{state.signal_name} backup no-clobber contract failed")

    cold = Server(
        state.signal_name,
        state.server.binary,
        state.server.extension,
        destination,
        free_port(),
        root / "cold-backup-logs",
        state.server.short_maintenance,
    )
    cold.start()
    try:
        cold_stats = stats(cold)
        copied = cold_stats[COUNT_FIELDS[state.signal_name]]
        if not before <= copied <= after:
            raise GateFailure(
                f"{state.signal_name} overlap snapshot {copied} outside admission window [{before}, {after}]"
            )
        semantic_oracle(cold)
    finally:
        cold.stop()
    events.append({
        "elapsed_seconds": elapsed,
        "fault": f"{state.signal_name}_backup_overlap",
        "result": "passed",
        "accepted_before": before,
        "accepted_after": after,
        "snapshot_count": copied,
        "sha256": digest,
        "report": report,
    })


def invalid_storage_probes(state: SignalState, root: pathlib.Path) -> None:
    source = (root / f"probe-source-{state.signal_name}.db").resolve()
    require_status(
        http_request(
            state.server,
            "POST",
            "/api/v1/backup",
            json.dumps({"destination": str(source)}),
            {"content-type": "application/json"},
            timeout=180,
        ),
        200,
        "probe backup",
    )

    corrupt = root / f"corrupt-{state.signal_name}.db"
    shutil.copy2(source, corrupt)
    with corrupt.open("r+b") as database:
        database.seek(0)
        database.write(b"not-a-sqlite-db!")
        database.flush()
        os.fsync(database.fileno())
    expect_start_failure(state, corrupt, root, "corrupt")

    readonly_root = root / f"readonly-{state.signal_name}"
    readonly_root.mkdir()
    readonly = readonly_root / "telemetry.db"
    shutil.copy2(source, readonly)
    readonly.chmod(0o444)
    readonly_root.chmod(0o555)
    try:
        expect_start_failure(state, readonly, root, "read-only")
    finally:
        readonly_root.chmod(0o755)
        readonly.chmod(0o644)


def expect_start_failure(state: SignalState, database: pathlib.Path, root: pathlib.Path, label: str) -> None:
    probe = Server(
        state.signal_name,
        state.server.binary,
        state.server.extension,
        database,
        free_port(),
        root / f"{label}-logs",
        state.server.short_maintenance,
    )
    try:
        probe.start(timeout=8)
    except GateFailure:
        return
    else:
        probe.kill()
        raise GateFailure(f"{state.signal_name} {label} storage unexpectedly became ready")


def descriptor_pressure_probe(state: SignalState, root: pathlib.Path) -> None:
    database = root / f"descriptor-{state.signal_name}.db"

    def limit_descriptors() -> None:
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))

    probe = Server(
        state.signal_name,
        state.server.binary,
        state.server.extension,
        database,
        free_port(),
        root / "descriptor-logs",
        state.server.short_maintenance,
    )
    probe.start(preexec_fn=limit_descriptors)
    connections: list[socket.socket] = []
    try:
        for _ in range(96):
            try:
                connection = socket.create_connection(("127.0.0.1", probe.port), timeout=0.2)
                connections.append(connection)
            except OSError:
                break
        for connection in connections:
            connection.close()
        connections.clear()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if probe.process is not None and probe.process.poll() is not None:
                raise GateFailure(f"{state.signal_name} exited under descriptor pressure")
            with contextlib.suppress(Exception):
                if http_request(probe, "GET", "/ready", timeout=1).status == 200:
                    break
            time.sleep(0.1)
        else:
            raise GateFailure(f"{state.signal_name} did not recover from descriptor pressure")
    finally:
        for connection in connections:
            connection.close()
        probe.stop()


def disk_full_probe(state: SignalState, root: pathlib.Path) -> dict[str, Any]:
    database = root / f"disk-full-{state.signal_name}.db"
    limit_bytes = 1_048_576

    def limit_file_size() -> None:
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (limit_bytes, limit_bytes))

    probe = Server(
        state.signal_name,
        state.server.binary,
        state.server.extension,
        database,
        free_port(),
        root / "disk-full-logs",
        state.server.short_maintenance,
    )
    probe.start(preexec_fn=limit_file_size)
    batch = {"metrics": 4096, "logs": 2048, "traces": 1024}[state.signal_name]
    accepted = 0
    durable = 0
    failure = ""
    try:
        for _cycle in range(32):
            if probe.process is None or probe.process.poll() is not None:
                failure = f"process_exit:{probe.process.returncode if probe.process else 'missing'}"
                break
            result = ingest_result(probe, accepted, batch)
            if result.status != expected_ingest_status(state.signal_name):
                failure = f"ingest_http_{result.status}:{result.body[:500]!r}"
                break
            accepted += batch
            barrier = http_request(probe, FLUSH_METHODS[state.signal_name], "/api/v1/flush", timeout=60)
            if barrier.status != 200:
                failure = f"flush_http_{barrier.status}:{barrier.body[:500]!r}"
                break
            current = stats(probe)
            if current[COUNT_FIELDS[state.signal_name]] != accepted:
                raise GateFailure(f"{state.signal_name} disk-full prefix mismatch before fault")
            durable = accepted
        if not failure:
            raise GateFailure(f"{state.signal_name} did not reach the 1 MiB file-size fault")
    finally:
        probe.kill()

    reopened = Server(
        state.signal_name,
        state.server.binary,
        state.server.extension,
        database,
        free_port(),
        root / "disk-full-reopen-logs",
        state.server.short_maintenance,
    )
    reopened.start()
    try:
        current = stats(reopened)
        recovered = current[COUNT_FIELDS[state.signal_name]]
        if not durable <= recovered <= accepted:
            raise GateFailure(
                f"{state.signal_name} disk-full recovery {recovered} outside [{durable}, {accepted}]"
            )
    finally:
        reopened.stop()
    return {
        "limit_bytes": limit_bytes,
        "accepted_before_failure": accepted,
        "durable_before_failure": durable,
        "recovered": recovered,
        "failure": failure,
    }


def initial_fault_matrix(
    states: dict[str, SignalState], root: pathlib.Path, events: list[dict[str, Any]]
) -> None:
    for state in states.values():
        address_conflict_probe(state, root)
        invalid_storage_probes(state, root)
        descriptor_pressure_probe(state, root)
        disk = disk_full_probe(state, root)
        events.append({
            "elapsed_seconds": 0.0,
            "fault": f"{state.signal_name}_startup_descriptor_disk_faults",
            "result": "passed",
            "disk_full": disk,
        })


def aggregate_counters(server: Server, final_stats: dict[str, Any]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for key in COUNTER_FIELDS:
        total = 0
        for epoch in [*server.epoch_stats, final_stats]:
            value = epoch.get(key, 0)
            if isinstance(value, int):
                total += value
        if total:
            totals[key] = total
    return totals


def result_for_state(
    state: SignalState,
    final: dict[str, Any],
    duration_seconds: float,
) -> dict[str, Any]:
    with state.state_lock:
        accepted = state.accepted
        writes = list(state.write_latencies)
        queries = {key: list(value) for key, value in state.query_latencies.items()}
        query_response_bytes_hwm = dict(state.query_response_bytes_hwm)
        query_result_rows_hwm = dict(state.query_result_rows_hwm)
        ingest_body_bytes_hwm = state.ingest_body_bytes_hwm
        errors = list(state.errors)
    physical = final.get("physical_database_bytes", final.get("disk_size", 0))
    logical = final.get("bytes_on_disk", final.get("total_bytes", 0))
    slopes = generation_slopes(state.rss_samples)
    long_running_slopes = [
        generation["slope_kib_per_hour"]
        for generation in slopes.values()
        if generation["span_seconds"] >= 2 * 60 * 60
    ]
    return {
        "accepted_and_durable_records": accepted,
        "durable_records_per_second": accepted / duration_seconds,
        "write_latency": latency_summary(writes),
        "query_latency": {key: latency_summary(value) for key, value in sorted(queries.items())},
        "body_and_result_watermarks": {
            "ingest_body_bytes_hwm": ingest_body_bytes_hwm,
            "query_response_bytes_hwm": dict(sorted(query_response_bytes_hwm.items())),
            "query_result_rows_hwm": dict(sorted(query_result_rows_hwm.items())),
        },
        "rss_hwm_kib": state.server.memory_hwm_kib,
        "rss_slope_kib_per_hour_after_warmup": max(long_running_slopes, default=0.0),
        "rss_slope_kib_per_hour_by_process_generation": slopes,
        "logical_storage_bytes": logical,
        "physical_storage_bytes": physical,
        "physical_bytes_per_record": physical / accepted if accepted else 0.0,
        "wal_hwm_bytes": state.max_watermarks.get("database_wal_bytes", 0),
        "resource_watermarks": dict(sorted(state.max_watermarks.items())),
        "maintenance_and_fault_counters": aggregate_counters(state.server, final),
        "process_generations": state.server.generation,
        "errors": errors,
        "final_stats": final,
        "samples": state.resource_samples,
    }


def enforce_gates(args: argparse.Namespace, report: dict[str, Any]) -> None:
    failures = []
    for signal_name, result in report["signals"].items():
        if result["errors"]:
            failures.append(f"{signal_name}: workload errors: {result['errors'][:3]!r}")
        if result["accepted_and_durable_records"] <= 0:
            failures.append(f"{signal_name}: no durable work")
        if result["wal_hwm_bytes"] > args.max_wal_bytes:
            failures.append(
                f"{signal_name}: WAL HWM {result['wal_hwm_bytes']} > {args.max_wal_bytes}"
            )
        if result["rss_hwm_kib"] > args.max_rss_kib[signal_name]:
            failures.append(
                f"{signal_name}: RSS HWM {result['rss_hwm_kib']} KiB > {args.max_rss_kib[signal_name]} KiB"
            )
        if args.duration_seconds >= 7_200 and result["rss_slope_kib_per_hour_after_warmup"] > args.max_rss_slope_kib_hour:
            failures.append(
                f"{signal_name}: RSS slope {result['rss_slope_kib_per_hour_after_warmup']:.1f} KiB/h > "
                f"{args.max_rss_slope_kib_hour:.1f} KiB/h"
            )
        for shape, latency in result["query_latency"].items():
            if latency["requests"] == 0:
                failures.append(f"{signal_name}/{shape}: no queries")
            elif latency["p99_ms"] > args.max_p99_ms:
                failures.append(
                    f"{signal_name}/{shape}: p99 {latency['p99_ms']:.2f} ms > {args.max_p99_ms:.2f} ms"
                )
            if result["body_and_result_watermarks"]["query_result_rows_hwm"].get(shape, 0) == 0:
                failures.append(f"{signal_name}/{shape}: every completed query returned zero rows")
        final = result["final_stats"]
        capacity = final.get(
            "command_queue_capacity_batches",
            final.get("command_queue_capacity_requests", 0),
        )
        queue_key = "queued_requests" if signal_name == "traces" else "queued_batches"
        queued_hwm = result["resource_watermarks"].get(queue_key, 0)
        if capacity and queued_hwm > capacity:
            failures.append(
                f"{signal_name}: {queue_key} HWM {queued_hwm} exceeds configured capacity {capacity}"
            )
        for key in ("queued_batches", "queued_requests", "in_flight_batches", "in_flight_requests"):
            if final.get(key, 0):
                failures.append(f"{signal_name}: final {key}={final[key]}")
        counters = result["maintenance_and_fault_counters"]
        expected_backup_errors = 0 if args.skip_faults else 1
        if counters.get("backup_errors", 0) != expected_backup_errors:
            failures.append(
                f"{signal_name}: expected {expected_backup_errors} observed no-clobber backup errors, "
                f"got {counters.get('backup_errors', 0)}"
            )
        for key, value in counters.items():
            if key != "backup_errors" and (key.endswith("_errors") or key.endswith("_timeouts")):
                if value:
                    failures.append(f"{signal_name}: {key}={value}")
    if failures:
        report["verdict"] = "failed"
        report["failures"] = failures
        raise GateFailure("; ".join(failures))
    report["verdict"] = "passed"
    report["failures"] = []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("short", "release"), default="short")
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--sample-seconds", type=float)
    parser.add_argument("--write-hz", type=float, default=4.0)
    parser.add_argument("--query-hz", type=float, default=5.0)
    parser.add_argument("--batch", type=int, default=64)
    parser.add_argument("--data-dir", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--extension", type=pathlib.Path, default=pathlib.Path("target/release/libtimeless_ext.so"))
    parser.add_argument("--server-dir", type=pathlib.Path, default=pathlib.Path("servers/target/release"))
    parser.add_argument("--skip-faults", action="store_true")
    parser.add_argument("--max-wal-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--max-p99-ms", type=float, default=10_000.0)
    parser.add_argument("--max-rss-slope-kib-hour", type=float, default=16_384.0)
    parser.add_argument("--max-rss-kib", action="append", default=[])
    args = parser.parse_args()
    if args.duration_seconds is None:
        args.duration_seconds = 120.0 if args.mode == "short" else 8 * 60 * 60.0
    if args.sample_seconds is None:
        args.sample_seconds = 5.0 if args.mode == "short" else 30.0
    if args.duration_seconds <= 0 or args.sample_seconds <= 0 or args.write_hz <= 0 or args.query_hz <= 0:
        parser.error("durations and rates must be positive")
    if args.batch <= 0 or args.batch % 4:
        parser.error("--batch must be positive and divisible by four")
    rss_limits = {"metrics": 512 * 1024, "logs": 512 * 1024, "traces": 768 * 1024}
    for item in args.max_rss_kib:
        try:
            signal_name, value = item.split("=", 1)
            if signal_name not in SIGNALS:
                raise ValueError
            rss_limits[signal_name] = int(value)
        except ValueError:
            parser.error("--max-rss-kib must be SIGNAL=KIB")
    args.max_rss_kib = rss_limits
    return args


def main() -> int:
    args = parse_args()
    args.extension = args.extension.resolve()
    args.server_dir = args.server_dir.resolve()
    args.output = args.output.resolve()
    if not args.extension.is_file():
        raise GateFailure(f"missing extension {args.extension}")
    binaries = {
        signal_name: args.server_dir / f"timeless-{signal_name}-api" for signal_name in SIGNALS
    }
    for binary in binaries.values():
        if not binary.is_file():
            raise GateFailure(f"missing release binary {binary}")

    temporary: tempfile.TemporaryDirectory[str] | None = None
    if args.data_dir is None:
        temporary = tempfile.TemporaryDirectory(prefix="timeless-production-gate-")
        root = pathlib.Path(temporary.name)
    else:
        root = args.data_dir.resolve()
        root.mkdir(parents=True, exist_ok=False)
    log_dir = root / "server-logs"
    states = {
        signal_name: SignalState(
            Server(
                signal_name,
                binaries[signal_name],
                args.extension,
                root / f"{signal_name}.db",
                free_port(),
                log_dir,
                args.mode == "short",
            ),
            args.batch,
        )
        for signal_name in SIGNALS
    }
    events: list[dict[str, Any]] = []
    active = threading.Event()
    active.set()
    stop = threading.Event()
    threads: list[threading.Thread] = []
    started_wall = dt.datetime.now(dt.timezone.utc)
    gate_started = time.monotonic()
    soak_started: float | None = None
    report: dict[str, Any] = {
        "schema": 1,
        "mode": args.mode,
        "started_at": started_wall.isoformat(),
        "configured_duration_seconds": args.duration_seconds,
        "write_hz_per_signal": args.write_hz,
        "query_hz_per_signal": args.query_hz,
        "batch_records": args.batch,
        "limits": {
            "max_wal_bytes": args.max_wal_bytes,
            "max_p99_ms": args.max_p99_ms,
            "max_rss_kib": args.max_rss_kib,
            "max_rss_slope_kib_hour": args.max_rss_slope_kib_hour,
        },
    }
    try:
        for state in states.values():
            state.server.start()
            with state.lock:
                write_once(state)
                durable_barrier_unlocked(state)
        if not args.skip_faults:
            initial_fault_matrix(states, root, events)

        report["preflight_seconds"] = time.monotonic() - gate_started
        report["soak_started_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        soak_started = time.monotonic()

        for state in states.values():
            writer = threading.Thread(
                target=writer_loop,
                args=(state, active, stop, 1.0 / args.write_hz),
                name=f"{state.signal_name}-writer",
            )
            reader = threading.Thread(
                target=query_loop,
                args=(state, active, stop, 1.0 / args.query_hz),
                name=f"{state.signal_name}-reader",
            )
            writer.start()
            reader.start()
            threads.extend((writer, reader))

        fault_schedule = [] if args.skip_faults else [
            (0.12, "slow"),
            (0.22, "backup_metrics"),
            (0.30, "graceful"),
            (0.42, "backup_logs"),
            (0.52, "abrupt"),
            (0.64, "backup_traces"),
            (0.74, "slow"),
            (0.84, "graceful"),
            (0.92, "abrupt"),
        ]
        next_fault = 0
        next_sample = 0.0
        while not stop.is_set():
            elapsed = time.monotonic() - soak_started
            if elapsed >= args.duration_seconds:
                break
            if elapsed >= next_sample:
                for state in states.values():
                    sample_state(state, elapsed)
                next_sample += args.sample_seconds
            if next_fault < len(fault_schedule) and elapsed >= args.duration_seconds * fault_schedule[next_fault][0]:
                fault = fault_schedule[next_fault][1]
                if fault == "slow":
                    slow_and_cancel_storm(states, events, elapsed)
                elif fault == "graceful":
                    restart_all(states, active, False, events, elapsed)
                elif fault == "abrupt":
                    restart_all(states, active, True, events, elapsed)
                else:
                    signal_name = fault.split("_", 1)[1]
                    backup_overlap_probe(states[signal_name], root, events, elapsed)
                next_fault += 1
            stop.wait(min(0.1, max(0.0, args.duration_seconds - elapsed)))

        stop.set()
        active.set()
        for thread in threads:
            thread.join(timeout=120)
            if thread.is_alive():
                raise GateFailure(f"worker {thread.name} did not stop")
        elapsed = time.monotonic() - soak_started
        finals = {}
        barriers = {}
        for signal_name, state in states.items():
            barriers[signal_name] = durable_barrier(state)
            finals[signal_name] = barriers[signal_name]["stats"]
            sample_state(state, elapsed)
        report.update({
            "elapsed_seconds": elapsed,
            "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "faults": events,
            "final_barriers": {key: value["flush"] for key, value in barriers.items()},
            "signals": {
                signal_name: result_for_state(state, finals[signal_name], elapsed)
                for signal_name, state in states.items()
            },
        })
        enforce_gates(args, report)
        return_code = 0
    except Exception as error:
        stop.set()
        active.set()
        report.update({
            "verdict": "failed",
            "fatal_error": repr(error),
            "elapsed_seconds": time.monotonic() - (soak_started or gate_started),
            "preflight_seconds": report.get("preflight_seconds", time.monotonic() - gate_started),
            "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "faults": events,
        })
        return_code = 1
    finally:
        for thread in threads:
            thread.join(timeout=5)
        for state in states.values():
            state.server.kill()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        if temporary is not None:
            temporary.cleanup()
    print(json.dumps({
        "verdict": report.get("verdict"),
        "output": str(args.output),
        "elapsed_seconds": report.get("elapsed_seconds"),
        "fatal_error": report.get("fatal_error"),
    }, indent=2, sort_keys=True))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
