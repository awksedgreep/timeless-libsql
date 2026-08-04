#!/usr/bin/env python3
"""Validate and execute immutable upstream query semantic oracles."""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")


def load_manifest(root: Path, relative: str) -> dict:
    path = root / relative
    return json.loads(path.read_text(encoding="utf-8"))


def validate_manifest(root: Path, manifest: dict) -> list[str]:
    errors: list[str] = []
    if manifest.get("schema_version") != 1:
        errors.append("manifest schema_version must be 1")
    oracles = manifest.get("oracles")
    if not isinstance(oracles, dict) or set(oracles) != {
        "prometheus",
        "victoriametrics",
        "victorialogs",
    }:
        errors.append("manifest must define exactly prometheus, victoriametrics, and victorialogs")
        return errors
    docs = (root / "docs/QUERY_ORACLES.md").read_text(encoding="utf-8")
    images: set[str] = set()
    for name, oracle in oracles.items():
        prefix = f"oracle {name}"
        image = oracle.get("image", "")
        if "@sha256:" not in image or image.rsplit("@", 1)[-1] == "":
            errors.append(f"{prefix}: image must use an immutable sha256 digest")
        if ":latest" in image or "@" not in image:
            errors.append(f"{prefix}: floating image reference is forbidden")
        if image in images:
            errors.append(f"{prefix}: duplicate image pin")
        images.add(image)
        source_commit = oracle.get("source_commit", "")
        if not COMMIT.match(source_commit):
            errors.append(f"{prefix}: source_commit must be a 40-character lowercase SHA")
        child = oracle.get("linux_amd64_digest", "")
        if not SHA256.match(child):
            errors.append(f"{prefix}: linux_amd64_digest must be sha256:<64 hex>")
        expected = oracle.get("version_contains")
        if not isinstance(expected, str) or not expected:
            errors.append(f"{prefix}: version_contains is required")
        for field in (oracle.get("version", ""), source_commit, image, child):
            if field and field not in docs:
                errors.append(f"{prefix}: docs/QUERY_ORACLES.md is missing {field}")
        fixtures = oracle.get("fixtures")
        if not isinstance(fixtures, list):
            errors.append(f"{prefix}: fixtures must be a list")
            continue
        for relative in fixtures:
            path = root / relative
            if not path.is_file():
                errors.append(f"{prefix}: missing fixture {relative}")
    return errors


def container_command(runtime: str, oracle: dict, extra: list[str]) -> list[str]:
    command = [runtime, "run", "--rm", "--platform", "linux/amd64"]
    entrypoint = oracle.get("version_entrypoint")
    if entrypoint:
        command.extend(["--entrypoint", entrypoint])
    command.append(oracle["image"])
    command.extend(extra)
    return command


def probe(runtime: str, manifest: dict) -> int:
    failures = 0
    for name, oracle in manifest["oracles"].items():
        command = container_command(runtime, oracle, oracle["version_args"])
        result = subprocess.run(command, text=True, capture_output=True, timeout=180)
        output = (result.stdout + result.stderr).strip()
        if result.returncode != 0 or oracle["version_contains"] not in output:
            print(f"{name}: version probe failed ({result.returncode})\n{output}", file=sys.stderr)
            failures += 1
        else:
            first = next((line for line in output.splitlines() if line.strip()), output)
            print(f"{name}: {first}")
    return 1 if failures else 0


def prometheus_smoke(root: Path, runtime: str, manifest: dict) -> int:
    oracle = manifest["oracles"]["prometheus"]
    fixture = (root / oracle["fixtures"][0]).resolve()
    command = [
        runtime,
        "run",
        "--rm",
        "--platform",
        "linux/amd64",
        "--entrypoint",
        "/bin/promtool",
        "--mount",
        f"type=bind,src={fixture},dst=/work/promql_smoke.yml,readonly",
        oracle["image"],
        "test",
        "rules",
        "/work/promql_smoke.yml",
    ]
    return subprocess.run(command, timeout=180).returncode


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def protobuf_varint(value: int) -> bytes:
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def protobuf_bytes(field: int, value: bytes) -> bytes:
    return protobuf_varint((field << 3) | 2) + protobuf_varint(len(value)) + value


def snappy_literal(value: bytes) -> bytes:
    """Encode one raw-Snappy literal without an optional native dependency."""
    length = len(value)
    if length == 0:
        return b"\x00"
    encoded = bytearray(protobuf_varint(length))
    adjusted = length - 1
    if length <= 60:
        encoded.append(adjusted << 2)
    else:
        width = max(1, (adjusted.bit_length() + 7) // 8)
        encoded.append((59 + width) << 2)
        encoded.extend(adjusted.to_bytes(width, "little"))
    encoded.extend(value)
    return bytes(encoded)


def prometheus_remote_write(timestamp_ms: int) -> bytes:
    def label(name: str, value: str) -> bytes:
        return protobuf_bytes(1, name.encode()) + protobuf_bytes(2, value.encode())

    def sample(value: float, at_ms: int) -> bytes:
        encoded = bytes([(1 << 3) | 1]) + struct.pack("<d", value)
        return encoded + protobuf_varint(2 << 3) + protobuf_varint(at_ms)

    def series(
        name: str,
        points: list[tuple[float, int]],
        extra_labels: dict[str, str] | None = None,
    ) -> bytes:
        labels = {"__name__": name, "job": "oracle", **(extra_labels or {})}
        encoded = b"".join(
            protobuf_bytes(1, label(key, value))
            for key, value in sorted(labels.items())
        )
        for value, at_ms in points:
            encoded += protobuf_bytes(2, sample(value, at_ms))
        return protobuf_bytes(1, encoded)

    write_request = series("oracle_lookback", [(7.0, timestamp_ms)])
    write_request += series(
        "oracle_temporal",
        [
            (float(index + 1), timestamp_ms + offset_ms)
            for index, offset_ms in enumerate(range(-30_000, 30_001, 10_000))
        ],
    )
    write_request += series(
        "oracle_arithmetic_lhs",
        [(8.0, timestamp_ms + 30_000)],
        {"host": "a", "zone": "east"},
    )
    write_request += series(
        "oracle_arithmetic_rhs",
        [(2.0, timestamp_ms + 30_000)],
        {"host": "a", "zone": "east"},
    )
    write_request += series(
        "oracle_arithmetic_rhs_duplicate",
        [(3.0, timestamp_ms + 30_000)],
        {"host": "a", "zone": "east"},
    )
    write_request += series(
        "oracle_matching_lhs",
        [(8.0, timestamp_ms + 30_000)],
        {"host": "a", "shared": "x", "zone": "east"},
    )
    write_request += series(
        "oracle_matching_rhs",
        [(2.0, timestamp_ms + 30_000)],
        {"host": "a", "shared": "x", "zone": "west"},
    )
    write_request += series(
        "oracle_matching_rhs_duplicate",
        [(3.0, timestamp_ms + 30_000)],
        {"host": "a", "shared": "x", "zone": "north"},
    )
    return snappy_literal(write_request)


def prometheus_api(root: Path, runtime: str, manifest: dict) -> int:
    oracle = manifest["oracles"]["prometheus"]
    fixture_path = next(
        root / path for path in oracle["fixtures"] if path.endswith("api_cases.json")
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    port = free_port()
    name = f"timeless-promql-oracle-{os.getpid()}"
    command = [
        runtime,
        "run",
        "--rm",
        "-d",
        "--name",
        name,
        "--platform",
        "linux/amd64",
        "-p",
        f"127.0.0.1:{port}:9090",
        oracle["image"],
        "--config.file=/etc/prometheus/prometheus.yml",
        "--storage.tsdb.path=/prometheus",
        "--web.listen-address=0.0.0.0:9090",
        "--web.enable-remote-write-receiver",
    ]
    started = subprocess.run(command, text=True, capture_output=True, timeout=180)
    if started.returncode != 0:
        print(started.stderr, file=sys.stderr)
        return 1
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(base + "/-/ready", timeout=1) as response:
                    if response.status == 200:
                        break
            except (OSError, urllib.error.URLError):
                time.sleep(0.1)
        else:
            print("prometheus API oracle did not become ready", file=sys.stderr)
            return 1

        sample_timestamp_ms = (int(time.time() * 1_000) // 60_000 - 1) * 60_000
        write = urllib.request.Request(
            base + "/api/v1/write",
            data=prometheus_remote_write(sample_timestamp_ms),
            headers={
                "content-type": "application/x-protobuf",
                "content-encoding": "snappy",
                "x-prometheus-remote-write-version": "0.1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(write, timeout=10) as response:
            if response.status != 204:
                print(f"prometheus oracle remote write returned {response.status}", file=sys.stderr)
                return 1

        failures = 0
        for case in fixture["cases"]:
            url = base + case["endpoint"] + "?" + urllib.parse.urlencode(case["params"])
            try:
                with urllib.request.urlopen(url, timeout=10) as response:
                    status = response.status
                    body = json.loads(response.read())
            except urllib.error.HTTPError as error:
                status = error.code
                body = json.loads(error.read())
            if status != case["status"] or body != case["body"]:
                print(
                    f"{case['id']}: expected {case['status']} {case['body']!r}; "
                    f"got {status} {body!r}",
                    file=sys.stderr,
                )
                failures += 1
            else:
                print(f"{case['id']}: ok")
        for case in fixture.get("lookback_cases", []):
            evaluation_ms = sample_timestamp_ms + case["evaluation_offset_ms"]
            params = {
                "query": "oracle_lookback",
                "time": str(evaluation_ms / 1_000),
                "lookback_delta": case["lookback_delta"],
            }
            url = base + "/api/v1/query?" + urllib.parse.urlencode(params)
            with urllib.request.urlopen(url, timeout=10) as response:
                body = json.loads(response.read())
            result = body.get("data", {}).get("result", [])
            expected_count = case["expected_result_count"]
            valid = (
                response.status == 200
                and body.get("status") == "success"
                and body.get("data", {}).get("resultType") == "vector"
                and len(result) == expected_count
            )
            if valid and expected_count == 1:
                valid = result == [
                    {
                        "metric": {"__name__": "oracle_lookback", "job": "oracle"},
                        "value": [evaluation_ms / 1_000, "7"],
                    }
                ]
            if not valid:
                print(f"{case['id']}: unexpected response {body!r}", file=sys.stderr)
                failures += 1
            else:
                print(f"{case['id']}: ok")
        for case in fixture.get("temporal_cases", []):
            query = case["query"]
            if "at_offset_ms" in case:
                at_ms = sample_timestamp_ms + case["at_offset_ms"]
                query = query.format(at=str(at_ms / 1_000))
            expected_values = [str(value) for value in case["expected_values"]]
            if "range" in case:
                range_case = case["range"]
                start_ms = sample_timestamp_ms + range_case["start_offset_ms"]
                end_ms = sample_timestamp_ms + range_case["end_offset_ms"]
                params = {
                    "query": query,
                    "start": str(start_ms / 1_000),
                    "end": str(end_ms / 1_000),
                    "step": range_case["step"],
                }
                expected_timestamps = [
                    start_ms + index * (end_ms - start_ms) // (len(expected_values) - 1)
                    for index in range(len(expected_values))
                ]
                expected_result_type = "matrix"
                expected_result = [
                    {
                        "metric": {"__name__": "oracle_temporal", "job": "oracle"},
                        "values": [
                            [timestamp / 1_000, value]
                            for timestamp, value in zip(expected_timestamps, expected_values)
                        ],
                    }
                ]
                endpoint = "/api/v1/query_range"
            else:
                evaluation_ms = sample_timestamp_ms + case["evaluation_offset_ms"]
                params = {"query": query, "time": str(evaluation_ms / 1_000)}
                expected_result_type = "vector"
                expected_result = [
                    {
                        "metric": {"__name__": "oracle_temporal", "job": "oracle"},
                        "value": [evaluation_ms / 1_000, expected_values[0]],
                    }
                ]
                endpoint = "/api/v1/query"
            url = base + endpoint + "?" + urllib.parse.urlencode(params)
            with urllib.request.urlopen(url, timeout=10) as response:
                body = json.loads(response.read())
            valid = (
                response.status == 200
                and body.get("status") == "success"
                and body.get("data", {}).get("resultType") == expected_result_type
                and body.get("data", {}).get("result") == expected_result
            )
            if not valid:
                print(
                    f"{case['id']}: expected {expected_result!r}; got {body!r}",
                    file=sys.stderr,
                )
                failures += 1
            else:
                print(f"{case['id']}: ok")
        for case in fixture.get("subquery_cases", []):
            query = case["query"]
            if "at_offset_ms" in case:
                at_ms = sample_timestamp_ms + case["at_offset_ms"]
                query = query.format(at=str(at_ms / 1_000))
            evaluation_ms = sample_timestamp_ms + case["evaluation_offset_ms"]
            range_case = case.get("range")
            if range_case:
                start_ms = sample_timestamp_ms + range_case["start_offset_ms"]
                end_ms = sample_timestamp_ms + range_case["end_offset_ms"]
                params = {
                    "query": query,
                    "start": str(start_ms / 1_000),
                    "end": str(end_ms / 1_000),
                    "step": range_case["step"],
                }
                endpoint = "/api/v1/query_range"
            else:
                params = {"query": query, "time": str(evaluation_ms / 1_000)}
                endpoint = "/api/v1/query"
            url = base + endpoint + "?" + urllib.parse.urlencode(params)
            with urllib.request.urlopen(url, timeout=10) as response:
                body = json.loads(response.read())
            if range_case:
                expected_result_type = "matrix"
                expected_values = case["expected_values"]
                expected_timestamps = [
                    start_ms
                    + index * (end_ms - start_ms) // (len(expected_values) - 1)
                    for index in range(len(expected_values))
                ]
                expected_result = [
                    {
                        "metric": {"job": "oracle"},
                        "values": [
                            [timestamp / 1_000, str(value)]
                            for timestamp, value in zip(
                                expected_timestamps, expected_values
                            )
                        ],
                    }
                ]
            elif "expected_matrix" in case:
                expected_result_type = "matrix"
                expected_metric = {"job": "oracle"}
                if not case.get("drop_metric_name"):
                    expected_metric["__name__"] = "oracle_temporal"
                expected_result = [
                    {
                        "metric": expected_metric,
                        "values": [
                            [
                                (sample_timestamp_ms + timestamp_offset_ms) / 1_000,
                                str(value),
                            ]
                            for timestamp_offset_ms, value in case["expected_matrix"]
                        ],
                    }
                ]
            else:
                expected_result_type = "vector"
                expected_result = [
                    {
                        "metric": {"job": "oracle"},
                        "value": [evaluation_ms / 1_000, str(case["expected_values"][0])],
                    }
                ]
            valid = (
                response.status == 200
                and body.get("status") == "success"
                and body.get("data", {}).get("resultType") == expected_result_type
                and body.get("data", {}).get("result") == expected_result
            )
            if not valid:
                print(
                    f"{case['id']}: expected {expected_result!r}; got {body!r}",
                    file=sys.stderr,
                )
                failures += 1
            else:
                print(f"{case['id']}: ok")
        for case in fixture.get("operator_cases", []):
            evaluation_ms = sample_timestamp_ms + case["evaluation_offset_ms"]
            range_case = case.get("range")
            if range_case:
                start_ms = sample_timestamp_ms + range_case["start_offset_ms"]
                end_ms = sample_timestamp_ms + range_case["end_offset_ms"]
                params = {
                    "query": case["query"],
                    "start": str(start_ms / 1_000),
                    "end": str(end_ms / 1_000),
                    "step": range_case["step"],
                }
                endpoint = "/api/v1/query_range"
                result_type = "matrix"
                values = case["expected_values"]
                if "expected_offsets_ms" in case:
                    timestamps = [
                        sample_timestamp_ms + offset
                        for offset in case["expected_offsets_ms"]
                    ]
                else:
                    timestamps = [
                        start_ms + index * (end_ms - start_ms) // (len(values) - 1)
                        for index in range(len(values))
                    ]
                result = [{
                    "metric": dict(case.get("expected_metric", {"job": "oracle"})),
                    "values": [
                        [timestamp / 1_000, str(value)]
                        for timestamp, value in zip(timestamps, values)
                    ],
                }]
            elif case.get("expected_empty"):
                params = {
                    "query": case["query"],
                    "time": str(evaluation_ms / 1_000),
                }
                endpoint = "/api/v1/query"
                result_type = "vector"
                result = []
            else:
                params = {
                    "query": case["query"],
                    "time": str(evaluation_ms / 1_000),
                }
                endpoint = "/api/v1/query"
                result_type = "vector"
                result = [{
                    "metric": dict(case.get("expected_metric", {"job": "oracle"})),
                    "value": [evaluation_ms / 1_000, str(case["expected_values"][0])],
                }]
            if result and "expected_metric" not in case and not case.get("drop_metric_name"):
                result[0]["metric"]["__name__"] = "oracle_temporal"
            url = base + endpoint + "?" + urllib.parse.urlencode(params)
            with urllib.request.urlopen(url, timeout=10) as response:
                body = json.loads(response.read())
            valid = (
                response.status == 200
                and body.get("status") == "success"
                and body.get("data", {}).get("resultType") == result_type
                and body.get("data", {}).get("result") == result
            )
            if not valid:
                print(f"{case['id']}: expected {result!r}; got {body!r}", file=sys.stderr)
                failures += 1
            else:
                print(f"{case['id']}: ok")
        for case in fixture.get("operator_error_cases", []):
            evaluation_ms = sample_timestamp_ms + case["evaluation_offset_ms"]
            params = {
                "query": case["query"],
                "time": str(evaluation_ms / 1_000),
            }
            url = base + "/api/v1/query?" + urllib.parse.urlencode(params)
            try:
                with urllib.request.urlopen(url, timeout=10) as response:
                    status = response.status
                    body = json.loads(response.read())
            except urllib.error.HTTPError as error:
                status = error.code
                body = json.loads(error.read())
            valid = (
                status == case["status"]
                and body.get("status") == "error"
                and body.get("errorType") == case["error_type"]
                and case["error_contains"] in body.get("error", "")
            )
            if not valid:
                print(f"{case['id']}: unexpected response {status} {body!r}", file=sys.stderr)
                failures += 1
            else:
                print(f"{case['id']}: ok")
        return 1 if failures else 0
    finally:
        subprocess.run(
            [runtime, "rm", "-f", name],
            text=True,
            capture_output=True,
            timeout=30,
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=("validate", "probe", "prometheus-smoke", "prometheus-api")
    )
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--manifest", default="tests/query_oracles/manifest.json")
    parser.add_argument("--runtime", default="docker")
    args = parser.parse_args()
    root = args.root.resolve()
    manifest = load_manifest(root, args.manifest)
    errors = validate_manifest(root, manifest)
    if errors:
        for error in errors:
            print(f"query-oracle: {error}", file=sys.stderr)
        return 1
    if args.command == "validate":
        print("query oracle manifest: ok")
        return 0
    if args.command == "probe":
        return probe(args.runtime, manifest)
    if args.command == "prometheus-smoke":
        return prometheus_smoke(root, args.runtime, manifest)
    return prometheus_api(root, args.runtime, manifest)


if __name__ == "__main__":
    raise SystemExit(main())
