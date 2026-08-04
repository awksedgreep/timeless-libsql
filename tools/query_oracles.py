#!/usr/bin/env python3
"""Validate and execute immutable upstream query semantic oracles."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("validate", "probe", "prometheus-smoke"))
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
    return prometheus_smoke(root, args.runtime, manifest)


if __name__ == "__main__":
    raise SystemExit(main())
