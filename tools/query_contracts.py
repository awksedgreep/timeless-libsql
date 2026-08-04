#!/usr/bin/env python3
"""Validate the public query matrices and their documentation contracts."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote


MATRICES = (
    "docs/PROMQL_FEATURE_MATRIX.md",
    "docs/LOGSQL_FEATURE_MATRIX.md",
)
LEGAL_STATUSES = {
    "shipped",
    "partial",
    "in progress",
    "missing",
    "experimental",
    "deferred",
    "library",
}
LEGAL_TARGETS = {"EXT", "API", "SQL", "LIB", "DEFER"}
LEGAL_PRIORITIES = {"P0", "P1", "P2", "P3", "EXP", "DEFER"}
ID_PATTERN = re.compile(r"^(?:PQL-[SO RFH]\d{2}|MQL-\d{2}|LQL-[FPSQ]\d{2})$".replace(" ", ""))
LINK_PATTERN = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
MARKER_PATTERN = re.compile(r"<!--\s*query-contract-shipped:\s*(.*?)\s*-->")


def plain(value: str) -> str:
    return value.strip().replace("`", "")


def cells(line: str) -> list[str]:
    return [part.strip() for part in line.strip().strip("|").split("|")]


@dataclass(frozen=True)
class MatrixRow:
    identifier: str
    status: str
    target: str
    priority: str
    foundation: str
    source: str
    line: int
    raw: str


def parse_matrix(path: Path) -> tuple[list[MatrixRow], list[str]]:
    rows: list[MatrixRow] = []
    errors: list[str] = []
    header: list[str] | None = None
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.startswith("|"):
            header = None
            continue
        values = cells(line)
        normalized = [plain(value).lower() for value in values]
        if "id" in normalized and "rust now" in normalized:
            header = normalized
            continue
        if header is None or all(set(value) <= {"-", ":"} for value in values):
            continue
        first = plain(values[0]) if values else ""
        if not re.match(r"^(?:PQL-|MQL-|LQL-)", first):
            continue
        if len(values) != len(header):
            errors.append(f"{path}:{number}: row has {len(values)} cells; expected {len(header)}")
            continue
        record = dict(zip(header, values))
        try:
            rows.append(
                MatrixRow(
                    identifier=first,
                    status=plain(record["rust now"]),
                    target=plain(record["target"]),
                    priority=plain(record["priority"]),
                    foundation=plain(record.get("foundation", "")),
                    source=str(path),
                    line=number,
                    raw=line,
                )
            )
        except KeyError as error:
            errors.append(f"{path}:{number}: required matrix column missing: {error.args[0]}")
    return rows, errors


def heading_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
        if not match:
            continue
        heading = re.sub(r"[`*~]", "", match.group(1)).lower()
        anchor = re.sub(r"[^\w\- ]", "", heading, flags=re.UNICODE)
        anchor = re.sub(r"\s+", "-", anchor).strip("-")
        count = counts.get(anchor, 0)
        counts[anchor] = count + 1
        anchors.add(anchor if count == 0 else f"{anchor}-{count}")
    return anchors


def validate_local_links(root: Path) -> list[str]:
    errors: list[str] = []
    markdown = [root / "README.md", *sorted((root / "docs").glob("*.md"))]
    markdown.extend(sorted((root / "servers" / "crates").glob("*/README.md")))
    for source in markdown:
        if not source.exists():
            continue
        for raw in LINK_PATTERN.findall(source.read_text(encoding="utf-8")):
            if raw.startswith(("http://", "https://", "mailto:", "#")):
                continue
            location, separator, anchor = unquote(raw).partition("#")
            target = (source.parent / location).resolve() if location else source.resolve()
            if not target.exists():
                errors.append(f"{source.relative_to(root)}: broken local link {raw}")
                continue
            if separator and target.suffix.lower() == ".md":
                if anchor not in heading_anchors(target):
                    errors.append(f"{source.relative_to(root)}: missing anchor {raw}")
    return errors


def parse_test_references(path: Path) -> tuple[dict[str, tuple[str, str]], list[str]]:
    references: dict[str, tuple[str, str]] = {}
    errors: list[str] = []
    header: list[str] | None = None
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.startswith("|"):
            header = None
            continue
        values = cells(line)
        normalized = [plain(value).lower() for value in values]
        if normalized[:3] == ["id", "test path", "test symbol"]:
            header = normalized
            continue
        if header is None or all(set(value) <= {"-", ":"} for value in values):
            continue
        identifier = plain(values[0])
        if not re.match(r"^(?:PQL-|MQL-|LQL-)", identifier):
            continue
        if identifier in references:
            errors.append(f"{path}:{number}: duplicate test reference {identifier}")
            continue
        record = dict(zip(header, values))
        references[identifier] = (plain(record["test path"]), plain(record["test symbol"]))
    return references, errors


def shipped_marker(path: Path) -> tuple[set[str], list[str]]:
    matches = MARKER_PATTERN.findall(path.read_text(encoding="utf-8"))
    if len(matches) != 1:
        return set(), [f"{path}: expected exactly one query-contract-shipped marker"]
    return set(matches[0].split()), []


def validate(root: Path) -> list[str]:
    errors: list[str] = []
    rows: list[MatrixRow] = []
    for relative in MATRICES:
        path = root / relative
        if not path.exists():
            errors.append(f"missing matrix {relative}")
            continue
        parsed, parse_errors = parse_matrix(path)
        rows.extend(parsed)
        errors.extend(parse_errors)

    identifiers: dict[str, MatrixRow] = {}
    for row in rows:
        location = f"{Path(row.source).relative_to(root)}:{row.line}"
        if not ID_PATTERN.match(row.identifier):
            errors.append(f"{location}: illegal row ID {row.identifier}")
        if row.identifier in identifiers:
            first = identifiers[row.identifier]
            errors.append(f"{location}: duplicate row ID {row.identifier} (first at {first.source}:{first.line})")
        identifiers[row.identifier] = row
        if row.status not in LEGAL_STATUSES:
            errors.append(f"{location}: illegal status {row.status}")
        if row.target not in LEGAL_TARGETS:
            errors.append(f"{location}: illegal target {row.target}")
        if row.priority not in LEGAL_PRIORITIES:
            errors.append(f"{location}: illegal priority {row.priority}")
        if row.target == "DEFER" and row.status != "deferred":
            errors.append(f"{location}: DEFER target must have deferred status")
        if row.status == "deferred" and row.target != "DEFER":
            errors.append(f"{location}: deferred status must have DEFER target")

    references_path = root / "docs/QUERY_TEST_REFERENCES.md"
    if references_path.exists():
        references, reference_errors = parse_test_references(references_path)
        errors.extend(reference_errors)
    else:
        references = {}
        errors.append("missing docs/QUERY_TEST_REFERENCES.md")
    shipped = {row.identifier for row in rows if row.status == "shipped"}
    for identifier in sorted(shipped - references.keys()):
        errors.append(f"shipped row {identifier} has no test reference")
    for identifier in sorted(references.keys() - shipped):
        errors.append(f"test reference {identifier} does not name a shipped row")
    for identifier, (relative, symbol) in references.items():
        path = root / relative
        if not path.is_file():
            errors.append(f"test reference {identifier} has missing path {relative}")
        elif symbol not in path.read_text(encoding="utf-8"):
            errors.append(f"test reference {identifier} has missing symbol {symbol} in {relative}")

    for row in rows:
        recipe_links = [
            link
            for link in LINK_PATTERN.findall(row.raw)
            if link.startswith("QUERY_SQL_EQUIVALENTS.md#")
        ]
        if row.status == "shipped" and (row.target == "SQL" or "SQL" in row.foundation):
            if not recipe_links:
                errors.append(f"shipped SQL-founded row {row.identifier} has no executable recipe link")

    for server, prefixes in (
        ("servers/crates/timeless-metrics-api/README.md", ("PQL-", "MQL-")),
        ("servers/crates/timeless-logs-api/README.md", ("LQL-",)),
    ):
        path = root / server
        if not path.exists():
            errors.append(f"missing server documentation {server}")
            continue
        actual, marker_errors = shipped_marker(path)
        errors.extend(marker_errors)
        expected = {
            row.identifier
            for row in rows
            if row.status == "shipped"
            and row.target == "API"
            and row.identifier.startswith(prefixes)
        }
        if actual != expected:
            errors.append(
                f"{server}: shipped marker mismatch; expected {sorted(expected)}, got {sorted(actual)}"
            )

    equivalents = root / "docs/QUERY_SQL_EQUIVALENTS.md"
    if equivalents.exists():
        for identifier in re.findall(r"`((?:PQL|MQL|LQL)-[A-Z]?\d{2})`", equivalents.read_text(encoding="utf-8")):
            if identifier not in identifiers:
                errors.append(f"docs/QUERY_SQL_EQUIVALENTS.md: unknown matrix row {identifier}")
    else:
        errors.append("missing docs/QUERY_SQL_EQUIVALENTS.md")

    errors.extend(validate_local_links(root))
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    errors = validate(args.root.resolve())
    if errors:
        for error in errors:
            print(f"query-contract: {error}", file=sys.stderr)
        return 1
    print("query contracts: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
