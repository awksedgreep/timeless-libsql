#!/usr/bin/env python3

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import query_contracts  # noqa: E402


PROM = """# Prom matrix

| ID | construct | Rust now | Elixir | foundation | target | priority |
|---|---|---|---|---|---|---|
| `PQL-S01` | selector ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-001)) | shipped | yes | `SQL` | `API` | P0 |
"""

LOGS = """# Logs matrix

| ID | construct | Rust now | foundation | target | priority |
|---|---|---|---|---|---|
| `LQL-F01` | filter | missing | `ROWS` | `API` | P0 |
"""


class QueryContractTests(unittest.TestCase):
    def fixture(self) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        for relative in (
            "docs",
            "tests",
            "servers/crates/timeless-metrics-api",
            "servers/crates/timeless-logs-api",
        ):
            (root / relative).mkdir(parents=True, exist_ok=True)
        (root / "README.md").write_text("# Root\n\n[Matrix](docs/PROMQL_FEATURE_MATRIX.md)\n")
        (root / "docs/PROMQL_FEATURE_MATRIX.md").write_text(PROM)
        (root / "docs/LOGSQL_FEATURE_MATRIX.md").write_text(LOGS)
        (root / "docs/QUERY_SQL_EQUIVALENTS.md").write_text("# SQL\n\n## SQL-PROM-001\n")
        (root / "docs/QUERY_TEST_REFERENCES.md").write_text(
            "# Tests\n\n| ID | test path | test symbol | coverage |\n"
            "|---|---|---|---|\n"
            "| `PQL-S01` | `tests/oracle.rs` | `selector_oracle` | fixture |\n"
        )
        (root / "tests/oracle.rs").write_text("fn selector_oracle() {}\n")
        (root / "servers/crates/timeless-metrics-api/README.md").write_text(
            "# Metrics\n\n<!-- query-contract-shipped: PQL-S01 -->\n"
        )
        (root / "servers/crates/timeless-logs-api/README.md").write_text(
            "# Logs\n\n<!-- query-contract-shipped: -->\n"
        )
        self.assertEqual(query_contracts.validate(root), [])
        return root

    def assert_invalid(self, root: Path, needle: str) -> None:
        errors = query_contracts.validate(root)
        self.assertTrue(any(needle in error for error in errors), errors)

    def test_duplicate_ids_fail(self) -> None:
        root = self.fixture()
        path = root / "docs/PROMQL_FEATURE_MATRIX.md"
        path.write_text(path.read_text() + PROM.splitlines()[-1] + "\n")
        self.assert_invalid(root, "duplicate row ID PQL-S01")

    def test_illegal_status_owner_and_priority_fail(self) -> None:
        root = self.fixture()
        path = root / "docs/PROMQL_FEATURE_MATRIX.md"
        path.write_text(PROM.replace("shipped", "almost").replace("`API`", "`BEAM`").replace("P0 |", "NOW |"))
        errors = query_contracts.validate(root)
        self.assertTrue(any("illegal status" in error for error in errors), errors)
        self.assertTrue(any("illegal target" in error for error in errors), errors)
        self.assertTrue(any("illegal priority" in error for error in errors), errors)

    def test_missing_shipped_test_reference_fails(self) -> None:
        root = self.fixture()
        (root / "docs/QUERY_TEST_REFERENCES.md").write_text("# Tests\n")
        self.assert_invalid(root, "shipped row PQL-S01 has no test reference")

    def test_missing_sql_recipe_anchor_fails(self) -> None:
        root = self.fixture()
        (root / "docs/QUERY_SQL_EQUIVALENTS.md").write_text("# SQL\n")
        self.assert_invalid(root, "missing anchor")

    def test_broken_local_link_fails(self) -> None:
        root = self.fixture()
        (root / "README.md").write_text("# Root\n\n[Gone](docs/GONE.md)\n")
        self.assert_invalid(root, "broken local link")

    def test_server_matrix_disagreement_fails(self) -> None:
        root = self.fixture()
        path = root / "servers/crates/timeless-metrics-api/README.md"
        path.write_text("# Metrics\n\n<!-- query-contract-shipped: -->\n")
        self.assert_invalid(root, "shipped marker mismatch")


if __name__ == "__main__":
    unittest.main()
