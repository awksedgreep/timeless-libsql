#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import query_oracles  # noqa: E402


class QueryOracleManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = query_oracles.load_manifest(ROOT, "tests/query_oracles/manifest.json")

    def assert_invalid(self, manifest: dict, needle: str) -> None:
        errors = query_oracles.validate_manifest(ROOT, manifest)
        self.assertTrue(any(needle in error for error in errors), errors)

    def test_checked_in_manifest_matches_docs_and_fixtures(self) -> None:
        self.assertEqual(query_oracles.validate_manifest(ROOT, self.manifest), [])

    def test_floating_image_is_rejected(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["oracles"]["prometheus"]["image"] = "docker.io/prom/prometheus:latest"
        self.assert_invalid(changed, "floating image reference")

    def test_short_source_commit_is_rejected(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["oracles"]["victorialogs"]["source_commit"] = "deadbeef"
        self.assert_invalid(changed, "source_commit")

    def test_manifest_is_stable_json(self) -> None:
        encoded = json.dumps(self.manifest, indent=2, sort_keys=True)
        self.assertIn('"schema_version": 1', encoded)

    def test_prometheus_api_fixture_has_unique_row_addressed_cases(self) -> None:
        fixture = query_oracles.load_manifest(
            ROOT, "tests/query_oracles/prometheus/api_cases.json"
        )
        identifiers = [case["id"] for case in fixture["cases"]]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        self.assertTrue(all(identifier.startswith("PQL-") for identifier in identifiers))
        for case in fixture.get("operator_cases", []):
            self.assertIn(case.get("result_order", "unordered"), {"ordered", "unordered"})

    def test_promql_vector_comparison_distinguishes_promised_order(self) -> None:
        expected = [
            {"metric": {"value": "first"}, "value": [1, "1"]},
            {"metric": {"value": "second"}, "value": [1, "2"]},
        ]
        reordered = list(reversed(expected))
        self.assertTrue(
            query_oracles.query_results_equal(expected, reordered, ordered=False)
        )
        self.assertFalse(
            query_oracles.query_results_equal(expected, reordered, ordered=True)
        )
        changed = copy.deepcopy(reordered)
        changed[0]["value"][1] = "3"
        self.assertFalse(
            query_oracles.query_results_equal(expected, changed, ordered=False)
        )


if __name__ == "__main__":
    unittest.main()
