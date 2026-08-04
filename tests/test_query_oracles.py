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


if __name__ == "__main__":
    unittest.main()
