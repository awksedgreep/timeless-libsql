#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import query_evidence  # noqa: E402


class QueryEvidenceTests(unittest.TestCase):
    def test_nearest_rank_percentiles_are_stable(self) -> None:
        values = list(range(1, 101))
        self.assertEqual(query_evidence.percentile(values, 0.50), 50)
        self.assertEqual(query_evidence.percentile(values, 0.95), 95)
        self.assertEqual(query_evidence.percentile(values, 0.99), 99)

    def test_numeric_delta_omits_gauges_without_change_and_strings(self) -> None:
        self.assertEqual(
            query_evidence.numeric_delta(
                {"count": 2, "bytes": 10, "module": "logs", "steady": 4},
                {"count": 5, "bytes": 22, "module": "logs", "steady": 4},
            ),
            {"count": 3, "bytes": 12},
        )

    def test_result_cardinality_parsers_reject_no_data_loss(self) -> None:
        self.assertEqual(
            query_evidence.query_json_cardinality(
                b'{"status":"success","data":{"result":[{},{}]}}'
            ),
            2,
        )
        self.assertEqual(query_evidence.ndjson_cardinality(b"{}\n{}\n"), 2)
        with self.assertRaises(RuntimeError):
            query_evidence.query_json_cardinality(b'{"status":"error"}')


if __name__ == "__main__":
    unittest.main()
