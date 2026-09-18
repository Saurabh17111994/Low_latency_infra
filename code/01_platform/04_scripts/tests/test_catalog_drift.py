#!/usr/bin/env python3
"""catalog_drift names the tables behind catalog-guard's over-count (2026-09-18).

catalog-guard stops at "catalog has MORE tables than the manifest describes (33/27) — extra
tables are drift, not health". A count says how much drift there is, not which tables: finding
the six that stood behind that message took a hand-run ZK listing, and four of them turned out to
be leftovers of an interrupted drill run. These tests pin the naming, including the case that
makes exact-set comparison necessary rather than case-insensitive matching.
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "code/01_platform/04_scripts"))

import catalog_drift  # noqa: E402


class DriftDiffTest(unittest.TestCase):
    def test_extras_and_missing_are_reported_sorted(self):
        extra, missing = catalog_drift.diff(
            live=["Signal_Candidates", "zz_leftover", "probe_tbl_1"],
            manifest=["Signal_Candidates", "raw_table_1"],
        )

        self.assertEqual(extra, ["probe_tbl_1", "zz_leftover"])
        self.assertEqual(missing, ["raw_table_1"])

    def test_a_healthy_catalog_has_no_drift(self):
        extra, missing = catalog_drift.diff(live=["a", "b"], manifest=["b", "a"])

        self.assertEqual(extra, [])
        self.assertEqual(missing, [])

    def test_a_lowercase_twin_is_drift_not_a_match(self):
        # The 2026-09-18 extras included `signal_candidates` next to the real `Signal_Candidates`:
        # a different table, not a spelling of the same one.
        extra, missing = catalog_drift.diff(live=["signal_candidates"], manifest=["Signal_Candidates"])

        self.assertEqual(extra, ["signal_candidates"])
        self.assertEqual(missing, ["Signal_Candidates"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
