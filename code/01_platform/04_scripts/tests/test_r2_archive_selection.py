#!/usr/bin/env python3
"""DEC-060 (2026-09-30): R2 archive selection core — one config list rules.

WHY THIS EXISTS
---------------
Operator requirement (2026-09-30): choose by config which tables are saved to
Cloudflare R2 at EOD, and save no other; never one merged lake table. The rules
are pinned here without a cluster:

  * exactly the listed tables are enabled (turned on when off),
  * every live-enabled table outside the list is turned OFF ("not any other"),
  * a listed table missing from the live catalog is reported, never skipped,
  * an empty list is refused — a config error must not mass-disable.

The cluster halves (catalog read + ALTERs) live in the sync tooling and carry
their own tests; this file pins the rules they must obey, and the gate
preflight guard reuses `plan()`.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPTS = str(Path(__file__).resolve().parents[1])
if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)

from r2_archive_selection import parse_selection, plan  # noqa: E402


class ParseSelectionTest(unittest.TestCase):
    def test_trims_comma_list_and_drops_empties(self) -> None:
        self.assertEqual(["a", "b", "c"], parse_selection(" a , b ,, c "))

    def test_empty_selection_is_refused(self) -> None:
        for bad in ("", "   ", ",", " , "):
            with self.assertRaises(ValueError):
                parse_selection(bad)

    def test_duplicates_collapse_preserving_order(self) -> None:
        self.assertEqual(["b", "a"], parse_selection("b,a,b"))


class PlanTest(unittest.TestCase):
    LIVE = {
        "raw_table_1": False,
        "candle_closed": True,
        "feature_values": True,
        "Order_Lifecycle": True,
    }

    def test_selected_and_live_enabled_is_ok(self) -> None:
        p = plan(self.LIVE, ["candle_closed"])
        self.assertEqual(["candle_closed"], p.ok)
        self.assertEqual([], p.enable)
        self.assertNotIn("candle_closed", p.disable)
        self.assertEqual(["Order_Lifecycle", "feature_values"], p.disable)
        self.assertEqual([], p.missing)
        self.assertFalse(p.is_clean(), "Order_Lifecycle is on outside the list and must be disabled")

    def test_selected_and_off_is_enabled(self) -> None:
        p = plan(self.LIVE, ["raw_table_1"])
        self.assertEqual(["raw_table_1"], p.enable)

    def test_live_enabled_outside_the_list_is_disabled(self) -> None:
        p = plan(self.LIVE, ["raw_table_1"])
        self.assertEqual(["Order_Lifecycle", "candle_closed", "feature_values"], p.disable)

    def test_missing_table_is_reported_not_skipped(self) -> None:
        p = plan(self.LIVE, ["candle_closed", "ghost_table"])
        self.assertEqual(["ghost_table"], p.missing)
        self.assertEqual(["candle_closed"], p.ok)

    def test_plan_is_idempotent_when_applied(self) -> None:
        p = plan(self.LIVE, ["raw_table_1"])
        live_after = dict(self.LIVE)
        for t in p.enable:
            live_after[t] = True
        for t in p.disable:
            live_after[t] = False
        p2 = plan(live_after, ["raw_table_1"])
        self.assertTrue(p2.is_clean(), f"second pass should be clean, got {p2}")

    def test_empty_list_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            plan(self.LIVE, [])

    def test_clean_plan_when_everything_already_matches(self) -> None:
        live = {"a": True, "b": False}
        self.assertTrue(plan(live, ["a"]).is_clean())


if __name__ == "__main__":
    unittest.main()
