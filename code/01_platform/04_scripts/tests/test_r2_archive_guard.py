#!/usr/bin/env python3
"""DEC-060 (S3): the gate preflight archive-selection guard.

WHY THIS EXISTS
---------------
The rule only holds if a certifying run checks it: no live table outside the
configured archive list may be set to archive, and with no list configured
nothing may be enabled at all (fail-closed). The pure decision lives in
`gate_preflight.archive_selection_drift`; the live read (ZK) is exercised by
the scoped gate run.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPTS = str(Path(__file__).resolve().parents[1])
if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)

from gate_preflight import archive_selection_drift  # noqa: E402


class ArchiveSelectionGuardTest(unittest.TestCase):
    LIVE = {"raw_table_1": False, "candle_closed": False, "Order_Lifecycle": False}

    def test_nothing_enabled_and_no_list_is_clean(self) -> None:
        self.assertEqual([], archive_selection_drift(self.LIVE, ""))

    def test_enabled_without_any_list_fails_closed(self) -> None:
        drift = archive_selection_drift(dict(self.LIVE, candle_closed=True), None)
        self.assertEqual(1, len(drift))
        self.assertIn("candle_closed", drift[0])
        self.assertIn("EOD_TABLES", drift[0])

    def test_enabled_outside_the_list_is_drift(self) -> None:
        live = dict(self.LIVE, candle_closed=True, Order_Lifecycle=True)
        drift = archive_selection_drift(live, "candle_closed")
        self.assertEqual(1, len(drift))
        self.assertIn("Order_Lifecycle", drift[0])
        self.assertNotIn("candle_closed", drift[0])

    def test_enabled_inside_the_list_is_clean(self) -> None:
        live = dict(self.LIVE, candle_closed=True)
        self.assertEqual([], archive_selection_drift(live, "candle_closed,raw_table_1"))

    def test_unreadable_flags_fail_closed(self) -> None:
        drift = archive_selection_drift(None, "raw_table_1")
        self.assertEqual(1, len(drift))
        self.assertIn("unreadable", drift[0])


if __name__ == "__main__":
    unittest.main()
