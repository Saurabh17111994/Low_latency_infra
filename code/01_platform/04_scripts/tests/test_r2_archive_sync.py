#!/usr/bin/env python3
"""DEC-060 (S2): the R2 archive sync — dry-run default, apply via ALTERs.

WHY THIS EXISTS
---------------
The sync is the only thing that changes live archive flags, so its contract is
pinned without a cluster: dry-run never calls --set; --apply sets exactly the
tables the core plan says; a failed ALTER is reported and exits 1; a missing
table, an unreadable live flag, a failed listing, or an empty list exits 2 and
changes nothing.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from subprocess import CompletedProcess

SCRIPTS = str(Path(__file__).resolve().parents[1])
if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)

from r2_archive_sync import SyncError, main, parse_listing, run_sync  # noqa: E402

LISTING = """tiering raw_table_1=disabled
tiering candle_closed=enabled
tiering Order_Lifecycle=enabled
eod-controller: RESULT=OK EXIT=0 TABLES=3 ENABLED=2
"""


class FakeRunner:
    def __init__(self, listing: str = LISTING, set_results=None):
        self.listing = listing
        self.calls: list[list[str]] = []
        self.set_results = set_results or {}

    def __call__(self, args):
        args = list(args)
        self.calls.append(args)
        if args[:2] == ["tiering", "--list"]:
            return CompletedProcess(args, 0, stdout=self.listing, stderr="")
        table = args[args.index("--tables") + 1]
        value = args[args.index("--set") + 1]
        rc = self.set_results.get((table, value), 0)
        return CompletedProcess(args, rc, stdout="", stderr="boom" if rc else "")


class ParseListingTest(unittest.TestCase):
    def test_parses_flags_and_ignores_the_result_line(self) -> None:
        live = parse_listing(LISTING)
        self.assertEqual(
            {"raw_table_1": False, "candle_closed": True, "Order_Lifecycle": True},
            live)

    def test_unreadable_flag_refuses(self) -> None:
        with self.assertRaises(SyncError):
            parse_listing("tiering ghost=unreadable\n")

    def test_empty_listing_refuses(self) -> None:
        with self.assertRaises(SyncError):
            parse_listing("")


class RunSyncTest(unittest.TestCase):
    def test_dry_run_prints_the_plan_and_never_sets(self) -> None:
        runner = FakeRunner()
        rc = run_sync("raw_table_1,candle_closed", runner, apply=False)
        self.assertEqual(0, rc)
        self.assertEqual([["tiering", "--list"]], runner.calls)

    def test_apply_sets_exactly_the_plan(self) -> None:
        runner = FakeRunner()
        rc = run_sync("raw_table_1,candle_closed", runner, apply=True)
        self.assertEqual(0, rc)
        sets = [c for c in runner.calls if c[:2] == ["tiering", "--set"]]
        self.assertCountEqual(
            [["tiering", "--set", "on", "--tables", "raw_table_1"],
             ["tiering", "--set", "off", "--tables", "Order_Lifecycle"]],
            sets)

    def test_failed_set_is_reported_and_exits_1(self) -> None:
        runner = FakeRunner(set_results={("raw_table_1", "on"): 1})
        rc = run_sync("raw_table_1,candle_closed", runner, apply=True)
        self.assertEqual(1, rc)

    def test_missing_table_refuses(self) -> None:
        runner = FakeRunner()
        rc = run_sync("raw_table_1,ghost", runner, apply=True)
        self.assertEqual(2, rc)
        self.assertEqual([["tiering", "--list"]], runner.calls)

    def test_empty_selection_exits_2_without_touching_the_cluster(self) -> None:
        runner = FakeRunner()
        rc = main(["--tables", ""], runner_factory=lambda b: runner)
        self.assertEqual(2, rc)
        self.assertEqual([], runner.calls)

    def test_failed_listing_exits_2(self) -> None:
        class Bad(FakeRunner):
            def __call__(self, args):
                self.calls.append(list(args))
                return CompletedProcess(list(args), 1, stdout="", stderr="unreadable")

        runner = Bad()
        rc = main(["--tables", "raw_table_1"], runner_factory=lambda b: runner)
        self.assertEqual(2, rc)


if __name__ == "__main__":
    unittest.main()
