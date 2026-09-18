#!/usr/bin/env python3
"""Unit tests for skip_inventory.py — CHG-220.

The certificate quotes unittest's summary line ("OK (skipped=11)") and records
nothing about which tests skipped: an accepted live-refusal and a silently
disabled test are indistinguishable in the record. The helper turns the
verbose log into one auditable line, and refuses to agree with a summary it
cannot itemize. Line shapes below are copied from a real `-v` run (2026-09-18).

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
"""

import contextlib
import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import skip_inventory as si  # noqa: E402

REASON_PIN = "DIGEST_PIN_LIVE=1 is unset — live registry opt-out"
REASON_CENSUS = "census withheld — census read 23 rows but Fluss reports 27"

SKIP_LOG = (
    f"test_a (t.Case.test_a) ... skipped '{REASON_PIN}'\n"
    "test_b (t.Case.test_b)\n"
    f"Docstring instead of the id. ... skipped '{REASON_CENSUS}'\n"
    "test_c (t.Case.test_c) ... skipped ''\n"
    f"test_d (t.Case.test_d) ... skipped '{REASON_PIN}'\n"
    "\n"
    "----------------------------------------------------------------------\n"
    "Ran 4 tests in 1.000s\n"
    "\n"
    "OK (skipped=4)\n"
)

CLEAN_LOG = "test_e (t.Case.test_e) ... ok\n\nRan 1 test in 0.001s\n\nOK\n"


class SummarizeTest(unittest.TestCase):
    """The parse: counts per reason, sorted by count then reason."""

    def test_counts_by_reason_and_sorts_by_count(self) -> None:
        report = si.summarize(SKIP_LOG)
        self.assertEqual((report.reported, report.found), (4, 4))
        self.assertEqual(
            report.reasons,
            [(2, REASON_PIN), (1, "(no reason given)"), (1, REASON_CENSUS)],
        )

    def test_clean_log_reports_zero(self) -> None:
        report = si.summarize(CLEAN_LOG)
        self.assertEqual((report.reported, report.found, report.reasons), (0, 0, []))

    def test_failed_summary_still_parses_its_skips(self) -> None:
        # The helper itemizes; the gate judges. A red suite's skips are still
        # real skips and must be named.
        text = (
            f"test_a (t.Case.test_a) ... skipped '{REASON_PIN}'\n"
            "test_b (t.Case.test_b) ... skipped ''\n"
            "Ran 2 tests in 0.100s\n\n"
            "FAILED (failures=1, skipped=2)\n"
        )
        report = si.summarize(text)
        self.assertEqual((report.reported, report.found), (2, 2))

    def test_chatter_mentioning_the_word_skipped_is_not_a_skip(self) -> None:
        # CHG-201's lesson: an unanchored pattern matched mid-line chatter.
        text = (
            "test_a (t.Case.test_a) ... ok\n"
            "DDL APPLY FAILED (exit 3)\n"
            "the sweep skipped the table and moved on\n"
            "Ran 1 test in 0.001s\n\nOK\n"
        )
        report = si.summarize(text)
        self.assertEqual((report.reported, report.found, report.reasons), (0, 0, []))


class FormatTest(unittest.TestCase):
    """The line the gate prints: one line, bounded, count first."""

    def test_zero_skips_reads_as_none(self) -> None:
        self.assertEqual(si.format_line(si.summarize(CLEAN_LOG)), "SKIPS: 0 — none")

    def test_counts_and_reasons_are_named(self) -> None:
        line = si.format_line(si.summarize(SKIP_LOG))
        self.assertTrue(line.startswith("SKIPS: 4 — "), line)
        self.assertIn(f"2× {REASON_PIN}", line)
        self.assertIn(f"1× {REASON_CENSUS}", line)
        self.assertIn("1× (no reason given)", line)
        self.assertEqual(len(line.splitlines()), 1)

    def test_long_reason_is_truncated_with_an_ellipsis(self) -> None:
        long_reason = "assembly " + "x" * 200
        text = (
            f"test_a (t.Case.test_a) ... skipped '{long_reason}'\n"
            "Ran 1 test in 0.001s\n\nOK (skipped=1)\n"
        )
        line = si.format_line(si.summarize(text))
        self.assertIn("…", line)
        self.assertNotIn(long_reason, line)
        self.assertLessEqual(len(line), 120)


class MainTest(unittest.TestCase):
    """Exit code: 0 only when the summary and the named skips agree."""

    def _run(self, log_text: str | None) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "python-tests.log")
            if log_text is not None:
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(log_text)
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = si.main([path])
            return rc, out.getvalue(), err.getvalue()

    def test_agreeing_log_exits_zero_and_prints_the_line(self) -> None:
        rc, out, _ = self._run(SKIP_LOG)
        self.assertEqual(rc, 0)
        self.assertTrue(out.startswith("SKIPS: 4 — "), out)

    def test_summary_claiming_more_skips_than_are_named_fails(self) -> None:
        # The safety property: a count nobody can itemize must never pass as
        # "audited". This is the exact hole CHG-220 closes.
        text = (
            f"test_a (t.Case.test_a) ... skipped '{REASON_PIN}'\n"
            "Ran 9 tests in 0.100s\n\nOK (skipped=3)\n"
        )
        rc, _, err = self._run(text)
        self.assertEqual(rc, 1)
        self.assertIn("3", err)
        self.assertIn("1", err)

    def test_log_without_any_summary_fails(self) -> None:
        rc, _, err = self._run("garbage\n")
        self.assertEqual(rc, 1)
        self.assertIn("summary", err)

    def test_missing_log_fails_loudly(self) -> None:
        rc, _, err = self._run(None)
        self.assertEqual(rc, 1)
        self.assertIn("no such", err.lower())


if __name__ == "__main__":
    unittest.main()
