"""Unit tests for the stale-claim scanner's live-claim hardening (2026-08-18,
CHG-033 follow-up): "now/current N" claims must read as CURRENT state
regardless of nearby date markers, and bare "now N/M/K" suite-triple claims
are a distinct failing claim type (the 340/234 masking class).

Run via: python3 -m unittest discover -s code/01_platform/04_scripts/tests
"""

import pathlib
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import stale_table_kind_scan as s


def scan_text(text: str) -> list:
    """Scan a single synthetic markdown file and return its hits."""
    with tempfile.TemporaryDirectory() as d:
        path = pathlib.Path(d) / "synthetic.md"
        path.write_text(text, encoding="utf-8")
        return s.scan_file(path)


def claim_tiers(hits: list) -> set:
    """{(claim_type, tier_label)} over the hits."""
    labels = {v: k for k, v in s.TIER_RANK.items()}
    return {(t, labels[r]) for r, _, t, _, _ in hits}


class LiveClaimClassificationTests(unittest.TestCase):
    """The masking class: a live "now/current N" count next to an unrelated
    date marker must NOT be read as "at that time"."""

    def test_now_claim_next_to_date_is_live_stale(self):
        # The exact 2026-08-18 masking case — the "now" clause carries both a
        # bare suite triple (234/0/8) and a module count (common 340), each of
        # which used to ride on the adjacent 2026-08-15 marker.
        hits = scan_text(
            "The 2026-08-15 audit verified the suites are green — "
            "now 234 /0/8-skips, common 340/0/1-skip, Go bridge PASS.\n")
        tiers = claim_tiers(hits)
        # "common 340" — a live count, must be LIVE-STALE, never LINE-ANNOTATED.
        self.assertIn(("test-count-stale", "LIVE-STALE"), tiers)
        # "now 234 /0/8-skips" — the bare suite triple, a distinct failing type.
        self.assertIn(("live-count-stale", "LIVE-STALE"), tiers)
        self.assertNotIn(("test-count-stale", "LINE-ANNOTATED"), tiers)

    def test_transition_claim_after_live_marker_is_a_change_record(self):
        # 2026-09-22: a change note ("common 735→739 on 2026-09-21") inserted right
        # after the C6 truth triple inherited the live "current" marker and
        # reported LIVE-STALE. "N→M" is a change record, so the claim span
        # annotates itself — the same way KIND_CHANGE annotates a reverted table
        # kind — and the live-marker rule must not apply to it.
        hits = scan_text(
            "> **2026-09-14 current test truth:** unit suites green 739/475/530 "
            "(common 735→739 on 2026-09-21 — the third-column parser case, "
            "CHG-290; then 736→739 with the manifest stamp rule, CHG-291)\n")
        tiers = claim_tiers(hits)
        self.assertIn(("test-count-stale", "LINE-ANNOTATED"), tiers)
        self.assertNotIn(("test-count-stale", "LIVE-STALE"), tiers)
        # Control: the same construction WITHOUT the arrow is still a live claim.
        ctrl = scan_text(
            "> **2026-09-14 current test truth:** unit suites green 739/475/530 "
            "(common 735 on 2026-09-21, CHG-290)\n")
        self.assertIn(("test-count-stale", "LIVE-STALE"), claim_tiers(ctrl))

    def test_number_first_live_claim_unmasked(self):
        # "current ... N common" (number-first) was invisible to the old regex;
        # now it is a claim AND a live claim.
        hits = scan_text(
            "the current default-run totals are 340 common / 0 failures / 1 skip\n")
        self.assertIn(("test-count-stale", "LIVE-STALE"), claim_tiers(hits))

    def test_truth_live_counts_are_filtered(self):
        # Once fixed to the current truth, the same constructions stay silent.
        # Truth is DERIVED from the scanner constants so a future truth bump
        # cannot silently re-red this gate (3rd occurrence of this failure class).
        ing = s.SUITE_TRIPLE_TRUTH["ingestion"]
        com = s.SUITE_TRIPLE_TRUTH["common"]
        hits = scan_text(
            "The 2026-08-15 audit verified the suites are green — "
            f"now {ing[0]}/0/{ing[2]}-skips, common {com[0]}/0/{com[2]}-skip, Go bridge PASS.\n")
        self.assertEqual(hits, [])
        hits = scan_text(
            f"the current default-run totals are {com[0]} common / 0 failures / {com[2]} skip\n")
        self.assertEqual(hits, [])

    def test_dated_claim_without_live_marker_stays_annotated(self):
        # A genuinely dated measurement keeps the LINE-ANNOTATED tier.
        hits = scan_text("fresh runs 2026-08-13: ingestion 180/0/7 skipped, all green\n")
        self.assertEqual(claim_tiers(hits),
                         {("test-count-stale", "LINE-ANNOTATED")})

    def test_dates_after_august_2026_annotate(self):
        # NUMERIC_MARKER used to hardcode 2026-08-\d{2}, so a claim dated from
        # 2026-09 on had no recognized date annotation and was reported
        # UNANNOTATED (a failing tier) unless a CHG-/DEC- marker happened to sit
        # within NUMERIC_WINDOW. The annotation reads "the count at that time",
        # so it must not expire with the calendar.
        for date in ("2026-08-13", "2026-09-15", "2026-12-31", "2027-01-02"):
            with self.subTest(date=date):
                hits = scan_text(
                    f"fresh runs {date}: ingestion 180/0/7 skipped, all green\n")
                self.assertEqual(
                    claim_tiers(hits), {("test-count-stale", "LINE-ANNOTATED")},
                    f"a claim dated {date} must be LINE-ANNOTATED")

    def test_c6_citation_not_double_fired(self):
        # "current truth is N/N/N" is a C6-triple citation (checked by
        # C6_TRIPLE_CLAIM_TYPES), not a suite-triple live claim — derived from
        # C6_TRIPLE_TRUTH so a truth bump cannot break the silent path.
        c6 = "/".join(str(n) for n in s.C6_TRIPLE_TRUTH)
        hits = scan_text(
            f"the current truth is {c6} (docs-audit C6 line {c6})\n")
        self.assertEqual(hits, [])

    def test_now_that_is_not_a_live_marker(self):
        # P6-628: probe with a STALE triple. Using the current truth
        # (SUITE_TRIPLE_TRUTH) made this vacuous: the truth filter suppressed any
        # hit, so it passed even if "now that …" were wrongly a live marker.
        hits = scan_text(
            "now that the suite is common 340/0/1, nothing fires 2026-08-13\n")
        tiers = claim_tiers(hits)
        self.assertNotIn(("live-count-stale", "LIVE-STALE"), tiers)
        self.assertNotIn(("test-count-stale", "LIVE-STALE"), tiers)

    def test_status_word_current_is_not_live(self):
        # "manifest is current" is a status word, not a live-count modifier —
        # the nearby "21 tables as of … — now 24" claim stays date-annotated.
        hits = scan_text(
            "manifest is current, no DDL drift detected "
            "(21 tables as of 2026-08-10 — now 24)\n")
        self.assertEqual(claim_tiers(hits),
                         {("tables-count-stale", "LINE-ANNOTATED")})

    def test_now_has_prose_is_not_live(self):
        # "now has an implementing test (suite 330/0/17)" — the triple is too
        # far from the temporal "now" (gap > 25 chars) to be a live claim.
        hits = scan_text(
            "the full required set now has an implementing test "
            "(suite 330/0/17). Covered 2026-08-17\n")
        self.assertEqual(hits, [])


class LiveClaimVerdictTests(unittest.TestCase):
    """End-to-end: a stale live claim fails the gate; a clean tree passes."""

    def _main(self, *argv: str) -> int:
        old = sys.argv
        sys.argv = ["stale_table_kind_scan.py", *argv]
        try:
            return s.main()
        finally:
            sys.argv = old

    def test_stale_live_claim_fails_verdict(self):
        with tempfile.TemporaryDirectory() as d:
            (pathlib.Path(d) / "live.md").write_text(
                "suites green (ingestion 193 at 2026-08-15 — now 234 /0/8-skips, "
                "common 340/0/1-skip)\n", encoding="utf-8")
            self.assertEqual(self._main("--dir", d), 1)

    def test_clean_tree_passes_verdict(self):
        with tempfile.TemporaryDirectory() as d:
            ing = s.SUITE_TRIPLE_TRUTH["ingestion"]
            com = s.SUITE_TRIPLE_TRUTH["common"]
            (pathlib.Path(d) / "clean.md").write_text(
                "suites green (ingestion 193 at 2026-08-15 — "
                f"now {ing[0]}/0/{ing[2]}-skips, common {com[0]}/0/{com[2]}-skip)\n",
                encoding="utf-8")
            self.assertEqual(self._main("--dir", d), 0)


class SameLineAndDdlGateTests(unittest.TestCase):
    """Wave 12 (P6-573, P6-574, P6-793, P6-794): the DDL gate must report what it
    could not check, and same-line duplicates must not hide each other."""

    def test_table_count_phrase_is_not_a_test_count_claim(self):
        # P6-793: "24 common tables" is a table-count context. The first
        # alternative excluded it with (?!\s+tables?); the reversed form did not,
        # so it was reported as a stale test count (24 != TEST_COUNT_TRUTH).
        hits = scan_text("The repo now has 24 common tables in the catalog\n")
        self.assertEqual([t for _, _, t, _, _ in hits], [])
        # control: the same shape as a real claim still fires.
        control = scan_text("The suites are 24 common / 489 compute as of today\n")
        self.assertIn("test-count-stale", [t for _, _, t, _, _ in control])

    def test_same_line_duplicate_with_different_tiers_are_both_reported(self):
        # P6-794: the table-kind/phase-status dedup key omitted the tier, so the
        # second hit on a line was dropped. Here the first claim is live and the
        # second sits next to a "legacy" marker, more than KIND_WINDOW away.
        filler = " " * 105
        hits = scan_text(
            f"feature_candles_15s is a LOG table{filler}legacy note: "
            "feature_candles_15s is a LOG table\n")
        labels = {v: k for k, v in s.TIER_RANK.items()}
        tiers = [labels[r] for r, _, t, _, _ in hits if t == "feature_candles_15s-as-LOG"]
        self.assertEqual(sorted(tiers), ["LINE-ANNOTATED", "UNANNOTATED"],
                         "both the live and the annotated claim must be reported")

    def run_scan_ddl(self, ddl_dir: pathlib.Path):
        return subprocess.run(
            [sys.executable, str(pathlib.Path(s.__file__)), "--ddl", str(ddl_dir)],
            capture_output=True, text=True, timeout=60)

    def test_missing_manifest_is_reported_not_traced(self):
        # P6-573: this is a CI gate — a missing input must read as a finding.
        with tempfile.TemporaryDirectory() as d:
            out = self.run_scan_ddl(pathlib.Path(d))
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("is missing", out.stderr)
        self.assertNotIn("Traceback", out.stderr + out.stdout)

    def test_unusable_manifest_is_reported_not_traced(self):
        for payload in ('{"tables": ', '{"tables": []}', '{"tables": "nope"}',
                        '{"tables": [{"table_kind": "LOG"}]}'):
            with self.subTest(payload=payload):
                with tempfile.TemporaryDirectory() as d:
                    root = pathlib.Path(d)
                    (root / "schema_manifest.json").write_text(payload, encoding="utf-8")
                    out = self.run_scan_ddl(root)
                self.assertEqual(out.returncode, 1, out.stdout)
                self.assertIn("is unreadable", out.stderr)
                self.assertNotIn("Traceback", out.stderr + out.stdout)

    def test_unparseable_ddl_file_is_drift_not_a_silent_pass(self):
        # P6-574: a file with no CREATE TABLE was dropped from `parsed`, so the
        # parity check printed a PASS over an unverifiable file.
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            (root / "schema_manifest.json").write_text(
                '{"tables": [{"table_name": "raw_table_1", "table_kind": "LOG",'
                ' "primary_key": "instrument_token"}]}', encoding="utf-8")
            (root / "01_raw_table_1.sql").write_text(
                "-- this file has no CREATE TABLE statement\nSELECT 1;\n", encoding="utf-8")
            out = self.run_scan_ddl(root)
        self.assertEqual(out.returncode, 1, out.stdout + out.stderr)
        self.assertIn("every DDL file parses", out.stdout)
        self.assertIn("unparsed: 01_raw_table_1.sql", out.stdout)
        self.assertIn("[DRIFT]", out.stdout)


if __name__ == "__main__":
    unittest.main()
