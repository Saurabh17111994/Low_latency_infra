"""L2-2: plan_tracker is a gate consumer, and every live tracker's roll-up must be fresh.

The finding: nothing under the gate ever ran `plan_tracker.py`, so the roll-up
tables it exists to keep derived could silently drift; and the marker predicate
was `startswith("- [")` at column 0, so an indented (nested) marker vanished
from the count. Both are pinned here.

Discovery is anchored on the tracker's own contract — a `## 0. Live tracker`
heading on its own line, plus a `## Overview` section after it — not on a
substring search, so a prose mention cannot enroll or drop a plan. The unit
tests below pin the marker rules the count depends on (indented markers count,
prose does not, unknown markers abort, a stale table exits 1 and round-trips
through --write).

Gate step 3 auto-discovers `test_*.py`, so this file joins the shield with no
gate-script change and no new numbered step.
"""
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
SCRIPT = ROOT / "code/01_platform/04_scripts/plan_tracker.py"
PLANS = ROOT / "docs/plans"
# The live trackers AGENTS.md names as the standing execution protocol.
AGENTS_NAMED = (
    "2026-09-22-fluss-1.0-upgrade.md",
    "2026-09-22-fluss-1.0-native-adoption.md",
)

TRACKER_RE = re.compile(r"(?m)^## 0\. Live tracker[ \t]*$")
OVERVIEW_RE = re.compile(r"(?m)^## Overview(?:\s|$)")


def is_live_tracker(text: str) -> bool:
    """The discovery predicate, anchored: heading on its own line, Overview after it."""
    match = TRACKER_RE.search(text)
    return bool(match and OVERVIEW_RE.search(text, match.end()))


def tracker_plans() -> list[Path]:
    return [p for p in sorted(PLANS.glob("*.md")) if is_live_tracker(p.read_text(encoding="utf-8"))]


def run_tracker(plan: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--plan", str(plan), *args],
        capture_output=True,
        text=True,
    )


def fixture(groups: str, rollup: str | None = None) -> str:
    """A minimal live-tracker plan: heading, roll-up table, Overview, detail groups.

    The default roll-up is deliberately a placeholder row, so `--check` reports
    it stale until `--write` derives the real table.
    """
    if rollup is None:
        rollup = (
            "| Stage | Tasks | done | wip | todo | live | decide | skip |\n"
            "|---|---|---|---|---|---|---|---|\n"
            "| placeholder | 0 | 0 | 0 | 0 | 0 | 0 | 0 |\n"
        )
    return (
        "# Fixture plan\n\n"
        "## 0. Live tracker\n\n"
        "**Roll-up**\n\n" + rollup + "\n"
        + groups
        + "\n## Overview\n\nSee the prose that mentions a - [x] marker mid-line; it is not one.\n"
    )


class LiveTrackerDiscovery(unittest.TestCase):
    """The gate consumer: every live tracker in the tree is fresh, and discovery bites."""

    def test_discovery_finds_the_agents_named_trackers(self):
        names = [p.name for p in tracker_plans()]
        self.assertGreaterEqual(len(names), 2, f"discovered trackers: {names}")
        for expected in AGENTS_NAMED:
            self.assertIn(expected, names, f"discovered trackers: {names}")

    def test_every_live_tracker_roll_up_is_fresh(self):
        plans = tracker_plans()
        self.assertTrue(plans, "no live trackers discovered — discovery is broken, not the plans")
        for plan in plans:
            proc = run_tracker(plan, "--check")
            self.assertEqual(
                proc.returncode,
                0,
                f"{plan.relative_to(ROOT)} roll-up is stale — run plan_tracker.py --write: "
                f"{proc.stdout}{proc.stderr}",
            )
            self.assertIn("OK", proc.stdout, f"{plan}: {proc.stdout}{proc.stderr}")

    def test_prose_mentions_do_not_enroll_a_plan(self):
        # A heading quoted mid-sentence, or indented, is prose — not a tracker section.
        self.assertFalse(is_live_tracker("we discussed ## 0. Live tracker in review and ## Overview\n"))
        self.assertFalse(is_live_tracker("  ## 0. Live tracker\n\n## Overview\n"))
        # And a real heading without the Overview anchor is not enrolled either.
        self.assertFalse(is_live_tracker("## 0. Live tracker\n\n**Roll-up**\n| a |\n"))

    def test_anchored_discovery_accepts_the_real_shape(self):
        self.assertTrue(is_live_tracker("## 0. Live tracker\n\ntext\n\n## Overview\n\nbody\n"))


class MarkerRules(unittest.TestCase):
    """The count/validation rules the roll-up depends on."""

    def test_indented_markers_are_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.md"
            plan.write_text(
                fixture("#### A\n\n- [x] top level\n  - [ ] nested detail\n"),
                encoding="utf-8",
            )
            write = run_tracker(plan, "--write")
            self.assertEqual(write.returncode, 0, write.stderr)
            text = plan.read_text(encoding="utf-8")
            self.assertIn("| A | 2 | 1 | 0 | 1 | 0 | 0 | 0 |", text)
            self.assertIn("| **Total** | **2** |", text)
            self.assertEqual(run_tracker(plan, "--check").returncode, 0)

    def test_indented_unknown_marker_exits_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.md"
            plan.write_text(
                fixture("#### A\n\n- [ ] fine\n  - [!] not a marker\n"),
                encoding="utf-8",
            )
            proc = run_tracker(plan, "--write")
            self.assertEqual(proc.returncode, 2, proc.stdout)
            self.assertIn("unknown marker", proc.stderr)

    def test_mid_line_marker_text_is_not_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.md"
            plan.write_text(
                fixture("#### A\n\nSome prose that says the - [x] marker.\n- [ ] real task\n"),
                encoding="utf-8",
            )
            proc = run_tracker(plan, "--write")
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("| A | 1 | 0 | 0 | 1 | 0 | 0 | 0 |", plan.read_text(encoding="utf-8"))

    def test_stale_table_exits_1_and_write_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.md"
            plan.write_text(fixture("#### A\n\n- [x] done one\n"), encoding="utf-8")
            stale = run_tracker(plan, "--check")
            self.assertEqual(stale.returncode, 1, stale.stdout)
            self.assertIn("STALE", stale.stderr)

            write = run_tracker(plan, "--write")
            self.assertEqual(write.returncode, 0, write.stderr)
            text = plan.read_text(encoding="utf-8")
            self.assertNotIn("placeholder", text, "the stale row must be replaced")
            self.assertIn("| A | 1 | 1 | 0 | 0 | 0 | 0 | 0 |", text)
            self.assertEqual(run_tracker(plan, "--check").returncode, 0)


if __name__ == "__main__":
    unittest.main()
