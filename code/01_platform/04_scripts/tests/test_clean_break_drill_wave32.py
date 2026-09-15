"""Wave-32 regression tests for clean_break_drill.py plan validation (P6-348).

The runner is a small CLI, so this suite drives the REAL entry point as a
subprocess (`python3 clean_break_drill.py ...`) and asserts on its exit code and
stderr — no cluster is touched, and every case runs with --dry-run or stops
before any destructive step.

`W32_CLEAN_BREAK` points at the runner under test; it defaults to the real
repository file and exists so the red leg can run against the pre-wave copy.

Red leg: every assertion marked `# disc` fails against the pre-wave script.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "code" / "01_platform" / "04_scripts").is_dir():
            return parent
    return here.parent


ROOT = _repo_root()
RUNNER = Path(os.environ.get(
    "W32_CLEAN_BREAK", ROOT / "code" / "01_platform" / "04_scripts" / "clean_break_drill.py"
))

PLAN_ERROR = "'tables' must be a non-empty list of table names"


class CleanBreakPlanTest(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="w32-cb-"))
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))

    def plan_file(self, payload, name: str = "plan.json") -> Path:
        path = self.tmp / name
        if isinstance(payload, str):
            path.write_text(payload)
        else:
            path.write_text(json.dumps(payload))
        return path

    def run_drill(self, *args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(RUNNER), *args],
            capture_output=True,
            text=True,
            cwd=str(cwd or self.tmp),
            timeout=60,
        )

    # ---- P6-348: a string is not a list ---------------------------------
    def test_comma_string_is_a_plan_error(self) -> None:  # disc
        """`"tables": "A,B"` iterates characters — validation must reject it."""
        f = self.plan_file({"tables": "Signal_Candidates,Positions"})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn(PLAN_ERROR, r.stderr)

    def test_comma_string_never_becomes_char_table_names(self) -> None:  # disc
        """The failure mode was single-character table names in the record."""
        f = self.plan_file({"tables": "Signal_Candidates,Positions"})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertNotIn("tables: ", r.stdout)
        for ch in "Signal_Candidtes,":
            self.assertNotIn(f"  tables: {ch}\n", r.stdout)

    def test_scalar_int_is_a_plan_error(self) -> None:  # disc
        f = self.plan_file({"tables": 5})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn(PLAN_ERROR, r.stderr)
        self.assertNotIn("Traceback", r.stderr)

    def test_scalar_bool_is_a_plan_error(self) -> None:  # disc
        f = self.plan_file({"tables": True})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertNotIn("Traceback", r.stderr)

    def test_scalar_and_string_never_raise_type_error(self) -> None:  # disc
        """A TypeError here would exit 1/0, not the declared plan-error 2."""
        for payload in ({"tables": 5}, {"tables": True}, {"tables": 3.5}, {"tables": "A,B"}):
            f = self.plan_file(payload, f"tables-{abs(hash(str(payload)))}.json")
            r = self.run_drill("--plan-file", str(f), "--dry-run")
            self.assertEqual(r.returncode, 2, f"{payload}: {r.stdout}{r.stderr}")

    # ---- unchanged contracts ---------------------------------------------
    def test_well_formed_list_passes_dry_run(self) -> None:
        f = self.plan_file({"drill": "cb", "tables": ["Signal_Candidates", "Positions"]})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("tables: Signal_Candidates, Positions", r.stdout)
        self.assertIn("dry-run", r.stdout)

    def test_empty_list_is_rejected(self) -> None:
        f = self.plan_file({"tables": []})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)

    def test_non_string_element_is_rejected(self) -> None:
        f = self.plan_file({"tables": ["Signal_Candidates", 3]})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)

    def test_empty_string_element_is_rejected(self) -> None:
        f = self.plan_file({"tables": ["Signal_Candidates", ""]})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)

    def test_missing_tables_key_is_rejected(self) -> None:
        f = self.plan_file({"drill": "cb"})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)

    def test_no_approve_refuses_destructively(self) -> None:
        f = self.plan_file({"tables": ["Signal_Candidates"]})
        r = self.run_drill("--plan-file", str(f), "--out", str(self.tmp / "out"))
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
        self.assertIn("--approve", r.stderr)

    def test_missing_plan_file_is_plan_error(self) -> None:
        r = self.run_drill("--plan-file", str(self.tmp / "nope.json"), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)

    def test_malformed_json_is_plan_error(self) -> None:
        f = self.plan_file("{not json", "bad.json")
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)

    def test_approved_run_writes_evidence(self) -> None:
        out = self.tmp / "ev"
        f = self.plan_file({"drill": "cb-1", "tables": ["Signal_Candidates", "Positions"]})
        r = self.run_drill("--plan-file", str(f), "--approve", "--out", str(out))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        files = list(out.glob("*-clean-break-drill.json"))
        self.assertEqual(len(files), 1, list(out.iterdir()))
        record = json.loads(files[0].read_text())
        self.assertEqual(record["status"], "APPROVED")
        self.assertEqual(record["plan"]["tables"], ["Signal_Candidates", "Positions"])
        self.assertIn("RESULT=APPROVED EXIT=0", r.stdout)

    def test_default_source_logs_map_each_table_to_itself(self) -> None:
        f = self.plan_file({"tables": ["Positions"]})
        r = self.run_drill("--plan-file", str(f), "--dry-run")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("'Positions': 'Positions'", r.stdout)


if __name__ == "__main__":
    unittest.main()
