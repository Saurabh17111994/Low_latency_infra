#!/usr/bin/env python3
"""Wave 36 — the harness has an entry point, and its guards run in the gate.

`holistic-measure.sh` produced the end-to-end zero-loss evidence, but nothing
named it: no Makefile target, no entry in docs/commands/COMMANDS.md, no CI job,
and no other script called it. It was reachable only by someone who already knew
the path and its knobs — and because nothing ran it, a commit that retired the
15s candle path (`0f3e5952`, which touched no file under `04_scripts`) left the
harness asserting a counter its own job graph no longer produced. Nobody was
told; the breakage surfaced months later by hand.

The same shape applies to `test-pipeline-lib.sh`: its 131 guards pin
`pipeline-lib.sh`, but its ONLY caller is the harness's own preflight
(`pipeline-lib.sh` G27c). Every other script that uses the library
(`tm-kill-full-load.sh`, `stage-soak-e2e.sh`, the soak scripts) can contradict a
guard in a change where nothing runs the guards.

These tests pin the two halves of the fix: the gate runs the guards (and cannot
be satisfied by a suite that reports a failure), and the harness is named in the
Makefile and the command reference.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
REPO = SCRIPTS.parents[2]
GATE = SCRIPTS / "run-monday-gates.sh"
GUARDS = SCRIPTS / "test-pipeline-lib.sh"
MAKEFILE = REPO / "Makefile"
COMMANDS = REPO / "docs/commands/COMMANDS.md"


class GateRunsTheHarnessGuards(unittest.TestCase):
    """The gate must RUN the guards, and must not be satisfied by a suite that
    reports a failure. The behavioural half executes the gate's own block text
    (extracted from run-monday-gates.sh) against stub guard suites, so the test
    cannot drift from the shipped code the way a pattern-only pin would.
    """

    @classmethod
    def setUpClass(cls):
        rest = GATE.read_text().split("Harness guards (test-pipeline-lib.sh)", 1)[1]
        block = rest.split("\n", 1)[1].split("# \u2500\u2500 0b.", 1)[0]
        assert "test-pipeline-lib.sh" in block, "extracted the wrong region"
        cls.block = block

    def _run(self, guards_body, guards_rc=0):
        """Run the gate's block verbatim with a stub suite and a stub gate_fail."""
        tmp = Path(tempfile.mkdtemp(prefix="wave36-gate-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        stub = tmp / "test-pipeline-lib.sh"
        stub.write_text(f"#!/usr/bin/env bash\n{guards_body}\nexit {guards_rc}\n")
        preamble = (
            "set -euo pipefail\n"
            f'SCRIPT_DIR="{tmp}"\n'
            f'SUMMARY="{tmp}/SUMMARY.txt"\n'
            f'GUARD_LOG="{tmp}/guards.log"\n'
            f'gate_fail() {{ echo "GATE_FAIL" >> "{tmp}/gatefail.txt"; exit 1; }}\n'
        )
        res = subprocess.run(["bash", "-c", preamble + self.block],
                             capture_output=True, text=True, timeout=120)
        return tmp, res

    def _summary(self, tmp):
        return (tmp / "SUMMARY.txt").read_text()

    def _gate_failed(self, tmp):
        f = tmp / "gatefail.txt"
        return f.exists() and "GATE_FAIL" in f.read_text()

    # ── the gate invokes it ─────────────────────────────────────────────────
    def test_the_gate_runs_the_guard_suite(self):
        self.assertIn('bash "$SCRIPT_DIR/test-pipeline-lib.sh"', GATE.read_text(),
                      "the harness guard suite is not run by the gate")

    # ── behaviour: a clean suite passes, and says so ────────────────────────
    def test_a_clean_suite_passes_and_the_gate_says_so(self):
        tmp, res = self._run('echo "guards: 131 passed, 0 failed"', guards_rc=0)
        self.assertEqual(0, res.returncode, res.stderr)
        self.assertFalse(self._gate_failed(tmp), "a clean suite failed the gate")
        self.assertIn("PASS: harness guards (guards: 131 passed, 0 failed", self._summary(tmp))

    # ── behaviour: a red suite fails the gate ───────────────────────────────
    def test_a_red_suite_fails_the_gate(self):
        tmp, res = self._run('echo "guards: 130 passed, 1 failed"', guards_rc=1)
        self.assertTrue(self._gate_failed(tmp), "a red guard suite did not fail the gate")
        self.assertIn("FAIL: harness guards", self._summary(tmp))

    # ── behaviour: exit 0 with a failure reported is still a failure ────────
    def test_a_suite_that_exits_zero_with_a_failure_is_refused(self):
        """Fail closed. A suite that stopped checking but still exits 0 must not
        buy the gate a PASS — the same shape as the Python step's 'Ran 0 tests'
        refusal. Pin the COMPARISON, and prove it by running it."""
        tmp, res = self._run('echo "guards: 6 passed, 1 failed"', guards_rc=0)
        self.assertTrue(self._gate_failed(tmp),
                        "the gate accepted a suite that reported 1 failed")
        self.assertNotIn("PASS: harness guards", self._summary(tmp))

    def test_a_suite_that_crashes_after_a_clean_summary_still_fails(self):
        """Exit code and verdict are checked INDEPENDENTLY.

        A suite that prints a clean summary and then dies (a trap firing, a
        `set -e` abort on the way out) reports `0 failed` on a run that did not
        finish. Only the exit-code half catches it, so that half must exist.
        """
        tmp, _ = self._run('echo "guards: 131 passed, 0 failed"', guards_rc=1)
        self.assertTrue(self._gate_failed(tmp),
                        "a suite that exited non-zero (but printed a clean line) "
                        "was accepted by the gate")

    def test_a_suite_that_reports_nothing_is_refused(self):
        tmp, _ = self._run('echo "nothing to see here"', guards_rc=0)
        self.assertTrue(self._gate_failed(tmp),
                        "the gate accepted a suite with an unparseable verdict")

    def test_the_suite_it_gates_still_reports_a_full_pass(self):
        """The real suite the gate runs: parseable, non-empty, fully green."""
        res = subprocess.run(["bash", str(GUARDS)], capture_output=True, text=True,
                             timeout=300)
        self.assertEqual(0, res.returncode, res.stdout[-2000:] + res.stderr[-2000:])
        m = re.search(r"guards: (\d+) passed, (\d+) failed", res.stdout)
        self.assertIsNotNone(m, "the guard suite no longer prints a parseable verdict")
        self.assertGreater(int(m.group(1)), 0, "the guard suite checked nothing")


class HarnessHasAnEntryPoint(unittest.TestCase):
    def test_the_makefile_names_the_harness(self):
        mk = MAKEFILE.read_text()
        self.assertRegex(mk, r"(?m)^holistic:\n\t@?bash code/01_platform/04_scripts/holistic-measure\.sh$",
                         "there is no `make holistic` target for the harness")
        self.assertRegex(mk, r"(?m)^holistic-quick:\n\t@?SMOKE_S=60 MAIN_S=300 bash ",
                         "there is no short-phase `make holistic-quick` target")

    def test_both_targets_are_phony(self):
        """A same-named file would silently shadow the target otherwise."""
        phony = " ".join(re.findall(r"(?m)^\.PHONY:.*$", MAKEFILE.read_text()))
        for t in ("holistic", "holistic-quick"):
            self.assertIn(t, phony.split(), f"{t} is missing from .PHONY")

    def test_the_command_reference_documents_it(self):
        # Pin the table CELL, not a substring: "make holistic-quick" (the other
        # row) also contains "make holistic", so a rename of the long form would
        # still satisfy a substring check.
        self.assertIn("| `make holistic` |", COMMANDS.read_text(),
                      "docs/commands/COMMANDS.md does not name the harness")


if __name__ == "__main__":
    unittest.main(verbosity=2)
