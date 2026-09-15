"""Wave-30 regression tests for chaos-run.sh (P6-058, 059, 344, 345, 346, 721).

The runner is exercised as the real entry point inside a throwaway tree: a copy
of `chaos-run.sh` under `<tmp>/tree/code/01_platform/04_scripts/chaos/`, the four
drill scripts replaced by stubs whose exit code and output come from files, and
`date`, `shellcheck` and `tee` optionally shimmed through a PATH prefix. The
runner derives its `logs/chaos/...` evidence directory from its own location, so
every artefact lands in the throwaway tree and nothing touches the repository.

Two pins are structural rather than behavioural (single inventory, dead RESULTS
array) — they are labelled `static` in the test name and read the script text.

Red leg: every assertion marked `# disc` fails against the pre-wave script.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

BASH = shutil.which("bash") or "/usr/bin/bash"

DRILLS = [
    "chaos-01-slot-kill.sh",
    "chaos-02-tm-kill.sh",
    "chaos-03-tablet-kill.sh",
    "chaos-04-vm-loss.sh",
]
STUB = """#!/usr/bin/env bash
# Stub drill: prints the recorded output, exits the recorded code.
echo "stub ${0##*/} running"
if [[ -s "${CHILD_DIR}/out-${0##*/}.txt" ]]; then
  cat "${CHILD_DIR}/out-${0##*/}.txt"
fi
exit "$(cat "${CHILD_DIR}/rc-${0##*/}.txt" 2>/dev/null || echo 0)"
"""


def _source_tree() -> Path:
    """The repo root: nearest ancestor holding the chaos runner."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "code" / "01_platform" / "04_scripts" / "chaos").is_dir():
            return parent
    return here.parent


SOURCE_TREE = Path(os.environ.get("W30_RUN_TREE", _source_tree()))


class ChaosRunTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="w30-chaosrun-")
        self.tmp = Path(self._tmp)
        self.tree = self.tmp / "tree"
        self.chaos = self.tree / "code" / "01_platform" / "04_scripts" / "chaos"
        self.chaos.mkdir(parents=True)
        self.runner = self.chaos / "chaos-run.sh"
        shutil.copyfile(SOURCE_TREE / "code" / "01_platform" / "04_scripts" / "chaos" / "chaos-run.sh", self.runner)
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        self.children = self.tmp / "children"
        self.children.mkdir()
        for drill in DRILLS:
            stub = self.chaos / drill
            stub.write_text(STUB)
            stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
            self.set_child(drill, rc=0)
        self.write_shim("date", '#!/usr/bin/env bash\necho "${SHIM_DATE:-20260915-120000}"\n')

    def tearDown(self) -> None:
        subprocess.run(["rm", "-rf", self._tmp], check=False)

    # --- helpers ---------------------------------------------------------

    def write_shim(self, name: str, body: str) -> Path:
        path = self.bin / name
        path.write_text(body)
        path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        return path

    def set_child(self, drill: str, rc: int, output: str = "") -> None:
        (self.children / f"rc-{drill}.txt").write_text(f"{rc}\n")
        (self.children / f"out-{drill}.txt").write_text(output)

    def env(self, **overrides) -> dict:
        base = {
            "CHILD_DIR": str(self.children),
            "SHIM_DATE": "20260915-120000",
            "PATH": f"{self.bin}:{os.environ['PATH']}",
        }
        base.update({k: str(v) for k, v in overrides.items()})
        return {**os.environ, **base}

    def run_runner(self, **env) -> subprocess.CompletedProcess:
        return subprocess.run(
            [BASH, str(self.runner)], capture_output=True, text=True, timeout=120, env=self.env(**env),
        )

    def output(self, r: subprocess.CompletedProcess) -> str:
        return r.stdout + r.stderr

    def evidence_dirs(self) -> list[Path]:
        root = self.tree / "logs" / "chaos"
        if not root.is_dir():
            return []
        return sorted(p for p in root.iterdir() if p.is_dir())

    def summaries(self) -> list[str]:
        return [str(d / "SUMMARY.txt") for d in self.evidence_dirs()]

    def summary_text(self, index: int = -1) -> str:
        return Path(self.summaries()[index]).read_text()


class VerdictTest(ChaosRunTestCase):
    """P6-059: the runner's own verdict must not overstate what was proven."""

    def test_all_pass_is_a_pass(self) -> None:
        r = self.run_runner()
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("CHAOS-SUITE: RESULT=PASS EXIT=0", r.stdout)
        for idx in (1, 2, 3, 4):
            self.assertIn(f"RESULT [{idx}]: PASS", r.stdout)

    def test_a_failure_fails_the_suite(self) -> None:
        self.set_child("chaos-03-tablet-kill.sh", rc=1)
        r = self.run_runner()
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("CHAOS-SUITE: RESULT=FAIL EXIT=1", r.stdout)
        self.assertIn("03-tablet-kill: FAIL", self.summary_text())

    def test_a_skip_does_not_fail_the_suite(self) -> None:
        self.set_child("chaos-04-vm-loss.sh", rc=3)
        r = self.run_runner()
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("RESULT [4]: SKIP (exit 3)", r.stdout)
        self.assertIn("04-vm-loss: SKIP", self.summary_text())

    def test_every_skip_is_not_a_pass(self) -> None:  # disc (P6-059)
        # 4xSKIP used to print RESULT=PASS EXIT=0: no drill ran, yet the suite
        # claimed resilience.
        for drill in DRILLS:
            self.set_child(drill, rc=3)
        r = self.run_runner()
        self.assertEqual(3, r.returncode, self.output(r))
        self.assertIn("CHAOS-SUITE: RESULT=SKIP EXIT=3", r.stdout)
        self.assertIn("0 passed, 4 skipped — no coverage proven", r.stdout)
        self.assertNotIn("RESULT=PASS", r.stdout)

    def test_a_child_usage_error_is_a_failure_not_a_skip(self) -> None:
        # A misconfigured drill (its own exit 2) is not an absent prerequisite.
        # Holds pre-fix as well (only 0/3 were special-cased) — a contract pin.
        self.set_child("chaos-02-tm-kill.sh", rc=2)
        r = self.run_runner()
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("RESULT [2]: FAIL (exit 2)", r.stdout)
        self.assertNotIn("RESULT [2]: SKIP", r.stdout)

    def test_a_named_leg_b_skip_is_reported_as_partial(self) -> None:  # disc (P6-058)
        # chaos-02 documents "PASS, or a named leg-B SKIP" and exits 0 for both,
        # so the runner used to call the skipped leg a plain PASS.
        self.set_child(
            "chaos-02-tm-kill.sh", rc=0,
            output="TM-KILL-CHAOS-02: [leg A] PASS — restore from checkpoint\n"
                   "TM-KILL-CHAOS-02: [leg B] SKIP — no flink-taskmanager container (stack not up)\n",
        )
        r = self.run_runner()
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("RESULT [2]: PARTIAL (exit 0)", r.stdout)
        self.assertIn("02-tm-kill: PARTIAL", self.summary_text())
        self.assertIn("passed=3 skipped=0 failed=0 partial=1 of 4", r.stdout)

    def test_the_headers_count_from_the_inventory(self) -> None:
        r = self.run_runner()
        self.assertIn("=== [1/4] 01-slot-kill ===", r.stdout)
        self.assertIn("=== [4/4] 04-vm-loss ===", r.stdout)
        self.assertNotIn("[5/4]", r.stdout)


class EvidenceTest(ChaosRunTestCase):
    """P6-344 / P6-721: the evidence directory and the evidence itself."""

    def test_two_runs_in_the_same_second_do_not_share_a_logdir(self) -> None:  # disc (P6-344)
        # `date` is pinned, so without a PID/mktemp suffix both runs share
        # LOGDIR and clobber each other's SUMMARY.txt.
        self.run_runner()
        self.run_runner()
        dirs = self.evidence_dirs()
        self.assertEqual(2, len(dirs), [str(d) for d in dirs])
        for d in dirs:
            self.assertRegex(d.name, r"^chaos-20260915-120000-\d+$")

    def test_an_unwritable_evidence_root_fails_loudly(self) -> None:  # disc (P6-344)
        # `mkdir -p` used to die under `set -e` with no message at all.
        (self.tree / "logs").write_text("not a directory\n")
        r = self.run_runner()
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("cannot create the evidence directory", r.stderr)

    def test_an_incomplete_child_log_is_an_evidence_failure(self) -> None:  # disc (P6-721)
        # Only the child's exit code was checked, so a failed `tee` left a
        # truncated log and the suite still reported success.
        self.write_shim(
            "tee",
            '#!/usr/bin/env bash\n'
            'case "$*" in\n'
            '  *SUMMARY.txt*) exec /usr/bin/tee "$@" ;;\n'
            'esac\n'
            'cat >/dev/null\n'  # drain, so the child is not SIGPIPE'd
            'exit 1\n',
        )
        r = self.run_runner()
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("EVIDENCE FAIL", r.stdout)
        self.assertIn("CHAOS-SUITE: RESULT=FAIL EXIT=1", self.summary_text())

    def test_each_drill_gets_its_own_log(self) -> None:
        self.run_runner()
        for drill in DRILLS:
            log = self.evidence_dirs()[0] / f"{drill[6:-3]}.log"
            self.assertTrue(log.exists(), f"{log} missing")
        self.assertIn("stub chaos-03-tablet-kill.sh running", (self.evidence_dirs()[0] / "03-tablet-kill.log").read_text())


class SelfCheckTest(ChaosRunTestCase):
    """P6-345: the static self-check must be visible and must not vanish."""

    def test_shellcheck_diagnostics_reach_the_summary(self) -> None:  # disc (P6-345)
        # shellcheck writes to stdout; the old `2>>"${SUMMARY}"` dropped it, so
        # the audit trail recorded a self-check failure with no reason.
        self.write_shim(
            "shellcheck",
            '#!/usr/bin/env bash\nprintf "%s\\n" "SC-DIAGNOSTIC-ON-STDOUT"\nexit 1\n',
        )
        r = self.run_runner()
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("SELF-CHECK FAIL", r.stdout)
        self.assertIn("SC-DIAGNOSTIC-ON-STDOUT", self.summary_text())

    def test_a_missing_shellcheck_is_announced(self) -> None:  # disc (P6-345)
        r = self.run_runner(PATH=str(self.path_without("shellcheck")))
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("SELF-CHECK WARN: shellcheck not installed", r.stdout)

    def test_a_broken_drill_fails_the_self_check(self) -> None:
        (self.chaos / "chaos-02-tm-kill.sh").write_text("#!/usr/bin/env bash\nif then\n")
        r = self.run_runner()
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("SELF-CHECK FAIL", r.stdout)
        self.assertIn("(self-check)", r.stdout)
        self.assertNotIn("RESULT [1]", r.stdout)  # nothing ran

    def path_without(self, *names: str) -> Path:
        """Every real /usr/bin and /bin entry except `names`, plus this test's shims."""
        narrowed = self.tmp / f"without-{'-'.join(names)}"
        narrowed.mkdir(exist_ok=True)
        excluded = set(names)
        for source in (Path(self.bin), Path("/usr/bin"), Path("/bin")):
            if not source.is_dir():
                continue
            for entry in source.iterdir():
                if entry.name in excluded:
                    continue
                target = narrowed / entry.name
                if target.is_symlink() or target.exists():
                    continue
                target.symlink_to(entry)
        return narrowed


class StructureTest(ChaosRunTestCase):
    """P6-346 / P6-721: pins on the runner's own text (structural, not behavioural)."""

    def script_text(self) -> str:
        return (self.chaos / "chaos-run.sh").read_text()

    def test_static_one_inventory_drives_both_loops(self) -> None:  # disc (P6-346)
        text = self.script_text()
        self.assertIn('TOTAL="${#TESTS[@]}"', text)
        self.assertNotIn('run_one "1"', text)
        self.assertNotIn('run_one "4"', text)
        # the drift the finding describes: each script named once (in the
        # inventory) instead of again in the self-check loop and the run order.
        for drill in DRILLS:
            self.assertEqual(1, text.count(drill), f"{drill} is listed more than once")

    def test_static_the_dead_results_array_is_gone(self) -> None:  # disc (P6-721)
        self.assertNotIn("RESULTS", self.script_text())


if __name__ == "__main__":
    unittest.main()
