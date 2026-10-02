#!/usr/bin/env python3
"""Pins for the parallel step pool in run-monday-gates.sh (CHG-519).

Seven of the 19 gate steps are stack-independent and independent of each other:
the python suites (3), the Go suite (5), the E2E binaries (6), the gateway suite
(14), the Rust suite (15), pin discipline (17) and the mock-arrow suite (19).
CHG-519 runs them as children of the same gate script — each with its own
summary fragment, self-limited by flock slot files — while the serial stack
chain proceeds. This file pins the shape so the pool cannot silently grow to
include a stack step, lose the step-6 dependency, re-run preflight under the
gate lock, or skip the fragment flush the verdict's banner count depends on.

The behavioural leg sources the real pool block against stub children and
checks the two outcomes the parent has to get right: every fragment reaches
SUMMARY.txt on success, and a non-zero child is recorded, flushed and reported
through gate_fail on failure.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
import unittest
from pathlib import Path

GATE = Path(__file__).resolve().parents[1] / "run-monday-gates.sh"
SRC = GATE.read_text(encoding="utf-8")

POOL_STEPS = ["3", "5", "6", "14", "15", "17", "19"]
# Steps that touch the live stack, the running containers or the reactor's
# certification surfaces: they stay in the serial chain.
SERIAL_STEPS = {"7", "8", "9", "10", "11", "12", "13", "16", "18"}

STEP_HEADER = re.compile(r"^if step_active (\d+); then$")


def _step_regions() -> dict[int, str]:
    regions: dict[int, list[str]] = {}
    current: int | None = None
    for line in SRC.splitlines():
        m = STEP_HEADER.match(line)
        if m:
            current = int(m.group(1))
            regions[current] = []
        elif current is not None:
            regions[current].append(line)
    return {n: "\n".join(body) for n, body in regions.items()}


FAKE_CHILD = r"""
set -euo pipefail
step=""
while [ $# -gt 0 ]; do
	case "$1" in
		--steps) step="$2"; shift 2 ;;
		*) shift ;;
	esac
done
sleep 0.2
{
	echo "=== [$step/19] fake step $step ==="
	if [ -n "${FAIL_STEP:-}" ] && [ "$step" = "$FAIL_STEP" ]; then
		echo "FAIL: fake step $step"
		exit 1
	fi
} >>"$GATE_POOL_SUMMARY"
exit 0
"""

HARNESS = r"""
set -euo pipefail
OUT_DIR="$1"
FAKE="$2"
FAIL_STEP="${3:-}"
BLOCK="$4"
SUMMARY="$OUT_DIR/SUMMARY.txt"
: >"$SUMMARY"
GATE_POOL_CHILD=""
GATE_POOL_OUT_DIR=""
GATE_POOL_SUMMARY=""
STEPS_SET=""
SWEEP=0
GATE_SWEEP_CHILD=""
GATE_PARALLEL=1
GATE_PARALLEL_JOBS=2
step_active() { return 0; }
gate_fail() { echo "VERDICT: FAIL"; exit 1; }
source "$BLOCK"
POOL_CHILD_CMD=(bash "$FAKE")
export FAIL_STEP
pool_launch_all
pool_drain_all
echo "DRAIN-OK"
"""


class PoolShape(unittest.TestCase):
    def test_step_count_is_still_19(self) -> None:
        self.assertEqual(SRC.count("if step_active "), 19,
                         "the pool must not add or remove a numbered step")

    def test_pool_steps_are_the_stack_independent_seven(self) -> None:
        m = re.search(r"^POOL_STEPS=\(([^)]*)\)$", SRC, re.M)
        self.assertIsNotNone(m, "POOL_STEPS is missing")
        self.assertEqual(m.group(1).split(), POOL_STEPS,
                         "the pooled set changed — re-check every dependency")

    def test_stack_steps_stay_serial(self) -> None:
        m = re.search(r"^POOL_STEPS=\(([^)]*)\)$", SRC, re.M)
        pool = set(m.group(1).split())
        overlap = pool & SERIAL_STEPS
        self.assertEqual(overlap, set(),
                         f"stack steps must not run concurrently with the chain: {overlap}")

    def test_dependency_on_e2e_binaries_waits_for_step_6(self) -> None:
        self.assertIn("pool_require_step 6", SRC,
                      "step 12/13 build on step 6's binaries; the sync is missing")
        self.assertLess(SRC.index("pool_require_step 6"),
                        SRC.index("if step_active 12; then"),
                        "the step-6 sync must run before step 12")

    def test_child_mode_skips_lock_and_preflight(self) -> None:
        self.assertIn('if [ -z "${GATE_SWEEP_CHILD:-}" ] && [ -z "$GATE_POOL_CHILD" ]; then', SRC,
                      "a pool child must not contend for the gate lock or re-run preflight")

    def test_child_mode_exits_before_the_verdict(self) -> None:
        child_exit = SRC.index('if [ -n "$GATE_POOL_CHILD" ]; then')
        verdict = SRC.index('STEPS_RUN="$(grep -cE')
        self.assertLess(child_exit, verdict,
                        "a pool child must not print a verdict or record a memo")

    def test_subset_and_sweep_default_to_serial(self) -> None:
        self.assertIn('GATE_PARALLEL="${GATE_PARALLEL:-auto}"', SRC)
        auto = SRC.index('if [ "$GATE_PARALLEL" = "auto" ]; then')
        body = SRC[auto:SRC.index("elif", auto)]
        self.assertIn('[ -z "$STEPS_SET" ]', body,
                      "auto mode must stay serial for --steps repair runs")
        self.assertIn('[ "${SWEEP:-0}" != "1" ]', body,
                      "auto mode must stay serial for sweeps")

    def test_cleanup_trap_kills_pool_children(self) -> None:
        self.assertIn("_gate_pool_cleanup", SRC)
        self.assertIn(
            "trap '_harness_abort; rm -f \"${_script_list:-}\"; _gate_pool_cleanup' EXIT", SRC,
            "an early exit would orphan pool children outside the gate lock")

    def test_children_self_limit_via_flock_slots(self) -> None:
        self.assertIn("GATE_PARALLEL_JOBS", SRC)
        self.assertIn("flock -n 8", SRC,
                      "the concurrency bound must be the flock slot files, not a busy-wait")

    def test_launch_uses_process_groups(self) -> None:
        launch = SRC[SRC.index("pool_launch() {"):SRC.index("pool_launch_all() {")]
        self.assertIn('"${POOL_SETSID[@]}"', launch,
                      "children need their own process group so cleanup kills their suites")
        self.assertIn("POOL_SETSID=(setsid)", SRC)

    def test_pooled_steps_never_count_skips_in_the_child(self) -> None:
        regions = _step_regions()
        for step in POOL_STEPS:
            body = regions[int(step)]
            self.assertNotIn("note_skip", body, f"step {step} skips must count in the parent")
            self.assertNotIn("warn_skip", body, f"step {step} skips must count in the parent")

    def test_fragments_flush_before_the_banner_count(self) -> None:
        self.assertIn("pool_drain_all", SRC)
        self.assertLess(SRC.index("pool_drain_all\n"),
                        SRC.index('STEPS_RUN="$(grep -cE'),
                        "the verdict's banner count needs every fragment flushed first")


class PoolBehaviour(unittest.TestCase):
    def _run(self, fail_step: str = "") -> tuple[subprocess.CompletedProcess, str, Path]:
        m = re.search(
            r"^# ── Parallel step pool \(GATE-PARALLEL-BEGIN\)[^\n]*\n(.*?)"
            r"^# ── Parallel step pool \(GATE-PARALLEL-END\)[^\n]*$",
            SRC, re.M | re.S)
        self.assertIsNotNone(m, "pool block markers are missing")
        block = m.group(1)
        td = Path(tempfile.mkdtemp(prefix="gate-pool-test-"))
        self.addCleanup(lambda: __import__("shutil").rmtree(td, ignore_errors=True))
        (td / "block.sh").write_text(block, encoding="utf-8")
        (td / "fake-child.sh").write_text(FAKE_CHILD, encoding="utf-8")
        (td / "harness.sh").write_text(HARNESS, encoding="utf-8")
        r = subprocess.run(
            ["bash", str(td / "harness.sh"), str(td), str(td / "fake-child.sh"),
             fail_step, str(td / "block.sh")],
            capture_output=True, text=True, timeout=120)
        summary = (td / "SUMMARY.txt").read_text(encoding="utf-8")
        return r, summary, td

    def test_success_flushes_every_fragment_and_verdict_stays_clean(self) -> None:
        r, summary, td = self._run()
        self.assertEqual(r.returncode, 0, f"harness failed:\n{r.stdout}\n{r.stderr}")
        self.assertIn("DRAIN-OK", r.stdout)
        for step in POOL_STEPS:
            self.assertIn(f"=== [{step}/19] fake step {step} ===", summary)
        self.assertNotIn("FAIL", summary)
        self.assertEqual(list(td.glob(".summary-step*")), [],
                         "fragments must be consumed, not left behind")

    def test_failure_is_flushed_and_reported_through_gate_fail(self) -> None:
        r, summary, _ = self._run(fail_step="5")
        self.assertEqual(r.returncode, 1, f"expected the gate to fail:\n{r.stdout}\n{r.stderr}")
        self.assertIn("VERDICT: FAIL", r.stdout)
        self.assertIn("=== [5/19] fake step 5 ===", summary,
                      "the failing child's fragment must reach SUMMARY.txt")
        self.assertIn("FAIL: fake step 5", summary)
        self.assertIn("FAIL: pooled step 5", summary,
                      "the parent must name the pooled step it failed on")


if __name__ == "__main__":
    unittest.main()
