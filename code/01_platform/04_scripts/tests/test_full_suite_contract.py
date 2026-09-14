#!/usr/bin/env python3
"""Static pins for run-full-suite.sh decisions that only a live run can exercise.

The helper tests (test_full_suite_helpers.py) drive the parts of the runner that
are pure functions. The rest of the wave-15 findings are about shape: which
branch exists, what is not on a command line, what the last statement of the
script is. Those are pinned here by reading the script, so a later edit that
undoes one of them fails in the fast suite rather than in a 7-hour soak.

Provenance: wave 15, P6-015/016/178/179/182/183/513/514/519/520/774.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SUITE = ROOT / "code/01_platform/04_scripts/run-full-suite.sh"
SRC = SUITE.read_text(encoding="utf-8")


class SuiteContract(unittest.TestCase):
    def test_a_failed_run_exits_non_zero(self) -> None:
        """P6-016: FAIL written to SUMMARY.txt with exit 0 hides every regression."""
        self.assertRegex(SRC, r'\[ "\$RESULT" = PASS \] \|\| exit 1')
        tail = [l.strip() for l in SRC.splitlines() if l.strip() and not l.strip().startswith("#")]
        self.assertEqual(tail[-1], "exit 0", "the PASS path must still exit 0")

    def test_the_destructive_drop_needs_an_optin_and_a_local_broker(self) -> None:
        """P6-015/518: table drops are irreversible; a failed dropper is not a pass."""
        self.assertIn("ALLOW_DESTRUCTIVE_DROP", SRC)
        self.assertRegex(SRC, r"refusing to drop owned tables")
        self.assertRegex(SRC, r"refusing destructive drop against non-local FLUSS_BOOTSTRAP")
        self.assertNotRegex(SRC, r"\$DROPPER[^\n]*2>/dev/null",
                            "the dropper must not hide its stderr")

    def test_an_abort_cleans_up_and_stops_its_container(self) -> None:
        """P6-178/183: no leaked broker, monitor, java or container."""
        self.assertIn("trap cleanup EXIT", SRC)
        self.assertRegex(SRC, r"CONTAINER_STARTED=1")
        self.assertRegex(SRC, r'\$\{CONTAINER_STARTED:-0\}" = 1 \]')
        self.assertIn("SOAK_KEEP_CONTAINER", SRC)

    def test_the_soak_container_is_force_recreated(self) -> None:
        """P6-182: a container leaked by an earlier run must not serve this stage."""
        self.assertRegex(SRC, r"up -d --force-recreate ingestion")

    def test_no_absolute_home_path_and_no_hardcoded_container_name(self) -> None:
        """P6-513: both break on any machine that is not this one."""
        self.assertNotIn("/home/saurabh", SRC)
        self.assertNotIn("01_docker-ingestion-1", SRC)

    def test_running_is_never_reported_as_a_verdict(self) -> None:
        """P6-514: a `set -e` abort used to leave RESULT=RUNNING in SUMMARY.txt."""
        self.assertRegex(SRC, r'\[ "\$RESULT" = RUNNING \] && RESULT="FAIL \(aborted before a verdict\)"')

    def test_the_broker_credential_is_not_on_a_command_line(self) -> None:
        """P6-179: argv is world-readable through /proc."""
        self.assertNotRegex(SRC, r"^\s*export ARROW_APP_ID", )
        self.assertNotRegex(SRC, r"--header \"Authorization")
        self.assertRegex(SRC, r'-K "\$O2_AUTH_FILE"')
        self.assertRegex(SRC, r'umask 077; mktemp')
        self.assertRegex(SRC, r"\brm -f \"\$\{O2_AUTH_FILE:-\}\"")

    def test_the_marathon_gate_counts_distinct_epochs(self) -> None:
        """P6-180: max_epoch >= 100 does not prove 100 reconnect cycles."""
        self.assertNotRegex(SRC, r"awk '/subscription_ack/")
        self.assertIn(r"\bevent=subscription_ack\b", SRC)
        self.assertRegex(SRC, r'\[ "\$\{MARATHON_EPOCHS:-0\}" -lt 100 \]')

    def test_the_container_ack_poll_cannot_abort_the_suite(self) -> None:
        """P6-520: `[ x ] && { ...; break; }` ends the list with a false test."""
        self.assertRegex(SRC, r'if \[ -f "\$SOAK_JOURNAL/ingestion\.json" \]; then')

    def test_gates_evidence_falls_back_to_none(self) -> None:
        """P6-519: `| head -1` exits 0 on an empty glob, so `|| echo none` never ran."""
        self.assertNotRegex(SRC, r'GATES_EVIDENCE="\$\(ls -td [^\)]*\|\s*head -1 \|\| echo none\)')
        self.assertRegex(SRC, r'\[ -n "\$GATES_EVIDENCE" \] \|\| GATES_EVIDENCE=none')

    def test_waits_step_by_seconds_not_by_twenty(self) -> None:
        """P6-774: a 20s step overshoots a deadline by up to 20s."""
        self.assertNotIn("sleep 20", SRC)
        self.assertRegex(SRC, r"while \[ \"\$\(date \+%s\)\" -lt \"\$target\" \]; do sleep 5; done")

    def test_the_lib_seam_runs_no_stage_and_leaves_no_trap(self) -> None:
        """The seam the helper tests depend on, and its safety property."""
        self.assertIn('LIB_MODE="${RUN_FULL_SUITE_LIB:-false}"', SRC)
        self.assertRegex(SRC, r'if \[ "\$LIB_MODE" = true \]; then\n\ttrap - EXIT')

    def test_ss_absence_is_a_preflight_failure(self) -> None:
        """P6-515: without ss every listener check silently says "not listening"."""
        self.assertRegex(SRC, r'command -v ss >/dev/null \|\| \{ echo "!! ss missing')


if __name__ == "__main__":
    unittest.main()
