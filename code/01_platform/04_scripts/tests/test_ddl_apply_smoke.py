#!/usr/bin/env python3
"""Guard tests for ddl_apply_smoke.py's docker probes (wave 14, P6-352).

The S4 drill shells out to the docker CLI on the host. Two of those probes had
no timeout and no exception handling while every sibling docker call in the same
file used 120s/900s timeouts, so a daemon that accepts the socket but never
answers hung the drill instead of degrading to a SKIP. These tests pin the
degradation without needing a docker daemon: the timeout is injected.
"""
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "code/01_platform/04_scripts"))

import ddl_apply_smoke as smoke  # noqa: E402


class EvidenceRootTest(unittest.TestCase):
    """Where the scenarios' evidence lands — and who decides it.

    The default (repo-root logs/ddl-apply) is container-owned after any container run, so the
    smoke used to fall back to a temp dir. That fallback silently discarded the per-apply evidence
    of a certificate run: the run dir kept only the 1 KB summary (found 2026-09-18 while profiling
    gate step 11 — run 6's apply logs were gone). The smoke now honours an explicitly-set
    DDL_APPLY_EVIDENCE_DIR, which is how the gate keeps step 11's evidence inside its own run dir.
    """

    def test_an_explicit_dir_wins_even_when_the_default_is_unwritable(self):
        root, why = smoke.resolve_evidence_root(
            "/repo/logs/ddl-apply", {"DDL_APPLY_EVIDENCE_DIR": "/runs/xyz/ddl-apply-evidence"},
            is_writable=lambda _: False)

        self.assertEqual(root, "/runs/xyz/ddl-apply-evidence")
        self.assertEqual(why, "explicit")

    def test_the_default_is_used_when_it_is_writable(self):
        root, why = smoke.resolve_evidence_root("/repo/logs/ddl-apply", {},
                                                is_writable=lambda _: True)

        self.assertEqual(root, "/repo/logs/ddl-apply")
        self.assertEqual(why, "default")

    def test_a_temp_dir_is_the_last_resort(self):
        root, why = smoke.resolve_evidence_root("/repo/logs/ddl-apply", {},
                                                is_writable=lambda _: False)

        self.assertIsNone(root, "no directory decided -> the caller makes a temp one")
        self.assertEqual(why, "temp")

    def test_a_blank_explicit_dir_is_not_treated_as_a_choice(self):
        root, why = smoke.resolve_evidence_root("/repo/logs/ddl-apply",
                                                {"DDL_APPLY_EVIDENCE_DIR": "   "},
                                                is_writable=lambda _: True)

        self.assertEqual(root, "/repo/logs/ddl-apply")
        self.assertEqual(why, "default")


class ScenarioBudgetTest(unittest.TestCase):
    """A scenario's timeout must outlast the tool's own drain budget.

    Measured 2026-09-18 while measuring step 11: scenario 2 took 882 s of its 900 s budget and
    scenario 4 ran out of it ("FAIL: timed out after 900s") — while DdlApplyTool's DRAIN_BUDGET is
    25 minutes. A healthy-but-slow cluster therefore failed step 11 from the outside, which is a
    false negative the certificate cannot afford. The two numbers are one coupling, so it is pinned
    rather than trusted.
    """

    def test_the_scenario_timeout_outlasts_the_tool_drain_budget(self) -> None:
        tool = (ROOT
                / "code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java"
                ).read_text(encoding="utf-8")
        match = re.search(r"DRAIN_BUDGET = Duration\.ofMinutes\((\d+)\)", tool)
        self.assertIsNotNone(match, "DdlApplyTool no longer defines DRAIN_BUDGET as Duration.ofMinutes(n)")
        drain_budget_s = int(match.group(1)) * 60

        self.assertGreater(
            smoke.SCENARIO_TIMEOUT_S, drain_budget_s,
            f"a scenario may only wait SCENARIO_TIMEOUT_S={smoke.SCENARIO_TIMEOUT_S}s while the"
            f" apply tool is entitled to wait DRAIN_BUDGET={drain_budget_s}s for its own teardown"
            f" — the scenario would time out on a slow-but-healthy cluster and fail the gate",
        )


class GateCapTest(unittest.TestCase):
    """The gate's outer cap must outlast every scenario it wraps, not one of them.

    Measured 2026-09-19, the screen run before a certificate attempt: the runner capped the whole
    smoke at 1800 s while CHG-226 had raised EACH scenario's budget to 1800 s. S1 and S2 PASSed and
    wrote their apply.json, then S4 was killed mid-apply at the cap — and because a SIGKILL
    discards a pipe-buffered stdout, ddl-smoke.log came back EMPTY, so the failure carried no
    evidence at all. A healthy-but-slow cluster therefore failed step 11 from the outside. The cap
    and the scenario budgets are one coupling, so it is pinned rather than trusted.
    """

    RUNNER = ROOT / "code/01_platform/04_scripts/run-monday-gates.sh"

    def _runner_cap_s(self) -> int:
        runner = self.RUNNER.read_text(encoding="utf-8")
        match = re.search(
            r'DDL_SMOKE_TIMEOUT_SEC="\$\{DDL_SMOKE_TIMEOUT_SEC:-(\d+)\}"', runner)
        self.assertIsNotNone(
            match,
            "run-monday-gates.sh no longer defaults DDL_SMOKE_TIMEOUT_SEC as "
            'DDL_SMOKE_TIMEOUT_SEC="${DDL_SMOKE_TIMEOUT_SEC:-<seconds>}"')
        return int(match.group(1))

    def test_the_outer_cap_outlasts_every_scenario_it_wraps(self) -> None:
        cap_s = self._runner_cap_s()
        worst_case_s = smoke.SCENARIO_COUNT * smoke.SCENARIO_TIMEOUT_S

        self.assertGreater(
            cap_s, worst_case_s,
            f"the gate caps the whole smoke at DDL_SMOKE_TIMEOUT_SEC={cap_s}s while its"
            f" {smoke.SCENARIO_COUNT} scenarios are each entitled to"
            f" SCENARIO_TIMEOUT_S={smoke.SCENARIO_TIMEOUT_S}s ({worst_case_s}s in total)"
            f" — a healthy-but-slow cluster would be killed from the outside and fail step 11",
        )

    def test_the_declared_scenario_count_matches_the_invocations(self) -> None:
        """A scenario nobody counts would silently re-break the cap."""
        source = (ROOT / "code/01_platform/04_scripts/ddl_apply_smoke.py").read_text(
            encoding="utf-8")
        main_body = source.split("def main()", 1)[1]
        invocations = len(re.findall(r"^\s*ok &= scenario", main_body, re.M))

        self.assertEqual(
            invocations, smoke.SCENARIO_COUNT,
            f"main() invokes {invocations} scenario(s) but SCENARIO_COUNT="
            f"{smoke.SCENARIO_COUNT} — the outer cap is sized from that count, so a new"
            f" scenario has to bump it",
        )

    def test_a_killed_smoke_still_writes_its_log(self) -> None:
        """The cap kill discarded the log; unbuffered stdout is what preserves it."""
        runner = self.RUNNER.read_text(encoding="utf-8")
        match = re.search(
            r'timeout -k 60 "\$DDL_SMOKE_TIMEOUT_SEC" python3 (\S+)', runner)
        self.assertIsNotNone(
            match,
            'run-monday-gates.sh no longer invokes the smoke as '
            '`timeout -k 60 "$DDL_SMOKE_TIMEOUT_SEC" python3 <flag>`')

        self.assertEqual(
            match.group(1), "-u",
            "the smoke must run as `python3 -u`: its stdout is redirected into the step log, and"
            " a cap kill (SIGKILL) discards a buffered stdout — which is how the 2026-09-19"
            " failure left an empty ddl-smoke.log",
        )


class DockerProbeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.compose = Path(self.tmp.name) / "docker-compose.yml"
        self.compose.write_text("services: {}\n")

    def test_docker_absent_is_a_skip(self):
        with mock.patch.object(smoke.shutil, "which", return_value=None):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("docker CLI not found", reason)

    def test_unresponsive_daemon_degrades_instead_of_hanging(self):
        with mock.patch.object(smoke.shutil, "which", return_value="/usr/bin/docker"), \
             mock.patch.object(smoke.subprocess, "run",
                               side_effect=subprocess.TimeoutExpired("docker", 120)):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("compose config probe did not run", reason)

    def test_unresponsive_daemon_on_image_inspect_also_degrades(self):
        ok = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(smoke.shutil, "which", return_value="/usr/bin/docker"), \
             mock.patch.object(smoke.subprocess, "run",
                               side_effect=[ok, subprocess.TimeoutExpired("docker", 120)]):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("docker image inspect did not run", reason)

    def test_invalid_compose_config_still_reports_its_own_reason(self):
        bad = mock.Mock(returncode=1, stdout="", stderr="missing variable")
        with mock.patch.object(smoke.shutil, "which", return_value="/usr/bin/docker"), \
             mock.patch.object(smoke.subprocess, "run", return_value=bad):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("compose config invalid", reason)


if __name__ == "__main__":
    unittest.main()
