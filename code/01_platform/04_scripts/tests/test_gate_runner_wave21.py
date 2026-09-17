#!/usr/bin/env python3
"""Static pins for the wave-21 gate-runner fixes (P6-186..190, P6-522..532, P6-775..776).

`run-monday-gates.sh` decides what a green gate means, so its own defects change
what a PASS proves. The suite-heavy steps cannot run hermetically (they need go /
mvn / docker / a live Fluss), so this file pins the fix SHAPES statically — the
same way W18 pinned the rollout guards — plus one behavioural leg that runs the
real step-3 python-gate block against a stub unittest tree:

* no `bash -c "cd '$VAR'"` interpolation anywhere (P6-187/188/190, P6-529):
  env-controlled dirs reach the command via a subshell `cd`, never inside a
  double-quoted `bash -c` string a single-quote can break out of.
* every `timeout` that guards a suite carries `-k` (P6-526/528, P6-532): a stuck
  child that ignores TERM is killed instead of blocking the gate forever.
* the python gate asserts a non-empty OK (P6-527): `Ran 0 tests` or `FAILED (`
  fails the step even when a stale `^OK` line is present.
* the shellcheck probe runs once (P6-775): no per-file WARN inside the loop, and
  the PASS line names the `-S warning` severity it actually enforces.
* compose/build/ownership evidence has its own log (P6-525, P6-776): STATIC_LOG
  carries only static checks, and the final manifest advertises every log.
"""

from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

GATE = Path(__file__).resolve().parents[1] / "run-monday-gates.sh"
SRC = GATE.read_text(encoding="utf-8")


class NoBashCInterpolation(unittest.TestCase):
    def test_no_bash_c_with_interpolated_cd(self) -> None:
        hits = [n for n, line in enumerate(SRC.split("\n"), 1)
                if re.search(r"""bash\s+-c\s+["'].*cd\s+['"]?\$""", line)]
        self.assertEqual(hits, [], f"bash -c interpolates an env dir at line(s) {hits}")

    def test_no_single_quoted_var_inside_double_quoted_bash_c(self) -> None:
        hits = [n for n, line in enumerate(SRC.split("\n"), 1)
                if "bash -c" in line and "'$" in line]
        self.assertEqual(hits, [], f"single-quote breakout shape at line(s) {hits}")

    def test_suites_reach_their_dir_via_subshell_cd(self) -> None:
        for var in ("BRIDGE_DIR", "CODE_DIR", "PROJECT_ROOT", "EXECUTOR_DIR",
                    "COMPUTE_DIR", "MOCK_ARROW_DIR"):
            self.assertIn(f'(cd "${var}"', SRC, f"{var} suite lost its subshell cd")


class TimeoutsKillAfter(unittest.TestCase):
    def test_every_suite_timeout_uses_kill_after(self) -> None:
        bare = [n for n, line in enumerate(SRC.split("\n"), 1)
                if re.search(r"^\s*(?:if\s+)?!\s*timeout\s+[0-9$]", line)
                and "-k" not in line]
        self.assertEqual(bare, [], f"timeout without -k at line(s) {bare}")

    def test_subset_pins_have_short_env_overridable_timeouts(self) -> None:
        for name, default in (("SCHEMA_PERF_TIMEOUT_SEC", "1200"),
                              ("SHUTDOWN_TIMEOUT_SEC", "1200"),
                              # CHG-199: 600 — the suite overruns 300 under
                              # gate load (see run-monday-gates.sh step 3).
                              ("PY_TIMEOUT_SEC", "600"),
                              ("ENTRYPOINT_TIMEOUT_SEC", "300"),
                              ("E2E_BUILD_TIMEOUT_SEC", "600")):
            self.assertIn(f'{name}="${{{name}:-{default}}}"', SRC,
                          f"{name} is not env-overridable with default {default}")

    def test_named_pins_fail_closed(self) -> None:
        """P6-189, shape corrected after a real Maven run (CHG-160).

        `failIfNoSpecifiedTests=true` cannot be used with `-pl <service> -am`:
        `common` joins the reactor, the pinned pattern matches nothing there, and
        surefire aborts that module — the pin then fails on every run for the
        wrong reason. The flag stays `false` and a named pin must carry a
        require_class_ran call for every class in its pattern instead.
        """
        invocations = [line for line in SRC.split("\n")
                       if "failIfNoSpecifiedTests=" in line and "mvn " in line]
        self.assertGreater(len(invocations), 0, "no -Dtest pins left to guard")
        for line in invocations:
            self.assertIn("failIfNoSpecifiedTests=false", line,
                          f"a `true` value aborts the build in the `common` module: {line.strip()[:120]}")
            pattern = re.search(r"-Dtest='([^']+)'", line)
            self.assertIsNotNone(pattern, f"no -Dtest pattern on the pin: {line.strip()[:120]}")
            for cls in pattern.group(1).split(","):
                self.assertRegex(
                    SRC,
                    rf"require_class_ran \"\$[A-Z_]+_LOG\" com\.trading\.ingestion\.{cls} ",
                    f"{cls} is pinned with -Dtest but nothing asserts that class ran")


class PythonGateShape(unittest.TestCase):
    def test_empty_or_failed_discovery_fails(self) -> None:
        # CHG-201: the FAILED leg is line-anchored — see the behavioural test.
        self.assertIn("Ran 0 tests|^FAILED \\(", SRC,
                      "step 3 lost its non-empty-OK assertion")

    def test_behavioural_nonempty_ok_gate(self) -> None:
        """Run step 3's real gate predicate against stub unittest outputs."""
        pred = re.search(
            r"if ! grep -q \"\^OK\" \"\$PY_LOG\" \|\| grep -qE '([^']+)' \"\$PY_LOG\"; then",
            SRC)
        self.assertIsNotNone(pred, "step-3 gate predicate not found")
        pattern = pred.group(1)
        cases = {
            "Ran 3 tests in 0.001s\n\nOK\n": False,          # genuine green passes
            "Ran 0 tests in 0.001s\n\nOK\n": True,            # empty discovery fails
            "Ran 1 test in 0.001s\n\nFAILED (failures=1)\n": True,  # failure fails
            "Ran 0 tests in 0.001s\n\nOK (skipped=1)\n": True,      # skip-only fails
            # CHG-201: mid-line chatter must not fail a green suite (grep is
            # line-oriented, so the model uses re.M like the real check).
            "Ran 3 tests in 0.001s\nDDL APPLY FAILED (exit 3)\n\nOK\n": False,
        }
        for body, must_fail in cases.items():
            has_ok = bool(re.search(r"^OK", body, flags=re.M))
            has_bad = bool(re.search(pattern, body, flags=re.M))
            fails = (not has_ok) or has_bad
            self.assertEqual(fails, must_fail, f"wrong verdict for {body!r}")


class StaticStepShape(unittest.TestCase):
    def test_shellcheck_probe_runs_once(self) -> None:
        self.assertEqual(SRC.count("shellcheck not installed"), 1,
                         "shellcheck WARN must print once, not once per file")

    def test_pass_line_names_the_enforced_severity(self) -> None:
        self.assertIn("shellcheck -S warning clean", SRC,
                      "PASS claims 'clean' without naming the -S warning severity")

    def test_bash3_fallback_without_mapfile(self) -> None:
        self.assertIn('BASH_MAJOR', SRC, "macOS bash 3.2 has no version probe")
        self.assertIn('while IFS= read -r _l; do SCRIPTS+=("$_l"); done', SRC,
                      "no portable read-loop fallback for bash 3.2")


class EvidenceSeparation(unittest.TestCase):
    def test_static_log_carries_only_static_checks(self) -> None:
        for n, line in enumerate(SRC.split("\n"), 1):
            if "STATIC_LOG" in line and "2>>" in line:
                if "static-checks" in line or "STATIC_LOG=" in line:
                    continue
                self.fail(f"line {n} appends non-static output to STATIC_LOG: {line.strip()[:120]}")

    def test_each_stage_has_its_own_log(self) -> None:
        for name in ("COMPOSE_CONFIG_LOG", "DOCKER_BUILD_LOG", "E2E_BUILD_LOG",
                     "OWNERSHIP_LOG"):
            self.assertIn(f'{name}="$OUT_DIR/', SRC, f"{name} not assigned up front")
            self.assertIn(f"${{{name}:-not-run}}", SRC, f"{name} missing from the manifest")

    def test_tmp_script_list_cleaned_on_all_exits(self) -> None:
        self.assertIn('rm -f "${_script_list:-}"', SRC,
                      "mktemp list has no EXIT-trap cleanup")


if __name__ == "__main__":
    unittest.main()
