"""Hermetic tests for run-b4-halted-e2e.sh (P6 wave 19: P6-175/176, P6-509..512, P6-772/773).

The script is top-level flow (no functions), so probes run the real script
with PATH stubs: `mvn` records argv/PWD and exits per MVN_FAIL; `timeout`
answers the Fluss reachability preflight per FLUSS_UP without touching the
network. Live Maven/Fluss are never needed.
"""
import os
import pathlib
import stat
import subprocess
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "run-b4-halted-e2e.sh"
SRC = SCRIPT.read_text()


class HaltedHarness(unittest.TestCase):
    def setUp(self):
        self.t = pathlib.Path(tempfile.mkdtemp())
        self.bin = self.t / "bin"
        self.bin.mkdir()
        (self.bin / "mvn").write_text(
            '#!/usr/bin/env bash\n'
            'echo "MVN PWD=$PWD ARGS=$*" >> "$MVN_CALLS"\n'
            'if [ "${MVN_FAIL:-}" = "gateway" ] && [[ "$PWD" == *06_execution_gateway* ]]; then exit 3; fi\n'
            'if [ "${MVN_FAIL:-}" = "compute" ] && [[ "$PWD" == *02_compute* ]]; then exit 4; fi\n'
            'exit 0\n')
        (self.bin / "timeout").write_text(
            '#!/usr/bin/env bash\n'
            '# Answers ONLY the Fluss preflight (the script calls timeout solely there).\n'
            'echo "TIMEOUT ARGS=$*" >> "$MVN_CALLS"\n'
            '[ "${FLUSS_UP:-1}" = "1" ]\n')
        for b in ("mvn", "timeout"):
            (self.bin / b).chmod(0o755)
        self.env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}",
                        MVN_CALLS=str(self.t / "calls.log"),
                        FLUSS_UP="1", FLUSS_BOOTSTRAP="stub:9123")
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.t)], check=False))

    def run_script(self, *args, extra=None, timeout=60):
        env = dict(self.env)
        if extra:
            env.update(extra)
        return subprocess.run(["bash", str(SCRIPT), *args], env=env,
                              capture_output=True, text=True, timeout=timeout)

    def calls(self):
        p = self.t / "calls.log"
        return p.read_text() if p.exists() else ""

    # P6-773: surplus args fail; all modes listed.
    def test_surplus_args_rejected(self):
        r = self.run_script("both", "--gateway-only")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("usage:", r.stderr)
        self.assertIn("both", r.stderr)

    def test_bad_mode_rejected_with_full_usage(self):
        r = self.run_script("bogus")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        for mode in ("both", "--gateway-only", "--compute-only"):
            self.assertIn(mode, r.stderr, mode)

    # P6-772: malformed bootstrap fails fast (before any mvn call).
    def test_bad_bootstrap_rejected_fast(self):
        for bad in ("noport", ":9123", "host:", "host:abc", "a:b:c"):
            r = self.run_script(extra={"FLUSS_BOOTSTRAP": bad})
            self.assertEqual(r.returncode, 2, f"{bad}: {r.stdout}{r.stderr}")
            self.assertIn("host:port", r.stderr, bad)
        self.assertNotIn("MVN PWD=", self.calls())

    # P6-510: unreachable Fluss fails before Maven with a make-ddl hint.
    def test_unreachable_fluss_fails_before_maven(self):
        r = self.run_script(extra={"FLUSS_UP": "0"})
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("unreachable", r.stderr)
        self.assertIn("make ddl", r.stderr)
        self.assertNotIn("MVN PWD=", self.calls())

    # P6-511: labels derive from the requested legs.
    def test_gateway_only_labels(self):
        r = self.run_script("--gateway-only")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("[1/1]", r.stdout)
        self.assertNotIn("[1/2]", r.stdout)
        self.assertIn("--gateway-only PASS", r.stdout)
        self.assertNotIn("ALL PASS", r.stdout)

    def test_compute_only_labels(self):
        r = self.run_script("--compute-only")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("[1/1]", r.stdout)
        self.assertIn("--compute-only PASS", r.stdout)
        self.assertNotIn("ALL PASS", r.stdout)

    def test_both_labels_and_footer(self):
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("[1/2]", r.stdout)
        self.assertIn("[2/2]", r.stdout)
        self.assertIn("ALL PASS", r.stdout)

    # P6-509: a gateway failure still runs compute; footer reports FAIL.
    def test_gateway_failure_still_runs_compute(self):
        r = self.run_script(extra={"MVN_FAIL": "gateway"})
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        c = self.calls()
        self.assertIn("06_execution_gateway", c)
        self.assertIn("02_compute", c)
        self.assertIn("FAIL", r.stderr)
        self.assertIn("gateway=3", r.stderr)
        self.assertIn("HALTED E2E: FAIL", r.stderr)
        self.assertNotIn("ALL PASS", r.stdout)

    # P6-175/176: fail-closed Maven flags on both legs, no quiet -q.
    def test_maven_flags_fail_closed_on_both_legs(self):
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        c = self.calls()
        for cls in ("B4HaltedIntentConsumeDeferE2ETest", "B4SignalIntentE2ETest"):
            self.assertIn(cls, c, cls)
        self.assertEqual(c.count("surefire.failIfNoSpecifiedTests=true"), 2, c)
        self.assertEqual(c.count("failIfNoTests=true"), 2, c)
        for line in c.splitlines():
            if line.startswith("MVN PWD="):
                self.assertNotIn(" -q", line, line)

    # Static pins for the guards that only fire live.
    def test_static_pins(self):
        # P6-509 pin: no `set -e` (script runs `set -uo pipefail`); legs
        # collect status instead.
        self.assertNotIn("set -euo pipefail", SRC)
        self.assertIn("set -uo pipefail", SRC)
        self.assertIn("|| gw_status=$?", SRC)
        self.assertIn("|| cp_status=$?", SRC)
        # P6-510 pin: /dev/tcp preflight before any mvn leg.
        self.assertIn("/dev/tcp/$fluss_host/$fluss_port", SRC)
        self.assertLess(SRC.index("/dev/tcp/"), SRC.index("mvn -o test -Dtest="))
        # P6-512 pin: single-leg footer disclaims full coverage.
        self.assertIn("not full B4.2 coverage", SRC)


if __name__ == "__main__":
    unittest.main()
