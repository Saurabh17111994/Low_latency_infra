"""Guards for wave 52: the entrypoint suite, the sandbox claim and the tiering pins.

Wave 52 fixed seven findings in the entrypoint FATAL-path suite (one of them by
adding coverage the file only promised), corrected the T9 sandbox module's claim
about what it needs, and verified three findings as already fixed. This module
keeps all three honest:

  * the entrypoint suite is run against deliberately broken fake entrypoints — one
    that exits 0 on every gate and one that hangs — and must fail on both, which is
    the only way to know its assertions bite;
  * the shapes this wave removed (unchecked mktemp, a bare "$@" under `set -u`,
    `chmod +x` on the checkout, a missing per-invocation timeout) must stay gone;
  * the three stale findings stay stale in the right direction: the audit's old
    literals absent, the scripts' real messages present.

No container is started; the only subprocesses are bash, shellcheck and the suite.
"""
import os
import shutil
import subprocess
import time
import unittest
from pathlib import Path

TESTS = Path(__file__).resolve().parent
ROOT = TESTS.parents[3]
ENTRY_SUITE = TESTS / "test_docker_entrypoint.sh"
T9_TEST = TESTS / "test_12_t9_order_sandbox.py"
TIERING_TEST = TESTS / "test_tiering_restart_guard.py"
TMKILL = ROOT / "code/01_platform/04_scripts/tm-kill-full-load.sh"


class FakeEntrypointTest(unittest.TestCase):
    """The suite must FAIL against a broken entrypoint — else it guards nothing."""

    def run_suite_against(self, body, timeout_s, wall_limit_s):
        fake = TESTS / ".w52_fake_entrypoint.sh"
        fake.write_text(body)
        env = dict(os.environ, ENTRYPOINT=str(fake), ENTRYPOINT_TIMEOUT=timeout_s)
        start = time.time()
        try:
            proc = subprocess.run(["bash", str(ENTRY_SUITE)], capture_output=True,
                                  text=True, timeout=wall_limit_s, env=env)
            out, rc = proc.stdout + proc.stderr, proc.returncode
        except subprocess.TimeoutExpired:
            rc, out = None, "(the suite itself exceeded its wall limit)"
        finally:
            fake.unlink()
        return rc, out, time.time() - start

    def test_suite_fails_when_every_gate_exits_zero(self):
        rc, out, _ = self.run_suite_against("#!/usr/bin/env bash\nexit 0\n", "5s", 90)
        self.assertNotEqual(0, rc, f"a fake entrypoint that always succeeds must fail the suite:\n{out}")

    def test_suite_fails_fast_when_a_gate_hangs(self):
        rc, out, elapsed = self.run_suite_against("#!/usr/bin/env bash\nsleep 30\n", "1s", 120)
        self.assertNotEqual(0, rc, f"a hanging entrypoint must fail the suite:\n{out}")
        self.assertIn("did not exit within", out,
                      "the failure must come from the per-invocation timeout (P6-239)")
        self.assertLess(elapsed, 60,
                        f"the timeout must bound each call, not the whole file: {elapsed:.1f}s")


class EntrypointScriptPinsTest(unittest.TestCase):
    """The removed shapes stay removed, and the new coverage stays present."""

    @classmethod
    def setUpClass(cls):
        cls.text = ENTRY_SUITE.read_text()

    def test_no_checkout_mutation(self):
        self.assertNotIn("chmod +x \"$ENTRYPOINT\"", self.text,
                         "P6-831: the suite must not chmod the working tree")
        self.assertIn("bash \"$ENTRYPOINT\"", self.text)

    def live_text(self):
        """The file minus comment lines: comments may name the rejected forms."""
        return "\n".join(l for l in self.text.splitlines() if not l.lstrip().startswith("#"))

    def test_invocation_is_bounded_and_portable(self):
        live = self.live_text()
        self.assertIn('command -v timeout', live, "P6-239: invocations must be bounded")
        self.assertIn('"$TIMEOUT_BIN" "$ENTRYPOINT_TIMEOUT"', live)
        self.assertIn('PATH=/usr/bin:/bin', live, "P6-622: a minimal PATH, not an empty env")
        self.assertIn('${@+"$@"}', live, "P6-832: the portable zero-word expansion")
        self.assertNotIn('${@-}', live, "`${@-}` passes one EMPTY argument to env (measured)")

    def test_mktemp_failure_is_loud(self):
        live = self.live_text()
        self.assertIn('TMP="$(mktemp -d)" ||', live, "P6-621: mktemp must fail fast")
        self.assertIn("trap 'rm -rf \"${TMP:?}\"' EXIT", live,
                      "P6-621: the trap must refuse an empty TMP")

    def test_promised_coverage_is_present(self):
        live = self.live_text()
        for label in ("unreadable manifest", "diverging manifest",
                      "non-executable bridge", "legacy INSTRUMENT_MANIFEST_PATH",
                      "empty FLUSS_BOOTSTRAP", "FATAL message is on stderr",
                      "FATAL message is not on stdout"):
            self.assertIn(label, live, f"P6-620/622/833: case missing: {label}")

    def test_the_suite_itself_is_syntax_and_shellcheck_clean(self):
        self.assertEqual(0, subprocess.run(["bash", "-n", str(ENTRY_SUITE)],
                                           capture_output=True).returncode)
        if shutil.which("shellcheck") is None:
            self.skipTest("shellcheck not installed")
        proc = subprocess.run(["shellcheck", "-S", "warning", str(ENTRY_SUITE)],
                              capture_output=True, text=True)
        self.assertEqual(0, proc.returncode, f"shellcheck: {proc.stdout}{proc.stderr}")


class StaleFindingsStayFixedTest(unittest.TestCase):
    """P6-238/240/844 were already fixed: the old literals must stay gone."""

    def test_t9_no_longer_pins_the_delisted_instrument(self):
        text = T9_TEST.read_text()
        self.assertNotIn('"BI-EQ"', text, "P6-238: the delisted symbol literal is back")
        self.assertNotIn("== 762583", text, "P6-238: the delisted token literal is back")
        self.assertIn("t9.BIEQ_SYMBOL", text)
        self.assertIn("t9.BIEQ_INSTRUMENT_TOKEN", text)

    def test_tiering_guard_pins_the_real_messages_not_the_pattern_string(self):
        text = TIERING_TEST.read_text()
        self.assertNotIn("fixed[- ]delay", text, "P6-844: the grep-pattern literal is back")
        # 2026-09-23 (Fluss 1.0.0): the pinned string is no longer the -D flag this
        # platform passed — Fluss 1.0.0 sets exponential-delay in its own code and
        # overrides that flag. The guard test must pin the strategy now accepted plus
        # the absence of the dead flag.
        self.assertIn("exponential-delay", text)
        self.assertNotIn("-Drestart-strategy.type=fixed-delay", text)
        self.assertIn("tiering_has_restart_strategy", text)

    def test_smoke_contract_strings_match_between_test_and_script(self):
        script = TMKILL.read_text()
        text = TIERING_TEST.read_text()
        for needle in ("smoke = compressed main drill (kill INCLUDED)",
                       "PASS — smoke drill green",
                       "source did not advance after recovery",
                       "SignalJob output did not advance after recovery"):
            self.assertIn(needle, text, f"P6-240: the test no longer asserts: {needle}")
            self.assertIn(needle, script, f"P6-240: the script no longer emits: {needle}")
        for retired in ("smoke gate — no TaskManager kill", "smoke gate completed only",
                        "smoke source did not advance", "smoke output did not advance"):
            self.assertNotIn(retired, script, f"P6-240: the retired string is back: {retired}")

    def test_sandbox_module_states_its_real_precondition(self):
        text = T9_TEST.read_text()
        self.assertIn("does need a working docker CLI", text,
                      "P6-616: the offline leg needs docker — say so")
        self.assertNotIn("No containers, no credentials, no market hours required", text)


if __name__ == "__main__":
    unittest.main()
