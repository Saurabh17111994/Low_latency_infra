#!/usr/bin/env python3
"""P6-189, corrected shape: a named `-Dtest` pin must prove each class RAN.

Why this test exists (CHG-160): the wave-21 fix set
`-Dsurefire.failIfNoSpecifiedTests=true` on the step-12/13 pins. A real offline
build showed that value aborts the reactor in the `common` module — `-pl
02_services/01_ingestion -am` pulls `common` in, the pattern matches nothing
there, and surefire fails the build (`No tests matching pattern … were
executed!`). So the flag is back to `false` and the strictness moved to
`require_class_ran`, which greps surefire's per-class closing line

    [INFO] Tests run: N, Failures: F, Errors: E, Skipped: S, Time elapsed: T s -- in <FQCN>

for a NAMED class with N >= 1. The fixtures below are verbatim shapes from the
real runs recorded in CHG-160 (step 12: 7 + 11 + 2 tests; step 13: 1 + 1).

`require_tests_run` (max non-zero count) cannot catch a partial rename: one
surviving class keeps the maximum positive, which is exactly the hole P6-189
named.
"""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
GATE = REPO / "code/01_platform/04_scripts/run-monday-gates.sh"
SRC = GATE.read_text(encoding="utf-8")


def _extract_function(name: str) -> str:
    """Return the source of a shell function defined in the gate script."""
    start = SRC.index(f"\n{name}() {{") + 1
    depth = 0
    for index in range(start, len(SRC)):
        char = SRC[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return SRC[start:index + 1]
    raise AssertionError(f"unbalanced braces while extracting {name}")


# Verbatim from the real step-12 and step-13 runs (CHG-160).
STEP12_LOG = """\
[INFO] Running com.trading.ingestion.DdlBootstrapSchemaAgreementTest
[INFO] Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.294 s -- in com.trading.ingestion.DdlBootstrapSchemaAgreementTest
[INFO] Running com.trading.ingestion.SchemaAgreementTest
[INFO] Tests run: 11, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.028 s -- in com.trading.ingestion.SchemaAgreementTest
[INFO] Running com.trading.ingestion.PerfBaselineTest
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13.67 s -- in com.trading.ingestion.PerfBaselineTest
[INFO] BUILD SUCCESS
"""

STEP13_LOG = """\
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.592 s -- in com.trading.ingestion.BridgeShutdownHookTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.207 s -- in com.trading.ingestion.BridgeShutdownRegressionTest
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
"""

SCHEMA_CLASSES = (
    "com.trading.ingestion.SchemaAgreementTest",
    "com.trading.ingestion.DdlBootstrapSchemaAgreementTest",
    "com.trading.ingestion.PerfBaselineTest",
)
SHUTDOWN_CLASSES = (
    "com.trading.ingestion.BridgeShutdownRegressionTest",
    "com.trading.ingestion.BridgeShutdownHookTest",
)


class ClassPin(unittest.TestCase):
    """Run the gate's own require_class_ran against fixtures and real logs."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w22-classpin-")
        self.tmp = Path(self._tmp.name)
        driver = self.tmp / "driver.sh"
        driver.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "SUMMARY=/dev/null\n"
            "gate_fail() { echo \"GATE_FAIL\" >&2; exit 1; }\n"
            + _extract_function("strip_ansi")
            + "\n"
            + _extract_function("require_class_ran")
            + "\nrequire_class_ran \"$1\" \"$2\" \"$3\"\n",
            encoding="utf-8",
        )
        self.driver = driver

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _log(self, name: str, text: str) -> Path:
        path = self.tmp / name
        path.write_text(text, encoding="utf-8")
        return path

    def _run(self, log: Path, fqcn: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(self.driver), str(log), fqcn, "step 12"],
            capture_output=True, text=True, timeout=60,
        )

    def test_every_real_class_line_passes(self) -> None:
        step12 = self._log("step12.log", STEP12_LOG)
        for fqcn in SCHEMA_CLASSES:
            proc = self._run(step12, fqcn)
            self.assertEqual(proc.returncode, 0, f"{fqcn}: {proc.stderr}")
        step13 = self._log("step13.log", STEP13_LOG)
        for fqcn in SHUTDOWN_CLASSES:
            proc = self._run(step13, fqcn)
            self.assertEqual(proc.returncode, 0, f"{fqcn}: {proc.stderr}")

    def test_a_colourised_line_passes(self) -> None:
        """Verbatim bytes from the real run: Maven bolds the class name.

        `[^[[1;34mINFO^[[m] ^[[1;32mTests run: ^[[0;1;32m1^[[m, ... -- in
        com.trading.ingestion.^[[1mBridgeShutdownHookTest^[[m`
        """
        coloured = ("[\x1b[1;34mINFO\x1b[m] \x1b[1;32mTests run: \x1b[0;1;32m1\x1b[m, "
                    "Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.592 s -- in "
                    "com.trading.ingestion.\x1b[1mBridgeShutdownHookTest\x1b[m\n"
                    "[\x1b[1;34mINFO\x1b[m] \x1b[1;32mTests run: \x1b[0;1;32m1\x1b[m, "
                    "Failures: 0, Errors: 0, Skipped: 0 -- in "
                    "com.trading.ingestion.\x1b[1mBridgeShutdownRegressionTest\x1b[m\n"
                    "[\x1b[1;34mINFO\x1b[m] "
                    "\x1b[1;32mBUILD SUCCESS\x1b[m\n")
        log = self._log("coloured.log", coloured)
        for fqcn in SHUTDOWN_CLASSES:
            proc = self._run(log, fqcn)
            self.assertEqual(proc.returncode, 0,
                             f"{fqcn} must pass on a colourised log: {proc.stderr}")

    def test_strip_ansi_removes_sgr_only(self) -> None:
        """The helper is what makes the per-class match possible at all."""
        raw = ("[\x1b[1;34mINFO\x1b[m] \x1b[1;32mTests run: \x1b[0;1;32m11\x1b[m, "
               "Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.028 s -- in "
               "com.trading.ingestion.\x1b[1mSchemaAgreementTest\x1b[m\n")
        stripped = ("[INFO] Tests run: 11, Failures: 0, Errors: 0, Skipped: 0, "
                    "Time elapsed: 0.028 s -- in com.trading.ingestion.SchemaAgreementTest\n")
        driver = self.tmp / "strip.sh"
        driver.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            + _extract_function("strip_ansi") + "\nstrip_ansi \"$1\"\n",
            encoding="utf-8",
        )
        log = self._log("raw.log", raw)
        proc = subprocess.run(["bash", str(driver), str(log)],
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout, stripped)

    def test_a_renamed_class_fails(self) -> None:
        # The class is gone from the log; BUILD SUCCESS is still there, which is
        # exactly the case the old -Dtest pin passed on.
        log = self._log("renamed.log", STEP13_LOG.replace("BridgeShutdownHookTest", "BridgeShutdown"))
        proc = self._run(log, "com.trading.ingestion.BridgeShutdownHookTest")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("GATE_FAIL", proc.stderr)
        self.assertIn("ran no tests", proc.stdout)

    def test_a_zero_count_class_fails(self) -> None:
        # A class line that exists with 0 tests is not a pass either.
        text = STEP13_LOG.replace(
            "[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.592 s -- in com.trading.ingestion.BridgeShutdownHookTest",
            "[INFO] Tests run: 0, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.0 s -- in com.trading.ingestion.BridgeShutdownHookTest")
        log = self._log("zero.log", text)
        proc = self._run(log, "com.trading.ingestion.BridgeShutdownHookTest")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("GATE_FAIL", proc.stderr)

    def test_an_absent_log_fails_closed(self) -> None:
        proc = self._run(self.tmp / "does-not-exist.log", SHUTDOWN_CLASSES[0])
        self.assertEqual(proc.returncode, 1, "a missing log must not read as a pass")

    def test_behavioural_leg_runs_against_the_real_logs_when_present(self) -> None:
        # When the wave's real Maven logs are still on disk, run the same
        # assertion against them — the fixtures above are copies of these lines.
        for path, fqcns in (("/tmp/w22-schemaperf-real.log", SCHEMA_CLASSES),
                            ("/tmp/w22-shutdownhook-v1.log", SHUTDOWN_CLASSES)):
            log = Path(path)
            if not log.is_file():
                self.skipTest(f"{path} not present (transient evidence)")
            for fqcn in fqcns:
                proc = self._run(log, fqcn)
                self.assertEqual(proc.returncode, 0, f"{path} {fqcn}: {proc.stderr}")


# L3-2: the UID-pin classes are gated on COMPUTE_INT_TEST_P6 and skip when the
# Fluss cluster is unreachable (assumeTrue). Step 16's second invocation must run
# them clean — these fixtures are the verbatim surefire shapes.
UID_CLASSES = (
    "com.trading.compute.signaljob.SignalJobOperatorUidTest",
    "com.trading.compute.signaljob.TradeDecisionsSinksUidTest",
)
UID_LOG = """\
[INFO] Running com.trading.compute.signaljob.SignalJobOperatorUidTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.204 s -- in com.trading.compute.signaljob.SignalJobOperatorUidTest
[INFO] Running com.trading.compute.signaljob.TradeDecisionsSinksUidTest
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.412 s -- in com.trading.compute.signaljob.TradeDecisionsSinksUidTest
[INFO] BUILD SUCCESS
"""


class CleanClassPin(unittest.TestCase):
    """L3-2: require_class_clean — the class ran AND did not skip."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="l32-cleancpin-")
        self.tmp = Path(self._tmp.name)
        driver = self.tmp / "driver.sh"
        driver.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "SUMMARY=/dev/null\n"
            "gate_fail() { echo \"GATE_FAIL\" >&2; exit 1; }\n"
            + _extract_function("strip_ansi")
            + "\n"
            + _extract_function("require_class_clean")
            + "\nrequire_class_clean \"$1\" \"$2\" \"$3\"\n",
            encoding="utf-8",
        )
        self.driver = driver

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _log(self, name: str, text: str) -> Path:
        path = self.tmp / name
        path.write_text(text, encoding="utf-8")
        return path

    def _run(self, log: Path, fqcn: str) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", str(self.driver), str(log), fqcn, "step 16"],
                              capture_output=True, text=True, timeout=60)

    def test_both_uid_classes_clean_pass(self) -> None:
        log = self._log("uid.log", UID_LOG)
        for fqcn in UID_CLASSES:
            proc = self._run(log, fqcn)
            self.assertEqual(proc.returncode, 0, f"{fqcn}: {proc.stderr}")

    def test_a_skipped_class_fails(self) -> None:
        # The cluster-down shape: surefire counts skipped tests in "Tests run"
        # (Tests run: 3, Skipped: 3) — require_class_ran would pass it; the
        # skipped count is what makes it a failure.
        text = UID_LOG.replace(
            "[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.204 s -- in com.trading.compute.signaljob.SignalJobOperatorUidTest",
            "[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 3, Time elapsed: 0.001 s -- in com.trading.compute.signaljob.SignalJobOperatorUidTest")
        log = self._log("skipped.log", text)
        proc = self._run(log, UID_CLASSES[0])
        self.assertEqual(proc.returncode, 1)
        self.assertIn("GATE_FAIL", proc.stderr)
        self.assertIn("must run clean", proc.stdout)

    def test_the_real_cluster_down_shape_fails(self) -> None:
        # Verbatim from the 2026-09-29 cluster-down run (logs/chg-421/
        # cluster-down-mvn.log): the @BeforeAll assumption aborts the class and
        # surefire reports 0/0/0/0 — require_class_ran's N>=1 alone would fail it,
        # and this pins that shape so it can never read as a pass.
        text = UID_LOG.replace(
            "[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.204 s -- in com.trading.compute.signaljob.SignalJobOperatorUidTest",
            "[INFO] Tests run: 0, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.734 s -- in com.trading.compute.signaljob.SignalJobOperatorUidTest")
        log = self._log("cluster-down.log", text)
        proc = self._run(log, UID_CLASSES[0])
        self.assertEqual(proc.returncode, 1)
        self.assertIn("GATE_FAIL", proc.stderr)
        self.assertIn("must run clean", proc.stdout)

    def test_a_class_summary_absent_fails(self) -> None:
        log = self._log("absent.log", UID_LOG.replace("SignalJobOperatorUidTest", "RenamedUidTest"))
        proc = self._run(log, UID_CLASSES[0])
        self.assertEqual(proc.returncode, 1)
        self.assertIn("no class summary", proc.stdout)

    def test_step_16_wires_the_second_invocation(self) -> None:
        self.assertIn('COMPUTE_UID_PIN_LOG="$OUT_DIR/compute-uid-pin.log"', SRC)
        self.assertIn("COMPUTE_INT_TEST_P6=true", SRC)
        self.assertIn("-Dtest=SignalJobOperatorUidTest,TradeDecisionsSinksUidTest", SRC)
        self.assertIn('FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"', SRC)
        self.assertIn("mvn -o test -Pdrill-reports", SRC,
                      "the UID run must keep its XMLs out of the default reports dir (C6)")
        for fqcn in UID_CLASSES:
            self.assertIn(f'require_class_clean "$COMPUTE_UID_PIN_LOG" "{fqcn}"', SRC)
        self.assertEqual(SRC.count("if step_active "), 19, "L3-2 must not add a numbered step")


if __name__ == "__main__":
    unittest.main()
