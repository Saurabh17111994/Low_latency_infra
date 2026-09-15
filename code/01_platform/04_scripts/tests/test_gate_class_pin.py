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


if __name__ == "__main__":
    unittest.main()
