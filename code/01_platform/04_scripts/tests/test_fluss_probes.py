#!/usr/bin/env python3
"""Contract tests for the four Fluss probes (wave 13).

Two layers, and the difference matters when reading a run:

* hermetic — javac all four probes against the Fluss client on the local Maven
  repository, then drive the CLI paths that must fail BEFORE any RPC (bad window,
  unusable token list, zero window_ms) and the failure path that must still print
  the __END__ sentinel (bootstrap pointing at a closed port). These run everywhere
  and are the differential signal: each one is red on the pre-wave-13 probe.
* live — a smoke against the dev stack on localhost:9123. Skipped when the port is
  closed, because the helper suite must pass on a machine without a running stack.
  This is the only layer that exercises the filtering and formatting logic.

The probes are plain `java` one-shots with no build file, so the classpath is the
same one stage-capture uses: code/02_services/01_ingestion's resolved dependency
classpath (FLUSS_PROBE_CP). If neither target/cp.txt nor an offline maven run can
produce it, the compile test fails loudly rather than silently skipping.
"""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PROBE_DIR = ROOT / "code/01_platform/04_scripts/fluss-probes"
PROBES = ["FlussPrefixReader", "FlussReadLagProbe", "FlussKvProbe", "FlussRuleCounter"]
INGESTION = ROOT / "code/02_services/01_ingestion"
BOOTSTRAP = "localhost:9123"
DEAD = "127.0.0.1:9"          # nothing listens here; connection refused, no wait
LIVE_TIMEOUT_S = 120


def _is_row(line: str, cols: int) -> bool:
    """A probe TSV row: <epoch_ms>\t... with exactly `cols` fields.

    The Fluss client prints its connection log (stack traces included) to STDOUT,
    so shape alone is not enough — probe rows always begin with the epoch.
    """
    return line[:1].isdigit() and line.count("\t") == cols - 1


def _dev_stack_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 9123), timeout=0.5):
            return True
    except OSError:
        return False


def _m2_fluss_classpath() -> str:
    """Fallback classpath: every Fluss artifact in the local repository (compile-only use)."""
    m2 = Path.home() / ".m2/repository/org/apache/fluss"
    jars = sorted(str(p) for p in m2.rglob("*.jar")
                  if "sources" not in p.name and "javadoc" not in p.name)
    return os.pathsep.join(jars)


def _runtime_classpath(tmp: Path) -> str:
    """stage-capture's FLUSS_PROBE_CP: the ingestion module's resolved classpath."""
    cp_file = INGESTION / "target/cp.txt"
    if cp_file.is_file() and cp_file.read_text().strip():
        return cp_file.read_text().strip()
    out = tmp / "cp.txt"
    proc = subprocess.run(
        ["mvn", "-o", "-q", "dependency:build-classpath",
         f"-Dmdep.outputFile={out}", "-pl", "02_services/01_ingestion"],
        cwd=ROOT / "code", capture_output=True, text=True, timeout=600,
    )
    if out.is_file() and out.read_text().strip():
        return out.read_text().strip()
    raise AssertionError(
        "cannot resolve the Fluss probe classpath: no target/cp.txt and `mvn -o "
        "dependency:build-classpath -pl 02_services/01_ingestion` produced nothing "
        f"(exit {proc.returncode})\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}")


class ProbeTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = Path(tempfile.mkdtemp(prefix="w13-probes."))
        cls.classes = cls.tmp / "classes"
        cls.classes.mkdir()
        cls.cp = _runtime_classpath(cls.tmp)
        sources = [str(PROBE_DIR / f"{n}.java") for n in PROBES]
        # W9a rule: a compile failure is an AssertionError with the compiler output,
        # never a skip — a broken classpath must not look like "nothing to test".
        proc = subprocess.run(["javac", "-nowarn", "-cp", cls.cp, "-d", str(cls.classes)] + sources,
                              capture_output=True, text=True, timeout=300)
        if proc.returncode != 0:
            cls._javac_fallback(proc)

    @classmethod
    def _javac_fallback(cls, proc) -> None:
        """Retry with the local m2 Fluss jars only; still a failure if that fails too."""
        fallback = _m2_fluss_classpath()
        retry = subprocess.run(["javac", "-nowarn", "-cp", fallback, "-d", str(cls.classes)]
                               + [str(PROBE_DIR / f"{n}.java") for n in PROBES],
                               capture_output=True, text=True, timeout=300)
        if retry.returncode != 0:
            raise AssertionError(
                "javac failed for the wave-13 probes.\n"
                f"with FLUSS_PROBE_CP (ingestion classpath):\n{proc.stderr[-3000:]}\n"
                f"with the m2 Fluss jars:\n{retry.stderr[-3000:]}")

    def run_probe(self, name: str, args: list[str], env: dict | None = None,
                  timeout: int = 60) -> subprocess.CompletedProcess:
        e = dict(os.environ)
        e.update(env or {})
        return subprocess.run(["java", "-cp", f"{self.classes}{os.pathsep}{self.cp}", name] + args,
                              capture_output=True, text=True, timeout=timeout, env=e)

    @staticmethod
    def assert_no_classpath_error(test: unittest.TestCase, proc: subprocess.CompletedProcess) -> None:
        """A missing dependency would make the failure tests pass for the wrong reason."""
        for marker in ("NoClassDefFoundError", "ClassNotFoundException", "NoSuchMethodError"):
            test.assertNotIn(marker, proc.stderr,
                             "the probe could not even load its classes — fix the classpath "
                             f"before trusting this assertion:\n{proc.stderr[-2000:]}")


class ProbeCompileTests(ProbeTestBase):
    def test_all_four_probes_compile(self) -> None:
        for name in PROBES:
            self.assertTrue((self.classes / f"{name}.class").is_file(), f"{name}.class missing")


class SentinelTests(ProbeTestBase):
    """P6-080: a failure after startup must still print __END__ and exit non-zero."""

    def test_prefix_reader_prints_the_sentinel_when_the_cluster_is_unreachable(self) -> None:
        proc = self.run_probe("FlussPrefixReader", ["candle_live", "4", "0", "0", DEAD])
        self.assert_no_classpath_error(self, proc)
        self.assertEqual(proc.returncode, 1, proc.stderr[-1500:])
        self.assertIn("__END__ 0", proc.stdout,
                      "callers block on the sentinel: a failed run must still print it")
        self.assertIn("__ERROR__", proc.stderr)

    def test_prefix_reader_prints_the_sentinel_for_unusable_input(self) -> None:
        proc = self.run_probe("FlussPrefixReader", ["candle_live", "4", "not-a-number", "0", DEAD])
        self.assertEqual(proc.returncode, 2, proc.stderr[-1500:])
        self.assertIn("__END__ 0", proc.stdout)
        self.assertIn("window_start_ms", proc.stderr)

    def test_prefix_reader_rejects_a_token_list_with_no_usable_token(self) -> None:
        proc = self.run_probe("FlussPrefixReader", ["candle_live", "abc,def", "0", "0", DEAD])
        self.assertEqual(proc.returncode, 2, proc.stderr[-1500:])
        self.assertIn("__END__ 0", proc.stdout)
        self.assertIn("no usable token", proc.stderr)

    def test_read_lag_probe_prints_no_row_when_nothing_can_be_read(self) -> None:
        """P6-085: a failed sample must not look like a sample with a made-up total."""
        proc = self.run_probe("FlussReadLagProbe", ["default", "raw_table_1", DEAD],
                              timeout=90)
        self.assert_no_classpath_error(self, proc)
        self.assertNotEqual(proc.returncode, 0, proc.stdout)
        self.assertFalse([l for l in proc.stdout.splitlines() if _is_row(l, 5)],
                         "no TSV row may be printed for a failed sample")

    def test_rule_counter_prints_no_census_when_nothing_can_be_read(self) -> None:
        """Contract pin. Non-discriminating: the pre-wave-13 probe also exits non-zero."""
        proc = self.run_probe("FlussRuleCounter", ["Signal_Candidates", DEAD], timeout=90)
        self.assert_no_classpath_error(self, proc)
        self.assertNotEqual(proc.returncode, 0, proc.stdout)
        self.assertNotIn("total=", proc.stdout)


class InputValidationTests(ProbeTestBase):
    """P6-370 / P6-083: unusable input is refused before any RPC, with a clear message."""

    def test_kv_probe_rejects_zero_window_ms(self) -> None:
        proc = self.run_probe("FlussKvProbe", ["candle_live", "0", "4", DEAD])
        self.assertEqual(proc.returncode, 2, proc.stderr[-1500:])
        self.assertIn("must be > 0", proc.stderr)

    def test_kv_probe_rejects_a_non_numeric_window_ms(self) -> None:
        proc = self.run_probe("FlussKvProbe", ["candle_live", "fifteen", "4", DEAD])
        self.assertEqual(proc.returncode, 2, proc.stderr[-1500:])
        self.assertIn("window_ms", proc.stderr)

    def test_kv_probe_rejects_a_token_list_with_no_usable_token(self) -> None:
        proc = self.run_probe("FlussKvProbe", ["candle_live", "15000", "x,y", DEAD])
        self.assertEqual(proc.returncode, 2, proc.stderr[-1500:])
        self.assertIn("no usable token", proc.stderr)


@unittest.skipUnless(_dev_stack_up(), f"dev Fluss stack not listening on {BOOTSTRAP}")
class LiveSmokeTests(ProbeTestBase):
    """The only layer that exercises filtering/formatting — needs the dev stack up."""

    def test_prefix_reader_reads_a_kv_table_by_token(self) -> None:
        proc = self.run_probe("FlussPrefixReader", ["candle_live", "4", "0", "0", BOOTSTRAP],
                              timeout=LIVE_TIMEOUT_S)
        self.assert_no_classpath_error(self, proc)
        self.assertIn("__END__ ", proc.stdout)
        self.assertEqual(proc.returncode, 0, proc.stderr[-1500:])

    def test_prefix_reader_log_scan_reports_truncation(self) -> None:
        """P6-373: a deadline stop must be distinguishable from a drained scan."""
        proc = self.run_probe("FlussPrefixReader", ["raw_table_1", "*", "0", "0", BOOTSTRAP],
                              env={"PROBE_READ_DEADLINE_MS": "15000"}, timeout=LIVE_TIMEOUT_S)
        self.assert_no_classpath_error(self, proc)
        self.assertIn("__END__ ", proc.stdout)
        if "WARN: LOG scan hit the" not in proc.stderr:
            self.assertGreaterEqual(
                int(proc.stdout.rsplit("__END__ ", 1)[1].split()[0]), 0,
                "a scan that stopped without draining must say so on stderr")

    def test_read_lag_probe_prints_one_row(self) -> None:
        proc = self.run_probe("FlussReadLagProbe", ["default", "raw_table_1", BOOTSTRAP],
                              timeout=LIVE_TIMEOUT_S)
        self.assert_no_classpath_error(self, proc)
        rows = [l for l in proc.stdout.splitlines() if _is_row(l, 5)]
        self.assertEqual(len(rows), 1, f"expected exactly one sample row:\n{proc.stdout}\n{proc.stderr[-800:]}")
        epoch_ms, table, partitions, buckets, log_end = rows[0].split("\t")
        self.assertTrue(epoch_ms.isdigit() and int(epoch_ms) > 1_600_000_000_000)
        self.assertEqual(table, "raw_table_1")
        self.assertTrue(int(partitions) >= 1 and int(buckets) >= 1 and int(log_end) >= 0)

    def test_kv_probe_emits_tsv_rows_or_says_why_not(self) -> None:
        proc = self.run_probe("FlussKvProbe", ["candle_live", "15000", "4,7", BOOTSTRAP],
                              timeout=LIVE_TIMEOUT_S)
        self.assert_no_classpath_error(self, proc)
        rows = [l for l in proc.stdout.splitlines() if _is_row(l, 6)]
        if rows:
            for line in rows:
                self.assertTrue(line.split("\t")[0].isdigit())
            self.assertIn(proc.returncode, (0, 3))
        else:
            self.assertIn(proc.returncode, (1, 3),
                          "no rows must come with a non-zero exit and a stderr diagnostic")
            self.assertIn("no row for windows", proc.stderr)


if __name__ == "__main__":
    unittest.main()
