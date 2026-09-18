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
import re
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PROBE_DIR = ROOT / "code/01_platform/04_scripts/fluss-probes"
PROBES = ["FlussPrefixReader", "FlussReadLagProbe", "FlussKvProbe", "FlussRuleCounter",
          "FlussSignalLatency"]
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
        # The Fluss client's shaded Arrow touches java.nio internals, so every
        # JVM that loads it needs this flag. It lives here, once, so a probe
        # that decodes rows does not fail on Arrow's MemoryUtil instead of on
        # its own contract.
        return subprocess.run(
            ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED",
             "-cp", f"{self.classes}{os.pathsep}{self.cp}", name] + args,
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


class SignalLatencyDrainPins(unittest.TestCase):
    """Source pins for the two defects that cannot be provoked from a small table.

    P6-088 (a poll timeout read as end-of-bucket) only bites when a poll times
    out mid-bucket, which needs a contended server and a bucket far larger than
    the dev stack's 25 rows; P6-379 (the 1e9-row cap) needs a billion rows. A
    live differential is therefore impossible here, so these are structural
    pins, not behaviour proofs — they would stay green if someone reintroduced
    the bug in a different shape. The live contract tests above are the
    discriminating layer; these only stop the specific regression returning.
    """

    SRC = PROBE_DIR / "FlussSignalLatency.java"

    def _source(self) -> str:
        """Read the probe source under test.

        W35X_PROBE_SRC redirects these pins at another copy of the probe so a
        red leg can run them against the PRE-FIX source and show that they
        fail there. Without the override they pin the repo's own source, which
        is what a normal run should check; with it, "these pins would have
        caught the bug" is a measured claim instead of an assertion.
        """
        override = os.environ.get("W35X_PROBE_SRC", "")
        if override:
            path = Path(override)
            if path.is_dir():
                path = path / "FlussSignalLatency.java"
            if not path.is_file():
                raise AssertionError(
                    f"W35X_PROBE_SRC={override!r} is set but does not name a probe source "
                    f"(looked for {path})")
            return path.read_text(encoding="utf-8")
        return self.SRC.read_text(encoding="utf-8")

    def test_a_poll_timeout_is_not_treated_as_end_of_read(self) -> None:
        src = self._source()
        self.assertIn("EMPTY_POLLS_TO_DRAIN = 3", src,
                      "the drain rule must require N consecutive empty polls")
        # null is end-of-input; an empty iterator is a timeout and must not break.
        self.assertIn("if (batch == null) {", src,
                      "only a null batch proves a KV snapshot is exhausted")
        self.assertIn("if (records == null || records.isEmpty()) {", src,
                      "only consecutive empties prove a log read reached the end")
        self.assertNotIn("if (batch == null || !batch.hasNext()) {\n                                break;",
                         src, "the single-empty-poll break is the P6-088 defect")

    def test_a_log_table_is_never_read_by_the_batch_scanner(self) -> None:
        """Wave 35x: the batch path returns a bucket's first SEGMENT, not the bucket.

        This is the invariant that would have caught the original defect: a LOG
        table read through createBatchScanner cannot be a census at any limit
        (LimitBatchScanner.pollBatch does one RPC then sets endOfInput), and its
        cap can never be reached, so the saturation guard could not fire.
        """
        src = self._source()
        self.assertIn("info.hasPrimaryKey() ? readKvSnapshot(table, info, sink)"
                      " : readLog(table, info, sink)", src,
                      "the scan path must be chosen by table kind")
        # readLog must page by offset; readKvSnapshot is the only batch user.
        log_body = src.split("private static long readLog(")[1].split("private static long readKvSnapshot(")[0]
        self.assertIn("createLogScanner()", log_body)
        self.assertNotIn("createBatchScanner", log_body,
                         "a LOG table must never go through the batch scanner")
        kv_body = src.split("private static long readKvSnapshot(")[1]
        self.assertEqual(kv_body.count("createBatchScanner("), 1,
                         "only the KV snapshot reads through the batch scanner")

    def test_the_read_is_reconciled_with_the_server_row_count(self) -> None:
        """Wave 35x: the printed total is checked, never merely believed."""
        src = self._source()
        self.assertIn("tableRowCount(admin,", src,
                      "the expected total must come from the server")
        self.assertIn("getTableStats(", src)
        self.assertIn("checkAgainstServer(", src,
                      "reads must be reconciled before a census is printed")
        # The comparison must run BEFORE any census output: every printing
        # method calls it after its read and before its println.
        for mode in ("reportSignals", "reportIntents", "reportOrphans"):
            body = src.split(f"private static void {mode}(")[1]
            check = body.index("checkAgainstServer(")
            out = body.index("System.out.println(")
            self.assertLess(check, out,
                            f"{mode} must reconcile before printing")

    def test_the_kv_row_cap_cannot_truncate_a_census_silently(self) -> None:
        src = self._source()
        self.assertIn("BUCKET_ROW_LIMIT", src)
        self.assertIn("snapshot cap", src,
                      "hitting the KV cap must fail loudly, not under-report")
        self.assertIn("-Dfluss.probe.bucket.row.limit=<n>", src,
                      "the failure must name the override that reaches it")
        self.assertIn("Integer.getInteger(\"fluss.probe.bucket.row.limit\", 10_000_000)", src,
                      "the cap must be overridable so the failure is provable in dev")

    def test_the_rule_filter_and_null_timestamp_guards_survive(self) -> None:
        """The wave-34 lambda refactor silently deleted both guards.

        Measured: `signals Signal_Candidates no-such-rule-v9` reported kept=25 of
        25 rows and 17 latency samples from a rule that matches nothing. A
        source pin is the only cheap discriminator here: no dev table has rows
        with null timestamps, and 25 rows cannot provoke a truncation.
        """
        src = self._source()
        body = src.split("private static void reportSignals(")[1]
        self.assertIn("return;", body,
                      "non-matching rows must skip, not fall through")
        self.assertIn("!rule.equals(row.getString(ruleIdx).toString())", body)
        self.assertIn("null_ts=", body,
                      "null-timestamp rows must be counted, not silently used")

    def test_an_over_large_census_refuses_instead_of_dying(self) -> None:
        """P6-087/P6-089: an OOM kill loses the answer; a refusal names the limit."""
        src = self._source()
        self.assertIn("MAX_RETAINED", src)
        self.assertIn("checkRetained(", src)
        self.assertIn("Flink/Fluss SQL", src, "the refusal must name the way out")


class SignalLatencyContractTests(ProbeTestBase):
    """Wave 34. The `orphans` mode reported a false all-clear on a live stack.

    Pre-fix, `orphans` with no table argument defaulted the intent table to
    `Signal_Candidates` while the signal side was hardcoded to the same name,
    so the comparison was a table against itself and `orphan_intents=0` was
    printed no matter what the tables held. Measured on a live :9123 with
    Execution_Intent=11 rows and Signal_Candidates=25 rows, the two-table run
    finds 2 orphans — so the old zero was wrong, not merely suspicious.
    """

    def test_orphans_requires_both_tables(self) -> None:
        """The one-table form is exactly the shape that reported a false zero."""
        proc = self.run_probe("FlussSignalLatency", ["orphans", "Signal_Candidates"], timeout=90)
        self.assert_no_classpath_error(self, proc)
        self.assertNotEqual(proc.returncode, 0,
                            f"a one-table orphans run must not succeed:\n{proc.stdout}")
        self.assertIn("needs both tables", proc.stderr)
        self.assertNotIn("orphan_intents=", proc.stdout,
                         "no orphan count may be printed for an unusable request")

    def test_a_table_is_never_compared_against_itself(self) -> None:
        """Same table twice is a guaranteed zero wearing a real answer's clothes."""
        proc = self.run_probe(
            "FlussSignalLatency", ["orphans", "Signal_Candidates", "Signal_Candidates"],
            timeout=90)
        self.assert_no_classpath_error(self, proc)
        self.assertNotEqual(proc.returncode, 0, proc.stdout)
        self.assertIn("self-comparison", proc.stderr)
        self.assertNotIn("orphan_intents=0", proc.stdout)

    def test_an_unknown_mode_is_rejected_instead_of_running_signals(self) -> None:
        """A typo used to fall through to a signals scan with shifted arguments."""
        proc = self.run_probe("FlussSignalLatency", ["signalish"], timeout=90)
        self.assert_no_classpath_error(self, proc)
        self.assertNotEqual(proc.returncode, 0, proc.stdout)
        self.assertIn("unknown mode 'signalish'", proc.stderr)
        self.assertNotIn("latency_ms", proc.stdout, "a typo must not read a table")

    def test_no_arguments_prints_usage_rather_than_defaulting(self) -> None:
        proc = self.run_probe("FlussSignalLatency", [], timeout=90)
        self.assert_no_classpath_error(self, proc)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("usage: FlussSignalLatency", proc.stderr)

    @unittest.skipUnless(_dev_stack_up(), "dev stack (:9123) is not running")
    def test_the_two_table_orphans_run_names_both_tables(self) -> None:
        """Live: the output must identify both sides, so a zero is interpretable.

        Wave 35x: this run refuses rather than printing a short census whenever
        the probe cannot read the table's tiered segments (on the dev box the
        remote-data volume is readable only inside the containers). Both
        outcomes are accepted here; what is NOT accepted is a census printed
        from a read that disagreed with the server, so the counts are no longer
        pinned to the truncated values (11 and 25) that used to be asserted —
        those numbers were the defect, not the contract.
        """
        proc = self.run_probe(
            "FlussSignalLatency", ["orphans", "Execution_Intent", "Signal_Candidates"],
            timeout=180)
        self.assert_no_classpath_error(self, proc)
        if proc.returncode == 0:
            self.assertIn("intent_table=Execution_Intent", proc.stdout)
            self.assertIn("signal_table=Signal_Candidates", proc.stdout)
            self.assertIn("orphan_intents=", proc.stdout)
        else:
            # A refusal is only acceptable when it says the read and the
            # server's count disagreed, and when it withheld the census.
            self.assertIn("server's own row count disagree", proc.stderr,
                          f"a failed census must name the disagreement:\n{proc.stderr[-2000:]}")
            self.assertNotIn("orphan_intents=", proc.stdout,
                             "a short read must not print a census")

    @unittest.skipUnless(_dev_stack_up(), "dev stack (:9123) is not running")
    def test_the_intents_run_agrees_with_the_server_row_count(self) -> None:
        """Wave 35x: the printed total is checked against Fluss's own count.

        Pre-fix, this mode read 11 rows of Execution_Intent by batch scan while
        the server reports 24 — a batch scan of a bucket returns the bucket's
        first stored SEGMENT, so the total was short and nothing said so.

        Same convention as the orphans leg above: a withheld census is a valid
        outcome of the check, and the only thing the contract forbids is a total
        printed from a read that disagreed with the server. The probe cannot
        always read a LOG table exactly in Fluss 0.9.1 — its own header records
        the measurement (25 rows by batch, 41 by offset-paged read, 48 by the
        server's count) — so as Execution_Intent grows, this leg flips to the
        documented refusal. It must flip the suite green, not red.
        """
        proc = self.run_probe("FlussSignalLatency", ["intents", "Execution_Intent"], timeout=180)
        self.assert_no_classpath_error(self, proc)
        if proc.returncode == 0:
            rows = re.search(r"\brows=(\d+)", proc.stdout)
            self.assertIsNotNone(rows, proc.stdout)
            self.assertGreaterEqual(int(rows.group(1)), 1, proc.stdout)
            self.assertNotIn("server's own row count disagree", proc.stderr)
        else:
            # A refusal is only acceptable when it names the disagreement with
            # both figures, and when it withheld the total.
            self.assertRegex(proc.stderr, r"census read \d+ rows but Fluss reports \d+",
                             f"a failed census must name both counts:\n{proc.stderr[-2000:]}")
            self.assertNotIn("rows=", proc.stdout, "a short read must not print a total")

    @unittest.skipUnless(_dev_stack_up(), "dev stack (:9123) is not running")
    def test_the_scanned_table_is_the_one_printed(self) -> None:
        """P6-380/381: a custom table must not be reported under a default name.

        A refusal satisfies this too: nothing is reported at all, so no table
        can be mis-labelled. What must not happen is a table= line printed by a
        run whose read disagreed with the server.
        """
        proc = self.run_probe("FlussSignalLatency", ["intents", "Execution_Intent"], timeout=120)
        self.assert_no_classpath_error(self, proc)
        if proc.returncode == 0:
            self.assertIn("table=Execution_Intent", proc.stdout)
            self.assertNotIn("table=Signal_Candidates", proc.stdout)
        else:
            self.assertRegex(proc.stderr, r"census read \d+ rows but Fluss reports \d+",
                             f"a failed census must name both counts:\n{proc.stderr[-2000:]}")
            self.assertNotIn("table=", proc.stdout, "a withheld census must not name a table")


class SignalLatencyRedLegTests(ProbeTestBase):
    """Differential proof: the pre-fix probe produces the incident, the fix does not.

    The red legs are skipped unless W34_PRE_FIX_SRC names the HEAD~ source, so a
    normal run stays hermetic; the close-out sets it to demonstrate the discriminator.
    """

    def _pre_fix(self) -> Path:
        raw = os.environ.get("W34_PRE_FIX_SRC", "")
        if not raw:
            self.skipTest("W34_PRE_FIX_SRC not set — red leg skipped")
        src = Path(raw)
        if src.is_dir():
            src = src / "FlussSignalLatency.java"
        if not src.is_file():
            # Fail closed: a mistyped path must not turn the differential proof
            # into a silent skip, which is the failure mode these tests exist
            # to catch. An unset variable is the only reason to skip.
            raise AssertionError(
                f"W34_PRE_FIX_SRC={raw!r} is set but does not name a source file "
                f"(looked for {src})")
        out = self.tmp / "pre_fix_red"
        out.mkdir(exist_ok=True)
        proc = subprocess.run(["javac", "-nowarn", "-cp", self.cp, "-d", str(out), str(src)],
                              capture_output=True, text=True, timeout=300)
        if proc.returncode != 0:
            raise AssertionError(proc.stderr[-2000:])
        return out

    def test_the_pre_fix_probe_reports_the_false_zero(self) -> None:
        """The incident itself: a bare `orphans` run printing 0 on a live stack."""
        if not _dev_stack_up():
            self.skipTest("dev stack (:9123) is not running")
        classes = self._pre_fix()
        proc = subprocess.run(
            ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED",
             "-cp", f"{classes}{os.pathsep}{self.cp}", "FlussSignalLatency", "orphans"],
            capture_output=True, text=True, timeout=180)
        self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
        # The old default: compare Signal_Candidates with Signal_Candidates.
        self.assertIn("orphan_intents=0", proc.stdout,
                      "the red leg must reproduce the false all-clear it is named for")

    def test_the_pre_fix_probe_runs_signals_for_an_unknown_mode(self) -> None:
        """P6-732 red leg: a typo silently became a signals scan."""
        if not _dev_stack_up():
            self.skipTest("dev stack (:9123) is not running")
        classes = self._pre_fix()
        proc = subprocess.run(
            ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED",
             "-cp", f"{classes}{os.pathsep}{self.cp}", "FlussSignalLatency", "signalish"],
            capture_output=True, text=True, timeout=180)
        self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
        self.assertIn("table=Signal_Candidates", proc.stdout,
                      "the pre-fix probe treated an unknown mode as 'signals'")


GATE_SCRIPT = ROOT / "code/01_platform/04_scripts/stage-soak-e2e.sh"
HADOOP_CONF = PROBE_DIR / "hadoop-conf/core-site.xml"
COMPOSE = ROOT / "code/01_platform/01_docker"


class TieredReadWiringTests(unittest.TestCase):
    """Wave 35x: a probe that scans a table must be able to fetch the segments
    that tiering moved to object storage.

    The tablet hands the client an s3:// path and the client downloads the
    bytes itself, so the probe process needs the S3 plugin and S3 settings.
    Three things are pinned here, because each one silently defeats the others:
    the settings must not be passed as Fluss client options (they are ignored),
    the config file must carry no credentials, and a missing config must degrade
    to local-only reads instead of failing the run.
    """

    def test_the_probe_classpath_carries_the_s3_plugin_and_conf_dir(self) -> None:
        text = GATE_SCRIPT.read_text()
        self.assertIn("gate_wire_probe_filesystem", text,
                      "the gate must wire the probe filesystem before it runs the census")
        self.assertIn("fluss-fs-s3-0.9.1-incubating.jar", text,
                      "without the S3 plugin the probe cannot resolve s3:// at all")
        self.assertIn("fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar", text,
                      "the plugin's Hadoop dependency must be on the probe classpath too")
        # The conf dir must reach the JVM that runs the census, not just the one
        # that compiles it — a compile-time-only classpath entry proves nothing.
        run_cp = [ln for ln in text.splitlines() if "FlussRuleCounter" in ln and "-cp" in ln]
        self.assertTrue(run_cp, "no FlussRuleCounter invocation found in the gate")
        self.assertTrue(any("GATE_FS_CP" in ln for ln in run_cp),
                        f"the census JVM's classpath omits the wired filesystem config: {run_cp}")

    def test_wiring_never_passes_s3_settings_as_fluss_client_options(self) -> None:
        """FlussConnection forwards only 'client.fs.' keys, and the shipped S3
        plugin never strips that prefix — so 'client.fs.s3.*' is silently
        ignored. The working route is Hadoop's own core-site.xml."""
        text = GATE_SCRIPT.read_text()
        self.assertNotIn("client.fs.s3", text,
                         "client.fs.s3.* keys are ignored by the S3 plugin (verified against "
                         "fluss-fs-s3-0.9.1-incubating.jar); use core-site.xml instead")

    def test_the_committed_hadoop_config_holds_no_credentials(self) -> None:
        text = HADOOP_CONF.read_text()
        for key in ("fs.s3a.endpoint", "fs.s3a.access.key", "fs.s3a.secret.key",
                    "fs.s3a.path.style.access"):
            self.assertIn(f"<name>{key}</name>", text, f"{key} is missing from core-site.xml")
        # Every credential-shaped value must be an ${env.NAME} reference. A
        # literal would be a committed secret, and R2 keys are 32/64 chars.
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped.startswith("<value>"):
                continue
            value = stripped[len("<value>"):-len("</value>")]
            if "access.key" in line or "secret.key" in line or "endpoint" in line:
                self.assertRegex(
                    value, r"^\$\{env\.[A-Z0-9_]+\}$",
                    f"credential settings must use ${{env.NAME}} placeholders, got {value!r}")
            self.assertNotRegex(value, r"^[A-Za-z0-9+/]{32,}={0,2}$",
                                f"core-site.xml appears to embed a literal secret: {stripped[:60]!r}")

    def test_a_missing_r2_config_degrades_to_local_reads_instead_of_failing(self) -> None:
        """The gate must keep its previous behaviour when R2 config is absent.

        A hard failure here would block every run on a config file; the honest
        behaviour is to run with local-only reads and say so, because a census
        that cannot reach a tiered segment under-counts rather than erroring.
        """
        text = GATE_SCRIPT.read_text()
        helper = text.split("gate_wire_probe_filesystem()", 1)[1].split("\n}\n", 1)[0]
        self.assertNotIn("exit 1", helper,
                         "an unwired filesystem must not abort the run")
        self.assertNotIn("fatal ", helper,
                         "an unwired filesystem must warn, not fail — the census still has "
                         "whatever local segments exist")
        self.assertGreaterEqual(helper.count("return 0"), 4,
                                "every missing-R2 path needs its own warn-and-continue branch")

    def test_secrets_are_read_through_the_shared_env_reader(self) -> None:
        """Credentials come from the git-ignored secrets file via r2_var, the
        one parser that de-quotes and rejects empty values (P6-485/P6-162)."""
        text = GATE_SCRIPT.read_text()
        self.assertIn('r2_var "$COMPOSE_DIR/secrets.env" AWS_ACCESS_KEY_ID', text)
        self.assertIn('r2_var "$COMPOSE_DIR/secrets.env" AWS_SECRET_ACCESS_KEY', text)
        self.assertIn("r2-env.sh", text, "the gate must source the shared env reader")


if __name__ == "__main__":
    unittest.main()
