"""Tests for the four watch commands (raw / live / candles / signals).

Static + stubbed coverage only; the live cluster is opt-in and smoked manually:

- Makefile wiring: the four targets exist, are phony, and call the right scripts.
- signal-candidates-table.sh: argv contract, stub-docker argv/mount, probe pins.
- strategy_live_board.py: job selection, vertex matching, board rendering with
  injected fake metrics, and the two Flink metric response shapes.
"""
import importlib.util
import os
import shutil
import stat
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
MAKEFILE = os.path.join(ROOT, "Makefile")
SIGNAL_RUNNER = os.path.join(SCRIPTS, "signal-candidates-table.sh")
SIGNAL_PROBE = os.path.join(SCRIPTS, "fluss-probes", "SignalCandidatesViewer.java")
BOARD = os.path.join(SCRIPTS, "strategy_live_board.py")


def load_board():
    spec = importlib.util.spec_from_file_location("strategy_live_board", BOARD)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MakefileWiring(unittest.TestCase):
    def setUp(self):
        self.text = open(MAKEFILE, encoding="utf-8").read()

    def test_the_four_targets_are_wired_to_the_right_scripts(self):
        expected = {
            "watch-raw": "./show-ticks.sh",
            "watch-live": "strategy_live_board.py",
            "watch-candles": "candle-features-table.sh",
            "watch-signals": "signal-candidates-table.sh",
        }
        for target, script in expected.items():
            marker = "\n%s:" % target
            self.assertIn(marker, self.text, "missing target %s" % target)
            recipe = self.text.split(marker, 1)[1].split("\n\n", 1)[0]
            self.assertIn(script, recipe, "%s must call %s" % (target, script))

    def test_the_four_targets_are_phony(self):
        phony = [line for line in self.text.splitlines() if line.startswith(".PHONY:")]
        joined = " ".join(phony)
        for target in ("watch-raw", "watch-live", "watch-candles", "watch-signals"):
            self.assertIn(target, joined, "%s must be phony" % target)

    def test_help_names_the_four_targets(self):
        for target in ("watch-raw", "watch-live", "watch-candles", "watch-signals"):
            self.assertIn(target, self.text, "%s missing from help" % target)


class SignalRunnerArgvContract(unittest.TestCase):
    def run_script(self, *args):
        return subprocess.run(["bash", SIGNAL_RUNNER] + list(args),
                              capture_output=True, text=True)

    def test_syntax(self):
        r = subprocess.run(["bash", "-n", SIGNAL_RUNNER], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_non_numeric_rows_is_usage_error(self):
        r = self.run_script("--rows", "abc")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_zero_rows_is_usage_error(self):
        r = self.run_script("--rows", "0")
        self.assertEqual(r.returncode, 2)

    def test_missing_value_is_usage_error(self):
        r = self.run_script("--rule")
        self.assertEqual(r.returncode, 2)

    def test_unknown_flag_is_usage_error(self):
        r = self.run_script("--nope")
        self.assertEqual(r.returncode, 2)

    def test_probe_source_pins_the_signal_shape_and_the_full_audit(self):
        self.assertTrue(os.path.exists(SIGNAL_PROBE), SIGNAL_PROBE)
        text = open(SIGNAL_PROBE, encoding="utf-8").read()
        self.assertIn("class SignalCandidatesViewer", text)
        # the required columns are resolved by name from the live schema
        for column in ("candidate_id", "instrument_token", "symbol", "rule_id",
                       "detection_ts", "evaluation_ts", "score_inputs"):
            self.assertIn('"%s"' % column, text, column)
        self.assertIn("subscribeFromBeginning", text)
        self.assertIn('"Signal_Candidates"', text)
        self.assertIn('"--full"', text)

    @unittest.skipUnless(shutil.which("javac") and os.path.exists(
        os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt")),
        "needs a JDK and a built ingestion classpath")
    def test_probe_compiles(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copy(SIGNAL_PROBE, os.path.join(d, "SignalCandidatesViewer.java"))
            cp = open(os.path.join(ROOT, "code", "02_services", "01_ingestion",
                                   "target", "cp.txt"), encoding="utf-8").read().strip()
            r = subprocess.run(["javac", "-nowarn", "-cp", cp, "-d", d,
                                os.path.join(d, "SignalCandidatesViewer.java")],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


class SignalRunnerStubbedDocker(unittest.TestCase):
    @unittest.skipUnless(shutil.which("javac") and os.path.exists(
        os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt")),
        "needs a JDK and a built ingestion classpath")
    def test_run_passes_argv_and_mounts_readable(self):
        with tempfile.TemporaryDirectory() as td:
            stub = os.path.join(td, "docker")
            log = os.path.join(td, "docker-observed.txt")
            with open(stub, "w", encoding="utf-8") as fh:
                fh.write(
                    '#!/usr/bin/env bash\n'
                    'set -u\n'
                    'printf "argv %s\\n" "$*" >> "$DOCKER_STUB_LOG"\n'
                    'case "$1" in\n'
                    '  run)\n'
                    '    src=""\n'
                    '    for a in "$@"; do\n'
                    '      case "$a" in *:/tmp/probe:ro) src="${a%%:/tmp/probe:ro}" ;; esac\n'
                    '    done\n'
                    '    printf "mount_mode %s\\n" "$(stat -c %a "$src")" >> "$DOCKER_STUB_LOG"\n'
                    '    printf "mount_classes %s\\n" "$(ls -1 "$src" | grep -c "\\.class$")"'
                    ' >> "$DOCKER_STUB_LOG"\n'
                    '    printf "SIGNALS table=default.Signal_Candidates inspected=1 shown=1\\n"\n'
                    '    ;;\n'
                    'esac\n'
                    'exit 0\n')
            os.chmod(stub, 0o755)
            env = os.environ.copy()
            env["PATH"] = td + os.pathsep + env["PATH"]
            env["DOCKER_STUB_LOG"] = log
            r = subprocess.run(["bash", SIGNAL_RUNNER, "--rows", "2", "--full"],
                               capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn("SIGNALS table=default.Signal_Candidates inspected=1 shown=1",
                          r.stdout)
            observed = open(log, encoding="utf-8").read()
            argv = [line for line in observed.splitlines() if line.startswith("argv ")][0]
            self.assertIn("SignalCandidatesViewer", argv)
            self.assertIn("--rows 2", argv)
            self.assertIn("--full", argv)
            self.assertIn("--database default", argv)
            self.assertIn("--table Signal_Candidates", argv)
            self.assertIn("/tmp/probe:/app/ingestion.jar", argv)
            mode = int([line.split(" ", 1)[1] for line in observed.splitlines()
                        if line.startswith("mount_mode ")][0], 8)
            self.assertTrue(mode & stat.S_IROTH and mode & stat.S_IXOTH,
                            "container uid must be able to traverse the mounted build dir")
            classes = int([line.split(" ", 1)[1] for line in observed.splitlines()
                           if line.startswith("mount_classes ")][0])
            self.assertGreater(classes, 0, "probe was not compiled into the build dir")


class LiveBoard(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.board = load_board()

    def test_select_running_job_ignores_canceled(self):
        overview = {"jobs": [
            {"jid": "a", "name": "signal-job-compute", "state": "CANCELED"},
            {"jid": "b", "name": "signal-job-compute", "state": "RUNNING"},
            {"jid": "c", "name": "other", "state": "RUNNING"},
        ]}
        job = self.board.select_running_job(overview)
        self.assertEqual("b", job["jid"])
        self.assertIsNone(self.board.select_running_job({"jobs": []}))

    def test_find_vertex_matches_case_insensitive_substring(self):
        vertices = [{"id": "v1", "name": "Source: raw-source"},
                    {"id": "v2", "name": "multi-tf-aggregator"},
                    {"id": "v3", "name": "strategy-host"}]
        self.assertEqual("v2", self.board.find_vertex(vertices, "multi-tf-aggregator")["id"])
        self.assertEqual("v3", self.board.find_vertex(vertices, "STRATEGY-HOST")["id"])
        self.assertIsNone(self.board.find_vertex(vertices, "nope"))

    def test_metric_values_reads_both_response_shapes(self):
        original = self.board.fetch_json
        try:
            self.board.fetch_json = lambda url, timeout=5: [
                {"id": "0", "value": "12.5"}, {"id": "1", "value": "NaN"}]
            self.assertEqual([12.5], self.board.metric_values("http://x", "j", "v", "m"))
            self.board.fetch_json = lambda url, timeout=5: [{"id": "m", "sum": 7.0}]
            self.assertEqual([7.0], self.board.metric_values("http://x", "j", "v", "m"))
            self.board.fetch_json = lambda url, timeout=5: (_ for _ in ()).throw(OSError())
            self.assertEqual([], self.board.metric_values("http://x", "j", "v", "m"))
        finally:
            self.board.fetch_json = original

    def test_render_board_prints_operators_counters_and_latency(self):
        job = {"jid": "jid1", "name": "signal-job-compute", "state": "RUNNING",
               "start-time": 1_000_000_000_000}
        vertices = [
            {"id": "agg", "name": "multi-tf-aggregator", "parallelism": 8},
            {"id": "host", "name": "strategy-host", "parallelism": 8},
        ]
        values = {
            ("agg", "numRecordsInPerSecond"): [100.0, 200.0],
            ("agg", "numRecordsOutPerSecond"): [100.0, 200.0],
            ("agg", "numRecordsIn"): [10.0], ("agg", "numRecordsOut"): [10.0],
            ("agg", "busyTimeMsPerSecond"): [100.0, 200.0],
            ("agg", "backPressuredTimeMsPerSecond"): [0.0, 50.0],
            ("host", "numRecordsInPerSecond"): [300.0],
            ("host", "numRecordsOutPerSecond"): [1.0],
            ("host", "numRecordsIn"): [30.0], ("host", "numRecordsOut"): [3.0],
            ("host", "busyTimeMsPerSecond"): [10.0],
            ("host", "backPressuredTimeMsPerSecond"): [0.0],
            ("agg", "compute.market.tick.emitted"): [4.0],
            ("host", "compute.merged.rows.emitted"): [9.0],
            ("host", "compute.latency.tick_to_strategy_p50"): [40.0, 60.0],
            ("host", "compute.latency.tick_to_strategy_p95"): [80.0],
            ("host", "compute.latency.tick_to_strategy_p99"): [93.0],
        }
        board = self.board.render_board(
            "http://x", job, vertices, lambda vid, name: values.get((vid, name), []))
        self.assertIn("strategy-live :: signal-job-compute RUNNING", board)
        self.assertIn("multi-tf-aggregator", board)
        self.assertIn("strategy-host", board)
        self.assertIn("300.0", board, "sum of per-subtask in/s")
        self.assertIn("compute.market.tick.emitted", board)
        self.assertIn("4", board)
        self.assertIn("compute.latency.tick_to_strategy", board)
        self.assertIn("p50=60.0", board, "worst subtask, not the average")
        self.assertIn("p99=93.0", board)

    def test_render_no_job_reports_states_and_the_start_hint(self):
        overview = {"jobs": [
            {"jid": "a", "name": "signal-job-compute", "state": "CANCELED",
             "start-time": 1_000_000_000_000},
            {"jid": "b", "name": "other", "state": "RUNNING",
             "start-time": 1_000_000_000_000},
        ]}
        text = self.board.render_no_job(overview)
        self.assertIn("no RUNNING signal-job-compute", text)
        self.assertIn("CANCELED", text)
        self.assertIn("make day", text)
        self.assertNotIn(" other ", text, "unrelated jobs are not listed")

    def test_cli_help_exits_zero(self):
        r = subprocess.run(["python3", BOARD, "--help"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("--watch", r.stdout)

    def test_cli_rejects_negative_watch(self):
        r = subprocess.run(["python3", BOARD, "--watch", "-1"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)


if __name__ == "__main__":
    unittest.main()
