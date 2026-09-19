"""Wave 42 — O2 provisioning/ingest tooling and the stage-capture parser.

Offline and hermetic: no cluster, no network, no docker. The O2 network paths are
exercised by monkeypatching each module's own helpers, and the compose path by
patching local_int_004_smoke's `_compose_json`, so nothing here touches a live
stack.
"""

import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "code" / "01_platform" / "04_scripts"
sys.path.insert(0, str(SCRIPTS))

RECONCILE = SCRIPTS / "ing-tcp001" / "reconcile-compare.py"
O2_PROVISION = SCRIPTS / "o2-provision.py"
O2_INGEST = SCRIPTS / "o2_ingest.py"


def load_module(name, path):
    """Load a script whose filename is not a valid module name (o2-provision.py)."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def run(*args):
    return subprocess.run(
        ["python3", *[str(a) for a in args]], capture_output=True, text=True, timeout=120
    )


class ReconcilePreGuardTest(unittest.TestCase):
    """P6-428 (a truncated pre-probe must fail closed), P6-748 (window delta)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def _w(self, name, body):
        p = self.d / name
        p.write_text(body)
        return p

    def _run(self, *args):
        return run(RECONCILE, "--bridge", self.bridge, *args)

    def test_truncated_pre_probe_fails_closed(self):
        # A truncated pre probe parses to {} — without a guard every delta is
        # post-0, so a real loss still passes the default >= comparison.
        self.bridge = self._w("bridge.txt", "arrow-tick-counts: total=5 t=100:n=5\n")
        pre = self._w("pre.txt", "")
        post = self._w("post.txt", "TOKEN 100 RAW=0 QUAR=5 TOTAL=5\n")
        r = self._run("--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("pre", r.stdout.lower())

    def test_pre_existing_raw_rows_do_not_fail_a_clean_window(self):
        # sink=quar: the window lost nothing (quar +1 for a bridge count of 1),
        # but RAW already held 5 rows from an earlier run on a reused cluster.
        self.bridge = self._w("bridge.txt", "arrow-tick-counts: total=1 t=100:n=1\n")
        pre = self._w("pre.txt", "TOKEN 100 RAW=5 QUAR=0 TOTAL=5\n")
        post = self._w("post.txt", "TOKEN 100 RAW=5 QUAR=1 TOTAL=6\n")
        r = self._run("--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("RESULT PASS", r.stdout)

    def test_fresh_raw_rows_in_the_window_still_fail(self):
        # The delta is what matters: one new RAW row in a quar-mode window is
        # still a real mismatch.
        self.bridge = self._w("bridge.txt", "arrow-tick-counts: total=1 t=100:n=1\n")
        pre = self._w("pre.txt", "TOKEN 100 RAW=5 QUAR=0 TOTAL=5\n")
        post = self._w("post.txt", "TOKEN 100 RAW=6 QUAR=1 TOTAL=7\n")
        r = self._run("--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)


class LocalInt004DdlTest(unittest.TestCase):
    """P6-457 — the DDL loop validated nothing (and never checked 13_order_correlation)."""

    CFG = {
        "networks": {"execution-net": {"internal": True}},
        "services": {
            "execution-bridge": {},
            "execution-gateway": {},
            "nautilus": {"environment": {"EXECUTION_ENABLED": "false"}},
        },
    }

    def _contract_with_ddl(self, present):
        mod = load_module("local_int_004_smoke", SCRIPTS / "local_int_004_smoke.py")
        root = Path(tempfile.mkdtemp(prefix="w42-ddl-"))
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        ddl = root / "code" / "01_platform" / "02_sql" / "ddl"
        ddl.mkdir(parents=True)
        for name in present:
            (ddl / name).write_text("-- fixture\n")
        mod.ROOT = root
        with mock.patch.object(mod, "_compose_json", return_value=self.CFG):
            return mod.offline_contract()

    def test_missing_order_correlation_ddl_is_reported(self):
        errs = self._contract_with_ddl(["09_order_lifecycle.sql", "10_positions.sql"])
        self.assertTrue(
            any("13_order_correlation" in e for e in errs),
            f"a missing 13_order_correlation.sql was not reported: {errs}",
        )

    def test_all_three_ddl_files_present_reports_no_ddl_error(self):
        errs = self._contract_with_ddl(
            ["09_order_lifecycle.sql", "10_positions.sql", "13_order_correlation.sql"]
        )
        self.assertFalse(
            [e for e in errs if "ddl" in e.lower() or "_order_" in e or "positions" in e],
            f"unexpected DDL errors: {errs}",
        )

    def test_no_no_op_lifecycle_loop(self):
        # P6-458: the loop iterated a literal list and its empty-sequence branch
        # was unreachable; the sample it consumed existed only for that loop.
        src = (SCRIPTS / "local_int_004_smoke.py").read_text()
        for dead in ("if not seq:", "rnd=random.Random", "import argparse, json, random"):
            self.assertFalse(dead in src, f"dead code still present: {dead}")


class O2ProvisionTest(unittest.TestCase):
    """P6-459/460/461/757/758/759 in o2-provision.py."""

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("O2_AUTH_BASIC", "dGVzdDp0ZXN0")
        cls._argv = sys.argv
        sys.argv = ["o2-provision.py"]
        try:
            cls.mod = load_module("o2_provision", O2_PROVISION)
        finally:
            sys.argv = cls._argv

    def test_seed_histogram_obeys_the_otlp_bucket_rule(self):
        # OTLP: bucketCounts has exactly len(explicitBounds)+1 entries (the +Inf
        # bucket). The real emitter applies this (R-036); the seed must too.
        name, bounds, counts, _total, count = self.mod.SEED_HISTOGRAM
        self.assertEqual(
            len(counts), len(bounds) + 1,
            f"{name}: {len(bounds)} bounds need {len(bounds)+1} bucket counts, got {len(counts)}",
        )
        self.assertEqual(sum(int(c) for c in counts), int(count), "buckets must sum to count")

    def test_net_rule_threshold_matches_its_description(self):
        # P6-758: the rule is a byte rate, so the description must not claim a
        # percentage of a capacity the expression never divides by.
        rule = next(r for r in self.mod.ALERTS if r["name"] == "INFRA-warn-net-80")
        self.assertIn("rate(", rule["promql"])
        op, threshold = rule["promql_condition"]
        self.assertGreater(
            threshold, 1_000,
            f"a byte-rate threshold of {threshold} trips on keep-alives",
        )
        self.assertNotIn("%", rule["desc"], "a byte rate cannot be a percentage")

    def test_dead_helpers_are_gone(self):
        src = O2_PROVISION.read_text()
        for dead in ("METRIC_TYPES = {", "def make_panel(", "def stream("):
            self.assertFalse(dead in src, f"dead definition still present: {dead}")

    def test_o2_unreachable_reports_not_ready_instead_of_a_traceback(self):
        # P6-461: only HTTPError was caught, so a stopped/unreachable O2 raised
        # URLError as a traceback instead of the documented health verdict.
        # Port 9 (discard) refuses instantly; no stack is touched.
        env = dict(os.environ, O2_AUTH_BASIC="dGVzdDp0ZXN0")
        r = subprocess.run(
            [sys.executable, str(O2_PROVISION), "http://127.0.0.1:9"],
            capture_output=True, text=True, timeout=60, env=env,
        )
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("not ready", r.stdout + r.stderr)
        self.assertNotIn("Traceback", r.stderr)

    def _api_stub(self, title, get_id_response, put_response=(200, {"hash": "h2"})):
        def api(method, path, body=None):
            if method == "GET" and path == "/dashboards":
                return 200, {"dashboards": [{"title": title, "dashboard_id": "d1"}]}
            if method == "GET":
                return get_id_response
            return put_response

        return api

    def test_dashboard_get_with_non_dict_body_is_reported_not_raised(self):
        # api() returns a raw string body on HTTPError, so full.get() would raise
        # AttributeError and abort the whole provisioning run.
        title = self.mod.DASHBOARDS[0]["title"]
        with mock.patch.object(self.mod, "api", self._api_stub(title, (500, "boom"))):
            out = io.StringIO()
            with mock.patch("sys.stdout", out):
                failures = self.mod.provision_dashboards()
        self.assertTrue(failures, "a failed dashboard GET must be counted")
        self.assertIn("boom", out.getvalue())

    def test_failed_dashboard_rewrite_is_not_called_converged(self):
        # A stale ?hash= makes the PUT fail; the run must not then report
        # "dashboard converged" and exit 0. The COMMAND dashboard converges by a
        # single hash-tagged PUT rewrite, so it is the path that can lie.
        title = next(d["title"] for d in self.mod.DASHBOARDS
                     if d["title"] == "COMMAND - Command Center")
        get_ok = (200, {"hash": "h", "v8": {"tabs": [{"tabId": "t0", "panels": []}]}})
        with mock.patch.object(self.mod, "api",
                               self._api_stub(title, get_ok, (500, "stale hash"))):
            out = io.StringIO()
            with mock.patch("sys.stdout", out):
                failures = self.mod.provision_dashboards()
        self.assertTrue(failures, "a failed dashboard PUT must be counted")
        self.assertNotIn(f"converged: {title}", out.getvalue())


class O2IngestTest(unittest.TestCase):
    """P6-462 (non-JSON 200 body), P6-760 (which secrets file was consulted)."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load_module("o2_ingest", O2_INGEST)

    def _main(self, argv, stdin, env=None, stderr=None):
        patches = [
            mock.patch.object(sys, "argv", argv),
            mock.patch.object(sys, "stdin", io.StringIO(stdin)),
        ]
        if stderr is not None:
            patches.append(mock.patch.object(sys, "stderr", stderr))
        with mock.patch.dict(os.environ, env or {}, clear=False):
            for p in patches:
                p.start()
                self.addCleanup(p.stop)
            return self.mod.main()

    def test_non_json_200_body_returns_4_not_a_traceback(self):
        # Documented contract: 4 = O2 refused. A proxy/HTML 200 body used to
        # raise JSONDecodeError through the caller.
        class _Resp:
            status = 200

            def read(self):
                return b"<html>proxy</html>"

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        with mock.patch.dict(os.environ, {"O2_AUTH_BASIC": "dGVzdDp0ZXN0"}):
            with mock.patch("urllib.request.urlopen", lambda *a, **k: _Resp()):
                rc = self._main(["o2_ingest.py", "host_io_latency"], '{"a":1}\n')
        self.assertEqual(rc, 4)

    def test_refusal_names_the_secrets_file_actually_consulted(self):
        custom = "/tmp/w42-custom-secrets-not-here.env"
        err = io.StringIO()
        env = {"O2_SECRETS_FILE": custom}
        env.pop("O2_AUTH_BASIC", None)
        with mock.patch.dict(os.environ, env, clear=False):
            os.environ.pop("O2_AUTH_BASIC", None)
            rc = self._main(["o2_ingest.py", "host_io_latency"], '{"a":1}\n', stderr=err)
        self.assertEqual(rc, 3)
        self.assertIn(custom, err.getvalue(), "the message must name the file it read")


class StageParseTest(unittest.TestCase):
    """P6-572 (blank epoch), P6-791 (source series collide), P6-792 (parse count)."""

    HEADER = (
        "epoch\tvertex_id\toperator\tnumRecordsIn\tnumRecordsOut\t"
        "busyMsSum\tbackpressuredMsSum\tidleMsSum"
    )

    @classmethod
    def setUpClass(cls):
        import stage_capture_parse

        cls.mod = stage_capture_parse

    def _capture(self, stage_rows):
        d = Path(tempfile.mkdtemp(prefix="w42-cap-"))
        self.addCleanup(lambda: __import__("shutil").rmtree(d, ignore_errors=True))
        (d / "stages.tsv").write_text(
            "\n".join([self.HEADER] + stage_rows) + "\n", encoding="utf-8"
        )
        return d

    def test_blank_epoch_rows_are_skipped(self):
        d = self._capture([
            "\tv1\tSource: raw-table-1 -> raw-validation\t10\t10\t\t\t",
            "3005\tv1\tSource: raw-table-1 -> raw-validation\t5100\t5100\t0\t990\t10",
            "3010\tv1\tSource: raw-table-1 -> raw-validation\t5200\t5200\t0\t1990\t10",
        ])
        rows = self.mod.parse_stages_tsv(d / "stages.tsv")
        self.assertEqual(len(rows), 2, "only the blank-epoch row may be dropped")
        self.assertTrue(
            all(isinstance(r["epoch"], int) for r in rows),
            f"a row with an unusable epoch reached the consumers: {rows}",
        )
        # The consumers that assume a number must not raise, and must still work.
        series = self.mod.operator_series(rows)
        for samples in series.values():
            rates = self.mod.compute_rates(samples)
            self.assertTrue(rates, "a real rate must survive the epoch filter")
            self.assertIsNotNone(self.mod.window_stats(rates, 3000, 3015))

    def test_latency_source_series_take_the_max_not_the_last(self):
        # Two sources emit latency for the same operator subtask; the metric name
        # carries the source id, which the parser's (task, sub, op) key drops.
        d = self._capture([])
        line = (
            'flink_taskmanager_job_task_latency_source_id_{src}_operator_id_1'
            '_operator_subtask_index_0_latency{{quantile="0.5",task_name="op_a",'
            'subtask_index="0",operator_subtask_index="0"}} {val}'
        )
        (d / "prom-1000.txt").write_text(
            line.format(src=1, val=40.0) + "\n" + line.format(src=2, val=10.0) + "\n",
            encoding="utf-8",
        )
        prom = self.mod.parse_prom_files(d)
        self.assertEqual(len(prom), 1)
        lat = prom[0]["latency"]
        self.assertEqual(
            list(lat.values())[0]["0.5"], 40.0,
            "the smaller later series overwrote the larger one instead of taking the max",
        )
        out = self.mod.latency_report(prom, [(1000, 1010)])
        self.assertIn("40", out)

    def test_stages_tsv_is_parsed_once_per_run(self):
        d = self._capture(["3000\tv1\tSource: raw-table-1 -> raw-validation\t10\t10\t0\t0\t0"])
        calls = []
        real = self.mod.parse_stages_tsv

        def counting(path):
            calls.append(str(path))
            return real(path)

        out_json = d / "out.json"
        with mock.patch.object(self.mod, "parse_stages_tsv", counting):
            with mock.patch("sys.stdout", io.StringIO()):
                rc = self.mod.main([str(d), "--json", str(out_json)])
        self.assertEqual(rc, 0)
        self.assertEqual(
            len(calls), 1, f"stages.tsv was parsed {len(calls)} times: {calls}"
        )
        self.assertTrue(json.loads(out_json.read_text())["operators"])


if __name__ == "__main__":
    unittest.main()
