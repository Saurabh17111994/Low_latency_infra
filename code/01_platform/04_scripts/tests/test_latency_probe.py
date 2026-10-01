#!/usr/bin/env python3
"""latency_probe tests (F5, 2026-09-28) — pure functions + a fake-Flink end-to-end.

No network: every REST call goes through ``fetch_json``, which the tests replace
with a canned responder. Auto-discovered by gate step 3 (pytest from the repo root).
"""

import importlib.util
import io
import pathlib
import sys
import unittest
from contextlib import redirect_stdout
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "code" / "01_platform" / "04_scripts" / "latency_probe.py"
SPEC = importlib.util.spec_from_file_location("latency_probe", SCRIPT)
probe = importlib.util.module_from_spec(SPEC)
sys.modules["latency_probe"] = probe
SPEC.loader.exec_module(probe)

MONITOR_VERTEX = {"id": "vm", "name": "fingerprint-dedup -> ingest-latency-monitor"}
HOST_VERTEX = {"id": "vs", "name": "strategy-host -> (canonical-signal-filter)"}


def fake_fetch(job_running=True, host_values=True):
    """Canned Flink REST: two metrics x two subtasks, deterministic values."""
    def fetch(url, timeout=8):
        if url.endswith("/jobs/overview"):
            jobs = ([{"jid": "j1", "name": probe.DEFAULT_JOB_NAME,
                      "state": "RUNNING"}] if job_running else [])
            return {"jobs": jobs}
        if url.endswith("/jobs/j1"):
            return {"vertices": [MONITOR_VERTEX, HOST_VERTEX]}
        if "/vertices/" in url and "metrics?get=" in url:
            wanted = url.split("get=", 1)[1].split(",")
            out = []
            for metric_id in wanted:
                quantile = metric_id.rsplit("_", 1)[-1]
                if not host_values:
                    value = None
                elif quantile == "median":
                    value = "24"
                elif quantile == "p95":
                    value = "44"
                elif quantile == "p99":
                    value = "68"
                else:
                    value = None
                out.append({"id": metric_id,
                            "value": None if value is None else value})
            return out
        raise AssertionError(f"unexpected url {url}")
    return fetch


class RunningJobTests(unittest.TestCase):
    def test_single_running_job_is_selected(self):
        with mock.patch.object(probe, "fetch_json", fake_fetch()):
            self.assertEqual(
                "j1", probe.running_job("http://jm", probe.DEFAULT_JOB_NAME))

    def test_zero_or_two_running_jobs_is_an_error(self):
        with mock.patch.object(probe, "fetch_json", fake_fetch(job_running=False)):
            with self.assertRaises(probe.ProbeError):
                probe.running_job("http://jm", probe.DEFAULT_JOB_NAME)
        two = lambda url, timeout=8: {"jobs": [
            {"jid": "a", "name": probe.DEFAULT_JOB_NAME, "state": "RUNNING"},
            {"jid": "b", "name": probe.DEFAULT_JOB_NAME, "state": "RUNNING"}]}
        with mock.patch.object(probe, "fetch_json", two):
            with self.assertRaises(probe.ProbeError):
                probe.running_job("http://jm", probe.DEFAULT_JOB_NAME)


class VertexTests(unittest.TestCase):
    def test_vertex_selectors_match_the_three_kpis(self):
        job = {"vertices": [MONITOR_VERTEX, HOST_VERTEX]}
        for _, _, selector in probe.METRICS:
            self.assertIn(probe.vertex_id(job, selector), ("vm", "vs"))

    def test_missing_vertex_is_an_error(self):
        with self.assertRaises(probe.ProbeError):
            probe.vertex_id({"vertices": []}, lambda name: True)


class RowsAndSummaryTests(unittest.TestCase):
    def test_collect_rows_parses_the_quantile_suffix_and_skips_nulls(self):
        with mock.patch.object(probe, "fetch_json", fake_fetch()):
            rows = probe.collect_rows("http://jm", "j1", "vm",
                                      "ingest-latency-monitor",
                                      "ingest_to_monitor", 2)
        self.assertEqual({"median", "p95", "p99"}, set(rows[0]))
        self.assertEqual(24.0, rows[0]["median"])
        self.assertEqual(2, len(rows))

    def test_spread_is_median_and_max_across_subtasks(self):
        rows = {0: {"median": 10.0}, 1: {"median": 30.0}}
        self.assertEqual((20.0, 30.0), probe.spread(rows, "median"))
        self.assertIsNone(probe.spread(rows, "p99"))

    def test_empty_window_reports_na(self):
        self.assertIn("median_med=NA", probe.metric_lines("x", {}))


class MainFlowTests(unittest.TestCase):
    def test_main_prints_all_metrics_and_writes_the_tsv(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / "latency.tsv"
            with mock.patch.object(probe, "fetch_json", fake_fetch()):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = probe.main(["--label", "test", "--subtasks", "2",
                                     "--out", str(out)])
            text = buf.getvalue()
            self.assertEqual(0, rc)
            self.assertIn("latency ingest_to_monitor subtasks=2", text)
            self.assertIn("latency tick_to_strategy subtasks=2", text)
            self.assertIn("latency ingest_to_strategy subtasks=2", text)
            tsv = out.read_text()
            self.assertIn("metric=ingest_to_monitor", tsv)
            self.assertIn("metric=tick_to_strategy", tsv)
            self.assertIn("metric=ingest_to_strategy", tsv)
            self.assertIn("# label=test job=j1", tsv)

    def test_main_exit_3_on_an_empty_window(self):
        with mock.patch.object(probe, "fetch_json", fake_fetch(host_values=False)):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = probe.main([])
        self.assertEqual(3, rc)
        self.assertIn("empty sliding window", buf.getvalue())

    def test_main_exit_2_without_a_running_job(self):
        with mock.patch.object(probe, "fetch_json", fake_fetch(job_running=False)):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = probe.main([])
        self.assertEqual(2, rc)


if __name__ == "__main__":
    unittest.main()
