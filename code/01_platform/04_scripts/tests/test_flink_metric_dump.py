#!/usr/bin/env python3
"""Guard the C2 live-metric path against stale Flink job summaries."""

import json
import os
import shlex
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit


class MetricFixture(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/jobs/sample":
            body = {
                "state": "RUNNING",
                "vertices": [
                    {
                        "id": "v1",
                        "name": "raw-table-1 source",
                        "metrics": {"read-records": 0, "write-records": 0},
                    },
                    {
                        "id": "v3",
                        "name": "subtask-zero-fallback",
                        "metrics": {"read-records": 11, "write-records": 13},
                    },
                    {
                        "id": "v2",
                        "name": "terminal-fallback",
                        "metrics": {"read-records": 7, "write-records": 9},
                    },
                ],
            }
            status = 200
        elif path == "/jobs/sample/vertices/v1/subtasks/metrics":
            # Live-verified Flink 2.2.1 shape: the subtasks endpoint answers
            # an aggregate per metric; "sum" is the cross-subtask total.  The
            # dump must take the sum (CHG-120 2026-09-01 — subtask-0-only
            # reads reported 1/8 of progress at parallelism 8).
            body = [
                {
                    "id": "numRecordsIn",
                    "min": 90.0,
                    "max": 110.0,
                    "avg": 100.0,
                    "sum": 800.0,
                    "skew": 5.0,
                },
                {
                    "id": "numRecordsOut",
                    "min": 70.0,
                    "max": 90.0,
                    "avg": 80.0,
                    "sum": 640.0,
                    "skew": 5.0,
                },
            ]
            status = 200
        elif path == "/jobs/sample/vertices/v3/metrics":
            body = [
                {"id": "0.numRecordsIn", "value": "11"},
                {"id": "0.numRecordsOut", "value": "13"},
            ]
            status = 200
        elif path == "/jobs/sample/vertices/v1/metrics":
            body = [
                {"id": "0.numRecordsIn", "value": "123"},
                {"id": "0.numRecordsOut", "value": "120"},
            ]
            status = 200
        else:
            body = {"error": "not found"}
            status = 404

        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: object) -> None:
        pass


class FlinkMetricDumpTest(unittest.TestCase):
    def test_running_job_sums_subtasks_and_keeps_fallbacks(self) -> None:
        server = HTTPServer(("127.0.0.1", 0), MetricFixture)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            root = Path(__file__).resolve().parents[4]
            lib = root / "code/01_platform/04_scripts/pipeline-lib.sh"
            with tempfile.TemporaryDirectory() as temp_dir:
                env = os.environ.copy()
                env["ROOT"] = str(root)
                env["OUT"] = temp_dir
                env["RATE_HZ"] = "10"
                env["FLINK_REST_URL"] = f"http://127.0.0.1:{server.server_port}"
                command = (
                    f"source {shlex.quote(str(lib))}; "
                    "flink_metric_dump sample"
                )
                result = subprocess.run(
                    ["bash", "-c", command],
                    check=True,
                    capture_output=True,
                    text=True,
                    env=env,
                )
            # v1: the subtasks endpoint wins — the SUM over 8 subtasks, not
            # the subtask-0-only aggregate (which would read 123 | 120).
            self.assertIn("raw-table-1 source | 800 | 640", result.stdout)
            # v3: no subtasks endpoint (404) — falls back to the subtask-0
            # vertex aggregate (documented partial view).
            self.assertIn("subtask-zero-fallback | 11 | 13", result.stdout)
            # v2: no live endpoints at all — the terminal job summary.
            self.assertIn("terminal-fallback | 7 | 9", result.stdout)
        finally:
            server.shutdown()
            server.server_close()

    def test_downstream_progress_proves_feed_when_raw_snapshot_is_zero(self) -> None:
        root = Path(__file__).resolve().parents[4]
        lib = root / "code/01_platform/04_scripts/pipeline-lib.sh"
        # P6-142: the lib refuses to be sourced without ROOT/RATE_HZ.
        env = os.environ.copy()
        env["ROOT"] = str(root)
        env["RATE_HZ"] = "10"
        dump = (
            "STATE RUNNING\n"
            "Source: raw-table-1 -> raw-validation | 0 | 0\n"
            "fingerprint-dedup | 0 | 0\n"
            "candle-15s -> (candle-late-drop-counter, candle-invalid-quarantine) | 26071 | 0\n"
        )
        command = (
            f"source {shlex.quote(str(lib))}; "
            f"pipeline_metric_input_progress {shlex.quote(dump)}"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.stdout.strip(), "26071")

    def test_the_multitf_path_proves_the_feed_without_naming_an_operator(self) -> None:
        """CHG-193 — the 0|0 race on the raw AND dedup vertices (run 10).

        The fallback used to match a hand-written operator set
        (fingerprint-dedup | candle-15s | forming-bar-*). `0f3e5952` retired the
        15s candle path and the forming-bar stack and added nothing to that set,
        so the only surviving name was fingerprint-dedup. When both the raw and
        dedup vertices answered the documented 0|0 snapshot, the guard had
        nothing left to match and declared a healthy pipeline dead — while
        multi-tf-aggregator was holding 458 132 records and the candle sinks
        534 528. The dump below is that run's `smoke/metrics-warmup.txt`,
        verbatim; the rule is now name-free, so it cannot rot again.
        """
        root = Path(__file__).resolve().parents[4]
        lib = root / "code/01_platform/04_scripts/pipeline-lib.sh"
        env = os.environ.copy()
        env["ROOT"] = str(root)
        env["RATE_HZ"] = "10"
        dump = (
            "STATE RUNNING\n"
            "Source: raw-table-1 -> raw-validation | 0 | 0\n"
            "fingerprint-dedup -> ingest-latency-monitor | 0 | 0\n"
            "multi-tf-aggregator | 458132 | 539648\n"
            "candle-closed-first-write-wins | 5120 | 5120\n"
            "candle-closed-sink: Writer | 5120 | 5120\n"
            "candle-live-sink: Writer | 534528 | 534528\n"
        )
        command = (
            f"source {shlex.quote(str(lib))}; "
            f"pipeline_metric_input_progress {shlex.quote(dump)}"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(
            result.stdout.strip(), "534528",
            "a healthy pipeline was reported as dead because the fallback only "
            "knew retired operator names")

    def test_the_raw_counters_still_win_when_they_are_readable(self) -> None:
        """The priority order is unchanged — the raw read is the reported value."""
        root = Path(__file__).resolve().parents[4]
        lib = root / "code/01_platform/04_scripts/pipeline-lib.sh"
        env = os.environ.copy()
        env["ROOT"] = str(root)
        env["RATE_HZ"] = "10"
        dump = (
            "STATE RUNNING\n"
            "Source: raw-table-1 -> raw-validation | 458640 | 458640\n"
            "multi-tf-aggregator | 458132 | 539648\n"
        )
        command = (
            f"source {shlex.quote(str(lib))}; "
            f"pipeline_metric_input_progress {shlex.quote(dump)}"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.stdout.strip(), "458640")

    def test_missing_raw_and_downstream_progress_fails_closed(self) -> None:
        root = Path(__file__).resolve().parents[4]
        lib = root / "code/01_platform/04_scripts/pipeline-lib.sh"
        # P6-142: the lib refuses to be sourced without ROOT/RATE_HZ.
        env = os.environ.copy()
        env["ROOT"] = str(root)
        env["RATE_HZ"] = "10"
        dump = (
            "STATE RUNNING\n"
            "Source: raw-table-1 -> raw-validation | 0 | 0\n"
            "candle-15s -> (candle-late-drop-counter, candle-invalid-quarantine) | 0 | 0\n"
        )
        command = (
            f"source {shlex.quote(str(lib))}; "
            f"pipeline_metric_input_progress {shlex.quote(dump)}"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.stdout.strip(), "-1")


if __name__ == "__main__":
    unittest.main()
