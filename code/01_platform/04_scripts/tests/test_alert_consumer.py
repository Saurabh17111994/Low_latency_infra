"""Unit tests for the G6 alert consumer (alert-consumer.py).

Offline: classification, record building, JSONL persistence round-trip,
filtering, and a live HTTP round-trip on an ephemeral port (the malformed-
body branch = the routing selftest's negative proof, runnable without O2).
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

CONSUMER = (
    Path(__file__).resolve().parents[2]
    / "01_platform"  # placeholder, fixed below
)
# tests/ -> 04_scripts -> 04_scripts parent layout:
#   code/01_platform/04_scripts/tests/test_alert_consumer.py
#   code/01_platform/01_docker/alert-consumer.py
CONSUMER = Path(__file__).resolve().parents[3] / "01_platform" / "01_docker" / "alert-consumer.py"

spec = importlib.util.spec_from_file_location("alert_consumer", CONSUMER)
ac = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ac)


class TestClassify(unittest.TestCase):
    def test_all_conventions(self):
        cases = [
            ("ING-crit-bridge-disconnected", ("crit", "ing")),
            ("ING-warn-capacity-80", ("warn", "ing")),
            ("SIGNAL-crit-checkpoint-failed", ("crit", "signal")),
            ("SIGNAL-error-source-stalled", ("error", "signal")),
            ("SIGNAL-warn-source-volume-drop", ("warn", "signal")),
            ("INFRA-crit-host-cpu-90", ("crit", "infra")),
            ("INFRA-warn-net-80", ("warn", "infra")),
            ("pos-state-high-active-count", ("warn", "pos-state")),
            ("selftest-g6-routing-probe", ("info", "other")),
        ]
        for name, expected in cases:
            self.assertEqual(ac.classify(name), expected, name)

    def test_empty_name(self):
        self.assertEqual(ac.classify(""), ("info", "other"))


class TestRecordAndStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = os.path.join(self.tmp.name, "alerts.jsonl")
        self._orig = ac.STORE_PATH
        ac.STORE_PATH = self.store

    def tearDown(self):
        ac.STORE_PATH = self._orig
        self.tmp.cleanup()

    def test_build_record_o2_shape(self):
        rec = ac.build_record('{"alert": {"name": "SIGNAL-crit-checkpoint-failed"}}')
        self.assertEqual(rec["name"], "SIGNAL-crit-checkpoint-failed")
        self.assertEqual(rec["severity"], "crit")
        self.assertEqual(rec["class"], "signal")
        self.assertTrue(rec["received_at"])

    def test_build_record_variant_shapes(self):
        # O2 template variants observed in the wild / future versions.
        rec = ac.build_record('{"AlertName": "ING-warn-heartbeat-failures"}')
        self.assertEqual(rec["name"], "ING-warn-heartbeat-failures")
        self.assertEqual(rec["severity"], "warn")
        rec = ac.build_record("{}")
        self.assertEqual(rec["name"], "")
        self.assertEqual(rec["severity"], "info")

    def test_persistence_roundtrip_and_filters(self):
        for n in ["ING-crit-a", "SIGNAL-warn-b", "INFRA-error-c", "pos-state-x"]:
            ac.append_record(ac.build_record(json.dumps({"alert": {"name": n}})))
        recs = ac.read_records(limit=10)
        self.assertEqual([r["name"] for r in recs],
                         ["ING-crit-a", "SIGNAL-warn-b", "INFRA-error-c", "pos-state-x"])
        # newest-last ordering with limit
        self.assertEqual(ac.read_records(limit=2)[-1]["name"], "pos-state-x")
        # filters
        self.assertEqual(len(ac.read_records(10, severity="crit")), 1)
        self.assertEqual(len(ac.read_records(10, cls="signal")), 1)
        self.assertEqual(ac.read_records(10, severity="crit")[0]["name"], "ING-crit-a")

    def test_missing_store_is_empty_not_error(self):
        ac.STORE_PATH = os.path.join(self.tmp.name, "nope", "alerts.jsonl")
        self.assertEqual(ac.read_records(10), [])

    def test_corrupt_line_skipped(self):
        with open(self.store, "w") as f:
            f.write(json.dumps({"received_at": "t", "name": "A-crit-a",
                                "severity": "crit", "class": "other", "body": {}}) + "\n")
            f.write("CORRUPT{not json\n")
        recs = ac.read_records(10)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["name"], "A-crit-a")


class TestHttpSurface(unittest.TestCase):
    """Ephemeral-port round-trip: POST delivery + GET endpoints + 400 path."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        ac.STORE_PATH = os.path.join(cls.tmp.name, "alerts.jsonl")
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), ac.Handler)
        cls.port = cls.srv.server_address[1]
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.tmp.cleanup()

    def _req(self, path, method="GET", data=None):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", method=method,
            data=data.encode() if data else None,
            headers={"Content-Type": "application/json"} if data else {})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def test_delivery_then_query(self):
        code, body = self._req("/noop", "POST",
                               '{"alert": {"name": "SIGNAL-crit-checkpoint-failed"}}')
        self.assertEqual(code, 200)
        self.assertIn('"ok": true', body)

        code, body = self._req("/alerts?limit=10")
        self.assertEqual(code, 200)
        recs = json.loads(body)["alerts"]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["severity"], "crit")

        code, body = self._req("/stats")
        self.assertEqual(code, 200)
        stats = json.loads(body)
        self.assertEqual(stats["total"], 1)
        self.assertEqual(stats["by_severity"], {"crit": 1})
        self.assertEqual(stats["last_delivery_at"], recs[0]["received_at"])

    def test_malformed_body_is_400_and_survives(self):
        code, _ = self._req("/noop", "POST", "not-json{")
        self.assertEqual(code, 400)
        # consumer still alive
        code, body = self._req("/healthz")
        self.assertEqual((code, '"ok": true' in body), (200, True))

    def test_unknown_path_404(self):
        code, _ = self._req("/whatever")
        self.assertEqual(code, 404)


if __name__ == "__main__":
    unittest.main()
