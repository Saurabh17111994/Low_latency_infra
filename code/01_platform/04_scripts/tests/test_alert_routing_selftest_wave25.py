"""Wave-25 guards for alert-routing-selftest.py (P6-305, P6-306).

The script's whole value is that it explains a broken routing chain. These tests
drive its API layer and its end-to-end path against fakes, so the failures the
audit found — a proxy's HTML error page, an unreachable O2, a rejected DELETE —
are exercised without an OpenObserve instance.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import unittest
import urllib.error
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "alert-routing-selftest.py"
_spec = importlib.util.spec_from_file_location("alert_routing_selftest", SCRIPT)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

RECORD = {
    "name": mod.TEST_ALERT,
    "severity": "info",
    "class": "other",
    "received_at": "2026-09-15T12:00:00Z",
}


class _Response:
    def __init__(self, status: int, body: str):
        self.status = status
        self._body = body.encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Api:
    """A fake OpenObserve: enough of the alert lifecycle for main() to run."""

    def __init__(self, *, delete_rc: int = 204, leftover: bool = False, create_rc: int = 201,
                 payload_key: str = "alert_id"):
        self.delete_rc = delete_rc
        self.payload_key = payload_key
        # Default payload_key is the REAL O2 v0.91.5 shape (alert_id, no id);
        # the script must resolve the id through both spellings.
        self.alerts = [{payload_key: "left-1", "name": mod.TEST_ALERT}] if leftover else []
        self.create_rc = create_rc
        self.calls: list[tuple[str, str]] = []

    def __call__(self, path: str, method: str = "GET", body=None):
        self.calls.append((path, method))
        if path == "alerts" and method == "GET":
            return 200, {"list": list(self.alerts)}
        if path == "alerts" and method == "POST":
            if self.create_rc in (200, 201):
                self.alerts.append({self.payload_key: "new-1", "name": mod.TEST_ALERT})
            return self.create_rc, {}
        if path.startswith("alerts/") and method == "DELETE":
            if self.delete_rc not in (200, 202, 204):
                return self.delete_rc, {"error": "delete refused"}
            aid = path.split("/", 1)[1]
            self.alerts = [a for a in self.alerts
                           if aid not in (a.get("id"), a.get("alert_id"))]
            return self.delete_rc, {}
        raise AssertionError(f"unexpected O2 call: {method} {path}")


def _consumer_ok(path, method="GET", body=None):
    if path == "/healthz":
        return 200, '{"ok": true}'
    if path == "/noop":
        return 400, "bad request"
    if path.startswith("/alerts"):
        return 200, json.dumps({"alerts": [RECORD]})
    raise AssertionError(f"unexpected consumer call: {method} {path}")


class O2ApiRobustnessTest(unittest.TestCase):
    """P6-305: failures must be reported, not raised."""

    def setUp(self):
        os.environ["O2_PASSWORD"] = "test-password"
        self._urlopen = mod.urllib.request.urlopen
        self.err = io.StringIO()

    def tearDown(self):
        mod.urllib.request.urlopen = self._urlopen

    def _call(self):
        with contextlib.redirect_stderr(self.err):
            return mod.o2_api("alerts")

    def test_an_unreachable_api_is_a_fail_not_a_traceback(self):
        def boom(*_a, **_k):
            raise urllib.error.URLError("connection refused")

        mod.urllib.request.urlopen = boom
        with self.assertRaises(SystemExit):
            self._call()
        self.assertIn("O2 API unreachable", self.err.getvalue())
        self.assertIn("connection refused", self.err.getvalue())

    def test_a_timeout_is_a_fail_not_a_traceback(self):
        def slow(*_a, **_k):
            raise TimeoutError("timed out")

        mod.urllib.request.urlopen = slow
        with self.assertRaises(SystemExit):
            self._call()
        self.assertIn("O2 API unreachable", self.err.getvalue())

    def test_an_html_error_page_from_a_proxy_does_not_crash(self):
        def html_502(*_a, **_k):
            raise urllib.error.HTTPError("http://o2/api", 502, "Bad Gateway", {}, io.BytesIO(b"<html>502</html>"))

        mod.urllib.request.urlopen = html_502
        status, body = self._call()
        self.assertEqual(502, status)
        self.assertEqual({"raw": "<html>502</html>"}, body)

    def test_a_non_json_success_body_does_not_crash(self):
        mod.urllib.request.urlopen = lambda *_a, **_k: _Response(200, "<html>login</html>")
        status, body = self._call()
        self.assertEqual(200, status)
        self.assertIn("raw", body)


class CleanupStatusTest(unittest.TestCase):
    """P6-306: a rejected DELETE used to leave an always-firing probe running."""

    def setUp(self):
        os.environ["O2_PASSWORD"] = "test-password"
        self._o2, self._consumer = mod.o2_api, mod.consumer
        mod.consumer = _consumer_ok
        self.out, self.err = io.StringIO(), io.StringIO()

    def tearDown(self):
        mod.o2_api, mod.consumer = self._o2, self._consumer

    def _run(self, api: _Api) -> int:
        mod.o2_api = api
        with contextlib.redirect_stdout(self.out), contextlib.redirect_stderr(self.err):
            return mod.main()

    def test_a_refused_cleanup_delete_fails_the_run(self):
        with self.assertRaises(SystemExit):
            self._run(_Api(delete_rc=500))
        self.assertIn("STILL enabled", self.err.getvalue())
        self.assertNotIn("PASS:", self.out.getvalue())

    def test_a_refused_leftover_delete_fails_before_creating_another(self):
        with self.assertRaises(SystemExit):
            self._run(_Api(delete_rc=404, leftover=True))
        self.assertIn("cannot delete leftover", self.err.getvalue())
        self.assertNotIn("PASS:", self.out.getvalue())

    def test_a_clean_run_passes(self):
        api = _Api()
        self.assertEqual(0, self._run(api))
        self.assertIn("temp alert deleted: True", self.out.getvalue())
        self.assertIn("PASS:", self.out.getvalue())
        self.assertEqual([], api.alerts, "the probe alert must be gone")

    def test_cleanup_deletes_a_real_o2_alert_id_payload(self):
        """O2 v0.91.5 lists alerts as {alert_id: ...} with no id — the script
        must not DELETE alerts/None and leak the always-firing probe."""
        api = _Api(leftover=True, payload_key="alert_id")
        self.assertEqual(0, self._run(api))
        self.assertEqual([], api.alerts, "alert_id-keyed probe must be deleted")
        self.assertIn("deleted leftover", self.out.getvalue())

    def test_cleanup_still_accepts_the_legacy_id_payload(self):
        api = _Api(leftover=True, payload_key="id")
        self.assertEqual(0, self._run(api))
        self.assertEqual([], api.alerts, "id-keyed probe must be deleted")

    def test_a_202_delete_is_accepted(self):
        api = _Api(delete_rc=202)
        self.assertEqual(0, self._run(api))
        self.assertEqual([], api.alerts)


if __name__ == "__main__":
    unittest.main()
