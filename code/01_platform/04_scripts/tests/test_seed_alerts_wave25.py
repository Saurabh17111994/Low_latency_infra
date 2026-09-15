"""Wave-25 guards for seed_alerts.py (P6-538, P6-778, P6-779, P6-780).

The script's documented contract is "exit 3 invalid file" and "dry-run prints the
plan a real run would execute". Both were untrue; these tests hold them to it by
driving main() against a temp alert file and a faked O2 API.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "seed_alerts.py"
_spec = importlib.util.spec_from_file_location("seed_alerts_wave25", SCRIPT)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

ALERTS = [
    {"name": "a-one", "stream_type": "metrics", "stream_name": "up"},
    {"name": "a-two", "stream_type": "logs", "stream_name": "trading_alerts"},
]


class _Api:
    """Fake O2 API: list/create/put, recording every call."""

    def __init__(self, existing=None, *, list_rc=200, post_rc=201, put_rc=200):
        self.existing = existing if existing is not None else {"list": []}
        self.list_rc, self.post_rc, self.put_rc = list_rc, post_rc, put_rc
        self.calls: list[tuple[str, str]] = []

    def __call__(self, _base, _org, _user, _pwd, path, method="GET", body=None):
        self.calls.append((path, method))
        if path == "v2/alerts" and method == "GET":
            return self.list_rc, json.dumps(self.existing)
        if path == "v2/alerts" and method == "POST":
            return self.post_rc, "{}"
        if path.startswith("v2/alerts/"):
            return self.put_rc, "{}"
        raise AssertionError(f"unexpected call: {method} {path}")


class SeedAlertsWave25Test(unittest.TestCase):
    def setUp(self):
        os.environ["O2_PASSWORD"] = "test-password"
        self._api, self._file = mod._api, mod.ALERT_FILE
        self.dir = Path(tempfile.mkdtemp(prefix="w25-alerts-"))
        self.file = self.dir / "alerts.json"
        self.out, self.err = io.StringIO(), io.StringIO()

    def tearDown(self):
        mod._api, mod.ALERT_FILE = self._api, self._file

    def _run(self, alerts=None, argv=("seed",), api: _Api | None = None) -> int:
        if alerts is not None:
            self.file.write_text(alerts if isinstance(alerts, str) else json.dumps(alerts))
        mod.ALERT_FILE = self.file
        mod._api = api or _Api()
        with mock.patch.object(sys, "argv", list(argv)), \
                contextlib.redirect_stdout(self.out), contextlib.redirect_stderr(self.err):
            return mod.main()

    # --- P6-538 / P6-779: the documented exit 3 ---------------------------
    def test_malformed_json_exits_three(self):
        self.assertEqual(3, self._run("{not json"))
        self.assertIn("cannot load", self.err.getvalue())

    def test_a_non_dict_element_exits_three(self):
        self.assertEqual(3, self._run(["just-a-string"]))
        self.assertIn("must be objects", self.err.getvalue())

    def test_an_unreadable_file_exits_three(self):
        mod.ALERT_FILE = self.dir  # a directory: read_text raises OSError
        mod._api = _Api()
        with mock.patch.object(sys, "argv", ["seed"]), contextlib.redirect_stderr(self.err):
            self.assertEqual(3, mod.main())
        self.assertIn("cannot load", self.err.getvalue())

    def test_a_missing_stream_type_exits_three_instead_of_keyerror(self):
        alerts = [{"name": "no-type", "stream_name": "up"}]
        self.assertEqual(3, self._run(alerts, argv=("seed", "--dry-run")))
        self.assertIn("missing stream_type", self.err.getvalue())
        self.assertNotIn("Traceback", self.err.getvalue())

    # --- P6-778: the dry-run plan must match a real run --------------------
    def test_dry_run_lists_and_keeps_an_existing_alert(self):
        api = _Api(existing={"list": [{"name": "a-one", "id": "1"}]})
        self.assertEqual(0, self._run(ALERTS, argv=("seed", "--dry-run"), api=api))
        self.assertIn(("v2/alerts", "GET"), api.calls)
        self.assertIn("[keep", self.out.getvalue())
        self.assertIn("[create]", self.out.getvalue())  # a-two, which does not exist
        self.assertNotIn(("v2/alerts", "POST"), api.calls, "dry-run must not write")

    def test_dry_run_reports_update_under_force(self):
        api = _Api(existing={"list": [{"name": "a-one", "id": "1"}]})
        self.assertEqual(0, self._run(ALERTS, argv=("seed", "--dry-run", "--force"), api=api))
        self.assertIn("[update", self.out.getvalue())

    def test_a_real_run_still_creates_a_missing_alert(self):
        api = _Api()
        self.assertEqual(0, self._run(ALERTS, api=api))
        self.assertEqual(2, len([c for c in api.calls if c == ("v2/alerts", "POST")]))
        self.assertIn("created=2", self.out.getvalue())

    def test_a_list_failure_does_not_stop_the_plan(self):
        api = _Api(list_rc=500)
        self.assertEqual(0, self._run(ALERTS, argv=("seed", "--dry-run"), api=api))
        self.assertIn("list alerts failed (500)", self.err.getvalue())
        self.assertIn("[create]", self.out.getvalue())

    # --- P6-780: a force update that cannot happen must be visible ---------
    def test_force_without_an_id_warns_and_counts_untouched(self):
        api = _Api(existing={"list": [{"name": "a-one"}]})  # no id, no alert_id
        self.assertEqual(0, self._run(ALERTS, argv=("seed", "--force"), api=api))
        self.assertIn("no id for existing alert 'a-one'", self.err.getvalue())
        self.assertIn("untouched=1", self.out.getvalue())
        self.assertNotIn(("v2/alerts/", "PUT"), api.calls)


if __name__ == "__main__":
    unittest.main()
