"""Wave-25 guards for seed_dashboards.py (P6-539, P6-781, P6-782, P6-783).

The script documents exit 3 for an invalid manifest or corpus and prints a plan
that is supposed to match a real run. These tests hold both to account, driving
main() over the real corpus with a faked API for the plan tests and over temp
fixtures for the failure paths.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import typing
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "seed_dashboards.py"
_spec = importlib.util.spec_from_file_location("seed_dashboards_wave25", SCRIPT)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

REAL_MANIFEST = mod.MANIFEST
REAL_DASH_DIR = mod.DASH_DIR
FIRST_TITLE = json.loads(REAL_MANIFEST.read_text())["dashboards"][0]["title"]


class _Api:
    """Fake O2 dashboard API: list + create + update, recording every call."""

    def __init__(self, dashboards=()):
        self.dashboards = list(dashboards)
        self.calls: list[tuple[str, str]] = []

    def __call__(self, _base, _org, _user, _pwd, path, method="GET", body=None):
        self.calls.append((path, method))
        if path == "/dashboards" and method == "GET":
            return 200, json.dumps({"dashboards": self.dashboards})
        if path == "/dashboards" and method == "POST":
            return 201, "{}"
        if path.startswith("/dashboards/") and method == "PUT":
            return 200, "{}"
        raise AssertionError(f"unexpected call: {method} {path}")


class SeedDashboardsWave25Test(unittest.TestCase):
    def setUp(self):
        os.environ["O2_PASSWORD"] = "test-password"
        self._api, self._manifest, self._dir = mod._api, mod.MANIFEST, mod.DASH_DIR
        self.tmp = Path(tempfile.mkdtemp(prefix="w25-dash-"))
        self.out, self.err = io.StringIO(), io.StringIO()

    def tearDown(self):
        mod._api, mod.MANIFEST, mod.DASH_DIR = self._api, self._manifest, self._dir

    def _corpus(self, manifest, files: dict[str, str]) -> None:
        """Write a temp manifest + dashboard files and point the module at them."""
        (self.tmp / "dashboards").mkdir(exist_ok=True)
        for name, text in files.items():
            (self.tmp / "dashboards" / name).write_text(text)
        (self.tmp / "manifest.json").write_text(
            manifest if isinstance(manifest, str) else json.dumps(manifest))
        mod.MANIFEST = self.tmp / "manifest.json"
        mod.DASH_DIR = self.tmp / "dashboards"

    def _run(self, argv=("seed",), api: _Api | None = None) -> int:
        mod._api = api or _Api()
        with mock.patch.object(sys, "argv", list(argv)), \
                contextlib.redirect_stdout(self.out), contextlib.redirect_stderr(self.err):
            return mod.main()

    def _exit3(self, argv=("seed",)) -> SystemExit:
        with self.assertRaises(SystemExit) as cm:
            self._run(argv)
        return cm.exception

    # --- P6-782: exit 3, not a traceback and not exit 1 -------------------
    def test_malformed_manifest_json_exits_three(self):
        self._corpus("{not json", {})
        self.assertEqual(3, self._exit3().code)
        self.assertIn("cannot read manifest", self.err.getvalue())

    def test_a_manifest_without_a_dashboards_key_exits_three(self):
        self._corpus({"schema_version": 1}, {})
        self.assertEqual(3, self._exit3().code)
        self.assertIn("no 'dashboards' list", self.err.getvalue())

    def test_a_record_without_a_file_exits_three(self):
        self._corpus({"dashboards": [{"title": "t"}]}, {})
        self.assertEqual(3, self._exit3().code)
        self.assertIn("need 'file' and 'title'", self.err.getvalue())

    def test_a_missing_dashboard_file_exits_three_not_one(self):
        self._corpus({"dashboards": [{"file": "gone.json", "title": "t"}]}, {})
        exc = self._exit3()
        self.assertEqual(3, exc.code, "a string argument to SystemExit exits 1, not 3")
        self.assertIn("references missing file", self.err.getvalue())

    def test_malformed_dashboard_json_exits_three(self):
        self._corpus({"dashboards": [{"file": "d.json", "title": "t"}]}, {"d.json": "{oops"})
        self.assertEqual(3, self._exit3().code)
        self.assertIn("cannot read dashboard", self.err.getvalue())

    # --- P6-781: the annotation must describe what is returned -------------
    def test_load_is_annotated_with_its_real_return_type(self):
        self.assertEqual(tuple[dict, list[tuple[dict, dict]]],
                         typing.get_type_hints(mod._load)["return"])

    # --- P6-539: the dry-run plan comes from the same list a run uses ------
    def test_dry_run_lists_and_keeps_an_existing_dashboard(self):
        api = _Api([{"title": FIRST_TITLE, "dashboard_id": "d1"}])
        self.assertEqual(0, self._run(("seed", "--dry-run"), api))
        self.assertIn(("/dashboards", "GET"), api.calls)
        self.assertIn("[keep", self.out.getvalue())
        self.assertIn("[create]", self.out.getvalue())
        self.assertLess(self.out.getvalue().index(FIRST_TITLE),
                        self.out.getvalue().index("RESULT:"))
        self.assertIn("created=7", self.out.getvalue(), "8 dashboards, 1 already exists")

    def test_dry_run_writes_nothing(self):
        api = _Api()
        self.assertEqual(0, self._run(("seed", "--dry-run"), api))
        self.assertEqual([("/dashboards", "GET")], api.calls)

    def test_a_real_run_still_creates_a_missing_dashboard(self):
        api = _Api()
        self.assertEqual(0, self._run(("seed",), api))
        self.assertEqual(8, len([c for c in api.calls if c == ("/dashboards", "POST")]))

    # --- P6-783: plaintext credentials off-loopback must be visible --------
    def test_a_remote_http_url_warns_about_the_password(self):
        with contextlib.redirect_stderr(self.err):
            mod._warn_insecure_url("http://o2.internal:5080")
        self.assertIn("not https", self.err.getvalue())
        self.assertIn("o2.internal", self.err.getvalue())

    def test_loopback_and_https_uris_do_not_warn(self):
        for base in ("http://localhost:5080", "http://127.0.0.1:5080", "https://o2.example:5080"):
            with contextlib.redirect_stderr(self.err):
                mod._warn_insecure_url(base)
        self.assertEqual("", self.err.getvalue())


if __name__ == "__main__":
    unittest.main()
