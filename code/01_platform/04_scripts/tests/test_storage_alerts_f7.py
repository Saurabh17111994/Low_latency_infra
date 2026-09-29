#!/usr/bin/env python3
"""F7 guards — storage/disk alerts, their provisioning, and the write-lock runbook.

Why this file exists (2026-09-28): a real-feed trading day ended with the Fluss
data disk at 85.13% and every append rejected (`DISK_WRITE_LOCKED`). Verification
found three things, and these offline pins hold each fix in place:

* the 80%-usage rule existed in the ``o2-provision.py`` catalog
  (`INFRA-crit-disk-20`) but had NEVER been provisioned into the running O2 —
  0 deliveries across 78,892 alert records while the disk sat at 85.13%, so
  nothing fired. A fresh OpenObserve (fresh start, daily VM) starts with ZERO
  alerts; provisioning must be a step, not an assumption.
* no lock-level alert existed: ``storage-alerts.json`` now carries the tablet
  (`fluss_logs`) and ingestion (`platform_logs`) nets, using the wording
  recorded in the incident.
* no runbook section and no capacity review existed.

Offline only: reads the artifacts, drives the seeder against a fake O2 API.
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

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "code/01_platform" / "04_scripts"
ALERTS_DIR = ROOT / "code/01_platform/01_docker/openobserve/alerts"
STORAGE = ALERTS_DIR / "storage-alerts.json"
RUNBOOK = ROOT / "docs/06_operations/07-lake-archive-ops.md"
PLAN = ROOT / "docs/plans/2026-09-22-fluss-1.0-native-adoption.md"
RECIPE = ROOT / "docs/05_deployment/CLOUDPE_DAILY_VM.md"

_spec = importlib.util.spec_from_file_location("seed_alerts_f7", SCRIPTS / "seed_alerts.py")
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


class StorageAlertCorpusTests(unittest.TestCase):
    def rules(self) -> dict:
        return {a["name"]: a for a in json.loads(STORAGE.read_text())}

    def test_lock_rule_reads_the_tablet_stream_with_the_recorded_wording(self):
        rule = self.rules()["storage-crit-fluss-disk-write-locked"]
        self.assertEqual(rule["stream_type"], "logs")
        self.assertEqual(rule["stream_name"], "fluss_logs")
        sql = rule["query_condition"]["sql"]
        self.assertIn("data disk usage reached", sql)
        self.assertIn("DISK_WRITE_LOCKED", sql)
        self.assertTrue(rule["enabled"])
        self.assertEqual(rule["destinations"], ["dev-webhook"])

    def test_ingestion_rule_is_the_second_net_on_platform_logs(self):
        rule = self.rules()["storage-crit-ingestion-append-blocked"]
        self.assertEqual(rule["stream_type"], "logs")
        self.assertEqual(rule["stream_name"], "platform_logs")
        sql = rule["query_condition"]["sql"]
        self.assertIn("DISK_WRITE_LOCKED", sql)
        self.assertIn("append UNCERTAIN", sql)
        # a single timeout is normal noise; the incident produced a stream of them
        self.assertGreaterEqual(rule["trigger_condition"]["threshold"], 2)

    def test_the_seeder_discovers_every_live_corpus(self):
        names = {p.name for p in mod.default_alert_files()}
        self.assertIn("storage-alerts.json", names)
        self.assertNotIn(
            "position-state-alerts.json", names,
            "H3-3: the position-state corpus is retired — its producers were deleted in 0f3e5952",
        )

    def test_duplicate_rule_names_fail_closed(self):
        rule = {"name": "dup-rule", "stream_type": "metrics", "stream_name": "up"}
        with tempfile.TemporaryDirectory() as tmp:
            one = Path(tmp) / "one.json"
            two = Path(tmp) / "two.json"
            one.write_text(json.dumps([rule]))
            two.write_text(json.dumps([dict(rule)]))
            err = io.StringIO()
            with mock.patch.dict(os.environ, {"O2_PASSWORD": "test-password"}), \
                    mock.patch.object(mod, "default_alert_files", lambda: [one, two]), \
                    mock.patch.object(sys, "argv", ["seed"]), \
                    contextlib.redirect_stderr(err):
                rc = mod.main()
        self.assertEqual(rc, 3)
        self.assertIn("duplicate alert name", err.getvalue())

    def test_a_missing_stream_does_not_block_the_other_corpus(self):
        bad = {"name": "bad-rule", "stream_type": "metrics", "stream_name": "missing-stream"}
        good = {"name": "good-rule", "stream_type": "metrics", "stream_name": "up"}
        with tempfile.TemporaryDirectory() as tmp:
            one = Path(tmp) / "one.json"
            two = Path(tmp) / "two.json"
            one.write_text(json.dumps([bad]))
            two.write_text(json.dumps([good]))
            calls: list[tuple] = []

            def fake_api(_base, _org, _user, _pwd, path, method="GET", body=None):
                calls.append((method, path, (body or {}).get("name")))
                if path == "v2/alerts" and method == "GET":
                    return 200, json.dumps({"list": []})
                if path == "v2/alerts" and method == "POST":
                    if (body or {}).get("name") == "bad-rule":
                        return 404, '{"message":"Stream missing-stream not found"}'
                    return 201, "{}"
                raise AssertionError(f"unexpected call: {method} {path}")

            err = io.StringIO()
            with mock.patch.dict(os.environ, {"O2_PASSWORD": "test-password"}), \
                    mock.patch.object(mod, "default_alert_files", lambda: [one, two]), \
                    mock.patch.object(mod, "_api", fake_api), \
                    mock.patch.object(sys, "argv", ["seed"]), \
                    contextlib.redirect_stderr(err):
                rc = mod.main()
        self.assertEqual(rc, 1, "a failed rule must keep the exit non-zero")
        self.assertIn("bad-rule", err.getvalue())
        self.assertIn(("POST", "v2/alerts", "good-rule"), calls,
                      "a missing metric stream must not block the other corpus")


class WriteLockRunbookTests(unittest.TestCase):
    def test_runbook_documents_the_thresholds_alerts_and_recovery(self):
        text = RUNBOOK.read_text()
        self.assertIn("Fluss write-locked", text)
        self.assertIn("0.85", text)
        self.assertIn("0.80", text)
        self.assertIn("docker builder prune", text)
        self.assertIn("storage-crit-fluss-disk-write-locked", text)
        self.assertIn("INFRA-crit-disk-20", text)

    def test_thresholds_still_anchor_to_the_read_1_0_0_defaults(self):
        plan = PLAN.read_text()
        self.assertRegex(plan, r"write-limit-ratio[^\n]*0\.85")
        self.assertRegex(plan, r"write-recover-ratio[^\n]*0\.80")

    def test_provisioning_reports_alert_failures_instead_of_silence(self):
        src = (SCRIPTS / "o2-provision.py").read_text()
        self.assertIn("alerts: created=", src)
        self.assertIn("re-run when live", src)

    def test_daily_vm_recipe_provisions_observability(self):
        text = RECIPE.read_text()
        self.assertIn("o2-provision.py", text)
        self.assertIn("seed_alerts.py", text)


if __name__ == "__main__":
    unittest.main()
