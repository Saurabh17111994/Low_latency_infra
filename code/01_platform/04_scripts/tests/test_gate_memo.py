#!/usr/bin/env python3
"""gate_memo.py — the record that stops a replay from reading as new evidence.

Why: a certificate is evidence about one frozen commit on one stack. Two green
runs on the same pair are not two pieces of evidence. The memo also has to be
harmless: a missing or corrupt memo must never break a gate run, so lookups fail
quietly and unreadable lines are skipped.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
MODULE = ROOT / "code" / "01_platform" / "04_scripts" / "gate_memo.py"
sys.path.insert(0, str(MODULE.parent))
import gate_memo  # noqa: E402


class GateMemoTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gate-memo-test-")
        self.path = gate_memo.memo_path(self.dir)
        self.fp = "96187501dc4d8bae:4778c98717bd50b3"

    def _cli(self, *args):
        return subprocess.run([sys.executable, str(MODULE), "--root", self.dir, *args],
                              capture_output=True, text=True)

    def test_memo_lives_under_logs(self):
        self.assertTrue(self.path.endswith(os.path.join("logs", ".gate-memo.jsonl")),
                        "the memo must live in logs/, which is untracked")

    def test_record_then_lookup_returns_the_certificate(self):
        self.assertTrue(gate_memo.record(self.path, self.fp, "logs/soak/run-a", now="2026-09-13T15:00:00+05:30"))
        item = gate_memo.lookup(self.path, self.fp)
        self.assertEqual(item["run_dir"], "logs/soak/run-a")
        self.assertEqual(item["first_certified"], "2026-09-13T15:00:00+05:30")

    def test_the_first_certificate_wins(self):
        gate_memo.record(self.path, self.fp, "logs/soak/first", now="2026-09-13T15:00:00+05:30")
        self.assertFalse(gate_memo.record(self.path, self.fp, "logs/soak/second", now="2026-09-13T16:00:00+05:30"),
                         "a replay is not a new certificate")
        item = gate_memo.lookup(self.path, self.fp)
        self.assertEqual(item["run_dir"], "logs/soak/first")
        self.assertEqual(len(Path(self.path).read_text().strip().splitlines()), 1)

    def test_unknown_pair_is_not_a_replay(self):
        self.assertIsNone(gate_memo.lookup(self.path, "deadbeef:cafebabe"))

    def test_missing_file_is_not_an_error(self):
        self.assertEqual(gate_memo.entries(self.path), [])

    def test_unreadable_lines_are_skipped(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.path).write_text('{"fingerprint": "half-writ\nnot json\n\n'
                                   + json.dumps({"fingerprint": "aa:bb", "first_certified": "t",
                                                 "run_dir": "d"}) + "\n")
        self.assertIsNone(gate_memo.lookup(self.path, "half-writ"))
        self.assertEqual(gate_memo.lookup(self.path, "aa:bb")["run_dir"], "d")

    def test_describe_names_the_time_and_the_run(self):
        text = gate_memo.describe({"first_certified": "2026-09-13T15:00:00+05:30",
                                   "run_dir": "logs/soak/monday-gates-1"})
        self.assertIn("REPLAYED@2026-09-13T15:00:00+05:30", text)
        self.assertIn("logs/soak/monday-gates-1", text)

    def test_record_rejects_an_empty_fingerprint(self):
        with self.assertRaises(ValueError):
            gate_memo.record(self.path, "", "logs/soak/x")

    def test_cli_lookup_says_nothing_when_the_pair_is_new(self):
        r = self._cli("lookup", self.fp)
        self.assertEqual(r.returncode, 1)
        self.assertEqual(r.stdout.strip(), "")

    def test_cli_lookup_prints_replayed_for_a_known_pair(self):
        self._cli("record", self.fp, "logs/soak/run-a")
        r = self._cli("lookup", self.fp)
        self.assertEqual(r.returncode, 0)
        self.assertIn("REPLAYED@", r.stdout)
        self.assertIn("logs/soak/run-a", r.stdout)

    def test_cli_record_is_idempotent(self):
        self.assertIn("RECORDED", self._cli("record", self.fp, "logs/soak/run-a").stdout)
        self.assertIn("ALREADY-RECORDED", self._cli("record", self.fp, "logs/soak/run-b").stdout)
        self.assertEqual(len(Path(self.path).read_text().strip().splitlines()), 1)

    def test_cli_refuses_instead_of_guessing(self):
        self.assertEqual(self._cli().returncode, 2)
        self.assertEqual(self._cli("lookup").returncode, 2)
        self.assertEqual(self._cli("frobnicate", self.fp).returncode, 2)


if __name__ == "__main__":
    unittest.main()
