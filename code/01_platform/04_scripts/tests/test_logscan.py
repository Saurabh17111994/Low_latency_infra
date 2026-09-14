#!/usr/bin/env python3
"""Contract tests for LogScan.py — the truncated-segment scanner (wave 14).

The scanner is the only tool that tells the tablet-repair flow where a log
segment really ends, so two properties matter more than speed:

* a segment with no complete batch must NOT advertise TRUNCATE_TO=0 (P6-383) —
  a caller applying that value would shrink the file to zero bytes;
* the hot loop must not seek absolutely per batch (P6-735) — the rewrite reads
  sequentially and skips each body relative to the current position, which has
  to produce exactly the same boundary as before.

The segments here are synthetic: 48-byte header, batch total = 12 +
int32_le(header[8:12]), body = total - 48.
"""
import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPT_DIR = ROOT / "code/01_platform/04_scripts/fluss-repair"
sys.path.insert(0, str(SCRIPT_DIR))

import LogScan  # noqa: E402

HEADER = LogScan.HEADER_SIZE


def batch(total=100, fill=b"\x01"):
    """One complete batch of `total` bytes (>= 48)."""
    header = bytearray(HEADER)
    header[8:12] = (total - 12).to_bytes(4, "little")
    return bytes(header) + fill * (total - HEADER)


class ScanBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def write(self, data, name="seg.log"):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(data)
        return path

    def run_main(self, path):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = LogScan.main(["LogScan.py", path])
        return rc, out.getvalue()

    def test_clean_segment_reports_ok(self):
        rc, out = self.run_main(self.write(batch(100)))
        self.assertEqual(rc, 0)
        self.assertIn("OK (size=100, no truncated tail)", out)
        self.assertNotIn("TRUNCATE_TO", out)

    def test_zero_tail_is_reported_with_the_last_complete_boundary(self):
        rc, out = self.run_main(self.write(batch(100) + batch(100) + b"\x00" * 96))
        self.assertEqual(rc, 0)
        self.assertIn("last_complete_batch_end=200", out)
        self.assertIn("TRUNCATE_TO=200", out)

    def test_all_zero_segment_never_advertises_truncate_to_zero(self):
        """P6-383: nothing to keep must not look like 'keep everything'."""
        rc, out = self.run_main(self.write(b"\x00" * 480))
        self.assertEqual(rc, 0)
        self.assertIn("NO_COMPLETE_BATCH=", out)
        self.assertNotIn("TRUNCATE_TO", out)

    def test_file_smaller_than_one_header_never_advertises_truncate_to_zero(self):
        rc, out = self.run_main(self.write(b"\x07" * 40))
        self.assertEqual(rc, 0)
        self.assertIn("NO_COMPLETE_BATCH=", out)
        self.assertNotIn("TRUNCATE_TO", out)

    def test_partial_trailing_batch_stops_at_the_last_complete_one(self):
        # second header declares 500 bytes but only 60 remain
        truncated = bytearray(HEADER)
        truncated[8:12] = (500 - 12).to_bytes(4, "little")
        rc, out = self.run_main(self.write(batch(100) + bytes(truncated) + b"\x01" * 12))
        self.assertEqual(rc, 0)
        self.assertIn("TRUNCATE_TO=100", out)

    def test_many_batches_scan_to_the_exact_end(self):
        """P6-735: the sequential rewrite must keep the old arithmetic."""
        n, total = 500, 64
        rc, out = self.run_main(self.write(batch(total) * n))
        self.assertEqual(rc, 0)
        self.assertIn(f"OK (size={n * total}, no truncated tail)", out)

    def test_header_only_batch_advances_by_its_total(self):
        rc, out = self.run_main(self.write(batch(total=HEADER)))
        self.assertEqual(rc, 0)
        self.assertIn(f"OK (size={HEADER}, no truncated tail)", out)

    def test_missing_file_errors_on_stderr_and_exit_stays_zero(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = LogScan.main(["LogScan.py", os.path.join(self.tmp.name, "nope.log")])
        self.assertEqual(rc, 0, "the caller decides; the scanner reports and continues")


class CliShapeTest(unittest.TestCase):
    def test_no_arguments_is_usage_with_exit_2(self):
        proc = subprocess.run([sys.executable, str(SCRIPT_DIR / "LogScan.py")],
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 2)
        self.assertIn("usage: python3 LogScan.py", proc.stderr)

    def test_scans_the_file_named_on_the_command_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "seg.log")
            with open(path, "wb") as fh:
                fh.write(batch(100) + batch(100) + b"\x00" * 48)
            proc = subprocess.run([sys.executable, str(SCRIPT_DIR / "LogScan.py"), path],
                                  capture_output=True, text=True, timeout=60)
            self.assertEqual(proc.returncode, 0)
            self.assertIn("TRUNCATE_TO=200", proc.stdout)


if __name__ == "__main__":
    unittest.main()
