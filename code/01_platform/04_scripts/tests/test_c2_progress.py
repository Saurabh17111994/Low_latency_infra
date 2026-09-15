import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from c2_progress import PhaseProgress, summarize_phase  # noqa: E402

MODULE = Path(__file__).resolve().parent.parent / "c2_progress.py"
HEADER = "epoch\tphase\telapsed_s\tstate\tsource_read\tall_writes\n"
# Three monotone samples: one per poll interval, nothing stale or reset.
ROWS = (
    "1\tpost-kill\t0\tRUNNING\t100\t200\n"
    "2\tpost-kill\t5\tRUNNING\t105\t208\n"
    "3\tpost-kill\t10\tRUNNING\t111\t219\n"
)


class C2ProgressTest(unittest.TestCase):
    def _summary(self, rows: str) -> PhaseProgress | None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metric-progress.tsv"
            path.write_text(HEADER + rows)
            return summarize_phase(path, "post-kill")

    def _run(self, rows: str, *args: str) -> "subprocess.CompletedProcess[str]":
        """Run the module the way the drill script does, flags and all."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metric-progress.tsv"
            path.write_text(HEADER + rows)
            return subprocess.run(
                [sys.executable, str(MODULE), *args, str(path), "post-kill"],
                capture_output=True,
                text=True,
                check=False,
            )

    def test_counter_reset_is_counted_without_accepting_reset_alone(self):
        summary = self._summary(
            "1\tpost-kill\t0\tRUNNING\t100\t200\n"
            "2\tpost-kill\t5\tRUNNING\t5\t8\n"
            "3\tpost-kill\t10\tRUNNING\t9\t14\n"
        )
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual((summary.read_resets, summary.write_resets), (1, 1))

    def test_a_missing_counter_is_not_a_sample(self):
        # P6-251: metric_totals() reports a missing counter as -1. A window where
        # every row carries -1 has no measurement in it at all, so the summary is
        # empty and the caller fails closed instead of reporting "no change".
        self.assertIsNone(self._summary(
            "1\tpost-kill\t0\tRUNNING\t-1\t-1\n"
            "2\tpost-kill\t5\tRUNNING\t-1\t-1\n"
        ))

    def test_the_jump_out_of_minus_one_is_not_an_increase(self):
        # The first real sample after a -1 run is a baseline, not progress: the
        # old code compared it against -1 and counted a huge increase.
        summary = self._summary(
            "1\tpost-kill\t0\tRUNNING\t-1\t-1\n"
            "2\tpost-kill\t5\tRUNNING\t100\t200\n"
            "3\tpost-kill\t10\tRUNNING\t110\t215\n"
        )
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(1, summary.read_increases)
        self.assertEqual(1, summary.write_increases)
        self.assertEqual((100, 110), (summary.first_read, summary.last_read))
        self.assertEqual(2, summary.samples)

    def test_reset_alone_does_not_count_as_an_increase(self):
        summary = self._summary(
            "1\tpost-kill\t0\tRUNNING\t100\t200\n"
            "2\tpost-kill\t5\tRUNNING\t0\t0\n"
        )
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual((summary.read_resets, summary.read_increases), (1, 0))
        self.assertEqual((summary.write_resets, summary.write_increases), (1, 0))


    def test_min_samples_accepts_a_window_sampled_end_to_end(self):
        proc = self._run(ROWS, "--min-samples", "3")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(len(proc.stdout.strip().split("\t")), 11)

    def test_min_samples_rejects_a_thin_window(self):
        proc = self._run(ROWS, "--min-samples", "4")
        self.assertEqual(proc.returncode, 2)
        self.assertIn("sampled 3 time(s)", proc.stderr)


if __name__ == "__main__":
    unittest.main()
