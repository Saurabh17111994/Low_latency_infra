import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from c2_progress import PhaseProgress, summarize_phase  # noqa: E402


class C2ProgressTest(unittest.TestCase):
    def _summary(self, rows: str) -> PhaseProgress | None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metric-progress.tsv"
            path.write_text(
                "epoch\tphase\telapsed_s\tstate\tsource_read\tall_writes\n"
                + rows
            )
            return summarize_phase(path, "post-kill")

    def test_counter_reset_is_counted_without_accepting_reset_alone(self):
        summary = self._summary(
            "1\tpost-kill\t0\tRUNNING\t100\t200\n"
            "2\tpost-kill\t5\tRUNNING\t5\t8\n"
            "3\tpost-kill\t10\tRUNNING\t9\t14\n"
        )
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual((summary.read_resets, summary.write_resets), (1, 1))

    def test_reset_alone_does_not_count_as_an_increase(self):
        summary = self._summary(
            "1\tpost-kill\t0\tRUNNING\t100\t200\n"
            "2\tpost-kill\t5\tRUNNING\t0\t0\n"
        )
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual((summary.read_resets, summary.read_increases), (1, 0))
        self.assertEqual((summary.write_resets, summary.write_increases), (1, 0))


if __name__ == "__main__":
    unittest.main()
