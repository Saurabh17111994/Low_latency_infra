#!/usr/bin/env python3
"""Wave 39 — fused_timeline.py and holistic-analyze.py must report what they claim.

One leg per audit finding: P6-399, P6-400, P6-401, P6-738, P6-739 (fused_timeline.py)
and P6-402, P6-403, P6-404, P6-740, P6-741, P6-742 (holistic-analyze.py).

Two of these are reachable offline and are tested by running the code:

* P6-399 — pagination. `_o2_search` asked for `from: 0, size: 5000` once, so an
  io-latency capture longer than ~2.8 h (2 s cadence, ~1800 rows/h) silently lost
  the tail — the last rows of a window, which is what a run analysis wants most.
* P6-400 — the documented exit code 4. The io-latency and flink_checkpoints
  queries were unguarded, so a URLError left `main()` as a traceback with exit 1.

The rest are arithmetic or shape fixes inside `main()`, which cannot be reached
without a live capture, so they are pinned at the source: the fixed expression
must be present AND the broken one must be gone. That is the same guard style the
gate uses for its own script (`test_gate_drill_exclusions.py`), and it is honest
about its limit — it proves the fix landed, not that the analysis is right.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "code/01_platform/04_scripts"
FUSED = SCRIPTS / "fused_timeline.py"
ANALYZE = SCRIPTS / "holistic-analyze.py"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _src(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class _FakeResp:
    """Stands in for urlopen()'s context manager."""

    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FusedTimelineTest(unittest.TestCase):
    """P6-399, P6-400, P6-401, P6-738, P6-739."""

    @classmethod
    def setUpClass(cls):
        cls.ft = _load(FUSED, "fused_timeline_w39")

    def test_p6_399_o2_search_paginates_instead_of_dropping_the_tail(self):
        pages = [{"hits": [{"i": i} for i in range(5000)]},          # a full page
                 {"hits": [{"i": 5000}, {"i": 5001}, {"i": 5002}]}]  # short → stop
        asked = []

        def fake_urlopen(req, timeout=None):
            asked.append(json.loads(req.data.decode())["query"]["from"])
            return _FakeResp(pages.pop(0) if pages else {"hits": []})

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            result = self.ft._o2_search("SELECT 1", 0, 10, "auth")

        self.assertEqual([h["i"] for h in result["hits"]], list(range(5003)),
                         "a full first page must be followed by the next one")
        self.assertEqual(asked, [0, 5000],
                         "the second request must ask for the page after the first")

    def test_p6_400_a_failed_o2_query_returns_the_documented_exit_4(self):
        if shutil.which("python3") is None:
            self.skipTest("python3 not on PATH")
        with tempfile.TemporaryDirectory() as cap:
            env = dict(os.environ,
                       O2_AUTH_BASIC="dXNlcjpwYXNz",
                       # port 9 (discard) refuses immediately: deterministic, no server
                       O2_URL="http://127.0.0.1:9")
            r = subprocess.run(
                [sys.executable, str(FUSED), "--capture", cap,
                 "--start", "1758000000", "--end", "1758000600"],
                capture_output=True, text=True, timeout=180, env=env)
        self.assertEqual(r.returncode, 4,
                         f"expected the documented exit 4, got {r.returncode}; "
                         f"stderr tail: {r.stderr[-400:]}")

    def test_p6_401_the_staleness_bound_expires_old_samples(self):
        src = _src(FUSED)
        self.assertNotIn("(t - g) <= 120", src,
                         "`t <= g and (t - g) <= 120` can never filter: t - g <= 0 always")
        self.assertIn("def forward_fill(grid, by_t, agg, max_age_s=120)", src,
                      "the bounded forward-fill lives in the shared helper")
        self.assertIn("(g - t) <= max_age_s", src,
                      "a sample older than the bound must not be forward-filled")
        self.assertIn("if not cand:", src,
                      "no in-bound sample must leave the grid point EMPTY (XC-6)")

    def test_p6_738_unused_imports_are_gone(self):
        src = _src(FUSED)
        self.assertNotIn("import io as _io", src, "dead import")
        self.assertNotIn("\nimport time\n", src, "dead import")

    def test_p6_739_a_bare_output_filename_does_not_break_the_run(self):
        src = _src(FUSED)
        self.assertNotIn("os.makedirs(os.path.dirname(out_path), exist_ok=True)", src,
                         'dirname("fused.tsv") is "" and makedirs("") raises')
        self.assertIn("if out_dir:", src,
                      "the directory must only be created when there is one")


class HolisticAnalyzeTest(unittest.TestCase):
    """P6-402, P6-403, P6-404, P6-740, P6-741, P6-742."""

    @classmethod
    def setUpClass(cls):
        cls.an = _load(ANALYZE, "holistic_analyze_w39")

    def test_p6_402_a_zero_last_event_ts_is_not_a_latency_burst(self):
        # XC-10 (G11 verification): the retired-preview parse loop this guard
        # inspected was unreachable (`prev_rows = []`) and was deleted. The
        # zero-last_event_ts hazard cannot exist without the loop; keep the
        # loop from returning in the unsafe shape.
        src = _src(ANALYZE)
        self.assertNotIn("for ln in prev_rows:", src,
                         "the retired preview parse loop must stay deleted")
        self.assertNotIn("buckets_1s.setdefault", src,
                         "the unguarded 1s-bucket append must not come back")

    def test_p6_403_p95_is_computed_by_the_percentile_helper(self):
        src = _src(ANALYZE)
        self.assertNotIn("v[int(len(v) * 0.95)]", src,
                         "an unsorted list indexed at 0.95*len is not a percentile")
        # CHG-538: the burst-slice call this used to pin was retired with the
        # preview e2e-latency leg; pin the live G6a percentile call instead.
        self.assertIn("pct(e2e_lat, 95)", src,
                      "the module's own pct() sorts before it picks")

    def test_p6_404_a_missing_run_start_does_not_crash_the_read_storm_block(self):
        src = _src(ANALYZE)
        self.assertNotIn("b[0] - run_start // 1000", src,
                         "run_start is None when the optional argument is omitted")
        self.assertIn("b[0] - run_start_ms // 1000", src,
                      "run_start_ms already means `run_start or 0`")

    def test_p6_740_a_failed_reader_compile_returns_none_not_an_empty_read(self):
        if shutil.which("javac") is None:
            self.skipTest("javac not on PATH")
        with tempfile.TemporaryDirectory() as out_dir:
            got = self.an.collect_rows_stream(
                "Signal_Candidates", "/nonexistent-classpath", out_dir, run_ms=1000)
        self.assertIsNone(got,
                          "[] is indistinguishable from a legitimately empty table and "
                          "silently bypasses the F4 fail-closed guard")

    def test_p6_741_the_dead_ms_lambda_is_gone(self):
        self.assertNotIn("ms = lambda v: v", _src(ANALYZE),
                         "defined and never referenced")

    def test_xc19_collect_rows_offset_mode_accepts_the_offset_marker(self):
        """XC-19: offset mode prints "@<offset> <row>"; the "(" filter made
        with_offset=True always report a failed read."""
        import subprocess

        with tempfile.TemporaryDirectory() as out_dir:
            def fake_run(cmd, **kw):
                if cmd[0] == "javac":
                    return subprocess.CompletedProcess(cmd, 0, "", "")
                return subprocess.CompletedProcess(
                    cmd, 0, "@123 (1,2,3)\n(4,5,6)\n", "")

            with mock.patch.object(self.an.subprocess, "run", fake_run):
                rows = self.an.collect_rows(
                    "T", "cp", out_dir, run_ms=1000, with_offset=True)
        self.assertEqual(rows, ["@123 (1,2,3)", "(4,5,6)"])

    def test_p6_742_an_empty_process_rate_list_does_not_abort_the_analysis(self):
        src = _src(ANALYZE)
        self.assertIn("if ranked:\n                top = sorted(ranked, reverse=True)[0]",
                      src,
                      "ranked stays [] when every io series has <2 samples, so the "
                      "verdict must be guarded before indexing [0]")


if __name__ == "__main__":
    unittest.main()
