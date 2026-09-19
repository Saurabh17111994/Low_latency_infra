"""Unit tests for ing-tcp001/reconcile-compare.py — the count-based
losslessness reconcile (ING-TCP-002). Synthetic tick-count fixtures with
known lost / extra / vanished mutations; missing or truncated counter files
must fail closed, never pass.
"""

import os
import subprocess
import sys
import tempfile
import unittest

COMPARE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "ing-tcp001", "reconcile-compare.py")


class ParseTests(unittest.TestCase):
    """parse_bridge / parse_probe unit behavior."""

    def setUp(self):
        # The module file is hyphenated (reconcile-compare.py), so it cannot be
        # imported by name — load it via importlib from its path.
        import importlib.util
        # P6-840: reuse the module-level COMPARE — deriving the path twice meant a
        # move had to be reflected in two places or the classes silently diverge.
        spec = importlib.util.spec_from_file_location("reconcile_compare", COMPARE)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.rc = mod
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _write(self, body):
        with tempfile.NamedTemporaryFile(
                "w", dir=self.tmp.name, suffix=".txt", delete=False) as fh:
            fh.write(body)
            return fh.name

    def test_parse_bridge_single_token(self):
        m = self.rc.parse_bridge(self._write(
            "arrow-tick-counts: total=3 t=100:n=3\n"))
        self.assertEqual(m, {100: 3})

    def test_parse_bridge_chunked_report(self):
        # The real report is chunked to 20 tokens/line to stay under the Java
        # log-line cap; every chunk repeats the running total.
        m = self.rc.parse_bridge(self._write(
            "arrow-tick-counts: total=5 chunk=0/2 t=100:n=3 t=200:n=2\n"
            "arrow-tick-counts: total=5 chunk=1/2 t=300:n=0\n"))
        self.assertEqual(m, {100: 3, 200: 2, 300: 0})

    def test_parse_bridge_ignores_non_token_lines(self):
        m = self.rc.parse_bridge(self._write(
            "arrow-bridge: HFT subscribed 1024 tokens\n"
            "arrow-tick-counts: total=1 t=7:n=1\n"))
        self.assertEqual(m, {7: 1})

    def test_parse_bridge_empty(self):
        self.assertEqual(self.rc.parse_bridge(self._write("")), {})

    def test_parse_probe(self):
        m = self.rc.parse_probe(self._write(
            "TOKEN 100 RAW=3 QUAR=0 TOTAL=3\n"
            "TOKEN 200 RAW=0 QUAR=2 TOTAL=2\n"
            "grand total rows=5\n"))
        self.assertEqual(m, {100: (3, 0), 200: (0, 2)})

    def test_parse_probe_empty(self):
        self.assertEqual(self.rc.parse_probe(self._write("")), {})

    def test_parse_probe_accepts_the_fluss_sentinel(self):
        """P6-625: the token field must accept -1. With the old `\d+` pattern the row
        was dropped silently, which is exactly why the CLI tolerance test above used
        to pass without reaching the script's `t != -1` guard."""
        m = self.rc.parse_probe(self._write(
            "TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n"
            "TOKEN -1 RAW=1 QUAR=0 TOTAL=1\n"))
        self.assertIn(-1, m)
        self.assertIn(100, m)


class CliTests(unittest.TestCase):
    """End-to-end CLI behavior: exit 0 = pass, 1 = mismatch, and the
    fail-closed guarantees for missing/truncated evidence files."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _write(self, name, body):
        with open(os.path.join(self.tmp.name, name), "w", encoding="utf-8") as fh:
            fh.write(body)
        return os.path.join(self.tmp.name, name)

    def _run(self, *args, **kwargs):
        return subprocess.run(
            [sys.executable, COMPARE, *args], capture_output=True, text=True,
            **kwargs)

    def _bridge(self, body):
        return self._write("bridge.txt", body)

    def _probe(self, name, body):
        return self._write(name, body)

    # ---- happy paths ----

    def test_exact_total_mode_passes(self):
        bridge = self._bridge("arrow-tick-counts: total=5 t=100:n=3 t=200:n=2\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt",
                           "TOKEN 100 RAW=3 QUAR=0 TOTAL=3\n"
                           "TOKEN 200 RAW=0 QUAR=2 TOTAL=2\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post,
                      "--exact", "--sink", "total")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("RESULT PASS", r.stdout)

    def test_default_quar_mode_passes_multi_epoch(self):
        # Multi-epoch validation: pre already has rows; deltas >= bridge count
        # with no vanished/unexpected tokens is a pass. RAW must stay 0 in quar
        # mode (any RAW>0 in post is a mismatch by design).
        bridge = self._bridge("arrow-tick-counts: total=2 t=100:n=2\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=5 TOTAL=5\n")
        post = self._probe("post.txt", "TOKEN 100 RAW=0 QUAR=7 TOTAL=7\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_raw_mode_tolerates_quarantine_rows(self):
        # At market hours stale edge ticks legitimately land in quarantine;
        # --sink raw compares RAW deltas only.
        bridge = self._bridge("arrow-tick-counts: total=3 t=100:n=3\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt", "TOKEN 100 RAW=3 QUAR=1 TOTAL=4\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post,
                      "--sink", "raw")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    # ---- mutation classes ----

    def test_exact_lost_ticks_fail(self):
        bridge = self._bridge("arrow-tick-counts: total=3 t=100:n=3\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt", "TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post,
                      "--exact", "--sink", "total")
        self.assertEqual(r.returncode, 1)
        self.assertIn("MISMATCH token=100", r.stdout)

    def test_default_lost_ticks_fail(self):
        bridge = self._bridge("arrow-tick-counts: total=3 t=100:n=3\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt", "TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1)
        self.assertIn("MISMATCH token=100", r.stdout)

    def test_exact_extra_ticks_fail(self):
        bridge = self._bridge("arrow-tick-counts: total=3 t=100:n=3\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt", "TOKEN 100 RAW=4 QUAR=0 TOTAL=4\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post,
                      "--exact", "--sink", "total")
        self.assertEqual(r.returncode, 1)
        self.assertIn("MISMATCH token=100", r.stdout)

    def test_vanished_tokens_fail(self):
        bridge = self._bridge("arrow-tick-counts: total=2 t=100:n=2\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=1 TOTAL=1\n")
        # Post is non-empty but no longer contains token 100: it vanished.
        post = self._probe("post.txt", "TOKEN 200 RAW=0 QUAR=5 TOTAL=5\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1)
        self.assertIn("vanished", r.stdout)

    def test_unexpected_sink_tokens_fail(self):
        bridge = self._bridge("arrow-tick-counts: total=2 t=100:n=2\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt",
                           "TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n"
                           "TOKEN 999 RAW=1 QUAR=0 TOTAL=1\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1)
        self.assertIn("unexpected sink tokens", r.stdout)

    def test_fluss_minus_one_sentinel_tolerated(self):
        # The probe emits token -1 (Fluss sentinel) in some modes; it must not
        # count as an unexpected sink token.
        bridge = self._bridge("arrow-tick-counts: total=2 t=100:n=2\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt",
                           "TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n"
                           "TOKEN -1 RAW=1 QUAR=0 TOTAL=1\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post,
                      "--sink", "total")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_quar_mode_rejects_raw_rows(self):
        # Post-close runs expect rows in quarantine; RAW>0 is a mismatch.
        bridge = self._bridge("arrow-tick-counts: total=2 t=100:n=2\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt", "TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1)
        self.assertIn("RAW>0", r.stdout)

    # ---- fail-closed: missing/truncated evidence never passes ----

    def test_missing_bridge_file_fails(self):
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt", "TOKEN 100 RAW=1 QUAR=0 TOTAL=1\n")
        r = self._run("--bridge", os.path.join(self.tmp.name, "nope.txt"),
                      "--pre", pre, "--post", post)
        self.assertNotEqual(r.returncode, 0, r.stdout)

    def test_empty_bridge_file_fails_closed(self):
        bridge = self._bridge("")  # truncated: zero bytes
        pre = self._probe("pre.txt", "")
        post = self._probe("post.txt", "")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1)
        self.assertIn("empty or truncated", r.stdout)

    def test_truncated_bridge_file_fails_closed(self):
        # Totals line survived but every t= chunk was cut: parse yields {} and
        # the reconcile must not vacuously pass.
        bridge = self._bridge("arrow-tick-counts: total=0 chunk=0/1\n")
        pre = self._probe("pre.txt", "")
        post = self._probe("post.txt", "")
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1)
        self.assertIn("empty or truncated", r.stdout)

    def test_empty_post_probe_fails_closed(self):
        bridge = self._bridge("arrow-tick-counts: total=2 t=100:n=2\n")
        pre = self._probe("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
        post = self._probe("post.txt", "")  # probe produced nothing
        r = self._run("--bridge", bridge, "--pre", pre, "--post", post)
        self.assertEqual(r.returncode, 1)
        self.assertIn("empty or truncated", r.stdout)


if __name__ == "__main__":
    unittest.main()
