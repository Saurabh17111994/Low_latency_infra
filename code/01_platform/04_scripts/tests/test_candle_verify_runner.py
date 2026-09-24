"""Tests for candle-verify.sh (argv contract; the live run is opt-in)."""
import os
import shutil
import subprocess
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "candle-verify.sh")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))


def run(*args):
    return subprocess.run(["bash", SCRIPT] + list(args), capture_output=True, text=True)


class ArgvContract(unittest.TestCase):
    def test_syntax(self):
        r = subprocess.run(["bash", "-n", SCRIPT], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_no_args_is_usage_error(self):
        r = run()
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_non_numeric_minutes_is_usage_error(self):
        r = run("--minutes", "abc")
        self.assertEqual(r.returncode, 2)

    def test_missing_value_is_usage_error(self):
        r = run("--minutes")
        self.assertEqual(r.returncode, 2)

    def test_unknown_flag_is_usage_error(self):
        r = run("--minutes", "1", "--nope")
        self.assertEqual(r.returncode, 2)

    def test_probe_source_is_where_the_runner_expects_it(self):
        probe = os.path.join(os.path.dirname(HERE), "fluss-probes", "CandleVerify.java")
        self.assertTrue(os.path.exists(probe), probe)
        text = open(probe, encoding="utf-8").read()
        # the runner compiles exactly this file name, which must match the public class
        self.assertIn("public class CandleVerify", text)
        self.assertIn("absentNoTrades", text)

    @unittest.skipUnless(shutil.which("javac") and os.path.exists(
        os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt")),
        "needs a JDK and a built ingestion classpath")
    def test_probe_compiles(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            probe = os.path.join(os.path.dirname(HERE), "fluss-probes", "CandleVerify.java")
            shutil.copy(probe, os.path.join(d, "CandleVerify.java"))
            cp = open(os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt"),
                      encoding="utf-8").read().strip()
            r = subprocess.run(["javac", "-nowarn", "-cp", cp, "-d", d, os.path.join(d, "CandleVerify.java")],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()
