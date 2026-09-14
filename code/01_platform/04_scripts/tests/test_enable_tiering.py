#!/usr/bin/env python3
"""CLI contract test for the EnableTiering repair tool (wave 14, P6-382).

EnableTiering was the one repair tool that read args[0] unchecked: with no
arguments it threw ArrayIndexOutOfBoundsException — no usage line, no exit
contract. The fix validates the argument before any connection attempt, so the
usability of the check does not depend on a running Fluss cluster.
"""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / "code/01_platform/04_scripts/fluss-repair/EnableTiering.java"
INGESTION = ROOT / "code/02_services/01_ingestion"


def _classpath():
    cp_file = INGESTION / "target/cp.txt"
    if cp_file.is_file() and cp_file.read_text().strip():
        return cp_file.read_text().strip()
    m2 = Path.home() / ".m2/repository/org/apache/fluss"
    jars = sorted(str(p) for p in m2.rglob("*.jar")
                  if "sources" not in p.name and "javadoc" not in p.name)
    return os.pathsep.join(jars)


class EnableTieringCliTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.classes = Path(cls.tmp.name) / "classes"
        cls.classes.mkdir()
        cls.cp = _classpath()
        proc = subprocess.run(["javac", "-nowarn", "-cp", cls.cp, "-d", str(cls.classes),
                               str(SOURCE)], capture_output=True, text=True, timeout=300)
        # A compile failure is an AssertionError (wave-9a rule), never a skip.
        if proc.returncode != 0:
            raise AssertionError(f"javac failed for EnableTiering.java:\n{proc.stderr[-3000:]}")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def run_tool(self, args, timeout=60):
        return subprocess.run(
            ["java", "-cp", f"{self.classes}{os.pathsep}{self.cp}", "EnableTiering"] + args,
            capture_output=True, text=True, timeout=timeout)

    def test_no_arguments_is_usage_with_exit_2(self):
        proc = self.run_tool([])
        self.assertEqual(proc.returncode, 2, proc.stderr[-1500:])
        self.assertIn("Usage: EnableTiering <table> [bootstrap-servers]", proc.stderr)
        self.assertNotIn("ArrayIndexOutOfBoundsException", proc.stderr)

    def test_blank_table_name_is_refused_before_connecting(self):
        proc = self.run_tool(["   "], timeout=90)
        self.assertEqual(proc.returncode, 2, proc.stderr[-1500:])
        self.assertIn("Usage: EnableTiering", proc.stderr)


if __name__ == "__main__":
    unittest.main()
