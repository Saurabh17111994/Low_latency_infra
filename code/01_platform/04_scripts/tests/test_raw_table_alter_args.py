"""Argument handling of RawTableAlter (v4 schema upgrade, 2026-09-24).

The class is compiled with the ingestion classpath and driven as a CLI. Every case
except the last fails during argument validation, i.e. before any connection is
opened; the last one asserts a `--bootstrap` address is really dialled, with a
listening socket standing in for the cluster. No real cluster is contacted, so no
case can reach the live raw_table_1 -- that is the tool's own prefix guard's job,
and it is only ever exercised by a human running plan/apply against a real table.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(os.environ.get("RAW_TABLE_ALTER_SOURCE")
              or ROOT / "code" / "01_platform" / "04_scripts" / "fluss-repair" / "RawTableAlter.java")
CP_FILE = ROOT / "code" / "02_services" / "01_ingestion" / "target" / "cp.txt"
COMMON_CLASSES = ROOT / "code" / "common" / "target" / "classes"


class RawTableAlterArgsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("javac") is None or shutil.which("java") is None:
            raise unittest.SkipTest("no JDK on PATH")
        if not CP_FILE.exists():
            raise unittest.SkipTest(f"{CP_FILE} missing -- build the ingestion module first")
        cls.tmp = Path(tempfile.mkdtemp(prefix="w9-rawalter-"))
        cls.addClassCleanup(shutil.rmtree, cls.tmp, True)
        cls.cp = os.pathsep.join([str(cls.tmp), str(COMMON_CLASSES), CP_FILE.read_text().strip()])
        compiled = subprocess.run(["javac", "-cp", cls.cp, "-d", str(cls.tmp), str(SOURCE)],
                                  capture_output=True, text=True, timeout=300)
        if compiled.returncode != 0:
            raise AssertionError(f"javac failed:\n{compiled.stderr[:2000]}")

    def run_alter(self, *args, timeout=60):
        try:
            return subprocess.run(["java", "-cp", self.cp, "RawTableAlter", *args],
                                  capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            self.fail(f"RawTableAlter {args} did not exit")

    def assert_refused(self, *args, expected):
        result = self.run_alter(*args)
        self.assertEqual(result.returncode, 2,
                         f"expected a pre-connection refusal; stdout={result.stdout[-300:]!r} "
                         f"stderr={result.stderr[-500:]!r}")
        self.assertIn(expected, result.stderr)
        self.assertIn("usage: RawTableAlter", result.stderr)
        return result

    def test_missing_subcommand_is_refused(self):
        self.assert_refused(expected="expected one of plan|apply|verify")

    def test_unknown_subcommand_is_refused_before_connecting(self):
        # The subcommand is validated before any connection is opened, so a typo
        # costs nothing and cannot leave a half-run apply on the live table.
        self.assert_refused("frobnicate", expected="expected one of plan|apply|verify")

    def test_surplus_positional_is_refused(self):
        self.assert_refused("plan", "extra", expected="unexpected argument: extra")

    def test_bootstrap_requires_a_value(self):
        self.assert_refused("plan", "--bootstrap", expected="--bootstrap needs HOST:PORT")

    def test_explicit_bootstrap_address_is_actually_dialled(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        accepted = []

        def serve():
            try:
                conn, _ = listener.accept()
                accepted.append(True)
                conn.close()
            except OSError:
                pass

        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        try:
            try:
                self.run_alter("plan", "--bootstrap", f"127.0.0.1:{port}", timeout=5)
            except Exception:                       # noqa: BLE001 - the dial is the assertion
                pass                                # connection closed -> fine
            worker.join(timeout=10)
        finally:
            listener.close()
        self.assertTrue(accepted, "the --bootstrap address was never dialled")


if __name__ == "__main__":
    unittest.main()
