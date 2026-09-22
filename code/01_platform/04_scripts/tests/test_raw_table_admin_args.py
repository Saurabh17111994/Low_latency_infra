"""Argument handling of RawTableAdmin (P6-004, P6-091, P6-384, P6-385).

The class is compiled with the ingestion classpath and driven as a CLI. Every case
except the last fails during argument validation, i.e. before any connection is
opened; the last one asserts a `--bootstrap` address is really dialled, with a
listening socket standing in for the cluster. No real cluster is contacted: the
connect-path cases point the bootstrap at a stub or at a closed port, so a case can
never reach the live raw_table_1.
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
SOURCE = Path(os.environ.get("RAW_TABLE_ADMIN_SOURCE")
              or ROOT / "code" / "01_platform" / "04_scripts" / "fluss-repair" / "RawTableAdmin.java")
CP_FILE = ROOT / "code" / "02_services" / "01_ingestion" / "target" / "cp.txt"
COMMON_CLASSES = ROOT / "code" / "common" / "target" / "classes"


class RawTableAdminArgsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("javac") is None or shutil.which("java") is None:
            raise unittest.SkipTest("no JDK on PATH")
        if not CP_FILE.exists():
            raise unittest.SkipTest(f"{CP_FILE} missing — build the ingestion module first")
        cls.tmp = Path(tempfile.mkdtemp(prefix="w9-rawadmin-"))
        cls.addClassCleanup(shutil.rmtree, cls.tmp, True)
        cls.cp = os.pathsep.join([str(cls.tmp), str(COMMON_CLASSES), CP_FILE.read_text().strip()])
        compiled = subprocess.run(["javac", "-cp", cls.cp, "-d", str(cls.tmp), str(SOURCE)],
                                  capture_output=True, text=True, timeout=300)
        # A compile failure is a real failure, not a skip: the same command is what
        # tiering-smoke.sh's GUARD-D runs.
        if compiled.returncode != 0:
            raise AssertionError(f"javac failed:\n{compiled.stderr[:2000]}")

    def run_admin(self, *args, timeout=60):
        try:
            return subprocess.run(["java", "-cp", self.cp, "RawTableAdmin", *args],
                                  capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self.fail(f"RawTableAdmin {args} did not exit ({exc})")

    def assert_refused(self, *args, expected):
        result = self.run_admin(*args)
        self.assertEqual(result.returncode, 2,
                         f"expected a pre-connection refusal; stdout={result.stdout[-300:]!r} "
                         f"stderr={result.stderr[-500:]!r}")
        self.assertIn(expected, result.stderr)
        self.assertIn("usage: RawTableAdmin", result.stderr)
        return result

    def test_missing_partition_argument_is_refused(self):
        self.assert_refused("add-partition", expected="add-partition needs exactly one")

    def test_non_date_partition_values_are_refused(self):
        # A wrong-format partition creates a second, non-auto partition that nothing
        # writes to (P6-091); 20260230 is the shape-valid but impossible date.
        for value in ("2026-01-01", "20260230", "2026_01_01", "abc", "202601"):
            with self.subTest(value=value):
                self.assert_refused("add-partition", value,
                                    expected="partition must be a real yyyyMMdd date")

    def test_bootstrap_is_not_a_positional_argument(self):
        # P6-004: `add-partition 20260101` used to dial 20260101 as the bootstrap.
        self.assert_refused("drop", "20260101", expected="drop takes no arguments")
        # ... and the flag is never silently swallowed as a subcommand or a value.
        self.assert_refused("--bootstrap", expected="unknown subcommand: --bootstrap")
        self.assert_refused("partitions", "--bootstrap", expected="--bootstrap needs HOST:PORT")

    def test_unknown_subcommand_is_refused_before_connecting(self):
        # P6-384: the old code opened a Fluss connection before looking at the
        # subcommand, so this failed slowly and for the wrong reason.
        self.assert_refused("frobnicate", expected="unknown subcommand: frobnicate")

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
            # 20260101 stays the partition; the flag address is what gets dialled.
            try:
                self.run_admin("add-partition", "20260101", "--bootstrap", f"127.0.0.1:{port}",
                               timeout=5)   # P1.5: accepted is set on accept(), which is
                                            # immediate; 30s was the CLI retry loop
            except Exception:                                   # noqa: BLE001 - the dial is the assertion
                pass                                            # connection refused/closed -> fine
            worker.join(timeout=10)
        finally:
            listener.close()
        self.assertTrue(accepted, "the --bootstrap address was never dialled")


if __name__ == "__main__":
    unittest.main()
