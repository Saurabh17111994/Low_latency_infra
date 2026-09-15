#!/usr/bin/env python3
"""Wave 23 — show-ticks.sh and smoke-test.sh preflight behaviour.

Both scripts are thin wrappers: they check a few things about their environment
and then hand over to `java`. Every test here runs the *real* script in a
sandbox tree with a `java` stub on PATH (plus a live TCP listener when the probe
must succeed), so the checks are the script's, not a re-implementation.

Pinned by these tests: arity (P6-695), the limit bound and its agreement with
TickTableViewer.MAX_LIMIT (P6-696), the bootstrap parse for `myhost`,
`host:abc`, `http://h:1`, `[::1]:9123` and comma lists (P6-296), the bounded
probe that keeps its reason (P6-297), `java` failing before the probe (P6-697),
env defaults instead of overrides (P6-687/688/689), and the artifact preflight
(P6-293). The `cd` guard and the Unix classpath note (P6-857) are asserted as
source text: `DIR` is derived from the script's own path, so the failure cannot
be triggered without breaking the read of the script itself.
"""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SHOW_TICKS = REPO / "show-ticks.sh"
SMOKE_TEST = REPO / "code/smoke-test.sh"
VIEWER = REPO / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java"

JAR_REL = "code/02_services/01_ingestion/target/ingestion.jar"
TEST_CLASSES_REL = "code/02_services/01_ingestion/target/test-classes"
# the script prints paths relative to the code dir, not the repo root
TEST_CLASSES_MSG = "02_services/01_ingestion/target/test-classes"


def _free_port() -> int:
    """A port that was listening a moment ago and is now closed."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Sandbox:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.root = tmp / "tree"
        self.bin = tmp / "stub-bin"
        self.call_log = tmp / "calls.log"
        self.env_dump = tmp / "java-env.txt"
        self.listener: socket.socket | None = None
        self.port = 0
        self._build()

    def _build(self) -> None:
        (self.root / "code/02_services/01_ingestion/target/test-classes").mkdir(parents=True)
        shutil.copy2(SHOW_TICKS, self.root / "show-ticks.sh")
        shutil.copy2(SMOKE_TEST, self.root / "code/smoke-test.sh")
        self.jar = self.root / JAR_REL
        self.jar.write_text("not a real jar\n")
        self.install_java_stub()

    def install_java_stub(self) -> None:
        self.bin.mkdir(parents=True, exist_ok=True)
        stub = self.bin / "java"
        stub.write_text(
            "#!/usr/bin/env bash\n"
            f"printf 'java %s\\n' \"$*\" >>{self.call_log}\n"
            f"env | grep -E '^(ARROW_|FLUSS_|RAW_TABLE_NAME=)' | sort >{self.env_dump}\n"
            "exit 0\n",
        )
        stub.chmod(0o755)

    def listen(self) -> int:
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(5)
        self.port = self.listener.getsockname()[1]
        return self.port

    def listen_v6(self) -> int | None:
        try:
            self.listener = socket.socket(socket.AF_INET6)
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind(("::1", 0))
            self.listener.listen(5)
            return self.listener.getsockname()[1]
        except OSError:
            return None

    def stop_listening(self) -> None:
        if self.listener is not None:
            self.listener.close()
            self.listener = None

    def env(self, *, java: bool = True, **overrides: str) -> dict[str, str]:
        path = f"{self.bin}:{os.environ['PATH']}" if java else self.tmp_no_java_path()
        base = {"PATH": path, "PROBE_TIMEOUT_SEC": "1"}
        base.update(overrides)
        return {**os.environ, **base}

    def tmp_no_java_path(self) -> str:
        """A PATH with bash and timeout but no java: simulates a missing JDK."""
        if not hasattr(self, "_nojdk"):
            nojdk = self.tmp / "no-jdk-bin"
            nojdk.mkdir(parents=True, exist_ok=True)
            # java is the only thing missing here; the script's other tools
            # (dirname, cat, env, grep, sort) must stay so the failure measures
            # a missing JDK and not a broken PATH.
            for tool in ("timeout", "bash", "dirname", "cat", "env", "grep", "sort"):
                found = shutil.which(tool)
                assert found, f"{tool} is required for this test"
                link = nojdk / tool
                if not link.exists():
                    link.symlink_to(found)
            self._nojdk = nojdk
        return str(self._nojdk)

    def run_ticks(self, *args: str, **overrides: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(self.root / "show-ticks.sh"), *args],
            cwd=self.root, env=self.env(**overrides),
            capture_output=True, text=True, timeout=60,
        )

    def run_smoke(self, **overrides: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(self.root / "code/smoke-test.sh")],
            cwd=self.tmp, env=self.env(**overrides),
            capture_output=True, text=True, timeout=60,
        )

    @property
    def calls(self) -> str:
        return self.call_log.read_text() if self.call_log.exists() else ""

    @property
    def env_text(self) -> str:
        return self.env_dump.read_text() if self.env_dump.exists() else ""


class TicksArgs(unittest.TestCase):
    """Arity and the row limit, before anything touches the network."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w23-ticks-")
        self.box = Sandbox(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_extra_argument_is_rejected(self) -> None:
        proc = self.box.run_ticks("20", "foo")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("Usage:", proc.stderr)
        self.assertEqual(self.box.calls, "", "the viewer ran with the typo ignored")

    def test_limit_above_the_viewer_cap_is_rejected(self) -> None:
        proc = self.box.run_ticks("10001")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("10000", proc.stderr)
        self.assertEqual(self.box.calls, "")

    def test_non_numeric_zero_and_fractional_limits_are_rejected(self) -> None:
        for value in ("0", "abc", "1.5", "-3", " 20"):
            with self.subTest(limit=value):
                proc = self.box.run_ticks(value)
                self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
                self.assertEqual(self.box.calls, "")

    def test_shell_bound_equals_the_viewer_cap(self) -> None:
        """The two numbers are one contract; this fails if either side moves."""
        java_cap = re.search(r"MAX_LIMIT\s*=\s*([0-9_]+)", VIEWER.read_text())
        self.assertIsNotNone(java_cap, "TickTableViewer no longer declares MAX_LIMIT")
        wanted = int(java_cap.group(1).replace("_", ""))
        shell_cap = re.search(r"^MAX_LIMIT=([0-9]+)$", SHOW_TICKS.read_text(), re.M)
        self.assertIsNotNone(shell_cap, "show-ticks.sh no longer declares MAX_LIMIT")
        self.assertEqual(int(shell_cap.group(1)), wanted)


class TicksBootstrap(unittest.TestCase):
    """P6-296/297: one parse for the probe and for the viewer."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w23-ticks-boot-")
        self.box = Sandbox(Path(self._tmp.name))

    def tearDown(self) -> None:
        self.box.stop_listening()
        self._tmp.cleanup()

    def test_multi_server_bootstrap_is_refused_by_the_probe(self) -> None:
        proc = self.box.run_ticks(FLUSS_BOOTSTRAP="h1:9123,h2:9123")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("Multiple bootstrap servers", proc.stderr)
        self.assertIn("h1:9123,h2:9123", proc.stderr)
        self.assertEqual(self.box.calls, "")

    def test_scheme_prefix_is_refused(self) -> None:
        proc = self.box.run_ticks(FLUSS_BOOTSTRAP="http://localhost:9123")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("without a scheme", proc.stderr)

    def test_missing_port_is_refused(self) -> None:
        proc = self.box.run_ticks(FLUSS_BOOTSTRAP="myhost")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("host:port", proc.stderr)

    def test_non_numeric_and_out_of_range_ports_are_refused(self) -> None:
        for value in ("localhost:abc", "localhost:0", "localhost:70000"):
            with self.subTest(bootstrap=value):
                proc = self.box.run_ticks(FLUSS_BOOTSTRAP=value)
                self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
                self.assertIn("port", proc.stderr)

    def test_ipv6_literal_is_probed_without_its_brackets(self) -> None:
        port = self.box.listen_v6()
        if port is None:
            self.skipTest("no IPv6 loopback on this host")
        proc = self.box.run_ticks(FLUSS_BOOTSTRAP=f"[::1]:{port}")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("TickTableViewer 20", self.box.calls)

    def test_probe_failure_names_the_endpoint_and_keeps_the_reason(self) -> None:
        port = _free_port()
        proc = self.box.run_ticks(FLUSS_BOOTSTRAP=f"127.0.0.1:{port}")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(f"TCP probe failed for 127.0.0.1:{port}:", proc.stderr)
        reason = proc.stderr.split("TCP probe failed for ", 1)[1].split("\n", 1)[0]
        self.assertGreater(len(reason.split(":", 1)[1].strip()), 0,
                           "the probe discards its reason")
        self.assertEqual(self.box.calls, "")

    def test_live_endpoint_reaches_the_viewer_with_the_limit(self) -> None:
        port = self.box.listen()
        proc = self.box.run_ticks("5", FLUSS_BOOTSTRAP=f"127.0.0.1:{port}")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("TickTableViewer 5", self.box.calls)


class TicksToolchain(unittest.TestCase):
    """P6-697 and the jar check: local problems are named before the probe."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w23-ticks-tool-")
        self.box = Sandbox(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_missing_java_is_reported_before_the_probe(self) -> None:
        proc = self.box.run_ticks(java=False, FLUSS_BOOTSTRAP="127.0.0.1:1")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("java not found", proc.stderr)
        self.assertNotIn("TCP probe failed", proc.stderr,
                         "a missing JDK was reported as a cluster problem")

    def test_missing_jar_is_reported_with_the_build_command(self) -> None:
        self.box.jar.unlink()
        proc = self.box.run_ticks()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("Missing ingestion JAR", proc.stderr)
        self.assertIn("mvn -pl 02_services/01_ingestion -am package", proc.stderr)


class SmokeTestPreflight(unittest.TestCase):
    """smoke-test.sh: env defaults, artifact preflight, named probe failure."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w23-smoke-")
        self.box = Sandbox(Path(self._tmp.name))

    def tearDown(self) -> None:
        self.box.stop_listening()
        self._tmp.cleanup()

    def test_env_overrides_reach_the_java_process(self) -> None:
        port = self.box.listen()
        proc = self.box.run_smoke(
            FLUSS_BOOTSTRAP=f"127.0.0.1:{port}",
            ARROW_APP_ID="acme-id", ARROW_APP_SECRET="acme-secret",
            ARROW_TOKEN="acme-token", RAW_TABLE_NAME="scratch_ticks",
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        env_text = self.box.env_text
        for expected in ("FLUSS_BOOTSTRAP=127.0.0.1:", "ARROW_APP_ID=acme-id",
                         "ARROW_APP_SECRET=acme-secret", "ARROW_TOKEN=acme-token",
                         "RAW_TABLE_NAME=scratch_ticks"):
            self.assertIn(expected, env_text)

    def test_local_defaults_apply_when_nothing_is_set(self) -> None:
        port = self.box.listen()
        proc = self.box.run_smoke(FLUSS_BOOTSTRAP=f"127.0.0.1:{port}")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        env_text = self.box.env_text
        for expected in ("ARROW_APP_ID=smoke-test", "ARROW_APP_SECRET=smoke-secret",
                         "ARROW_TOKEN=fake-token-for-test", "RAW_TABLE_NAME=raw_table_1",
                         "ARROW_MAX_EVENT_AGE_MS=5000",
                         "ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000"):
            self.assertIn(expected, env_text)
        self.assertIn("SmokeTest", self.box.calls)

    def test_missing_test_classes_fail_fast(self) -> None:
        shutil.rmtree(self.box.root / TEST_CLASSES_REL)
        proc = self.box.run_smoke()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(TEST_CLASSES_MSG, proc.stderr)
        self.assertIn("test-compile", proc.stderr)
        self.assertEqual(self.box.calls, "", "java ran without the test classes")

    def test_missing_jar_fails_fast(self) -> None:
        self.box.jar.unlink()
        proc = self.box.run_smoke()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("Missing", proc.stderr)
        self.assertEqual(self.box.calls, "")

    def test_down_cluster_fails_with_the_endpoint_and_reason(self) -> None:
        port = _free_port()
        proc = self.box.run_smoke(FLUSS_BOOTSTRAP=f"127.0.0.1:{port}")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(f"TCP probe failed for 127.0.0.1:{port}:", proc.stderr)
        self.assertEqual(self.box.calls, "", "SmokeTest ran against a dead cluster")

    def test_first_endpoint_of_a_list_is_the_one_probed(self) -> None:
        port = self.box.listen()
        proc = self.box.run_smoke(FLUSS_BOOTSTRAP=f"127.0.0.1:{port},other:9123")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("SmokeTest", self.box.calls)


class SmokeTestSource(unittest.TestCase):
    """P6-857: the cd guard is source-verified — DIR comes from the script path,
    so a `cd` failure would mean the script could not be read at all."""

    def test_cd_guard_and_classpath_note_are_present(self) -> None:
        src = SMOKE_TEST.read_text()
        self.assertIn('cd "$DIR" || {', src)
        self.assertIn("cannot cd to script dir", src)
        self.assertIn("Unix only", src)
        self.assertTrue(os.access(SMOKE_TEST, os.X_OK), "smoke-test.sh lost its exec bit")
        self.assertTrue(os.access(SHOW_TICKS, os.X_OK), "show-ticks.sh lost its exec bit")


if __name__ == "__main__":
    unittest.main()
