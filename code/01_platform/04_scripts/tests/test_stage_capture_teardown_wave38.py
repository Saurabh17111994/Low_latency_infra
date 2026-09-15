#!/usr/bin/env python3
"""Wave 38 — stage-capture.sh must stop the io-latency probe it forked (P6-562).

The script forks the host disk-latency probe before its sampling loop and only
`wait`s for it at the very end. Every fail-fast exit in between (job left
RUNNING, a dead evidence leg, an incomplete evidence set) therefore abandoned
the probe: it kept writing into $OUT_DIR for up to DURATION_S+30s after
stage-capture had died, burned iostat load and raced the next run's teardown.

These tests run the *real* script with `curl`/`docker`/`iostat` stubs on PATH
and watch /proc for the probe's processes, so what is pinned is the script's
own teardown, not a re-implementation of it.

The three things being pinned:
  1. a fail-fast exit stops the probe (it used to leak);
  2. the normal path does NOT kill it — the tail waits out the probe's extra
     samples on purpose ("never kill mid-write"), and does it without
     signalling a recycled pid;
  3. a signal (INT/TERM) exits 130/143 rather than 0. A trap that forwards a
     bare `$?` reports SUCCESS here, because a signal arriving while bash waits
     on a child leaves $? = 0 — measured, and the reason the codes are explicit.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
CAPTURE_SRC = REPO / "code/01_platform/04_scripts/stage-capture.sh"
PROBE_SRC = REPO / "code/01_platform/04_scripts/io-latency-probe.sh"
STUBS = Path(__file__).resolve().parent / "stubs"

# Keeps the forked probe genuinely alive: `half-exit` closes stdout but the stub
# keeps running, so the probe's reader parks in proc.wait() for IOSTAT_WAIT_SEC
# instead of finishing in milliseconds. Without this the probe is already gone
# when the trap runs and every teardown assertion below would hold vacuously.
PARKED = {"W24_IOSTAT_SCENARIO": "half-exit", "IOSTAT_WAIT_SEC": "600"}


class Sandbox:
    """Sandbox tree holding the real stage-capture.sh plus its stubs."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.bin = root / "bin"
        self.bin.mkdir()
        self.script = root / "stage-capture.sh"
        shutil.copy2(CAPTURE_SRC, self.script)
        # The real probe, unchanged: the trap under test is the caller's.
        shutil.copy2(PROBE_SRC, root / "io-latency-probe.sh")
        for name, stub in (("curl", "wave38_curl.py"),
                           ("docker", "wave38_docker.py"),
                           ("iostat", "wave24_iostat.py")):
            shutil.copy2(STUBS / stub, self.bin / name)
            (self.bin / name).chmod(0o755)
        shutil.copy2(STUBS / "wave24_o2_ingest.py", root / "o2_ingest.py")
        self.out = root / "out"
        self.out.mkdir()
        self.curl_state = root / "curl-state.json"

    def env(self, **extra) -> dict:
        environ = dict(os.environ)
        environ["PATH"] = f"{self.bin}:{environ['PATH']}"
        environ.update({
            "OUT_DIR": str(self.out),
            "DURATION_S": "60",
            "CAPTURE_INTERVAL_S": "1",
            "FLINK_REST_URL": "http://flink.test:8081",
            "TM_PROM_URL": "http://tm.test:9250",
            "W38_CURL_STATE": str(self.curl_state),
            "W24_IOSTAT_SCENARIO": "normal",
            "W24_O2_MODE": "fast",
        })
        environ.update(extra)
        return environ

    def run(self, timeout: float = 40, **extra):
        proc = subprocess.Popen(
            ["bash", str(self.script)], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=self.env(**extra),
            start_new_session=True,
        )
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.reap(proc)
            raise
        return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)

    @staticmethod
    def reap(proc) -> None:
        """Kill the whole subtree: `subprocess` signals only the direct child."""
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)

    def probe_processes(self) -> list[str]:
        """Pids whose cmdline names this sandbox's probe or its iostat stub."""
        found = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
            except OSError:
                continue
            if str(self.root) in cmd and ("io-latency-probe" in cmd
                                          or "iostat" in cmd):
                found.append(entry.name)
        return found

    def settle(self, deadline_s: float = 8.0) -> list[str]:
        """Wait out the async reap; return whatever is still alive."""
        deadline = time.monotonic() + deadline_s
        left = self.probe_processes()
        while left and time.monotonic() < deadline:
            time.sleep(0.1)
            left = self.probe_processes()
        return left


class StageCaptureTeardownTest(unittest.TestCase):
    """P6-562 — the forked io probe is stopped on the way out."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w38-teardown-")
        self.sb = Sandbox(Path(self._tmp.name))

    def tearDown(self) -> None:
        # Never leave a stray probe behind if a test failed mid-flight.
        for pid in self.sb.probe_processes():
            try:
                os.kill(int(pid), signal.SIGKILL)
            except (OSError, ValueError):
                pass
        self._tmp.cleanup()

    def test_a_fail_fast_exit_stops_the_probe(self):
        """The P6-562 leak: job leaves RUNNING -> exit 2 -> probe must not survive.

        The probe must still be ALIVE when the fail-fast exit happens, or the
        assertion proves nothing: the parked-reader scenario below is what makes
        it long-lived, exactly as it is in production (DURATION_S+30s of iostat).
        """
        if not Path("/proc").is_dir():
            self.skipTest("/proc is not available")
        res = self.sb.run(W38_CURL_MODE="fail-after-preflight", **PARKED)
        self.assertEqual(res.returncode, 2, f"{res.stdout}\n{res.stderr}")
        self.assertIn("stopping the io-latency probe", res.stderr)

        left = self.sb.settle()
        self.assertEqual(left, [], f"fail-fast exit leaked the probe: pids {left}")

    def test_the_normal_path_does_not_kill_the_probe(self):
        """The other half: the trap must stay out of the way when the run ends.

        The sampling loop is over at DURATION_S=2 while the probe is still
        parked, so the tail `wait` is what ends it — the probe gets to deliver
        its OWN verdict (here: its grace expires and it reports the failure
        itself, exit 4). A trap that killed on the way out would cut that wait
        short and the probe's verdict would never be written.

        The script's exit stays 0: a failed probe is a WARN, not a capture
        failure, and the trap must not turn the tail's non-fatal path into one.
        """
        res = self.sb.run(DURATION_S="2", CAPTURE_INTERVAL_S="1",
                          **{**PARKED, "IOSTAT_WAIT_SEC": "3"})
        self.assertNotIn("stopping the io-latency probe", res.stderr)
        self.assertEqual(res.returncode, 0, f"{res.stdout}\n{res.stderr}")
        log = (self.sb.out / "io-latency-probe.log").read_text()
        self.assertIn("iostat did not exit in 3s, killing it", log,
                      f"the probe was denied its own ending:\n{log}")
        self.assertIn("WARN — io-latency probe failed", res.stderr)
        self.assertEqual(self.sb.settle(3.0), [],
                         "the probe outlived its own run on the normal path")

    def test_signals_stop_the_probe_and_exit_130_143(self):
        """INT/TERM must not report success, and must not leak the probe."""
        if not Path("/proc").is_dir():
            self.skipTest("/proc is not available")
        for signum, expected in ((signal.SIGTERM, 143), (signal.SIGINT, 130)):
            with self.subTest(signal=signum):
                tmp = tempfile.TemporaryDirectory(prefix="w38-signal-")
                try:
                    sb = Sandbox(Path(tmp.name))
                    proc = subprocess.Popen(
                        ["bash", str(sb.script)], stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE, text=True, env=sb.env(**PARKED),
                        start_new_session=True)
                    # Let the preflight finish and the probe fork.
                    deadline = time.monotonic() + 15
                    while not sb.probe_processes() and time.monotonic() < deadline:
                        time.sleep(0.1)
                    live = sb.probe_processes()
                    self.assertTrue(live, "the probe never started; not discriminating")
                    os.kill(proc.pid, signum)
                    try:
                        _, err = proc.communicate(timeout=20)
                    except subprocess.TimeoutExpired:
                        sb.reap(proc)
                        self.fail("stage-capture ignored the signal")
                    self.assertEqual(proc.returncode, expected, err)
                    self.assertIn("stopping the io-latency probe", err)
                    self.assertEqual(sb.settle(), [], f"signal leaked the probe")
                finally:
                    tmp.cleanup()


class WarningStreamTest(unittest.TestCase):
    """P6-863 — a WARN belongs on stderr like its siblings.

    Every FAIL in this script already went to stderr; two WARNs did not, so a
    caller that splits the streams (or greps stderr for warnings) silently lost
    them. The severity is low — the script's own caller merges the streams into
    one run log, so nothing was broken end to end — but the inconsistency is a
    trap for the next consumer.
    """

    def test_every_warn_and_fail_line_goes_to_stderr(self):
        """Source-text guard: the whole class, not just the two lines fixed."""
        offenders = [
            (n, line.strip())
            for n, line in enumerate(CAPTURE_SRC.read_text().splitlines(), 1)
            if line.strip().startswith("echo ")
            and ("WARN" in line or "FAIL" in line)
            and ">&2" not in line
        ]
        self.assertEqual(offenders, [],
                         f"WARN/FAIL lines missing >&2: {offenders}")

    def test_the_probe_warn_reaches_stderr_on_a_real_run(self):
        """The behavioural half: an actually-emitted WARN is on stderr."""
        with tempfile.TemporaryDirectory(prefix="w38-warn-") as td:
            sb = Sandbox(Path(td))
            res = sb.run(DURATION_S="2", CAPTURE_INTERVAL_S="1",
                         **{**PARKED, "IOSTAT_WAIT_SEC": "3"})
            self.assertIn("WARN — io-latency probe failed", res.stderr)
            self.assertNotIn("WARN — io-latency probe failed", res.stdout)


if __name__ == "__main__":
    unittest.main()
