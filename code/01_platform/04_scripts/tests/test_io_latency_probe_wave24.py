#!/usr/bin/env python3
"""Wave 24 — io-latency-probe.sh evidence integrity and teardown.

The probe streams `iostat -x 2 N` into a TSV (raw truth) and a JSONL (the
OpenObserve single-pane source). Every test here runs the *real* script in a
sandbox tree with the `iostat` and `o2_ingest.py` stubs from `tests/stubs/` in
place, so what is pinned is the script's own parsing, filtering and teardown
behaviour, not a re-implementation of it.

Pinned findings: the documented no-match fallback (P6-119 — a DEVICE argument
that matches nothing keeps ALL devices and warns), one epoch plus one host
snapshot per interval block (P6-433), no stale avg-cpu reuse across intervals
(P6-431), TSV/JSONL column agreement (P6-750), loud-but-bounded handling of
malformed rows (P6-432), iostat stderr as DEVNULL instead of an undrained pipe
(P6-430), a half-exited iostat killed with the documented exit 4 instead of a
traceback (P6-120/P6-434), DURATION_S validation before any side effect
(P6-429), the stages-dir guards (P6-749) and the bounded O2 push (P6-751).

Two scenarios reproduce, against the pre-fix script, the ways a stuck child used
to escape: `verbose-stderr` (an undrained stderr pipe blocks the child once its
64KiB buffer fills — P6-430) and `half-exit` (the child closes stdout but keeps
running, so `proc.wait()` in the finally timed out unhandled — P6-120).
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PROBE_SRC = REPO / "code/01_platform/04_scripts/io-latency-probe.sh"
STUBS = Path(__file__).resolve().parent / "stubs"


class Sandbox:
    """Sandbox tree holding the real probe plus its two stubs."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.bin = root / "bin"
        self.bin.mkdir()
        self.script = root / "io-latency-probe.sh"
        shutil.copy2(PROBE_SRC, self.script)
        self._write(root / "o2_ingest.py", STUBS / "wave24_o2_ingest.py", 0o644)
        self._write(self.bin / "iostat", STUBS / "wave24_iostat.py", 0o755)
        self.out = root / "out"
        self.out.mkdir()

    @staticmethod
    def _write(path: Path, source: Path, mode: int) -> None:
        shutil.copy2(source, path)
        path.chmod(mode)

    @property
    def stages(self) -> Path:
        return self.out / "stages"

    def run(self, duration: str, *devices: str, env: dict | None = None,
            out_dir: Path | None = None, timeout: float = 30):
        environ = dict(os.environ)
        environ["PATH"] = f"{self.bin}:{environ['PATH']}"
        environ.setdefault("W24_IOSTAT_SCENARIO", "normal")
        environ.setdefault("W24_O2_MODE", "fast")
        environ.update(env or {})
        # Own process group + an explicit reap on timeout. `subprocess.run(
        # timeout=)` signals ONLY bash, so a capture that outran the timeout
        # left the probe's python reader and its own iostat reparented to init
        # — still blocked on the stdout pipe, and on the verbose-stderr
        # scenario (P6-430) blocked forever: a wave-24 red leg left a pair of
        # them alive for 6 hours. Killing the group takes the whole subtree.
        proc = subprocess.Popen(
            ["bash", str(self.script), str(out_dir or self.out), duration,
             *devices],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=environ, start_new_session=True,
        )
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._reap_group(proc)
            raise
        return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)

    @staticmethod
    def _reap_group(proc: subprocess.Popen) -> None:
        """TERM then KILL the capture's whole process group, and drain it."""
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                break
            try:
                proc.wait(timeout=5)
                break
            except subprocess.TimeoutExpired:
                continue
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                stream.close()

    def tsv(self) -> tuple[list[str], list[dict]]:
        lines = [ln for ln in
                 (self.stages / "io-latency.tsv").read_text(
                     encoding="utf-8").splitlines() if ln]
        header = lines[0].split("\t")
        return header, [dict(zip(header, ln.split("\t"))) for ln in lines[1:]]

    def jsonl(self) -> list[dict]:
        return [json.loads(ln) for ln in
                (self.stages / "io-latency.jsonl").read_text(
                    encoding="utf-8").splitlines() if ln.strip()]


class ProbeTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.sb = Sandbox(Path(tmp.name))


class DeviceFilterTest(ProbeTest):
    """P6-119 — the documented fallback has to be decidable, so rows wait."""

    def test_requested_device_keeps_only_that_device(self):
        res = self.sb.run("120", "nvme0n1")
        self.assertEqual(res.returncode, 0, res.stderr)
        _, rows = self.sb.tsv()
        self.assertEqual([r["device"] for r in rows], ["nvme0n1"])

    def test_unmatched_device_keeps_all_devices_and_warns(self):
        res = self.sb.run("120", "nvme9")
        self.assertEqual(res.returncode, 0, res.stderr)
        _, rows = self.sb.tsv()
        self.assertEqual(sorted(r["device"] for r in rows),
                         ["nvme0n1", "nvme1"])
        self.assertIn("none of ['nvme9']", res.stderr)
        self.assertIn("kept ALL devices (documented fallback)", res.stderr)

    def test_a_match_beside_a_non_match_still_filters(self):
        # partial match: nvme9 does not exist, the other two do — the requested
        # set wins, the absent device is simply not in the output
        res = self.sb.run("120", "nvme0n1", "nvme1", "nvme9")
        self.assertEqual(res.returncode, 0, res.stderr)
        _, rows = self.sb.tsv()
        self.assertEqual(sorted(r["device"] for r in rows),
                         ["nvme0n1", "nvme1"])


class IntervalConsistencyTest(ProbeTest):
    """P6-433/P6-431/P6-750 — one interval, one snapshot, no reuse."""

    def test_rows_of_one_interval_share_epoch_and_host_stats(self):
        # `paced-rows` publishes the interval's two device rows more than a
        # second apart, so a per-row timestamp cannot land in one epoch_s
        res = self.sb.run("120", "nvme0n1", "nvme1",
                          env={"W24_IOSTAT_SCENARIO": "paced-rows"})
        self.assertEqual(res.returncode, 0, res.stderr)
        _, rows = self.sb.tsv()
        self.assertEqual([r["device"] for r in rows], ["nvme0n1", "nvme1"])
        self.assertEqual(rows[0]["epoch_s"], rows[1]["epoch_s"])
        self.assertEqual(rows[0]["cpu_mhz_avg"], rows[1]["cpu_mhz_avg"])
        self.assertEqual(rows[0]["mem_avail_mb"], rows[1]["mem_avail_mb"])

    def test_tsv_and_jsonl_agree_on_the_cpu_breakdown(self):
        res = self.sb.run("120", "nvme0n1")
        self.assertEqual(res.returncode, 0, res.stderr)
        header, rows = self.sb.tsv()
        jsonl = self.sb.jsonl()
        self.assertEqual(len(rows), len(jsonl))
        # every JSONL field has a TSV column, and the values agree
        self.assertLessEqual(set(jsonl[0]), set(header))
        # the emitted rows are interval 2's (interval 1 is the since-boot
        # average and is skipped per device), so nice is the 2.0 that block
        # printed — in both sinks
        self.assertEqual(rows[0]["cpu_nice_pct"], "2.0")
        self.assertEqual(jsonl[0]["cpu_nice_pct"], 2.0)
        self.assertEqual(jsonl[0]["cpu_user_pct"], 11.0)

    def test_absent_cpu_block_does_not_reuse_the_previous_interval(self):
        res = self.sb.run("120", "nvme0n1", "nvme1",
                          env={"W24_IOSTAT_SCENARIO": "no-cpu-block"})
        self.assertEqual(res.returncode, 0, res.stderr)
        _, rows = self.sb.tsv()
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row["cpu_user_pct"], "")
            self.assertEqual(row["cpu_nice_pct"], "")
            self.assertEqual(row["cpu_idle_pct"], "")

    def test_malformed_device_row_is_dropped_loudly_and_counted(self):
        res = self.sb.run("120", "nvme0n1", "nvme1",
                          env={"W24_IOSTAT_SCENARIO": "malformed-row"})
        self.assertEqual(res.returncode, 0, res.stderr)
        _, rows = self.sb.tsv()
        self.assertEqual([r["device"] for r in rows], ["nvme1"])
        self.assertIn("dropping malformed row 'nvme0n1'", res.stderr)
        self.assertIn("dropped 1 malformed row(s)", res.stderr)


class ProbeFailureTest(ProbeTest):
    """Documented exit codes: 2 = caller input, 3 = no iostat, 4 = degraded."""

    def test_duration_must_be_a_positive_integer(self):
        for bad in ("abc", "120s", "0", "-5", "1.5"):
            with self.subTest(duration=bad):
                res = self.sb.run(bad, "nvme0n1")
                self.assertEqual(res.returncode, 2, res.stderr)
                self.assertIn("DURATION_S must be a positive integer",
                              res.stderr)
                # the check runs before any side effect
                self.assertFalse((self.sb.stages / "io-latency.tsv").exists())

    def test_unwritable_stages_dir_fails_with_the_documented_exit(self):
        blocker = self.sb.root / "blocker"
        blocker.write_text("not a directory", encoding="utf-8")
        res = self.sb.run("120", "nvme0n1", out_dir=blocker)
        self.assertEqual(res.returncode, 4, res.stderr)
        self.assertIn("cannot create", res.stderr)

    def test_read_only_stages_dir_fails_with_the_documented_exit(self):
        ro = self.sb.root / "ro"
        ro.mkdir()
        ro.chmod(0o500)
        self.addCleanup(ro.chmod, 0o700)
        res = self.sb.run("120", "nvme0n1",
                          env={"IO_PROBE_STAGES_DIR": str(ro)})
        self.assertEqual(res.returncode, 4, res.stderr)
        self.assertIn("cannot write", res.stderr)


class ProbeTeardownTest(ProbeTest):
    """P6-430/P6-120/P6-434 — the child never hangs the capture."""

    def test_verbose_iostat_stderr_does_not_stall_the_capture(self):
        res = self.sb.run("120", "nvme0n1",
                          env={"W24_IOSTAT_SCENARIO": "verbose-stderr"})
        self.assertEqual(res.returncode, 0, res.stderr)
        _, rows = self.sb.tsv()
        self.assertEqual(len(rows), 1)

    def test_half_exited_iostat_is_killed_and_reported_degraded(self):
        res = self.sb.run("120", "nvme0n1",
                          env={"W24_IOSTAT_SCENARIO": "half-exit",
                               "IOSTAT_WAIT_SEC": "1"})
        self.assertEqual(res.returncode, 4, res.stderr)
        self.assertIn("did not exit in 1s, killing it", res.stderr)
        self.assertIn("evidence DEGRADED", res.stderr)


class HarnessReapTest(ProbeTest):
    """The harness must not outlive its own timeout.

    `subprocess.run(timeout=)` signals only bash, so a capture that outran the
    timeout left the probe's reader and its own iostat reparented to init and
    blocked on the stdout pipe — permanently, on the verbose-stderr scenario.
    A wave-24 red leg left such a pair alive for six hours.
    """

    @staticmethod
    def _named_by(root: Path) -> list[str]:
        """PIDs whose command line names this sandbox (reader and stubs)."""
        found = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
            except OSError:
                continue          # exited between listdir and read
            if str(root) in cmd:
                found.append(entry.name)
        return found

    def test_a_timed_out_capture_reaps_its_whole_subtree(self):
        if not Path("/proc").is_dir():
            self.skipTest("/proc is not available")
        with self.assertRaises(subprocess.TimeoutExpired):
            # IOSTAT_WAIT_SEC keeps the reader alive well past the timeout, so
            # without the group kill it survives to be caught here.
            self.sb.run("120", "nvme0n1",
                        env={"W24_IOSTAT_SCENARIO": "half-exit",
                             "IOSTAT_WAIT_SEC": "600"},
                        timeout=1.5)
        deadline = time.time() + 5
        left = self._named_by(self.sb.root)
        while left and time.time() < deadline:
            time.sleep(0.1)
            left = self._named_by(self.sb.root)
        self.assertEqual(left, [], f"timed-out capture leaked {left}")


class O2PushTest(ProbeTest):
    """P6-751 — the best-effort push is bounded; the file evidence wins."""

    def test_push_failure_warns_and_keeps_the_exit_code_zero(self):
        res = self.sb.run("120", "nvme0n1", env={"W24_O2_MODE": "fail"})
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("WARN — O2 push failed", res.stderr)
        self.assertEqual(len(self.sb.tsv()[1]), 1)

    def test_hung_push_is_bounded(self):
        start = time.monotonic()
        res = self.sb.run("120", "nvme0n1",
                          env={"W24_O2_MODE": "hang",
                               "O2_PUSH_TIMEOUT_SEC": "1"},
                          timeout=45)
        elapsed = time.monotonic() - start
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("O2 push failed or timed out after 1s", res.stderr)
        self.assertLess(elapsed, 20, "the push bound did not hold")
        self.assertEqual(len(self.sb.tsv()[1]), 1)


if __name__ == "__main__":
    unittest.main()
