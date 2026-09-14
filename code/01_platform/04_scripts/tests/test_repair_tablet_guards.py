"""Guard rails of repair-tablet.sh (P6-092..P6-100, P6-386..P6-393).

`docker` is stubbed on PATH: every call is recorded and replayed from env knobs, so
these tests never touch a container, a volume or the network. The truncation step is
stubbed as well; the verification logic itself is driven directly against real files
in VerifyAndTruncateTest, because that is where the data-loss risk lives.
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
REPAIR = ROOT / "code" / "01_platform" / "04_scripts" / "fluss-repair" / "repair-tablet.sh"
VERIFY = ROOT / "code" / "01_platform" / "04_scripts" / "fluss-repair" / "verify-and-truncate.py"

TABLE = "raw_table_1-1"
SEGMENT = f"/d/default/{TABLE}/20260901-p21/log-0/00000000000000000000.log"
# A scan line pair exactly as LogScan.py prints it for a torn segment.
SCAN_TORN = (f"{SEGMENT}: size=32157696 last_complete_batch_end=32156904 zero_tail=792 bytes\n"
             "TRUNCATE_TO=32156904\n")
SCAN_CLEAN = f"{SEGMENT}: size=32157696 last_complete_batch_end=32157696 zero_tail=0 bytes\n"

STUB = r'''#!/usr/bin/env python3
"""Replayable stand-in for the docker CLI: records argv, answers from env knobs."""
import os
import sys


def env(name, default=""):
    return os.environ.get(name, default)


def emit(text):
    sys.stdout.write(text)
    if text and not text.endswith("\n"):
        sys.stdout.write("\n")


def next_state():
    """First value of STUB_STATE_SEQ, then the following ones, clamped at the last."""
    seq = [s for s in env("STUB_STATE_SEQ").split(",") if s]
    if not seq:
        return env("STUB_STATE", "exited")
    counter = env("STUB_STATE_COUNTER")
    n = 0
    if counter:
        try:
            n = int(open(counter).read().strip() or "0")
        except OSError:
            n = 0
        with open(counter, "w", encoding="utf-8") as handle:
            handle.write(str(n + 1))
    return seq[min(n, len(seq) - 1)]


args = sys.argv[1:]
cmd = args[0] if args else ""
with open(os.environ["STUB_CALLS"], "a", encoding="utf-8") as recorded:
    recorded.write(" ".join(args) + "\n")

if cmd == "ps" and "-a" not in args:
    emit(env("STUB_NAME", "fixture-tablet-1") + "\t" + env("STUB_PS_STATUS", "Up 3 minutes"))
elif cmd == "ps":
    emit(env("STUB_NAME", "fixture-tablet-1"))
elif cmd == "inspect":
    fmt = ""
    for flag in ("-f", "--format"):
        if flag in args:
            fmt = args[args.index(flag) + 1]
            break
    if "State.Status" in fmt:
        emit(next_state())
    elif "RestartPolicy" in fmt:
        emit(env("STUB_POLICY", "always"))
    elif "Mounts" in fmt:
        emit(env("STUB_VOLUME", "fixture-volume"))
elif cmd == "logs":
    emit(env("STUB_LOGS", ""))
elif cmd == "run":
    joined = " ".join(args)
    if "test -d" in joined:
        sys.exit(int(env("STUB_TEST_D_RC", "0")))
    if "ls -dt" in joined:
        emit(env("STUB_AUTODETECT", "/d/default/raw_table_1-1/"))
    elif "mindepth" in joined:
        sys.stdout.write(open(env("STUB_TABLES")).read() if env("STUB_TABLES") else "")
    elif "find" in joined:
        sys.stdout.write(open(env("STUB_SEGMENTS")).read() if env("STUB_SEGMENTS") else "")
    elif "LogScan.py" in joined:
        sys.stdout.write(open(env("STUB_SCAN_FILE")).read())
        if env("STUB_SCAN_ECHO", "1") == "1":
            for line in sys.stdin.read().splitlines():
                if line:
                    sys.stderr.write("SCANNED " + line + "\n")
        sys.exit(int(env("STUB_SCAN_RC", "0")))
    elif "verify-and-truncate.py" in joined:
        emit(env("STUB_VERIFY_OUT", "truncated /d/x.log -> 12"))
        sys.exit(int(env("STUB_VERIFY_RC", "0")))
'''


class RepairGuardTest(unittest.TestCase):
    def setUp(self):
        self.sandbox = Path(tempfile.mkdtemp(prefix="w9-repair-"))
        self.addCleanup(shutil.rmtree, self.sandbox, True)
        bin_dir = self.sandbox / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "docker"
        stub.write_text(STUB)
        stub.chmod(0o755)
        self.calls = self.sandbox / "calls.txt"
        self.calls.write_text("")
        self.counter = self.sandbox / "state-counter"
        self.scan = self.sandbox / "scan.log"
        self.scan.write_text(SCAN_TORN)
        self.segments = self.sandbox / "segments.txt"
        self.segments.write_text(SEGMENT + "\n")
        self.tables = self.sandbox / "tables.nul"
        self.tables.write_bytes(f"/d/default/{TABLE}\0".encode())
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "HOME": os.environ.get("HOME", "/tmp"),
            "STUB_CALLS": str(self.calls),
            "STUB_STATE_COUNTER": str(self.counter),
            "STUB_SCAN_FILE": str(self.scan),
            "STUB_SEGMENTS": str(self.segments),
            "STUB_TABLES": str(self.tables),
            "STUB_STATE": "running",          # stop_tablet sees a running tablet ...
            # ... which exits on the stop poll, is still exited when the truncation
            # asserts it, and is running again after the restart.
            "STUB_STATE_SEQ": "running,exited,exited,running",
        }

    def run_repair(self, *args, **overrides):
        env = dict(self.env)
        env.update({key: str(value) for key, value in overrides.items()})
        return subprocess.run(["bash", str(REPAIR), *args], env=env, capture_output=True,
                              text=True, timeout=180)

    def recorded(self):
        """Recorded docker invocations, as argv token lists."""
        return [line.split() for line in self.calls.read_text().split("\n") if line]

    def first_index(self, command, *needles):
        """Index of the first `<command> ...` call containing every needle."""
        for index, argv in enumerate(self.recorded()):
            if argv[0] == command and all(needle in " ".join(argv) for needle in needles):
                return index
        self.fail(f"no recorded `{command}` call matching {needles}; recorded={self.recorded()}")

    def assert_not_called(self, command, *needles):
        for argv in self.recorded():
            if argv[0] == command and all(needle in " ".join(argv) for needle in needles):
                self.fail(f"unexpected docker call: {' '.join(argv)}")

    def first_index_containing(self, needle):
        """Index of the first recorded call mentioning `needle` anywhere in its argv."""
        for index, argv in enumerate(self.recorded()):
            if needle in " ".join(argv):
                return index
        self.fail(f"no recorded docker call containing {needle!r}; recorded={self.recorded()}")

    def test_table_argument_cannot_escape_the_table_dir(self):
        # P6-392: TABLE_ARG becomes a path under /d/default.
        for value in ("../../etc", "a b", "x;id", "raw_table_1-1/*", "/etc/passwd", "..", "."):
            with self.subTest(value=value):
                self.calls.write_text("")
                result = self.run_repair(value)
                self.assertEqual(result.returncode, 2, result.stderr[-400:])
                self.assertIn("invalid table argument", result.stderr)
                self.assert_not_called("run")           # no find, no scan, no truncate
                self.assert_not_called("stop")

    def test_dry_run_changes_nothing_at_all(self):
        # P6-388 / P6-393: previously the Up-guard refused a legitimate dry run, and
        # a dry run that did report still restarted the tablet afterwards.
        result = self.run_repair(DRY_RUN=1)
        self.assertEqual(result.returncode, 0, result.stderr[-400:])
        self.assertIn("DRY_RUN=1 — no changes made", result.stdout)
        self.assertIn("truncate -s 32156904", result.stdout)
        self.assertIn("torn-looking tails may be in-progress appends", result.stdout)
        self.assert_not_called("stop")
        self.assert_not_called("start")
        self.assert_not_called("update")
        self.assert_not_called("verify-and-truncate")

    def test_stop_precedes_scan_and_policy_is_pinned_and_restored(self):
        # P6-096 (no scan-to-truncate race), P6-097 (policy pinned in single-table
        # mode), P6-100 (wait for exit), P6-092 (restore on the way out).
        result = self.run_repair()
        self.assertEqual(result.returncode, 0, result.stderr[-600:])
        pinned = self.first_index("update", "--restart=no")
        stopped = self.first_index("stop")
        # The scan container (read-only volume + read-only script dir) and the
        # truncation container (writable volume) — addressed by their docker argv,
        # not by the LogScan.py text inside the sh -c body.
        scanned = self.first_index("run", "/d:ro", "/s:ro")
        verified = self.first_index("run", "verify-and-truncate.py")
        restored = self.first_index("update", "--restart=always")
        started = self.first_index("start")
        self.assertLess(pinned, stopped, "the restart policy must be pinned before the stop")
        self.assertLess(stopped, scanned, "the tablet must be stopped before the scan")
        self.assertLess(scanned, verified, "the scan must precede the verification")
        self.assertLess(restored, started, "the policy must be restored before the restart")
        self.assertIn("tablet status: running", result.stdout)
        # ... and the volume is mounted read-only for the scan (P6-386).
        scan_call = " ".join(self.recorded()[scanned])
        self.assertIn("fixture-volume:/d:ro", scan_call)
        self.assertIn("/s:ro", scan_call)
        # Only the truncation may write to the volume; the scan must not.
        self.assertIn("fixture-volume:/d ", " ".join(self.recorded()[verified]) + " ")

    def test_scanner_failure_refuses_and_restores_the_policy(self):
        # P6-095: a crashing LogScan.py used to read as "nothing to repair" (rc 2).
        result = self.run_repair(STUB_SCAN_RC="1")
        self.assertEqual(result.returncode, 1, result.stdout[-400:])
        self.assertIn("LogScan.py failed", result.stderr)
        self.assert_not_called("verify-and-truncate")
        self.first_index("update", "--restart=always")     # P6-092 on the error path

    def test_scanner_must_report_every_segment(self):
        result = self.run_repair(STUB_SCAN_ECHO="0")
        self.assertEqual(result.returncode, 1, result.stdout[-400:])
        self.assertIn("handed 1 segment(s) but reported 0", result.stderr)
        self.assert_not_called("verify-and-truncate")

    def test_verification_failure_never_restarts_the_tablet(self):
        # P6-098 / P6-099: if the pre-flight refuses, nothing was cut — so the tool
        # must not report success by starting the tablet again either.
        result = self.run_repair(STUB_VERIFY_RC="1")
        self.assertEqual(result.returncode, 1, result.stdout[-400:])
        self.assertIn("NO segment was truncated", result.stderr)
        self.assert_not_called("start")
        self.first_index("update", "--restart=always")

    def test_tablet_that_does_not_stop_is_refused(self):
        # P6-100: truncating while the process may still be flushing.
        result = self.run_repair(STOP_WAIT_SECS=2, STUB_STATE_SEQ="running")
        self.assertEqual(result.returncode, 1, result.stdout[-400:])
        self.assertIn("still running after 2s", result.stderr)
        self.assert_not_called("LogScan.py")
        self.first_index("update", "--restart=always")

    def test_dead_tablet_after_the_repair_fails_the_run(self):
        # P6-390 / P6-391: only `Restarting` used to count as failure, so an Exited
        # tablet was reported as a successful recovery.
        result = self.run_repair(STUB_STATE_SEQ="running,exited,exited,exited")
        self.assertEqual(result.returncode, 1, result.stdout[-400:])
        self.assertIn("not running — recovery did NOT complete", result.stderr)
        self.assertIn("docker start", result.stderr)

    def test_sweep_without_table_dirs_is_an_error(self):
        # P6-389: a silent 0-table sweep used to "succeed" and restart the tablet.
        self.tables.write_bytes(b"")
        result = self.run_repair("--all")
        self.assertEqual(result.returncode, 2, result.stdout[-400:])
        self.assertIn("no table directories", result.stderr)
        self.assert_not_called("start")
        self.first_index("update", "--restart=always")

    def test_sweep_scans_every_table_and_restarts_once(self):
        self.tables.write_bytes(f"/d/default/{TABLE}\0/d/default/feature_candles_15s\0".encode())
        result = self.run_repair("--all", STUB_STATE_SEQ="running,exited,exited,exited,running")
        self.assertEqual(result.returncode, 0, result.stderr[-600:])
        scans = [argv for argv in self.recorded() if "LogScan.py" in " ".join(argv)]
        self.assertEqual(len(scans), 2, scans)
        self.assertEqual(len([argv for argv in self.recorded() if argv[0] == "start"]), 1)
        self.assertIn("sweep done: 2 segment(s) truncated across 2 table(s)", result.stdout)


class VerifyAndTruncateTest(unittest.TestCase):
    """The destructive step itself: verify EVERY pair before cutting anything."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="w9-verify-"))
        self.addCleanup(shutil.rmtree, self.dir, True)

    def segment(self, name, data, zeros):
        path = self.dir / name
        path.write_bytes(data + bytes(zeros))
        return path

    def pairs(self, *entries):
        path = self.dir / "pairs.tsv"
        path.write_text("".join(f"{p}\t{end}\n" for p, end in entries))
        return path

    def run_verify(self, pairs_path):
        return subprocess.run(["python3", str(VERIFY), str(pairs_path)],
                              capture_output=True, text=True, timeout=60)

    def test_clean_tail_is_truncated(self):
        path = self.segment("a.log", b"DATA" * 100, 500)
        result = self.run_verify(self.pairs((path, 400)))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(path.stat().st_size, 400)

    def test_non_zero_tail_is_refused_and_left_alone(self):
        # P6-098: "0 < end < size" is not a safety check.
        path = self.segment("a.log", b"DATA" * 100 + bytes(400) + b"X", 0)
        result = self.run_verify(self.pairs((path, 400)))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("not all zero", result.stderr)
        self.assertEqual(path.stat().st_size, 801)

    def test_unsafe_boundary_is_refused(self):
        path = self.segment("a.log", b"DATA" * 100, 0)
        for end in (0, path.stat().st_size, path.stat().st_size + 1):
            with self.subTest(end=end):
                result = self.run_verify(self.pairs((path, end)))
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn("unsafe truncation", result.stderr)
                self.assertEqual(path.stat().st_size, 400)

    def test_one_bad_pair_leaves_every_segment_untouched(self):
        # P6-099: the old loop cut the first segment before failing on the second.
        good = self.segment("good.log", b"DATA" * 100, 500)
        bad = self.segment("bad.log", b"DATA" * 100 + bytes(400) + b"X", 0)
        result = self.run_verify(self.pairs((good, 400), (bad, 400)))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("NO segment was truncated", result.stderr)
        self.assertEqual(good.stat().st_size, 900)
        self.assertEqual(bad.stat().st_size, 801)

    def test_bad_input_is_refused(self):
        empty = self.dir / "empty.tsv"
        empty.write_text("")
        for pairs_path in (self.dir / "missing.tsv", empty):
            with self.subTest(pairs=str(pairs_path)):
                result = self.run_verify(pairs_path)
                self.assertEqual(result.returncode, 2, result.stdout)

    def test_malformed_offset_line_is_refused(self):
        path = self.segment("a.log", b"DATA" * 100, 500)
        broken = self.dir / "broken.tsv"
        broken.write_text(f"{path}\tabc\n")
        result = self.run_verify(broken)
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("expected '<path>", result.stderr)
        self.assertEqual(path.stat().st_size, 900)


if __name__ == "__main__":
    unittest.main()
