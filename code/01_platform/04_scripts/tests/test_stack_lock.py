#!/usr/bin/env python3
"""stack-lock.sh — the lock that keeps two writers off one stack.

Why this test exists: on 2026-09-13 two certificate runs were refused at the
preflight because a second worktree rebuilt the flink/compute images mid-window,
and `make clean` (`compose down -v`) could have deleted a running certificate's
catalog. The lock is only worth having if a contender really is stopped before
the command runs, so these tests hold the real flock and demand a refusal.
"""

import fcntl
import os
import subprocess
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
LOCKER = ROOT / "code" / "01_platform" / "04_scripts" / "stack-lock.sh"

HOLDER = """
import fcntl, pathlib, sys, time
lock = pathlib.Path(sys.argv[1])
pathlib.Path(lock.parent).mkdir(parents=True, exist_ok=True)
handle = open(lock, "a+")  # create-if-absent, same as the gate's `exec 9<>`
fcntl.flock(handle, fcntl.LOCK_EX)
pathlib.Path(str(lock) + ".held").write_text("held")
time.sleep(float(sys.argv[2]))
"""


class StackLockTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self._tmpdir())
        self.lock = self.tmp / ".monday-gates.lock"
        self.env = dict(os.environ, STACK_LOCK_DIR=str(self.tmp))
        self.env.pop("STACK_LOCK_HELD", None)

    def _tmpdir(self) -> str:
        import tempfile
        self._dir = tempfile.mkdtemp(prefix="stack-lock-test-")
        return self._dir

    def _hold(self):
        holder = subprocess.Popen([sys.executable, "-c", HOLDER, str(self.lock), "30"])
        deadline = time.time() + 10
        while time.time() < deadline:
            if Path(str(self.lock) + ".held").exists():
                return holder
            time.sleep(0.05)
        holder.kill()
        self.fail("the test's own lock holder never acquired the lock")

    def _run(self, *argv, env=None):
        marker = self.tmp / f"ran-{len(list(self.tmp.glob('ran-*')))}"
        target = [str(marker)] if not argv else list(argv)
        cmd = ["bash", str(LOCKER), "bash", "-c", 'touch "$1"', "_"] + target
        return subprocess.run(cmd, capture_output=True, text=True,
                              env=env or self.env, cwd=str(ROOT)), marker

    def test_refuses_while_another_process_holds_the_lock(self):
        holder = self._hold()
        try:
            r, marker = self._run()
            self.assertEqual(r.returncode, 4, "a held lock must stop the command")
            self.assertIn("STACK BUSY", r.stderr)
            self.assertFalse(marker.exists(), "the command must not run at all")
        finally:
            holder.kill()
            holder.wait()

    def test_runs_once_the_holder_is_gone(self):
        holder = self._hold()
        holder.kill()
        holder.wait()
        r, marker = self._run()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertTrue(marker.exists(), "the command must run once the lock is free")

    def test_a_locked_section_passes_through(self):
        """The gate holds the lock and then runs commands that mutate the stack:
        if those contended, a green run could fail against itself."""
        holder = self._hold()
        try:
            env = dict(self.env, STACK_LOCK_HELD="1")
            r, marker = self._run(env=env)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertTrue(marker.exists(), "STACK_LOCK_HELD must bypass the lock")
        finally:
            holder.kill()
            holder.wait()

    def test_no_arguments_is_a_usage_error(self):
        r = subprocess.run(["bash", str(LOCKER)], capture_output=True, text=True,
                           env=self.env, cwd=str(ROOT))
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_lock_path_is_the_one_the_gate_uses(self):
        """Both must lock the same inode; different paths would be two locks."""
        gate = (ROOT / "code" / "01_platform" / "04_scripts" /
                "run-monday-gates.sh").read_text(encoding="utf-8")
        self.assertIn('GATE_LOCK_FILE="$PROJECT_ROOT/logs/.monday-gates.lock"', gate,
                      "the gate's lock path moved — stack-lock.sh must follow it")
        self.assertIn('.monday-gates.lock', LOCKER.read_text(encoding="utf-8"),
                      "stack-lock.sh must lock the gate's file, not one of its own")


if __name__ == "__main__":
    unittest.main()
