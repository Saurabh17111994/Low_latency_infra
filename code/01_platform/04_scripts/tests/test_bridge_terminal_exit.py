"""H2-3 guard: the Go bridge's exit code tells the truth about why it ended.

A terminal runtime failure (here: the single-socket policy violation) must exit
``exitTerminalRuntime = 3`` after the normal drain; a requested stop stays 0. The
assertion runs the built binary in a subprocess — an in-process call cannot observe
the process exit code.

The go toolchain is a gate prerequisite (Go 1.24+); when it is absent the test skips
so a plain ``pytest`` run on a machine without Go stays green.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
GO_BRIDGE = REPO / "code/02_services/01_ingestion/go-bridge"
GO = shutil.which("go")
EXIT_TERMINAL_RUNTIME = 3


class BridgeTerminalExitTest(unittest.TestCase):

    @unittest.skipUnless(GO, "go toolchain not installed (gate prerequisite)")
    def test_policy_violation_exits_terminal_runtime_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "arrow-bridge"
            build = subprocess.run(
                [GO, "build", "-o", str(binary), "."],
                cwd=GO_BRIDGE, capture_output=True, text=True, timeout=300)
            self.assertEqual(0, build.returncode, f"go build failed:\n{build.stderr}")

            env = {
                **os.environ,
                "ARROW_APP_ID": "test-app",
                "ARROW_APP_SECRET": "test-secret",
                "ARROW_FAKE_BROKER": "1",
                "TRANSPORT": "proto",
                # More tokens than one connection can hold (1024) forces a second
                # slot; the blank approval pair makes that a policy violation.
                "ARROW_HFT_CONNECTIONS": "2",
                "ARROW_INSTRUMENT_TOKENS": ",".join(str(i) for i in range(1, 1026)),
                "ARROW_HFT_MULTI_CONNECTION_APPROVED": "",
                "DEPLOYMENT_ENV": "",
            }
            run = subprocess.run([str(binary)], env=env, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.PIPE, timeout=120)
            stderr = run.stderr.decode("utf-8", errors="replace")
            self.assertEqual(
                EXIT_TERMINAL_RUNTIME, run.returncode,
                "a policy-violation run must exit 3 (terminal), not pretend a requested "
                f"stop exited 0; stderr tail:\n{stderr[-400:]}")
            self.assertIn("planned=2", stderr,
                          "the run must actually have planned two sockets before judging "
                          "the exit code")


if __name__ == "__main__":
    unittest.main()
