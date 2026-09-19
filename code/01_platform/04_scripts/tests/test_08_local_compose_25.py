"""25-instrument extended prod-scale smoke via Nautilus trader (fake broker).

Extends LOCAL-INT-004 (canonical 10) to 25 random instruments — same
lifecycle: signal→instruction→gateway→Nautilus→FakeBridge→ReportEnvelope→
Fluss Order_Lifecycle/Positions/Order_Correlation→Babysitter zero-actions,
no ARROW_* egress. Offline PASS without containers.

P6-596: the live leg needs the execution-t3=fake stack, and it SKIPS when that
stack is not up. The harness answers "no bridge/nautilus" with a contract-only
PASS, so a green run without the stack proves nothing live happened.

P6-800: the harness runs under sys.executable — the interpreter running this
suite — so a virtualenv, or a host without a `python3` on PATH, cannot make the
smoke fail for a reason unrelated to what it checks.
"""
import json, shutil, subprocess, sys, unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
HARNESS = ROOT / "code/01_platform/04_scripts/local_int_004_smoke.py"
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"


def run_harness(*args, expect_ok=True):
    """Run the harness out-of-process and return its stdout (P6-801).

    P6-599's lesson: the harness reports failures as `FAIL [tag]: reason` on
    stdout, so a bare CalledProcessError hides the only useful part. The exit
    code is checked here and the output carried into the failure.
    """
    cmd = [sys.executable, str(HARNESS), *args]
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if expect_ok and proc.returncode != 0:
        raise AssertionError(f"{' '.join(cmd)} exited {proc.returncode}:\n{proc.stdout}")
    return proc.stdout


def live_stack_up():
    """True when the stack the live leg needs is actually running (P6-596).

    The probe mirrors the harness's own (same env files, same profile); it is
    deliberately conservative — anything unexpected answers False, which skips
    the leg instead of claiming a live run that never happened.
    """
    docker = shutil.which("docker")
    if docker is None:
        return False
    cmd = [docker, "compose", "-f", str(COMPOSE)]
    for name in (".env", "secrets.env"):
        cmd += ["--env-file", str(COMPOSE.parent / name)]
    cmd += ["--profile", "execution-t3", "ps", "--format", "json"]
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return False
    running = set()
    for line in out.splitlines():
        try:
            running.add(json.loads(line).get("Service") or "")
        except ValueError:
            continue
    return bool({"execution-bridge", "nautilus"} & running)


class Nautilus25SmokeTest(unittest.TestCase):
    def test_25_instruments_offline_contract(self):
        """25-instrument offline: 25 random instruments, fake lifecycle via Nautilus — must PASS without containers."""
        out = run_harness("--offline", "--instruments", "25")
        self.assertIn("PASS LOCAL-INT-004 [offline-25]", out, f"25 offline failed: {out}")
        self.assertIn("25 instruments", out)
        self.assertIn("Nautilus trader", out)
        self.assertNotIn("FAIL", out)

    def test_25_instruments_live_gated(self):
        """25-instrument live: needs the execution-t3=fake stack; otherwise SKIP, never a silent contract-only PASS."""
        if not live_stack_up():
            self.skipTest("execution-t3 fake stack not up (no execution-bridge/nautilus) — "
                          "the harness answers a down stack with a contract-only PASS, so a green "
                          "live run here would prove nothing (P6-596)")
        out = run_harness("--live", "--instruments", "25")
        self.assertIn("PASS LOCAL-INT-004 [live-25]", out, f"25 live failed: {out}")
        self.assertNotIn("FAIL", out)

    def test_10_still_passes_after_extension(self):
        """Canonical 10-instrument must still pass (regression guard)."""
        out = run_harness("--offline", "--instruments", "10")
        self.assertIn("PASS LOCAL-INT-004 [offline-10]", out, f"10 offline failed: {out}")

    def test_pool_guard_fires_out_of_process(self):
        """P6-801: the pool guard the 25-instrument PASS rests on is enforced, and it is
        enforced out-of-process — the harness is never executed inside this test process.

        `PASS [offline-25]` means "the pool really had 25" only because the harness
        refuses n > pool. Asking for an impossible n proves that refusal still fires.
        """
        out = run_harness("--offline", "--instruments", "1000000", expect_ok=False)
        self.assertIn("pool too small", out, f"the pool guard no longer fires: {out}")


if __name__ == "__main__":
    unittest.main()
