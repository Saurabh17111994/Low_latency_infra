#!/usr/bin/env python3
"""M2-1: the one observability-provisioning path (provision-observability.sh).

The daily VM guide used to run `o2-provision.py` and `seed_alerts.py` bare — a
fresh VM without `O2_AUTH_BASIC` exited 2 from the first and the second never ran,
so the day ran blind. The wrapper refuses without `secrets.env`, sources the env
files, derives `O2_AUTH_BASIC` from `O2_PASSWORD`, and runs both under `set -e`.

Offline: a temp tree carries the real wrapper and recording stub provisioners.
"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "code/01_platform/04_scripts/provision-observability.sh"
GUIDE = ROOT / "docs/05_deployment/CLOUDPE_DAILY_VM.md"


class ObservabilityProvisionTest(unittest.TestCase):
    def _tree(self):
        """A temp repo tree with the real wrapper and recording stubs."""
        tmp = Path(tempfile.mkdtemp(prefix="m2-1-prov-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        (tmp / "code/01_platform/04_scripts").mkdir(parents=True)
        (tmp / "code/01_platform/01_docker").mkdir(parents=True)
        shutil.copy2(SCRIPT, tmp / "code/01_platform/04_scripts/provision-observability.sh")
        for name in ("o2-provision.py", "seed_alerts.py"):
            (tmp / "code/01_platform/04_scripts" / name).write_text(
                "import os, sys\n"
                "with open(os.environ['M21_CALLS'], 'a') as fh:\n"
                f"    fh.write('{name} ' + ' '.join(sys.argv[1:]) + ' auth=' + "
                "os.environ.get('O2_AUTH_BASIC', '<unset>') + '\\n')\n"
            )
        return tmp

    def _run(self, tmp):
        calls = tmp / "calls.txt"
        env = {**os.environ, "M21_CALLS": str(calls)}
        proc = subprocess.run(
            ["bash", str(tmp / "code/01_platform/04_scripts/provision-observability.sh")],
            capture_output=True, text=True, env=env, timeout=60)
        return proc, calls

    def test_refuses_without_secrets_env_and_names_step_two(self):
        tmp = self._tree()
        proc, calls = self._run(tmp)
        self.assertEqual(1, proc.returncode)
        self.assertIn("secrets.env", proc.stderr)
        self.assertIn("step 2", proc.stderr)
        self.assertFalse(calls.exists(), "no provisioner may run without secrets.env")

    def test_derives_auth_basic_and_runs_both_provisioners(self):
        tmp = self._tree()
        (tmp / "code/01_platform/01_docker/secrets.env").write_text("O2_PASSWORD=secret\n")
        proc, calls = self._run(tmp)
        self.assertEqual(0, proc.returncode, proc.stderr)
        text = calls.read_text()
        self.assertIn("o2-provision.py", text)
        self.assertIn("seed_alerts.py", text)
        self.assertNotIn("<unset>", text, "O2_AUTH_BASIC must be derived from O2_PASSWORD")

    def test_a_failed_first_provisioner_stops_the_chain(self):
        tmp = self._tree()
        (tmp / "code/01_platform/01_docker/secrets.env").write_text("O2_PASSWORD=secret\n")
        (tmp / "code/01_platform/04_scripts/o2-provision.py").write_text(
            "import sys\nsys.exit(1)\n")
        proc, calls = self._run(tmp)
        self.assertEqual(1, proc.returncode)
        text = calls.read_text() if calls.exists() else ""
        self.assertNotIn("seed_alerts.py", text,
                         "set -e must stop the chain when the first provisioner fails")


class GuideWrapperPinTest(unittest.TestCase):
    def test_the_guide_runs_the_one_wrapper(self):
        text = GUIDE.read_text()
        self.assertIn("provision-observability.sh", text)
        self.assertNotIn("`python3 code/01_platform/04_scripts/o2-provision.py`", text)

    def test_the_guide_has_no_shell_placeholders(self):
        # C18 joins the guide to the runbook set; `<...>` placeholders cannot be pasted.
        for block in re.findall(r"```bash\n(.*?)```", GUIDE.read_text(), re.S):
            for line in block.splitlines():
                if "<" in line and ">" in line:
                    self.fail(f"placeholder in a shell block: {line}")


if __name__ == "__main__":
    unittest.main()
