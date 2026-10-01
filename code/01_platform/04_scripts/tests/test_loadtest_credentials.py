#!/usr/bin/env python3
"""Loadtest credential/env contract (CHG-511).

The fake-broker loadtest starts the real ingestion JVM, whose current config
requires ``ARROW_APP_SECRET`` + ``ARROW_PASSWORD`` + ``ARROW_TOTP_KEY`` and
``DEPLOYMENT_ENV``. The script sources the 0600 secrets file; ``source`` alone
keeps the values shell-local, so the JVM failed its required-key validation
and the feed never started. These pins hold the fix: the three Arrow keys are
exported to the JVM child, the file's other secrets stay shell-local, and
``DEPLOYMENT_ENV=dev`` is in the JVM env block.

Auto-discovered by gate step 3 (``test_*.py``, pytest from the repo root).
"""

import pathlib
import re
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "code" / "01_platform" / "04_scripts" / "loadtest-run.sh"


def extract_function(name: str) -> str:
    """Lift ``name() { ... }`` out of the script (same idiom as the shell guards)."""
    text = SCRIPT.read_text()
    match = re.search(rf"^{name}\(\) \{{.*?^\}}", text, re.S | re.M)
    assert match, f"{name}() not found in {SCRIPT}"
    return match.group(0)


class ResolveArrowCredentialsTests(unittest.TestCase):
    def test_arrow_secrets_reach_the_jvm_child(self):
        function = extract_function("resolve_arrow_credentials")
        with tempfile.TemporaryDirectory() as tmp:
            secrets = pathlib.Path(tmp) / "secrets.env"
            secrets.write_text(
                "ARROW_APP_SECRET=app-secret\n"
                "ARROW_PASSWORD=pw\n"
                "ARROW_TOTP_KEY=totp\n"
                "AWS_ACCESS_KEY_ID=not-exported\n")
            secrets.chmod(0o600)
            script = (
                'fail() { echo "$*" >&2; exit 1; }\n'
                f"SECRETS_FILE={secrets}\n"
                f"{function}\n"
                "resolve_arrow_credentials >/dev/null\n"
                # visible to a child process (the JVM inherits the env)
                "bash -c 'echo \"$ARROW_APP_SECRET|$ARROW_PASSWORD|$ARROW_TOTP_KEY\"'\n"
                # and only the three Arrow keys leave the shell
                "export -p | grep -c 'declare -x AWS_ACCESS_KEY_ID' || true\n"
            )
            proc = subprocess.run(["bash", "-c", script],
                                  capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            lines = proc.stdout.strip().splitlines()
            self.assertEqual(lines[0], "app-secret|pw|totp",
                             "the three Arrow keys must reach the JVM child")
            self.assertEqual(lines[1].strip(), "0",
                             "non-Arrow secrets must stay shell-local")


class LoadtestEnvBlockTests(unittest.TestCase):
    def test_deployment_env_pinned_in_jvm_env_block(self):
        text = SCRIPT.read_text()
        block = re.search(r"LOG_DIR=.*?\bjava\b", text, re.S)
        self.assertIsNotNone(block, "JVM env block not found in loadtest-run.sh")
        self.assertIn('DEPLOYMENT_ENV="dev"', block.group(0),
                      "the current IngestionConfig requires DEPLOYMENT_ENV; "
                      "the loadtest JVM env block must pin it to dev")


if __name__ == "__main__":
    unittest.main()
