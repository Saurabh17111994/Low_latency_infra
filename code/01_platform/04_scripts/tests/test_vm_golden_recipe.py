"""CHG-362 guards for the CloudPe daily-VM recipe (script + profile + guide).

The daily-VM flow is profile + docs + one build script, so these are offline
pins over the three artifacts: a later edit must not quietly drop the
after-close EOD, the stop gate, the profile-driven unit, or the daily commands.

Offline only: reads the files, never runs docker or the script.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
SCRIPT = ROOT / "code/01_platform/04_scripts/vm-golden-build.sh"
DOC = ROOT / "docs/05_deployment/CLOUDPE_DAILY_VM.md"
VM_ENV = ROOT / "code/01_platform/01_docker/.env.vm.example"
PROJECT_IMAGES = (
    "01_docker-ingestion", "01_docker-compute", "01_docker-nautilus",
    "01_docker-execution-bridge", "01_docker-execution-gateway",
    "01_docker-ddl-apply", "01_docker-eod-controller",
)


class VmGoldenRecipeTests(unittest.TestCase):
    def test_profile_pins_the_post_close_eod_and_the_stop_gate(self):
        text = VM_ENV.read_text()
        self.assertRegex(text, r"(?m)^ALLOW_FRESH=1$")
        self.assertRegex(text, r"(?m)^EOD_AT=15:45$")
        self.assertRegex(text, r"(?m)^EOD_OFFLOAD=lake$")
        self.assertRegex(text, r"(?m)^DAY_STOP_REQUIRE_EOD=1$")

    def test_unit_reads_the_profile_and_never_bakes_secrets(self):
        text = SCRIPT.read_text()
        self.assertIn("trading-eod.service", text)
        self.assertIn("EnvironmentFile=$ENV_DIR/.env.vm", text)
        self.assertIn("eod_schedule.py", text)
        self.assertIn("NEVER writes secrets", text)
        for secret_key in ("AWS_SECRET_ACCESS_KEY", "ARROW_TOTP_KEY",
                           "ARROW_APP_SECRET", "EOD_MASTER_KEY"):
            self.assertNotIn(secret_key, text,
                             "the golden build must never handle secrets")

    def test_script_checks_the_project_image_set(self):
        text = SCRIPT.read_text()
        for image in PROJECT_IMAGES:
            self.assertIn(image, text,
                          "the build script must refuse a snapshot without the "
                          "project image set (compose would otherwise build)")
        self.assertIn("--check", text)

    def test_guide_keeps_the_daily_commands_and_the_safe_to_destroy_rule(self):
        text = DOC.read_text()
        self.assertIn('make day ARGS="start"', text)
        self.assertIn('make day ARGS="stop"', text)
        self.assertIn("tiering-start.sh", text)
        self.assertIn("secrets.env", text)
        self.assertIn("15:45", text)


if __name__ == "__main__":
    unittest.main()
