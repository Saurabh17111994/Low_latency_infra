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
        self.assertRegex(text, r"(?m)^EOD_STATE_FILE=/var/lib/trading/eod-state\.json$",
                         "M2-2: the slot state must be durable, not in /tmp")
        self.assertRegex(text, r"(?m)^EOD_RETRY_DELAY_SEC=\d+$")

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

    def test_profile_selects_the_compose_eod_runner(self):
        text = VM_ENV.read_text()
        self.assertRegex(text, r"(?m)^EOD_RUNNER=compose$",
                         "the fresh VM has no host JDK/m2, so the EOD controller must run "
                         "in the eod-controller compose service")

    def test_check_proves_the_in_image_toolchain_before_the_snapshot(self):
        text = SCRIPT.read_text()
        for probe in (
            "--entrypoint java",
            "test -f /app/probe/FlussReadLagProbe.class'",
            "test -f /app/code/01_platform/04_scripts/eod_controller.py",
            "test -d /opt/ddl-apply/m2/repository'",
            "eod_controller.py --help",
            "^EOD_RUNNER=compose$",
        ):
            self.assertIn(probe, text,
                          f"--check must prove {probe!r} before the volume is snapshotted")
        self.assertIn("python3 tzdata", text,
                      "the golden build must install the host scheduler toolchain "
                      "(python3 + tzdata for zoneinfo)")

    def test_profile_records_the_daily_dev_full_operating_mode(self):
        """H4-2 (P1-3): the daily VM intentionally runs the dev multi-socket universe.

        The profile left DEPLOYMENT_ENV/UNIVERSE unset, so the effective mode came
        from day_run defaults (dev + full, 2433 tokens, 3 sockets) — a working mode
        that read as an accident. The decision is now recorded in the profile and the
        guide; the pre-existing production refusal (CHG-320) stays.
        """
        text = VM_ENV.read_text()
        self.assertRegex(text, r"(?m)^DEPLOYMENT_ENV=dev$")
        self.assertRegex(text, r"(?m)^UNIVERSE=full$")
        self.assertIn("2433", text)
        self.assertIn("three approved dev sockets", text)
        self.assertIn("CHG-320", text)

    def test_profile_enables_the_candle_and_strategy_chain(self):
        """A fresh VM must not silently boot with candles/signals off.

        The compose defaults are `MULTITF_ENABLED=false` and empty/false for
        the strategy flags, and the daily profile is what makes a fresh VM
        need no flags — but `.env.example` (the `.env` the golden build
        copies) carries none of them. Without these pins the day writes no
        candles (the EOD's `EOD_TABLES=candle_features` archives nothing) and
        runs no strategies; the dev `.env` calls out the same silent-off
        contract (2026-09-27 turn-on, Q3(a)).
        """
        text = VM_ENV.read_text()
        for pinned in (
            "MULTITF_ENABLED=true",
            "STRATEGY_HOST_ENABLED=true",
            "STRATEGIES=n7-range-breakout-v1",
            "EXECUTION_INTENT_ENABLED=true",
        ):
            self.assertRegex(text, r"(?m)^" + re.escape(pinned) + r"$",
                             f"the daily VM profile must pin {pinned} "
                             "(compose defaults are false/empty)")

    def test_guide_repeats_the_daily_universe(self):
        text = DOC.read_text()
        self.assertIn("2433", text)
        self.assertIn("UNIVERSE=full", text)
        self.assertIn("DEPLOYMENT_ENV=dev", text)

    def test_check_proves_the_runtime_artifacts_the_vm_boots_with(self):
        """CHG-493: images are not enough — the instrument manifest tree lives
        outside the repo, and compute.jar + the plugin jars are gitignored
        (rsync-only). A snapshot that passes `--check` without them dies at
        09:15 on the VM, so the check must fail before the snapshot.
        """
        text = SCRIPT.read_text()
        for probe in (
            "Arrow_broker/instruments/cash_stocks",
            "target/compute.jar",
            "fluss-plugins/iceberg",
            "flink-plugins/dstl-dfs",
        ):
            self.assertIn(probe, text,
                          f"--check must prove {probe!r} before the snapshot")

    def test_guide_transfers_the_runtime_artifacts_not_just_the_repo(self):
        """CHG-493: a repo-only transfer misses the manifest tree; a git clone
        cannot carry the gitignored jars. The guide must name both."""
        text = DOC.read_text()
        self.assertIn("Arrow_broker", text)
        self.assertIn("compute.jar", text)

    def test_guide_keeps_the_daily_commands_and_the_safe_to_destroy_rule(self):
        text = DOC.read_text()
        self.assertIn('make day ARGS="start"', text)
        self.assertIn('make day ARGS="stop"', text)
        self.assertIn("tiering-start.sh", text)
        self.assertIn("secrets.env", text)
        self.assertIn("15:45", text)


if __name__ == "__main__":
    unittest.main()
