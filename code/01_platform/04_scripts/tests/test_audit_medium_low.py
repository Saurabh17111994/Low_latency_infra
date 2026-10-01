"""Audit guards (2026-10-01, MEDIUM/LOW batch) — static, offline, reads files only.

One test class per finding; each fails against the pre-fix tree (red leg) and
pins the fixed property (green leg):

  1. run-ingestion-full.sh exports LOG_DIR so the log4j JSON journal lands in
     the repo tree instead of the absent /data/ingestion/logs default.
  2. the soak tools no longer default to the dead code/logs/ingestion.json;
     they resolve the newest ingestion-*.json under ${SOAK_JOURNAL_DIR}.
  3. POSITION_STATE_TABLE (a knob no code reads) is gone from the infra files.
  4. .env.example documents the code-read INSTRUMENT_MANIFEST_VERSION.
  5. prod_vms.example.json carries no personal key path.
  6. evidence defaults are repo-anchored, not cwd-relative (clean_break_drill.py,
     disaster_drills.py, stage-capture.sh).
  7. the capture tool's runtime artifact is gitignored.
  8. ComputeAlertLogs accepts an optional http(s):// collector scheme (P2-191).

No process is started, no cluster/stack is touched, no evidence is written.
"""

import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "code" / "01_platform" / "04_scripts"
DOCKER = ROOT / "code" / "01_platform" / "01_docker"
COMPUTE = (ROOT / "code" / "02_services" / "02_compute" / "src" / "main" / "java"
           / "com" / "trading" / "compute")


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class RunIngestionFullLogDirTest(unittest.TestCase):
    def test_log_dir_export_defaults_under_repo_logs(self):
        text = read(ROOT / "code" / "run-ingestion-full.sh")
        self.assertIn(
            'export LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs/ingestion}"', text,
            "the launcher must export LOG_DIR (log4j default /data/ingestion/logs "
            "does not exist on the dev host, so the JSON journal is lost)")


class SoakJournalDefaultsTest(unittest.TestCase):
    TOOLS = ("soak-monitor.sh", "soak-headroom.sh", "soak-reconnect-loop.sh")

    def test_no_tool_defaults_to_the_dead_code_logs_path(self):
        for name in self.TOOLS:
            text = read(SCRIPTS / name)
            self.assertNotIn("code/logs/ingestion.json", text,
                             f"{name}: the writer emits ingestion-<HOST>-<VM_ID>.json")

    def test_tools_resolve_the_soak_journal_dir(self):
        for name in self.TOOLS:
            text = read(SCRIPTS / name)
            self.assertIn("${SOAK_JOURNAL_DIR", text,
                          f"{name}: must reference the soak overlay's journal dir")
            self.assertIn("ingestion-*.json", text,
                          f"{name}: must resolve the newest per-host journal")


class DeadPositionStateKnobTest(unittest.TestCase):
    FILES = (DOCKER / ".env.example", DOCKER / "docker-compose.yml",
             DOCKER / "docker-stack.yml")

    def test_position_state_table_knob_is_gone(self):
        for f in self.FILES:
            self.assertNotIn("POSITION_STATE_TABLE", read(f),
                             f"{f.name}: dead knob (no reader) must not be set")


class EnvExampleManifestVersionTest(unittest.TestCase):
    def test_manifest_version_documented(self):
        text = read(DOCKER / ".env.example")
        self.assertRegex(text, r"(?m)^INSTRUMENT_MANIFEST_VERSION=1\s*$",
                         "InstrumentManifestLoader reads this key; R-247 makes a "
                         "refreshed CSV a NEW approved version")


class ProdVmsTemplateTest(unittest.TestCase):
    def test_no_personal_key_path(self):
        text = read(SCRIPTS / "prod_vms.example.json")
        self.assertNotIn("/home/saurabh", text)
        self.assertIn("/home/<user>/.ssh/id_ed25519", text)
        json.loads(text)  # the placeholder must keep the template valid JSON


class EvidenceDefaultsTest(unittest.TestCase):
    def test_clean_break_default_is_repo_anchored(self):
        text = read(SCRIPTS / "clean_break_drill.py")
        self.assertNotIn("os.getcwd()", text)
        self.assertRegex(
            text,
            r'--out", default=os\.path\.join\(REPO_ROOT, "logs", "clean-break"\)',
            "clean_break_drill.py --out default must sit under the repo root")

    def test_disaster_drills_evidence_dir_is_absolute(self):
        text = read(SCRIPTS / "disaster_drills.py")
        self.assertRegex(
            text,
            r'EVIDENCE_DIR = os\.path\.join\(REPO_ROOT, "logs", "disaster-drills"\)')

    def test_stage_capture_out_dir_is_repo_anchored(self):
        text = read(SCRIPTS / "stage-capture.sh")
        self.assertIn(
            'OUT_DIR="${OUT_DIR:-$REPO_ROOT/logs/tracker-14/', text,
            "stage-capture.sh must derive the repo root from the script location")
        self.assertIn(
            'REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"', text)


class GitignoreCaptureArtifactTest(unittest.TestCase):
    def test_capture_artifact_is_ignored(self):
        text = read(ROOT / ".gitignore")
        self.assertIn(
            "code/02_services/01_ingestion/go-bridge/marketdata-capture.jsonl", text,
            "the capture tool's default output is runtime data, never committed")


class ComputeAlertLogsSchemeTest(unittest.TestCase):
    def test_collector_scheme_is_optional(self):
        text = read(COMPUTE / "telemetry" / "ComputeAlertLogs.java")
        self.assertIn("(https?://)?", text,
                      "P2-191: an optional http(s):// scheme must validate")
        self.assertNotIn("TODO: support https:// collector endpoint", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
