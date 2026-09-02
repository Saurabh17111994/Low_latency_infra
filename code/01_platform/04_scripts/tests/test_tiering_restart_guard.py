import unittest
from pathlib import Path
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[4]


class TieringRestartGuardTest(unittest.TestCase):
    def test_tiering_submit_pins_and_verifies_fixed_delay_restart(self):
        script = (ROOT / "code" / "01_platform" / "04_scripts" / "tiering-start.sh").read_text()
        self.assertIn("-Drestart-strategy.type=fixed-delay", script)
        self.assertIn("-Drestart-strategy.fixed-delay.attempts=3", script)
        self.assertIn("-Drestart-strategy.fixed-delay.delay=30 s", script)
        self.assertIn("tiering_has_restart_strategy", script)
        self.assertIn("without fixed-delay restart", script)
        self.assertIn("fixed[- ]delay", script)

    def test_tm_kill_drill_serializes_shared_cluster_ownership(self):
        script = (ROOT / "code" / "01_platform" / "04_scripts" / "tm-kill-full-load.sh").read_text()
        self.assertIn('C2_MODE="${C2_MODE:-main}"', script)
        self.assertIn('C2_MODE=smoke', script)
        self.assertIn("smoke gate — no TaskManager kill", script)
        self.assertIn("smoke gate completed only", script)
        self.assertIn("smoke source did not advance", script)
        self.assertIn("smoke output did not advance", script)
        self.assertIn("flock -n 9", script)
        self.assertIn("multiple Fluss Lake Tiering jobs detected", script)
        self.assertIn("cancel_preflight_tiering", script)
        self.assertIn("before table purge", script)
        self.assertIn("Babysitter Positions observer", script)
        self.assertIn("duplicate Babysitter auxiliary jobs detected", script)
        self.assertIn("c2_progress.py", script)
        self.assertIn("POST_READ_INCREASES", script)
        self.assertIn("POST_WRITE_INCREASES", script)
        self.assertIn('"INITIALIZING"', script)
        self.assertIn('"RECONCILING"', script)
        self.assertIn('"CANCELING"', script)

        quick = (ROOT / "code" / "01_platform" / "04_scripts" / "chaos" / "chaos-02-tm-kill.sh").read_text()
        self.assertIn("curl and python3 are required for the recovery probe", quick)
        self.assertIn("any(j.get(\"state\") == \"RUNNING\"", quick)

    def test_warmup_gate_polls_metric_report_grace_before_failing(self):
        # 2026-09-01 (attempt 20260901-125337): on a freshly restarted
        # TaskManager no checkpoint completed inside the 45s warm-up, so the
        # accumulated vertex metrics were all still unreported and the single
        # warm-up snapshot failed a healthy, fully-fed pipeline. The gate
        # must poll with a bounded grace window instead of one snapshot.
        script = (ROOT / "code" / "01_platform" / "04_scripts" / "tm-kill-full-load.sh").read_text()
        self.assertIn('WARMUP_GRACE_S="${WARMUP_GRACE_S:-120}"', script)
        self.assertIn("warm-up metric gate", script)
        self.assertIn("report grace", script)
        self.assertIn("WARMUP_DEADLINE", script)
        # still fail-closed: the fatal path remains reachable after grace
        self.assertIn("raw-path progress metric is not advancing", script)
        # and the launcher must pin the working directory to the repo root
        # (ingestion's ConfigGuard walks UP from cwd to find code/)
        self.assertIn('cd "$ROOT"', script)

    def test_compute_keeps_fluss_out_of_the_shaded_artifact(self):
        pom = ET.parse(ROOT / "code" / "02_services" / "02_compute" / "pom.xml")
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        scopes = {}
        for dependency in pom.findall(".//m:dependencies/m:dependency", ns):
            artifact = dependency.findtext("m:artifactId", namespaces=ns)
            scope = dependency.findtext("m:scope", namespaces=ns) or "compile"
            if artifact in {"fluss-flink-2.2", "fluss-client"}:
                scopes[artifact] = scope
        self.assertEqual(
            scopes,
            {"fluss-flink-2.2": "provided", "fluss-client": "provided"},
        )

    def test_pipeline_rejects_split_fluss_classpath_before_submit(self):
        lib = (ROOT / "code" / "01_platform" / "04_scripts" / "pipeline-lib.sh").read_text()
        preflight = lib[lib.index("pipeline_preflight() {"):lib.index("# ---------- faketool")]
        self.assertIn("pipeline_validate_compute_jar || return 1", preflight)
        self.assertIn("pipeline_validate_compose_bind_sources || return 1", preflight)
        self.assertIn("org\\/apache\\/fluss", lib)


if __name__ == "__main__":
    unittest.main()
