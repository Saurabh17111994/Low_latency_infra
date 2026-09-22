import unittest
from pathlib import Path
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[4]


class TieringRestartGuardTest(unittest.TestCase):
    def test_tiering_submit_and_guard_follow_fluss_1_0_restart_ownership(self):
        script = (ROOT / "code" / "01_platform" / "04_scripts" / "tiering-start.sh").read_text()
        # 2026-09-23 (Fluss 1.0.0 upgrade): from wave 8 until this change the submit
        # pinned the strategy with three -D flags (fixed-delay, unbounded attempts,
        # 30 s delay). Fluss 1.0.0's FlussLakeTiering now sets
        # RestartStrategyOptions.RESTART_STRATEGY = exponential-delay inside the
        # Configuration it passes to getExecutionEnvironment(...), which overrides the
        # submitter — so the flags were removed as dead weight.
        # The guard's job is unchanged: prove the effective strategy cannot give up
        # permanently (P6-250), verified against the parsed execution-config (P6-249)
        # rather than a raw-body grep. exponential-delay has no attempt cap, so it is
        # accepted alongside fixed-delay.
        self.assertNotIn("-Drestart-strategy.", script, "the dead restart-strategy flags are back")
        # The prefix ban above covers any re-added flag, including a capped one.
        self.assertIn("Do NOT re-add the flags", script,
                      "the explanation for the removed flags is gone")
        self.assertIn("exponential-delay", script)
        self.assertIn("RestartStrategyOptions.RESTART_STRATEGY", script)
        self.assertIn("tiering_has_restart_strategy", script)
        self.assertIn("without an accepted restart strategy", script)
        self.assertIn('json.load(sys.stdin).get("execution-config")', script)

    def test_tm_kill_drill_serializes_shared_cluster_ownership(self):
        script = (ROOT / "code" / "01_platform" / "04_scripts" / "tm-kill-full-load.sh").read_text()
        self.assertIn('C2_MODE="${C2_MODE:-main}"', script)
        self.assertIn('C2_MODE=smoke', script)
        # 2026-09-02 rework (doctrine comment at the top of the drill; CHG-120):
        # smoke is the SAME drill compressed WITH the real SIGKILL. The no-kill
        # smoke was removed because it was blind to the post-kill catch-up
        # restart it exists to catch, so these four assertions pin the current
        # contract instead of the retired no-kill one.
        self.assertIn("smoke = compressed main drill (kill INCLUDED)", script)
        self.assertIn("PASS — smoke drill green", script)
        self.assertIn("source did not advance after recovery", script)
        self.assertIn("SignalJob output did not advance after recovery", script)
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
        # 2026-09-15 rework (wave 27, CHG-167): the second pin named the retired
        # one-liner (`any(j.get("state") == "RUNNING"`). The intent it protected is
        # unchanged and is now pinned against the rewrite: the live leg asks Flink
        # which jobs are RUNNING and requires one of the pre-kill ids specifically
        # (a freshly submitted job is not a restore). The guard message above is
        # restored verbatim in the script.
        self.assertIn("curl and python3 are required for the recovery probe", quick)
        self.assertIn('if job.get("state") == "RUNNING"', quick)
        self.assertIn('grep -qxF "${jid}" "${pre_file}"', quick)

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
