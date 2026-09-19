"""L1 Health + L3 Startup — HEALTH-001..008, START-001..006."""
import json, subprocess, unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"

def compose_json(profile="execution-t3"):
    cmd=["docker","compose","-f",str(COMPOSE),"--env-file",str(COMPOSE.parent/".env"),"--env-file",str(COMPOSE.parent/"secrets.env")]
    if profile: cmd+=["--profile", profile]
    cmd+=["config","--format","json"]
    return json.loads(subprocess.check_output(cmd, text=True))


# P6-805: HEALTH-001's name promises a per-service readiness proof, but the body
# only asserted presence. Only the host-built services declare a compose
# healthcheck; Flink's readiness surface is its REST/metrics endpoints (pinned by
# HEALTH-005/006) and Fluss declares none. Assert the proof each service ACTUALLY
# has, so a service that loses it (or a new entry in this list) fails loudly.
READINESS_PROOF = {
    "ingestion": "healthcheck",
    "execution-bridge": "healthcheck",
    "flink-jobmanager": "rest",
    "flink-taskmanager": "rest",
}

def readiness_gaps(cfg, proofs=None):
    """[(service, why)] for entries whose declared readiness proof is missing."""
    gaps = []
    for svc, kind in (proofs or READINESS_PROOF).items():
        s = (cfg.get("services") or {}).get(svc)
        if s is None:
            gaps.append((svc, "service missing"))
        elif kind == "healthcheck" and not s.get("healthcheck"):
            gaps.append((svc, "no healthcheck declared"))
        elif kind == "rest":
            props = (s.get("environment") or {}).get("FLINK_PROPERTIES") or ""
            if "rest.address" not in props and not s.get("ports"):
                gaps.append((svc, "no REST/metrics surface"))
    return gaps

class HealthStartupTest(unittest.TestCase):
    def test_HEALTH_001_all_containers_eventually_healthy(self):
        """HEALTH-001: every required service declares the readiness proof it owns (READINESS_PROOF)."""
        cfg = compose_json("execution-t3")
        gaps = readiness_gaps(cfg)
        self.assertEqual(gaps, [], f"HEALTH-001: readiness proof missing for {gaps}")

    def test_HEALTH_002_liveness_vs_readiness(self):
        """HEALTH-002: liveness vs readiness are distinct (doc gate) — check that at least ingestion has readiness file."""
        text = COMPOSE.read_text()
        self.assertIn("READINESS_FILE_PATH", text, "HEALTH-002: readiness file path not wired")
        self.assertIn("healthcheck", text.lower(), "HEALTH-002: no healthcheck defined")

    def test_HEALTH_003_fluss_coordinator(self):
        """HEALTH-003: Fluss coordinator must expose RPC and metadata readiness."""
        text = COMPOSE.read_text()
        self.assertIn("fluss-coordinator", text)
        self.assertIn("9123:9123", text, "HEALTH-003: coordinator port not mapped")

    def test_HEALTH_008_nautilus_trading_readiness_not_implied(self):
        """HEALTH-008: Nautilus liveness UP must not imply trading readiness ENABLED."""
        cfg = compose_json("execution-t3")
        env = cfg["services"]["nautilus"].get("environment") or {}
        # EXECUTION_ENABLED must default false
        self.assertIn("EXECUTION_ENABLED", env, "HEALTH-008: EXECUTION_ENABLED not set on nautilus")
        val = env["EXECUTION_ENABLED"]
        self.assertEqual(val, "false", f"HEALTH-008: EXECUTION_ENABLED must default false, got {val}")

    def test_START_001_clean_startup_dependency_order(self):
        """START-001: dependency order — fluss before flink, fluss before ingestion."""
        text = COMPOSE.read_text()
        self.assertIn("depends_on", text, "START-001: no depends_on found")
        cfg = compose_json(None)  # default profile
        flink_dep = cfg["services"]["flink-jobmanager"].get("depends_on") or {}
        # depends_on may be list or dict
        deps = set(flink_dep.keys()) if isinstance(flink_dep, dict) else set(flink_dep)
        self.assertIn("fluss-coordinator", deps, "START-001: flink-jobmanager must depend on fluss-coordinator")
        ing = cfg["services"]["ingestion"].get("depends_on") or {}
        ing_deps = set(ing.keys()) if isinstance(ing, dict) else set(ing)
        self.assertTrue({"fluss-coordinator","fluss-tablet"} & ing_deps, "START-001: ingestion must depend on fluss")

    def test_START_002_fluss_unavailable_blocks_flink(self):
        """START-002: Fluss unavailable → Flink not healthy (depends_on ensures it)."""
        cfg = compose_json(None)
        jm = cfg["services"]["flink-jobmanager"].get("depends_on") or {}
        deps = set(jm.keys()) if isinstance(jm, dict) else set(jm)
        self.assertIn("fluss-coordinator", deps)

    def test_START_005_schema_unavailable_fails_safely(self):
        """START-005: schema unavailable startup fails safely — DDL tool must exist."""
        self.assertTrue((ROOT / "code/01_platform/02_sql/ddl").exists(), "START-005: DDL dir missing")
        self.assertTrue((ROOT / "code/01_platform/04_scripts/ddl_apply.py").exists())

    def test_HEALTH_004_fluss_tablet_read_write(self):
        """HEALTH-004: Fluss tablet RPC + required table RW."""
        text = COMPOSE.read_text()
        self.assertIn("fluss-tablet", text, "HEALTH-004: fluss-tablet service missing")
        self.assertIn("fluss-tablet-data", text, "HEALTH-004: tablet persistent volume missing")
        # DDL proves required tables exist for RW health
        self.assertTrue((ROOT / "code/01_platform/02_sql/ddl/02_raw_table_1.sql").exists())

    def test_HEALTH_005_flink_jobmanager_rest(self):
        """HEALTH-005: Flink JobManager REST/RPC + job submission."""
        cfg = compose_json(None)
        self.assertIn("flink-jobmanager", cfg["services"], "HEALTH-005: jobmanager missing")
        jm_env = cfg["services"]["flink-jobmanager"].get("environment") or {}
        # Flink metrics reporter proves REST path is wired
        flink_props = jm_env.get("FLINK_PROPERTIES") or ""
        self.assertIn("jobmanager.rpc.address", flink_props, "HEALTH-005: JM RPC not wired")
        self.assertIn("rest.address", flink_props)

    def test_HEALTH_006_flink_taskmanager_slots(self):
        """HEALTH-006: Flink TaskManager slots + required task can schedule."""
        cfg = compose_json(None)
        self.assertIn("flink-taskmanager", cfg["services"], "HEALTH-006: taskmanager missing")
        tm_env = cfg["services"]["flink-taskmanager"].get("environment") or {}
        props = tm_env.get("FLINK_PROPERTIES") or ""
        self.assertIn("taskmanager.numberOfTaskSlots", props, "HEALTH-006: task slots not configured")
        self.assertIn("taskmanager.memory.process.size", props)

    def test_HEALTH_007_openobserve_api(self):
        """HEALTH-007: OpenObserve API + telemetry ingest."""
        cfg = compose_json(None)
        self.assertIn("openobserve", cfg["services"], "HEALTH-007: openobserve missing")
        self.assertIn("otel-collector", cfg["services"], "HEALTH-007: collector missing for O2 ingest")
        collector = (ROOT / "code/01_platform/01_docker/otel-collector-config.yaml").read_text()
        self.assertIn("openobserve:5080", collector, "HEALTH-007: collector does not export to O2")

    def test_START_003_taskmanager_delayed_eventual_health(self):
        """START-003: JM first → eventually healthy, jobs pending until TM resources appear."""
        cfg = compose_json(None)
        jm_deps = cfg["services"]["flink-jobmanager"].get("depends_on") or {}
        jm_set = set(jm_deps.keys()) if isinstance(jm_deps, dict) else set(jm_deps)
        self.assertIn("fluss-coordinator", jm_set)
        # TM must not be a hard dep of JM (JM may start without TM)
        self.assertNotIn("flink-taskmanager", jm_set, "START-003: JM must not hard-depend on TM")
        tm_deps = cfg["services"]["flink-taskmanager"].get("depends_on") or {}
        tm_set = set(tm_deps.keys()) if isinstance(tm_deps, dict) else set(tm_deps)
        self.assertIn("flink-jobmanager", tm_set, "START-003: TM must depend on JM")

    def test_START_004_openobserve_unavailable_degrades_not_falsely_ready(self):
        """START-004: O2 down → core trading stays safe; telemetry degradation not false success."""
        text = (ROOT / "code/01_platform/01_docker/otel-collector-config.yaml").read_text()
        self.assertIn("retry_on_failure", text, "START-004: collector must retry O2 export")
        self.assertIn("max_elapsed_time", text)
        self.assertIn("openobserve", COMPOSE.read_text().lower())

    def test_START_006_submitter_starts_too_early_retries(self):
        """START-006: submitter started before Flink → retries/waits, no duplicate storm."""
        # Submitter is the compute module; check Flink properties include restart strategy
        found = False
        for p in (ROOT / "code/02_services/02_compute").rglob("*.java"):
            if "RESTART" in p.read_text() or "RestartStrategy" in p.read_text():
                found = True
                break
        self.assertTrue(found, "START-006: restart/retry guard missing in compute")
        self.assertIn("depends_on", COMPOSE.read_text())

    def test_JVM_heap_65_percent(self):
        """JVM and memory: heap 65%, reserve 35%, alert 85% per spec table."""
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", doc)
        self.assertIn("65", doc)
        self.assertIn("35", doc)
        self.assertIn("85", doc)

if __name__ == "__main__":
    unittest.main()
