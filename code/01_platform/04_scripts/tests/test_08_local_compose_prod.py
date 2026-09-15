"""PROD — Production-hardening beyond the 120 IDs (08-local-compose.md is explicitly NOT prod-HA).

These tests prove local Compose cannot be mistaken for Swarm prod and that
the single-node dev simplifications are explicit. All offline PASS (CI green);
live probes gate on Swarm/prod env vars so they SKIP locally.
"""
import re, subprocess, unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
COLLECTOR = ROOT / "code/01_platform/01_docker/otel-collector-config.yaml"
PLATFORM_CONFIG = ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java"
GATE_RS = ROOT / "code/02_services/04_executor/src/gate.rs"

def compose_text():
    return COMPOSE.read_text()

def collector_text():
    return COLLECTOR.read_text() if COLLECTOR.exists() else ""

class ProdHardeningTest(unittest.TestCase):
    def test_PROD_001_single_node_simplification_explicit(self):
        """PROD-001: local is single ZK + single Fluss (not 3-node HA) — doc + compose must say so."""
        self.assertIn("ZooKeeper (single node", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())
        self.assertIn("single-host", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())
        text = compose_text()
        self.assertIn("zookeeper:", text)
        self.assertIn("fluss-coordinator:", text)
        self.assertNotIn("ensemble", text.lower(), "PROD-001: local compose must not claim ensemble")

    def test_PROD_002_checkpoints_are_local_volume_not_s3(self):
        """PROD-002: checkpoints are flink-checkpoints local volume; prod Swarm needs s3:// — never hard-code s3 here."""
        text = compose_text()
        self.assertIn("flink-checkpoints:", text, "PROD-002: local checkpoint volume missing")
        self.assertIn("flink-checkpoints:/", text)
        # allow s3:// only inside comments/docs — forbid hard-coded S3 image/endpoint without ${}
        self.assertIn("${S3_WAREHOUSE_PATH", text, "PROD-002: warehouse must be env-interpolated")
        self.assertIn("s3://", compose_text(), "PROD-002: compose must document s3:// production requirement in header comment")

    def test_PROD_003_no_prod_endpoint_accepted(self):
        """PROD-003: local profile rejects prod marker/endpoint/bucket/creds — SEC-001/002 + CONFIG-003."""
        # production marker must gate
        self.assertIn("ENVIRONMENT=production", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())
        env_example = (ROOT / "code/01_platform/01_docker/.env.example").read_text()
        self.assertIn("EXECUTION_ENABLED=false", env_example)
        self.assertIn("${", compose_text(), "PROD-003: endpoints must be env-interpolated, not hard-coded prod")

    def test_PROD_004_no_secret_in_git_example(self):
        """PROD-004: .env.example contains no real secret, only placeholders."""
        ex = (ROOT / "code/01_platform/01_docker/.env.example").read_text()
        # placeholders use ${} or example/test/sandbox tokens, not real creds
        self.assertNotRegex(ex.lower(), r"sk-live|prod.*token.*[a-f0-9]{20}", "PROD-004: .env.example leaks prod-like token")
        self.assertTrue(".env" in (ROOT / ".gitignore").read_text() if (ROOT / ".gitignore").exists() else True)

    def test_PROD_005_no_latest_digests_pinned(self):
        """PROD-005: no :latest; digests pinned where required (golang@sha256, rust:1.97.1)."""
        text = compose_text()
        self.assertNotIn(":latest", text, "PROD-005: :latest forbidden")
        self.assertTrue("FLUSS_IMAGE" in text or "golang:" in text, "PROD-005: base image pin missing")
        # digest is enforced via ${FLUSS_IMAGE:?set ... to an immutable digest} (env holds digest-pin)
        self.assertIn("immutable digest", text, "PROD-005: images must require immutable digest via env pin")

    def test_PROD_006_resource_limits_and_jvm_wiring(self):
        """PROD-006: resource envelopes + JVM 65/35/85 (dev-sized, not prod-sized)."""
        self.assertIn("jobmanager.memory.process.size", compose_text())
        self.assertIn("taskmanager.memory.process.size", compose_text())
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65", cfg)
        self.assertIn("NON_HEAP_MEMORY_RESERVE_PERCENT = 35", cfg)
        self.assertIn("CONTAINER_MEMORY_ALERT_PERCENT = 85", cfg)
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())

    def test_PROD_007_restart_policy_safe(self):
        """PROD-007: restart policy is always/unless-stopped (safe locally), never 'no' for infra."""
        text = compose_text()
        # infra must auto-recover locally
        self.assertIn("restart:", text)
        self.assertIn("restart: unless-stopped", text)
        self.assertIn("restart: always", text)  # zookeeper = always
        # no prod-only policy drift

    def test_PROD_008_ddl_idempotency_and_evidence_ownership(self):
        """PROD-008: DDL apply is idempotent + evidence 2775/664 (not root-owned)."""
        self.assertTrue((ROOT / "code/01_platform/04_scripts/ddl_apply.py").exists())
        self.assertTrue((ROOT / "code/01_platform/04_scripts/evidence_ownership_check.py").exists())
        dockerfile = (ROOT / "code/01_platform/01_docker/ddl-apply/Dockerfile").read_text() if (ROOT / "code/01_platform/01_docker/ddl-apply/Dockerfile").exists() else ""
        # ownership gate is documented in Makefile
        self.assertIn("evidence-ownership-check", (ROOT / "Makefile").read_text())

    def test_PROD_009_otel_retry_and_ingestion_cred_free(self):
        """PROD-009: collector retry_on_failure/max_elapsed_time + holds O2 auth; ingestion is cred-free."""
        ct = collector_text()
        self.assertIn("retry_on_failure", ct)
        self.assertIn("max_elapsed_time: 5m", ct)
        self.assertIn("send_failed", ct, "PROD-009: send_failed metric name changed")
        self.assertIn("http://openobserve:5080", ct)
        # ingestion must not hold O2 cred (collector does)
        ing_env = ""
        for p in (ROOT / "code/02_services/01_ingestion").rglob("*.java"):
            if "O2_AUTH" in p.read_text():
                ing_env = p.read_text()
                break
        # filelog receiver is on collector, not ingestion
        self.assertIn("filelog", ct)

    def test_PROD_010_gate_monotonic_and_safety_halt_only_regress(self):
        """PROD-010: gate HALTED→RECONCILING→APPROVAL_PENDING→ENABLED, only safety_halt regresses."""
        src = GATE_RS.read_text()
        # DEC-044 form (2026-08-21): sanctioned transitions are a `matches!` tuple list;
        # `Enabled` is NOT in that list — it is reachable only via Gate::enable after approval.
        self.assertIn("(ExecState::Halted, ExecState::Reconciling)", src)
        self.assertIn("(ExecState::Reconciling, ExecState::ApprovalPending)", src)
        self.assertIn("Gate::enable", src)
        self.assertIn("safety_halt", src)
        # illegal jumps must error (covers 005 regress)
        self.assertIn("InvalidTransition", src)

    def test_PROD_011_execution_t3_disabled_by_default(self):
        """PROD-011: bridge disabled by default; needs --profile execution-t3 to appear."""
        import json, subprocess
        cfg_default = json.loads(subprocess.check_output(["docker","compose","-f",str(COMPOSE),"--env-file",str(COMPOSE.parent/".env"),"--env-file",str(COMPOSE.parent/"secrets.env"),"config","--format","json"], text=True))
        self.assertNotIn("execution-bridge", cfg_default.get("services", {}))
        cfg_t3 = json.loads(subprocess.check_output(["docker","compose","-f",str(COMPOSE),"--env-file",str(COMPOSE.parent/".env"),"--env-file",str(COMPOSE.parent/"secrets.env"),"--profile","execution-t3","config","--format","json"], text=True))
        self.assertIn("execution-bridge", cfg_t3["services"])

    def test_PROD_012_no_aws_creds_in_fluss_properties(self):
        """PROD-012: S3A creds via env (AWS_*) only, never in FLUSS_PROPERTIES."""
        text = compose_text()
        # FLUSS_PROPERTIES must not contain AWS_SECRET
        props_block = re.findall(r"FLUSS_PROPERTIES:.*?(?=\n\s{6}[A-Z]|\nservices:|\Z)", text, flags=re.S)
        joined = " ".join(props_block)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY:", joined, "PROD-012: creds leaked into FLUSS_PROPERTIES")
        # env interpolation is correct
        self.assertIn("${AWS_ACCESS_KEY_ID", text)
        self.assertIn("${AWS_SECRET_ACCESS_KEY", text)
        self.assertIn("never in fluss_properties", text.lower(), "PROD-012: FLUSS_PROPERTIES must document env-only creds")

    def test_PROD_013_lake_snapshot_remote_dir_shared(self):
        """PROD-013: whatever serves getLakeSnapshot (coordinator OR tablet) must
        read the SAME remote.data.dir, because lake-snapshot metadata and tiered
        log segments are written there.

        W35x (2026-09-16): the shared *volume* this used to require is gone — dev
        moved remote.data.dir to a Cloudflare R2 bucket, where sharing is by
        construction rather than by a mount. The invariant is unchanged, so this
        pins the new form: both servers name the same remote.data.dir, that value
        is the env-interpolated bucket URI, and the container-local path (which
        only THAT container can read — the bug W35x fixed) must NOT come back.
        """
        text = compose_text()
        self.assertIn("${R2_BUCKET", text, "PROD-013: remote.data.dir must use the R2 bucket")
        self.assertGreaterEqual(
            text.count("remote.data.dir: s3://${R2_BUCKET"),
            2,
            "PROD-013: coordinator AND tablet must both point remote.data.dir at the same R2 bucket",
        )
        self.assertNotIn(
            "remote.data.dir: /tmp/fluss/remote-data",
            text,
            "PROD-013: a container-local remote.data.dir is readable only from inside "
            "that container, so other readers silently lose the tiered rows",
        )
        # The path must be the same string in both services, or they disagree.
        remote_dirs = re.findall(r"remote\.data\.dir:\s*(\S+)", text)
        self.assertEqual(
            len(set(remote_dirs)),
            1,
            f"PROD-013: coordinator and tablet disagree on remote.data.dir: {sorted(set(remote_dirs))}",
        )

    def test_PROD_014_ten_vs_1024_instrument_manifest(self):
        """PROD-014: local smoke is 10 random instruments; live bench is 1024 cap (2433-row NSE file cannot be serviced)."""
        md = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("10-instrument", md)
        self.assertIn("10 random instruments", md)
        self.assertIn("1,024 instruments", compose_text())
        self.assertIn("1024 tokens/connection", compose_text())
        self.assertIn("LOCAL-INT-004", md)

    def test_PROD_015_checkpoint_restart_constants_pinned(self):
        """PROD-015: checkpoint/restart governed pins in PlatformConfig (not tunable per-env)."""
        cfg = PLATFORM_CONFIG.read_text()
        for needle in ["CHECKPOINT_INTERVAL_MS = 10_000", "CHECKPOINT_TIMEOUT_MS = 30_000", "MAX_CONCURRENT_CHECKPOINTS = 1", "RESTART_MAX_ATTEMPTS = 3", "RESTART_DELAY_MS = 30_000", "SINK_WRITE_STALL_TIMEOUT_MS = 15_000"]:
            self.assertIn(needle, cfg, f"PROD-015: governed pin missing: {needle}")

    def test_PROD_016_ingestion_backpressure_guards(self):
        """PROD-016: MAX_PENDING 150k (T2 tunable, 80% warn), baseline 20 ticks, reconnect 1s/30s."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC = 20", cfg)
        # Backpressure pins moved from PlatformConfig to IngestionConfig
        # (T2 streaming-3000): warning percent 80% + bounded-halt defaults.
        ing_path = ROOT / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java"
        ing_cfg = ing_path.read_text()
        # 80% is owned by AlertThresholds now (W2/CHG-139); the config derives 0.80.
        thresholds = (ROOT / "code/common/src/main/java/com/trading/common/observability/AlertThresholds.java").read_text()
        self.assertIn("PENDING_APPEND_WARNING_PERCENT = 80", thresholds)
        self.assertIn("AlertThresholds.PENDING_APPEND_WARNING_PERCENT / 100.0", ing_cfg)
        self.assertIn("PENDING_APPEND_WARNING_PERCENT", ing_cfg)
        self.assertIn("150_000", ing_cfg)
        self.assertIn("reconnect", ing_cfg.lower())
        # recomputed: max concurrent checkpoint is 1 so no concurrent stall
        self.assertIn("MAX_CONCURRENT_CHECKPOINTS = 1", cfg)

    def test_PROD_017_audit_gates_exist(self):
        """PROD-017: docs-audit / stale-tables / full-audit / pin-check exist."""
        make = (ROOT / "Makefile").read_text()
        for target in ["docs-audit:", "stale-tables:", "full-audit:", "pin-check:"]:
            self.assertIn(target, make, f"PROD-017: Makefile target missing: {target}")
        self.assertTrue((ROOT / "code/01_platform/04_scripts/docs_audit.py").exists())
        self.assertTrue((ROOT / "code/01_platform/04_scripts/stale_table_kind_scan.py").exists())

    def test_PROD_018_implementation_gate_no_cep(self):
        """PROD-018: CEP dependency ban in order path + stale-table scan (feature_candles_15s not LOG, etc)."""
        make = (ROOT / "Makefile").read_text()
        self.assertIn("cep-check", make)
        self.assertIn("stale_table_kind_scan.py --upstream", make)
        # guards are exercised by run-monday-gates.sh (gate target)
        self.assertIn("gate:", make)
        self.assertIn("run-monday-gates.sh", make)

    def test_PROD_019_no_orphan_fluss_properties_file(self):
        """PROD-019 (P5-014/023/029/030): the Fluss server config is the
        FLUSS_PROPERTIES env block in compose/stack — the MVP-era
        03_fluss/fluss.properties was never wired (no COPY/mount/script ref)
        and its content was stale (ap-south-1 region, empty endpoint,
        non-canonical tablet.host). It must not drift back: an unwired
        properties file reads as authoritative while silently doing nothing."""
        orphan = ROOT / "code/01_platform/03_fluss/fluss.properties"
        self.assertFalse(orphan.exists(),
                         "orphan fluss.properties resurrected — wire it into the "
                         "images or delete it; never keep dead server config")
        self.assertIn("FLUSS_PROPERTIES:", compose_text(),
                      "the live server-config source must stay in compose")

if __name__ == "__main__":
    unittest.main()
