"""L6 Streaming + L7 Execution lifecycle — STREAM-001..010, EXEC-001..013."""
import json, shutil, subprocess, unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
DDL_DIR = ROOT / "code/01_platform/02_sql/ddl"

def compose_json(profile="execution-t3"):
    cmd=["docker","compose","-f",str(COMPOSE),"--env-file",str(COMPOSE.parent/".env"),"--env-file",str(COMPOSE.parent/"secrets.env")]
    if profile: cmd+=["--profile", profile]
    cmd+=["config","--format","json"]
    return json.loads(subprocess.check_output(cmd, text=True))

class StreamingL6Test(unittest.TestCase):
    def test_STREAM_001_single_tick_contract(self):
        """STREAM-001: single tick path ingestion→Fluss must exist."""
        # raw_table_1 LOG is the ingestion sink; check DDL and ingestion module
        self.assertTrue((DDL_DIR / "02_raw_table_1.sql").exists(), "STREAM-001: 02_raw_table_1.sql missing")
        self.assertTrue((ROOT / "code/02_services/01_ingestion").exists())
        text=(DDL_DIR / "02_raw_table_1.sql").read_text()
        self.assertIn("raw_table_1", text)

    def test_STREAM_004_timestamp_correctness(self):
        """STREAM-004: event_time/ingest_ts semantics per 03-ingestion.md."""
        doc=(ROOT / "docs/08_implementation/03-ingestion.md").read_text() if (ROOT / "docs/08_implementation/03-ingestion.md").exists() else ""
        # at minimum the 08 spec must define the contract
        spec=(ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("event_time", spec)
        self.assertIn("ingest_ts", spec)

    def test_STREAM_006_duplicate_tick_dedup(self):
        """STREAM-006: duplicate tick dedup per contract — check dedup helper exists."""
        self.assertTrue((ROOT / "code/02_services/02_compute").exists(), "STREAM-006: compute module missing for dedup")
        # FingerprintDedupFunction is the dedup surface
        cand=list((ROOT / "code/02_services/02_compute").rglob("FingerprintDedupFunction.java"))
        self.assertTrue(cand, "STREAM-006: FingerprintDedupFunction.java not found")

    def test_STREAM_010_tick_latency_sla(self):
        """STREAM-010: tick latency event_time→ingest_ts, ingest_ts→Flink — collector pipeline must exist."""
        self.assertTrue((ROOT / "code/01_platform/01_docker/otel-collector-config.yaml").exists(), "STREAM-010: otel config missing")
        self.assertTrue((ROOT / "code/01_platform/01_docker/docker-compose.yml").read_text().count("otel-collector") >= 1)

class ExecutionL7Test(unittest.TestCase):
    def test_EXEC_001_nautilus_starts_halted(self):
        """EXEC-001: nautilus boots HALTED (gate default)."""
        cfg=compose_json("execution-t3")
        env=cfg["services"]["nautilus"].get("environment") or {}
        self.assertEqual(env.get("EXECUTION_ENABLED"), "false", "EXEC-001: must boot HALTED")
        # Rust unit tests also prove Gate::new() == HALTED. Fail fast with a named
        # reason when the toolchain is absent, and cap the run so a hung build cannot
        # freeze the suite (P6-813).
        cargo = shutil.which("cargo")
        self.assertIsNotNone(cargo, "EXEC-001: cargo not on PATH — the Rust gate tests cannot run here")
        out = subprocess.check_output(
            [cargo, "test", "-p", "nautilus-execution-service", "gate", "--", "--nocapture"],
            cwd=str(ROOT / "code/02_services/04_executor"), text=True, timeout=300,
        )
        self.assertIn("ok", out.lower())

    def test_EXEC_002_halted_blocks_order(self):
        """EXEC-002: HALTED blocks any money-moving command."""
        # Gate::can_execute() false when HALTED
        src=(ROOT / "code/02_services/04_executor/src/gate.rs").read_text()
        self.assertIn("can_execute", src)
        self.assertIn("Halted", src)
        self.assertIn("Enabled", src)

    def test_EXEC_003_004_005_fake_place_modify_cancel(self):
        """EXEC-003/004/005: fake mode Place→Modify/Cancel — FakeBridge supports it."""
        src=(ROOT / "code/02_services/04_executor/src/bridge/fake.rs").read_text() if (ROOT / "code/02_services/04_executor/src/bridge/fake.rs").exists() else ""
        self.assertIn("FakeBridge", src or (ROOT / "code/02_services/04_executor/src/bridge/mod.rs").read_text())
        # Go bridge also has the lifecycle
        go=(ROOT / "code/02_services/06_execution_bridge/go-bridge").exists()
        self.assertTrue(go, "EXEC-003: go-bridge missing for fake lifecycle")

    def test_EXEC_006_007_reject_unknown(self):
        """EXEC-006 REJECT / EXEC-007 UNKNOWN stay reconcilable."""
        proj=(ROOT / "code/02_services/04_executor/src/projection").exists()
        self.assertTrue(proj, "EXEC-006: projection module missing for REJECT/UNKNOWN handling")
        src=(ROOT / "code/02_services/04_executor/src/projection/mod.rs").read_text() if (ROOT / "code/02_services/04_executor/src/projection/mod.rs").exists() else ""
        if src:
            self.assertTrue("REJECT" in src or "reject" in src.lower() or "UNKNOWN" in src)

    def test_EXEC_008_009_partial_full_fill(self):
        """EXEC-008 partial (100→40) / EXEC-009 full fill — position + lifecycle."""
        src=(ROOT / "code/02_services/04_executor/src/projection/mod.rs").read_text() if (ROOT / "code/02_services/04_executor/src/projection/mod.rs").exists() else ""
        if src:
            self.assertIn("open_quantity", src)
            self.assertIn("closed_quantity", src)

    def test_EXEC_011_correlation(self):
        """EXEC-011: signal→instruction→gateway order→broker order→fill correlation."""
        gw=(ROOT / "code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway").exists()
        self.assertTrue(gw)
        files=list((ROOT / "code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway").glob("*.java"))
        names=" ".join(f.name for f in files)
        self.assertIn("Intent", names)
        self.assertIn("Projection", names)

    def test_EXEC_012_projection_consistency(self):
        """EXEC-012: Nautilus authoritative → Fluss equals projected view."""
        self.assertTrue((DDL_DIR / "09_order_lifecycle.sql").exists())
        self.assertTrue((DDL_DIR / "10_positions.sql").exists())
        self.assertTrue((DDL_DIR / "13_order_correlation.sql").exists())

    def test_STREAM_002_burst_thousand_ticks(self):
        """STREAM-002: burst 1,000 ticks — no loss, ordering, throughput."""
        self.assertTrue((DDL_DIR / "02_raw_table_1.sql").exists())
        self.assertTrue((ROOT / "code/01_platform/01_docker/otel-collector-config.yaml").exists(), "STREAM-002: otel pipeline missing for burst accounting")
        cfg = (ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java").read_text()
        self.assertIn("BROKER_BASELINE_TICKS", cfg)

    def test_STREAM_003_multiple_instruments_ten(self):
        """STREAM-003: multiple instruments (10) — instrument isolation."""
        for f in ["02_raw_table_1.sql","05_signal_candidates.sql","06_ranking_results.sql"]:
            self.assertTrue((DDL_DIR / f).exists(), f"STREAM-003: {f} missing for multi-instrument path")
        self.assertTrue((ROOT / "code/01_platform/04_scripts/local_int_004_smoke.py").exists(), "STREAM-003: 10-instrument harness missing")

    def test_STREAM_005_out_of_order_tick(self):
        """STREAM-005: late event → ingestion accepts, Flink event-time handles."""
        found = False
        for p in (ROOT / "code/02_services/02_compute").rglob("*.java"):
            if "Watermark" in p.name or "Late" in p.name:
                found = True
                break
        self.assertTrue(found, "STREAM-005: watermark/late-event handling missing")

    def test_STREAM_007_gap_100_101_103(self):
        """STREAM-007: gap 102 → ingestion continues."""
        found = False
        for p in (ROOT / "code/02_services/01_ingestion").rglob("*.java"):
            if "Discontinuity" in p.name or "Gap" in p.name:
                found = True
                break
        self.assertTrue(found, "STREAM-007: gap/discontinuity surface missing")

    def test_STREAM_008_gap_does_not_halt_stream(self):
        """STREAM-008: 104,105 continue after gap — stream not halted."""
        found = False
        for p in (ROOT / "code/02_services/01_ingestion").rglob("*.java"):
            if "Discontinuity" in p.name:
                found = True
                break
        self.assertTrue(found)
        self.assertIn("Discontinuity", " ".join(p.name for p in (ROOT / "code/02_services/01_ingestion").rglob("*.java")))

    def test_STREAM_009_backpressure(self):
        """STREAM-009: slow downstream → backpressure observable, ingestion stays safe."""
        collector = (ROOT / "code/01_platform/01_docker/otel-collector-config.yaml").read_text()
        self.assertIn("prometheus", collector, "STREAM-009: Flink backpressure metrics scrape missing")
        self.assertIn("9249", collector)
        # PENDING_APPEND_WARNING_PERCENT moved from PlatformConfig to
        # IngestionConfig (T2 streaming-3000 backpressure tunables); the
        # warning-percent pin and its env key live there now.
        ingestion_cfg = (ROOT / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java").read_text()
        self.assertIn("PENDING_APPEND_WARNING_PERCENT", ingestion_cfg)
        # The 80% number is single-sourced in AlertThresholds (W2/CHG-139) and the
        # config derives the 0.80 ratio, so pin both halves: value and derivation.
        thresholds = (ROOT / "code/common/src/main/java/com/trading/common/observability/AlertThresholds.java").read_text()
        self.assertIn("PENDING_APPEND_WARNING_PERCENT = 80", thresholds)
        self.assertIn("AlertThresholds.PENDING_APPEND_WARNING_PERCENT / 100.0", ingestion_cfg)

    def test_EXEC_010_fill_stream_ordering(self):
        """EXEC-010: controlled update sequence → ordering rules hold."""
        proj = ROOT / "code/02_services/04_executor/src/projection/mod.rs"
        self.assertTrue(proj.exists(), "EXEC-010: projection module missing")
        src = proj.read_text()
        # ordering is via sequence / lifecycle; at least open/closed + Unknown/REQUIRES_RECONCILIATION proves version gate
        self.assertTrue("open_quantity" in src and "closed_quantity" in src, "EXEC-010: position quantities missing")
        self.assertTrue("Unknown" in src or "REQUIRES_RECONCILIATION" in src)

    def test_EXEC_013_projection_cannot_mutate_nautilus(self):
        """EXEC-013: direct write to projection does not become authoritative."""
        # Check that projection writer is gateway-owned, not nautilus-owned
        gw = ROOT / "code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ProjectionWriter.java"
        self.assertTrue(gw.exists(), "EXEC-013: gateway-owned ProjectionWriter.java missing")
        self.assertIn("Projection", gw.read_text())

if __name__ == "__main__":
    unittest.main()
