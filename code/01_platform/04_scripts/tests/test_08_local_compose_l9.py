"""L9 — Observability  OBS-001..012.

Per 08-local-compose.md §L9. All pass offline (contract checks on collector
config, PlatformConfig, promotion probes); live probes degrade gracefully.
"""
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
COLLECTOR = ROOT / "code/01_platform/01_docker/otel-collector-config.yaml"
PLATFORM_CONFIG = ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java"
INGESTION_METRICS = ROOT / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java"
GATE_RS = ROOT / "code/02_services/04_executor/src/gate.rs"

# P6-611: services allowed to wire Arrow credentials in the compose SOURCE. The
# rendered config cannot be used for this — every service carries
# `env_file: secrets.env`, so eight of them render ARROW_* keys (the false-fire
# already documented in L2's NETWORK-008).
ARROW_CARRIERS_ALLOWED = {"execution-bridge", "ingestion"}


def arrow_env_carriers(path):
    """{service: [ARROW_* env keys]} explicitly wired in the compose source."""
    import yaml
    cfg = yaml.safe_load(Path(path).read_text()) or {}
    out = {}
    for name, svc in (cfg.get("services") or {}).items():
        env = (svc or {}).get("environment") or {}
        keys = set(env) if isinstance(env, dict) else set(env or [])
        arrow = sorted(str(k) for k in keys if str(k).startswith("ARROW_"))
        if arrow:
            out[name] = arrow
    return out


def compose_text() -> str:
    return COMPOSE.read_text()


def collector_text() -> str:
    return COLLECTOR.read_text() if COLLECTOR.exists() else ""


class ObservabilityL9Test(unittest.TestCase):
    def test_OBS_001_every_service_emits_telemetry(self):
        """OBS-001: every required service emits telemetry to OTel collector / OpenObserve."""
        ct = collector_text()
        self.assertTrue(COLLECTOR.exists(), "OBS-001: otel-collector-config.yaml missing")
        # collector has OTLP (ingestion), prometheus (Flink), filelog (ingestion/flink/fluss)
        self.assertIn("otlp:", ct, "OBS-001: otlp receiver missing")
        self.assertIn("prometheus:", ct, "OBS-001: prometheus scrape missing")
        self.assertIn("filelog", ct, "OBS-001: filelog receiver missing")
        self.assertIn("otel-collector", compose_text(), "OBS-001: collector service missing")
        self.assertIn("openobserve", compose_text(), "OBS-001: openobserve service missing")
        self.assertTrue(INGESTION_METRICS.exists(), "OBS-001: OtlpMetricsEmitter missing")

    def test_OBS_002_health_telemetry_contains_service_identity(self):
        """OBS-002: health telemetry carries service identity (resource.service.name)."""
        ct = collector_text()
        self.assertIn("service.name", ct, "OBS-002: service identity not wired in collector")
        self.assertIn("resource", ct)

    def test_OBS_003_flink_checkpoint_failure_observable(self):
        """OBS-003: Flink checkpoint failure is observable (PrometheusReporter :9249 scrape)."""
        ct = collector_text()
        self.assertIn("9249", ct, "OBS-003: Flink :9249 scrape missing")
        self.assertIn("flink-jobmanager:9249", ct)
        self.assertIn("flink-taskmanager:9249", ct)
        self.assertTrue(
            (ROOT / "code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/SignalJobObjectStoreCheckpointIntegrationTest.java").exists(),
            "OBS-003: checkpoint integration test missing",
        )

    def test_OBS_004_restart_count_observable(self):
        """OBS-004: restart count is observable (gate safety_halt_count + compose restart policy)."""
        self.assertIn("safety_halt_count", GATE_RS.read_text(), "OBS-004: gate safety_halt_count missing")
        self.assertIn("restart:", compose_text())

    def test_OBS_005_ws_reconnect_observable(self):
        """OBS-005: WS reconnect is observable (ingestion reconnect metrics + discontinuity evidence)."""
        # Reconnect evidence is the observable
        # P6-610: the writer IS the evidence. The old form read it only when it
        # existed and otherwise fell back to the literal word it then searched for, so
        # a missing writer passed. Assert the file, then assert on its content.
        writer = (ROOT / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/"
                  "discontinuity/DiscontinuityWriter.java")
        self.assertTrue(writer.exists(), "OBS-005: DiscontinuityWriter missing — no reconnect evidence")
        self.assertIn("reconnect", writer.read_text().lower())

    def test_OBS_006_gap_count_observable(self):
        """OBS-006: gap count is observable (ingestion gap detection + metrics)."""
        # Ingestion must have gap/discontinuity handling
        candidates = list((ROOT / "code/02_services/01_ingestion").rglob("*.java"))
        names = " ".join(p.name for p in candidates)
        self.assertTrue("Discontinuity" in names or "Gap" in names, "OBS-006: gap/discontinuity surface missing")
        # Gap is also L6 STREAM-007/008

    def test_OBS_007_tick_throughput_observable(self):
        """OBS-007: tick throughput is observable (ingestion metrics + flink backpressure)."""
        ct = collector_text()
        # throughput via filelog/otlp + flink prometheus metrics
        self.assertIn("otlp", ct)
        self.assertTrue(INGESTION_METRICS.exists())

    def test_OBS_008_tick_to_ingest_latency_observable(self):
        """OBS-008: tick→ingest latency is observable (STREAM-010 SLA)."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("event_time", spec)
        self.assertIn("ingest_ts", spec)

    def test_OBS_009_instruction_to_order_latency_observable(self):
        """OBS-009: instruction→order latency is observable (execution gateway)."""
        # Execution gateway must exist to emit this latency
        gw = ROOT / "code/02_services/06_execution_gateway"
        self.assertTrue(gw.exists(), "OBS-009: execution gateway missing")
        # Collector must have ingestion+execution path to measure it
        self.assertIn("openobserve", collector_text())

    def test_OBS_010_fill_to_lifecycle_latency_observable(self):
        """OBS-010: fill→lifecycle latency is observable (projection)."""
        proj = ROOT / "code/02_services/04_executor/src/projection/mod.rs"
        self.assertTrue(proj.exists(), "OBS-010: projection module missing")
        self.assertIn("openobserve", collector_text())

    def test_OBS_011_no_secrets_in_logs(self):
        """OBS-011: no secrets in logs (SEC-010 / CONFIG-005)."""
        # Mirrors L2 SEC-010 — collector filelog reads JSON logs that must not contain secrets
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("No secrets in logs", spec)
        # P6-611: the loop that used to sit here asserted True on every line, so it
        # verified nothing (and nothing at all when the list was empty). Assert the real
        # property against the compose source; network placement is L2's NETWORK-008.
        carriers = arrow_env_carriers(COMPOSE)
        self.assertTrue(carriers, "OBS-011: no service wires ARROW_* — this guard would be checking nothing")
        outside = sorted(set(carriers) - ARROW_CARRIERS_ALLOWED)
        self.assertEqual(outside, [],
                         f"OBS-011: ARROW_* wired outside {sorted(ARROW_CARRIERS_ALLOWED)}: {outside}")

    def test_OBS_012_telemetry_failure_not_unsafe(self):
        """OBS-012: telemetry failure does not cause unsafe trading behavior (non-critical path)."""
        ct = collector_text()
        # Collector is observability-only; spec explicitly says so
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("Observability must be non-critical", spec)
        # Retry semantics prove failure is handled without halting ingestion
        self.assertIn("retry_on_failure", ct, "OBS-012: collector retry_on_failure missing — telemetry failure would be silent")
        self.assertIn("max_elapsed_time", ct)


if __name__ == "__main__":
    unittest.main()
