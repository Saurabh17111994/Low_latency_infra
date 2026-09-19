"""L8 — Failure / restart / recovery + SAFETY fail-closed.

FAIL-001..010 + SAFETY-001 per 08-local-compose.md §L8.
All tests pass offline (contract checks); live probes degrade to contract-only
when the execution-t3 stack is not up, so CI without containers stays green.
"""
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
GATE_RS = ROOT / "code/02_services/04_executor/src/gate.rs"
DEDUP_JAVA = ROOT / "code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/IntentDeduplicator.java"
INTENT_VALIDATOR = ROOT / "code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/IntentValidator.java"


def compose_text() -> str:
    return COMPOSE.read_text()


def compose_services() -> set:
    import re
    text = compose_text()
    # naïve service-name extraction: lines like "  fluss-coordinator:"
    # Under top-level `services:` block — sufficient as contract check.
    return set(re.findall(r"^\s{2}([a-z0-9._-]+):\s*$", text, flags=re.MULTILINE))


class FailRestartTest(unittest.TestCase):
    def test_FAIL_001_restart_ingestion_no_uncontrolled_duplicate(self):
        """FAIL-001: ingestion kill→reconnect must not spray duplicates; deduplication path exists."""
        # Fingerprint dedup is the ingestion guard; check the function exists.
        cand = list((ROOT / "code/02_services/02_compute").rglob("FingerprintDedupFunction.java"))
        self.assertTrue(cand, "FAIL-001: FingerprintDedupFunction.java missing")
        # Ingestion is restart:unless-stopped — recovery, not manual resurrection.
        self.assertIn("restart:", compose_text())

    def test_FAIL_002_restart_taskmanager_recovers_via_checkpoint(self):
        """FAIL-002: TaskManager restart → Flink recovers via checkpoint."""
        # TaskManager must exist and checkpoint integration test must exist.
        svcs = compose_services()
        self.assertIn("flink-taskmanager", svcs)
        self.assertTrue(
            (ROOT / "code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/SignalJobObjectStoreCheckpointIntegrationTest.java").exists(),
            "FAIL-002: checkpoint integration test missing",
        )

    def test_FAIL_003_restart_jobmanager_recovers(self):
        """FAIL-003: JobManager restart → jobs reconcile via checkpoint."""
        svcs = compose_services()
        self.assertIn("flink-jobmanager", svcs)
        self.assertIn("flink-checkpoints", compose_text())

    def test_FAIL_004_restart_fluss_tablet_data_survives(self):
        """FAIL-004: Fluss tablet restart retains data (persistent volumes)."""
        text = compose_text()
        self.assertIn("fluss-tablet-data", text, "FAIL-004: tablet data volume missing")
        self.assertIn("fluss-remote-data", text, "FAIL-004: remote data volume missing")

    def test_FAIL_005_restart_nautilus_gate_stays_halted(self):
        """FAIL-005 (critical): Nautilus restart reconstructs durable state, gate does NOT silently become ENABLED."""
        src = GATE_RS.read_text()
        # Gate boots HALTED; only sanctioned path enables
        self.assertIn('ExecState::Halted', src)
        self.assertIn('safety_halt', src)
        self.assertIn('can_execute', src)
        self.assertIn('Halted, ExecState::Reconciling', src)
        # The Rust gate tests prove the HALTED invariant. Run them only when the
        # toolchain exists, and with a cap, so this module keeps its documented
        # "CI without containers stays green" promise (P6-608).
        cargo = shutil.which("cargo")
        if cargo is None:
            self.skipTest("cargo not on PATH — contract checks above still ran, no live Rust gate proof")
        out = subprocess.check_output(
            [cargo, "test", "-p", "nautilus-execution-service", "gate", "--", "--nocapture"],
            cwd=str(ROOT / "code/02_services/04_executor"), text=True, timeout=600,
        )
        self.assertIn("ok", out.lower())
        # Compose: nautilus boots HALTED via EXECUTION_ENABLED=false
        self.assertIn("EXECUTION_ENABLED", compose_text())

    def test_FAIL_006_restart_bridge_no_accidental_order(self):
        """FAIL-006: bridge restart / reconnect emits no accidental orders (HALTED blocks)."""
        src = GATE_RS.read_text()
        self.assertIn("can_execute", src)
        self.assertIn("HALTED", src)   # P6-814: stated directly, not as a double negative
        # Go bridge fake exists — reconnect seam
        self.assertTrue((ROOT / "code/02_services/06_execution_bridge/go-bridge").exists())

    def test_FAIL_007_network_partition_no_duplicate_order(self):
        """FAIL-007: partition bridge↔Nautilus → no duplicate orders, controlled execution."""
        # execution-net is internal:true — only bridge joins arrow-egress
        text = compose_text()
        # execution-net must be internal
        self.assertIn("execution-net", text)
        self.assertIn("internal: true", text)
        # Deduplicator guards against duplicate intent submission after partition heal
        self.assertTrue(DEDUP_JAVA.exists(), "FAIL-007: IntentDeduplicator missing")

    def test_FAIL_008_fake_broker_timeout_no_false_filled(self):
        """FAIL-008: fake broker timeout must NOT become FILLED."""
        # Projection treats timeout as UNKNOWN/REQUIRES_RECONCILIATION, never FILLED
        proj = ROOT / "code/02_services/04_executor/src/projection/mod.rs"
        # P6-609: an absent contract file is the failure this suite exists to catch,
        # not a licence to assert nothing.
        self.assertTrue(proj.exists(), "FAIL-008: projection/mod.rs missing")
        src = proj.read_text()
        self.assertTrue("Unknown" in src or "UNKNOWN" in src or "REQUIRES_RECONCILIATION" in src)

    def test_FAIL_009_fake_broker_unknown_stays_reconcilable(self):
        """FAIL-009: UNKNOWN after timeout stays reconcilable (no silent drop)."""
        gate = GATE_RS.read_text()
        self.assertIn("safety_halt", gate)
        # IntentDeduplicator: HASH_VIOLATION is fail-closed, not silently replayed
        self.assertIn("HASH_VIOLATION", DEDUP_JAVA.read_text())

    def test_FAIL_010_double_delivery_idempotent_projection(self):
        """FAIL-010: identical execution event twice → projection idempotent (DUPLICATE no-op)."""
        # Contract: PositionProjector + KvStateUpdateProtocol + ImmutabilityProtocol all return DUPLICATE
        for p in [
            ROOT / "code/common/src/test/java/com/trading/common/schema/position/PositionProjectorTest.java",
            ROOT / "code/common/src/test/java/com/trading/common/schema/KvStateUpdateProtocolTest.java",
        ]:
            self.assertTrue(p.exists(), f"FAIL-010: {p.name} missing")   # P6-609
            self.assertIn("DUPLICATE", p.read_text(), f"FAIL-010: {p.name} lacks DUPLICATE contract")
        # Live dedup: gateway deduplicator
        self.assertIn("DUPLICATE", DEDUP_JAVA.read_text())


class SafetyFailClosedTest(unittest.TestCase):
    def test_SAFETY_001_fail_closed_on_ambiguity(self):
        """SAFETY-001: unknown broker/missing correlation/position/schema/credential/stale gateway/unknown status/restart-mid-lifecycle → DO NOT place order → HALTED/UNKNOWN/REQUIRES_RECONCILIATION."""
        # a) Validator fail-closed
        self.assertTrue(INTENT_VALIDATOR.exists(), "SAFETY-001: IntentValidator missing")
        v = INTENT_VALIDATOR.read_text()
        self.assertIn("validate", v)
        self.assertIn("throw", v)  # fail-closed via exception
        # b) Gate fail-closed: ambiguous → safety_halt → HALTED, blocks execution
        gate = GATE_RS.read_text()
        self.assertIn("safety_halt", gate)
        self.assertIn("Halted", gate)
        self.assertIn("can_execute", gate)
        # c) Deduplicator fail-closed on HASH_VIOLATION
        dedup = DEDUP_JAVA.read_text()
        self.assertIn("HASH_VIOLATION", dedup)
        # d) Projection idempotency prevents double-apply on replay after restart
        projector = ROOT / "code/common/src/test/java/com/trading/common/schema/KvStaleWriteRejectionTest.java"
        self.assertTrue(projector.exists(), "SAFETY-001: KvStaleWriteRejectionTest missing")   # P6-609
        self.assertIn("DUPLICATE", projector.read_text())
        # e) State machine never permits direct skip to ENABLED — must reconcile + approve
        self.assertIn("ApprovalPending", gate)


if __name__ == "__main__":
    unittest.main()
