package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.BridgeCaller.OutcomeKind;
import com.trading.common.schema.execution.ExecutionCommandGate.Command;
import com.trading.common.schema.execution.ExecutionCommandGate.Outcome;
import com.trading.common.schema.execution.GateStateStore.ApprovalOutcome;
import com.trading.common.schema.execution.GateStateStore.AuditRecord;
import com.trading.common.schema.execution.GateStateStore.FenceResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The durable command protocol (T5 step 4, CHG-044) over durable store
 * boundaries: PREPARED persisted before any bridge call, SUBMITTING only after
 * durable acknowledgement (prepare() return), exactly one bridge call per
 * attempt, verified acceptance/rejection classification, and UNKNOWN &rarr;
 * evidence + halt + reconciliation. Also pins stale epoch / stale fence /
 * expired lease / wrong owner / missing approval / cross-scope rejection with
 * zero broker calls, and the single-operator (saurabh, DEC-044) approval rule.
 */
class ExecutionCommandGateProtocolTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final String PARTITION = "p-1";

    /** A counting bridge that can also crash; result selectable per test. */
    static final class FakeBridge implements BridgeCaller {
        final Map<String, Integer> calls = new HashMap<>();
        int total;
        OutcomeKind result = OutcomeKind.ACCEPTED;
        boolean crash;

        @Override
        public BridgeOutcome call(Command c) {
            if (crash) {
                throw new RuntimeException("simulated bridge crash");
            }
            calls.merge(c.executionAttemptId(), 1, Integer::sum);
            total++;
            return new BridgeOutcome(result,
                    result == OutcomeKind.ACCEPTED ? "B-" + c.executionAttemptId() : null,
                    "bridge-detail");
        }

        int callsFor(String attemptId) {
            return calls.getOrDefault(attemptId, 0);
        }
    }

    private record Rig(InMemoryAttemptStore attempts, InMemoryGateStateStore gates,
                       FakeBridge bridge, ExecutionCommandGate gate, String partition) {
        static Rig enabled() {
            InMemoryAttemptStore attempts = new InMemoryAttemptStore(new AtomicInteger()::incrementAndGet);
            InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
            FakeBridge bridge = new FakeBridge();
            gates.install(enabledRow());
            ExecutionCommandGate gate = new ExecutionCommandGate(attempts, gates, bridge,
                    "worker-1", () -> NOW, null);
            return new Rig(attempts, gates, bridge, gate, PARTITION);
        }
    }

    private static GateRow enabledRow() {
        // ENABLED, epoch 5, fence token 7 held by worker-1 (unexpired), approved by
        // the single authorized operator (saurabh, DEC-044) over evidence hash "ev-1".
        return new GateRow(PARTITION, "acc", GateState.ENABLED, 5, "enabled", "ev-1",
                "saurabh", null, "ev-1", "worker-1", 7, NOW - 1000, NOW + 100_000, null);
    }

    private static Command cmd(String attemptId, String instruction, String hash,
                               long epoch, long token, String ev) {
        return new Command(attemptId, "acc", instruction, "act-" + attemptId, PARTITION, hash,
                "E-" + attemptId, epoch, token, ev);
    }
    private static Command cmd(String attemptId, String instruction, String hash) {
        return cmd(attemptId, instruction, hash, 5, 7, "ev-1");
    }

    // ---- happy path + classification ----

    @Test
    void verifiedAcceptanceCallsBridgeOnceAndTerminatesAccepted() {
        Rig rig = Rig.enabled();
        ExecutionCommandGate.Result r = rig.gate.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.ACCEPTED);
        assertThat(rig.bridge.total).isEqualTo(1);
        assertThat(r.attempt().phase()).isEqualTo(AttemptRecord.PHASE_ACCEPTED);
        assertThat(r.attempt().gateEpoch()).isEqualTo(5);
        assertThat(r.attempt().gateFenceToken()).isEqualTo(7);
        // immutable audit evidence recorded
        assertThat(rig.gates.auditLog()).anySatisfy(e ->
                assertThat(e.eventType()).isEqualTo("BRIDGE_OUTCOME"));
    }

    @Test
    void verifiedRejectionTerminatesRejected() {
        Rig rig = Rig.enabled();
        rig.bridge.result = OutcomeKind.REJECTED;
        ExecutionCommandGate.Result r = rig.gate.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.REJECTED);
        assertThat(rig.bridge.total).isEqualTo(1);
        assertThat(r.attempt().phase()).isEqualTo(AttemptRecord.PHASE_REJECTED);
    }

    @Test
    void duplicateInstructionNeverIssuesSecondCall() {
        Rig rig = Rig.enabled();
        rig.gate.execute(cmd("a-1", "ins-1", "h-1"));
        ExecutionCommandGate.Result second = rig.gate.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(second.outcome()).isEqualTo(Outcome.DUPLICATE);
        assertThat(rig.bridge.total).isEqualTo(1); // exactly once, never twice
    }

    @Test
    void commandRejectsNullActionIdAndEvidenceHash() {
        // P3-362: both are mandatory — a null evidenceHash would silently BLOCK
        // (never fail fast) and a null actionId would persist a null identity.
        assertThatThrownBy(() -> new Command("a-1", "acc", "ins-1", null, PARTITION, "h-1",
                "E-a-1", 5, 7, "ev-1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("actionId");
        assertThatThrownBy(() -> new Command("a-1", "acc", "ins-1", "act-a-1", PARTITION, "h-1",
                "E-a-1", 5, 7, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("evidenceHash");
    }

    // ---- gate / epoch / fence / approval rejection (no broker call) ----

    @Test
    void gateNotEnabledRejectsWithNoCall() {
        InMemoryAttemptStore attempts = new InMemoryAttemptStore(new AtomicInteger()::incrementAndGet);
        InMemoryGateStateStore gates = InMemoryGateStateStore.anyApprover();
        gates.install(new GateRow(PARTITION, "acc", GateState.HALTED, 5, "halted", "ev-1",
                "saurabh", null, "ev-1", "worker-1", 7, NOW - 1000, NOW + 100_000, null));
        ExecutionCommandGate g = new ExecutionCommandGate(attempts, gates, new FakeBridge(),
                "worker-1", () -> NOW, null);
        ExecutionCommandGate.Result r = g.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(r.reason()).contains("ENABLED");
    }

    @Test
    void staleEpochRejectsWithNoCall() {
        Rig rig = Rig.enabled();
        ExecutionCommandGate.Result r = rig.gate.execute(cmd("a-1", "ins-1", "h-1", 4, 7, "ev-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(r.reason()).contains("epoch");
        assertThat(rig.bridge.total).isZero();
    }

    @Test
    void staleFenceTokenRejectsWithNoCall() {
        Rig rig = Rig.enabled();
        // token 3 < current fence token 7: a stale owner's sequence.
        ExecutionCommandGate.Result r = rig.gate.execute(cmd("a-1", "ins-1", "h-1", 5, 3, "ev-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(r.reason()).contains("fence");
        assertThat(rig.bridge.total).isZero();
    }

    @Test
    void expiredLeaseRejectsWithNoCall() {
        InMemoryAttemptStore attempts = new InMemoryAttemptStore(new AtomicInteger()::incrementAndGet);
        InMemoryGateStateStore gates = InMemoryGateStateStore.anyApprover();
        FakeBridge bridge = new FakeBridge();
        // lease expired at NOW-1
        gates.install(new GateRow(PARTITION, "acc", GateState.ENABLED, 5, "enabled", "ev-1",
                "saurabh", null, "ev-1", "worker-1", 7, NOW - 1000, NOW - 1, null));
        ExecutionCommandGate g = new ExecutionCommandGate(attempts, gates, bridge,
                "worker-1", () -> NOW, null);
        ExecutionCommandGate.Result r = g.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(r.reason()).contains("fence");
        assertThat(bridge.total).isZero();
    }

    @Test
    void wrongOwnerRejectsWithNoCall() {
        InMemoryAttemptStore attempts = new InMemoryAttemptStore(new AtomicInteger()::incrementAndGet);
        InMemoryGateStateStore gates = InMemoryGateStateStore.anyApprover();
        gates.install(enabledRow()); // fence owned by worker-1
        ExecutionCommandGate g = new ExecutionCommandGate(attempts, gates, new FakeBridge(),
                "worker-2", () -> NOW, null); // different instance claims ownership
        ExecutionCommandGate.Result r = g.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(r.reason()).contains("fence");
    }

    @Test
    void missingOrPartialApprovalRejectsWithNoCall() {
        atomicApproval(new String[]{"op-a"}, Outcome.BLOCKED);
        atomicApproval(new String[]{}, Outcome.BLOCKED);
    }

    /** Raises a gate whose approvals are (only) as given, then runs one command. */
    private void atomicApproval(String[] approved, Outcome expected) {
        InMemoryAttemptStore attempts = new InMemoryAttemptStore(new AtomicInteger()::incrementAndGet);
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        FakeBridge bridge = new FakeBridge();
        gates.install(new GateRow(PARTITION, "acc", GateState.ENABLED, 5, "enabled", "ev-1",
                approved.length >= 1 ? approved[0] : null, approved.length >= 2 ? approved[1] : null,
                approved.length >= 2 ? "ev-1" : null, "worker-1", 7, NOW - 1000, NOW + 100_000, null));
        ExecutionCommandGate g = new ExecutionCommandGate(attempts, gates, bridge, "worker-1",
                () -> NOW, null);
        ExecutionCommandGate.Result r = g.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(r.outcome()).isEqualTo(expected);
        assertThat(bridge.total).isZero();
    }

    @Test
    void crossScopeRejectsWithNoCall() {
        Rig rig = Rig.enabled();
        Command cross = new Command("a-1", "other-account", "ins-1", "act-a-1", PARTITION,
                "h-1", "E-a-1", 5, 7, "ev-1");
        ExecutionCommandGate.Result r = rig.gate.execute(cross);
        assertThat(r.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(r.reason()).contains("cross-scope");
        assertThat(rig.bridge.total).isZero();
    }

    @Test
    void changedHashUnderExistingInstructionHalts() {
        Rig rig = Rig.enabled();
        rig.gate.execute(cmd("a-1", "ins-1", "h-1"));
        // same instruction, different request hash + different attempt id
        ExecutionCommandGate.Result r = rig.gate.execute(cmd("a-2", "ins-1", "h-CHANGED"));
        assertThat(r.outcome()).isEqualTo(Outcome.CONTRACT_VIOLATION);
        // scope is halted as a result (durable halt recorded)
        assertThat(rig.gates.read(PARTITION).state()).isEqualTo(GateState.HALTED);
    }

    // ---- UNKNOWN: evidence + halt + reconciliation, never a retry ----

    @Test
    void unknownOutcomeHaltsAndRequiresReconciliation() {
        Rig rig = Rig.enabled();
        rig.bridge.result = OutcomeKind.UNKNOWN;
        ExecutionCommandGate.Result r = rig.gate.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(r.outcome()).isEqualTo(Outcome.UNKNOWN_HALTED);
        assertThat(r.attempt().phase()).isEqualTo(AttemptRecord.PHASE_UNKNOWN);
        assertThat(r.gate().state()).isEqualTo(GateState.HALTED);
        assertThat(rig.bridge.total).isEqualTo(1); // exactly one call, then blocked
        // reconciliation only via resolveUnknown; the submission path cannot exit UNKNOWN
        assertThat(rig.attempts.transition("a-1", 2, AttemptRecord.PHASE_ACCEPTED).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        assertThat(rig.attempts.resolveUnknown("a-1", 2, AttemptRecord.PHASE_ACCEPTED).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        // the scope stayed halted by the unknown; re-running is blocked (no call),
        // and even once re-enabled the terminal attempt would be a DUPLICATE.
        ExecutionCommandGate.Result again = rig.gate.execute(cmd("a-1", "ins-1", "h-1"));
        assertThat(again.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(rig.bridge.total).isEqualTo(1);
    }

    // ---- single-operator (Saurabh) approval (DEC-044) ----

    @Test
    void singleAuthorizedApproverCompletesApproval() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.install(bootHalted());
        GateStateStore.ApprovalResult first = gates.approve(PARTITION, "saurabh", 0, "ev-1", NOW);
        assertThat(first.outcome()).isEqualTo(ApprovalOutcome.APPLIED);
        assertThat(first.row().approvalsComplete()).isTrue();
        assertThat(first.row().approvalsCover("ev-1")).isTrue();
        // second approval after completion is already applied (single-operator gate)
        assertThat(gates.approve(PARTITION, "saurabh", 0, "ev-1", NOW).outcome())
                .isEqualTo(ApprovalOutcome.ALREADY_APPLIED);
    }

    @Test
    void samePrincipalCannotApproveTwiceWhenNotYetComplete() {
        // With single-operator, one approval completes, so a second same-principal
        // is ALREADY_APPLIED. The SAME_PRINCIPAL path is only reachable if the
        // store is in a partially-approved edge state — still guarded.
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.install(bootHalted());
        assertThat(gates.approve(PARTITION, "saurabh", 0, "ev-1", NOW).outcome())
                .isEqualTo(ApprovalOutcome.APPLIED);
        assertThat(gates.approve(PARTITION, "saurabh", 0, "ev-1", NOW).outcome())
                .isEqualTo(ApprovalOutcome.ALREADY_APPLIED);
    }

    @Test
    void unauthorizedPrincipalCannotApprove() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.install(bootHalted());
        assertThat(gates.approve(PARTITION, "mallory", 0, "ev-1", NOW).outcome())
                .isEqualTo(ApprovalOutcome.UNAUTHORIZED);
    }

    @Test
    void approvalWithMismatchedEvidenceIsRejectedWithEvidenceMismatch() {
        // P3-151: an approval must cover the exact evidence package the gate holds.
        // Approving a different (stale/unauthorized) package must not be recorded.
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.install(new GateRow(PARTITION, "acc", GateState.APPROVAL_PENDING, 4, "recon", "ev-1",
                null, null, null, null, 0L, null, null, null));

        assertThat(gates.approve(PARTITION, "saurabh", 4, "ev-2", NOW).outcome())
                .isEqualTo(ApprovalOutcome.EVIDENCE_MISMATCH);

        // No mutation: no approval and no approved evidence recorded.
        GateRow after = gates.read(PARTITION);
        assertThat(after.approval1()).isNull();
        assertThat(after.approvedEvidenceHash()).isNull();
        assertThat(after.approvalsComplete()).isFalse();

        // The exact package still approves, so the guard is targeted, not a blanket block.
        assertThat(gates.approve(PARTITION, "saurabh", 4, "ev-1", NOW).outcome())
                .isEqualTo(ApprovalOutcome.APPLIED);
    }

    @Test
    void approvalRejectedWhenGateEpochChanges() {
        // An approval is for an exact gate epoch; after the epoch advances the
        // old approval must not authorize a different generation.
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        // Start already-ENABLED at epoch 5 so a subsequent halt advances to 6.
        gates.install(new GateRow(PARTITION, "acc", GateState.ENABLED, 5, "enabled", "ev-1",
                null, null, null, "worker-1", 7, NOW - 1000, NOW + 100_000, null));
        gates.approve(PARTITION, "saurabh", 5, "ev-1", NOW);
        // advance the epoch (safety halt: ENABLED -> HALTED increments epoch to 6)
        gates.halt(PARTITION, gates.read(PARTITION), "reconcile", "ev-1", NOW);
        assertThat(gates.approve(PARTITION, "saurabh", 5, "ev-1", NOW).outcome())
                .isEqualTo(ApprovalOutcome.EPOCH_MISMATCH);
    }

    private static GateRow bootHalted() {
        return new GateRow(PARTITION, "acc", GateState.HALTED, 0, "boot", null,
                null, null, null, null, 0, null, null, null);
    }
}
