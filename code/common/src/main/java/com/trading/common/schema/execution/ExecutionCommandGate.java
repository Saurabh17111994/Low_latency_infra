package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.BridgeCaller.OutcomeKind;
import com.trading.common.schema.execution.GateStateStore.AuditRecord;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The durable transaction-like command protocol (T5 sequence step 4, CHG-044),
 * composed over durable {@link AttemptStore} + {@link GateStateStore} + a
 * {@link BridgeCaller}. It implements the order-safety invariant
 * (docs/08_implementation/01-foundation.md &rarr; "Order safety invariant") and
 * the T5 target behavior:
 *
 * <ol>
 *   <li>validate the current gate epoch and fenced owner;</li>
 *   <li>persist PREPARED (request hash + deterministic client ref + gate epoch +
 *       fence token) and await durable acknowledgement;</li>
 *   <li>transition to SUBMITTING only after that durable acknowledgement;</li>
 *   <li>call the bridge exactly once for that attempt;</li>
 *   <li>classify verified acceptance/rejection; all ambiguity becomes UNKNOWN;</li>
 *   <li>for UNKNOWN: persist evidence, halt the scope, require reconciliation
 *       (only {@code resolveUnknown} may exit UNKNOWN, never auto-retry);</li>
 *   <li>reject stale lease/fence/epoch/cross-scope immediately before the call.</li>
 * </ol>
 *
 * <p>Crash-window safety (asserted by {@code ExecutionCommandGateCrashWindowTest}):
 * because the bridge is called only between a durable SUBMITTING and the durable
 * outcome, a restart that observes a PREPARED attempt resumes it safely (no call
 * was issued yet), while a restart that observes SUBMITTING or UNKNOWN halts and
 * requires reconciliation rather than issuing a second place request — zero
 * duplicate commands for every injected crash window.
 */
public final class ExecutionCommandGate {

    /** A single money-moving command; identity is deterministic and caller-supplied. */
    public record Command(
            String executionAttemptId,
            String accountScopeId,
            String instructionId,
            String actionId,
            String executionPartitionId,
            String requestHash,
            String clientOrderRef,
            long gateEpoch,
            long gateFenceToken,
            String evidenceHash) {

        public Command {
            Objects.requireNonNull(executionAttemptId, "executionAttemptId");
            Objects.requireNonNull(accountScopeId, "accountScopeId");
            Objects.requireNonNull(instructionId, "instructionId");
            // P3-362: actionId + evidenceHash are as mandatory as the other
            // identities — a null evidenceHash would silently BLOCK the command
            // (approvedEvidenceHash.equals(null) == false) and persist null
            // evidence on halt/audit, breaking the evidence chain.
            Objects.requireNonNull(actionId, "actionId");
            Objects.requireNonNull(executionPartitionId, "executionPartitionId");
            Objects.requireNonNull(requestHash, "requestHash");
            Objects.requireNonNull(clientOrderRef, "clientOrderRef");
            Objects.requireNonNull(evidenceHash, "evidenceHash");
        }
    }

    /** The terminal classification after a bridge round-trip. */
    public enum Outcome {
        /** Bridge positively accepted the order. */
        ACCEPTED,
        /** Bridge positively rejected the order. */
        REJECTED,
        /** A duplicate (instruction_id, request_hash) already exists — no new call. */
        DUPLICATE,
        /** Gate is not ENABLED, epoch/fence/approval/cross-scope check failed — no call. */
        BLOCKED,
        /** Same instruction_id, different request_hash — contract violation; scope halted. */
        CONTRACT_VIOLATION,
        /** Ambiguous outcome (or crash left SUBMITTING/UNKNOWN) — gate halted, reconciliation required. */
        UNKNOWN_HALTED
    }

    public record Result(Outcome outcome, AttemptRecord attempt, GateRow gate,
                         String reason, int bridgeCallsForAttempt) {}

    /** Injectable crash points for the crash-window suite (no-op in production). */
    public interface CrashHooks {
        void afterPrepared();
        void afterSubmitting();
        void beforeBridge();
        void afterBridge();
    }

    private final AttemptStore attempts;
    private final GateStateStore gateStore;
    private final BridgeCaller bridge;
    private final String ownerInstanceId;
    private final LongSupplier clock;
    private final Runnable haltObserver;
    private final CrashHooks crashHooks;
    // P3-132: exactly-once issuance guard — concurrent execute() calls share one
    // engine, so the set must be thread-safe. Unbounded by design (one entry per
    // attempt id, same cardinality as the attempt store); entries are never
    // evicted because a late duplicate must still be recognized as issued.
    private final Set<String> issuedAttempts = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public ExecutionCommandGate(AttemptStore attempts, GateStateStore gateStore,
                                BridgeCaller bridge, String ownerInstanceId,
                                LongSupplier clock, Runnable haltObserver) {
        this(attempts, gateStore, bridge, ownerInstanceId, clock, haltObserver,
                new CrashHooks() {
                    @Override public void afterPrepared() {}
                    @Override public void afterSubmitting() {}
                    @Override public void beforeBridge() {}
                    @Override public void afterBridge() {}
                });
    }

    public ExecutionCommandGate(AttemptStore attempts, GateStateStore gateStore,
                                BridgeCaller bridge, String ownerInstanceId,
                                LongSupplier clock, Runnable haltObserver,
                                CrashHooks crashHooks) {
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.gateStore = Objects.requireNonNull(gateStore, "gateStore");
        this.bridge = Objects.requireNonNull(bridge, "bridge");
        this.ownerInstanceId = Objects.requireNonNull(ownerInstanceId, "ownerInstanceId");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.haltObserver = haltObserver == null ? () -> {} : haltObserver;
        this.crashHooks = Objects.requireNonNull(crashHooks, "crashHooks");
    }

    /**
     * Execute one money-moving command through the durable protocol. Returns a
     * {@link Result}; on {@code BLOCKED} no broker call is made. On
     * {@code UNKNOWN_HALTED} the scope is halted and only an explicit
     * reconciliation of the UNKNOWN attempt may progress.
     */
    public Result execute(Command cmd) {
        Objects.requireNonNull(cmd, "cmd");
        long now = clock.getAsLong();
        GateRow row = gateStore.read(cmd.executionPartitionId());
        if (row == null) {
            return blocked(cmd, null, "no gate row for partition " + cmd.executionPartitionId());
        }
        if (!row.accountScopeId().equals(cmd.accountScopeId())) {
            return blocked(cmd, row, "cross-scope: command scope " + cmd.accountScopeId()
                    + " != gate scope " + row.accountScopeId());
        }
        if (row.state() != GateState.ENABLED) {
            return blocked(cmd, row, "gate not ENABLED (state=" + row.state() + ")");
        }
        if (row.epoch() != cmd.gateEpoch()) {
            return blocked(cmd, row, "stale epoch: command " + cmd.gateEpoch()
                    + " != gate epoch " + row.epoch());
        }
        if (!row.fenceValidFor(ownerInstanceId, cmd.gateFenceToken(), now)) {
            return blocked(cmd, row, "fence invalid: owner/token/lease before the call");
        }
        if (!row.approvalsComplete() || !row.approvalsCover(cmd.evidenceHash())) {
            return blocked(cmd, row, "approval missing or for a different evidence package (single-operator Saurabh)");
        }

        // 2. Persist PREPARED with request hash + client ref + gate epoch + fence token
        //    (durable acknowledgement = CREATED/DUPLICATE returned by the store).
        AttemptStore.PrepareResult pr = attempts.prepare(new AttemptStore.PrepareRequest(
                cmd.executionAttemptId(), cmd.accountScopeId(), cmd.instructionId(),
                cmd.actionId(), cmd.executionPartitionId(), cmd.requestHash(),
                cmd.clientOrderRef(), cmd.gateFenceToken(), cmd.gateEpoch(), now));
        if (pr.status() == AttemptStore.Status.CONTRACT_VIOLATION) {
            GateRow halted = halt(cmd, row, pr.reason(), cmd.evidenceHash());
            return new Result(Outcome.CONTRACT_VIOLATION, pr.record(), halted,
                    pr.reason(), bridgeCallsFor(cmd));
        }
        AttemptRecord attempt = pr.record();

        // One attempt per (instruction_id, request_hash): never a second submission.
        if (attempt.phase().equals(AttemptRecord.PHASE_SUBMITTING)) {
            // A bridge round-trip may already be in flight for this attempt (crash
            // after SUBMITTING). Conservative: halt; never issue a second call.
            return unknownAndHalt(cmd, attempt, row,
                    "attempt already SUBMITTING on restart — uncertain whether the bridge was called");
        }
        if (attempt.phase().equals(AttemptRecord.PHASE_UNKNOWN)) {
            return unknownAndHalt(cmd, attempt, row, "attempt in UNKNOWN — reconciliation required");
        }
        if (AttemptRecord.TERMINAL_PHASES.contains(attempt.phase())) {
            return new Result(Outcome.DUPLICATE, attempt, row,
                    "already " + attempt.phase() + "; no second place request",
                    bridgeCallsFor(cmd));
        }

        // 3. SUBMITTING after durable acknowledgement (PREPARED -> SUBMITTING).
        crashHooks.afterPrepared();
        AttemptStore.TransitionResult sub = attempts.transition(
                attempt.executionAttemptId(), attempt.phaseEpoch(), AttemptRecord.PHASE_SUBMITTING);
        if (sub.outcome() != AttemptStore.TransitionOutcome.APPLIED) {
            return unknownAndHalt(cmd, sub.record() == null ? attempt : sub.record(), row,
                    "cannot transition to SUBMITTING (" + sub.reason() + ") — halted, no call");
        }
        crashHooks.afterSubmitting();

        // P3-007: re-validate immediately before the money-moving call with a
        // fresh read + fresh clock — PREPARED + SUBMITTING writes take arbitrary
        // time and an expired lease, concurrent halt, epoch bump, or fence steal
        // in that window must not issue an order.
        GateRow fresh = gateStore.read(cmd.executionPartitionId());
        long freshNow = clock.getAsLong();
        String staleReason = revalidationReason(cmd, fresh, freshNow);
        if (staleReason != null) {
            return unknownAndHalt(cmd, sub.record(), fresh == null ? row : fresh,
                    staleReason);
        }
        // 4. Exactly one bridge call, guarded against duplicate issuance.
        if (!issuedAttempts.add(cmd.executionAttemptId())) {
            throw new IllegalStateException(
                    "duplicate bridge issuance for " + cmd.executionAttemptId() + " — invariant violated");
        }
        crashHooks.beforeBridge();
        BridgeCaller.BridgeOutcome outcome;
        try {
            outcome = bridge.call(cmd);
        } catch (Exception e) {
            outcome = new BridgeCaller.BridgeOutcome(OutcomeKind.UNKNOWN, null,
                    "transport failure: " + e.getMessage());
        }
        crashHooks.afterBridge();

        // 5-6. Classify; persist call evidence; ambiguous -> UNKNOWN + halt.
        auditOutcome(cmd, sub.record(), outcome);
        if (outcome.ambiguous()) {
            AttemptStore.TransitionResult unk = attempts.transition(cmd.executionAttemptId(),
                    sub.record().phaseEpoch(), AttemptRecord.PHASE_UNKNOWN);
            // P3-133: a rejected UNKNOWN transition leaves durable SUBMITTING —
            // never report an outcome the durable row does not hold.
            if (unk.outcome() != AttemptStore.TransitionOutcome.APPLIED) {
                return unknownAndHalt(cmd, unk.record() == null ? sub.record() : unk.record(), row,
                        "UNKNOWN persist failed (" + unk.reason() + ") — halted, no assumed outcome");
            }
            return unknownAndHalt(cmd, unk.record() == null ? sub.record() : unk.record(), row,
                    "ambiguous bridge outcome: " + outcome.detail());
        }
        String terminal = outcome.kind() == OutcomeKind.ACCEPTED
                ? AttemptRecord.PHASE_ACCEPTED : AttemptRecord.PHASE_REJECTED;
        AttemptStore.TransitionResult finalRec = attempts.transition(cmd.executionAttemptId(),
                sub.record().phaseEpoch(), terminal);
        // P3-133: STALE_EPOCH/ILLEGAL/NOT_FOUND leaves durable SUBMITTING while
        // the caller would hear ACCEPTED — halt instead of diverging.
        if (finalRec.outcome() != AttemptStore.TransitionOutcome.APPLIED) {
            return unknownAndHalt(cmd, finalRec.record() == null ? sub.record() : finalRec.record(), row,
                    "terminal persist failed (" + finalRec.reason() + ") — halted, no assumed outcome");
        }
        return new Result(outcome.kind() == OutcomeKind.ACCEPTED ? Outcome.ACCEPTED : Outcome.REJECTED,
                finalRec.record() == null ? sub.record() : finalRec.record(),
                row, outcome.detail(), bridgeCallsFor(cmd));
    }

    /** Ephemeral fence: acquire the partition lease (mirrors the ZooKeeper-style sequence). */
    public GateStateStore.FenceResult acquireFence(String partitionId, long leaseMs) {
        return gateStore.acquire(partitionId, ownerInstanceId, leaseMs, clock.getAsLong());
    }

    /**
     * Pre-bridge re-check (P3-007): the same gate/fence/approval rules as entry,
     * evaluated on a fresh read with a fresh clock. Returns null when the call
     * may proceed, else the halt reason.
     */
    private String revalidationReason(Command cmd, GateRow fresh, long freshNow) {
        if (fresh == null) {
            return "gate row vanished before bridge — no call";
        }
        if (!fresh.accountScopeId().equals(cmd.accountScopeId())) {
            return "cross-scope changed before bridge — no call";
        }
        if (fresh.state() != GateState.ENABLED) {
            return "gate no longer ENABLED (state=" + fresh.state() + ") before bridge — no call";
        }
        if (fresh.epoch() != cmd.gateEpoch()) {
            return "epoch moved before bridge: command " + cmd.gateEpoch()
                    + " != gate epoch " + fresh.epoch() + " — no call";
        }
        if (!fresh.fenceValidFor(ownerInstanceId, cmd.gateFenceToken(), freshNow)) {
            return "fence invalid immediately before the call: owner/token/lease — no call";
        }
        if (!fresh.approvalsComplete() || !fresh.approvalsCover(cmd.evidenceHash())) {
            return "approval missing or for a different evidence package before bridge — no call";
        }
        return null;
    }

    private Result unknownAndHalt(Command cmd, AttemptRecord attempt, GateRow row, String reason) {
        GateRow halted = halt(cmd, row, reason, cmd.evidenceHash());
        return new Result(Outcome.UNKNOWN_HALTED, attempt, halted, reason, bridgeCallsFor(cmd));
    }

    private Result blocked(Command cmd, GateRow row, String reason) {
        return new Result(Outcome.BLOCKED, null, row, reason, bridgeCallsFor(cmd));
    }

    /**
     * Raise and durably VERIFY the safety halt (P3-134). The {@code expected}
     * witness can be stale (CAS rejection) or the halt can lose a race, so on an
     * unconfirmed result escalate to an unconditional halt (null expected = halt
     * regardless of generation, per the {@link GateStateStore#halt} contract) and
     * confirm the durable row is HALTED before returning.
     *
     * <p>Throwing on an unconfirmed halt is the fail-closed choice: reporting
     * {@code UNKNOWN_HALTED} while the gate could still be ENABLED would let the
     * next {@code execute()} pass the ENABLED check and place a second order.
     * Returning the VERIFIED row also means callers never record a gate state the
     * durable store does not hold.
     */
    private GateRow halt(Command cmd, GateRow row, String reason, String evidenceHash) {
        String partitionId = cmd.executionPartitionId();
        GateRow halted = gateStore.halt(partitionId, row, reason, evidenceHash, clock.getAsLong());
        haltObserver.run();
        if (halted == null || halted.state() != GateState.HALTED) {
            halted = gateStore.halt(partitionId, null, reason, evidenceHash, clock.getAsLong());
        }
        GateRow verified = halted != null ? halted : gateStore.read(partitionId);
        if (verified == null || verified.state() != GateState.HALTED) {
            throw new IllegalStateException("halt not durably HALTED for " + partitionId
                    + " — refusing to report UNKNOWN_HALTED");
        }
        return verified;
    }

    private void auditOutcome(Command cmd, AttemptRecord sub, BridgeCaller.BridgeOutcome o) {
        gateStore.audit(new AuditRecord(cmd.executionPartitionId(), "BRIDGE_OUTCOME",
                clock.getAsLong(), cmd.gateEpoch(), cmd.gateFenceToken(),
                o.kind() + (o.brokerOrderId() == null ? "" : " id=" + o.brokerOrderId())
                        + " detail=" + o.detail(),
                cmd.evidenceHash()));
    }

    private int bridgeCallsFor(Command cmd) {
        return issuedAttempts.contains(cmd.executionAttemptId()) ? 1 : 0;
    }
}
