package com.trading.common.model;

/**
 * Runtime validator for {@link GateState} and {@link AttemptPhase} transitions.
 *
 * <p>Topology-only: every transition is checked against the canonical legal
 * matrix, and illegal transitions are rejected with an auditable result that
 * carries the rejection detail. The contract:
 *
 * <ul>
 *   <li>Gate transitions compare current epoch/phase before proceeding</li>
 *   <li>Every transition is auditable (result carries reason + detail)</li>
 *   <li>Any state transitions to {@link GateState#HALTED}, even on a stale
 *       epoch — a delayed halt must still fence a live gate</li>
 *   <li>Stale-epoch requests are rejected without side-effects (except halts)</li>
 *   <li>Null inputs fail closed ({@code rejected}) — never throw, never NPE</li>
 * </ul>
 *
 * <p>What this validator does NOT decide (enforced at the store/gate boundary,
 * which owns identity, time, and durability):
 * <ul>
 *   <li>single-operator approval + evidence binding (see the gate store's
 *       {@code approve} + {@code approvalsCover}) — an
 *       {@code APPROVAL_PENDING → ENABLED} "allowed" here is topology-only and
 *       advisory; the durable boundary is the authority;</li>
 *   <li>fence ownership/lease liveness (see {@code fenceValidFor} at the gate
 *       boundary, checked again immediately before any bridge call);</li>
 *   <li>forward-epoch gaps ({@code requestEpoch > currentEpoch}) are surfaced
 *       in the result detail for restart/hydrate/replay callers — the caller
 *       that owns epoch bookkeeping decides.</li>
 * </ul>
 *
 * <p>The legal matrices are not defined here: {@link AttemptPhase#legalTargets}
 * and {@link GateState#legalTargets} are the single source of truth (T5
 * reconciliation, CHG-044). This validator delegates to those tables, and the
 * attempt-store writer routes its submission/reconciliation legality through
 * this validator — one chain, so no two meanings can exist for the same
 * transition. The Rust gate ({@code code/02_services/04_executor/src/gate.rs})
 * enforces the same enablement path structurally ({@code transition} accepts
 * only the forward steps; {@code enable} requires epoch + authorized approval
 * + bound evidence); it is a same-rules re-implementation, not a table shared
 * with this file.
 *
 * <p>Corrected vs the pre-T5 matrix: ACCEPTED &rarr; REJECTED is now rejected
 * (ACCEPTED is terminal per the attempt-store's TERMINAL_PHASES); terminal
 * phases can no longer become UNKNOWN; PREPARED can no longer go to CANCELLED
 * or UNKNOWN (its only exit is SUBMITTING); HALTED &rarr; APPROVAL_PENDING and
 * APPROVAL_PENDING &rarr; RECONCILING are rejected (they skip the only
 * enablement path / silently re-reconcile instead of halting).
 *
 * <p>Source: docs/08_implementation/05-execution-core.md &rarr; "State machines"
 * and "Attempt protocol"; docs/08_implementation/01-foundation.md &rarr;
 * "Order safety invariant" (orig L699).
 */
public final class GateTransitionValidator {

    private GateTransitionValidator() {}

    /**
     * Validate a gate-state transition.
     *
     * @param currentState the gate's current state (never null)
     * @param targetState  the desired target state (never null)
     * @param currentEpoch the gate's current epoch
     * @param requestEpoch the epoch of the transition request
     * @return the validation result
     */
    public static GateResult validateGateTransition(
            GateState currentState,
            GateState targetState,
            long currentEpoch,
            long requestEpoch) {

        // P3-341: fail closed on null — never NPE, never allow null == null.
        if (currentState == null || targetState == null) {
            return GateResult.rejected(currentState, targetState,
                    "null gate state: current=" + currentState + " target=" + targetState);
        }

        // P3-120: safety halt always lands — bypass stale-epoch so a delayed
        // halt still fences a live gate. Idempotent halt stays idempotent.
        if (targetState == GateState.HALTED) {
            if (currentState == GateState.HALTED) {
                return GateResult.allowed(currentState, targetState,
                        "idempotent: already in HALTED");
            }
            return GateResult.allowed(currentState, targetState,
                    "safety halt: " + currentState + " → HALTED"
                            + (requestEpoch < currentEpoch
                                ? " (stale epoch " + requestEpoch + " < " + currentEpoch
                                    + ", halt still applies)" : ""));
        }

        // 1. Stale-epoch rejection — the request is for an old generation.
        // R-076: the audit trail must record the ACTUAL current state as the
        // from-state — the old code reported HALTED, which would make an
        // operator believe the gate was already halted when it may have been
        // ENABLED.
        if (requestEpoch < currentEpoch) {
            return GateResult.rejected(currentState, targetState,
                    "stale epoch: request=" + requestEpoch + " < current=" + currentEpoch
                            + "; epoch mismatch may indicate a lost lease or delayed message");
        }

        // 2. Same-state idempotent — no-op
        if (currentState == targetState) {
            return GateResult.allowed(currentState, targetState,
                    "idempotent: already in " + targetState);
        }

        // 3. Check legal transition against the canonical matrix.
        boolean legal = currentState.legalTargets().contains(targetState);
        if (!legal) {
            return GateResult.rejected(currentState, targetState,
                    "illegal transition: " + currentState + " → " + targetState);
        }

        // 4. Forward-epoch gap (requestEpoch > currentEpoch)
        // Allowed but logged — the caller owns epoch management.
        // The executor fencing lease must still be valid.

        return GateResult.allowed(currentState, targetState,
                "legal transition: " + currentState + " → " + targetState
                        + (requestEpoch > currentEpoch
                            ? " (forward epoch " + requestEpoch + ")"
                            : ""));
    }

    /**
     * Validate an attempt-phase transition.
     *
     * <p>Every attempt transition must be auditable. Only SUBMITTING can become
     * UNKNOWN (network failure, timeout, crash); terminal phases never become
     * UNKNOWN. UNKNOWN &rarr; ACCEPTED / REJECTED / CANCELLED is legal in the
     * full matrix but is <b>reconciliation-only</b> — callers must route it
     * through {@code resolveUnknown}, never through the submission path
     * (see {@link #isReconciliationOnly}).
     *
     * @param currentPhase the attempt's current phase
     * @param targetPhase  the desired target phase
     * @param reason       human-readable reason (for audit trail; may be null,
     *                     in which case the verdict detail is carried as reason)
     * @return the validation result (never throws on null)
     */
    public static AttemptResult validateAttemptTransition(
            AttemptPhase currentPhase,
            AttemptPhase targetPhase,
            String reason) {

        // P3-342: fail closed on null — never NPE, never allow null == null.
        if (currentPhase == null || targetPhase == null) {
            return AttemptResult.rejected(currentPhase, targetPhase, reason,
                    "null attempt phase: current=" + currentPhase + " target=" + targetPhase);
        }

        // Same-phase — no-op (idempotent)
        if (currentPhase == targetPhase) {
            return AttemptResult.allowed(currentPhase, targetPhase, reason,
                    "idempotent: already in " + targetPhase);
        }

        boolean legal = AttemptPhase.isLegal(currentPhase, targetPhase);
        if (!legal) {
            return AttemptResult.rejected(currentPhase, targetPhase, reason,
                    "illegal transition: " + currentPhase + " → " + targetPhase);
        }

        return AttemptResult.allowed(currentPhase, targetPhase, reason,
                "legal transition: " + currentPhase + " → " + targetPhase);
    }

    /**
     * Whether {@code from -> to} is legal on the submission path.
     *
     * <p>Submission covers PREPARED &rarr; SUBMITTING and the SUBMITTING exits
     * (ACCEPTED / REJECTED / CANCELLED / UNKNOWN). UNKNOWN's exits are
     * reconciliation-only and return false here — route them through
     * {@link #isReconciliationOnly} / {@code resolveUnknown} instead.
     */
    public static boolean isSubmissionLegal(AttemptPhase from, AttemptPhase to) {
        if (from == null || to == null || from == to) {
            return false;
        }
        return !from.isReconciliationSource() && from.legalTargets().contains(to);
    }

    /**
     * Whether {@code from -> to} is legal <b>only</b> through the explicit
     * reconciliation path (UNKNOWN &rarr; terminal). Callers that must separate
     * the submission path from reconciliation (the attempt-store writer, the
     * durable command gate) use this to route UNKNOWN exits through
     * {@code resolveUnknown} instead of the submission path.
     */
    public static boolean isReconciliationOnly(AttemptPhase from, AttemptPhase to) {
        return from != null && from.isReconciliationSource()
                && from.legalTargets().contains(to);
    }

    // ---- result types ----

    /** Outcome of a gate-state transition validation. Never throws on null. */
    public record GateResult(
            boolean allowed,
            GateState from,
            GateState to,
            String reason,
            String detail) {

        public static GateResult allowed(GateState from, GateState to, String detail) {
            return new GateResult(true, from, to, detail, detail);
        }

        public static GateResult rejected(GateState from, GateState to, String detail) {
            return new GateResult(false, from, to, detail, detail);
        }
    }

    /** Outcome of an attempt-phase transition validation. Never throws on null. */
    public record AttemptResult(
            boolean allowed,
            AttemptPhase from,
            AttemptPhase to,
            String reason,
            String detail) {

        public static AttemptResult allowed(AttemptPhase from, AttemptPhase to,
                                            String reason, String detail) {
            return new AttemptResult(true, from, to,
                    reason == null ? detail : reason, detail == null ? reason : detail);
        }

        public static AttemptResult rejected(AttemptPhase from, AttemptPhase to,
                                             String reason, String detail) {
            return new AttemptResult(false, from, to,
                    reason == null ? detail : reason, detail == null ? reason : detail);
        }
    }
}
