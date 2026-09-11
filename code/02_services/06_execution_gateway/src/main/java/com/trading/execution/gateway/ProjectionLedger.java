package com.trading.execution.gateway;

import java.util.Map;
import java.util.Objects;

/**
 * Durable projection workflow states. Cross-table writes are deliberately not atomic.
 *
 * <p>P3-322: callers must persist {@code nextState} only <i>after</i> the
 * corresponding side effect succeeds (at-least-once replay of idempotent
 * upserts); the reverse order silently loses on crash between ledger-put and
 * side effect. ProjectionApplier already does side-effect-first.
 */
public final class ProjectionLedger {
    public enum State { RECEIVED, AUDIT_WRITTEN, LIFECYCLE_APPLIED,
        POSITION_APPLIED_OR_NOT_REQUIRED, QUARANTINED, FAILED, COMPLETE }
    private static final Map<State, State> NEXT = Map.of(
            State.RECEIVED, State.AUDIT_WRITTEN,
            State.AUDIT_WRITTEN, State.LIFECYCLE_APPLIED,
            State.LIFECYCLE_APPLIED, State.POSITION_APPLIED_OR_NOT_REQUIRED,
            State.POSITION_APPLIED_OR_NOT_REQUIRED, State.COMPLETE);
    private ProjectionLedger() {}

    public static State advance(State current, State requested) {
        // P3-100/P3-323: fail fast — Map.of rejects null keys with a bare NPE,
        // and (null, null) would return null as a "valid" State that later NPEs
        // far from the source (ledger.put does e.state().name()).
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(requested, "requested");
        if (current == requested) return current;
        // P3-099: QUARANTINED/FAILED are terminal dispositions reachable from
        // any forward state — without them a poison pill has no durable end
        // state and incomplete() returns it forever (infinite retry, and every
        // recovery scan re-appends the non-idempotent quarantine LOG row).
        if ((requested == State.QUARANTINED || requested == State.FAILED) && !terminal(current)) return requested;
        if (NEXT.get(current) != requested) throw new IllegalStateException(
                "invalid ledger transition " + current + " -> " + requested);
        return requested;
    }
    public static boolean terminal(State state) {
        // P3-100/P3-324: terminal(null)==false misclassifies corruption as
        // recoverable work — fail fast instead.
        Objects.requireNonNull(state, "state");
        // P3-099/P3-320: QUARANTINED/FAILED end the recovery walk exactly like
        // COMPLETE — ProjectionApplier must treat them as terminal too.
        return state == State.COMPLETE || state == State.QUARANTINED || state == State.FAILED;
    }
    public static boolean recoverable(State state) { return !terminal(state); }
}
