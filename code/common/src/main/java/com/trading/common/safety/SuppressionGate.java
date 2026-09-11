package com.trading.common.safety;

/**
 * Decision gate for slot-scoped safety suppression (plan.md &sect;
 * "Slot-scoped safety propagation", rules 2-3). While a slot is unsafe,
 * NEW candidates/rankings/reservations/decisions for that slot's tokens are
 * suppressed and any not-yet-published in-flight decision is discarded;
 * healthy slots continue. A published decision is never retracted.
 *
 * <p>Post-recovery: {@code RECOVERED} admits only post-recovery input — a
 * candidate evaluated from data older than the recovery epoch (i.e. from the
 * failed connection generation) is still suppressed. Tick presence never
 * clears an unsafe state; only a {@code RECOVERED} row does.
 */
public final class SuppressionGate {

    private SuppressionGate() {}

    /** Outcome of evaluating one candidate/decision against the tracker. */
    public enum Verdict {
        /** Slot safe (or token unassigned): proceed normally. */
        ALLOW,
        /** Slot unsafe: do not create new candidates/rankings/decisions. */
        SUPPRESS_NEW,
        /** Slot unsafe and the in-flight decision is unpublished: drop it. */
        DISCARD_INFLIGHT
    }

    /**
     * Context of an in-flight decision (the compute pipeline does not exist
     * yet, so the consumer supplies this when it has one).
     *
     * @param published    true once the decision was emitted downstream
     */
    public record InFlightDecision(boolean published) {}

    /**
     * @param tracker    tracker holding per-slot safety state
     * @param token      instrument token the candidate/decision is about
     * @param inputEpoch connection epoch of the input data being evaluated
     * @param inFlight   in-flight decision context, or {@code null} for NEW work
     */
    public static Verdict evaluate(SafetyStateTracker tracker, long token,
                                   long inputEpoch, InFlightDecision inFlight) {
        String slotId = tracker.slotAssignment().slotIdOf(token);
        if (slotId == null) {
            return Verdict.ALLOW; // unassigned token — not our suppression domain
        }
        SlotSafetyState state = tracker.stateOf(slotId);
        if (state == null) {
            return Verdict.ALLOW; // never unsafe — nothing to suppress
        }
        if (state.status() == SlotSafetyStatus.UNSAFE) {
            if (inFlight == null) {
                return Verdict.SUPPRESS_NEW;
            }
            return inFlight.published() ? Verdict.ALLOW : Verdict.DISCARD_INFLIGHT;
        }
        // RECOVERED: admit only post-recovery input (newer connection epoch).
        // P3-127: the stale case mirrors the UNSAFE branch — an unpublished
        // stale in-flight must DISCARD (callers branch on it), and a published
        // one is ALLOW (published decisions are never retracted).
        if (inputEpoch < state.connectionEpoch()) {
            if (inFlight == null) {
                return Verdict.SUPPRESS_NEW;
            }
            return inFlight.published() ? Verdict.ALLOW : Verdict.DISCARD_INFLIGHT;
        }
        return Verdict.ALLOW;
    }
}
