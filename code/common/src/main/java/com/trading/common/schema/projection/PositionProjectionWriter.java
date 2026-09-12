package com.trading.common.schema.projection;

import com.trading.common.model.PositionState;
import com.trading.common.schema.KvStateUpdateProtocol;
import com.trading.common.schema.position.PositionLifecycle;
import com.trading.common.schema.position.PositionSnapshot;
import com.trading.common.schema.position.PositionsColumns;
import java.util.Objects;

/**
 * Serializer half of the position projection (T6, CHG-045): maps an
 * authoritative {@link NautilusPositionEvent} into a {@link PositionSnapshot}
 * (Positions KV row) and applies the version gate. It performs NO arithmetic —
 * open/closed/avg come unchanged from Nautilus; it only validates invariants
 * and rejects stale/conflict/inconsistent updates. A Nautilus event that
 * violates the quantity/state invariants is {@code POSITION_VIOLATION}
 * (quarantine + halt).
 */
public final class PositionProjectionWriter {

    private PositionProjectionWriter() {}

    public enum Outcome { APPLIED, DUPLICATE, STALE, VIOLATION }

    public record PositionWriteResult(Outcome outcome, PositionSnapshot snapshot,
            QuarantineReason reason, String detail) {

        public static PositionWriteResult applied(PositionSnapshot s) {
            return new PositionWriteResult(Outcome.APPLIED, s, null, null);
        }
        public static PositionWriteResult duplicate(PositionSnapshot s) {
            return new PositionWriteResult(Outcome.DUPLICATE, s, null, null);
        }
        /**
         * P3-500 sibling: {@code detail} names the rejected write and both versions. The caller
         * holds the event, and the violation arm below takes its detail the same way.
         */
        public static PositionWriteResult stale(PositionSnapshot s, String detail) {
            return new PositionWriteResult(Outcome.STALE, s, QuarantineReason.STALE_EVENT, detail);
        }
        public static PositionWriteResult violation(QuarantineReason r, String detail) {
            return new PositionWriteResult(Outcome.VIOLATION, null, r, detail);
        }
    }

    /**
     * Serialize the Nautilus event into a Positions row under the version gate.
     *
     * @param current current row (null = none)
     * @param event   the Nautilus-computed position event (authoritative)
     * @param nowMs   deterministic timestamp
     */
    public static PositionWriteResult apply(PositionSnapshot current,
            NautilusPositionEvent event, long nowMs) {
        Objects.requireNonNull(event, "event");

        // Invariant validation (no arithmetic) before the version gate.
        if (!eventInvariantsHold(event)) {
            return PositionWriteResult.violation(QuarantineReason.POSITION_VIOLATION,
                    "Nautilus event violates quantity/state invariants");
        }

        // P3-509: null-safe — a null sourceEventId in a stored row must route
        // to the version gate (quarantine path), not NPE past it.
        boolean contentMatches = current != null
                && Objects.equals(current.sourceEventId(), event.sourceEventId());
        // P3-170: the "empty slot" sentinel 0 collides with a genuine first
        // version 0 (sourceSequence >= 0 is legal) — a brand-new position's
        // first update at 0 would evaluate(0,0,false)=CONFLICT and quarantine
        // legitimate startup data. A null row can never be a collision.
        if (current == null) {
            return PositionWriteResult.applied(new PositionSnapshot(
                    event.positionId(),
                    event.tradeContextId(),
                    event.accountScopeId(),
                    event.instrumentToken(),
                    event.exchange(),
                    event.symbol(),
                    event.side(),
                    event.state(),
                    event.openQuantity(),
                    event.closedQuantity(),
                    event.averageEntryPaise(),
                    event.averageExitPaise(),
                    event.sourceEventId(),
                    event.sourceSequence(),
                    nowMs,
                    nowMs,
                    PositionsColumns.SCHEMA_VERSION_V2));
        }
        long currentVersion = current.sourceVersion();
        switch (KvStateUpdateProtocol.evaluate(currentVersion, event.sourceSequence(),
                contentMatches)) {
            case DUPLICATE -> { return PositionWriteResult.duplicate(current); }
            // P3-405: REGRESSION (older version, different content — the stored
            // row conflicts with a stale write, possible divergence) is not a
            // benign STALE replay — surface it as a violation with its own
            // reason so quarantine/diagnostics keep the distinction.
            case STALE -> {
                return PositionWriteResult.stale(current, "stale position write "
                        + event.positionId() + " version " + event.sourceSequence()
                        + " (current version " + current.sourceVersion() + ")");
            }
            case REGRESSION -> {
                return PositionWriteResult.violation(QuarantineReason.TERMINAL_REGRESSION,
                        "position version regression for " + event.positionId());
            }
            case CONFLICT, UNKNOWN -> {
                return PositionWriteResult.violation(QuarantineReason.POSITION_VIOLATION,
                        "version check for position " + event.positionId());
            }
            case APPLIED -> { /* fall through */ }
        }

        PositionSnapshot next = new PositionSnapshot(
                event.positionId(),
                event.tradeContextId(),
                event.accountScopeId(),
                event.instrumentToken(),
                event.exchange(),
                event.symbol(),
                event.side(),
                event.state(),
                event.openQuantity(),
                event.closedQuantity(),
                event.averageEntryPaise(),
                event.averageExitPaise(),
                event.sourceEventId(),
                event.sourceSequence(),
                current == null ? nowMs : current.createdTs(),
                nowMs,
                PositionsColumns.SCHEMA_VERSION_V2);
        return PositionWriteResult.applied(next);
    }

    /**
     * Validate the Nautilus-computed event without recomputing it: state is
     * consistent with the derived quantity reading, and quantities are sane.
     */
    static boolean eventInvariantsHold(NautilusPositionEvent e) {
        PositionState derived =
                PositionLifecycle.derive(e.openQuantity(), e.closedQuantity(), true);
        if (derived == PositionState.UNKNOWN) {
            return false;
        }
        if (e.state() == PositionState.FLAT) {
            return e.openQuantity() == 0 && e.closedQuantity() == 0;
        }
        // The declared state must match the quantity-derived reading — the
        // projection never recomputes, but it refuses an internally-contradictory
        // Nautilus event (e.g. open==closed yet OPEN).
        return e.state() == derived;
    }
}
