package com.trading.common.schema.position;

import com.trading.common.model.PositionState;

/**
 * Position lifecycle (SCH-20; DEC-013): FLAT → OPEN → REDUCING → CLOSED with
 * legal re-entry, plus the derived-state rule that keeps the machine honest.
 *
 * <p>Two complementary readings: {@link #derive} is the pure quantity
 * function (no write path can fabricate a state from the quantities alone);
 * the projector additionally applies a cycle-aware rule (a fresh BUY after a
 * full close re-opens the cycle to OPEN even while the cumulative
 * {@code closed_quantity} is positive — see {@code PositionProjector}).
 * {@link #isLegalTransition} rejects impossible jumps (FLAT → CLOSED without
 * ever opening, an exit that overshoots the open quantity).
 */
public final class PositionLifecycle {

    private PositionLifecycle() {}

    /** Derive the state from the quantity invariants. */
    public static PositionState derive(long openQuantity, long closedQuantity, boolean traded) {
        if (openQuantity < 0 || closedQuantity < 0 || openQuantity < closedQuantity) {
            return PositionState.UNKNOWN;
        }
        if (openQuantity == 0 && closedQuantity == 0) {
            return traded ? PositionState.CLOSED : PositionState.FLAT;
        }
        if (closedQuantity == 0) {
            return PositionState.OPEN;
        }
        return openQuantity > closedQuantity
                ? PositionState.REDUCING
                : PositionState.CLOSED;
    }

    /**
     * Validate a transition from the current state to the derived next state.
     * Same-state steps (adding to an OPEN position, further partial exits) are
     * legal; impossible jumps are not.
     */
    public static boolean isLegalTransition(PositionState from, PositionState to) {
        // P3-164: null must poison like UNKNOWN. Prior state is sourced from
        // PositionSnapshot.state(), which the record does not reject, so a corrupted/hydrated
        // null reached `switch (from)` and threw — an uncontrolled exception instead of the
        // controlled `false` -> VIOLATION. `isLegalTransition(null, null)` also returned true via
        // the `from == to` shortcut, validating a transition between two absent states.
        if (from == null || to == null) {
            return false;
        }
        if (from == PositionState.UNKNOWN || to == PositionState.UNKNOWN) {
            return false;
        }
        if (from == to) {
            return true;
        }
        return switch (from) {
            case FLAT -> to == PositionState.OPEN;
            case OPEN -> to == PositionState.REDUCING || to == PositionState.CLOSED;
            case REDUCING -> to == PositionState.OPEN || to == PositionState.CLOSED;
            case CLOSED -> to == PositionState.OPEN;
            case UNKNOWN -> false;
        };
    }
}
