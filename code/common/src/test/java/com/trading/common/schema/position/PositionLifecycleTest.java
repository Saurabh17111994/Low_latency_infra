package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.PositionState;
import org.junit.jupiter.api.Test;

/** SCH-20 lifecycle machine: derived state + legal transition matrix. */
class PositionLifecycleTest {

    @Test
    void deriveFromQuantities() {
        assertThat(PositionLifecycle.derive(0, 0, false)).isEqualTo(PositionState.FLAT);
        assertThat(PositionLifecycle.derive(0, 0, true)).isEqualTo(PositionState.CLOSED);
        assertThat(PositionLifecycle.derive(10, 0, true)).isEqualTo(PositionState.OPEN);
        assertThat(PositionLifecycle.derive(10, 3, true)).isEqualTo(PositionState.REDUCING);
        assertThat(PositionLifecycle.derive(10, 10, true)).isEqualTo(PositionState.CLOSED);
        assertThat(PositionLifecycle.derive(-1, 0, true)).isEqualTo(PositionState.UNKNOWN);
        assertThat(PositionLifecycle.derive(5, 8, true)).isEqualTo(PositionState.UNKNOWN);
    }

    @Test
    void legalTransitionsMatrix() {
        // Same-state steps are legal.
        assertThat(PositionLifecycle.isLegalTransition(PositionState.OPEN, PositionState.OPEN))
                .isTrue();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.REDUCING,
                PositionState.REDUCING)).isTrue();
        // Forward steps.
        assertThat(PositionLifecycle.isLegalTransition(PositionState.FLAT, PositionState.OPEN))
                .isTrue();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.OPEN,
                PositionState.REDUCING)).isTrue();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.OPEN, PositionState.CLOSED))
                .isTrue();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.REDUCING,
                PositionState.CLOSED)).isTrue();
        // Re-entry.
        assertThat(PositionLifecycle.isLegalTransition(PositionState.CLOSED, PositionState.OPEN))
                .isTrue();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.REDUCING,
                PositionState.OPEN)).isTrue();
        // Impossible jumps.
        assertThat(PositionLifecycle.isLegalTransition(PositionState.FLAT, PositionState.CLOSED))
                .isFalse();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.FLAT, PositionState.REDUCING))
                .isFalse();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.OPEN, PositionState.FLAT))
                .isFalse();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.CLOSED, PositionState.FLAT))
                .isFalse();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.CLOSED, PositionState.CLOSED))
                .isTrue();
        // UNKNOWN poisons everything.
        assertThat(PositionLifecycle.isLegalTransition(PositionState.UNKNOWN, PositionState.OPEN))
                .isFalse();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.OPEN, PositionState.UNKNOWN))
                .isFalse();
    }

    @Test void closedReentryMintsNewPositionIdCycle() {
        // FLAT -> OPEN -> CLOSED -> OPEN cycle is legal per dossier §Position states
        assertThat(PositionLifecycle.isLegalTransition(PositionState.FLAT, PositionState.OPEN)).isTrue();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.OPEN, PositionState.CLOSED)).isTrue();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.CLOSED, PositionState.OPEN)).isTrue();
        // Unknown poisons
        assertThat(PositionLifecycle.isLegalTransition(PositionState.UNKNOWN, PositionState.FLAT)).isFalse();
    }
    @Test void reducingToClosedAndReopen() {
        assertThat(PositionLifecycle.derive(10, 5, true)).isEqualTo(PositionState.REDUCING);
        assertThat(PositionLifecycle.derive(10, 10, true)).isEqualTo(PositionState.CLOSED);
        assertThat(PositionLifecycle.derive(10, 0, true)).isEqualTo(PositionState.OPEN);
    }

    @Test
    void nullStatesPoisonLikeUnknown() {
        // P3-164: (null, null) returned TRUE through the `from == to` shortcut — validating a
        // transition between two absent states — while (null, OPEN) threw from `switch (from)`.
        // Both are now the controlled `false` the contract promises.
        assertThat(PositionLifecycle.isLegalTransition(null, null)).isFalse();
        assertThat(PositionLifecycle.isLegalTransition(null, PositionState.OPEN)).isFalse();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.FLAT, null)).isFalse();
        assertThat(PositionLifecycle.isLegalTransition(PositionState.OPEN, null)).isFalse();
    }

}
