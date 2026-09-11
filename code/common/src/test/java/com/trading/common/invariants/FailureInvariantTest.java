package com.trading.common.invariants;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.invariants.FailureInvariant.Disposition;
import com.trading.common.invariants.FailureInvariant.Phase;
import org.junit.jupiter.api.Test;

/**
 * P3-111: the failure-phase &rarr; disposition mapping is central, exhaustive and
 * total — a new phase cannot be silently unmapped and ambiguity can never resolve
 * to anything but {@link Disposition#UNKNOWN} (which forces quarantine + halt).
 */
class FailureInvariantTest {

    @Test
    void everyPhaseHasAMappingAndAmbiguityIsUnknown() {
        for (Phase phase : Phase.values()) {
            assertThat(FailureInvariant.resolve(phase)).as("%s", phase).isNotNull();
        }
        assertThat(FailureInvariant.resolve(null)).isEqualTo(Disposition.UNKNOWN);
        assertThat(FailureInvariant.onAmbiguity()).isEqualTo(Disposition.UNKNOWN);
    }

    @Test
    void moneyMovingPhasesResolveToUnknownNotRetry() {
        // The side effect may already have landed — a retry would double-apply,
        // so these must never resolve to RETRY.
        for (Phase phase : new Phase[]{Phase.DURING_PROCESSING, Phase.TIMEOUT, Phase.RESTART,
                Phase.STALE, Phase.CORRUPT}) {
            assertThat(FailureInvariant.resolve(phase)).as("%s", phase)
                    .isEqualTo(Disposition.UNKNOWN);
        }
        // Before any side effect leaks a retry is safe; already-decided outcomes
        // and duplicates must not be re-applied.
        assertThat(FailureInvariant.resolve(Phase.BEFORE_ACK)).isEqualTo(Disposition.RETRY);
        assertThat(FailureInvariant.resolve(Phase.AFTER_ACK)).isEqualTo(Disposition.DROP);
        assertThat(FailureInvariant.resolve(Phase.DUPLICATE)).isEqualTo(Disposition.DROP);
    }

    @Test
    void everyAmbiguousResolutionRequiresQuarantineAndHalt() {
        assertThat(FailureInvariant.requiresQuarantineAndHalt(FailureInvariant.resolve(null))).isTrue();
        assertThat(FailureInvariant.requiresQuarantineAndHalt(Disposition.UNKNOWN)).isTrue();
        assertThat(FailureInvariant.requiresQuarantineAndHalt(null)).isTrue();
        assertThat(FailureInvariant.requiresQuarantineAndHalt(Disposition.DROP)).isFalse();
        assertThat(FailureInvariant.requiresQuarantineAndHalt(Disposition.RETRY)).isFalse();
    }
}
