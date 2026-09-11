package com.trading.common.invariants;

import java.util.EnumSet;
import java.util.Set;

/**
 * Order-safety invariant (docs/08_implementation/01-foundation.md &rarr; "Order safety invariant", orig L699).
 *
 * <p>Pre-call gate for any money-moving call. All checks must pass to {@link GateDecision#PROCEED};
 * any failed check yields {@link GateDecision#NO_CALL}; an unknown broker outcome yields
 * {@link GateDecision#HALTED}.
 */
public final class OrderSafetyGate {

    private OrderSafetyGate() {}

    public enum GateCheck {
        IDENTITY_RESOLVED, STATE_VERIFIED, CHANGELOG_CONSISTENT, BROKER_OUTCOME_KNOWN
    }

    public enum GateDecision {
        PROCEED, NO_CALL, HALTED
    }

    public static GateDecision decide(Set<GateCheck> passed) {
        // P3-115: null is ambiguity — fail closed to HALTED, never NPE on the
        // pre-money-call path.
        if (passed == null) {
            return GateDecision.HALTED;
        }
        if (passed.containsAll(EnumSet.allOf(GateCheck.class))) {
            return GateDecision.PROCEED;
        }
        if (!passed.contains(GateCheck.BROKER_OUTCOME_KNOWN)) {
            return GateDecision.HALTED;
        }
        return GateDecision.NO_CALL;
    }
}
