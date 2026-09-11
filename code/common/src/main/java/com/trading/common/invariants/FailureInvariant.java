package com.trading.common.invariants;

/**
 * Failure invariant (docs/08_implementation/01-foundation.md &rarr; "Failure invariant", orig L684).
 *
 * <p>Defined behavior for each failure phase. Ambiguity is never guessed: it resolves to
 * {@link Disposition#UNKNOWN}, which forces quarantine + halt rather than a silent default.
 *
 * <p>P3-111: the Phase/Disposition pair is vocabulary, not a central mapping —
 * disposition depends on call-site context (a BEFORE_ACK retry is safe for an
 * idempotent read, fatal for a money-moving call), so no single Phase-&gt;Disposition
 * table can be correct. Each call site decides its disposition and defaults to
 * UNKNOWN on ambiguity; use {@link #requiresQuarantineAndHalt} to enforce the halt.
 */
public final class FailureInvariant {

    private FailureInvariant() {}

    public enum Phase {
        BEFORE_ACK, DURING_PROCESSING, AFTER_ACK, TIMEOUT, DUPLICATE, RESTART, STALE, CORRUPT
    }

    public enum Disposition {
        RETRY, DROP, HALT, QUARANTINE, UNKNOWN
    }

    /** Ambiguity always resolves to UNKNOWN (no silent success). Callers must quarantine + halt on UNKNOWN. */
    public static Disposition onAmbiguity() {
        return Disposition.UNKNOWN;
    }

    /**
     * P3-112: enforcement guard — returns true when the disposition requires
     * quarantine + halt (null/UNKNOWN/QUARANTINE/HALT). A boolean, not a
     * throwing helper, so safety paths never need try/catch to stay halted.
     */
    public static boolean requiresQuarantineAndHalt(Disposition disposition) {
        return disposition == null || disposition == Disposition.UNKNOWN
                || disposition == Disposition.QUARANTINE || disposition == Disposition.HALT;
    }
}
