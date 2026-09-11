package com.trading.common.invariants;

/**
 * Failure invariant (docs/08_implementation/01-foundation.md &rarr; "Failure invariant", orig L684).
 *
 * <p>Defined behavior for each failure phase. Ambiguity is never guessed: it resolves to
 * {@link Disposition#UNKNOWN}, which forces quarantine + halt rather than a silent default.
 *
 * <p>P3-111: {@link #resolve} is the central default mapping from a failure
 * phase to its disposition, so call sites do not each invent one. Disposition can
 * still depend on call-site context (a BEFORE_ACK retry is safe for an idempotent
 * read, fatal for a money-moving call), so {@code resolve} is the <b>default</b>:
 * a call site with extra context may override it, but it must never resolve
 * ambiguity to anything but {@link Disposition#UNKNOWN}, and it must enforce the
 * halt with {@link #requiresQuarantineAndHalt}.
 */
public final class FailureInvariant {

    private FailureInvariant() {}

    public enum Phase {
        BEFORE_ACK, DURING_PROCESSING, AFTER_ACK, TIMEOUT, DUPLICATE, RESTART, STALE, CORRUPT
    }

    public enum Disposition {
        RETRY, DROP, HALT, QUARANTINE, UNKNOWN
    }

    /**
     * P3-111: the canonical default disposition for a failure phase. Exhaustive
     * over {@link Phase} so a new phase cannot be silently unmapped, and total
     * over null/unmapped input — anything ambiguous resolves to
     * {@link Disposition#UNKNOWN}, which {@link #requiresQuarantineAndHalt}
     * enforces. Money-moving failure phases (DURING_PROCESSING, TIMEOUT, RESTART,
     * STALE, CORRUPT) deliberately default to UNKNOWN rather than a retry: the
     * side-effect outcome is unknown, so the caller must reconcile.
     */
    public static Disposition resolve(Phase phase) {
        if (phase == null) {
            return onAmbiguity();
        }
        return switch (phase) {
            // No side effect can have escaped yet — the request may be retried.
            case BEFORE_ACK -> Disposition.RETRY;
            // The side effect may or may not have landed — never guess.
            case DURING_PROCESSING, TIMEOUT, RESTART, STALE, CORRUPT -> Disposition.UNKNOWN;
            // The outcome is already decided or already applied — never re-apply.
            case AFTER_ACK, DUPLICATE -> Disposition.DROP;
        };
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
