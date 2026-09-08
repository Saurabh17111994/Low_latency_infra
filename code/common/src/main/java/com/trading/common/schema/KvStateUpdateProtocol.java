package com.trading.common.schema;

/**
 * KV state-update protocol
 * (docs/08_implementation/01-foundation.md &rarr; "KV state update protocol", orig L477).
 *
 * <p>Projection update with version / duplicate / stale / regression / conflict checks.
 * Any non-clean result returns {@link Outcome#UNKNOWN} and must trigger quarantine + halt.
 */
public final class KvStateUpdateProtocol {

    private KvStateUpdateProtocol() {}

    public enum Outcome {
        APPLIED,    // clean newer version
        DUPLICATE,  // same version already present
        STALE,      // older than current
        REGRESSION, // value moved backward unexpectedly
        CONFLICT,   // version collision with different content
        UNKNOWN     // ambiguous; quarantine + halt
    }

    public static Outcome evaluate(long currentVersion, long incomingVersion, boolean contentMatches) {
        if (currentVersion < 0 || incomingVersion < 0) {
            return Outcome.UNKNOWN;
        }
        if (incomingVersion == currentVersion) {
            return contentMatches ? Outcome.DUPLICATE : Outcome.CONFLICT;
        }
        if (incomingVersion < currentVersion) {
            return contentMatches ? Outcome.STALE : Outcome.REGRESSION;
        }
        // Forward versions are version-clean only; value-level regression
        // (terminal/quantity moving backward) is NOT checked here and must
        // be validated by the caller even when APPLIED is returned.
        return Outcome.APPLIED;
    }

    /**
     * A non-clean outcome requires halt + quarantine.
     *
     * <p>Note: production halt semantics live at the call sites, which switch
     * on {@link Outcome} directly ({@code PositionProjector},
     * {@code PositionProjectionWriter}, {@code OrderLifecycleProjector},
     * {@code PositionsObservationOperator}) — every one of them already treats
     * STALE as a non-halting soft reject. Keep this method consistent with
     * those switches if it gains callers.
     */
    public static boolean requiresHalt(Outcome o) {
        return o != Outcome.APPLIED && o != Outcome.DUPLICATE;
    }
}
