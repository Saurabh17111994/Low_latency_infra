package com.trading.common.schema;

/**
 * EOD controller state machine + offload gate
 * (docs/08_implementation/01-foundation.md &rarr; "EOD controller and offload gate", orig L511).
 *
 * <p>State machine:
 * <pre>{@code
 * PENDING → WRITING → COMMITTED → VERIFYING → VERIFIED
 *                     ↘ FAILED_RETRYABLE
 *                     ↘ FAILED_MANUAL
 * }</pre>
 *
 * <p>Source data for a trading day SHALL not expire while the manifest is unverified,
 * retryable, or under reconciliation. Source data cannot expire unless state is
 * {@code VERIFIED}, and at least three complete trading days remain live.
 */
public enum EodControllerState {
    PENDING,             // waiting for end-of-day boundary
    WRITING,             // writing manifest + data to lake
    COMMITTED,           // lake commit succeeded; reconciliation pending
    VERIFYING,           // lake read-back / reconciliation in progress
    VERIFIED,            // safe; source retention may expire
    FAILED_RETRYABLE,    // offload/verify failed; retry with backoff (extends retention)
    FAILED_MANUAL;       // offload/verify failed; requires manual reconciliation

    /**
     * True when {@code this -> next} is a legal transition of the state machine.
     * Single source of truth for the transition table (mirrors the machine in
     * 02-schema-storage.md); {@code EodOffloadRecord.isLegalTransition}
     * delegates here.
     */
    public boolean canTransitionTo(EodControllerState next) {
        return switch (this) {
            case PENDING -> next == WRITING;
            case WRITING -> next == COMMITTED
                    || next == FAILED_RETRYABLE || next == FAILED_MANUAL;
            case COMMITTED -> next == VERIFYING
                    || next == FAILED_RETRYABLE || next == FAILED_MANUAL;
            case VERIFYING -> next == VERIFIED
                    || next == FAILED_RETRYABLE || next == FAILED_MANUAL;
            // P4-287/289: retry re-enters WRITING only. The old VERIFYING edge
            // let a WRITING failure (no content committed) skip COMMITTED and
            // go straight to verification — EodController.advance never takes
            // it (retries always re-enter WRITING), so it was a latent bypass.
            case FAILED_RETRYABLE -> next == WRITING
                    || next == FAILED_MANUAL;
            case FAILED_MANUAL -> next == PENDING;
            case VERIFIED -> false;
        };
    }

    /**
     * Necessary but NOT sufficient for source expiry: caller must also
     * enforce the 3-live-day floor (see EodPlanner/EodRetentionPolicy).
     * Only true for VERIFIED; VERIFIED alone does not permit expiry.
     */
    public boolean permitsSourceExpiry() {
        return this == VERIFIED;
    }

    /** Unverified or retryable states require retention extension. */
    public boolean requiresRetentionExtension() {
        return this == PENDING || this == WRITING || this == COMMITTED
                || this == VERIFYING || this == FAILED_RETRYABLE || this == FAILED_MANUAL;
    }

    /** Retryable failure — the controller should retry with backoff. */
    public boolean isRetryable() {
        return this == FAILED_RETRYABLE;
    }
}
