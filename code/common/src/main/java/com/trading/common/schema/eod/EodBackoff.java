package com.trading.common.schema.eod;

import java.util.Random;

/**
 * Capped exponential backoff with jitter for EOD controller retries (SCH-23).
 * The controller owns retry/backoff; the schedule is computed here so it is
 * unit-testable and deterministic under a seeded {@link Random}.
 *
 * <p>Delay = min(base × 2^min(retryCount, 20), max) × jitter(0.8..1.2),
 * clamped to max. A failed offload that keeps retrying therefore backs off
 * until the cap, and the jitter prevents thundering-herd re-runs across
 * tables at the same EOD boundary.
 */
public final class EodBackoff {

    /** Governing starting point: 1 s base, 5 min cap (tuning keys, not pinned). */
    public static final long DEFAULT_BASE_MS = 1_000L;
    public static final long DEFAULT_MAX_MS = 300_000L;

    private EodBackoff() {}

    private static final Random SHARED = new Random();

    /** Shared {@link Random} for callers without their own (records, planners). */
    public static Random rng() {
        return SHARED;
    }

    /** Retry delay for {@code retryCount} (0 = first failure), clamped to [jittered, max]. */
    public static long nextRetryDelayMs(int retryCount, long baseMs, long maxMs, Random rng) {
        if (baseMs <= 0 || maxMs < baseMs) {
            throw new IllegalArgumentException("baseMs and maxMs must satisfy 0 < baseMs <= maxMs, "
                    + "got base=" + baseMs + " max=" + maxMs);
        }
        if (rng == null) {
            throw new IllegalArgumentException("rng must not be null");
        }
        long exponent = Math.min(Math.max(retryCount, 0), 20);
        // P4-270: saturate before multiplying — baseMs * (1L << exponent)
        // overflows to negative for huge tuned baseMs, and min(negative, max)
        // returns the negative (tight retry loop via past nextRetryAtMs).
        long shift = 1L << exponent;
        long capped = baseMs > maxMs / shift ? maxMs : baseMs * shift;
        double jitter = 0.8 + 0.4 * rng.nextDouble(); // [0.8, 1.2)
        return Math.min((long) (capped * jitter), maxMs);
    }

    /** Absolute epoch-millis of the next retry. */
    public static long nextRetryAtMs(long nowMs, int retryCount, long baseMs, long maxMs,
            Random rng) {
        return nowMs + nextRetryDelayMs(retryCount, baseMs, maxMs, rng);
    }
}
