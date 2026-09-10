package com.trading.compute.signaljob;

/**
 * Canonical-candle policy (CANDLE-KV-REPLAY-001 A2).
 *
 * <p>A candle row is <b>canonical</b> iff both version columns equal the
 * expected values exactly: {@code algorithm_version == expectedAlgorithm}
 * <em>and</em> {@code configuration_version == expectedConfiguration}. Any
 * other combination — an algorithm iteration, an unversioned row, a blank
 * value — is non-canonical and must be excluded from the canonical current
 * state (the KV projection and the migration audit).
 *
 * <p>{@code output_ts} is deliberately <em>not</em> part of the identity:
 * two rows for the same {@code (instrument_token, window_start)} that agree on
 * the business columns are the same canonical candle re-emitted by a replay or
 * restart, no matter when they were emitted. The KV table therefore converges
 * to one row per key regardless of replay history.
 *
 * <p>Strict exact-match semantics: {@code null} and blank values never match,
 * and whitespace is not trimmed (a padded value is a different version
 * string, not an equivalent one).
 */
public final class CanonicalCandlePolicy {

    private CanonicalCandlePolicy() {}

    /**
     * @param algorithmVersion         the row's {@code algorithm_version}
     * @param configurationVersion     the row's {@code configuration_version}
     * @param expectedAlgorithm        the pinned expected algorithm version
     * @param expectedConfiguration    the pinned expected configuration version
     * @return true iff both row versions exactly equal the expected values
     */
    public static boolean isCanonical(
            String algorithmVersion,
            String configurationVersion,
            String expectedAlgorithm,
            String expectedConfiguration) {
        return expectedAlgorithm != null
                && !expectedAlgorithm.isBlank()
                && expectedAlgorithm.equals(algorithmVersion)
                && expectedConfiguration != null
                && !expectedConfiguration.isBlank()
                && expectedConfiguration.equals(configurationVersion);
    }
}
