package com.trading.common.version;

/**
 * Evidence-gated version placeholders
 * (docs/08_implementation/01-foundation.md &rarr; "Placeholder policy", orig L178).
 *
 * <p>Each MUST be replaced by a pinned, evidence-verified value before the corresponding
 * live-money path is enabled. They are intentionally NOT real versions, so a forgotten
 * substitution is obvious rather than silently wrong.
 */
public final class PlaceholderVersions {

    private PlaceholderVersions() {}

    public static final String BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED = "BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED";
    public static final String FLINK_VERSION_TO_BE_PINNED = "FLINK_VERSION_TO_BE_PINNED";
    public static final String FLUSS_VERSION_TO_BE_PINNED = "FLUSS_VERSION_TO_BE_PINNED";
    public static final String ARROW_API_CONTRACT_TO_BE_VERIFIED = "ARROW_API_CONTRACT_TO_BE_VERIFIED";
    public static final String OPENALGO_API_CONTRACT_TO_BE_VERIFIED = "OPENALGO_API_CONTRACT_TO_BE_VERIFIED";
    public static final String SCHEMA_LIFECYCLE_TO_BE_VERIFIED = "SCHEMA_LIFECYCLE_TO_BE_VERIFIED";

    /**
     * A placeholder is any value carrying the sentinel marker {@code _TO_BE_PINNED}
     * or {@code _TO_BE_VERIFIED} (R-268).
     *
     * <p>The previous implementation compared against the constant <em>values</em>.
     * But the class contract says each constant must be replaced by a pinned
     * value before live money — the moment the constants are updated, an exact
     * equality check against them silently stops matching and the gate opens.
     * Matching the sentinel <em>shape</em> keeps working regardless of what the
     * constants currently hold.
     */
    public static boolean isPlaceholder(String value) {
        if (value == null) {
            return false;
        }
        String v = value.trim().toUpperCase(java.util.Locale.ROOT);
        // P6-856: both markers are documented with the leading underscore (R-268),
        // and every constant in this class carries it. Matching the bare
        // "TO_BE_VERIFIED" treated unrelated text as a placeholder — a spurious
        // start refusal — and contradicted the _TO_BE_PINNED check beside it.
        return v.contains("_TO_BE_PINNED") || v.contains("_TO_BE_VERIFIED");
    }
}
