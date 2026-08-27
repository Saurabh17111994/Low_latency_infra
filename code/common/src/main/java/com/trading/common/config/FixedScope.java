package com.trading.common.config;

/**
 * Fixed, non-negotiable platform scope
 * (docs/08_implementation/01-foundation.md &rarr; "Fixed scope", orig L23).
 *
 * <p>These bounds are architecture, not tuning; they are constants, not configuration.
 */
public final class FixedScope {

    private FixedScope() {}

    public static final int MAX_INSTRUMENTS = 3_000;
    public static final long BASELINE_TICKS_PER_SEC = 50_000L;

    /**
     * Theoretical per-instrument-cap ceiling (3,000 × 20 ticks/s). The
     * 60,000 ticks/s peak is the generator stress bound (mock PEAK profile,
     * MOCK-UNIT-002); the acceptance gate is 50,000 ticks/s sustained
     * (DEC-036: measured feed/tablet ceiling 58.9–59.7k rows/s).
     */
    public static final long PEAK_TICKS_PER_SEC = 60_000L;
    public static final int MAX_TICKS_PER_INSTRUMENT_PER_SEC = 20;

    /** No Complex Event Processing operator/dependency is used anywhere in the platform. */
    public static final boolean CEP_PROHIBITED = true;

    /** Every accepted tick is appended to raw_table_1; none are silently dropped. */
    public static final boolean EVERY_TICK_TO_RAW_TABLE_1 = true;

    /** The Signal job must not write a temp feature/candidate to Fluss and read it back. */
    public static final boolean FLINK_FLUSS_FLINK_ROUNDTRIP_PROHIBITED = true;

    /** Hard ceiling on total sustained ticks/s across all instruments. */
    public static long maxSustainedTicksPerSec() {
        return (long) MAX_INSTRUMENTS * MAX_TICKS_PER_INSTRUMENT_PER_SEC;
    }
}
