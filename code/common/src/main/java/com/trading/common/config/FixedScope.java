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

    /**
     * Sustained aggregate acceptance gate: 60,000 ticks/s. Re-raised from 50,000 by DEC-045
     * (2026-08-27), which superseded DEC-036's 50k gate; the 90,000 peak campaign stays retired.
     * Pinned by {@code FixedScopeTest} — this is an acceptance number, not a tuning knob.
     */
    public static final long BASELINE_TICKS_PER_SEC = 60_000L;

    /**
     * Two quantities that agree today: the theoretical per-instrument-cap ceiling
     * (3,000 × 20 = {@link #maxSustainedTicksPerSec()}) and the mock generator's PEAK stress bound
     * (MOCK-UNIT-002). They are deliberately not derived from each other — the ceiling follows the
     * per-instrument cap, the generator is free to be re-profiled independently — so the equality
     * is a comment, not an assertion. DEC-045: 60,000, i.e. at the measured feed/tablet ceiling
     * (58.9–59.7k rows/s) with no margin.
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
