package com.trading.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * DEC-045 (2026-08-27): the per-instrument hard cap is 20 ticks/s, the sustained acceptance gate is
 * 60,000 ticks/s (superseding DEC-036's 50k), and the 90k peak campaign stays retired.
 *
 * <p>P6-270/P6-662: the constant had drifted from the decision record that changed it, and nothing
 * read it, so no test noticed. These numbers are cited by perf and acceptance rows, so they are
 * pinned here instead of left to a comment.
 */
class FixedScopeTest {

    @Test
    void sustainedGateIsTheDec045Number() {
        assertEquals(60_000L, FixedScope.BASELINE_TICKS_PER_SEC,
                "DEC-045 re-raised the sustained gate to 60k; 50k is DEC-036 and is superseded");
    }

    @Test
    void capCeilingDerivesFromTheInstrumentAperture() {
        assertEquals(20, FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC, "DEC-045: 30 -> 20");
        assertEquals(FixedScope.MAX_INSTRUMENTS * 20L, FixedScope.maxSustainedTicksPerSec());
    }

    @Test
    void generatorPeakIsABoundNotADerivedValue() {
        // They coincide today (60k), but the generator's stress profile is not the cap ceiling:
        // re-profiling the mock must not have to move the instrument aperture, or vice versa.
        assertTrue(FixedScope.PEAK_TICKS_PER_SEC >= FixedScope.BASELINE_TICKS_PER_SEC,
                "the generator must be able to stress the accepted sustained gate");
    }

    /** R-263 (moved here from PlatformConfigTest when that class's R-199 helper was deleted). */
    @Test
    void platformConfigDelegatesToThisSingleSource() {
        assertEquals(FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC,
                PlatformConfig.BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC);
        assertEquals(20, PlatformConfig.BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC);
    }
}
