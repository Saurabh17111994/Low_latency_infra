package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Heap-window memory contract (2026-09-03 redesign): entries are short
 * strings held once in an insertion-ordered set — no TTL timestamps, no
 * value objects, no backend overhead. This test pins the structural bound
 * (the only thing standing between the operator and unbounded growth) and
 * the global-cap formula, directly against the production {@code Window}.
 */
@DisplayName("Heap-window memory contract: structural bound + global cap formula")
class DedupStateSizeTest {

    @Test
    @DisplayName("window never exceeds its bound under 10x overfill")
    void windowBoundHoldsUnderOverfill() {
        FingerprintDedupFunction.Window w = new FingerprintDedupFunction.Window(2000);
        for (int i = 0; i < 20_000; i++) {
            w.put("fp-" + i, Boolean.TRUE);
        }
        assertEquals(2000, w.size(), "self-trim holds the bound under 10x overfill");
    }

    @Test
    @DisplayName("global cap tolerates 16x the instrument universe, nothing less structural")
    void globalCapFormula() {
        int max = 2000;
        long cap = (long) max * FingerprintDedupFunction.GLOBAL_CAP_HEADROOM_TOKENS;
        assertEquals(32_768_000L, cap, "2000 x 16384 headroom tokens");
        assertTrue(cap > 1_024L * max, "16x headroom over the 1024-instrument universe");
    }
}
