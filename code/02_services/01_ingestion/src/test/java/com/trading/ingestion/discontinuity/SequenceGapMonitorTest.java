// T7-F11: sequence-gap detection unit tests.

package com.trading.ingestion.discontinuity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** T7-F11: SequenceGapMonitor — gap vs duplicate vs epoch-reset semantics. */
@DisplayName("T7-F11: SequenceGapMonitor")
class SequenceGapMonitorTest {

    @Test
    @DisplayName("contiguous sequences: no gaps")
    void contiguousNoGap() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        assertFalse(m.onTick("hft-0/1", 1), "first tick no baseline");
        assertFalse(m.onTick("hft-0/1", 2));
        assertFalse(m.onTick("hft-0/1", 3));
        assertEquals(0, m.gapCount());
    }

    @Test
    @DisplayName("jump > 1 is a gap")
    void jumpIsGap() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        m.onTick("hft-0/1", 1);
        m.onTick("hft-0/1", 2);
        assertTrue(m.onTick("hft-0/1", 5), "2→5 is a gap of 2");
        assertTrue(m.onTick("hft-0/1", 7), "5→7 is a gap of 1");
        assertEquals(2, m.gapCount());
    }

    @Test
    @DisplayName("out-of-order / duplicate is NOT a gap (at-least-once, T7-D1)")
    void duplicateNotGap() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        m.onTick("hft-0/1", 1);
        m.onTick("hft-0/1", 2);
        assertFalse(m.onTick("hft-0/1", 2), "duplicate seq 2 is not a gap");
        assertFalse(m.onTick("hft-0/1", 1), "late seq 1 is not a gap");
        assertEquals(0, m.gapCount());
    }

    @Test
    @DisplayName("P1-079: late tick never regresses the baseline — 1,2,1,3 is NOT a gap")
    void lateTickNeverRegressesBaseline() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        m.onTick("hft-0/1", 1);
        m.onTick("hft-0/1", 2);
        assertFalse(m.onTick("hft-0/1", 1), "late seq 1 must not move the baseline");
        assertFalse(m.onTick("hft-0/1", 3), "3 follows baseline 2 — no loss occurred");
        assertEquals(0, m.gapCount());
    }

    @Test
    @DisplayName("P1-079: gap after duplicates still detected against the held baseline")
    void gapAfterDuplicateStillDetected() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        m.onTick("hft-0/1", 1);
        m.onTick("hft-0/1", 2);
        assertFalse(m.onTick("hft-0/1", 2), "duplicate is not a gap");
        assertTrue(m.onTick("hft-0/1", 5), "5 still jumps the held baseline 2");
        assertEquals(1, m.gapCount());
    }

    @Test
    @DisplayName("epoch bump resets baseline — restart is not a gap (T7-F2)")
    void epochResetNotGap() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        m.onTick("hft-0/1", 1);
        m.onTick("hft-0/1", 2);
        // new epoch → new key → fresh baseline, seq restarts at 1
        assertFalse(m.onTick("hft-0/2", 1), "epoch bump: seq 1 with new epoch key");
        assertEquals(0, m.gapCount());
    }

    @Test
    @DisplayName("zero/negative seq ignored")
    void zeroSeqIgnored() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        assertFalse(m.onTick("hft-0/1", 0));
        assertFalse(m.onTick("hft-0/1", -1));
        assertEquals(0, m.gapCount());
    }

    @Test
    @DisplayName("slot isolation — different slots never cross-talk")
    void slotIsolation() {
        SequenceGapMonitor m = new SequenceGapMonitor();
        m.onTick("hft-0/1", 1);
        m.onTick("hft-0/1", 2);
        // different slot, same-ish numbers — no gap
        assertFalse(m.onTick("hft-1/1", 1), "hft-1 starts fresh");
        assertEquals(0, m.gapCount());
    }
}
