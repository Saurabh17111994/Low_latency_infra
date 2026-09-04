package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MultiTimeframeState}.
 *
 * <p>Covers: all 6 forming accumulators present per {@link Timeframe},
 * rings start empty, quote fields default 0, discontinuity defaults false,
 * monotonic gate defaults, accessor identity, and resetForming() keeps rings.
 */
class MultiTimeframeStateTest {

    @Test
    void allSixFormingAccumulatorsPresentViaAccessors() {
        MultiTimeframeState state = new MultiTimeframeState();
        for (Timeframe tf : Timeframe.values()) {
            CandleAccumulator acc = state.forming(tf);
            assertNotNull(acc, "forming(" + tf + ") must not be null");
            // Fresh accumulator defaults: firstEventTime sentinel, no OHLC yet
            assertEquals(Long.MAX_VALUE, acc.firstEventTime,
                    "forming " + tf + " fresh firstEventTime should be MAX");
            assertEquals(Long.MIN_VALUE, acc.lastEventTime,
                    "forming " + tf + " fresh lastEventTime should be MIN");
            assertEquals(0L, acc.volume);
            assertEquals(0L, acc.tickCount);
            assertNull(acc.firstFingerprint);
            assertNull(acc.lastFingerprint);
        }
        // Also verify named fields are non-null
        assertNotNull(state.formingFifteenS);
        assertNotNull(state.formingThirtyS);
        assertNotNull(state.formingOneM);
        assertNotNull(state.formingThreeM);
        assertNotNull(state.formingFiveM);
        assertNotNull(state.formingFifteenM);
    }

    @Test
    void namedFormingFieldsMatchAccessor() {
        MultiTimeframeState state = new MultiTimeframeState();
        assertTrue(state.forming(Timeframe.FIFTEEN_S) == state.formingFifteenS);
        assertTrue(state.forming(Timeframe.THIRTY_S) == state.formingThirtyS);
        assertTrue(state.forming(Timeframe.ONE_M) == state.formingOneM);
        assertTrue(state.forming(Timeframe.THREE_M) == state.formingThreeM);
        assertTrue(state.forming(Timeframe.FIVE_M) == state.formingFiveM);
        assertTrue(state.forming(Timeframe.FIFTEEN_M) == state.formingFifteenM);
    }

    @Test
    void allSixClosedRingsStartEmptyViaAccessors() {
        MultiTimeframeState state = new MultiTimeframeState();
        for (Timeframe tf : Timeframe.values()) {
            MultiTimeframeClosedRing ring = state.closed(tf);
            assertNotNull(ring, "closed(" + tf + ") must not be null");
            assertTrue(ring.isEmpty(), "closed ring " + tf + " should start empty");
            assertEquals(0, ring.size());
            assertTrue(ring.snapshotNewestFirst().isEmpty());
        }
        assertNotNull(state.closedFifteenS);
        assertNotNull(state.closedThirtyS);
        assertNotNull(state.closedOneM);
        assertNotNull(state.closedThreeM);
        assertNotNull(state.closedFiveM);
        assertNotNull(state.closedFifteenM);
    }

    @Test
    void namedClosedFieldsMatchAccessor() {
        MultiTimeframeState state = new MultiTimeframeState();
        assertTrue(state.closed(Timeframe.FIFTEEN_S) == state.closedFifteenS);
        assertTrue(state.closed(Timeframe.THIRTY_S) == state.closedThirtyS);
        assertTrue(state.closed(Timeframe.ONE_M) == state.closedOneM);
        assertTrue(state.closed(Timeframe.THREE_M) == state.closedThreeM);
        assertTrue(state.closed(Timeframe.FIVE_M) == state.closedFiveM);
        assertTrue(state.closed(Timeframe.FIFTEEN_M) == state.closedFifteenM);
    }

    @Test
    void quoteFieldsDefaultZero() {
        MultiTimeframeState state = new MultiTimeframeState();
        assertEquals(0L, state.lastBidPaise);
        assertEquals(0L, state.lastAskPaise);
        assertEquals(0L, state.lastBidSize);
        assertEquals(0L, state.lastAskSize);
        assertEquals(0L, state.lastQuoteEventTime);
    }

    @Test
    void discontinuityDefaultsFalse() {
        MultiTimeframeState state = new MultiTimeframeState();
        assertFalse(state.discontinuityPending,
                "discontinuityPending must default false");
        assertEquals(0L, state.lastDiscontinuityEventTime);
    }

    @Test
    void monotonicGateDefaults() {
        MultiTimeframeState state = new MultiTimeframeState();
        assertEquals(Long.MIN_VALUE, state.lastEventTime,
                "lastEventTime sentinel should be MIN");
        assertNull(state.lastFingerprint);
    }

    @Test
    void sixFormingAccumulatorsAreDistinctInstances() {
        MultiTimeframeState state = new MultiTimeframeState();
        CandleAccumulator[] accs = new CandleAccumulator[Timeframe.values().length];
        int i = 0;
        for (Timeframe tf : Timeframe.values()) {
            accs[i++] = state.forming(tf);
        }
        for (int a = 0; a < accs.length; a++) {
            for (int b = a + 1; b < accs.length; b++) {
                assertNotSame(accs[a], accs[b],
                        "forming accumulators must be distinct instances");
            }
        }
    }

    @Test
    void sixClosedRingsAreDistinctInstances() {
        MultiTimeframeState state = new MultiTimeframeState();
        MultiTimeframeClosedRing[] rings = new MultiTimeframeClosedRing[Timeframe.values().length];
        int i = 0;
        for (Timeframe tf : Timeframe.values()) {
            rings[i++] = state.closed(tf);
        }
        for (int a = 0; a < rings.length; a++) {
            for (int b = a + 1; b < rings.length; b++) {
                assertNotSame(rings[a], rings[b],
                        "closed rings must be distinct instances");
            }
        }
    }

    @Test
    void resetFormingClearsFormingButKeepsRings() {
        MultiTimeframeState state = new MultiTimeframeState();
        // Mutate forming accumulators
        state.formingFifteenS.openPaise = 12345L;
        state.formingFifteenS.highPaise = 12350L;
        state.formingOneM.openPaise = 999L;
        state.formingFifteenM.closePaise = 777L;

        // Fill some closed rings
        ClosedCandle c1 = new ClosedCandle(1000L, 1010L, 1005L, 995L, 1002L, 50L, 2L, 1010L, "fp-1");
        ClosedCandle c2 = new ClosedCandle(2000L, 2010L, 2005L, 1995L, 2002L, 60L, 3L, 2010L, "fp-2");
        state.closed(Timeframe.FIFTEEN_S).add(c1);
        state.closed(Timeframe.ONE_M).add(c2);
        assertEquals(1, state.closedFifteenS.size());
        assertEquals(1, state.closedOneM.size());

        // Capture ring identities before reset
        MultiTimeframeClosedRing ringFifteenSBefore = state.closedFifteenS;
        MultiTimeframeClosedRing ringOneMBefore = state.closedOneM;

        state.resetForming();

        // Forming should be fresh (new instances, zeroed fields)
        assertNotSame(ringFifteenSBefore, null); // sanity
        for (Timeframe tf : Timeframe.values()) {
            CandleAccumulator acc = state.forming(tf);
            assertNotNull(acc);
            assertEquals(0L, acc.openPaise, "reset forming " + tf + " open should be 0");
            assertEquals(0L, acc.volume, "reset forming " + tf + " volume should be 0");
            assertEquals(Long.MAX_VALUE, acc.firstEventTime);
            assertEquals(Long.MIN_VALUE, acc.lastEventTime);
        }
        // Named fields also replaced (check at least one not same object as mutated)
        // We mutated formingFifteenS directly; after reset it should be a new object
        // (openPaise 0). So we can't compare identity easily without saving old ref,
        // but we validated zeroed fields above which implies new or cleared.

        // Rings must be preserved (same instances and same content)
        assertTrue(state.closedFifteenS == ringFifteenSBefore,
                "resetForming must keep same ring instances");
        assertTrue(state.closedOneM == ringOneMBefore);
        assertEquals(1, state.closedFifteenS.size());
        assertEquals(1000L, state.closedFifteenS.get(0).windowStart);
        assertEquals(1, state.closedOneM.size());
        assertEquals(2000L, state.closedOneM.get(0).windowStart);

        // Other rings still empty
        assertTrue(state.closedThirtyS.isEmpty());
        assertTrue(state.closedThreeM.isEmpty());
        assertTrue(state.closedFiveM.isEmpty());
        assertTrue(state.closedFifteenM.isEmpty());
    }

    @Test
    void quoteAndDiscontinuitySurviveReset() {
        MultiTimeframeState state = new MultiTimeframeState();
        state.lastBidPaise = 1000L;
        state.lastAskPaise = 1010L;
        state.lastBidSize = 5L;
        state.lastAskSize = 7L;
        state.lastQuoteEventTime = 123456789L;
        state.discontinuityPending = true;
        state.lastDiscontinuityEventTime = 987654321L;
        state.lastEventTime = 555L;
        state.lastFingerprint = "fp-xyz";

        state.resetForming();

        assertEquals(1000L, state.lastBidPaise);
        assertEquals(1010L, state.lastAskPaise);
        assertEquals(5L, state.lastBidSize);
        assertEquals(7L, state.lastAskSize);
        assertEquals(123456789L, state.lastQuoteEventTime);
        assertTrue(state.discontinuityPending);
        assertEquals(987654321L, state.lastDiscontinuityEventTime);
        assertEquals(555L, state.lastEventTime);
        assertEquals("fp-xyz", state.lastFingerprint);
    }

    @Test
    void isWarmReflectsClosedRingCapacity() {
        MultiTimeframeState state = new MultiTimeframeState();
        for (Timeframe tf : Timeframe.values()) {
            assertFalse(state.isWarm(tf), "fresh state should not be warm for " + tf);
        }
        // Fill 15 for ONE_M only
        for (int i = 0; i < 15; i++) {
            state.closedOneM.add(new ClosedCandle(i * 1000L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, "fp-" + i));
        }
        assertTrue(state.isWarm(Timeframe.ONE_M), "ONE_M should be warm after 15");
        assertFalse(state.isWarm(Timeframe.FIFTEEN_S), "other TF should still be cold");
        assertFalse(state.isWarm(Timeframe.FIFTEEN_M), "other TF should still be cold");
    }

    @Test
    void timeframeValuesCountIsSix() {
        assertEquals(6, Timeframe.values().length, "exactly 6 timeframes expected");
        // Also ensures our state covers all
        MultiTimeframeState state = new MultiTimeframeState();
        for (Timeframe tf : Timeframe.values()) {
            assertNotNull(state.forming(tf));
            assertNotNull(state.closed(tf));
        }
    }
}
