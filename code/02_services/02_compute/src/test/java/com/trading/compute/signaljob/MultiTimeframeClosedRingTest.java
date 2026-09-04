package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MultiTimeframeClosedRing}.
 *
 * <p>Covers: capacity 15 (Decision 6), newest-first snapshot order, empty
 * state, single element, all 15, 16th evicts oldest, and get() newest-first
 * indexing.
 */
class MultiTimeframeClosedRingTest {

    private static ClosedCandle candle(long windowStart) {
        return new ClosedCandle(
                windowStart,
                windowStart + 15_000L, // windowEnd (placeholder)
                windowStart, // openPaise
                windowStart + 5, // high
                windowStart - 5, // low
                windowStart + 2, // close
                100L,
                1L,
                windowStart + 10,
                "fp-" + windowStart);
    }

    @Test
    void capacityConstantIs15() {
        assertEquals(15, MultiTimeframeClosedRing.CAPACITY,
                "capacity must be 15 per design decision 6");
    }

    @Test
    void emptyStateIsEmpty() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        assertTrue(ring.isEmpty(), "new ring must be empty");
        assertEquals(0, ring.size());
        assertTrue(ring.snapshotNewestFirst().isEmpty(), "snapshot of empty ring must be empty");
        assertTrue(ring.snapshotOldestFirst().isEmpty());
    }

    @Test
    void singleElementStoredAndRetrievedNewestFirst() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        ClosedCandle c = candle(1000L);
        ring.add(c);

        assertFalse(ring.isEmpty());
        assertEquals(1, ring.size());
        assertEquals(c.windowStart, ring.get(0).windowStart);
        assertEquals(c.closePaise, ring.get(0).closePaise);

        List<ClosedCandle> snap = ring.snapshotNewestFirst();
        assertEquals(1, snap.size());
        assertEquals(1000L, snap.get(0).windowStart);

        List<ClosedCandle> oldest = ring.snapshotOldestFirst();
        assertEquals(1, oldest.size());
        assertEquals(1000L, oldest.get(0).windowStart);
    }

    @Test
    void fifteenElementsFillWithoutEvictionInNewestFirstOrder() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        for (int i = 0; i < 15; i++) {
            ring.add(candle(i * 1000L));
        }
        assertEquals(15, ring.size());
        assertFalse(ring.isEmpty());

        // newest-first: 14_000, 13_000, ... 0
        List<ClosedCandle> newest = ring.snapshotNewestFirst();
        assertEquals(15, newest.size());
        for (int i = 0; i < 15; i++) {
            long expectedWindowStart = (14 - i) * 1000L;
            assertEquals(expectedWindowStart, newest.get(i).windowStart,
                    "newest-first index " + i + " should be " + expectedWindowStart);
        }

        // oldest-first: 0, 1_000, ... 14_000
        List<ClosedCandle> oldest = ring.snapshotOldestFirst();
        assertEquals(15, oldest.size());
        for (int i = 0; i < 15; i++) {
            assertEquals(i * 1000L, oldest.get(i).windowStart);
        }

        // get(newest-first) mirrors snapshot
        assertEquals(14_000L, ring.get(0).windowStart);
        assertEquals(0L, ring.get(14).windowStart);
        assertEquals(7_000L, ring.get(7).windowStart);
    }

    @Test
    void sixteenthAddEvictsOldest() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        for (int i = 0; i < 15; i++) {
            ring.add(candle(i * 1000L));
        }
        // Add 16th
        ring.add(candle(15_000L));

        assertEquals(15, ring.size(), "size must stay capped at 15 after 16th add");
        assertFalse(ring.isEmpty());

        List<ClosedCandle> newest = ring.snapshotNewestFirst();
        assertEquals(15, newest.size());
        // newest should be 15_000 at index 0, oldest now 1_000 at index 14
        assertEquals(15_000L, newest.get(0).windowStart);
        assertEquals(14_000L, newest.get(1).windowStart);
        assertEquals(1_000L, newest.get(14).windowStart);

        // Oldest (0) must be gone
        for (ClosedCandle c : newest) {
            assertTrue(c.windowStart != 0L, "evicted oldest (0) must not remain");
        }

        // get newest-first
        assertEquals(15_000L, ring.get(0).windowStart);
        assertEquals(1_000L, ring.get(14).windowStart);
    }

    @Test
    void multipleEvictionsKeepLast15() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        for (int i = 0; i < 30; i++) {
            ring.add(candle(i * 1000L));
        }
        assertEquals(15, ring.size());
        List<ClosedCandle> newest = ring.snapshotNewestFirst();
        // Should hold 15_000..29_000 newest-first
        assertEquals(29_000L, newest.get(0).windowStart);
        assertEquals(15_000L, newest.get(14).windowStart);
    }

    @Test
    void getOutOfBoundsThrows() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        assertThrows(IndexOutOfBoundsException.class, () -> ring.get(0));
        ring.add(candle(1000L));
        assertThrows(IndexOutOfBoundsException.class, () -> ring.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> ring.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> ring.get(2));
    }

    @Test
    void addRejectsNull() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        assertThrows(NullPointerException.class, () -> ring.add(null));
    }

    @Test
    void snapshotIsCopyNotView() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        ring.add(candle(1000L));
        List<ClosedCandle> snap = ring.snapshotNewestFirst();
        snap.clear();
        // Original ring must be unaffected
        assertEquals(1, ring.size());
        assertEquals(1, ring.snapshotNewestFirst().size());
    }

    @Test
    void clearEmptiesRing() {
        MultiTimeframeClosedRing ring = new MultiTimeframeClosedRing();
        for (int i = 0; i < 5; i++) ring.add(candle(i * 1000L));
        assertEquals(5, ring.size());
        ring.clear();
        assertTrue(ring.isEmpty());
        assertEquals(0, ring.size());
        assertTrue(ring.snapshotNewestFirst().isEmpty());
    }
}
