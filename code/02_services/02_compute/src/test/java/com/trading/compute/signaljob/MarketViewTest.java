package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link MarketView} read semantics over the mutable {@link MarketSnapshot}
 * (2026-10-01 native design): 0 = not provided, derived values computed on
 * read, the depth walk returns "cannot fill" instead of a silent partial
 * fill, and the two change clocks drive the age helpers.
 */
@DisplayName("MarketView: presence, derived values, depth walk, freshness")
class MarketViewTest {

    private static MarketSnapshot depthLadder() {
        MarketSnapshot m = new MarketSnapshot();
        // bids: 100.00 × 100, 99.99 × 400
        m.bidPx1 = 10_000L;
        m.bidQty1 = 100L;
        m.bidPx2 = 9_999L;
        m.bidQty2 = 400L;
        // asks: 100.05 × 200, 100.06 × 150, 100.07 × 300
        m.askPx1 = 10_005L;
        m.askQty1 = 200L;
        m.askPx2 = 10_006L;
        m.askQty2 = 150L;
        m.askPx3 = 10_007L;
        m.askQty3 = 300L;
        return m;
    }

    @Test
    void emptySnapshotReportsNothing() {
        MarketSnapshot m = new MarketSnapshot();
        assertFalse(m.hasStats());
        assertFalse(m.hasDepth());
        assertFalse(m.hasLevel(1));
        assertEquals(0L, m.marketChangedAt());
        assertEquals(0L, m.totalBidQty());
        assertEquals(0L, m.totalAskQty());
        assertEquals(0L, m.spreadPaise());
        assertEquals(0L, m.micropricePaise());
        assertTrue(Double.isNaN(m.depthImbalance()), "no depth -> not-ready NaN");
        assertEquals(MarketView.INSUFFICIENT_DEPTH, m.walkAvgPricePaise(true, 10L));
        assertEquals(MarketView.INSUFFICIENT_DEPTH, m.slippagePaise(false, 10L));
        assertEquals(Long.MAX_VALUE, m.ageMs(999_999L), "never seen -> infinitely old");
        assertEquals(Long.MAX_VALUE, m.statsAgeMs(999_999L));
        assertEquals(Long.MAX_VALUE, m.depthAgeMs(999_999L));
        assertTrue(m.isStale(999_999L, 1_000L));
        assertFalse(m.isFresh(999_999L, 1_000L));
    }

    @Test
    void depthLadderExposesEveryLevelAndRejectsOutOfRange() {
        MarketSnapshot m = depthLadder();
        assertTrue(m.hasLevel(1));
        assertTrue(m.hasLevel(2));
        assertFalse(m.hasLevel(3), "no level-3 bid -> not a full level");
        assertFalse(m.hasLevel(4));
        assertFalse(m.hasLevel(5));
        assertEquals(10_000L, m.bidPxPaise(1));
        assertEquals(9_999L, m.bidPxPaise(2));
        assertEquals(0L, m.bidPxPaise(3));
        assertEquals(10_005L, m.askPxPaise(1));
        assertEquals(10_007L, m.askPxPaise(3));
        assertEquals(0L, m.askOrd(5));
        assertThrows(IllegalArgumentException.class, () -> m.bidPxPaise(0));
        assertThrows(IllegalArgumentException.class, () -> m.askQty(6));
        assertThrows(IllegalArgumentException.class, () -> m.hasLevel(6));
    }

    @Test
    void totalsSpreadAndImbalance() {
        MarketSnapshot m = depthLadder();
        assertEquals(500L, m.totalBidQty());
        assertEquals(650L, m.totalAskQty());
        assertEquals(5L, m.spreadPaise());
        assertEquals((500.0 - 650.0) / 1150.0, m.depthImbalance(), 1e-12);
    }

    @Test
    void micropriceWeightsEachSideByTheOppositeSize() {
        MarketSnapshot m = new MarketSnapshot();
        m.bidPx1 = 10_000L;
        m.bidQty1 = 300L;
        m.askPx1 = 10_010L;
        m.askQty1 = 100L;
        // (10000·100 + 10010·300) / 400 = 10008 (half-up)
        assertEquals(10_008L, m.micropricePaise());
    }

    @Test
    @DisplayName("walking 500 shares: 200@100.05 + 150@100.06 + 150@100.07 -> avg 100.06, ~1 paise slippage")
    void buyWalkUsesTheRealLadder() {
        MarketSnapshot m = depthLadder();
        assertEquals(10_006L, m.walkAvgPricePaise(true, 500L));
        assertEquals(1L, m.slippagePaise(true, 500L));
    }

    @Test
    void sellWalkUsesBidsAndReportsAdverseSlippage() {
        MarketSnapshot m = depthLadder();
        // 100@100.00 + 200@99.99 -> 29999.80/300 -> 9999 paise; adverse = 10000-9999
        assertEquals(9_999L, m.walkAvgPricePaise(false, 300L));
        assertEquals(1L, m.slippagePaise(false, 300L));
    }

    @Test
    void walkRefusesWhenTheVisibleBookCannotFill() {
        MarketSnapshot m = depthLadder();
        // Visible asks: 200 + 150 + 300 = 650; 651 cannot fill.
        assertEquals(MarketView.INSUFFICIENT_DEPTH, m.walkAvgPricePaise(true, 651L));
        assertEquals(MarketView.INSUFFICIENT_DEPTH, m.slippagePaise(true, 651L));
        assertEquals(MarketView.INSUFFICIENT_DEPTH, m.walkAvgPricePaise(true, 0L));
        // Level gaps never crash the walk: only level 1 is present.
        MarketSnapshot gap = new MarketSnapshot();
        gap.askPx1 = 10_000L;
        gap.askQty1 = 10L;
        gap.askPx3 = 10_002L;
        gap.askQty3 = 10L;
        assertEquals(10_001L, gap.walkAvgPricePaise(true, 20L));
    }

    @Test
    void freshnessAgesComeFromTheTwoChangeClocks() {
        MarketSnapshot m = depthLadder();
        m.statsChangedAt = 1_000L;
        m.depthChangedAt = 2_000L;
        assertTrue(m.hasStats());
        assertTrue(m.hasDepth());
        assertEquals(2_000L, m.marketChangedAt());
        assertEquals(1_500L, m.statsAgeMs(2_500L));
        assertEquals(500L, m.depthAgeMs(2_500L));
        assertEquals(500L, m.ageMs(2_500L));
        assertEquals(0L, m.statsAgeMs(500L), "now before the change clamps to 0");
        assertFalse(m.isStale(2_500L, 500L), "500 is not older than 500");
        assertTrue(m.isStale(2_501L, 500L));
        assertTrue(m.isFresh(2_500L, 500L));
    }
}
