package com.trading.compute.feature;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.signaljob.MarketSnapshot;
import com.trading.compute.signaljob.Timeframe;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Market-snapshot features in the merged table (2026-10-02, CHG-512): the 42
 * values the host captures from every accepted tick (12 day/flow stats + the
 * 30-column depth ladder) are registered as MARKET-cadence features, fed from
 * the shared {@link com.trading.compute.signaljob.MarketView} on every tick,
 * and stored on every sealed {@code candle_features} row of every timeframe —
 * the book/day stats as of that row's close.
 */
class MarketFeaturesTest {

    /** ids 3..44, in CandleLiveColumns transport order. */
    private static final int FIRST = 3;
    private static final int LAST = 44;

    private static final String[] NAMES = {
        // 12 stats
        "total_buy_qty", "total_sell_qty", "day_open_paise", "day_high_paise",
        "day_low_paise", "prev_close_paise", "vwap_paise", "open_interest",
        "oi_day_high", "oi_day_low", "lower_limit_paise", "upper_limit_paise",
        // 30 depth, attribute-major per side
        "bid_px_1", "bid_px_2", "bid_px_3", "bid_px_4", "bid_px_5",
        "bid_qty_1", "bid_qty_2", "bid_qty_3", "bid_qty_4", "bid_qty_5",
        "bid_ord_1", "bid_ord_2", "bid_ord_3", "bid_ord_4", "bid_ord_5",
        "ask_px_1", "ask_px_2", "ask_px_3", "ask_px_4", "ask_px_5",
        "ask_qty_1", "ask_qty_2", "ask_qty_3", "ask_qty_4", "ask_qty_5",
        "ask_ord_1", "ask_ord_2", "ask_ord_3", "ask_ord_4", "ask_ord_5"
    };

    /** Full book + stats: every gate open, distinct values so ids cannot alias. */
    private static MarketSnapshot fullMarket() {
        MarketSnapshot m = new MarketSnapshot();
        m.totalBuyQty = 101L;
        m.totalSellQty = 102L;
        m.dayOpenPaise = 103L;
        m.dayHighPaise = 104L;
        m.dayLowPaise = 105L;
        m.prevClosePaise = 106L;
        m.vwapPaise = 107L;
        m.openInterest = 108L;
        m.oiDayHigh = 109L;
        m.oiDayLow = 110L;
        m.lowerLimitPaise = 111L;
        m.upperLimitPaise = 112L;
        m.bidPx1 = 201L;
        m.bidPx2 = 202L;
        m.bidPx3 = 203L;
        m.bidPx4 = 204L;
        m.bidPx5 = 205L;
        m.bidQty1 = 301L;
        m.bidQty2 = 302L;
        m.bidQty3 = 303L;
        m.bidQty4 = 304L;
        m.bidQty5 = 305L;
        m.bidOrd1 = 401L;
        m.bidOrd2 = 402L;
        m.bidOrd3 = 403L;
        m.bidOrd4 = 404L;
        m.bidOrd5 = 405L;
        m.askPx1 = 501L;
        m.askPx2 = 502L;
        m.askPx3 = 503L;
        m.askPx4 = 504L;
        m.askPx5 = 505L;
        m.askQty1 = 601L;
        m.askQty2 = 602L;
        m.askQty3 = 603L;
        m.askQty4 = 604L;
        m.askQty5 = 605L;
        m.askOrd1 = 701L;
        m.askOrd2 = 702L;
        m.askOrd3 = 703L;
        m.askOrd4 = 704L;
        m.askOrd5 = 705L;
        m.statsChangedAt = 1_000L;
        m.depthChangedAt = 1_001L;
        return m;
    }

    /** The value {@link #fullMarket()} assigns to {@code id}. */
    private static double expectedValue(int id) {
        if (id <= 14) {
            return 101.0 + (id - FIRST);
        }
        if (id <= 19) {
            return 200.0 + (id - 14); // bid_px_1 = 201 at id 15
        }
        if (id <= 24) {
            return 300.0 + (id - 19); // bid_qty_1 = 301 at id 20
        }
        if (id <= 29) {
            return 400.0 + (id - 24); // bid_ord_1 = 401 at id 25
        }
        if (id <= 34) {
            return 500.0 + (id - 29); // ask_px_1 = 501 at id 30
        }
        if (id <= 39) {
            return 600.0 + (id - 34); // ask_qty_1 = 601 at id 35
        }
        return 700.0 + (id - 39); // ask_ord_1 = 701 at id 40
    }

    @Test
    void marketFeaturesAreRegisteredInTransportOrder() {
        assertEquals(LAST + 1, FeatureRegistry.SIZE, "the 42 market features are ids 3..44");
        for (int i = 0; i < NAMES.length; i++) {
            int id = FIRST + i;
            FeatureDef def = FeatureRegistry.byId(id);
            assertEquals(NAMES[i], def.name(), "id " + id);
            assertEquals(FeatureStatus.ACTIVE, def.status(), "id " + id);
            assertEquals(FeatureCadence.MARKET, def.cadence(), "id " + id);
            for (Timeframe tf : Timeframe.values()) {
                assertTrue(def.serves(tf), NAMES[i] + " must serve " + tf);
            }
        }
        assertArrayEquals(
                IntStream.rangeClosed(FIRST, LAST).toArray(), FeatureRegistry.MARKET_IDS);
    }

    @Test
    void marketIdsRideEveryTimeframeRowButNeverTheCloseUpdate() {
        for (Timeframe tf : Timeframe.values()) {
            int[] ids = FeatureRegistry.idsFor(tf);
            for (int id = FIRST; id <= LAST; id++) {
                boolean found = false;
                for (int candidate : ids) {
                    if (candidate == id) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found, "id " + id + " missing from the " + tf + " row");
            }
            for (int id : FeatureRegistry.closeIdsFor(tf)) {
                assertTrue(id < FIRST, "market features are fed from the snapshot, not a close");
            }
        }
    }

    @Test
    void fullMarketLandsInEveryTimeframeSnapshot() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        features.onMarket(fullMarket());
        Map<Integer, Double> out = new HashMap<>();
        for (Timeframe tf : Timeframe.values()) {
            out.clear();
            features.snapshot(tf, out);
            assertEquals(LAST - FIRST + 1, out.size(), "tf=" + tf);
            for (int id = FIRST; id <= LAST; id++) {
                assertEquals(expectedValue(id), out.get(id), 1e-9, "id=" + id + " tf=" + tf);
            }
        }
    }

    @Test
    void neverSeenGroupsStayOutOfStorage() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        Map<Integer, Double> out = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, out);
        assertTrue(out.isEmpty(), "nothing seen yet -> nothing stored");

        // Stats only: the 12 stats land, the 30 depth values stay out.
        MarketSnapshot statsOnly = new MarketSnapshot();
        statsOnly.vwapPaise = 555L;
        statsOnly.statsChangedAt = 1L;
        features.onMarket(statsOnly);
        out.clear();
        features.snapshot(Timeframe.ONE_M, out);
        assertEquals(
                IntStream.rangeClosed(FIRST, 14).boxed().collect(Collectors.toSet()),
                out.keySet());
        assertEquals(555.0, out.get(9), 1e-9, "vwap_paise");

        // Depth only: the seen bid level 2 lands; other levels and the ask side stay out.
        PerInstrumentFeatures depth = new PerInstrumentFeatures();
        MarketSnapshot depthOnly = new MarketSnapshot();
        depthOnly.bidPx2 = 9_902L;
        depthOnly.bidQty2 = 12L;
        depthOnly.bidOrd2 = 3L;
        depthOnly.depthChangedAt = 2L;
        depth.onMarket(depthOnly);
        out.clear();
        depth.snapshot(Timeframe.ONE_M, out);
        assertEquals(Set.of(16, 21, 26), out.keySet(), "only the seen bid level 2 lands");
        assertEquals(9_902.0, out.get(16), 1e-9, "bid_px_2");
        assertEquals(12.0, out.get(21), 1e-9, "bid_qty_2");
        assertEquals(3.0, out.get(26), 1e-9, "bid_ord_2");
    }

    @Test
    void latestReadsTheMarketValueTimeframeIndependently() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        features.onMarket(fullMarket());
        assertEquals(101.0, features.latest(FIRST, Timeframe.FIFTEEN_S), 1e-9);
        assertEquals(101.0, features.latest(FIRST, Timeframe.FIFTEEN_M), 1e-9);
        assertEquals(101.0, features.latestLive(FIRST, Timeframe.ONE_M), 1e-9);
    }

    @Test
    void laterSnapshotOverwritesTheStoredValue() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        features.onMarket(fullMarket());
        MarketSnapshot second = fullMarket();
        second.vwapPaise = 999L;
        features.onMarket(second);
        Map<Integer, Double> out = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, out);
        assertEquals(999.0, out.get(9), 1e-9, "the latest-known value wins");
    }
}
