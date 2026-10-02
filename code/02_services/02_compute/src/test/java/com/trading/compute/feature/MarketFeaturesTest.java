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
 * Market-snapshot features in the merged table (2026-10-02, CHG-512/CHG-516):
 * the raw market values the host captures from every accepted tick — 12
 * day/flow stats, the 30-column depth ladder, 7 extra feed-provided values
 * (day volume, change flag, atv, btv, imbalance, indicative close, reference
 * price) — plus 3 book-derived metrics (spread, depth imbalance, microprice)
 * are registered as MARKET-cadence features, fed from the shared
 * {@link com.trading.compute.signaljob.MarketView} on every tick, and stored
 * on every sealed {@code candle_features} row of every timeframe — the
 * book/day stats as of that row's close.
 */
class MarketFeaturesTest {

    /** ids 3..44, in CandleLiveColumns transport order. */
    private static final int FIRST = 3;
    private static final int LAST = 44;

    /** ids 45..54: the extra raw values + the book-derived metrics (CHG-516). */
    private static final int EXTRA_FIRST = 45;
    private static final int EXTRA_LAST = 54;

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
        "ask_ord_1", "ask_ord_2", "ask_ord_3", "ask_ord_4", "ask_ord_5",
        // 7 extra raw values + 3 derived metrics
        "day_volume", "change_flag", "atv", "btv", "imbalance_qty",
        "indicative_close_paise", "ref_price_paise",
        "spread_paise", "depth_imbalance", "microprice_paise"
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
        m.dayVolume = 801L;
        m.dayVolumeSeen = true;
        m.changeFlag = 802L;
        m.changeFlagSeen = true;
        m.atv = 803L;
        m.atvSeen = true;
        m.btv = 804L;
        m.btvSeen = true;
        m.imbalanceQty = 805L;
        m.imbalanceQtySeen = true;
        m.indicativeClosePaise = 806L;
        m.indicativeCloseSeen = true;
        m.refPricePaise = 807L;
        m.refPriceSeen = true;
        m.statsChangedAt = 1_000L;
        m.depthChangedAt = 1_001L;
        return m;
    }

    /** The value {@link #fullMarket()} assigns to {@code id}. */
    private static double expectedValue(int id) {
        if (id >= EXTRA_FIRST) {
            switch (id) {
                case 45: return 801.0; // day_volume
                case 46: return 802.0; // change_flag
                case 47: return 803.0; // atv
                case 48: return 804.0; // btv
                case 49: return 805.0; // imbalance_qty
                case 50: return 806.0; // indicative_close_paise
                case 51: return 807.0; // ref_price_paise
                case 52: return 300.0; // spread = ask_px_1 - bid_px_1
                case 53: return -1500.0 / 4530.0; // (1515 - 3015) / (1515 + 3015)
                default: return 301.0; // microprice, half-up on the L1 sizes
            }
        }
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
        assertEquals(EXTRA_LAST + 1, FeatureRegistry.SIZE,
                "the 52 market features are ids 3..54");
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
                IntStream.rangeClosed(FIRST, EXTRA_LAST).toArray(), FeatureRegistry.MARKET_IDS);
    }

    @Test
    void marketIdsRideEveryTimeframeRowButNeverTheCloseUpdate() {
        for (Timeframe tf : Timeframe.values()) {
            int[] ids = FeatureRegistry.idsFor(tf);
            for (int id = FIRST; id <= EXTRA_LAST; id++) {
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
            assertEquals(EXTRA_LAST - FIRST + 1, out.size(), "tf=" + tf);
            for (int id = FIRST; id <= EXTRA_LAST; id++) {
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
        assertEquals(Set.of(16, 21, 26, 53), out.keySet(),
                "only the seen bid level 2 lands; depth_imbalance derives from the visible ladder");
        assertEquals(9_902.0, out.get(16), 1e-9, "bid_px_2");
        assertEquals(12.0, out.get(21), 1e-9, "bid_qty_2");
        assertEquals(3.0, out.get(26), 1e-9, "bid_ord_2");
        assertEquals(1.0, out.get(53), 1e-9, "one-sided book -> imbalance 1.0");
    }

    @Test
    void extrasUseTheirOwnSeenGate() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        MarketSnapshot m = new MarketSnapshot();
        m.changeFlag = 0L;
        m.changeFlagSeen = true;
        m.dayVolume = 123L;
        m.dayVolumeSeen = true;
        features.onMarket(m);
        Map<Integer, Double> out = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, out);
        assertEquals(Set.of(45, 46), out.keySet(),
                "a provided 0 stores as 0; never-provided extras stay out");
        assertEquals(0.0, out.get(46), 1e-9, "change_flag provided as 0");
        assertEquals(123.0, out.get(45), 1e-9, "day_volume");
    }

    @Test
    void derivedBookMetricsFollowTheLadder() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        MarketSnapshot m = new MarketSnapshot();
        m.bidPx1 = 100L;
        m.bidQty1 = 30L;
        m.askPx1 = 104L;
        m.askQty1 = 10L;
        m.depthChangedAt = 1L;
        features.onMarket(m);
        Map<Integer, Double> out = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, out);
        assertEquals(Set.of(15, 20, 25, 30, 35, 40, 52, 53, 54), out.keySet(),
                "the seen L1 trios (ord gates on the price and stores its 0) + the derived metrics");
        assertEquals(0.0, out.get(25), 1e-9, "bid_ord_1 present as 0 (price seen, count not provided)");
        assertEquals(0.0, out.get(40), 1e-9, "ask_ord_1 present as 0");
        assertEquals(4.0, out.get(52), 1e-9, "spread = ask - bid");
        assertEquals(0.5, out.get(53), 1e-9, "depth imbalance = (30 - 10) / 40");
        assertEquals(103.0, out.get(54), 1e-9, "microprice = (100*10 + 104*30) / 40");
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
