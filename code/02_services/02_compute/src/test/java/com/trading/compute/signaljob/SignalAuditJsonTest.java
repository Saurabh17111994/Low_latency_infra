package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * v2 audit JSON shape pin (CHG-504): the full document, the 0 = not provided
 * null convention, non-finite derived values, and the missing-view shape.
 * Consumers key on {@code v}; this test is the contract they can rely on.
 */
@DisplayName("SignalAuditJson: v2 shape, null conventions, full market block")
class SignalAuditJsonTest {

    private static MarketSnapshot filledMarket() {
        MarketSnapshot m = new MarketSnapshot();
        m.totalBuyQty = 1L;
        m.totalSellQty = 2L;
        m.dayOpenPaise = 3L;
        m.dayHighPaise = 4L;
        m.dayLowPaise = 5L;
        m.prevClosePaise = 6L;
        m.vwapPaise = 7L;
        m.openInterest = 8L;
        m.oiDayHigh = 9L;
        m.oiDayLow = 10L;
        m.lowerLimitPaise = 11L;
        m.upperLimitPaise = 12L;
        m.bidPx1 = 101L; m.bidQty1 = 11L; m.bidOrd1 = 21L;
        m.bidPx2 = 102L; m.bidQty2 = 12L; m.bidOrd2 = 22L;
        m.bidPx3 = 103L; m.bidQty3 = 13L; m.bidOrd3 = 23L;
        m.bidPx4 = 104L; m.bidQty4 = 14L; m.bidOrd4 = 24L;
        m.bidPx5 = 105L; m.bidQty5 = 15L; m.bidOrd5 = 25L;
        m.askPx1 = 201L; m.askQty1 = 31L; m.askOrd1 = 41L;
        m.askPx2 = 202L; m.askQty2 = 32L; m.askOrd2 = 42L;
        m.askPx3 = 203L; m.askQty3 = 33L; m.askOrd3 = 43L;
        m.askPx4 = 204L; m.askQty4 = 34L; m.askOrd4 = 44L;
        m.askPx5 = 205L; m.askQty5 = 35L; m.askOrd5 = 45L;
        m.statsChangedAt = 1_000L;
        m.depthChangedAt = 1_001L;
        return m;
    }

    @Test
    @DisplayName("full document renders exactly the pinned v2 shape")
    void fullDocumentShape() {
        String json = SignalAuditJson.build(
                "n7-range-breakout-v1", "FIFTEEN_S", "BUY",
                90_000L, 105_000L, 10_006L, 10_005L, 10_007L,
                "live", 200_000L, 2_000L,
                90_000L, 105_000L, 10_005L, 10_006L, 10_004L, 10_005L, 100L,
                filledMarket());

        String expected = "{\"v\":2,"
                + "\"strategy\":{\"id\":\"n7-range-breakout-v1\",\"tf\":\"FIFTEEN_S\","
                + "\"side\":\"BUY\",\"windowStart\":90000,\"windowEnd\":105000,"
                + "\"levelHigh\":10006,\"levelLow\":10005,\"range\":1,\"triggerPrice\":10007},"
                + "\"fire\":{\"path\":\"live\",\"detectionTs\":200000,\"evaluationTs\":2000},"
                + "\"candle\":{\"windowStart\":90000,\"windowEnd\":105000,\"open\":10005,"
                + "\"high\":10006,\"low\":10004,\"close\":10005,\"volume\":100},"
                + "\"market\":{"
                + "\"bid\":[{\"px\":101,\"qty\":11,\"ord\":21},{\"px\":102,\"qty\":12,\"ord\":22},"
                + "{\"px\":103,\"qty\":13,\"ord\":23},{\"px\":104,\"qty\":14,\"ord\":24},"
                + "{\"px\":105,\"qty\":15,\"ord\":25}],"
                + "\"ask\":[{\"px\":201,\"qty\":31,\"ord\":41},{\"px\":202,\"qty\":32,\"ord\":42},"
                + "{\"px\":203,\"qty\":33,\"ord\":43},{\"px\":204,\"qty\":34,\"ord\":44},"
                + "{\"px\":205,\"qty\":35,\"ord\":45}],"
                + "\"stats\":{\"totalBuyQty\":1,\"totalSellQty\":2,\"dayOpen\":3,\"dayHigh\":4,"
                + "\"dayLow\":5,\"prevClose\":6,\"vwap\":7,\"openInterest\":8,\"oiDayHigh\":9,"
                + "\"oiDayLow\":10,\"lowerLimit\":11,\"upperLimit\":12},"
                + "\"derived\":{\"spread\":100,\"microprice\":127,"
                + "\"depthImbalance\":-0.43478260869565216,\"totalBidQty\":65,\"totalAskQty\":165},"
                + "\"clocks\":{\"statsChangedAt\":1000,\"depthChangedAt\":1001,"
                + "\"statsAgeMs\":1000,\"depthAgeMs\":999}}}";
        assertEquals(expected, json);
    }

    @Test
    @DisplayName("0 means not provided: market values render null, never a fabricated zero")
    void zeroRendersAsNull() {
        MarketSnapshot m = new MarketSnapshot();
        m.depthChangedAt = 1_000L; // depth seen, but every value is still absent
        String json = SignalAuditJson.build(
                "r", "FIFTEEN_S", "SELL", 1L, 2L, 3L, 2L, 3L,
                "arm", 10L, 10L, 1L, 2L, null, null, null, null, null, m);

        assertTrue(json.contains("\"totalBuyQty\":null"), json);
        assertTrue(json.contains("\"bid\":[{\"px\":null,\"qty\":null,\"ord\":null}"), json);
        assertTrue(json.contains("\"spread\":null"), json);
        assertTrue(json.contains("\"microprice\":null"), json);
        assertTrue(json.contains("\"depthImbalance\":null"), json);
        assertTrue(json.contains("\"statsChangedAt\":null"), json);
        assertTrue(json.contains("\"depthChangedAt\":1000"), json);
        // hasDepth is true, but every level is still 0 = not provided, so the
        // sums are not meaningful and render null with the same convention.
        assertTrue(json.contains("\"totalBidQty\":null"), json);
        assertTrue(json.contains("\"totalAskQty\":null"), json);
        // Age 0 is a real value (the clock moved at/after \"now\") — never null.
        assertTrue(json.contains("\"depthAgeMs\":0"), json);
        assertTrue(json.contains("\"candle\":{\"windowStart\":1,\"windowEnd\":2,"
                + "\"open\":null,\"high\":null,\"low\":null,\"close\":null,\"volume\":null}"), json);
    }

    @Test
    @DisplayName("a missing view renders the same object shape with nulls (one parse schema)")
    void missingViewRendersNullMarket() {
        String json = SignalAuditJson.build(
                "r", "FIFTEEN_S", "BUY", 1L, 2L, 3L, 2L, 3L,
                "live", 10L, 10L, 1L, 2L, 1L, 2L, 1L, 2L, 3L, null);

        assertTrue(json.contains("\"market\":{\"bid\":[{\"px\":null,\"qty\":null,"
                + "\"ord\":null},{\"px\":null,\"qty\":null,\"ord\":null},"
                + "{\"px\":null,\"qty\":null,\"ord\":null},{\"px\":null,\"qty\":null,"
                + "\"ord\":null},{\"px\":null,\"qty\":null,\"ord\":null}],"
                + "\"ask\":[{\"px\":null,\"qty\":null,\"ord\":null}"), json);
        assertTrue(json.contains("\"clocks\":{\"statsChangedAt\":null,\"depthChangedAt\":null,"
                + "\"statsAgeMs\":null,\"depthAgeMs\":null}}"), json);
    }

    @Test
    @DisplayName("NaN imbalance and null candle fields stay valid JSON")
    void nanAndNullCandleStayValid() {
        // Empty depth: NaN imbalance must render null, not the invalid token NaN.
        MarketSnapshot m = new MarketSnapshot();
        m.depthChangedAt = 1_000L;
        String json = SignalAuditJson.build(
                "r", "FIFTEEN_S", "BUY", 1L, 2L, 3L, 2L, 3L,
                "live", 10L, 10L, null, null, null, null, null, null, null, m);
        assertTrue(json.contains("\"depthImbalance\":null"), json);
        assertTrue(!json.contains("NaN"), json);
        assertTrue(json.endsWith("}}"), json);
    }

    @Test
    @DisplayName("string values are JSON-escaped")
    void stringsAreEscaped() {
        String json = SignalAuditJson.build(
                "r\"\\\\x", "FIFTEEN_S", "BUY", 1L, 2L, 3L, 2L, 3L,
                "live", 10L, 10L, 1L, 2L, null, null, null, null, null, null);
        assertTrue(json.contains("\"id\":\"r\\\"\\\\\\\\x\""), json);
    }
}
