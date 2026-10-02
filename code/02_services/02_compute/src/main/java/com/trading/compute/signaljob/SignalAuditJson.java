package com.trading.compute.signaljob;

/**
 * Shared builder for the versioned strategy audit written to
 * {@code Signal_Candidates.score_inputs} (v2, 2026-10-02, CHG-504).
 *
 * <p><b>Why.</b> Signal rows are immutable, so the audit is the one chance to
 * record <i>why</i> a strategy fired. v1 stamped only N7's seven setup fields;
 * the market state the rule actually read (the 42 base latest-known values +
 * the two change clocks; the seven CHG-516 extras are feature-only and not in
 * this document), the candle in context, and the fire path were lost. v2
 * carries all of it as one self-contained JSON document, so "why did this
 * signal fire?" is answered by the row itself — no raw-tick reconstruction.
 *
 * <p><b>Shape (stable per version).</b>
 * <pre>{@code
 * {"v":2,
 *  "strategy":{"id":...,"tf":...,"side":...,"windowStart":...,"windowEnd":...,
 *              "levelHigh":...,"levelLow":...,"range":...,"triggerPrice":...},
 *  "fire":{"path":"live"|"arm","detectionTs":...,"evaluationTs":...},
 *  "candle":{"windowStart":...,"windowEnd":...,"open":...,"high":...,
 *            "low":...,"close":...,"volume":...},
 *  "market":{"bid":[{"px":...,"qty":...,"ord":...} x5],
 *            "ask":[...],
 *            "stats":{"totalBuyQty":...,"totalSellQty":...,"dayOpen":...,
 *                     "dayHigh":...,"dayLow":...,"prevClose":...,"vwap":...,
 *                     "openInterest":...,"oiDayHigh":...,"oiDayLow":...,
 *                     "lowerLimit":...,"upperLimit":...},
 *            "derived":{"spread":...,"microprice":...,"depthImbalance":...,
 *                       "totalBidQty":...,"totalAskQty":...},
 *            "clocks":{"statsChangedAt":...,"depthChangedAt":...,
 *                      "statsAgeMs":...,"depthAgeMs":...}}}
 * }</pre>
 *
 * <p><b>Null conventions.</b> The market model's {@code 0 = not provided} is
 * kept distinguishable: a never-seen value renders {@code null}, never a
 * fabricated zero. Non-finite derived values (NaN imbalance) render
 * {@code null} — the document is always valid JSON. A missing {@link MarketView}
 * (a direct legacy callback without the host's view) renders the same object
 * shape with null values, so consumers parse one schema.
 *
 * <p><b>Cost.</b> Built only at fire time — signals are rare, so the hot path
 * pays nothing. No new state, no new column: the document rides the existing
 * free-STRING {@code score_inputs} column (DDL frozen); the {@code v} field
 * makes a future typed-column migration mechanical.
 */
public final class SignalAuditJson {

    /** Audit schema version carried in every v2 document. */
    public static final int VERSION = 2;

    private SignalAuditJson() {}

    /**
     * Candle context for the audit: the triggering forming row on the live
     * path, the arming closed candle on the arm path. Every field is nullable
     * ({@code null} = not carried by that row).
     */
    public record AuditCandle(
            Long windowStart,
            Long windowEnd,
            Long openPaise,
            Long highPaise,
            Long lowPaise,
            Long closePaise,
            Long volume) {}

    /**
     * Renders the full v2 audit document. All parameters are the strategy's
     * own decision inputs; {@code market} may be null (no view available).
     */
    public static String build(
            String ruleId,
            String tf,
            String side,
            long setupWindowStart,
            long setupWindowEnd,
            long levelHighPaise,
            long levelLowPaise,
            long triggerPricePaise,
            String firePath,
            long detectionTs,
            long evaluationTs,
            Long candleWindowStart,
            Long candleWindowEnd,
            Long candleOpenPaise,
            Long candleHighPaise,
            Long candleLowPaise,
            Long candleClosePaise,
            Long candleVolume,
            MarketView market) {
        StringBuilder sb = new StringBuilder(1_024);
        sb.append("{\"v\":").append(VERSION);

        sb.append(",\"strategy\":{");
        appendString(sb, "id", ruleId, true);
        appendString(sb, "tf", tf, false);
        appendString(sb, "side", side, false);
        appendNumber(sb, "windowStart", setupWindowStart, false);
        appendNumber(sb, "windowEnd", setupWindowEnd, false);
        appendNumber(sb, "levelHigh", levelHighPaise, false);
        appendNumber(sb, "levelLow", levelLowPaise, false);
        appendNumber(sb, "range", levelHighPaise - levelLowPaise, false);
        appendNumber(sb, "triggerPrice", triggerPricePaise, false);
        sb.append('}');

        sb.append(",\"fire\":{");
        appendString(sb, "path", firePath, true);
        appendNumber(sb, "detectionTs", detectionTs, false);
        appendNumber(sb, "evaluationTs", evaluationTs, false);
        sb.append('}');

        sb.append(",\"candle\":{");
        appendNullable(sb, "windowStart", candleWindowStart, true);
        appendNullable(sb, "windowEnd", candleWindowEnd, false);
        appendNullable(sb, "open", candleOpenPaise, false);
        appendNullable(sb, "high", candleHighPaise, false);
        appendNullable(sb, "low", candleLowPaise, false);
        appendNullable(sb, "close", candleClosePaise, false);
        appendNullable(sb, "volume", candleVolume, false);
        sb.append('}');

        sb.append(",\"market\":{");
        appendMarket(sb, market, evaluationTs);
        sb.append("}}");
        return sb.toString();
    }

    // ── market block ─────────────────────────────────────────────────────

    private static void appendMarket(StringBuilder sb, MarketView m, long now) {
        sb.append("\"bid\":[");
        appendLevels(sb, m, true);
        sb.append("],\"ask\":[");
        appendLevels(sb, m, false);
        sb.append(']');

        sb.append(",\"stats\":{");
        appendNullable(sb, "totalBuyQty", valueOrNull(m == null ? 0L : m.totalBuyQty()), true);
        appendNullable(sb, "totalSellQty", valueOrNull(m == null ? 0L : m.totalSellQty()), false);
        appendNullable(sb, "dayOpen", valueOrNull(m == null ? 0L : m.dayOpenPaise()), false);
        appendNullable(sb, "dayHigh", valueOrNull(m == null ? 0L : m.dayHighPaise()), false);
        appendNullable(sb, "dayLow", valueOrNull(m == null ? 0L : m.dayLowPaise()), false);
        appendNullable(sb, "prevClose", valueOrNull(m == null ? 0L : m.prevClosePaise()), false);
        appendNullable(sb, "vwap", valueOrNull(m == null ? 0L : m.vwapPaise()), false);
        appendNullable(sb, "openInterest", valueOrNull(m == null ? 0L : m.openInterest()), false);
        appendNullable(sb, "oiDayHigh", valueOrNull(m == null ? 0L : m.oiDayHigh()), false);
        appendNullable(sb, "oiDayLow", valueOrNull(m == null ? 0L : m.oiDayLow()), false);
        appendNullable(sb, "lowerLimit", valueOrNull(m == null ? 0L : m.lowerLimitPaise()), false);
        appendNullable(sb, "upperLimit", valueOrNull(m == null ? 0L : m.upperLimitPaise()), false);
        sb.append('}');

        boolean hasDepth = m != null && m.hasDepth();
        sb.append(",\"derived\":{");
        appendNullable(sb, "spread", hasDepth ? spreadOrNull(m) : null, true);
        appendNullable(sb, "microprice", hasDepth ? micropriceOrNull(m) : null, false);
        appendDouble(sb, "depthImbalance", hasDepth ? imbalanceOrNull(m) : null, false);
        appendNullable(sb, "totalBidQty", hasDepth ? m.totalBidQty() : null, false);
        appendNullable(sb, "totalAskQty", hasDepth ? m.totalAskQty() : null, false);
        sb.append('}');

        long statsChangedAt = m == null ? 0L : m.statsChangedAt();
        long depthChangedAt = m == null ? 0L : m.depthChangedAt();
        sb.append(",\"clocks\":{");
        appendNullable(sb, "statsChangedAt", statsChangedAt <= 0L ? null : statsChangedAt, true);
        appendNullable(sb, "depthChangedAt", depthChangedAt <= 0L ? null : depthChangedAt, false);
        if (statsChangedAt <= 0L) {
            appendNullable(sb, "statsAgeMs", null, false);
        } else {
            // Age 0 is a real value (the group changed at or after "now") —
            // never collapse it to null, which means "never seen".
            appendNumber(sb, "statsAgeMs", m.statsAgeMs(now), false);
        }
        if (depthChangedAt <= 0L) {
            appendNullable(sb, "depthAgeMs", null, false);
        } else {
            appendNumber(sb, "depthAgeMs", m.depthAgeMs(now), false);
        }
        sb.append('}');
    }

    private static void appendLevels(StringBuilder sb, MarketView m, boolean bid) {
        for (int level = 1; level <= MarketView.DEPTH_LEVELS; level++) {
            if (level > 1) {
                sb.append(',');
            }
            sb.append("{\"px\":");
            appendNumberOrNull(sb, m == null ? 0L : (bid ? m.bidPxPaise(level) : m.askPxPaise(level)));
            sb.append(",\"qty\":");
            appendNumberOrNull(sb, m == null ? 0L : (bid ? m.bidQty(level) : m.askQty(level)));
            sb.append(",\"ord\":");
            appendNumberOrNull(sb, m == null ? 0L : (bid ? m.bidOrd(level) : m.askOrd(level)));
            sb.append('}');
        }
    }

    private static Long spreadOrNull(MarketView m) {
        long spread = m.spreadPaise();
        // spreadPaise() is 0 when either side is absent; a genuine 0 spread
        // (locked book) is possible but indistinguishable — the 0 = not
        // provided convention wins, same as every other market value.
        return spread == 0L ? null : spread;
    }

    private static Long micropriceOrNull(MarketView m) {
        long microprice = m.micropricePaise();
        return microprice == 0L ? null : microprice;
    }

    private static Double imbalanceOrNull(MarketView m) {
        double imbalance = m.depthImbalance();
        return Double.isFinite(imbalance) ? imbalance : null;
    }

    private static Long valueOrNull(long v) {
        return v == 0L ? null : v;
    }

    // ── JSON primitives ──────────────────────────────────────────────────

    private static void appendString(StringBuilder sb, String name, String value, boolean first) {
        comma(sb, first);
        sb.append('"').append(name).append("\":");
        if (value == null) {
            sb.append("null");
            return;
        }
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }

    private static void appendNumber(StringBuilder sb, String name, long value, boolean first) {
        comma(sb, first);
        sb.append('"').append(name).append("\":").append(value);
    }

    private static void appendNullable(StringBuilder sb, String name, Long value, boolean first) {
        comma(sb, first);
        sb.append('"').append(name).append("\":");
        appendNumberOrNull(sb, value == null ? 0L : value);
    }

    private static void appendDouble(StringBuilder sb, String name, Double value, boolean first) {
        comma(sb, first);
        sb.append('"').append(name).append("\":");
        if (value == null || !Double.isFinite(value)) {
            sb.append("null");
        } else {
            sb.append(value);
        }
    }

    /** Renders {@code 0} as null (the market model's "not provided" convention). */
    private static void appendNumberOrNull(StringBuilder sb, long value) {
        if (value == 0L) {
            sb.append("null");
        } else {
            sb.append(value);
        }
    }

    private static void comma(StringBuilder sb, boolean first) {
        if (!first) {
            sb.append(',');
        }
    }
}
