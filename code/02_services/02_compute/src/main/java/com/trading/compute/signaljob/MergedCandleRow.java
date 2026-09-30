package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.Map;

/**
 * Wave B (DEC-059): one row of the merged {@code candle_features} KV table —
 * {@code candle_closed}'s 15 columns + the feature snapshot (DEC-057: a MAP
 * keyed by the append-only registry ids) + {@code sealed}.
 *
 * <p>One writer only (the strategy host): forming rows ({@code sealed = false})
 * on the live cadence, the final sealed row at close with the close-cadence
 * features. A sealed row is terminal — {@link #acceptsUpdate(boolean)} is the
 * single rule the writer consults, so a late tick can never rewrite a sealed
 * window (DDL 35; DEC-059).
 */
public record MergedCandleRow(
        long instrumentToken,
        String exchange,
        String symbol,
        String tf,
        long windowStart,
        long windowEnd,
        long openPaise,
        long highPaise,
        long lowPaise,
        long closePaise,
        long volume,
        int tickCount,
        long lastEventTime,
        String lastEventFingerprint,
        String schemaVersion,
        Map<Integer, Double> features,
        boolean sealed)
        implements Serializable {

    public MergedCandleRow {
        features = features == null ? Map.of() : Map.copyOf(features);
    }

    /** Sealed rows are terminal: no update may rewrite one (DEC-059). */
    public static boolean acceptsUpdate(boolean existingSealed) {
        return !existingSealed;
    }

    /** A forming row from the candle snapshot + the current feature values. */
    public static MergedCandleRow forming(
            ClosedCandle candle, long token, String exchange, String symbol, String tf,
            Map<Integer, Double> features) {
        return from(candle, token, exchange, symbol, tf, features, false);
    }

    /** The final sealed row (written once at close with the close-cadence features). */
    public static MergedCandleRow sealed(
            ClosedCandle candle, long token, String exchange, String symbol, String tf,
            Map<Integer, Double> features) {
        return from(candle, token, exchange, symbol, tf, features, true);
    }

    private static MergedCandleRow from(
            ClosedCandle candle, long token, String exchange, String symbol, String tf,
            Map<Integer, Double> features, boolean sealed) {
        return new MergedCandleRow(
                token, exchange, symbol, tf,
                candle.windowStart, candle.windowEnd,
                candle.openPaise, candle.highPaise, candle.lowPaise, candle.closePaise,
                candle.volume, (int) candle.tickCount,
                candle.lastEventTime, candle.lastFingerprint,
                CandleClosedColumns.SCHEMA_VERSION_V1, features, sealed);
    }
}
