package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Candle-preview emission contract (low-latency candles Phase 1, 2026-08-29):
 * every preview row must carry the window identity, the business OHLCV fields
 * accumulated SO FAR, the {@code is_preview=TRUE} marker, and the pinned
 * preview schema version — positioned in the CandlePreviewColumns order the
 * KV sink serializes against. Previews must NOT run the OHLC invariant check
 * (they are partial); the final candle path is unchanged.
 */
@DisplayName("CandlePreviewEmitFunction.buildRow carries window identity + is_preview + versions")
class CandlePreviewEmitFunctionTest {

    private static final long T0 = 1_750_000_000_000L;

    /** Same 5-key baseline as CandleEmitFunctionTest; tuning keys take defaults. */
    private static SignalJobConfig config() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "false");
        return SignalJobConfig.from(env);
    }

    @Test
    @DisplayName("preview row carries window identity, partial OHLCV, is_preview=TRUE, pinned versions")
    void rowCarriesWindowIdentityAndPartialOhlcv() {
        SignalJobConfig config = config();

        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = 100;
        acc.highPaise = 110;   // may still climb before window end
        acc.lowPaise = 99;     // may still dip
        acc.closePaise = 105;  // tracks the latest price
        acc.volume = 25;
        acc.tickCount = 7;

        TimeWindow window = new TimeWindow(T0, T0 + 15_000L);
        GenericRowData row = CandlePreviewEmitFunction.buildRow(2885L, acc, window,
                1_750_000_005_000L, config);

        assertEquals(14, row.getArity(), "preview row must have the shared 14-column layout");
        assertEquals(2885L, row.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", row.getString(CandlePreviewColumns.EXCHANGE).toString());
        assertEquals("TEST", row.getString(CandlePreviewColumns.SYMBOL).toString());
        assertEquals(T0, row.getLong(CandlePreviewColumns.WINDOW_START));
        assertEquals(T0 + 15_000L, row.getLong(CandlePreviewColumns.WINDOW_END));
        assertEquals(100L, row.getLong(CandlePreviewColumns.OPEN_PAISE));
        assertEquals(110L, row.getLong(CandlePreviewColumns.HIGH_PAISE));
        assertEquals(99L, row.getLong(CandlePreviewColumns.LOW_PAISE));
        assertEquals(105L, row.getLong(CandlePreviewColumns.CLOSE_PAISE));
        assertEquals(25L, row.getLong(CandlePreviewColumns.VOLUME));
        assertEquals(7, row.getInt(CandlePreviewColumns.TICK_COUNT));

        // The visibility marker + pinned preview schema version:
        assertTrue(row.getBoolean(CandlePreviewColumns.IS_PREVIEW),
                "preview row must carry is_preview=TRUE");
        assertEquals("1", row.getString(CandlePreviewColumns.SCHEMA_VERSION).toString(),
                "preview schema version must be the pinned v1");
        assertEquals(1_750_000_005_000L, row.getLong(CandlePreviewColumns.OUTPUT_TS));
    }

    @Test
    @DisplayName("preview rows never carry the final-candle version columns (they are visibility-only)")
    void previewOmitsFinalOnlyColumns() {
        // The preview table deliberately drops algorithm_version and
        // configuration_version (CanonicalCandlePolicy fields) — they only
        // make sense on a COMPLETED candle. Guard the 14-column layout.
        SignalJobConfig config = config();
        assertEquals(14, CandlePreviewColumns.FIELD_COUNT);
        for (String name : CandlePreviewColumns.NAMES) {
            if (name.equals("algorithm_version") || name.equals("configuration_version")) {
                throw new AssertionError("preview must not carry " + name);
            }
        }
        assertTrue(CandlePreviewColumns.NAMES.length == 14,
                "preview row must stay 14 columns");
    }
}
