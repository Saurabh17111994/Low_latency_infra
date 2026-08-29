package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

/** Verifies CandlePreviewTrigger actually fires previews on event time. */
class CandlePreviewTriggerTest {

    static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    static RowData tick(long token, long ts, long price) {
        GenericRowData r = new GenericRowData(20);
        r.setField(RawTableColumns.EVENT_FINGERPRINT, org.apache.flink.table.data.StringData.fromString("fp-" + token + "-" + ts));
        r.setField(RawTableColumns.INSTRUMENT_TOKEN, token);
        r.setField(RawTableColumns.EXCHANGE, org.apache.flink.table.data.StringData.fromString("NSE"));
        r.setField(RawTableColumns.SYMBOL, org.apache.flink.table.data.StringData.fromString("TEST"));
        r.setField(RawTableColumns.EVENT_TIME, ts);
        r.setField(RawTableColumns.TICK_TYPE, org.apache.flink.table.data.StringData.fromString("TRADE"));
        r.setField(RawTableColumns.LAST_PRICE_PAISE, price);
        r.setField(RawTableColumns.LAST_QTY, 100L);
        r.setField(RawTableColumns.SCHEMA_VERSION, "2");
        return r;
    }

    static KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> create(
            long windowMs, long intervalMs) throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        KeySelector<RowData, Long> keySelector =
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN);
        WindowOperatorBuilder<RowData, Long, TimeWindow> builder = new WindowOperatorBuilder<>(
                TumblingEventTimeWindows.of(Duration.ofMillis(windowMs)),
                CandlePreviewTrigger.of(Duration.ofMillis(intervalMs)),
                new ExecutionConfig(),
                TypeInformation.of(RowData.class),
                keySelector,
                Types.LONG);
        builder.allowedLateness(Duration.ofMillis(5000));
        OneInputStreamOperator<RowData, RowData> operator = builder.aggregate(
                new CandleAggregateFunction(),
                new CandlePreviewEmitFunction(config),
                TypeInformation.of(CandleAccumulator.class));
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, Types.LONG);
        harness.open();
        return harness;
    }

    @Test
    void previewFiresOnEventTime() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h = create(15000, 1000);
        long t0 = 1_700_000_000_000L; // aligned to 15s? 1700000000000 % 15000 = 5000 — align:
        t0 = (t0 / 15000) * 15000;    // 1699999995000
        // Feed a tick at window start + 100ms (event time).
        h.processElement(new StreamRecord<>(tick(1L, t0 + 100, 100L), t0 + 100));
        // Advance the WATERMARK (event time) through the window — the
        // event-time preview timers at +1s/+2s/+3s must fire.
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 1000));
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 2000));
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 3000));
        long emitted = h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (StreamRecord<?>) o)
                .filter(r -> r.getValue() instanceof RowData)
                .count();
        System.out.println("preview rows emitted after 3s of event time: " + emitted);
        assertTrue(emitted >= 1, "expected >=1 preview row after 3s of event time, got " + emitted);
        h.close();
    }

    /**
     * Regression (2026-08-29, live): at high element rates the naive re-arm
     * on every element pushed the processing-time preview timer forward
     * forever — the window consumed 1.68M records and emitted 0 previews.
     * The once-per-window registration flag must keep the timer firing even
     * when elements keep arriving between event-time advances.
     */
    @Test
    void previewStillFiresUnderContinuousElements() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h = create(15000, 1000);
        long t0 = 1_700_000_000_000L;
        t0 = (t0 / 15000) * 15000;
        // Simulate 20Hz x 1024 tokens / 8 parallel ~= 2560 el/s: feed 2000
        // elements within the first 3s without advancing event time.
        for (int i = 0; i < 2000; i++) {
            long ts = i % 3000; // 0..2999ms into the window (relative to t0)
            h.processElement(new StreamRecord<>(tick(1L, t0 + ts, 100L + i), t0 + ts));
        }
        // Now advance event time — the first preview timer (registered once
        // at the first element) must fire at t0+1000 and re-arm.
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 1000));
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 2000));
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 3000));
        long emitted = h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (StreamRecord<?>) o)
                .filter(r -> r.getValue() instanceof RowData)
                .count();
        System.out.println("preview rows emitted under continuous elements: " + emitted);
        assertTrue(emitted >= 2, "expected >=2 preview rows under load, got " + emitted);
        h.close();
    }

    /**
     * Regression (2026-08-29, live): the register-once flag used keyed
     * {@code ValueState<Boolean>}, which is shared across ALL windows of a
     * key. After the first window set the flag, every later window of the
     * same key never registered a preview timer — observed live as
     * {@code processingTimeFires=20k} (one stale re-arming timer) with
     * {@code previews.emitted=0}. The flag must be per-window: each window
     * of the same key must get its own preview timer.
     */
    @Test
    void everyWindowOfSameKeyGetsPreviews() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h = create(15000, 1000);
        long t0 = 1_700_000_000_000L;
        t0 = (t0 / 15000) * 15000;

        // Window 1: tick at start+100ms, previews at +1s/+2s (event time).
        h.processElement(new StreamRecord<>(tick(1L, t0 + 100, 100L), t0 + 100));
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 1000));
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 2000));

        // Complete window 1 (event time) so its state is purged; then start
        // window 2 with the SAME key — this is the case that failed live.
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 15000));
        h.processElement(new StreamRecord<>(tick(1L, t0 + 15100, 200L), t0 + 15100));

        // Advance event time: window 2's preview timer must have been
        // registered (per-window flag), not suppressed by window 1's flag.
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 16100));
        h.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(t0 + 17100));

        long emitted = h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (StreamRecord<?>) o)
                .filter(r -> r.getValue() instanceof RowData)
                .count();
        System.out.println("preview rows emitted across two windows of same key: " + emitted);
        // Window 1 contributes >=1 (preview at +1s or +2s), window 2 must
        // contribute >=1 (its own timer). The old per-key flag bug yields
        // only window-1 rows (the stale timer re-arms past window 1's end and
        // is skipped by the guard) -> assert >=2 catches it.
        assertTrue(emitted >= 2, "expected previews in BOTH windows of the same key, got " + emitted);
        h.close();
    }
}
