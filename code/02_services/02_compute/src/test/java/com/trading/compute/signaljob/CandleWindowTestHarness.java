package com.trading.compute.signaljob;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorBuilder;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

/**
 * Test support: builds the REAL window operator the {@code SignalJob} graph
 * runs (same {@code WindowOperatorBuilder} path {@code WindowedStream.aggregate}
 * uses internally — {@code TumblingEventTimeWindows} + {@code EventTimeTrigger}
 * + {@code CandleAggregateFunction} → {@code CandleEmitFunction}, with
 * allowed-lateness and the {@code CandleLateDrop} late-data tag) and wraps it
 * in a keyed one-input operator harness. This is the Flink 2.x replacement for
 * the removed {@code ProcessWindowFunctionTestHarness}: the window-operator
 * harness drives the exact emit path ({@code CandleEmitFunction.process} with
 * real {@code windowState()}) with injected watermarks and processing time.
 */
final class CandleWindowTestHarness {

    private CandleWindowTestHarness() {}

    /** Same 5-key baseline as SignalJobConfigTest; tuning keys take defaults. */
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

    /**
     * Builds the real candle window operator and wraps it in a keyed harness.
     *
     * @param windowMs         the tumbling window size (production: 15000)
     * @param allowedLateness  the allowed-lateness bound (production: 5000)
     */
    static KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> create(
            long windowMs, long allowedLatenessMs) throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        KeySelector<RowData, Long> keySelector =
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN);

        WindowOperatorBuilder<RowData, Long, TimeWindow> builder = new WindowOperatorBuilder<>(
                TumblingEventTimeWindows.of(Duration.ofMillis(windowMs)),
                EventTimeTrigger.create(),
                new ExecutionConfig(),
                // The window input is the deduped raw tick — a RowData whose
                // layout the operators read via RawTableColumns. The type info
                // only feeds the builder's serialization plumbing; GenericTypeInfo
                // (Kryo) is fine because the window stores the CandleAccumulator,
                // not the raw rows.
                TypeInformation.of(RowData.class),
                keySelector,
                Types.LONG);
        builder.allowedLateness(Duration.ofMillis(allowedLatenessMs));
        builder.sideOutputLateData(CandleLateDrop.OUTPUT);
        OneInputStreamOperator<RowData, RowData> operator = builder.aggregate(
                new CandleAggregateFunction(),
                new CandleEmitFunction(config),
                TypeInformation.of(CandleAccumulator.class));

        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, Types.LONG);
        harness.open();
        return harness;
    }

    /** Candle output rows only — filters out watermarks/timers in {@code getOutput()}. */
    static long candleRowCount(KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream()
                .filter(o -> o instanceof org.apache.flink.streaming.runtime.streamrecord.StreamRecord)
                .count();
    }
}
