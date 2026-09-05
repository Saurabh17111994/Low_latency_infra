package com.trading.compute.signaljob;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Smoke stub strategy (strategy-host design, 2026-09-05): the placeholder
 * every real strategy starts as. It counts completed candles and live ticks
 * per instrument and timeframe and never emits — its counters prove the host
 * routes both streams, while the canonical filter's refusal to admit its id
 * proves nothing leaks to current-state.
 *
 * <p>Copy this file to start a real strategy: keep the counting shape for
 * flow visibility, replace the never-emit with rule math, and register the
 * new id in {@link Strategies}.
 */
public class StubSmokeStrategy implements SignalStrategy {

    private static final long serialVersionUID = 1L;

    /** Rule id: deliberately NOT in the canonical admitted set (by design). */
    public static final String RULE_ID = "stub-smoke-v1";

    private final long[] closedPerTf = new long[Timeframe.values().length];
    private final long[] livePerTf = new long[Timeframe.values().length];

    @Override
    public String ruleId() {
        return RULE_ID;
    }

    @Override
    public void onClosedCandle(RowData closed, Collector<RowData> out) {
        closedPerTf[tfOf(closed.getString(CandleClosedColumns.TF).toString()).ordinal()]++;
    }

    @Override
    public void onLiveTick(RowData live, Collector<RowData> out) {
        livePerTf[tfOf(live.getString(CandleLiveColumns.TF).toString()).ordinal()]++;
    }

    /** Test seam: closed candles seen for one timeframe. */
    long closedCountForTest(Timeframe tf) {
        return closedPerTf[tf.ordinal()];
    }

    /** Test seam: live ticks seen for one timeframe. */
    long liveCountForTest(Timeframe tf) {
        return livePerTf[tf.ordinal()];
    }

    private static Timeframe tfOf(String code) {
        return Timeframe.valueOf(code);
    }
}
