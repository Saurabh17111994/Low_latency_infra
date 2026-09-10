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
 * <p>Per-timeframe counters below are for THIS instance's instrument only.
 * The host creates one instance per (instrument_token, ruleId), so
 * per-instrument isolation comes from instance isolation, not from these
 * arrays (P2-177: do not copy this shape into a singleton/shared context).
 *
 * <p>Copy this file to start a real strategy: keep the counting shape for
 * flow visibility, replace the never-emit with rule math, and register the
 * new id in {@link Strategies}.
 */
public class StubSmokeStrategy implements SignalStrategy {

    private static final long serialVersionUID = 1L;

    /** Counter name for poison-TF skips on both paths (P2-059/060). */
    static final String SKIPPED_POISON_TF = "skipped_poison_tf";

    /** Rule id: deliberately NOT in the canonical admitted set (by design). */
    public static final String RULE_ID = "stub-smoke-v1";

    private final long[] closedPerTf = new long[Timeframe.values().length];
    private final long[] livePerTf = new long[Timeframe.values().length];

    /**
     * Poison-skip counter scope (P2-059/060): the stub's skips are host-visible
     * through this handle, so a corrupt producer shows up on the dashboard
     * instead of vanishing silently. The template copy must keep this — an
     * uncounted skip is a blind spot.
     */
    private final Metrics metrics;

    /** Production path: host hands each instance its per-rule handle. */
    public StubSmokeStrategy(SignalJobConfig config, Metrics metrics) {
        this.metrics =
                metrics == null ? new NoopMetrics() : metrics;
    }

    /** Heap path (unit harnesses): counts on the heap only. */
    public StubSmokeStrategy() {
        this(null, new HeapMetrics());
    }

    /** Minimal heap metrics so the no-arg path still counts skips. */
    private static final class HeapMetrics implements Metrics {
        private static final long serialVersionUID = 1L;
        private final java.util.Map<String, Long> counts = new java.util.HashMap<>();

        @Override
        public void inc(String name, long n) {
            counts.merge(name, n, Long::sum);
        }
    }

    /** Null-object metrics for callers that pass a null handle. */
    private static final class NoopMetrics implements Metrics {
        private static final long serialVersionUID = 1L;

        @Override
        public void inc(String name, long n) {}
    }

    /** Test seam: poison-skip count (P2-059/060). */
    long skippedPoisonTfForTest() {
        if (metrics instanceof HeapMetrics heap) {
            return heap.counts.getOrDefault(SKIPPED_POISON_TF, 0L);
        }
        return -1L;
    }

    @Override
    public String ruleId() {
        return RULE_ID;
    }

    @Override
    public void onClosedCandle(RowData closed, Collector<RowData> out) {
        // P2-059: a smoke stub proving routing must never be the blast
        // radius — skip corrupt discriminators instead of crashing the host.
        // Every skip is counted: an uncounted skip is a blind spot.
        if (closed == null || closed.isNullAt(CandleClosedColumns.TF)) {
            metrics.inc(SKIPPED_POISON_TF, 1);
            return;
        }
        try {
            closedPerTf[tfOf(closed.getString(CandleClosedColumns.TF).toString()).ordinal()]++;
        } catch (IllegalArgumentException unknownTf) {
            // skip unknown TF: never fail the operator on a poison discriminator
            metrics.inc(SKIPPED_POISON_TF, 1);
        }
    }

    @Override
    public void onLiveTick(RowData live, Collector<RowData> out) {
        // P2-060: same skip discipline on the live path (larger blast radius —
        // N7 would survive a corrupt candle_live row that kills this stub).
        if (live == null || live.isNullAt(CandleLiveColumns.TF)) {
            metrics.inc(SKIPPED_POISON_TF, 1);
            return;
        }
        try {
            livePerTf[tfOf(live.getString(CandleLiveColumns.TF).toString()).ordinal()]++;
        } catch (IllegalArgumentException unknownTf) {
            // skip unknown TF: never fail the operator on a poison discriminator
            metrics.inc(SKIPPED_POISON_TF, 1);
        }
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
