package com.trading.compute.signaljob;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * Shared breakout-rule lookback over COMPLETED candles (Phase 2, 2026-08-29).
 *
 * <p>Holds the bounded ring buffers (highs, closes) of the last
 * {@code lookback} completed candles per instrument, and evaluates the
 * "20-candle breakout" rule against a candidate candle's OHLCV. Used by BOTH
 * {@link SignalDetectionFunction} (finals, authoritative) and
 * {@link EarlySignalFunction} (previews, tentative) so the rule and the
 * lookback semantics live in exactly one place — the plan's "read-only state
 * getter, package-private, no duplication" step 12.
 *
 * <p>Rule v1 (DEC-034), evaluated on the candidate candle:
 * <ol>
 *   <li>Bullish: {@code close > open} (strict; a flat candle never fires).</li>
 *   <li>Breakout: {@code close > max(high of the previous {@code lookback}
 *       completed candles)} (strict).</li>
 *   <li>Trend filter: {@code close > mean(close of the previous
 *       {@code lookback} completed candles)} — exact integer comparison
 *       {@code close * n > sum}, no rounding.</li>
 * </ol>
 * No signal before {@code lookback} completed candles exist per instrument
 * (warm-up); conditions 2/3 use only candles strictly before the candidate.
 *
 * <p>The {@link ValueState} instances are owned by the enclosing operator (the
 * descriptor names are operator-scoped in Flink). Package-private and
 * read-only by contract: callers append completed candles and evaluate; they
 * never mutate the buffers directly.
 */
final class SignalLookbackState {

    private final ValueState<List<Long>> highsState;
    private final ValueState<List<Long>> closesState;
    private final int lookback;

    /** Same descriptors as the pre-Phase-2 SignalDetectionFunction (no state
     *  format change — restore-compatible). */
    private static final ValueStateDescriptor<List<Long>> HIGHS_DESCRIPTOR =
            new ValueStateDescriptor<>("signal-candle-highs", Types.LIST(Types.LONG));
    private static final ValueStateDescriptor<List<Long>> CLOSES_DESCRIPTOR =
            new ValueStateDescriptor<>("signal-candle-closes", Types.LIST(Types.LONG));

    SignalLookbackState(ValueState<List<Long>> highsState, ValueState<List<Long>> closesState,
            int lookback) {
        this.highsState = highsState;
        this.closesState = closesState;
        this.lookback = lookback;
    }

    static ValueStateDescriptor<List<Long>> highsDescriptor() {
        return HIGHS_DESCRIPTOR;
    }

    static ValueStateDescriptor<List<Long>> closesDescriptor() {
        return CLOSES_DESCRIPTOR;
    }

    /** True once {@code lookback} completed candles are buffered (warm-up done). */
    boolean isWarm() throws Exception {
        List<Long> highs = highsState.value();
        return highs != null && highs.size() >= lookback;
    }

    /** Highest high of the last {@code lookback} completed candles (0 when cold). */
    long maxHigh() throws Exception {
        List<Long> highs = highsState.value();
        if (highs == null) {
            return 0;
        }
        long max = 0;
        for (long h : highs) {
            max = Math.max(max, h);
        }
        return max;
    }

    /** Sum of the last {@code lookback} completed closes (0 when cold). */
    long sumCloses() throws Exception {
        List<Long> closes = closesState.value();
        if (closes == null) {
            return 0;
        }
        long sum = 0;
        for (long c : closes) {
            sum += c;
        }
        return sum;
    }

    /** Number of completed candles buffered (0 when cold). */
    int size() throws Exception {
        List<Long> highs = highsState.value();
        return highs == null ? 0 : highs.size();
    }

    /** Append a COMPLETED candle to the ring buffers (drop oldest beyond lookback). */
    void append(long high, long close) throws Exception {
        List<Long> highs = highsState.value();
        if (highs == null) {
            highs = new ArrayList<>();
        }
        List<Long> closes = closesState.value();
        if (closes == null) {
            closes = new ArrayList<>();
        }
        highs.add(high);
        closes.add(close);
        while (highs.size() > lookback) {
            highs.remove(0);
        }
        while (closes.size() > lookback) {
            closes.remove(0);
        }
        highsState.update(highs);
        closesState.update(closes);
    }

    /**
     * Evaluate the breakout rule on a candidate candle (completed final or
     * in-progress preview). Returns false while cold (warm-up) or when any
     * condition fails.
     */
    boolean evaluate(long open, long close) throws Exception {
        if (!isWarm()) {
            return false;
        }
        long maxHigh = maxHigh();
        List<Long> closes = closesState.value();
        long sum = 0;
        for (long c : closes) {
            sum += c;
        }
        boolean bullish = close > open;
        boolean breakout = close > maxHigh;
        boolean trend = close * (long) closes.size() > sum;
        return bullish && breakout && trend;
    }
}
