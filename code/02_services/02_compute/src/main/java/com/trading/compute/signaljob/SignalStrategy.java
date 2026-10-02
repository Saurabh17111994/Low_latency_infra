package com.trading.compute.signaljob;

import com.trading.compute.feature.FeatureView;
import java.io.Serializable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Plug-and-play strategy contract (strategy-host design, 2026-09-05).
 *
 * <p>A strategy is pure rule math over two streams the platform owns:
 * completed candles and live forming-candle snapshots, both keyed by
 * {@code instrument_token} by the host before dispatch. The host owns
 * everything else — routing, per-instrument slots, emit-dedup, metrics,
 * sinks, and operator UIDs — so adding a strategy never touches
 * {@code SignalJob}, and removing one is a config-line deletion.
 *
 * <p>One instance serves exactly one instrument: the host creates a fresh
 * instance per {@code (token, ruleId)} on first sight (heap, intentional
 * amnesia — a restore rebuilds from replayed candles, exactly like the retired N7 operator's slots). Instances must therefore be cheap to
 * construct and must keep per-instrument state in plain fields.
 *
 * <p>Emitted rows must be full 22-column {@code Signal_Candidates} rows
 * ({@link SignalCandidatesTableColumns}) with a deterministic
 * {@code candidate_id} that is unique per logical signal — the host dedups
 * on it across restores. The id MUST embed {@code instrument_token}
 * (P2-176): host dedup state is keyed state partitioned by token, so only a
 * token-bearing id makes per-key dedup equivalent to global dedup. A null
 * or blank id is dropped and counted by the host, never failed: silent
 * unkeyed rows would defeat exactly-once.
 */
public interface SignalStrategy extends Serializable {

    /**
     * Metrics handle the host hands to each strategy instance at
     * construction. Per-rule scope ({@code strategy/<ruleId>}) — a new
     * strategy is observable with no dashboard change. Implementations must
     * tolerate a handle that only counts on the heap (unit harnesses).
     */
    interface Metrics extends Serializable {
        void inc(String name, long n);
    }

    /** Rule id stamped on emitted rows; unique across registered strategies. */
    String ruleId();

    /**
     * Completed candle ({@link CandleClosedColumns} layout). Arming-only in
     * the N7 shape, but the contract permits emission here too (the host
     * dedups both inputs identically).
     */
    void onClosedCandle(RowData closed, Collector<RowData> out) throws Exception;

    /**
     * Live forming-candle snapshot ({@link CandleLiveColumns} layout). The
     * host calls this at most once per {@code (token, last_event_time)} —
     * implementations decide firing. This is the per-trade-price evaluation
     * point in the N7 shape.
     */
    void onLiveTick(RowData live, Collector<RowData> out) throws Exception;

    /**
     * Feature-aware overload (DEC-056): the same live tick plus the shared
     * {@link FeatureView} for this instrument. The host calls <b>this</b>
     * method when it is overridden; the default delegates to
     * {@link #onLiveTick(RowData, Collector)}, so existing strategies keep
     * working unchanged. Override one or the other, not both.
     */
    default void onLiveTick(RowData live, FeatureView features, Collector<RowData> out)
            throws Exception {
        onLiveTick(live, out);
    }

    /**
     * Feature-aware overload (DEC-056): the closed candle plus the shared
     * {@link FeatureView}. Same delegation contract as
     * {@link #onLiveTick(RowData, FeatureView, Collector)}.
     */
    default void onClosedCandle(RowData closed, FeatureView features, Collector<RowData> out)
            throws Exception {
        onClosedCandle(closed, out);
    }

    /**
     * Context-aware overload (C2, live-candle context): the live forming-candle
     * snapshot plus the {@link ContextView} for on-demand old data. The host
     * calls <b>this</b> form; the default delegates to
     * {@link #onLiveTick(RowData, FeatureView, Collector)}, so strategies that
     * never fetch behave exactly as before. Override one form, not both.
     *
     * <p>A request whose value is not cached schedules exactly one
     * asynchronous fetch and returns {@code null} for this tick; the host then
     * calls {@link #onContextReady} with the same live candle as soon as the
     * data arrives. Never block or wait inside this call.
     */
    default void onLiveTick(RowData live, ContextView context, FeatureView features,
            Collector<RowData> out) throws Exception {
        onLiveTick(live, features, out);
    }

    /**
     * Context-ready re-evaluation (C2): the host calls this on the operator
     * thread when data a strategy requested on
     * {@link #onLiveTick(RowData, ContextView, FeatureView, Collector)} became
     * available, passing the most recent live forming-candle snapshot. The
     * strategy decides on that live candle — a decision is never deferred to
     * the candle close (hard requirement,
     * docs/plans/2026-09-30-strategy-context-live-fetch.md).
     *
     * <p><b>Idempotency (hard):</b> the same
     * {@code (instrument_token, last_event_time)} may be evaluated more than
     * once — one or more live ticks plus ready wake-ups. Implementations must
     * be idempotent; emitted rows are deduped by their deterministic
     * {@code candidate_id}.
     *
     * <p>Only data-ready transitions call this: a fetch that timed out or
     * resolved absent is counted and retried after a cooldown, and the strategy
     * is re-evaluated on its normal live ticks meanwhile.
     */
    default void onContextReady(RowData live, ContextView context, Collector<RowData> out)
            throws Exception {}

    /**
     * Feature-aware {@link #onContextReady(RowData, ContextView, Collector)};
     * the host calls this form.
     */
    default void onContextReady(RowData live, ContextView context, FeatureView features,
            Collector<RowData> out) throws Exception {
        onContextReady(live, context, out);
    }

    /**
     * View-aware overload (2026-10-01 native design): the live forming-candle
     * snapshot plus the shared {@link StrategyView} bundle (market + features
     * + context). The host calls <b>this</b> form; the default delegates to
     * {@link #onLiveTick(RowData, ContextView, FeatureView, Collector)}, so
     * every strategy written before the view existed keeps working unchanged.
     * Override one form of a callback, not several.
     */
    default void onLiveTick(RowData live, StrategyView view, Collector<RowData> out)
            throws Exception {
        onLiveTick(live, view.context(), view.features(), out);
    }

    /**
     * View-aware {@link #onClosedCandle(RowData, FeatureView, Collector)};
     * the host calls this form. The market view carries the latest snapshot
     * for the instrument (refreshed on the canonical forming row), so a
     * strategy can read market state on the close path too.
     */
    default void onClosedCandle(RowData closed, StrategyView view, Collector<RowData> out)
            throws Exception {
        onClosedCandle(closed, view.features(), out);
    }

    /**
     * View-aware {@link #onContextReady(RowData, ContextView, FeatureView, Collector)};
     * the host calls this form.
     */
    default void onContextReady(RowData live, StrategyView view, Collector<RowData> out)
            throws Exception {
        onContextReady(live, view.context(), view.features(), out);
    }

    /**
     * Market-only update (2026-10-02, CHG-505): the host hands the fresh market
     * state when a non-trade tick changed it and no forming-candle row was
     * emitted ({@code STRATEGY_MARKET_TICK_ENABLED}). The row is a
     * {@link CandleLiveColumns} layout with {@code TF="MKT"} — identity and the
     * 51-column market section are meaningful; the candle columns are null.
     *
     * <p><b>Default no-op</b>, so every strategy written before this callback
     * behaves exactly as before. A strategy that wants to fire on book/stat
     * moves alone overrides this and decides with its own rule; emitted rows go
     * through the host's per-rule dedup exactly like the other callbacks. The
     * host calls this instead of {@link #onLiveTick} on a market row — never
     * both.
     */
    default void onMarketUpdate(RowData market, StrategyView view, Collector<RowData> out)
            throws Exception {}
}
