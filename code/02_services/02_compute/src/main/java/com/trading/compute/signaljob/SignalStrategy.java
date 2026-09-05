package com.trading.compute.signaljob;

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
 * amnesia — a restore rebuilds from replayed candles, exactly like
 * {@link N7SignalFunction}'s slots). Instances must therefore be cheap to
 * construct and must keep per-instrument state in plain fields.
 *
 * <p>Emitted rows must be full 22-column {@code Signal_Candidates} rows
 * ({@link SignalCandidatesTableColumns}) with a deterministic
 * {@code candidate_id} that is unique per logical signal — the host dedups
 * on it across restores. A null or blank id fails the emission fast: silent
 * unkeyed rows would defeat exactly-once.
 */
public interface SignalStrategy extends Serializable {

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
}
