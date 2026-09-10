package com.trading.compute.signaljob;

import java.io.Serializable;

/**
 * Closed-candle lite snapshot kept in {@link MultiTimeframeClosedRing}.
 *
 * <p>Design §F §C.1: a closed candle is immutable once sealed. This POJO holds
 * the minimal fields needed for signal lookback (last 15 per TF) plus
 * diagnostics. Stored fields mirror the task's normative list plus
 * {@code windowEnd} for sink parity (design field table stores both
 * start/end). Tick fields reuse the same paise/qty units as
 * {@link CandleAccumulator}.
 *
 * <p>D1 (2026-08-30): public class + public fields for Flink POJO extraction.
 * Must stay public with a public no-arg constructor.
 */
public class ClosedCandle implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Inclusive window start (epoch-millis, half-open [start,end)). */
    public long windowStart;

    /** Exclusive window end (= start + tfMs, or truncated at 15:30). */
    public long windowEnd;

    public long openPaise;
    public long highPaise;
    public long lowPaise;
    public long closePaise;

    /** Summed qty of TRADE ticks only (quote rows never increment). */
    public long volume;

    /** Count of TRADE ticks only. */
    public long tickCount;

    /** Order key of the last tick in the window (close). */
    public long lastEventTime;

    /** Fingerprint of the last tick (tie-break). */
    public String lastFingerprint;

    /** Public no-arg constructor for Flink POJO. Fields default to 0 / null. */
    public ClosedCandle() {}

    public ClosedCandle(
            long windowStart,
            long windowEnd,
            long openPaise,
            long highPaise,
            long lowPaise,
            long closePaise,
            long volume,
            long tickCount,
            long lastEventTime,
            String lastFingerprint) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.openPaise = openPaise;
        this.highPaise = highPaise;
        this.lowPaise = lowPaise;
        this.closePaise = closePaise;
        this.volume = volume;
        this.tickCount = tickCount;
        this.lastEventTime = lastEventTime;
        this.lastFingerprint = lastFingerprint;
    }

}
