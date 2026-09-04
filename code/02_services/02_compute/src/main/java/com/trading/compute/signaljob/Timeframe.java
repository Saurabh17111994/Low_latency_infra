package com.trading.compute.signaljob;

import java.io.Serializable;

/**
 * Canonical set of candle timeframes for the multi-timeframe aggregator.
 *
 * <p>Six timeframes cover both epoch-aligned (wall-clock) and session-aligned
 * (NSE cash 09:15 IST open anchor) bucketing per design Decisions 3–5 and §D.
 * Each constant carries its window length in epoch-millis, its alignment mode,
 * and the exact {@code tf} STRING code used as discriminator in the DDL
 * contracts for {@code candle_live} / {@code candle_closed} (PK
 * {@code (instrument_token, tf, window_start)}). Track A uses the same codes —
 * they must match exactly.
 *
 * <p>Plain Java, zero Flink imports, {@link Serializable} — safe to embed in
 * Flink state, checkpoints, and Fluss row payloads without pulling the Flink
 * runtime into the type's transitive closure.
 */
public enum Timeframe implements Serializable {

    /**
     * 15-second timeframe — epoch-aligned.
     * {@code bucketStart = floor(t / 15_000) * 15_000}, timezone-free.
     * Half-open {@code [start, start+15_000)}.
     */
    FIFTEEN_S(15_000L, false, "FIFTEEN_S"),

    /**
     * 30-second timeframe — epoch-aligned.
     * {@code bucketStart = floor(t / 30_000) * 30_000}.
     * Half-open {@code [start, start+30_000)}.
     */
    THIRTY_S(30_000L, false, "THIRTY_S"),

    /**
     * 1-minute timeframe — epoch-aligned.
     * {@code bucketStart = floor(t / 60_000) * 60_000}.
     * Half-open {@code [start, start+60_000)}.
     */
    ONE_M(60_000L, false, "ONE_M"),

    /**
     * 3-minute timeframe — session-aligned to NSE open 09:15 IST.
     * {@code bucketStart = floor((t - openMs)/180_000)*180_000 + openMs}
     * where {@code openMs} is 09:15 Asia/Kolkata of t's date.
     * Pre-open and {@code >=15:30} filtered before bucketing.
     */
    THREE_M(180_000L, true, "THREE_M"),

    /**
     * 5-minute timeframe — session-aligned to NSE open 09:15 IST.
     * {@code bucketStart = floor((t - openMs)/300_000)*300_000 + openMs}.
     * Half-open; boundaries at 09:15, 09:20, 09:25, … 15:30.
     */
    FIVE_M(300_000L, true, "FIVE_M"),

    /**
     * 15-minute timeframe — session-aligned to NSE open 09:15 IST.
     * {@code bucketStart = floor((t - openMs)/900_000)*900_000 + openMs}.
     * Boundaries at 09:15, 09:30, 09:45, … 15:30 (the final 15m bucket is
     * {@code [15:15, 15:30)}; at 15:30 forced-roll seals it).
     */
    FIFTEEN_M(900_000L, true, "FIFTEEN_M");

    private final long windowMs;
    private final boolean sessionAligned;
    private final String code;

    Timeframe(long windowMs, boolean sessionAligned, String code) {
        this.windowMs = windowMs;
        this.sessionAligned = sessionAligned;
        this.code = code;
    }

    /**
     * Window length in milliseconds.
     *
     * @return window duration; one of 15000, 30000, 60000, 180000, 300000, 900000
     */
    public long windowMs() {
        return windowMs;
    }

    /**
     * Whether bucketing is anchored to the NSE session open (09:15 IST) rather than epoch.
     *
     * @return true for THREE_M, FIVE_M, FIFTEEN_M; false for FIFTEEN_S, THIRTY_S, ONE_M
     */
    public boolean isSessionAligned() {
        return sessionAligned;
    }

    /**
     * Exact {@code tf} STRING discriminator used in Fluss DDL PK
     * {@code (instrument_token, tf, window_start)}.
     * Matches Track A codes exactly: {@code FIFTEEN_S}, {@code THIRTY_S},
     * {@code ONE_M}, {@code THREE_M}, {@code FIVE_M}, {@code FIFTEEN_M}.
     *
     * @return tf code — equals {@link #name()} by contract
     */
    public String code() {
        return code;
    }
}
