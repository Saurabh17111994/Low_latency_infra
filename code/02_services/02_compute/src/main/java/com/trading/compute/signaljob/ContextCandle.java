package com.trading.compute.signaljob;

/**
 * One immutable closed candle read from {@code candle_closed} — the value the
 * strategy context provider caches and strategies read
 * (docs/plans/2026-09-30-strategy-context-live-fetch.md).
 *
 * <p>The source row is always the sealed, first-write-wins closed candle
 * (7 d TTL, PK {@code (instrument_token, tf, window_start)}); the live table
 * is never a context source. All price fields are paise, all timestamp fields
 * are epoch millis UTC — the same units as the DDL contract.
 */
public record ContextCandle(
        long token,
        Timeframe tf,
        long windowStart,
        long windowEnd,
        long openPaise,
        long highPaise,
        long lowPaise,
        long closePaise,
        long volume,
        int tickCount,
        long lastEventTime) {}
