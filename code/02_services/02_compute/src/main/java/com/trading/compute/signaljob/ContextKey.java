package com.trading.compute.signaljob;

/**
 * Cache and fetch identity of one closed candle: the {@code candle_closed}
 * primary key {@code (instrument_token, tf, window_start)} (DDL 33, PK order).
 *
 * <p>Heap-only value — it never enters Flink state and is never serialized.
 */
record ContextKey(long token, Timeframe tf, long windowStart) {}
