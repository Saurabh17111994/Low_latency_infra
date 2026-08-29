package com.trading.common.schema;

import java.util.List;

/**
 * Shared, versioned contract for the candle-preview table
 * ({@code feature_candles_15s_preview}, low-latency candles Phase 1,
 * 2026-08-29).
 *
 * <p>A KV current-state table carrying the <b>in-progress</b> OHLCV of each
 * 15-second window per instrument, overwritten every 1s by
 * {@code CandlePreviewEmitFunction} (compute job). The final candle lives in
 * {@code feature_candles_15s} (unchanged); the preview row simply stops being
 * updated at window end and expires after the short TTL (60s).
 *
 * <p>Columns are a strict subset of {@link CandleTableSchema} plus an
 * {@code is_preview} marker (always TRUE here) — mirroring the DDL
 * {@code 30_feature_candles_15s_preview.sql} exactly. This table is NOT the
 * frozen v3 candle table; it is a separate, ephemeral, visibility-only
 * surface. Signal detection must never consume previews (partial candles
 * would corrupt its lookback) — the early-signal path (Phase 2) reads the
 * same {@code highs/closes} state but only suggests tentatives.
 */
public final class CandlePreviewTableSchema {

    private CandlePreviewTableSchema() {}

    /** DDL schema version of the preview row (v1 since Phase 1). */
    public static final String ROW_SCHEMA_VERSION = "1";

    /** KV current-state table (upsert on PK instrument_token, window_start). */
    public static final String TABLE = "feature_candles_15s_preview";

    /** The table's primary key columns, in PK order (KV upsert identity). */
    public static final List<String> PRIMARY_KEY_COLUMNS =
            List.of("instrument_token", "window_start");

    /** The table uses 16 buckets. */
    public static final int BUCKET_COUNT = 16;

    /** The table routes by instrument_token. */
    public static final String BUCKET_KEY = "instrument_token";

    /**
     * The 14 preview columns in DDL index order (v1). Physical writer layouts
     * ({@code CandlePreviewColumns}) MUST derive from this list so the sink,
     * the preflight metadata validator, and the DDL cannot drift apart.
     */
    public static final List<String> COLUMNS = List.of(
            "instrument_token",
            "exchange",
            "symbol",
            "window_start",
            "window_end",
            "open_paise",
            "high_paise",
            "low_paise",
            "close_paise",
            "volume",
            "tick_count",
            "is_preview",
            "output_ts",
            "schema_version");

    /**
     * Fluss {@code DataTypeRoot} name per column, DDL index order.
     * Plain strings so the shared module stays free of a compile-time Fluss
     * dependency; the compute-side validator compares live {@code TableInfo}
     * metadata against this list.
     */
    public static final List<String> COLUMN_TYPE_ROOTS = List.of(
            "BIGINT",   // instrument_token
            "STRING",   // exchange
            "STRING",   // symbol
            "BIGINT",   // window_start
            "BIGINT",   // window_end
            "BIGINT",   // open_paise
            "BIGINT",   // high_paise
            "BIGINT",   // low_paise
            "BIGINT",   // close_paise
            "BIGINT",   // volume
            "INTEGER",  // tick_count
            "BOOLEAN",  // is_preview
            "BIGINT",   // output_ts
            "STRING");  // schema_version

    /** DDL nullability intent per column (all NOT NULL in the DDL). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false, false, false, false,
            false, false, false, false, false);

    /** Column count — must equal {@code COLUMNS.size()}; mirrors the DDL. */
    public static final int FIELD_COUNT = COLUMNS.size();
}
