package com.trading.common.schema;

import java.util.List;

/**
 * Shared, versioned contract for the feature-candle table
 * (docs/08_implementation/04-signal-job.md §Absorbed documents — retired candle era).
 *
 * <p>One table carries the 15-column v2 candle row
 * ({@code code/01_platform/02_sql/ddl/03_feature_candles_15s.sql},
 * R-012):
 * <ul>
 *   <li>{@link #TABLE} — KV current-state, one row per non-empty 15s
 *       window per instrument (PK {@code instrument_token, window_start}).
 *       A replay/restart re-emits the same key as an idempotent upsert
 *       instead of a duplicate LOG append (user requirement 2026-08-13:
 *       candle tables are KV-only, no LOG+KV twin).</li>
 * </ul>
 *
 * <p>{@code output_ts} is emit metadata, not row identity: an upsert for
 * the same key that agrees on the business fields is the same canonical
 * candle re-emitted (replay / restart), regardless of {@code output_ts}
 * ({@code CanonicalCandlePolicy}, compute module).
 */
public final class CandleTableSchema {

    private CandleTableSchema() {}

    /** DDL schema version of the candle row (v2 since R-012). */
    public static final String ROW_SCHEMA_VERSION = "2";

    /** KV current-state table (upsert on PK instrument_token, window_start). */
    public static final String TABLE = "feature_candles_15s";

    /**
     * The table's primary key columns, in PK order (KV upsert identity) —
     * mirrors the DDL 03 PRIMARY KEY clause. Bucket key
     * {@link #BUCKET_KEY} is a strict subset (Fluss requires pk ⊇ bucketKey).
     */
    public static final List<String> PRIMARY_KEY_COLUMNS =
            List.of("instrument_token", "window_start");

    /** The table uses 16 buckets. */
    public static final int BUCKET_COUNT = 16;

    /** The table routes by instrument_token. */
    public static final String BUCKET_KEY = "instrument_token";

    /**
     * The 15 candle columns in DDL index order (v2, R-012). The KV sink row
     * builder MUST derive from this list so the sink, the preflight metadata
     * validator, and the DDL cannot drift apart. (P4-337: this table is
     * KV-only — no LOG twin — so every "sink" below means the KV sink.)
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
            "algorithm_version",
            "configuration_version",
            "output_ts",
            "schema_version");

    /**
     * Fluss {@code DataTypeRoot} name per column, DDL index order (tracker 14
     * P1 — CANDLE-SCHEMA-002). Mirrors {@code 03_feature_candles_15s.sql}
     * exactly: 9× BIGINT
     * (instrument_token, window_start, window_end, open/high/low/close_paise,
     * volume, output_ts), 1× INTEGER (tick_count), 5× STRING (exchange,
     * symbol, algorithm_version, configuration_version, schema_version).
     *
     * <p>The values are plain {@code DataTypeRoot.name()} strings so the
     * shared module stays free of a compile-time Fluss dependency; the
     * compute-side validator ({@code CandleTableContractValidator}) compares
     * live {@code TableInfo} metadata against this list.
     */
    public static final List<String> COLUMN_TYPE_ROOTS = List.of(
            "BIGINT",
            "STRING",
            "STRING",
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "INTEGER",
            "STRING",
            "STRING",
            "BIGINT",
            "STRING");

    /**
     * DDL nullability intent per column (all NOT NULL in the DDL).
     *
     * <p><b>Enforcement caveat (tracker 14 P1):</b> Fluss does not carry DDL
     * NOT NULL into live metadata — a live KV table reports every column
     * nullable (verified against the dev cluster 2026-08-10). The validator
     * therefore does not enforce nullability; this list is the DDL intent
     * used for the evidence report.
     */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false, false, false, false,
            false, false, false, false, false, false);

    /**
     * Canonical algorithm/configuration pair (tracker 14 P2 —
     * CANDLE-CANONICAL-001). A candle row passes validation only when BOTH
     * version columns equal these values exactly ({@code CanonicalCandlePolicy});
     * any other combination is non-canonical and must be excluded — emitted
     * candle KV rows carry the canonical pair as part of their row identity
     * for replay evidence. The pair is pinned here as the single source of
     * truth so {@code SignalJobConfig} (startup gate) and the validation
     * filter cannot drift. Changing the pair is a governed change, not a
     * tuning knob. (P4-337: stale "LOG rows" wording from the retired
     * LOG+KV twin era fixed — the table is KV-only.)
     */
    public static final String CANONICAL_ALGORITHM_VERSION = "candle-15s-v1";
    public static final String CANONICAL_CONFIGURATION_VERSION = "1.0.0";

    /** Column count — must equal {@code COLUMNS.size()}; mirrors the DDL. */
    public static final int FIELD_COUNT = 15;

    static {
        // P4-248: literal FIELD_COUNT so the size guard can catch drift
        // (deriving from COLUMNS would trivially pass). P4-247: Fluss needs
        // BUCKET_KEY in PRIMARY_KEY_COLUMNS in COLUMNS with BUCKET_COUNT > 0.
        if (COLUMNS.size() != FIELD_COUNT
                || COLUMN_TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT) {
            throw new ExceptionInInitializerError(
                    "CandleTableSchema drift: COLUMNS=" + COLUMNS.size()
                            + " TYPE_ROOTS=" + COLUMN_TYPE_ROOTS.size()
                            + " NULLABLE=" + COLUMN_NULLABLE_IN_DDL.size()
                            + " expected " + FIELD_COUNT);
        }
        if (!COLUMNS.containsAll(PRIMARY_KEY_COLUMNS)
                || !PRIMARY_KEY_COLUMNS.contains(BUCKET_KEY)
                || !COLUMNS.contains(BUCKET_KEY)
                || BUCKET_COUNT <= 0) {
            throw new ExceptionInInitializerError(
                    "Invalid PK/bucket contract: pk=" + PRIMARY_KEY_COLUMNS
                            + " bucketKey=" + BUCKET_KEY + " buckets=" + BUCKET_COUNT);
        }
    }
}
