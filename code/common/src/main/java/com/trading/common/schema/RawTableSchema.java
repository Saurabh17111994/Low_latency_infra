package com.trading.common.schema;

import java.util.List;

/**
 * Shared, versioned contract for the raw tick table
 * ({@code code/01_platform/02_sql/ddl/02_raw_table_1.sql}, v3, daily partitions).
 *
 * <p>One table carries every accepted market tick as an immutable LOG row:
 * <ul>
 *   <li>{@link #TABLE} — LOG (no primary key), bucket key
 *       {@code instrument_token}, 16 buckets, 7-day TTL, Iceberg offload.</li>
 * </ul>
 *
 * <p>This class is the <b>single source of truth</b> for the 21-column
 * raw-table layout (configuration-driven plan SC1, 2026-08-29). The DDL
 * ({@code 02_raw_table_1.sql}), {@code DdlBootstrap}, and
 * {@code TypedFlussRowConverter} all derive from it — a schema change is one
 * edit, not three. v2 dropped the 8 quote/option columns the ingestion path
 * never populates (28 → 20); v3 then adds the {@code event_day} partition key.
 */
public final class RawTableSchema {

    private RawTableSchema() {}

    /** DDL schema version of the raw row (v3 since daily-partition migration). */
    public static final String ROW_SCHEMA_VERSION = "3";

    /** Immutable LOG table holding every accepted market tick. */
    public static final String TABLE = "raw_table_1";

    /** Table kind — LOG (no primary key). */
    public static final String TABLE_KIND = "LOG";

    /** Live log retention (DDL {@code table.log.ttl}). */
    public static final String LOG_TTL = "9d";

    /** Live partition retention (DDL {@code table.auto-partition.num-retention}). */
    public static final String PARTITION_RETENTION = "9";

    /** The table routes by instrument_token (16 buckets). */
    public static final String BUCKET_KEY = "instrument_token";

    /** The table uses 16 buckets (DDL bucket.num = '16'). */
    public static final int BUCKET_COUNT = 16;

    /** Partition key — must be COLUMNS.get(0) for the v3 daily-partition migration. */
    public static final String PARTITION_KEY = "event_day";

    /**
     * The 21 raw columns in DDL index order (v3, daily-partition migration). Physical
     * writer layouts ({@code TypedFlussRowConverter}) and bootstrap DDL
     * ({@code DdlBootstrap}) MUST derive from this list so they cannot drift
     * apart.
     */
    public static final List<String> COLUMNS = List.of(
            "event_day",              // partition key, yyyyMMdd IST (v3)
            "event_fingerprint",
            "fingerprint_version",
            "connection_id",
            "connection_epoch",
            "instrument_token",
            "exchange",
            "symbol",
            "event_time",
            "ingest_ts",
            "ack_ts",
            "tick_type",
            "last_price_paise",
            "last_qty",
            "raw_payload",
            "payload_hash",
            "decoder_version",
            "protocol_version",
            "validity_state",
            "validity_reason",
            "schema_version");

    /**
     * Fluss {@code DataTypeRoot} name per column, DDL index order. Mirrors
     * {@code 02_raw_table_1.sql} exactly: 7× BIGINT
     * (connection_epoch, instrument_token, event_time, ingest_ts, ack_ts,
     * last_price_paise, last_qty), 1× BYTES (raw_payload), 13× STRING (the
     * rest). Counted: 7 BIGINT + 1 BYTES + 13 STRING = 21.
     *
     * <p>Values are plain {@code DataTypeRoot.name()} strings so the shared
     * module stays free of a compile-time Fluss dependency; ingestion's
     * {@code DdlBootstrap} maps them onto Fluss {@code DataTypes}.
     */
    public static final List<String> COLUMN_TYPE_ROOTS = List.of(
            "STRING",   // event_day (partition key)
            "STRING",   // event_fingerprint
            "STRING",   // fingerprint_version
            "STRING",   // connection_id
            "BIGINT",   // connection_epoch
            "BIGINT",   // instrument_token
            "STRING",   // exchange
            "STRING",   // symbol
            "BIGINT",   // event_time
            "BIGINT",   // ingest_ts
            "BIGINT",   // ack_ts
            "STRING",   // tick_type
            "BIGINT",   // last_price_paise
            "BIGINT",   // last_qty
            "BYTES",    // raw_payload
            "STRING",   // payload_hash
            "STRING",   // decoder_version
            "STRING",   // protocol_version
            "STRING",   // validity_state
            "STRING",   // validity_reason
            "STRING");  // schema_version

    /** Column count — must equal {@code COLUMNS.size()}; mirrors the DDL. */
    public static final int FIELD_COUNT = COLUMNS.size();

    private static final java.util.Set<String> ALLOWED_TYPE_ROOTS =
            java.util.Set.of("STRING", "BIGINT", "BYTES");

    static {
        if (COLUMN_TYPE_ROOTS.size() != COLUMNS.size()) {
            throw new IllegalStateException("COLUMNS(" + COLUMNS.size()
                    + ") != COLUMN_TYPE_ROOTS(" + COLUMN_TYPE_ROOTS.size() + ")");
        }
        if (FIELD_COUNT != COLUMNS.size()) {
            throw new IllegalStateException("FIELD_COUNT drift");
        }
        if (!COLUMNS.contains(BUCKET_KEY)) {
            throw new IllegalStateException("BUCKET_KEY not in COLUMNS: " + BUCKET_KEY);
        }
        if (!PARTITION_KEY.equals(COLUMNS.get(0))) {
            throw new IllegalStateException("PARTITION_KEY must be COLUMNS.get(0)");
        }
        // Allowed vocabulary is intentionally narrow: DdlText.type() accepts a
        // wider forward-cover set (TIMESTAMP/DATE/DECIMAL/...), but today's
        // raw corpus is STRING/BIGINT/BYTES only — a wider root here means a
        // DDL/bootstrap mismatch, so fail at class-load.
        for (String root : COLUMN_TYPE_ROOTS) {
            if (!ALLOWED_TYPE_ROOTS.contains(root)) {
                throw new IllegalStateException("Unsupported type root: " + root);
            }
        }
    }
}
