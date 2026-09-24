package com.trading.common.schema;

import java.util.List;

/**
 * Shared, versioned contract for the raw tick table
 * ({@code code/01_platform/02_sql/ddl/02_raw_table_1.sql}, v4, daily partitions).
 *
 * <p>One table carries every accepted market tick as an immutable LOG row:
 * <ul>
 *   <li>{@link #TABLE} — LOG (no primary key), bucket key
 *       {@code instrument_token}, 16 buckets, 7-day TTL, Iceberg offload.</li>
 * </ul>
 *
 * <p>This class is the <b>single source of truth</b> for the 72-column
 * raw-table layout (configuration-driven plan SC1, 2026-08-29). The DDL
 * ({@code 02_raw_table_1.sql}), {@code DdlBootstrap}, and
 * {@code TypedFlussRowConverter} all derive from it — a schema change is one
 * edit, not three. v2 dropped the 8 quote/option columns the ingestion path
 * never populates (28 → 20); v3 added the {@code event_day} partition key; v4
 * stores every full-mode field the bridge already carried and dropped (the 21–71
 * block), so one shared schema serves BOTH feeds — standard (free-plan) and
 * HFT — with the 8 feed-specific fields NULL on the feed that lacks them.
 */
public final class RawTableSchema {

    private RawTableSchema() {}

    /** DDL schema version of the raw row (v4 since full-mode field capture). */
    public static final String ROW_SCHEMA_VERSION = "4";

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
     * The 72 raw columns in DDL index order (v4, full-mode field capture). Physical
     * writer layouts ({@code TypedFlussRowConverter}) and bootstrap DDL
     * ({@code DdlBootstrap}) MUST derive from this list so they cannot drift
     * apart.
     */
    public static final List<String> COLUMNS = List.of(
            "event_day",
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
            "schema_version",
            // --- v4: full-mode field capture (appended AFTER schema_version; 0-20 never move) ---
            "open_paise",
            "high_paise",
            "low_paise",
            "close_paise",
            "vwap_paise",
            "volume",
            "volume_delta",
            "total_buy_qty",
            "total_sell_qty",
            "open_interest",
            "bid_px_1",
            "bid_px_2",
            "bid_px_3",
            "bid_px_4",
            "bid_px_5",
            "bid_qty_1",
            "bid_qty_2",
            "bid_qty_3",
            "bid_qty_4",
            "bid_qty_5",
            "bid_ord_1",
            "bid_ord_2",
            "bid_ord_3",
            "bid_ord_4",
            "bid_ord_5",
            "ask_px_1",
            "ask_px_2",
            "ask_px_3",
            "ask_px_4",
            "ask_px_5",
            "ask_qty_1",
            "ask_qty_2",
            "ask_qty_3",
            "ask_qty_4",
            "ask_qty_5",
            "ask_ord_1",
            "ask_ord_2",
            "ask_ord_3",
            "ask_ord_4",
            "ask_ord_5",
            "change_flag",
            "oi_day_high",
            "oi_day_low",
            "lower_limit_paise",
            "upper_limit_paise",
            "imbalance_qty",
            "indicative_close_paise",
            "ref_price_paise",
            "last_traded_time",
            "atv",
            "btv");

    /**
     * Fluss {@code DataTypeRoot} name per column, DDL index order. Mirrors
     * {@code 02_raw_table_1.sql} exactly: 58× BIGINT (the 7 legacy BIGINTs plus
     * all 51 v4 columns), 1× BYTES (raw_payload), 13× STRING (the rest).
     * Counted: 58 BIGINT + 1 BYTES + 13 STRING = 72.
     *
     * <p>v4 numerics are all BIGINT deliberately: {@code ALLOWED_TYPE_ROOTS} below
     * and {@code DdlBootstrap.toType} admit only STRING/BIGINT/BYTES, so a narrower
     * int would need a new root in three places.
     *
     * <p>Values are plain {@code DataTypeRoot.name()} strings so the shared
     * module stays free of a compile-time Fluss dependency; ingestion's
     * {@code DdlBootstrap} maps them onto Fluss {@code DataTypes}.
     */
    public static final List<String> COLUMN_TYPE_ROOTS = List.of(
            "STRING",   // event_day
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
            "BYTES",   // raw_payload
            "STRING",   // payload_hash
            "STRING",   // decoder_version
            "STRING",   // protocol_version
            "STRING",   // validity_state
            "STRING",   // validity_reason
            "STRING",   // schema_version
            // --- v4: every added column is BIGINT NULL ---
            "BIGINT",   // open_paise
            "BIGINT",   // high_paise
            "BIGINT",   // low_paise
            "BIGINT",   // close_paise
            "BIGINT",   // vwap_paise
            "BIGINT",   // volume
            "BIGINT",   // volume_delta
            "BIGINT",   // total_buy_qty
            "BIGINT",   // total_sell_qty
            "BIGINT",   // open_interest
            "BIGINT",   // bid_px_1
            "BIGINT",   // bid_px_2
            "BIGINT",   // bid_px_3
            "BIGINT",   // bid_px_4
            "BIGINT",   // bid_px_5
            "BIGINT",   // bid_qty_1
            "BIGINT",   // bid_qty_2
            "BIGINT",   // bid_qty_3
            "BIGINT",   // bid_qty_4
            "BIGINT",   // bid_qty_5
            "BIGINT",   // bid_ord_1
            "BIGINT",   // bid_ord_2
            "BIGINT",   // bid_ord_3
            "BIGINT",   // bid_ord_4
            "BIGINT",   // bid_ord_5
            "BIGINT",   // ask_px_1
            "BIGINT",   // ask_px_2
            "BIGINT",   // ask_px_3
            "BIGINT",   // ask_px_4
            "BIGINT",   // ask_px_5
            "BIGINT",   // ask_qty_1
            "BIGINT",   // ask_qty_2
            "BIGINT",   // ask_qty_3
            "BIGINT",   // ask_qty_4
            "BIGINT",   // ask_qty_5
            "BIGINT",   // ask_ord_1
            "BIGINT",   // ask_ord_2
            "BIGINT",   // ask_ord_3
            "BIGINT",   // ask_ord_4
            "BIGINT",   // ask_ord_5
            "BIGINT",   // change_flag
            "BIGINT",   // oi_day_high
            "BIGINT",   // oi_day_low
            "BIGINT",   // lower_limit_paise
            "BIGINT",   // upper_limit_paise
            "BIGINT",   // imbalance_qty
            "BIGINT",   // indicative_close_paise
            "BIGINT",   // ref_price_paise
            "BIGINT",   // last_traded_time
            "BIGINT",   // atv
            "BIGINT");   // btv

    /**
     * Indexes below this are the frozen layout (v3): an upgrade appends after them
     * and never renumbers, so a live table's first columns must equal
     * {@code COLUMNS.subList(0, FROZEN_PREFIX_COLUMNS)}. The ALTER applier
     * ({@code 04_scripts/fluss-repair/RawTableAlter.java}) refuses to widen a table
     * that does not still match this prefix — a shortened or foreign table would
     * otherwise be "upgraded" into a different schema.
     */
    public static final int FROZEN_PREFIX_COLUMNS = 21;

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
