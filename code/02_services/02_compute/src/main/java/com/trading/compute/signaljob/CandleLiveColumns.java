package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the live/forming candle row — the in-memory
 * stream the strategy host reads (fast feed {@code candle-live-tick} or the
 * 1 s {@code candle-live} mirror; the live path writes nothing to Fluss,
 * DEC-059).
 *
 * <p>The first {@link #DDL_FIELD_COUNT} columns mirror the candle prefix of
 * the merged table {@code candle_features}
 * ({@code code/01_platform/02_sql/ddl/35_candle_features.sql}), pinned by
 * {@code CandleLiveColumnsAgreementTest}. Index {@link #INGEST_TS} is a
 * trailing observability probe: the ingestion accept wall-clock
 * ({@code raw.ingest_ts}) of the tick that formed the row, carried so the
 * strategy host can report {@code compute.latency.ingest_to_strategy}
 * (ingestion accept -> host read). The probe is never written to any table
 * and is not part of the pinned DDL prefix.
 *
 * <p>The trailing {@link #MARKET_SECTION_START}..{@link #FIELD_COUNT}-1
 * section is the strategy market snapshot (2026-10-01 native design): the 42
 * latest-known raw market values — 12 day/flow stats and the 30-column depth
 * ladder (5 levels × price/qty/order-count × 2 sides) — plus two change
 * clocks ({@link #MKT_STATS_CHANGED_AT}, {@link #MKT_DEPTH_CHANGED_AT}),
 * captured by the aggregator from every accepted tick (trade or quote).
 * Null means "this feed did not provide the field" (never zero), and a value
 * is latest-known, not necessarily from the tick that formed the row.
 *
 * <p><b>Canonical-row transport (2026-10-01).</b> The section is written
 * only on the {@code FIFTEEN_S} forming row — the first row emitted for every
 * accepted tick — so one row per tick carries it instead of six. The host
 * decodes it on that row only and refreshes the per-instrument
 * {@link MarketSnapshot}; strategies read the shared {@link MarketView} on
 * every callback. Transport-only: never persisted, absent from every DDL.
 *
 * <p>Phase 0 multi-TF aggregator contract: tf discriminator values are
 * {@code FIFTEEN_S, THIRTY_S, ONE_M, THREE_M, FIVE_M, FIFTEEN_M}.
 */
public final class CandleLiveColumns {

    private CandleLiveColumns() {}

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int EXCHANGE = 1;
    public static final int SYMBOL = 2;
    public static final int TF = 3;
    public static final int WINDOW_START = 4;
    public static final int WINDOW_END = 5;
    public static final int OPEN_PAISE = 6;
    public static final int HIGH_PAISE = 7;
    public static final int LOW_PAISE = 8;
    public static final int CLOSE_PAISE = 9;
    public static final int VOLUME = 10;
    public static final int TICK_COUNT = 11;
    public static final int LAST_EVENT_TIME = 12;
    public static final int LAST_EVENT_FINGERPRINT = 13;
    public static final int SCHEMA_VERSION = 14;

    /**
     * Trailing observability probe (never stored): ingestion accept wall-clock
     * of the close-setting tick — the host's {@code ingest_to_strategy} KPI anchor.
     */
    public static final int INGEST_TS = 15;

    // ── market snapshot section (transport-only; see class javadoc) ──────
    /** First market-snapshot index — immediately after the ingest probe. */
    public static final int MARKET_SECTION_START = 16;

    // stats (12)
    public static final int MKT_TOTAL_BUY_QTY = 16;
    public static final int MKT_TOTAL_SELL_QTY = 17;
    public static final int MKT_DAY_OPEN_PAISE = 18;
    public static final int MKT_DAY_HIGH_PAISE = 19;
    public static final int MKT_DAY_LOW_PAISE = 20;
    public static final int MKT_PREV_CLOSE_PAISE = 21;
    public static final int MKT_VWAP_PAISE = 22;
    public static final int MKT_OPEN_INTEREST = 23;
    public static final int MKT_OI_DAY_HIGH = 24;
    public static final int MKT_OI_DAY_LOW = 25;
    public static final int MKT_LOWER_LIMIT_PAISE = 26;
    public static final int MKT_UPPER_LIMIT_PAISE = 27;

    // depth ladder (30): bid px/qty/ord 1..5, then ask px/qty/ord 1..5
    public static final int MKT_BID_PX_1 = 28;
    public static final int MKT_BID_PX_2 = 29;
    public static final int MKT_BID_PX_3 = 30;
    public static final int MKT_BID_PX_4 = 31;
    public static final int MKT_BID_PX_5 = 32;
    public static final int MKT_BID_QTY_1 = 33;
    public static final int MKT_BID_QTY_2 = 34;
    public static final int MKT_BID_QTY_3 = 35;
    public static final int MKT_BID_QTY_4 = 36;
    public static final int MKT_BID_QTY_5 = 37;
    public static final int MKT_BID_ORD_1 = 38;
    public static final int MKT_BID_ORD_2 = 39;
    public static final int MKT_BID_ORD_3 = 40;
    public static final int MKT_BID_ORD_4 = 41;
    public static final int MKT_BID_ORD_5 = 42;
    public static final int MKT_ASK_PX_1 = 43;
    public static final int MKT_ASK_PX_2 = 44;
    public static final int MKT_ASK_PX_3 = 45;
    public static final int MKT_ASK_PX_4 = 46;
    public static final int MKT_ASK_PX_5 = 47;
    public static final int MKT_ASK_QTY_1 = 48;
    public static final int MKT_ASK_QTY_2 = 49;
    public static final int MKT_ASK_QTY_3 = 50;
    public static final int MKT_ASK_QTY_4 = 51;
    public static final int MKT_ASK_QTY_5 = 52;
    public static final int MKT_ASK_ORD_1 = 53;
    public static final int MKT_ASK_ORD_2 = 54;
    public static final int MKT_ASK_ORD_3 = 55;
    public static final int MKT_ASK_ORD_4 = 56;
    public static final int MKT_ASK_ORD_5 = 57;

    // change clocks (2)
    public static final int MKT_STATS_CHANGED_AT = 58;
    public static final int MKT_DEPTH_CHANGED_AT = 59;

    /** Number of market-snapshot columns (42 values + 2 clocks). */
    public static final int MARKET_FIELD_COUNT = 44;

    public static final int FIELD_COUNT = 60;

    /** Leading columns pinned to the merged-table DDL prefix; probes follow. */
    public static final int DDL_FIELD_COUNT = 15;

    public static final String SCHEMA_VERSION_V1 = "1";

    /** Fluss {@code DataTypeRoot} name per column; the market section is BIGINT NULL by construction. */
    public static final List<String> TYPE_ROOTS = List.of(
            "BIGINT", "STRING", "STRING", "STRING",
            "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "INTEGER", "BIGINT", "STRING", "STRING",
            "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT");

    /**
     * DDL nullability per column (35 DDL prefix v1): exchange, symbol,
     * last_event_fingerprint nullable. Trailing probes/market columns are
     * nullable by construction, not by DDL.
     */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, true, true, false,
            false, false,
            false, false, false, false,
            false, false, false, true, false,
            false,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true,
            true, true, true, true);

    /** DDL column names in index order (diagnostics + agreement pin). */
    private static final String[] NAMES = {
            "instrument_token", "exchange", "symbol", "tf",
            "window_start", "window_end",
            "open_paise", "high_paise", "low_paise", "close_paise",
            "volume", "tick_count",
            "last_event_time", "last_event_fingerprint", "schema_version",
            "ingest_ts",
            "mkt_total_buy_qty", "mkt_total_sell_qty",
            "mkt_day_open_paise", "mkt_day_high_paise", "mkt_day_low_paise",
            "mkt_prev_close_paise", "mkt_vwap_paise",
            "mkt_open_interest", "mkt_oi_day_high", "mkt_oi_day_low",
            "mkt_lower_limit_paise", "mkt_upper_limit_paise",
            "mkt_bid_px_1", "mkt_bid_px_2", "mkt_bid_px_3", "mkt_bid_px_4", "mkt_bid_px_5",
            "mkt_bid_qty_1", "mkt_bid_qty_2", "mkt_bid_qty_3", "mkt_bid_qty_4", "mkt_bid_qty_5",
            "mkt_bid_ord_1", "mkt_bid_ord_2", "mkt_bid_ord_3", "mkt_bid_ord_4", "mkt_bid_ord_5",
            "mkt_ask_px_1", "mkt_ask_px_2", "mkt_ask_px_3", "mkt_ask_px_4", "mkt_ask_px_5",
            "mkt_ask_qty_1", "mkt_ask_qty_2", "mkt_ask_qty_3", "mkt_ask_qty_4", "mkt_ask_qty_5",
            "mkt_ask_ord_1", "mkt_ask_ord_2", "mkt_ask_ord_3", "mkt_ask_ord_4", "mkt_ask_ord_5",
            "mkt_stats_changed_at", "mkt_depth_changed_at"
    };

    /** Immutable DDL column names in index order (diagnostics + agreement pin). */
    public static final List<String> COLUMN_NAMES = List.of(
            "instrument_token", "exchange", "symbol", "tf",
            "window_start", "window_end",
            "open_paise", "high_paise", "low_paise", "close_paise",
            "volume", "tick_count",
            "last_event_time", "last_event_fingerprint", "schema_version",
            "ingest_ts",
            "mkt_total_buy_qty", "mkt_total_sell_qty",
            "mkt_day_open_paise", "mkt_day_high_paise", "mkt_day_low_paise",
            "mkt_prev_close_paise", "mkt_vwap_paise",
            "mkt_open_interest", "mkt_oi_day_high", "mkt_oi_day_low",
            "mkt_lower_limit_paise", "mkt_upper_limit_paise",
            "mkt_bid_px_1", "mkt_bid_px_2", "mkt_bid_px_3", "mkt_bid_px_4", "mkt_bid_px_5",
            "mkt_bid_qty_1", "mkt_bid_qty_2", "mkt_bid_qty_3", "mkt_bid_qty_4", "mkt_bid_qty_5",
            "mkt_bid_ord_1", "mkt_bid_ord_2", "mkt_bid_ord_3", "mkt_bid_ord_4", "mkt_bid_ord_5",
            "mkt_ask_px_1", "mkt_ask_px_2", "mkt_ask_px_3", "mkt_ask_px_4", "mkt_ask_px_5",
            "mkt_ask_qty_1", "mkt_ask_qty_2", "mkt_ask_qty_3", "mkt_ask_qty_4", "mkt_ask_qty_5",
            "mkt_ask_ord_1", "mkt_ask_ord_2", "mkt_ask_ord_3", "mkt_ask_ord_4", "mkt_ask_ord_5",
            "mkt_stats_changed_at", "mkt_depth_changed_at");

    /** Stream type info for emitted candle_live rows (v1 DDL order). */
    public static final InternalTypeInfo<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(false),                       // instrument_token (NOT NULL)
                new VarCharType(VarCharType.MAX_LENGTH),     // exchange (nullable)
                new VarCharType(VarCharType.MAX_LENGTH),     // symbol (nullable)
                new VarCharType(false, VarCharType.MAX_LENGTH), // tf (NOT NULL)
                new BigIntType(false),                       // window_start (NOT NULL)
                new BigIntType(false),                       // window_end (NOT NULL)
                new BigIntType(false),                       // open_paise (NOT NULL)
                new BigIntType(false),                       // high_paise (NOT NULL)
                new BigIntType(false),                       // low_paise (NOT NULL)
                new BigIntType(false),                       // close_paise (NOT NULL)
                new BigIntType(false),                       // volume (NOT NULL)
                new IntType(false),                          // tick_count (NOT NULL)
                new BigIntType(false),                       // last_event_time (NOT NULL)
                new VarCharType(VarCharType.MAX_LENGTH),     // last_event_fingerprint (nullable)
                new VarCharType(false, VarCharType.MAX_LENGTH), // schema_version (NOT NULL)
                new BigIntType(false),                       // ingest_ts probe (NOT NULL set on emit)
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true),
                new BigIntType(true), new BigIntType(true)
            },
            NAMES.clone());

    static {
        if (COLUMN_NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || ROW_TYPE_INFO.toRowSize() != FIELD_COUNT) {
            throw new IllegalStateException(
                    "CandleLiveColumns drift: COLUMN_NAMES/TYPE_ROOTS/COLUMN_NULLABLE_IN_DDL/ROW_TYPE_INFO"
                            + " must all have FIELD_COUNT=" + FIELD_COUNT);
        }
        if (DDL_FIELD_COUNT <= 0 || DDL_FIELD_COUNT > FIELD_COUNT) {
            throw new IllegalStateException(
                    "CandleLiveColumns drift: DDL_FIELD_COUNT must be in (0, FIELD_COUNT]");
        }
        checkIndex(INSTRUMENT_TOKEN, "instrument_token");
        checkIndex(EXCHANGE, "exchange");
        checkIndex(SYMBOL, "symbol");
        checkIndex(TF, "tf");
        checkIndex(WINDOW_START, "window_start");
        checkIndex(WINDOW_END, "window_end");
        checkIndex(OPEN_PAISE, "open_paise");
        checkIndex(HIGH_PAISE, "high_paise");
        checkIndex(LOW_PAISE, "low_paise");
        checkIndex(CLOSE_PAISE, "close_paise");
        checkIndex(VOLUME, "volume");
        checkIndex(TICK_COUNT, "tick_count");
        checkIndex(LAST_EVENT_TIME, "last_event_time");
        checkIndex(LAST_EVENT_FINGERPRINT, "last_event_fingerprint");
        checkIndex(SCHEMA_VERSION, "schema_version");
        checkIndex(INGEST_TS, "ingest_ts");
        checkIndex(MKT_TOTAL_BUY_QTY, "mkt_total_buy_qty");
        checkIndex(MKT_TOTAL_SELL_QTY, "mkt_total_sell_qty");
        checkIndex(MKT_DAY_OPEN_PAISE, "mkt_day_open_paise");
        checkIndex(MKT_DAY_HIGH_PAISE, "mkt_day_high_paise");
        checkIndex(MKT_DAY_LOW_PAISE, "mkt_day_low_paise");
        checkIndex(MKT_PREV_CLOSE_PAISE, "mkt_prev_close_paise");
        checkIndex(MKT_VWAP_PAISE, "mkt_vwap_paise");
        checkIndex(MKT_OPEN_INTEREST, "mkt_open_interest");
        checkIndex(MKT_OI_DAY_HIGH, "mkt_oi_day_high");
        checkIndex(MKT_OI_DAY_LOW, "mkt_oi_day_low");
        checkIndex(MKT_LOWER_LIMIT_PAISE, "mkt_lower_limit_paise");
        checkIndex(MKT_UPPER_LIMIT_PAISE, "mkt_upper_limit_paise");
        checkIndex(MKT_BID_PX_1, "mkt_bid_px_1");
        checkIndex(MKT_BID_PX_2, "mkt_bid_px_2");
        checkIndex(MKT_BID_PX_3, "mkt_bid_px_3");
        checkIndex(MKT_BID_PX_4, "mkt_bid_px_4");
        checkIndex(MKT_BID_PX_5, "mkt_bid_px_5");
        checkIndex(MKT_BID_QTY_1, "mkt_bid_qty_1");
        checkIndex(MKT_BID_QTY_2, "mkt_bid_qty_2");
        checkIndex(MKT_BID_QTY_3, "mkt_bid_qty_3");
        checkIndex(MKT_BID_QTY_4, "mkt_bid_qty_4");
        checkIndex(MKT_BID_QTY_5, "mkt_bid_qty_5");
        checkIndex(MKT_BID_ORD_1, "mkt_bid_ord_1");
        checkIndex(MKT_BID_ORD_2, "mkt_bid_ord_2");
        checkIndex(MKT_BID_ORD_3, "mkt_bid_ord_3");
        checkIndex(MKT_BID_ORD_4, "mkt_bid_ord_4");
        checkIndex(MKT_BID_ORD_5, "mkt_bid_ord_5");
        checkIndex(MKT_ASK_PX_1, "mkt_ask_px_1");
        checkIndex(MKT_ASK_PX_2, "mkt_ask_px_2");
        checkIndex(MKT_ASK_PX_3, "mkt_ask_px_3");
        checkIndex(MKT_ASK_PX_4, "mkt_ask_px_4");
        checkIndex(MKT_ASK_PX_5, "mkt_ask_px_5");
        checkIndex(MKT_ASK_QTY_1, "mkt_ask_qty_1");
        checkIndex(MKT_ASK_QTY_2, "mkt_ask_qty_2");
        checkIndex(MKT_ASK_QTY_3, "mkt_ask_qty_3");
        checkIndex(MKT_ASK_QTY_4, "mkt_ask_qty_4");
        checkIndex(MKT_ASK_QTY_5, "mkt_ask_qty_5");
        checkIndex(MKT_ASK_ORD_1, "mkt_ask_ord_1");
        checkIndex(MKT_ASK_ORD_2, "mkt_ask_ord_2");
        checkIndex(MKT_ASK_ORD_3, "mkt_ask_ord_3");
        checkIndex(MKT_ASK_ORD_4, "mkt_ask_ord_4");
        checkIndex(MKT_ASK_ORD_5, "mkt_ask_ord_5");
        checkIndex(MKT_STATS_CHANGED_AT, "mkt_stats_changed_at");
        checkIndex(MKT_DEPTH_CHANGED_AT, "mkt_depth_changed_at");
        if (MARKET_SECTION_START != INGEST_TS + 1
                || MARKET_FIELD_COUNT != FIELD_COUNT - MARKET_SECTION_START) {
            throw new IllegalStateException(
                    "CandleLiveColumns drift: market section must start at "
                            + (INGEST_TS + 1) + " and span the trailing columns");
        }
    }

    private static void checkIndex(int index, String expected) {
        if (!NAMES[index].equals(expected)) {
            throw new IllegalStateException(
                    "CandleLiveColumns drift: index " + index + " must be '"
                            + expected + "', got '" + NAMES[index] + "'");
        }
    }

    /** Defensive copy of the DDL column names (callers must not retain the array). */
    public static String[] namesCopy() {
        return NAMES.clone();
    }
}
