package com.trading.compute.signaljob;

import com.trading.common.schema.RawTableSchema;
import java.util.Arrays;
import java.util.List;

/**
 * Physical column layout of {@code raw_table_1} as consumed by the Signal job.
 *
 * <p>Must mirror {@code code/01_platform/02_sql/ddl/02_raw_table_1.sql} v4 (72
 * columns) exactly. The Fluss source's {@code RowDataDeserializationSchema}
 * produces rows in table column order, so field indexes are the DDL positions.
 * v2 (R-054/R-231) removed the quote (bid/ask) and option columns — do not
 * v2 indexes are NOT reused and must not be, but v4 brings quote data back as the
 * flat bid_/ask_ block at indexes 31-60.
 *
 * <p>Only the columns this job reads get a named constant. Order for the rest is
 * still enforced: {@link #validateSchemaContract} compares all 72 names against
 * {@link RawTableSchema#COLUMNS} at class load, so a shift cannot go unnoticed.
 * Add a constant when you add a reader, not in advance.
 */
public final class RawTableColumns {

    private RawTableColumns() {}

    public static final int EVENT_DAY = 0;
    public static final int EVENT_FINGERPRINT = 1;
    public static final int FINGERPRINT_VERSION = 2;
    public static final int CONNECTION_ID = 3;
    public static final int CONNECTION_EPOCH = 4;
    public static final int INSTRUMENT_TOKEN = 5;
    public static final int EXCHANGE = 6;
    public static final int SYMBOL = 7;
    public static final int EVENT_TIME = 8;
    public static final int INGEST_TS = 9;
    public static final int ACK_TS = 10;
    public static final int TICK_TYPE = 11;
    public static final int LAST_PRICE_PAISE = 12;
    public static final int LAST_QTY = 13;
    public static final int RAW_PAYLOAD = 14;
    public static final int PAYLOAD_HASH = 15;
    public static final int DECODER_VERSION = 16;
    public static final int PROTOCOL_VERSION = 17;
    public static final int VALIDITY_STATE = 18;
    public static final int VALIDITY_REASON = 19;
    public static final int SCHEMA_VERSION = 20;

    // --- v4: full-mode field capture (indexes 21-71, all BIGINT NULL) ---
    /**
     * Traded qty since the previous tick for this token: 0 = no trade, NULL = unknown
     * baseline. Candle volume sums THIS; {@link #LAST_QTY} is a last-trade size, not an
     * increment, so summing it under-counts batched trades.
     */
    public static final int VOLUME_DELTA = 27;

    // --- strategy market snapshot (2026-10-01): the 16 raw extras the signal
    // job carries to strategies on every forming row. Add a constant when a
    // reader lands; MultiTimeframeAggregateFunction#updateMarketSnapshot is
    // the only reader today. ---
    /** Day open. */
    public static final int OPEN_PAISE = 21;
    /** Day high. */
    public static final int HIGH_PAISE = 22;
    /** Day low. */
    public static final int LOW_PAISE = 23;
    /** PREVIOUS day's close — not today's. */
    public static final int CLOSE_PAISE = 24;
    /** Day VWAP. */
    public static final int VWAP_PAISE = 25;
    /** Cumulative day buy quantity (TBQ). */
    public static final int TOTAL_BUY_QTY = 28;
    /** Cumulative day sell quantity (TSQ). */
    public static final int TOTAL_SELL_QTY = 29;
    /** Open interest. */
    public static final int OPEN_INTEREST = 30;
    /** Level-1 best bid price. */
    public static final int BID_PX_1 = 31;
    /** Level-1 best bid quantity. */
    public static final int BID_QTY_1 = 36;
    /** Level-1 best ask price. */
    public static final int ASK_PX_1 = 46;
    /** Level-1 best ask quantity. */
    public static final int ASK_QTY_1 = 51;
    /** OI day high (standard token stream only). */
    public static final int OI_DAY_HIGH = 62;
    /** OI day low (standard token stream only). */
    public static final int OI_DAY_LOW = 63;
    /** Lower circuit limit. */
    public static final int LOWER_LIMIT_PAISE = 64;
    /** Upper circuit limit. */
    public static final int UPPER_LIMIT_PAISE = 65;

    public static final int FIELD_COUNT = 72;

    /** DDL column names in index order (diagnostics). Do not expose mutably. */
    private static final String[] NAMES = {
         "event_day", "event_fingerprint", "fingerprint_version", "connection_id",
         "connection_epoch", "instrument_token", "exchange", "symbol", "event_time",
         "ingest_ts", "ack_ts", "tick_type", "last_price_paise", "last_qty", "raw_payload",
         "payload_hash", "decoder_version", "protocol_version", "validity_state",
         "validity_reason", "schema_version", "open_paise", "high_paise", "low_paise",
         "close_paise", "vwap_paise", "volume", "volume_delta", "total_buy_qty",
         "total_sell_qty", "open_interest", "bid_px_1", "bid_px_2", "bid_px_3", "bid_px_4",
         "bid_px_5", "bid_qty_1", "bid_qty_2", "bid_qty_3", "bid_qty_4", "bid_qty_5",
         "bid_ord_1", "bid_ord_2", "bid_ord_3", "bid_ord_4", "bid_ord_5", "ask_px_1",
         "ask_px_2", "ask_px_3", "ask_px_4", "ask_px_5", "ask_qty_1", "ask_qty_2", "ask_qty_3",
         "ask_qty_4", "ask_qty_5", "ask_ord_1", "ask_ord_2", "ask_ord_3", "ask_ord_4",
         "ask_ord_5", "change_flag", "oi_day_high", "oi_day_low", "lower_limit_paise",
         "upper_limit_paise", "imbalance_qty", "indicative_close_paise", "ref_price_paise",
         "last_traded_time", "atv", "btv"
    };

    public static final List<String> COLUMN_NAMES = List.copyOf(Arrays.asList(NAMES));

    static {
        validateSchemaContract(FIELD_COUNT, Arrays.asList(NAMES));
        check(EVENT_DAY == RawTableSchema.COLUMNS.indexOf("event_day"), "EVENT_DAY");
        check(EVENT_FINGERPRINT == RawTableSchema.COLUMNS.indexOf("event_fingerprint"), "EVENT_FINGERPRINT");
        check(FINGERPRINT_VERSION == RawTableSchema.COLUMNS.indexOf("fingerprint_version"), "FINGERPRINT_VERSION");
        check(CONNECTION_ID == RawTableSchema.COLUMNS.indexOf("connection_id"), "CONNECTION_ID");
        check(CONNECTION_EPOCH == RawTableSchema.COLUMNS.indexOf("connection_epoch"), "CONNECTION_EPOCH");
        check(INSTRUMENT_TOKEN == RawTableSchema.COLUMNS.indexOf("instrument_token"), "INSTRUMENT_TOKEN");
        check(EXCHANGE == RawTableSchema.COLUMNS.indexOf("exchange"), "EXCHANGE");
        check(SYMBOL == RawTableSchema.COLUMNS.indexOf("symbol"), "SYMBOL");
        check(EVENT_TIME == RawTableSchema.COLUMNS.indexOf("event_time"), "EVENT_TIME");
        check(INGEST_TS == RawTableSchema.COLUMNS.indexOf("ingest_ts"), "INGEST_TS");
        check(ACK_TS == RawTableSchema.COLUMNS.indexOf("ack_ts"), "ACK_TS");
        check(TICK_TYPE == RawTableSchema.COLUMNS.indexOf("tick_type"), "TICK_TYPE");
        check(LAST_PRICE_PAISE == RawTableSchema.COLUMNS.indexOf("last_price_paise"), "LAST_PRICE_PAISE");
        check(LAST_QTY == RawTableSchema.COLUMNS.indexOf("last_qty"), "LAST_QTY");
        check(RAW_PAYLOAD == RawTableSchema.COLUMNS.indexOf("raw_payload"), "RAW_PAYLOAD");
        check(PAYLOAD_HASH == RawTableSchema.COLUMNS.indexOf("payload_hash"), "PAYLOAD_HASH");
        check(DECODER_VERSION == RawTableSchema.COLUMNS.indexOf("decoder_version"), "DECODER_VERSION");
        check(PROTOCOL_VERSION == RawTableSchema.COLUMNS.indexOf("protocol_version"), "PROTOCOL_VERSION");
        check(VALIDITY_STATE == RawTableSchema.COLUMNS.indexOf("validity_state"), "VALIDITY_STATE");
        check(VALIDITY_REASON == RawTableSchema.COLUMNS.indexOf("validity_reason"), "VALIDITY_REASON");
        check(SCHEMA_VERSION == RawTableSchema.COLUMNS.indexOf("schema_version"), "SCHEMA_VERSION");
        check(VOLUME_DELTA == RawTableSchema.COLUMNS.indexOf("volume_delta"), "VOLUME_DELTA");
        check(OPEN_PAISE == RawTableSchema.COLUMNS.indexOf("open_paise"), "OPEN_PAISE");
        check(HIGH_PAISE == RawTableSchema.COLUMNS.indexOf("high_paise"), "HIGH_PAISE");
        check(LOW_PAISE == RawTableSchema.COLUMNS.indexOf("low_paise"), "LOW_PAISE");
        check(CLOSE_PAISE == RawTableSchema.COLUMNS.indexOf("close_paise"), "CLOSE_PAISE");
        check(VWAP_PAISE == RawTableSchema.COLUMNS.indexOf("vwap_paise"), "VWAP_PAISE");
        check(TOTAL_BUY_QTY == RawTableSchema.COLUMNS.indexOf("total_buy_qty"), "TOTAL_BUY_QTY");
        check(TOTAL_SELL_QTY == RawTableSchema.COLUMNS.indexOf("total_sell_qty"), "TOTAL_SELL_QTY");
        check(OPEN_INTEREST == RawTableSchema.COLUMNS.indexOf("open_interest"), "OPEN_INTEREST");
        check(BID_PX_1 == RawTableSchema.COLUMNS.indexOf("bid_px_1"), "BID_PX_1");
        check(BID_QTY_1 == RawTableSchema.COLUMNS.indexOf("bid_qty_1"), "BID_QTY_1");
        check(ASK_PX_1 == RawTableSchema.COLUMNS.indexOf("ask_px_1"), "ASK_PX_1");
        check(ASK_QTY_1 == RawTableSchema.COLUMNS.indexOf("ask_qty_1"), "ASK_QTY_1");
        check(OI_DAY_HIGH == RawTableSchema.COLUMNS.indexOf("oi_day_high"), "OI_DAY_HIGH");
        check(OI_DAY_LOW == RawTableSchema.COLUMNS.indexOf("oi_day_low"), "OI_DAY_LOW");
        check(LOWER_LIMIT_PAISE == RawTableSchema.COLUMNS.indexOf("lower_limit_paise"), "LOWER_LIMIT_PAISE");
        check(UPPER_LIMIT_PAISE == RawTableSchema.COLUMNS.indexOf("upper_limit_paise"), "UPPER_LIMIT_PAISE");
    }

    private static void check(boolean ok, String col) {
        if (!ok) {
            throw new IllegalStateException("RawTableColumns index drift: " + col);
        }
    }

    static void validateSchemaContract(int fieldCount, List<String> names) {
        if (RawTableSchema.FIELD_COUNT != fieldCount
                || !RawTableSchema.COLUMNS.equals(names)) {
            throw new IllegalStateException(
                    "raw_table_1 schema mismatch: expected " + RawTableSchema.FIELD_COUNT
                            + " cols " + RawTableSchema.COLUMNS + " but got " + fieldCount
                            + " cols " + names + " at firstDiff=" + firstDiff(names));
        }
    }

    private static String firstDiff(List<String> names) {
        List<String> expected = RawTableSchema.COLUMNS;
        int n = Math.min(expected.size(), names.size());
        for (int i = 0; i < n; i++) {
            if (!expected.get(i).equals(names.get(i))) {
                return i + ": expected " + expected.get(i) + " got " + names.get(i);
            }
        }
        if (expected.size() != names.size()) {
            return "size: expected " + expected.size() + " got " + names.size();
        }
        return "none";
    }

    public static String name(int index) {
        if (index < 0 || index >= FIELD_COUNT) {
            throw new IllegalArgumentException(
                    "invalid raw_table_1 field index " + index + ", expected 0 <= index < " + FIELD_COUNT);
        }
        return NAMES[index];
    }

    public static String[] namesCopy() {
        return NAMES.clone();
    }
}
