package com.trading.compute.signaljob;

import com.trading.common.schema.RawTableSchema;
import java.util.Arrays;
import java.util.List;

/**
 * Physical column layout of {@code raw_table_1} as consumed by the Signal job.
 *
 * <p>Must mirror {@code code/01_platform/02_sql/ddl/02_raw_table_1.sql} v3 (21
 * columns) exactly. The Fluss source's {@code RowDataDeserializationSchema}
 * produces rows in table column order, so field indexes are the DDL positions.
 * v2 (R-054/R-231) removed the quote (bid/ask) and option columns — do not
 * re-add indexes for them.
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

    public static final int FIELD_COUNT = 21;

    /** DDL column names in index order (diagnostics). Do not expose mutably. */
    private static final String[] NAMES = {
        "event_day", "event_fingerprint", "fingerprint_version", "connection_id",
        "connection_epoch", "instrument_token", "exchange", "symbol", "event_time",
        "ingest_ts", "ack_ts", "tick_type", "last_price_paise", "last_qty", "raw_payload",
        "payload_hash", "decoder_version", "protocol_version", "validity_state",
        "validity_reason", "schema_version"
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
