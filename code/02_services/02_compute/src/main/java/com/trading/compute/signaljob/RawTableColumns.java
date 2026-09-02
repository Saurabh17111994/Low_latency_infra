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

    /** DDL column names in index order (diagnostics). */
    public static final String[] NAMES = {
        "event_day", "event_fingerprint", "fingerprint_version", "connection_id",
        "connection_epoch", "instrument_token", "exchange", "symbol", "event_time",
        "ingest_ts", "ack_ts", "tick_type", "last_price_paise", "last_qty", "raw_payload",
        "payload_hash", "decoder_version", "protocol_version", "validity_state",
        "validity_reason", "schema_version"
    };

    static {
        validateSchemaContract(FIELD_COUNT, Arrays.asList(NAMES));
    }

    static void validateSchemaContract(int fieldCount, List<String> names) {
        if (RawTableSchema.FIELD_COUNT != fieldCount
                || !RawTableSchema.COLUMNS.equals(names)) {
            throw new IllegalStateException(
                    "raw_table_1 schema mismatch: compute indexes must match shared DDL order");
        }
    }

    public static String name(int index) {
        return NAMES[index];
    }
}
