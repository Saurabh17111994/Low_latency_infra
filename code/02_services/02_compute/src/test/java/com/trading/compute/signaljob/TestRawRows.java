package com.trading.compute.signaljob;

import com.trading.common.config.PlatformConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * Test fixture builder for {@code raw_table_1} v4 rows (72 columns, DDL order).
 * Only the columns the operators read need values; the rest default to null.
 *
 * <p>{@code qty} is the tick's traded quantity. Since v4 (2026-09-24) the volume
 * source is {@link RawTableColumns#VOLUME_DELTA} -- the quantity traded since the
 * previous tick -- and {@code last_qty} is a single trade's size. A tick that
 * carries one trade reports both, so this builder sets both from {@code qty};
 * {@link #withVolumeDelta} exists for the cases where the two must differ.
 */
final class TestRawRows {

    private TestRawRows() {}

    static GenericRowData row(long instrumentToken, long eventTime, String fingerprint,
            String tickType, long pricePaise, long qty) {
        GenericRowData row = new GenericRowData(RawTableColumns.FIELD_COUNT);
        row.setField(RawTableColumns.EVENT_DAY, StringData.fromString("20260901"));
        row.setField(RawTableColumns.EVENT_FINGERPRINT, StringData.fromString(fingerprint));
        row.setField(RawTableColumns.FINGERPRINT_VERSION, StringData.fromString("v2"));
        row.setField(RawTableColumns.INSTRUMENT_TOKEN, instrumentToken);
        row.setField(RawTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(RawTableColumns.SYMBOL, StringData.fromString("TEST"));
        row.setField(RawTableColumns.EVENT_TIME, eventTime);
        row.setField(RawTableColumns.TICK_TYPE, StringData.fromString(tickType));
        row.setField(RawTableColumns.LAST_PRICE_PAISE, pricePaise);
        row.setField(RawTableColumns.LAST_QTY, qty);
        row.setField(RawTableColumns.VOLUME_DELTA, qty);
        row.setField(RawTableColumns.VALIDITY_STATE, StringData.fromString("VALID_TRADE"));
        row.setField(RawTableColumns.SCHEMA_VERSION,
                StringData.fromString(PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION));
        return row;
    }

    static RowData withValidity(RowData base, String validityState) {
        ((GenericRowData) base).setField(RawTableColumns.VALIDITY_STATE,
                validityState == null ? null : StringData.fromString(validityState));
        return base;
    }

    static RowData withFingerprint(RowData base, String fingerprint) {
        ((GenericRowData) base).setField(RawTableColumns.EVENT_FINGERPRINT,
                fingerprint == null ? null : StringData.fromString(fingerprint));
        return base;
    }

    static RowData withPrice(RowData base, long pricePaise) {
        ((GenericRowData) base).setField(RawTableColumns.LAST_PRICE_PAISE, pricePaise);
        return base;
    }

    static RowData withQty(RowData base, long qty) {
        ((GenericRowData) base).setField(RawTableColumns.LAST_QTY, qty);
        ((GenericRowData) base).setField(RawTableColumns.VOLUME_DELTA, qty);
        return base;
    }

    /** Sets only the v4 volume source, leaving {@code last_qty} as it is. */
    static RowData withVolumeDelta(RowData base, Long volumeDelta) {
        ((GenericRowData) base).setField(RawTableColumns.VOLUME_DELTA, volumeDelta);
        return base;
    }

    static RowData withSchemaVersion(RowData base, String schemaVersion) {
        ((GenericRowData) base).setField(RawTableColumns.SCHEMA_VERSION,
                schemaVersion == null ? null : StringData.fromString(schemaVersion));
        return base;
    }

    static RowData withEventTime(RowData base, long eventTime) {
        ((GenericRowData) base).setField(RawTableColumns.EVENT_TIME, eventTime);
        return base;
    }

    static RowData withIngestTs(RowData base, long ingestTs) {
        ((GenericRowData) base).setField(RawTableColumns.INGEST_TS, ingestTs);
        return base;
    }
}
