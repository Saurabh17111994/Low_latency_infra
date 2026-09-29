package com.trading.compute.babysitter;

import com.trading.common.schema.position.PositionsColumns;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

/** Minimal current-value {@code Positions} changelog row builder for tests. */
final class TestPositionsRows {

    private TestPositionsRows() {}

    static RowData row(RowKind kind, String pid, String eventId, long version, String state,
            long open, long closed) {
        GenericRowData r = new GenericRowData(kind, PositionsColumns.FIELD_COUNT);
        r.setField(PositionsColumns.POSITION_ID, StringData.fromString(pid));
        r.setField(PositionsColumns.TRADE_CONTEXT_ID, StringData.fromString("tc-1"));
        r.setField(PositionsColumns.ACCOUNT_SCOPE_ID, StringData.fromString("acct-1"));
        r.setField(PositionsColumns.INSTRUMENT_TOKEN, 7L);
        r.setField(PositionsColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(PositionsColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(PositionsColumns.SIDE, StringData.fromString("BUY"));
        r.setField(PositionsColumns.STATE, StringData.fromString(state));
        r.setField(PositionsColumns.OPEN_QUANTITY, open);
        r.setField(PositionsColumns.CLOSED_QUANTITY, closed);
        // L5-3: the canonical encoding — a live quantity carries a price, a flat
        // side stays NULL (the deserializer reads NULL as 0).
        r.setField(PositionsColumns.AVERAGE_ENTRY_PAISE, open == 0 ? null : 250_000L);
        r.setField(PositionsColumns.AVERAGE_EXIT_PAISE, closed == 0 ? null : 260_000L);
        r.setField(PositionsColumns.SOURCE_EVENT_ID, StringData.fromString(eventId));
        r.setField(PositionsColumns.SOURCE_VERSION, version);
        r.setField(PositionsColumns.CREATED_TS, 1_000L);
        r.setField(PositionsColumns.LAST_UPDATE_TS, 2_000L);
        r.setField(PositionsColumns.SCHEMA_VERSION,
                StringData.fromString(PositionsColumns.SCHEMA_VERSION_V2));
        return r;
    }

    static RowData row(String pid, String eventId, long version, String state,
            long open, long closed) {
        return row(RowKind.INSERT, pid, eventId, version, state, open, closed);
    }
}
