package com.trading.execution.gateway;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

/**
 * H1-1: row copy for the Safety_Halt_Requests application writeback.
 *
 * <p>The consumer records {@code application_result}/{@code applied_ts} (columns 11/12) by
 * upserting the whole KV row. This helper copies every other column unchanged, typed by the DDL
 * v3 declaration ({@code 18_safety_halt_requests.sql} / {@code DdlBootstrap.SAFETY_HALT_SCHEMA}):
 * STRING at 0–7, 10, 11, 13, 14, 16, 17, 18, 19; BIGINT at 8, 9, 12, 15; INT at 20.
 *
 * <p>Typed reads instead of a generic {@code getField}: the scanned row may be any
 * {@link InternalRow} implementation, and only the typed getters are on the interface. The index
 * table lives here once so the processor and both store implementations cannot drift.
 */
final class SafetyHaltRows {
    static final int COLUMNS = 21;
    static final int IDX_APPLICATION_RESULT = 11;
    static final int IDX_APPLIED_TS = 12;

    private SafetyHaltRows() {}

    /** Copies {@code row} with columns 11/12 replaced. Never mutates the input. */
    static GenericRow withApplication(InternalRow row, String applicationResult, long appliedTs) {
        if (row.getFieldCount() != COLUMNS) {
            throw new IllegalArgumentException("expected " + COLUMNS + " cols, got " + row.getFieldCount());
        }
        if (applicationResult == null || applicationResult.isBlank()) {
            throw new IllegalArgumentException("applicationResult must not be blank");
        }
        GenericRow out = new GenericRow(COLUMNS);
        for (int i = 0; i < COLUMNS; i++) {
            if (row.isNullAt(i)) {
                out.setField(i, null);
                continue;
            }
            Object value = switch (i) {
                case 8, 9, 15 -> row.getLong(i);
                case 20 -> row.getInt(i);
                default -> row.getString(i);
            };
            out.setField(i, value);
        }
        out.setField(IDX_APPLICATION_RESULT, BinaryString.fromString(applicationResult));
        out.setField(IDX_APPLIED_TS, appliedTs);
        return out;
    }

    /** The row's {@code halt_request_id} (column 0) as a String. */
    static String haltRequestId(InternalRow row) {
        return row.getString(0).toString();
    }
}
