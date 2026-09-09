package com.trading.compute.safetyhalt;

import com.trading.common.safety.SafetyHaltRequestParser;
import com.trading.common.safety.SlotSafetyRequest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bridges a {@link RowData} changelog row of the {@code Safety_Halt_Requests}
 * table into the tested {@link SlotSafetyRequest} model via the column map
 * accepted by {@link SafetyHaltRequestParser}.
 *
 * <p>Column positions are the DDL v3 column order
 * (code/01_platform/02_sql/ddl/18_safety_halt_requests.sql, 21 columns).
 * Only the columns the consumer needs are read; the row contract gate
 * (contract_version = 2, state vocabulary, UNSAFE-reason) lives in the
 * parser and tracker, both unit-tested in the common module.
 */
public final class SafetyHaltRowDataBridge {

    // DDL v3 column positions (0-indexed, 21 columns total).
    private static final int IDX_HALT_REQUEST_ID = 0;
    private static final int IDX_SOURCE_COMPONENT = 4;
    private static final int IDX_REASON_CODE = 6;
    private static final int IDX_DETECTION_TIME = 8;
    private static final int IDX_SLOT_ID = 14;
    private static final int IDX_CONNECTION_EPOCH = 15;
    private static final int IDX_MANIFEST_FINGERPRINT = 16;
    private static final int IDX_ASSIGNED_TOKEN_SET_HASH = 17;
    private static final int IDX_STATE = 18;
    private static final int IDX_CONTRACT_VERSION = 20;

    private SafetyHaltRowDataBridge() {}

    /** P2-012: DDL v3 width — any add/reorder must fail loud, never mis-map. */
    private static final int EXPECTED_ARITY = 21;

    /** P2-014: null becomes null (parser throws ParseException), never NPE. */
    private static String textOrNull(RowData row, int idx) {
        return row.isNullAt(idx) ? null : row.getString(idx).toString();
    }

    /**
     * @param row a current-value changelog row ({@link RowKind#INSERT} or
     *            {@link RowKind#UPDATE_AFTER}); BEFORE/DELETE rows are not
     *            valid inputs
     * @throws SafetyHaltRequestParser.ParseException on any contract violation
     */
    public static SlotSafetyRequest toRequest(RowData row) {
        // P2-013: the bridge enforces its own documented contract — the
        // upstream filter is one refactor away from disappearing.
        if (row == null) {
            throw new SafetyHaltRequestParser.ParseException("row must not be null");
        }
        RowKind kind = row.getRowKind();
        if (kind != RowKind.INSERT && kind != RowKind.UPDATE_AFTER) {
            throw new SafetyHaltRequestParser.ParseException(
                    "non-current-value RowKind: " + kind);
        }
        // P2-012: brittle positional mapping must fail loud on DDL drift.
        if (row.getArity() != EXPECTED_ARITY) {
            throw new SafetyHaltRequestParser.ParseException(
                    "expected arity " + EXPECTED_ARITY + ", got " + row.getArity());
        }
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(SafetyHaltRequestParser.COL_HALT_REQUEST_ID,
                textOrNull(row, IDX_HALT_REQUEST_ID));
        map.put(SafetyHaltRequestParser.COL_SOURCE_COMPONENT,
                textOrNull(row, IDX_SOURCE_COMPONENT));
        map.put(SafetyHaltRequestParser.COL_SLOT_ID,
                textOrNull(row, IDX_SLOT_ID));
        // P2-015: null flows into readLong/readInt -> ParseException as
        // designed, never an uncaught NPE or a silent epoch-0.
        map.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH,
                row.isNullAt(IDX_CONNECTION_EPOCH) ? null : row.getLong(IDX_CONNECTION_EPOCH));
        map.put(SafetyHaltRequestParser.COL_STATE,
                textOrNull(row, IDX_STATE));
        map.put(SafetyHaltRequestParser.COL_REASON_CODE,
                textOrNull(row, IDX_REASON_CODE));
        map.put(SafetyHaltRequestParser.COL_MANIFEST_FINGERPRINT,
                textOrNull(row, IDX_MANIFEST_FINGERPRINT));
        map.put(SafetyHaltRequestParser.COL_ASSIGNED_TOKEN_SET_HASH,
                textOrNull(row, IDX_ASSIGNED_TOKEN_SET_HASH));
        map.put(SafetyHaltRequestParser.COL_DETECTION_TIME,
                row.isNullAt(IDX_DETECTION_TIME) ? null : row.getLong(IDX_DETECTION_TIME));
        map.put(SafetyHaltRequestParser.COL_CONTRACT_VERSION,
                row.isNullAt(IDX_CONTRACT_VERSION) ? null : row.getInt(IDX_CONTRACT_VERSION));
        return SafetyHaltRequestParser.parse(map);
    }
}
