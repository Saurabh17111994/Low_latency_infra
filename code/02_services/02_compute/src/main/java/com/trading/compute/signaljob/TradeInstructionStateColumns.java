package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the authoritative instruction-hash index
 * {@code trade_instruction_state} (SCH-19, REQ-FLS-008/015).
 *
 * <p>Must mirror the frozen 4-column layout of
 * {@code code/01_platform/02_sql/ddl/25_trade_instruction_state.sql} v1 (KV,
 * PK {@code instruction_id}) — the LOG twin of {@code Trade_Decisions} that
 * the instruction-feed protocol checks before every append. Pinned by
 * {@code TradeInstructionStateColumnsAgreementTest} against the DDL file
 * itself (cross-boundary pin habit).
 */
public final class TradeInstructionStateColumns {

    private TradeInstructionStateColumns() {}

    public static final int INSTRUCTION_ID = 0;
    public static final int CANONICAL_HASH = 1;
    public static final int FIRST_WRITTEN_TS = 2;
    public static final int SCHEMA_VERSION = 3;

    public static final int FIELD_COUNT = 4;

    public static final String SCHEMA_VERSION_V1 = "1";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of("STRING", "STRING", "BIGINT", "STRING");

    /** DDL nullability per column (25 DDL v1): all NOT NULL. */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(false, false, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    private static final String[] NAMES_INTERNAL = {
        "instruction_id", "canonical_hash", "first_written_ts", "schema_version"
    };

    /**
     * Immutable DDL column names in index order (P2-189: the array above is
     * private — this is the only public view, so no caller can mutate the
     * shared projection).
     */
    public static final List<String> COLUMN_NAMES = List.of(
        "instruction_id", "canonical_hash", "first_written_ts", "schema_version");

    /**
     * @deprecated Use {@link #COLUMN_NAMES} — the public array is retained
     * only for source compatibility and is a fresh copy per access, so
     * mutation cannot corrupt the shared projection.
     */
    @Deprecated
    public static String[] NAMES() {
        return NAMES_INTERNAL.clone();
    }

    /** Stream type info for emitted index rows, derived from the v1 DDL order (all NOT NULL). */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new VarCharType(false, VarCharType.MAX_LENGTH), // instruction_id NOT NULL
                new VarCharType(false, VarCharType.MAX_LENGTH), // canonical_hash NOT NULL
                new BigIntType(false),                          // first_written_ts NOT NULL
                new VarCharType(false, VarCharType.MAX_LENGTH)  // schema_version NOT NULL
            },
            NAMES_INTERNAL.clone());

    static {
        // P2-243 tripwire (CandleClosedColumns P2-127 pattern): every layout
        // constant must agree on FIELD_COUNT, or class-load fails loud.
        if (COLUMN_NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || ((org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                                ROW_TYPE_INFO)
                        .toRowSize()
                        != FIELD_COUNT) {
            throw new IllegalStateException(
                    "TradeInstructionStateColumns drift: COLUMN_NAMES/TYPE_ROOTS/"
                            + "COLUMN_NULLABLE_IN_DDL/ROW_TYPE_INFO must all have FIELD_COUNT="
                            + FIELD_COUNT);
        }
        checkIndex(INSTRUCTION_ID, "instruction_id");
        checkIndex(CANONICAL_HASH, "canonical_hash");
        checkIndex(FIRST_WRITTEN_TS, "first_written_ts");
        checkIndex(SCHEMA_VERSION, "schema_version");
    }

    private static void checkIndex(int index, String expected) {
        if (!NAMES_INTERNAL[index].equals(expected)) {
            throw new IllegalStateException(
                    "TradeInstructionStateColumns drift: index " + index + " must be '"
                            + expected + "', got '" + NAMES_INTERNAL[index] + "'");
        }
    }

    /** Defensive copy of the DDL column names (callers must not retain the array). */
    public static String[] namesCopy() {
        return NAMES_INTERNAL.clone();
    }
}
