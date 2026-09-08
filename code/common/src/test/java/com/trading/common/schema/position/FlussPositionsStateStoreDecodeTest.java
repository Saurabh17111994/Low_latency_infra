package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.PositionState;
import java.lang.reflect.Method;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

/**
 * P4-352: {@code FlussPositionsStateStore.toSnapshot} must decode NULL avg
 * prices (nullable in DDL — NULL iff the matching quantity is 0) as 0L
 * instead of crashing on {@code getLong}.
 */
class FlussPositionsStateStoreDecodeTest {

    private static PositionSnapshot toSnapshot(InternalRow r) throws Exception {
        Method m = FlussPositionsStateStore.class.getDeclaredMethod("toSnapshot", InternalRow.class);
        m.setAccessible(true);
        try {
            return (PositionSnapshot) m.invoke(null, r);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException re) throw re;
            throw e;
        }
    }

    private static GenericRow fullRow() {
        Object[] v = new Object[PositionsColumns.FIELD_COUNT];
        v[PositionsColumns.POSITION_ID] = BinaryString.fromString("pos-1");
        v[PositionsColumns.TRADE_CONTEXT_ID] = BinaryString.fromString("tc-1");
        v[PositionsColumns.ACCOUNT_SCOPE_ID] = BinaryString.fromString("acc-1");
        v[PositionsColumns.INSTRUMENT_TOKEN] = 123L;
        v[PositionsColumns.EXCHANGE] = BinaryString.fromString("NSE");
        v[PositionsColumns.SYMBOL] = BinaryString.fromString("RELIANCE");
        v[PositionsColumns.SIDE] = BinaryString.fromString("LONG");
        v[PositionsColumns.STATE] = BinaryString.fromString(PositionState.OPEN.name());
        v[PositionsColumns.OPEN_QUANTITY] = 10L;
        v[PositionsColumns.CLOSED_QUANTITY] = 0L;
        v[PositionsColumns.AVERAGE_ENTRY_PAISE] = 250000L;
        v[PositionsColumns.AVERAGE_EXIT_PAISE] = null;
        v[PositionsColumns.SOURCE_EVENT_ID] = BinaryString.fromString("e-1");
        v[PositionsColumns.SOURCE_VERSION] = 1L;
        v[PositionsColumns.CREATED_TS] = 1_700_000_000_000L;
        v[PositionsColumns.LAST_UPDATE_TS] = 1_700_000_000_000L;
        v[PositionsColumns.SCHEMA_VERSION] = BinaryString.fromString("2");
        return GenericRow.of(v);
    }

    @Test
    void nullAvgExitDecodesAsZero() throws Exception {
        PositionSnapshot s = toSnapshot(fullRow());
        assertThat(s.averageExitPaise()).isZero();
        assertThat(s.averageEntryPaise()).isEqualTo(250000L);
    }

    @Test
    void nullAvgEntryDecodesAsZero() throws Exception {
        GenericRow row = fullRow();
        row.setField(PositionsColumns.AVERAGE_ENTRY_PAISE, null);
        PositionSnapshot s = toSnapshot(row);
        assertThat(s.averageEntryPaise()).isZero();
    }
}
