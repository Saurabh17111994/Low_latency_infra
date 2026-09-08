package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import java.lang.reflect.Method;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

/**
 * P4-042: {@code FlussAttemptStore.fromRow} must fail closed on a legacy row
 * whose {@code gate_fence_token} is null (v3 declares NOT NULL) instead of
 * decoding fence {@code 0} — a silently minted never-acquired fence.
 */
class FlussAttemptStoreFromRowTest {

    private static AttemptRecord fromRow(InternalRow r) throws Exception {
        Method m = FlussAttemptStore.class.getDeclaredMethod("fromRow", InternalRow.class);
        m.setAccessible(true);
        try {
            return (AttemptRecord) m.invoke(null, r);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException re) throw re;
            throw e;
        }
    }

    private static GenericRow fullRow() {
        Object[] v = new Object[ExecutionAttemptsColumns.FIELD_COUNT];
        v[ExecutionAttemptsColumns.EXECUTION_ATTEMPT_ID] = BinaryString.fromString("a-1");
        v[ExecutionAttemptsColumns.ACCOUNT_SCOPE_ID] = BinaryString.fromString("acc-1");
        v[ExecutionAttemptsColumns.INSTRUCTION_ID] = BinaryString.fromString("ins-1");
        v[ExecutionAttemptsColumns.ACTION_ID] = null;
        v[ExecutionAttemptsColumns.EXECUTION_PARTITION_ID] = BinaryString.fromString("p-1");
        v[ExecutionAttemptsColumns.REQUEST_HASH] = BinaryString.fromString("h-1");
        v[ExecutionAttemptsColumns.CLIENT_ORDER_REF] = BinaryString.fromString("E1");
        v[ExecutionAttemptsColumns.BROKER_ORDER_ID] = null;
        v[ExecutionAttemptsColumns.GATE_EPOCH] = 7L;
        v[ExecutionAttemptsColumns.PHASE] = BinaryString.fromString("PREPARED");
        v[ExecutionAttemptsColumns.PHASE_EPOCH] = 0L;
        v[ExecutionAttemptsColumns.OUTCOME] = null;
        v[ExecutionAttemptsColumns.OUTCOME_DETAIL] = null;
        v[ExecutionAttemptsColumns.PREPARED_TS] = 1_700_000_000_000L;
        v[ExecutionAttemptsColumns.SUBMITTED_TS] = null;
        v[ExecutionAttemptsColumns.TERMINAL_TS] = null;
        v[ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY] = null;
        v[ExecutionAttemptsColumns.RETRY_ATTEMPT] = 0;
        v[ExecutionAttemptsColumns.GATE_FENCE_TOKEN] = 42L;
        v[ExecutionAttemptsColumns.SCHEMA_VERSION] = BinaryString.fromString("3");
        return GenericRow.of(v);
    }

    @Test
    void nullFenceTokenFailsClosed() {
        GenericRow row = fullRow();
        row.setField(ExecutionAttemptsColumns.GATE_FENCE_TOKEN, null);
        assertThatThrownBy(() -> fromRow(row))
                .hasMessageContaining("without gate_fence_token");
    }

    @Test
    void presentFenceTokenDecodes() throws Exception {
        AttemptRecord rec = fromRow(fullRow());
        assertThat(rec.gateFenceToken()).isEqualTo(42L);
    }
}
