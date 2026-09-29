package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.position.PositionsColumns;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

/**
 * L5-3: the gateway writer canonicalizes the Positions average-price encoding
 * ({@code average_*_paise = 0} iff the matching quantity is 0): a null on a
 * flat side is written as 0, a leftover price on a flat side is canonicalized
 * to 0, and a null with a live quantity fails closed instead of writing NULL
 * beside a live quantity.
 */
class FlussProjectionWriterPositionEncodingTest {

    private static NormalizedExecutionEvent event(Long avgEntry, Long avgExit, long open, long closed) {
        long now = 1_700_000_000_000L;
        return new NormalizedExecutionEvent("pb-1", "acc-1", "part-1", 0L, "actor-1", "FILL", now,
                new NormalizedExecutionEvent.Audit("audit-1", "evh", "summary"),
                new NormalizedExecutionEvent.Fill("fp-1", "v1", "broker-1", "instr-1", "att-1", "tc-1",
                        "EXECUTED", open, 0L, open, 15_000L, "fill-1", now, now, now,
                        new byte[] {1}, "ph-1", "CORRELATED", null, "v1"),
                new NormalizedExecutionEvent.Lifecycle("broker-1", "instr-1", "att-1", "tc-1",
                        "FILLED", open, 0L, 15_000L, 1L, now, now, "CORRELATED"),
                new NormalizedExecutionEvent.Position("pos-1", "tc-1", 7L, "NSE", "TEST", "BUY",
                        "OPEN", open, closed, avgEntry, avgExit, 1L, now, now),
                new NormalizedExecutionEvent.Correlation("instr-1", "att-1", "clref-1", "broker-1",
                        "tc-1", "pos-1", "VERIFIED", "ev", now));
    }

    private static long longField(InternalRow row, int index) {
        return row.getLong(index);
    }

    @Test
    void nullExitPriceOnAFlatSideIsWrittenAsZero() {
        InternalRow row = FlussProjectionWriter.positionRow(event(15_000L, null, 100L, 0L));
        assertThat(longField(row, PositionsColumns.AVERAGE_ENTRY_PAISE)).isEqualTo(15_000L);
        assertThat(longField(row, PositionsColumns.AVERAGE_EXIT_PAISE)).isZero();
    }

    @Test
    void leftoverPricesOnAFlatSideAreCanonicalizedToZero() {
        InternalRow row = FlussProjectionWriter.positionRow(event(15_000L, 11_000L, 100L, 0L));
        assertThat(longField(row, PositionsColumns.AVERAGE_EXIT_PAISE)).isZero();
    }

    @Test
    void nullPriceWithALiveQuantityFailsClosed() {
        assertThatThrownBy(() -> FlussProjectionWriter.positionRow(event(null, null, 100L, 0L)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("average_entry_paise");
        assertThatThrownBy(() -> FlussProjectionWriter.positionRow(event(15_000L, null, 100L, 100L)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("average_exit_paise");
    }

    @Test
    void livePricesPassThroughUnchanged() {
        InternalRow row = FlussProjectionWriter.positionRow(event(15_000L, 11_000L, 100L, 100L));
        assertThat(longField(row, PositionsColumns.AVERAGE_ENTRY_PAISE)).isEqualTo(15_000L);
        assertThat(longField(row, PositionsColumns.AVERAGE_EXIT_PAISE)).isEqualTo(11_000L);
    }
}
