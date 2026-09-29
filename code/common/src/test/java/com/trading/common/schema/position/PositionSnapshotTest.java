package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.PositionState;
import org.junit.jupiter.api.Test;

/**
 * L5-3: the canonical average-price encoding ({@code average_*_paise = 0} iff
 * the matching quantity is 0, 10_positions.sql) is enforced by the record
 * itself — a live quantity can never carry a zero price and a flat side can
 * never carry a stale one.
 */
class PositionSnapshotTest {

    private static PositionSnapshot snapshot(long open, long closed, long avgEntry, long avgExit) {
        return new PositionSnapshot("pos-1", "tc-1", "acc-1", 7L, "NSE", "TEST", "BUY",
                PositionState.OPEN, open, closed, avgEntry, avgExit, "ev-1", 1L,
                1_000L, 1_000L, PositionsColumns.SCHEMA_VERSION_V2);
    }

    @Test
    void flatAndLiveSidesCarryTheCanonicalValues() {
        assertThat(snapshot(0, 0, 0, 0).currentQuantity()).isZero();
        assertThat(snapshot(100, 0, 10_050, 0).averageEntryPaise()).isEqualTo(10_050);
        assertThat(snapshot(100, 100, 10_050, 11_000).averageExitPaise()).isEqualTo(11_000);
    }

    @Test
    void aLiveQuantityWithAZeroPriceIsRejected() {
        assertThatThrownBy(() -> snapshot(100, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("average_entry_paise");
        assertThatThrownBy(() -> snapshot(100, 100, 10_050, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("average_exit_paise");
    }

    @Test
    void aFlatSideWithAStalePriceIsRejected() {
        assertThatThrownBy(() -> snapshot(0, 0, 10_050, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("average_entry_paise");
        assertThatThrownBy(() -> snapshot(100, 0, 10_050, 11_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("average_exit_paise");
    }
}
