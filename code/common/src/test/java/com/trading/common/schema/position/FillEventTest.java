package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * P3-159 pin for the {@link FillEvent} identity invariant: {@code sourceEventId}
 * is the fill identity used by the duplicate/conflict content check, and it is
 * dereferenced by {@link PositionProjector} and by
 * {@code FlussPositionsStateStore.upsert}, so a null/blank one is rejected at
 * construction rather than surfacing as an NPE mid-projection.
 */
class FillEventTest {

    private static FillEvent fill(String sourceEventId, long sourceVersion) {
        return new FillEvent("pos-acc-1-123-BUY-1", "tc-1", "acc-1", 123L, "NSE", "RELIANCE",
                FillEvent.SIDE_BUY, 10L, 10050L, sourceEventId, sourceVersion, 0L);
    }

    @Test
    void rejectsNullSourceEventId() {
        assertThatThrownBy(() -> fill(null, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source_event_id");
    }

    @Test
    void rejectsBlankSourceEventId() {
        assertThatThrownBy(() -> fill("   ", 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source_event_id");
    }

    @Test
    void negativeSourceVersionIsDeliberatelyNotRejectedHere() {
        // Negativity must flow to PositionProjector / KvStateUpdateProtocol, which maps it to
        // UNKNOWN -> VIOLATION (PositionProjectorTest.negativeVersionIsUnknownViolation).
        // Rejecting it here would change that pinned outcome into a constructor failure.
        assertThatCode(() -> fill("pb-1", -1L)).doesNotThrowAnyException();
    }

    @Test
    void withPositionIdRebindsOnlyThePositionId() {
        FillEvent rebound = fill("pb-1", 7L).withPositionId("pos-acc-1-123-BUY-2");

        assertThat(rebound.positionId()).isEqualTo("pos-acc-1-123-BUY-2");
        assertThat(rebound.sourceEventId()).isEqualTo("pb-1");
        assertThat(rebound.sourceVersion()).isEqualTo(7L);
        assertThat(rebound.tradeContextId()).isEqualTo("tc-1");
        assertThat(rebound.accountScopeId()).isEqualTo("acc-1");
        assertThat(rebound.fillQty()).isEqualTo(10L);
        assertThat(rebound.fillPricePaise()).isEqualTo(10050L);
        assertThat(rebound.side()).isEqualTo(FillEvent.SIDE_BUY);
    }
}
