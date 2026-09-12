package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.PositionState;
import com.trading.common.schema.position.PositionSnapshot;
import org.junit.jupiter.api.Test;

class PositionProjectionWriterTest {

    private static final long NOW = 1000L;

    private static NautilusPositionEvent event(long seq, String side, long open, long closed,
            PositionState state) {
        return new NautilusPositionEvent(
                "pos-1", "tc-1", "acc-1", 1001L, "CME", "wti", side, state,
                open, closed, 1000L, 900L, "evt-" + seq, seq, NOW);
    }

    @Test
    void serializesNautilusEventWithoutRecomputing() {
        NautilusPositionEvent e = event(1L, "BUY", 10, 0, PositionState.OPEN);
        PositionProjectionWriter.PositionWriteResult r = PositionProjectionWriter.apply(null, e, NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectionWriter.Outcome.APPLIED);
        // open/closed/avg are carried unchanged from Nautilus — not recomputed.
        assertThat(r.snapshot().openQuantity()).isEqualTo(10);
        assertThat(r.snapshot().closedQuantity()).isEqualTo(0);
        assertThat(r.snapshot().averageEntryPaise()).isEqualTo(1000L);
        assertThat(r.snapshot().state()).isEqualTo(PositionState.OPEN);
        assertThat(r.snapshot().sourceVersion()).isEqualTo(1L);
    }

    @Test
    void duplicateVersionNoOp() {
        NautilusPositionEvent e = event(1L, "BUY", 10, 0, PositionState.OPEN);
        PositionProjectionWriter.PositionWriteResult first = PositionProjectionWriter.apply(null, e, NOW);
        PositionProjectionWriter.PositionWriteResult dupe = PositionProjectionWriter.apply(
                first.snapshot(), e, NOW);
        assertThat(dupe.outcome()).isEqualTo(PositionProjectionWriter.Outcome.DUPLICATE);
    }

    @Test
    void staleVersionRejected() {
        NautilusPositionEvent newer = event(2L, "BUY", 10, 0, PositionState.OPEN);
        PositionProjectionWriter.PositionWriteResult first = PositionProjectionWriter.apply(null, newer, NOW);
        // Same content (same source event id) at an older version: benign replay.
        NautilusPositionEvent older = new NautilusPositionEvent(
                "pos-1", "tc-1", "acc-1", 1001L, "CME", "wti", "BUY",
                PositionState.OPEN, 10, 0, 1000L, 900L, "evt-2", 1L, NOW);
        PositionProjectionWriter.PositionWriteResult stale = PositionProjectionWriter.apply(
                first.snapshot(), older, NOW);
        assertThat(stale.outcome()).isEqualTo(PositionProjectionWriter.Outcome.STALE);
        // P3-500 sibling: the detail names the rejected write, not only the current version.
        assertThat(stale.detail())
                .contains("pos-1")
                .contains("version 1")
                .contains("current version 2");
    }

    @Test
    void regressionVersionIsViolation() {
        // P3-405: older version with different content is divergence, not a
        // benign stale replay — distinct outcome and reason.
        NautilusPositionEvent newer = event(2L, "BUY", 10, 0, PositionState.OPEN);
        PositionProjectionWriter.PositionWriteResult first = PositionProjectionWriter.apply(null, newer, NOW);
        NautilusPositionEvent older = event(1L, "BUY", 5, 0, PositionState.OPEN);
        PositionProjectionWriter.PositionWriteResult r = PositionProjectionWriter.apply(
                first.snapshot(), older, NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectionWriter.Outcome.VIOLATION);
        assertThat(r.reason()).isEqualTo(QuarantineReason.TERMINAL_REGRESSION);
    }

    @Test
    void firstRowAtVersionZeroIsApplied() {
        // P3-170: sourceSequence 0 is legal — a null row is never a collision.
        NautilusPositionEvent e = event(0L, "BUY", 10, 0, PositionState.OPEN);
        PositionProjectionWriter.PositionWriteResult r = PositionProjectionWriter.apply(null, e, NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectionWriter.Outcome.APPLIED);
    }

    @Test
    void inconsistentEventIsViolation() {
        // open=5 closed=5 with state OPEN contradicts the derived reading => violation.
        NautilusPositionEvent bad = new NautilusPositionEvent(
                "pos-1", "tc-1", "acc-1", 1001L, "CME", "wti", "BUY",
                PositionState.OPEN, 5, 5, 1000L, 900L, "evt-1", 1L, NOW);
        PositionProjectionWriter.PositionWriteResult r = PositionProjectionWriter.apply(null, bad, NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectionWriter.Outcome.VIOLATION);
        assertThat(r.reason()).isEqualTo(QuarantineReason.POSITION_VIOLATION);
    }

    @Test
    void conflictVersionIsViolation() {
        NautilusPositionEvent e1 = event(1L, "BUY", 10, 0, PositionState.OPEN);
        PositionProjectionWriter.PositionWriteResult first = PositionProjectionWriter.apply(null, e1, NOW);
        NautilusPositionEvent conflicting = new NautilusPositionEvent(
                "pos-1", "tc-1", "acc-1", 1001L, "CME", "wti", "BUY",
                PositionState.OPEN, 20, 0, 1000L, 900L, "evt-DIFFERENT", 1L, NOW);
        PositionProjectionWriter.PositionWriteResult r =
                PositionProjectionWriter.apply(first.snapshot(), conflicting, NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectionWriter.Outcome.VIOLATION);
    }
}
