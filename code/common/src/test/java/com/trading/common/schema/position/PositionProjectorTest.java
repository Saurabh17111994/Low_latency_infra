package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.PositionState;
import org.junit.jupiter.api.Test;

/**
 * SCH-20 projector core (pure JVM): version-gated fill projection over the
 * Positions KV shape with lifecycle validation. Every quantity invariant and
 * every version outcome is driven here; the operator wiring (Action Capture)
 * is the only remaining integration.
 */
class PositionProjectorTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final String POS = "acc-1:12345";
    private static final String CTX = "ctx-1";

    private static FillEvent buy(long qty, long price, long version, String eventId) {
        return new FillEvent(POS, CTX, "acc-1", 12345L, "NSE", "TEST",
                FillEvent.SIDE_BUY, qty, price, eventId, version, NOW);
    }

    private static FillEvent sell(long qty, long price, long version, String eventId) {
        return new FillEvent(POS, CTX, "acc-1", 12345L, "NSE", "TEST",
                FillEvent.SIDE_SELL, qty, price, eventId, version, NOW);
    }

    @Test
    void staleReasonNamesTheRejectedFillAndBothVersions() {
        // P3-500: the reason used to carry only the current version, so quarantine triage could not
        // tell which fill was rejected without an extra lookup. STALE = older version, same fill
        // identity (KvStateUpdateProtocol.evaluate), reached here by replaying an older version.
        PositionSnapshot current =
                PositionProjector.apply(null, buy(10, 100, 2, "f1"), NOW).snapshot();

        PositionProjector.ProjectionResult r =
                PositionProjector.apply(current, buy(10, 100, 1, "f1"), NOW);

        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.STALE);
        assertThat(r.reason())
                .contains("f1")
                .contains("version 1")
                .contains("current version 2");
    }

    @Test
    void firstBuyOpensPosition() {
        PositionProjector.ProjectionResult r =
                PositionProjector.apply(null, buy(10, 100, 1, "f1"), NOW);

        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.APPLIED);
        assertThat(r.snapshot().state()).isEqualTo(PositionState.OPEN);
        assertThat(r.snapshot().openQuantity()).isEqualTo(10);
        assertThat(r.snapshot().closedQuantity()).isZero();
        assertThat(r.snapshot().currentQuantity()).isEqualTo(10);
        assertThat(r.snapshot().averageEntryPaise()).isEqualTo(100);
        assertThat(r.snapshot().createdTs()).isEqualTo(NOW);
        assertThat(r.snapshot().lastUpdateTs()).isEqualTo(NOW);
    }

    @Test
    void secondBuyUpdatesWeightedAverageEntry() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 1, "f1"), NOW);
        PositionProjector.ProjectionResult second =
                PositionProjector.apply(first.snapshot(), buy(10, 200, 2, "f2"), NOW);

        assertThat(second.outcome()).isEqualTo(PositionProjector.Outcome.APPLIED);
        assertThat(second.snapshot().openQuantity()).isEqualTo(20);
        assertThat(second.snapshot().averageEntryPaise()).isEqualTo(150);
        assertThat(second.snapshot().state()).isEqualTo(PositionState.OPEN);
    }

    @Test
    void partialSellEntersReducing() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 1, "f1"), NOW);
        PositionProjector.ProjectionResult second =
                PositionProjector.apply(first.snapshot(), sell(3, 120, 2, "f2"), NOW);

        assertThat(second.outcome()).isEqualTo(PositionProjector.Outcome.APPLIED);
        assertThat(second.snapshot().state()).isEqualTo(PositionState.REDUCING);
        assertThat(second.snapshot().closedQuantity()).isEqualTo(3);
        assertThat(second.snapshot().currentQuantity()).isEqualTo(7);
        assertThat(second.snapshot().averageExitPaise()).isEqualTo(120);
    }

    @Test
    void fullSellClosesPosition() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 1, "f1"), NOW);
        PositionProjector.ProjectionResult second =
                PositionProjector.apply(first.snapshot(), sell(10, 110, 2, "f2"), NOW);

        assertThat(second.outcome()).isEqualTo(PositionProjector.Outcome.APPLIED);
        assertThat(second.snapshot().state()).isEqualTo(PositionState.CLOSED);
        assertThat(second.snapshot().currentQuantity()).isZero();
    }

    @Test
    void sellOvershootIsViolation() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 1, "f1"), NOW);
        PositionProjector.ProjectionResult second =
                PositionProjector.apply(first.snapshot(), sell(15, 110, 2, "f2"), NOW);

        assertThat(second.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
        assertThat(second.reason()).contains("overshoots");
    }

    @Test
    void firstFillCannotBeASell() {
        PositionProjector.ProjectionResult r =
                PositionProjector.apply(null, sell(5, 110, 1, "f1"), NOW);

        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
    }

    @Test
    void reentryAfterFullClose() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 1, "f1"), NOW);
        PositionProjector.ProjectionResult second =
                PositionProjector.apply(first.snapshot(), sell(10, 110, 2, "f2"), NOW);
        PositionProjector.ProjectionResult third =
                PositionProjector.apply(second.snapshot(), buy(5, 90, 3, "f3"), NOW);

        assertThat(third.outcome()).isEqualTo(PositionProjector.Outcome.APPLIED);
        assertThat(third.snapshot().state()).isEqualTo(PositionState.OPEN);
        assertThat(third.snapshot().openQuantity()).isEqualTo(15);
        assertThat(third.snapshot().closedQuantity()).isEqualTo(10);
        assertThat(third.snapshot().averageEntryPaise()).isEqualTo(90);
    }

    @Test
    void duplicateVersionNoOp() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 5, "f5"), NOW);
        PositionProjector.ProjectionResult dup =
                PositionProjector.apply(first.snapshot(), buy(10, 100, 5, "f5"), NOW);

        assertThat(dup.outcome()).isEqualTo(PositionProjector.Outcome.DUPLICATE);
        assertThat(dup.snapshot()).isSameAs(first.snapshot());
    }

    @Test
    void staleVersionRejected() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 5, "f5"), NOW);
        PositionProjector.ProjectionResult stale =
                PositionProjector.apply(first.snapshot(), buy(10, 100, 3, "f5"), NOW);

        assertThat(stale.outcome()).isEqualTo(PositionProjector.Outcome.STALE);
        assertThat(stale.reason()).contains("stale");
    }

    @Test
    void sameVersionDifferentContentIsConflictViolation() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 5, "f5"), NOW);
        PositionProjector.ProjectionResult conflict =
                PositionProjector.apply(first.snapshot(), buy(10, 999, 5, "f-other"), NOW);

        assertThat(conflict.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
        assertThat(conflict.reason()).contains("CONFLICT");
    }

    @Test
    void lowerVersionDifferentContentIsRegressionViolation() {
        PositionProjector.ProjectionResult first =
                PositionProjector.apply(null, buy(10, 100, 5, "f5"), NOW);
        PositionProjector.ProjectionResult regression =
                PositionProjector.apply(first.snapshot(), sell(3, 999, 2, "f2"), NOW);

        assertThat(regression.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
        assertThat(regression.reason()).contains("REGRESSION");
    }

    @Test
    void negativeVersionIsUnknownViolation() {
        PositionProjector.ProjectionResult r =
                PositionProjector.apply(null, buy(10, 100, -1, "f-1"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
    }

    // --- P3-165: the never-throws contract must survive a null identity / null fill ---

    @Test
    void nullFillIsRejectedAsAContractViolation() {
        // Exact message, not `contains("fill")`: the JVM's helpful NullPointerException text also
        // mentions the variable name, so a loose assertion would pass even without the guard.
        assertThatThrownBy(() -> PositionProjector.apply(null, null, NOW))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fill");
    }

    @Test
    void nullSnapshotEventIdYieldsControlledConflictNotNpe() {
        PositionSnapshot first =
                PositionProjector.apply(null, buy(10, 100, 5, "f5"), NOW).snapshot();
        // A snapshot hydrated from a corrupt store — the record does not reject a null event id,
        // so the content check must not assume one side is non-null.
        PositionSnapshot corrupt = new PositionSnapshot(
                first.positionId(), first.tradeContextId(), first.accountScopeId(),
                first.instrumentToken(), first.exchange(), first.symbol(), first.side(),
                first.state(), first.openQuantity(), first.closedQuantity(),
                first.averageEntryPaise(), first.averageExitPaise(),
                null, 5L, first.createdTs(), first.lastUpdateTs(), first.schemaVersion());

        // Same version, and content cannot match a null -> CONFLICT -> VIOLATION, not an NPE.
        PositionProjector.ProjectionResult r =
                PositionProjector.apply(corrupt, buy(10, 100, 5, "f5"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
        assertThat(r.reason()).contains("CONFLICT");
    }

    // --- P3-393: a null prior state must be rejected, not thrown from ---

    @Test
    void nullSnapshotStateYieldsViolationNotNpe() {
        PositionSnapshot first =
                PositionProjector.apply(null, buy(10, 100, 5, "f5"), NOW).snapshot();
        // PositionSnapshot does not reject a null state, so a corrupt store can supply one.
        PositionSnapshot corrupt = new PositionSnapshot(
                first.positionId(), first.tradeContextId(), first.accountScopeId(),
                first.instrumentToken(), first.exchange(), first.symbol(), first.side(),
                null, first.openQuantity(), first.closedQuantity(),
                first.averageEntryPaise(), first.averageExitPaise(),
                first.sourceEventId(), 5L, first.createdTs(), first.lastUpdateTs(),
                first.schemaVersion());

        // A newer version passes the gate, so the run reaches the lifecycle check — where
        // isLegalTransition(null, OPEN) used to throw. A null prior now poisons like UNKNOWN.
        PositionProjector.ProjectionResult r =
                PositionProjector.apply(corrupt, buy(10, 100, 6, "f6"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
        assertThat(r.reason()).contains("illegal transition");
    }

    // --- P3-392 / P3-394: version-gate and arithmetic boundaries ---

    @Test
    void firstFillWithVersionZeroIsApplied() {
        // With no prior snapshot there is nothing to compare against, so the version gate must not
        // run: a first fill carrying version 0 used to evaluate CONFLICT -> VIOLATION, rejecting a
        // legitimate first write and mislabelling it for quarantine triage.
        PositionProjector.ProjectionResult r =
                PositionProjector.apply(null, buy(10, 100, 0, "f0"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.APPLIED);
        assertThat(r.snapshot().sourceVersion()).isZero();
    }

    @Test
    void negativeFirstVersionIsStillViolation() {
        // The P3-392 fix must not launder a negative first version into a clean write.
        PositionProjector.ProjectionResult r =
                PositionProjector.apply(null, buy(10, 100, -1, "f-1"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
    }

    @Test
    void overflowIsViolationNotAWrappedAverage() {
        // P3-394: `open` is cumulative across cycles and the average multiplies price by quantity,
        // so a saturated fill wrapped silently and corrupted avgEntry/avgExit — or threw from the
        // snapshot invariant out of a never-throws path. Exact arithmetic makes it a VIOLATION.
        PositionProjector.ProjectionResult r = PositionProjector.apply(null,
                buy(Long.MAX_VALUE, Long.MAX_VALUE, 1, "f-big"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjector.Outcome.VIOLATION);
        assertThat(r.reason()).contains("overflow");
    }
}
