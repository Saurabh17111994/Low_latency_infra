package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.GateState;
import com.trading.common.schema.ownership.ExecutionGateColumns;
import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P3-370: TRANSITION_TS used to be faked from {@code fenceAcquiredTs} (the transition time was
 * dropped, so the durable writer re-interpreted an unrelated column) with a 0L fallback. It is now
 * stamped by the mutating event itself.
 *
 * <p>The timestamps below are deliberately all DIFFERENT, so a regression to the old
 * "derive it from fenceAcquiredTs" behaviour cannot pass.
 */
class GateTransitionTsTest {

    private static final long FENCE_ACQUIRED_TS = 111L;
    private static final long LEASE_EXPIRES_TS = 222L;
    private static final long EVENT_TS = 999L;

    private static GateRow row(long fenceAcquiredTs, long transitionTs) {
        return new GateRow("p-1", "acct-1", GateState.ENABLED, 1L, "reason", "ev-hash",
                "ops-1", null, "ev-hash", "inst-1", 7L, fenceAcquiredTs, LEASE_EXPIRES_TS, null,
                transitionTs, null);
    }

    private static GateRow liveRow() {
        return new GateRow("p-1", "acct-1", GateState.ENABLED, 1L, "reason", "ev-hash",
                "ops-1", null, "ev-hash", "inst-1", 7L, FENCE_ACQUIRED_TS, EVENT_TS + 1_000L, null,
                FENCE_ACQUIRED_TS, null);
    }

    /** Calls the private durable encoder so the column mapping is asserted, not just the record. */
    private static Object[] encode(GateRow r) throws Exception {
        Method m = FlussGateStateStore.class.getDeclaredMethod("encode", GateRow.class);
        m.setAccessible(true);
        return (Object[]) m.invoke(null, r);
    }

    @Test
    @DisplayName("a state transition is stamped with its own clock, not the fence acquisition")
    void stateTransitionStampsItsOwnTime() {
        GateRow before = row(FENCE_ACQUIRED_TS, FENCE_ACQUIRED_TS);

        GateRow after = before.withState(GateState.HALTED, "halted by operator", "ev-hash", EVENT_TS);

        assertThat(after.transitionTs()).isEqualTo(EVENT_TS);
        assertThat(after.transitionTs())
                .as("must not be fenceAcquiredTs — that is the old, misleading behaviour")
                .isNotEqualTo(after.fenceAcquiredTs());
        assertThat(after.epoch()).isEqualTo(before.epoch() + 1);
    }

    @Test
    @DisplayName("every fence copy stamps its own event time")
    void fenceCopiesStampTheirOwnEventTime() {
        GateRow base = row(FENCE_ACQUIRED_TS, FENCE_ACQUIRED_TS);
        // A LIVING row for the renew leg: withRenewedLease fails closed on an expired lease
        // (P3-148), so the base row's 222 horizon would (correctly) reject a renewal at 999.
        GateRow live = liveRow();

        assertThat(base.withFence("inst-2", 8L, EVENT_TS, 1_000L).transitionTs())
                .as("withFence").isEqualTo(EVENT_TS);
        assertThat(base.withFenceLost(EVENT_TS).transitionTs())
                .as("withFenceLost").isEqualTo(EVENT_TS);
        assertThat(live.withRenewedLease(EVENT_TS, 1_000L).transitionTs())
                .as("withRenewedLease").isEqualTo(EVENT_TS);
        assertThat(base.withFenceCleared(EVENT_TS).transitionTs())
                .as("withFenceCleared").isEqualTo(EVENT_TS);
    }

    @Test
    @DisplayName("the durable encoder writes the transition time, not fence_acquired_ts")
    void durableEncodingWritesTheTransitionTime() throws Exception {
        Object[] v = encode(row(FENCE_ACQUIRED_TS, EVENT_TS));

        assertThat(((Number) v[ExecutionGateColumns.TRANSITION_TS]).longValue())
                .as("the old code wrote fence_acquired_ts here, which is a different event")
                .isEqualTo(EVENT_TS);
        assertThat(((Number) v[ExecutionGateColumns.FENCE_ACQUIRED_TS]).longValue())
                .isEqualTo(FENCE_ACQUIRED_TS);
    }

    @Test
    @DisplayName("a synthetic row records 0 (unknown) rather than a plausible-looking wrong time")
    void syntheticRowRecordsUnknown() throws Exception {
        // The 14-arg constructor is the test/synthetic seam: it has no event to stamp.
        GateRow synthetic = new GateRow("p-1", "acct-1", GateState.ENABLED, 1L, null, null,
                null, null, null, null, 0L, FENCE_ACQUIRED_TS, LEASE_EXPIRES_TS, null);

        assertThat(synthetic.transitionTs())
                .as("0 means 'not recorded' — it must not be invented from a neighbouring column")
                .isZero();
        assertThat(((Number) encode(synthetic)[ExecutionGateColumns.TRANSITION_TS]).longValue())
                .isZero();
    }

    @Test
    @DisplayName("withFenceCleared is idempotent and preserves the recorded transition time")
    void clearedRowIsIdempotent() {
        GateRow cleared = row(FENCE_ACQUIRED_TS, EVENT_TS).withFenceCleared(EVENT_TS);

        assertThat(cleared.ownerInstanceId()).isNull();
        assertThat(cleared.fenceToken())
                .as("P3-374: the token is the write-ordering version and must be retained")
                .isEqualTo(7L);
        assertThat(cleared.transitionTs()).isEqualTo(EVENT_TS);
        assertThat(cleared.withFenceCleared(EVENT_TS))
                .as("repeated clear with the same ts returns the same instance")
                .isSameAs(cleared);
    }
}
