package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.OrderLifecycleState;
import com.trading.common.schema.projection.OrderLifecycleSnapshot;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M1-6: the versioned read-evaluate-write gate in front of {@code Order_Lifecycle}. The old path
 * blind-upserted every event; these tests pin the monotonicity contract — stale/duplicate events
 * never write, conflicts/regressions/unrecognized vocabulary quarantine + halt (epoch+1), and a
 * lookup failure refuses the write (503 upstream) instead of falling back to an upsert.
 */
class OrderLifecycleWriterGateTest {

    private static NormalizedExecutionEvent event(String postbackId, String brokerId, String state,
            long version, long cumulative, long pending) {
        return new NormalizedExecutionEvent(postbackId, "acc-1", "part-1", 7L, "nautilus",
                "LIFECYCLE", 1_700_000_000_000L, null, null,
                new NormalizedExecutionEvent.Lifecycle(brokerId, "instr-1", "att-1", "tc-1", state,
                        cumulative, pending, null, version, 1_700_000_000_000L,
                        1_700_000_000_000L, "VERIFIED"),
                null, null);
    }

    private static OrderLifecycleSnapshot snapshot(String sourceEventId, String state, long version,
            long cumulative, long pending) {
        return new OrderLifecycleSnapshot("acc-1", "BRK-1", "instr-1", "att-1", "tc-1",
                OrderLifecycleState.valueOf(state), cumulative, pending, 0L, sourceEventId, version,
                1_700_000_000_000L, 1_700_000_000_000L, "CORRELATED", "2");
    }

    /** Recording sinks + a swappable stored-row lookup; no Fluss, no clock. */
    private static final class Probe {
        final List<OrderLifecycleSnapshot> upserts = new ArrayList<>();
        final List<String> stales = new ArrayList<>();
        final List<String> quarantines = new ArrayList<>();
        final List<String> halts = new ArrayList<>();
        OrderLifecycleWriteGate.StoredRowLookup lookup = (scope, broker) -> null;

        OrderLifecycleWriteGate gate() {
            return new OrderLifecycleWriteGate(lookup,
                    (partition, reason, evidence) -> halts.add(partition + "|" + reason),
                    upserts::add,
                    (event, detail) -> stales.add(detail),
                    (event, reason) -> quarantines.add(reason),
                    () -> 1_700_000_000_999L);
        }
    }

    @Test
    void applies_the_first_event_via_the_projector_and_upserts_its_snapshot() throws Exception {
        Probe probe = new Probe();
        probe.gate().apply(event("pb-1", "BRK-1", "ACCEPTED", 100L, 5L, 0L));

        assertThat(probe.upserts).hasSize(1);
        assertThat(probe.upserts.get(0).normalizedState()).isEqualTo(OrderLifecycleState.PENDING);
        assertThat(probe.upserts.get(0).sourceVersion()).isEqualTo(100L);
        assertThat(probe.quarantines).isEmpty();
        assertThat(probe.halts).isEmpty();
    }

    @Test
    void stale_ack_after_canceled_is_ignored_and_audited_never_written() throws Exception {
        Probe probe = new Probe();
        // A cancel ack at version 2000 already landed; a replayed pre-cancel ack at 1000 arrives.
        probe.lookup = (scope, broker) -> snapshot("pb-cancel", "CANCELLED", 2000L, 5L, 0L);
        probe.gate().apply(event("pb-1", "BRK-1", "FILLED", 1000L, 5L, 0L));

        assertThat(probe.upserts).isEmpty();
        assertThat(probe.stales).hasSize(1);
        assertThat(probe.quarantines).isEmpty();
        assertThat(probe.halts).isEmpty();
    }

    @Test
    void same_version_different_content_quarantines_and_halts() throws Exception {
        Probe probe = new Probe();
        probe.lookup = (scope, broker) -> snapshot("pb-other", "PENDING", 500L, 5L, 0L);
        probe.gate().apply(event("pb-1", "BRK-1", "PARTIAL", 500L, 10L, 0L));

        assertThat(probe.upserts).isEmpty();
        assertThat(probe.quarantines).hasSize(1);
        assertThat(probe.quarantines.get(0)).contains("CONFLICT");
        assertThat(probe.halts).hasSize(1);
        assertThat(probe.halts.get(0)).startsWith("part-1|");
    }

    @Test
    void terminal_regression_quarantines_and_halts() throws Exception {
        Probe probe = new Probe();
        probe.lookup = (scope, broker) -> snapshot("pb-cancel", "CANCELLED", 100L, 5L, 0L);
        // A newer version cannot un-cancel: CANCELLED is terminal.
        probe.gate().apply(event("pb-2", "BRK-1", "PENDING", 200L, 5L, 5L));

        assertThat(probe.upserts).isEmpty();
        assertThat(probe.quarantines).hasSize(1);
        assertThat(probe.quarantines.get(0)).contains("REGRESSION");
        assertThat(probe.halts).hasSize(1);
    }

    @Test
    void exact_duplicate_replay_has_no_effect() throws Exception {
        Probe probe = new Probe();
        probe.lookup = (scope, broker) -> snapshot("pb-1", "PENDING", 100L, 5L, 0L);
        probe.gate().apply(event("pb-1", "BRK-1", "PENDING", 100L, 5L, 0L));

        assertThat(probe.upserts).isEmpty();
        assertThat(probe.stales).isEmpty();
        assertThat(probe.quarantines).isEmpty();
        assertThat(probe.halts).isEmpty();
    }

    @Test
    void lookup_failure_refuses_the_write_and_propagates() {
        Probe probe = new Probe();
        probe.lookup = (scope, broker) -> {
            throw new OrderLifecycleLookupUnavailableException("fluss down", null);
        };

        assertThatThrownBy(() -> probe.gate().apply(event("pb-1", "BRK-1", "ACCEPTED", 100L, 5L, 0L)))
                .isInstanceOf(OrderLifecycleLookupUnavailableException.class);
        assertThat(probe.upserts).isEmpty();
        assertThat(probe.quarantines).isEmpty();
        assertThat(probe.halts).isEmpty();
    }

    @Test
    void unrecognized_state_quarantines_and_halts_on_vocabulary_drift() throws Exception {
        Probe probe = new Probe();
        probe.gate().apply(event("pb-1", "BRK-1", "BOGUS_STATE", 100L, 5L, 0L));

        assertThat(probe.upserts).isEmpty();
        assertThat(probe.quarantines).hasSize(1);
        assertThat(probe.quarantines.get(0)).contains("unrecognized lifecycle state");
        assertThat(probe.halts).hasSize(1);
    }

    @Test
    void canonical_state_vocabulary_is_pinned() {
        assertThat(OrderLifecycleWriteGate.canonicalState("SUBMITTING"))
                .isEqualTo(OrderLifecycleState.SUBMITTING);
        assertThat(OrderLifecycleWriteGate.canonicalState("ACCEPTED"))
                .isEqualTo(OrderLifecycleState.PENDING);
        assertThat(OrderLifecycleWriteGate.canonicalState("NEW"))
                .isEqualTo(OrderLifecycleState.PENDING);
        assertThat(OrderLifecycleWriteGate.canonicalState("partially_filled"))
                .isEqualTo(OrderLifecycleState.PARTIAL);
        assertThat(OrderLifecycleWriteGate.canonicalState("COMPLETE"))
                .isEqualTo(OrderLifecycleState.FILLED);
        assertThat(OrderLifecycleWriteGate.canonicalState("CANCELED"))
                .isEqualTo(OrderLifecycleState.CANCELLED);
        assertThat(OrderLifecycleWriteGate.canonicalState("CANCELLED"))
                .isEqualTo(OrderLifecycleState.CANCELLED);
        assertThat(OrderLifecycleWriteGate.canonicalState("REJECTED"))
                .isEqualTo(OrderLifecycleState.REJECTED);
        assertThat(OrderLifecycleWriteGate.canonicalState("UNKNOWN"))
                .isEqualTo(OrderLifecycleState.UNKNOWN);
        assertThat(OrderLifecycleWriteGate.canonicalState("SOMETHING_ELSE")).isNull();
        assertThat(OrderLifecycleWriteGate.canonicalState(null)).isNull();
    }

    @Test
    void one_stable_stripe_per_composite_key() {
        OrderLifecycleWriteGate gate = new Probe().gate();
        assertThat(gate.stripeFor("acc-1", "BRK-1"))
                .isSameAs(gate.stripeFor("acc-1", "BRK-1"));
        assertThat(OrderLifecycleWriteGate.STRIPE_COUNT).isEqualTo(64);
    }

    @Test
    void lookup_failure_maps_to_503_through_the_projection_wrapper() {
        Exception direct = new OrderLifecycleLookupUnavailableException("fluss down", null);
        Exception wrapped = new IllegalStateException("wrapped by applyProjection", direct);

        assertThat(GatewayHttpServer.projectionFailureStatus(direct)).isEqualTo(503);
        assertThat(GatewayHttpServer.projectionFailureStatus(wrapped)).isEqualTo(503);
        assertThat(GatewayHttpServer.projectionFailureStatus(new IllegalStateException("boom")))
                .isEqualTo(500);
    }
}
