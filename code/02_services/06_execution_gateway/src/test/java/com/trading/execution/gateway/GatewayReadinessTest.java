package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class GatewayReadinessTest {
    @Test void startsNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        assertThat(r.snapshot().executionReady()).isFalse();
        assertThat(r.snapshot().reason()).isEqualTo("starting");
    }
    @Test void executionReadyOnlyWhenAllDimensionsReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "fluss ok");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.protocol(true, "protocol ok");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.durableWrites(true, "durable ok");
        assertThat(r.snapshot().executionReady()).isTrue();
    }
    @Test void anyDimensionFalseMakesNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
        r.fluss(false, "fluss down");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.fluss(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
        r.protocol(false, "protocol mismatch");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.protocol(true, "ok");
        r.durableWrites(false, "backlog");
        assertThat(r.snapshot().executionReady()).isFalse();
    }
    @Test void failClosesAll() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
        r.fail("changelog gap");
        GatewayReadiness.Snapshot s = r.snapshot();
        assertThat(s.healthy()).isFalse();
        assertThat(s.executionReady()).isFalse();
        assertThat(s.reason()).isEqualTo("changelog gap");
    }
    @Test void healthyNeverImpliesExecutionReady() {
        GatewayReadiness r = new GatewayReadiness();
        // healthy is true at start but executionReady must be false until all dims ready
        assertThat(r.snapshot().healthy()).isTrue();
        assertThat(r.snapshot().executionReady()).isFalse();
    }
    @Test void projectionBacklogAbovePolicyMakesNotReady() {
        // MAX_PENDING_PROJECTION_RECORDS policy: backlog > max -> durableWrites false
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok");
        int maxPending = 1000;
        int pending = 1001;
        boolean backlogOk = pending <= maxPending;
        r.durableWrites(backlogOk, backlogOk ? "ok" : "projection backlog " + pending + " > " + maxPending);
        assertThat(r.snapshot().executionReady()).isFalse();
        assertThat(r.snapshot().reason()).contains("backlog");
        // within limit -> ready
        r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
    }
    @Test void bridgeDisconnectMakesNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(false, "bridge disconnected"); r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isFalse();
    }
    @Test void clockViolationMakesNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(false, "clock offset 5s > 2s");
        assertThat(r.snapshot().executionReady()).isFalse();
        assertThat(r.snapshot().reason()).contains("clock");
    }

    // --- Latch policy: fail() is one-way until restart (P3-081/P3-082) -------------------------

    @Test void failLatchesAndLaterDimensionUpdatesCannotResurrectFlags() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(true, "ok");
        r.fail("poison row");
        // P3-082: these three used to be able to PARTIALLY resurrect the snapshot after a fail,
        // because fail() was a bare set() racing the updaters' updateAndGet — leaving e.g.
        // healthy=false with flussReady=true, or clobbering the fail reason.
        r.fluss(true, "late fluss");
        r.protocol(true, "late protocol");
        r.durableWrites(true, "late durable");
        GatewayReadiness.Snapshot s = r.snapshot();
        assertThat(s.healthy()).isFalse();
        assertThat(s.flussReady()).isFalse();
        assertThat(s.protocolReady()).isFalse();
        assertThat(s.durableWriteReady()).isFalse();
        assertThat(s.executionReady()).isFalse();
        assertThat(s.reason()).isEqualTo("poison row");
    }

    @Test void failPreservesTheFirstFailureReason() {
        // The reader re-invokes the violation handler for every bad record it meets, so without
        // this the operator's diagnosis would be whatever the LAST repeat said.
        GatewayReadiness r = new GatewayReadiness();
        r.fail("first cause");
        r.fail("second cause");
        assertThat(r.snapshot().reason()).isEqualTo("first cause");
    }

    @Test void restoreIfDrainedCannotClearALatchedFail() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(true, "ok");
        r.fail("latched");
        r.restoreIfDrained(() -> 0, "projection backlog drained");
        assertThat(r.snapshot().durableWriteReady()).isFalse();
        assertThat(r.snapshot().reason()).isEqualTo("latched");
    }

    @Test void snapshotReasonIsNeverNullOrBlank() {
        // P3-300: e.getMessage() is null for plenty of exceptions, and the reason is served by
        // /readyz and matched by operators.
        assertThat(new GatewayReadiness.Snapshot(true, true, true, true, null).reason())
                .isEqualTo("unknown");
        assertThat(new GatewayReadiness.Snapshot(true, true, true, true, "   ").reason())
                .isEqualTo("unknown");
    }

    // --- One shared reason slot must still name the real blocker (P3-301) ------------------------

    @Test void aReadyDimensionDoesNotMaskAnotherDimensionsCause() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(false, "fluss down");
        r.protocol(true, "private protocol configured");
        GatewayReadiness.Snapshot s = r.snapshot();
        assertThat(s.flussReady()).isFalse();
        assertThat(s.protocolReady()).isTrue();
        assertThat(s.reason())
                .as("an unrelated success must not become the visible reason")
                .isEqualTo("fluss down");
    }

    @Test void theAllClearAdoptsTheFinalReason() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(false, "fluss down");
        r.protocol(true, "protocol ok");
        r.durableWrites(true, "durable ok");
        assertThat(r.snapshot().executionReady()).isFalse();
        assertThat(r.snapshot().reason()).isEqualTo("fluss down");
        // Completing readiness is the all-clear, where the last success is the honest summary.
        r.fluss(true, "fluss recovered");
        assertThat(r.snapshot().executionReady()).isTrue();
        assertThat(r.snapshot().reason()).isEqualTo("fluss recovered");
    }
}
