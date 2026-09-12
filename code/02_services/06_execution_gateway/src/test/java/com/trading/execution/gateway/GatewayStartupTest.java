package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * G1 characterization pin for {@link GatewayStartup#applyStartupReadiness} — the startup readiness
 * wiring captured as it behaves TODAY, before the Stage-3 fixes change it.
 *
 * <p>Some of these assertions deliberately pin behaviour that is filed as a defect. That is the
 * point: the fix then has to change this file, so the change is deliberate and reviewable instead
 * of silent drift. Each such assertion names its finding.
 */
class GatewayStartupTest {

    @Test
    void disabledExecutionLeavesEveryDimensionFalseAndFailClosed() {
        GatewayReadiness readiness = new GatewayReadiness();

        GatewayStartup.applyStartupReadiness(readiness, false);

        GatewayReadiness.Snapshot s = readiness.snapshot();
        // `healthy` is untouched by the disabled branch — executionReady() is false because the
        // three dimensions are false, not because the gateway is unhealthy.
        assertThat(s.healthy()).isTrue();
        assertThat(s.flussReady()).isFalse();
        assertThat(s.protocolReady()).isFalse();
        assertThat(s.durableWriteReady()).isFalse();
        assertThat(s.executionReady()).isFalse();
        assertThat(s.reason()).isEqualTo("execution disabled via EXECUTION_ENABLED=false");
    }

    @Test
    void enabledExecutionSetsEveryDimensionReady() {
        GatewayReadiness readiness = new GatewayReadiness();

        GatewayStartup.applyStartupReadiness(readiness, true);

        GatewayReadiness.Snapshot s = readiness.snapshot();
        assertThat(s.healthy()).isTrue();
        assertThat(s.flussReady()).isTrue();
        assertThat(s.protocolReady()).isTrue();
        assertThat(s.durableWriteReady()).isTrue();
        assertThat(s.executionReady()).isTrue();
    }

    @Test
    void enabledBranchLosesTheFlussTablesReasonToTheGateStoresReason() {
        // P3-479 pinned: the enabled branch calls fluss(true, ...) TWICE, and a dimension update
        // simply replaces the shared reason. The operator therefore never sees the distinct
        // "Fluss tables opened" state. When P3-479 is fixed this assertion must change — that is
        // exactly what pinning it is for.
        GatewayReadiness readiness = new GatewayReadiness();

        GatewayStartup.applyStartupReadiness(readiness, true);

        assertThat(readiness.snapshot().reason())
                .isEqualTo("Execution_Gate / Execution_Attempts stores opened (WP-3)")
                .isNotEqualTo("Fluss tables opened");
    }

    @Test
    void disabledBranchCannotClearAPriorFailButClobbersItsReason() {
        // Characterization of two things at once:
        //   - the disabled branch writes dimension flags while PRESERVING `healthy`, so it can
        //     neither clear nor set a fail() latch (the latch semantics are P3-081/P3-082);
        //   - it does overwrite the stored reason, so the original failure cause is lost — the
        //     shared-reason problem filed as P3-301.
        GatewayReadiness readiness = new GatewayReadiness();
        readiness.fail("poison row");

        GatewayStartup.applyStartupReadiness(readiness, false);

        GatewayReadiness.Snapshot s = readiness.snapshot();
        assertThat(s.healthy()).isFalse();
        assertThat(s.executionReady()).isFalse();
        assertThat(s.reason())
                .as("the fail reason is clobbered by a dimension update (P3-301)")
                .isEqualTo("execution disabled via EXECUTION_ENABLED=false");
    }
}
