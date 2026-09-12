package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * P3-063 regression pin for the projection handler's failure contract in
 * {@link ExecutionGatewayMain}.
 *
 * <p>The defect: the handler reported {@code e.getMessage()} as the readiness reason, which is null
 * for any no-message throwable (an NPE, a no-arg {@code IllegalStateException}). P3-300 normalises
 * a null reason to the literal {@code "unknown"}, so the operator lost the cause entirely, and the
 * rethrown {@code IllegalStateException} carried no message either. The reason must name what
 * failed — a durable-write outage an operator cannot diagnose is a worse outage.
 */
class ExecutionGatewayMainProjectionFailureTest {

    @Test
    void projectionFailureNamesTheCauseInsteadOfReportingUnknown() {
        GatewayReadiness readiness = new GatewayReadiness();
        IllegalStateException cause = new IllegalStateException((String) null);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ExecutionGatewayMain.applyProjection(() -> { throw cause; }, readiness));

        assertThat(thrown.getMessage())
                .as("the wrapper must carry a diagnosable message")
                .isNotNull()
                .contains("IllegalStateException");
        assertThat(thrown.getCause()).isSameAs(cause);
        assertThat(readiness.snapshot().reason())
                .as("'unknown' means the cause was dropped before it reached /readyz")
                .isNotEqualTo("unknown")
                .contains("IllegalStateException");
        assertThat(readiness.snapshot().durableWriteReady())
                .as("a failed projection must poison durable writes until restart")
                .isFalse();
    }
}
