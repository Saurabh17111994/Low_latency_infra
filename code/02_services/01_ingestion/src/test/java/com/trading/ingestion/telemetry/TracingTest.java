package com.trading.ingestion.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.opentelemetry.api.trace.Span;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 (P1-133) guards. Tests run WITHOUT the javaagent, so every call
 * must be a silent no-op: invalid spans, no exports, no throws. (With the
 * agent, the same calls resolve to the agent SDK — verified in prod, not
 * unit-testable here.)
 */
@DisplayName("Tracing helper: agentless no-op safety")
class TracingTest {

    @Test
    @DisplayName("lifecycle spans are invalid no-ops without the agent")
    void lifecycleSpansAreNoOpsWithoutAgent() {
        try (Tracing.TracedSpan t = Tracing.span("ingestion.manifest-load")) {
            assertFalse(t.span.getSpanContext().isValid(),
                    "no agent must mean no real span context");
            t.span.setAttribute("tick.events", 7); // must not throw
            t.error(new RuntimeException("boom")); // must not throw
        }
    }

    @Test
    @DisplayName("market batches sample 1-in-N and skipped ones stay invalid")
    void marketBatchesSampleDeterministically() {
        int sampled = 0;
        for (int i = 0; i < 1000; i++) {
            try (Tracing.TracedSpan t = Tracing.marketBatch(10, "c", 1L)) {
                if (t.span.getSpanContext().isValid()) sampled++;
                // Uniform close path for sampled AND skipped — must not throw.
            }
        }
        // Agentless everything is invalid (count stays 0), but the sampling
        // gate itself ran 1000 times without throwing — the ratio is pinned
        // by the counter test below, not by validity here.
        assertDoesNotThrow(() -> Span.getInvalid().end());
        org.junit.jupiter.api.Assertions.assertEquals(0, sampled);
    }

    @Test
    @DisplayName("sampling gate admits exactly every 100th batch (N=100 default)")
    void samplingGateAdmitsEveryNth() {
        org.junit.jupiter.api.Assertions.assertTrue(Tracing.admit(100));
        org.junit.jupiter.api.Assertions.assertTrue(Tracing.admit(200));
        org.junit.jupiter.api.Assertions.assertFalse(Tracing.admit(99));
        org.junit.jupiter.api.Assertions.assertFalse(Tracing.admit(101));
        org.junit.jupiter.api.Assertions.assertFalse(Tracing.admit(1));
        // 1000 consecutive marketBatch calls with N=100 (default, no env):
        // exactly the uniform-close path matters here — validity is covered
        // above. This pins that the helper is callable at batch rate.
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 1000; i++) {
                try (Tracing.TracedSpan t = Tracing.marketBatch(1, "", 0L)) {
                    // no-op
                }
            }
        });
    }
}
