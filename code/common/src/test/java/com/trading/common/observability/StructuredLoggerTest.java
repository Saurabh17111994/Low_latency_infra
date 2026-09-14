package com.trading.common.observability;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link StructuredLogger} entry-point guards: an unusable action or context must
 * fail before the MDC is touched, and a correlation id must be splittable back
 * into its four parts.
 */
@DisplayName("Structured logger guards")
class StructuredLoggerTest {

    @Test
    void actionRunsWithAndWithoutContext() {
        AtomicBoolean ran = new AtomicBoolean();
        StructuredLogger.withContext(() -> ran.set(true), null);
        assertThat(ran).isTrue();
        StructuredLogger.withContext(() -> ran.set(true), Map.of("correlation_id", "cid"));
        assertThat(ran).isTrue();
    }

    @Test
    void nullActionIsRejected() {
        assertThrows(NullPointerException.class,
                () -> StructuredLogger.withContext(null, Map.of("k", "v")));
    }

    @Test
    void unusableContextIsRejectedBeforeTheActionRuns() {
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("correlation_id", null);
        Map<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "v");
        Map<String, String> blankKey = new HashMap<>();
        blankKey.put("  ", "v");

        for (Map<String, String> bad : List.of(nullValue, nullKey, blankKey)) {
            AtomicBoolean ran = new AtomicBoolean();
            assertThrows(IllegalArgumentException.class,
                    () -> StructuredLogger.withContext(() -> ran.set(true), bad));
            assertThat(ran).isFalse();
        }
    }

    @Test
    void correlationIdIsFourPartsAndIsValidated() {
        assertThat(StructuredLogger.correlationId("executor", "vm1", "1.0.0", "trace-1"))
                .isEqualTo("executor/vm1/1.0.0/trace-1");
        assertThat(StructuredLogger.correlationId("executor", "vm1", "1.0.0", "trace-1")
                .split("/")).hasSize(4);
        assertThrows(IllegalArgumentException.class,
                () -> StructuredLogger.correlationId("executor", "vm1", "1.0.0", null));
        assertThrows(IllegalArgumentException.class,
                () -> StructuredLogger.correlationId("executor", "vm1", "1.0.0", "  "));
        // An embedded '/' would make the id ambiguous.
        assertThrows(IllegalArgumentException.class,
                () -> StructuredLogger.correlationId("executor/vm2", "vm1", "1.0.0", "trace-1"));
    }
}
