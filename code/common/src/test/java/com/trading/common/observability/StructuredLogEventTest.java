package com.trading.common.observability;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredLogEventTest {

    private StructuredLogEvent sample() {
        return StructuredLogEvent.builder(
                        1_752_539_000L, "INFO", "executor", "order-submit",
                        "execution", "host-1", "vm1", "prod",
                        "executor/vm1/1.0.0/trace-1", "trace-1", "span-1",
                        "order submitted")
                .symbol("RELIANCE-EQ")
                .strategy("candle-momentum")
                .exception("none")
                .build();
    }

    @Test
    void requiredFieldsArePresent() {
        Map<String, String> attrs = sample().toAttributes();
        for (String k : new String[]{
                "timestamp", "level", "service", "component", "subsystem",
                "host", "vm_id", "environment", "correlation_id",
                "trace_id", "span_id", "message"}) {
            assertThat(attrs).containsKey(k);
        }
    }

    @Test
    void optionalFieldsIncludedWhenSet() {
        Map<String, String> attrs = sample().toAttributes();
        assertThat(attrs).containsEntry("symbol", "RELIANCE-EQ");
        assertThat(attrs).containsEntry("strategy", "candle-momentum");
        assertThat(attrs).containsEntry("exception", "none");
        assertThat(attrs).doesNotContainKey("stacktrace");
    }

    @Test
    void correlationAndTraceIdentityCarried() {
        StructuredLogEvent e = sample();
        assertThat(e.correlationId).isEqualTo("executor/vm1/1.0.0/trace-1");
        assertThat(e.traceId).isEqualTo("trace-1");
    }

    @Test
    void unsetOrNegativeTimestampIsRejected() {
        // R-130 lists timestamp as required, but only the 11 String fields were
        // checked: 0 (the default long) or a negative stamp built happily and
        // emitted timeUnixNano "0".
        for (long ts : new long[]{0L, -1L}) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> StructuredLogEvent.builder(ts, "INFO", "svc", "comp", "sub",
                            "host", "vm", "prod", "cid", "trace", "span", "msg").build());
            assertThat(e).hasMessageContaining("timestamp");
        }
    }
}
