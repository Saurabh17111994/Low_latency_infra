package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

class NautilusIntentClientTest {
    @Test void haltedGateDoesNotEmitPrivateCommand() throws Exception {
        GatewayConfig config = GatewayConfig.from(GatewayConfigTest.values());
        GatewayReadiness readiness = new GatewayReadiness();
        ControlStateStore controls = new ControlStateStore() {
            private final InternalRow gate = GenericRow.of(BinaryString.fromString("part"),
                    BinaryString.fromString("acct"), BinaryString.fromString("HALTED"), 4L,
                    BinaryString.fromString("reason"), null, null, null, null, 1L, BinaryString.fromString("2"));
            public Lookup lookup(String table, List<Object> key) { return new Lookup(Status.FOUND, gate, "ok"); }
            public void replaySafetyHalts(java.util.function.Consumer<InternalRow> c) {}
            public void close() {}
        };
        try {
            IntentRecord intent = new IntentRecord("i", "c", "t", "acct", "part", 1,
                    "NSE", "ABC", "BUY", 1, "MARKET", null, "MIS", "DAY", "s", "1", "cfg",
                    1, null, "hash", null, "1", 0);
            assertThat(new NautilusIntentClient(config, controls, readiness).forward(intent))
                    .isEqualTo(IntentSink.Result.DEFERRED);
            assertThat(readiness.snapshot().executionReady()).isFalse();
        } finally { controls.close(); }
    }

    @Test void stateDecodeFailureDefersAndSurfacesReason() throws Exception {
        // P3-314: a row whose STATE column cannot be decoded must still fail closed
        // (DEFERRED) but surface the real failure reason instead of deferring behind
        // the constant "fence token unavailable or expired".
        java.util.Map<String, String> env = GatewayConfigTest.values();
        env.put("EXECUTION_ENABLED", "true");
        GatewayConfig config = GatewayConfig.from(env);
        GatewayReadiness readiness = new GatewayReadiness();
        ControlStateStore controls = new ControlStateStore() {
            private final InternalRow gate = GenericRow.of(BinaryString.fromString("part"),
                    BinaryString.fromString("acct"), 1, 4L,
                    BinaryString.fromString("reason"), null, null, null, null, 1L,
                    BinaryString.fromString("2"));
            public Lookup lookup(String table, List<Object> key) { return new Lookup(Status.FOUND, gate, "ok"); }
            public void replaySafetyHalts(java.util.function.Consumer<InternalRow> c) {}
            public void close() {}
        };
        try {
            IntentRecord intent = new IntentRecord("i", "c", "t", "acct", "part", 1,
                    "NSE", "ABC", "BUY", 1, "MARKET", null, "MIS", "DAY", "s", "1", "cfg",
                    1, null, "hash", null, "1", 0);
            assertThat(new NautilusIntentClient(config, controls, readiness).forward(intent))
                    .isEqualTo(IntentSink.Result.DEFERRED);
            assertThat(readiness.snapshot().reason()).contains("state decode failed");
        } finally { controls.close(); }
    }
}
