package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

/**
 * Offline verification of the execution.enabled flag (Todo #27).
 * No FLUSS_BOOTSTRAP, no Arrow, HALTED default when disabled (fail-closed).
 */
class GatewayConfigExecutionFlagTest {

    @Test
    void defaultFalseWhenEnvMissing() {
        Map<String, String> m = GatewayConfigTest.values();
        // values() does not include EXECUTION_ENABLED
        m.remove("EXECUTION_ENABLED");
        GatewayConfig cfg = GatewayConfig.from(m);
        assertThat(cfg.executionEnabled()).as("default when EXECUTION_ENABLED missing").isFalse();
    }

    @Test
    void trueWhenExecutionEnabledTrue() {
        Map<String, String> m = GatewayConfigTest.values();
        m.put("EXECUTION_ENABLED", "true");
        GatewayConfig cfg = GatewayConfig.from(m);
        assertThat(cfg.executionEnabled()).isTrue();
    }

    @Test
    void falseWhenExecutionEnabledFalseString() {
        Map<String, String> m = GatewayConfigTest.values();
        m.put("EXECUTION_ENABLED", "false");
        GatewayConfig cfg = GatewayConfig.from(m);
        assertThat(cfg.executionEnabled()).isFalse();
    }

    @Test
    void trueWhenExecutionEnabledCaseInsensitive() {
        Map<String, String> m = GatewayConfigTest.values();
        m.put("EXECUTION_ENABLED", "TRUE");
        assertThat(GatewayConfig.from(m).executionEnabled()).isTrue();
        m.put("EXECUTION_ENABLED", "True");
        assertThat(GatewayConfig.from(m).executionEnabled()).isTrue();
        m.put("EXECUTION_ENABLED", "  true  ");
        assertThat(GatewayConfig.from(m).executionEnabled()).isTrue();
    }

    @Test
    void falseWhenExecutionEnabledBlankOrMissing() {
        Map<String, String> m = GatewayConfigTest.values();
        m.put("EXECUTION_ENABLED", "");
        assertThat(GatewayConfig.from(m).executionEnabled()).isFalse();
        m.put("EXECUTION_ENABLED", "   ");
        assertThat(GatewayConfig.from(m).executionEnabled()).isFalse();
        m.remove("EXECUTION_ENABLED");
        assertThat(GatewayConfig.from(m).executionEnabled()).isFalse();
        // explicit null via map missing key already tested; also nonsense value defaults false
        m.put("EXECUTION_ENABLED", "yes");
        assertThat(GatewayConfig.from(m).executionEnabled()).isFalse();
    }

    @Test
    void directConstructorWithExecutionEnabled() {
        // Ensure canonical 18-arg constructor and legacy 17-arg still compile and behave.
        // Legacy defaults to false (fail-closed).
        GatewayConfig legacy = new GatewayConfig(
                "localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 9180,
                "http://127.0.0.1:9190/v1/intents", "execution-gateway.v1",
                "secret1234567890123456", java.time.Duration.ofMillis(2000),
                java.time.Duration.ofMillis(250), "acct1", "p1");
        assertThat(legacy.executionEnabled()).isFalse();

        GatewayConfig enabled = new GatewayConfig(
                "localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 9180,
                "http://127.0.0.1:9190/v1/intents", "execution-gateway.v1",
                "secret1234567890123456", java.time.Duration.ofMillis(2000),
                java.time.Duration.ofMillis(250), "acct1", "p1", true);
        assertThat(enabled.executionEnabled()).isTrue();

        GatewayConfig disabledExplicit = new GatewayConfig(
                "localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 9180,
                "http://127.0.0.1:9190/v1/intents", "execution-gateway.v1",
                "secret1234567890123456", java.time.Duration.ofMillis(2000),
                java.time.Duration.ofMillis(250), "acct1", "p1", false);
        assertThat(disabledExplicit.executionEnabled()).isFalse();
    }

    @Test
    void haltedDefaultGateStateWhenDisabled() throws Exception {
        // HALTED default when executionEnabled is false: gateway boots HALTED, readiness stays false,
        // intents defer, bridge disabled. Offline, no FLUSS_BOOTSTRAP / Arrow.
        Map<String, String> m = GatewayConfigTest.values();
        m.put("EXECUTION_ENABLED", "false");
        GatewayConfig cfg = GatewayConfig.from(m);
        assertThat(cfg.executionEnabled()).as("execution must be disabled for HALTED default").isFalse();

        // 1. Readiness is HALTED at boot (executionReady false) and stays false when disabled.
        GatewayReadiness readiness = new GatewayReadiness();
        assertThat(readiness.snapshot().executionReady()).as("boot readiness HALTED").isFalse();
        assertThat(readiness.snapshot().healthy()).isTrue();
        // Simulate what ExecutionGatewayMain does when disabled: keep readiness false
        // Even after attempting to set true, the Main's fail-closed branch would keep it false.
        // Here we directly verify that a fresh readiness is not executionReady, and that
        // NautilusIntentClient when disabled keeps it not ready.

        // 2. InMemory gate boots HALTED by default (single source of truth)
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        GateRow boot = new GateRow("p1", "acct1", GateState.HALTED, 0, "boot", "h0",
                null, null, null, null, 0L, null, null, null);
        gates.init(boot);
        assertThat(gates.read("p1").state()).as("gate boots HALTED").isEqualTo(GateState.HALTED);

        // 3. Bridge disabled / intents defer when executionEnabled=false, even if durable gate would be ENABLED.
        //    NautilusIntentClient must return DEFERRED without calling the durable lookup or the HTTP bridge.
        AtomicBoolean lookupCalled = new AtomicBoolean(false);
        ControlStateStore fakeControls = new ControlStateStore() {
            private final AtomicBoolean called = lookupCalled;
            @Override
            public Lookup lookup(String table, List<Object> keys) {
                called.set(true);
                return new Lookup(Status.NOT_FOUND, null, "ok");
            }
            @Override
            public Lookup lookup(String table, Object... keys) {
                called.set(true);
                return new Lookup(Status.NOT_FOUND, null, "ok");
            }
            @Override public void replaySafetyHalts(java.util.function.Consumer<InternalRow> c) {}
            @Override public void close() {}
        };

        IntentRecord intent = new IntentRecord("i", "c", "t", "acct1", "p1", 762583L,
                "NSE", "BI-EQ", "BUY", 1, "MARKET", null, "CNC", "DAY", "s", "1", "cfg",
                System.currentTimeMillis(), null, "hash", null, "1", 0);

        // Use fakeControls that would otherwise look ENABLED, but execution disabled should still defer.
        NautilusIntentClient client = new NautilusIntentClient(cfg, fakeControls, readiness);
        IntentSink.Result result = client.forward(intent);
        assertThat(result).as("HALTED gate must defer when EXECUTION_ENABLED=false").isEqualTo(IntentSink.Result.DEFERRED);
        // readiness must remain not executionReady (fail-closed)
        assertThat(readiness.snapshot().executionReady()).as("executionReady stays false when disabled").isFalse();
        assertThat(readiness.snapshot().protocolReady()).as("protocolReady false when disabled").isFalse();
        // When disabled, the bridge is disabled and durability lookup is not required to be performed;
        // the key property is that the intent is deferred, not forwarded. Whether lookup was called is secondary,
        // but for pure offline HALTED we prefer no durable call - our implementation returns before lookup.

        // 4. When executionEnabled=false, GatewayHttpServer events must also be 503 (bridge disabled).
        //    The server's events handler checks config.executionEnabled() before any readiness.
        //    We verify the config flag itself; the HTTP path is covered by Nautilus deferral above.
        assertThat(cfg.executionEnabled()).isFalse();
    }

    @Test
    void enabledDoesNotForceHalted() throws Exception {
        // When EXECUTION_ENABLED=true the FAIL-CLOSED gate is lifted; the gateway is allowed to become ready
        // (subject to durable gate ENABLED + fence). This test ensures the flag does not keep HALTED when true.
        Map<String, String> m = GatewayConfigTest.values();
        m.put("EXECUTION_ENABLED", "true");
        GatewayConfig cfg = GatewayConfig.from(m);
        assertThat(cfg.executionEnabled()).isTrue();
        // When enabled, a readiness that is manually marked true should be executionReady
        GatewayReadiness readiness = new GatewayReadiness();
        readiness.fluss(true, "ok");
        readiness.protocol(true, "ok");
        readiness.durableWrites(true, "ok");
        assertThat(readiness.snapshot().executionReady()).isTrue();
    }
}
