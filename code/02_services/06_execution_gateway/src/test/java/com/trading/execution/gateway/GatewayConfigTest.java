package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GatewayConfigTest {
    static Map<String, String> values() {
        Map<String, String> m = new HashMap<>();
        m.put("FLUSS_BOOTSTRAP", "fluss:9123"); m.put("FLUSS_DATABASE", "default");
        m.put("EXECUTION_INTENT_TABLE", "Execution_Intent"); m.put("EXECUTION_GATE_TABLE", "Execution_Gate");
        m.put("EXECUTION_ATTEMPTS_TABLE", "Execution_Attempts"); m.put("ORDER_CORRELATION_TABLE", "Order_Correlation");
        m.put("PROJECTION_LEDGER_TABLE", "Postback_Projection_Ledger"); m.put("SAFETY_HALT_TABLE", "Safety_Halt_Requests");
        m.put("GATEWAY_BIND_HOST", "127.0.0.1"); m.put("GATEWAY_BIND_PORT", "9180");
        m.put("NAUTILUS_PRIVATE_ENDPOINT", "http://127.0.0.1:9190/v1/intents");
        m.put("GATEWAY_PROTOCOL_VERSION", "execution-gateway.v1"); m.put("GATEWAY_SHARED_SECRET", "private");
        m.put("GATEWAY_REQUEST_TIMEOUT_MS", "2000"); m.put("GATEWAY_POLL_TIMEOUT_MS", "250");
        m.put("ACCOUNT_SCOPE_ID", "acct"); m.put("EXECUTION_PARTITION_ID", "part");
        return m;
    }
    @Test void acceptsOnlyPrivateFlussConfiguration() {
        GatewayConfig c = GatewayConfig.from(values());
        assertThat(c.flussBootstrap()).isEqualTo("fluss:9123");
        assertThat(c.bindPort()).isEqualTo(9180);
    }
    @Test void requiredSecretAndScopesCannotBeBlank() {
        Map<String, String> m = values(); m.put("GATEWAY_SHARED_SECRET", "");
        assertThatThrownBy(() -> GatewayConfig.from(m)).hasMessageContaining("GATEWAY_SHARED_SECRET");
    }

    /**
     * C2: the request budget is one site's full retry budget (3 x timeout + backoff), granted to
     * the whole request. Derived, so raising GATEWAY_REQUEST_TIMEOUT_MS cannot leave it behind.
     */
    @Test void requestBudgetIsDerivedFromTheTimeoutWhenUnset() {
        GatewayConfig c = GatewayConfig.from(values());
        assertThat(c.requestBudget()).isEqualTo(Duration.ofMillis(3 * 2000 + 2 * 200));
        Map<String, String> raised = values(); raised.put("GATEWAY_REQUEST_TIMEOUT_MS", "4000");
        assertThat(GatewayConfig.from(raised).requestBudget())
                .isEqualTo(Duration.ofMillis(3 * 4000 + 2 * 200));
    }

    @Test void explicitRequestBudgetIsHonoured() {
        Map<String, String> m = values(); m.put("GATEWAY_REQUEST_BUDGET_MS", "3000");
        assertThat(GatewayConfig.from(m).requestBudget()).isEqualTo(Duration.ofMillis(3000));
    }

    /**
     * Below one attempt plus backoff a budget can retry nothing, and at zero it would shed every
     * call after a single attempt with no backoff applied - the "the wait never actually happened"
     * shape from P3-023. Failing startup is the only safe response; the message names both numbers
     * so an operator can see which is wrong.
     */
    @Test void requestBudgetTooSmallToRetryIsRefusedAtStartup() {
        Map<String, String> m = values(); m.put("GATEWAY_REQUEST_BUDGET_MS", "2100");
        assertThatThrownBy(() -> GatewayConfig.from(m))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GATEWAY_REQUEST_BUDGET_MS (2100ms)")
                .hasMessageContaining("2200ms");
    }

    @Test void nonPositiveRequestBudgetIsRefused() {
        Map<String, String> m = values(); m.put("GATEWAY_REQUEST_BUDGET_MS", "0");
        assertThatThrownBy(() -> GatewayConfig.from(m))
                .hasMessageContaining("GATEWAY_REQUEST_BUDGET_MS must be positive");
    }
    /**
     * P3-073: the record's synthesized toString() includes every component, so the gateway's HMAC
     * secret went into any startup dump or error context. The dump must stay useful without it.
     */
    @Test void toStringDoesNotPrintTheSharedSecret() {
        Map<String, String> m = values();
        m.put("GATEWAY_SHARED_SECRET", "s3cr3t-sentinel-9f2a");
        GatewayConfig c = GatewayConfig.from(m);
        assertThat(c.toString()).doesNotContain("s3cr3t-sentinel-9f2a");
        assertThat(c.toString()).contains("fluss:9123").contains("127.0.0.1").contains("***");
    }

}
