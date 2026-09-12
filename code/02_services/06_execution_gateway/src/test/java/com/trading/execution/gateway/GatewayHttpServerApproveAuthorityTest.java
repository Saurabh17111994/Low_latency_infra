package com.trading.execution.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Readiness honesty: {@code /control/approve} must never report success it did not achieve.
 *
 * <p>The defect this pins: the 3-arg form is the PRODUCTION wiring (used by
 * {@code ExecutionGatewayMain}), and it builds a per-process {@link InMemoryGateStateStore}
 * placeholder. Approve answered {@code 200 APPLIED} against that map while the durable gate
 * the events path actually consults was untouched — the most dangerous available answer,
 * because an operator reads it as "the gate is now open for business" and stops watching.
 *
 * <p>These tests exist because the fix was implemented without being falsified: a guard that
 * is asserted but unverified is exactly the class of defect being fixed here.
 */
class GatewayHttpServerApproveAuthorityTest {

    private static final String BEARER = "Bearer secret1234567890123456";

    /** Mirrors the production GatewayConfig shape, including executionEnabled=true. */
    private static GatewayConfig config() {
        return new GatewayConfig("localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 0,
                "http://127.0.0.1:9190/v1/intents", "execution-gateway.v1",
                "secret1234567890123456", Duration.ofMillis(2000), Duration.ofMillis(250),
                "acct1", "p1", true);
    }

    private static int port(GatewayHttpServer s) throws Exception {
        var f = s.getClass().getDeclaredField("server");
        f.setAccessible(true);
        return ((com.sun.net.httpserver.HttpServer) f.get(s)).getAddress().getPort();
    }

    /**
     * A FULLY VALID, correctly authenticated approve request — every field the handler
     * requires, including {@code executionPartitionId}. Deliberately complete: an earlier
     * version omitted that field, so a falsified run returned {@code 400} (validation) rather
     * than the {@code 200 APPLIED} the test exists to rule out. The point is to prove a
     * request that WOULD have succeeded is refused, not merely that malformed input is.
     */
    private static HttpResponse<String> postApprove(GatewayHttpServer s) throws Exception {
        var req = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port(s) + "/control/approve"))
                .header("Content-Type", "application/json")
                .header("Authorization", BEARER)
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"principal\":\"saurabh\",\"epoch\":1,\"evidenceHash\":\"h1\","
                                + "\"executionPartitionId\":\"p1\"}"))
                .build();
        return HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("production form refuses approve rather than answering 200 against a placeholder")
    void approveFailsClosedWithoutDurableGateStore() throws Exception {
        try (GatewayHttpServer prod =
                new GatewayHttpServer(config(), new GatewayReadiness(), n -> {})) {
            prod.start();
            var res = postApprove(prod);

            assertEquals(501, res.statusCode(),
                    "approve must not report success without a durable gate store: " + res.body());
            // The success token must not appear at all — a refusal that still says APPLIED
            // would leave the original confusion intact.
            assertFalse(res.body().contains("APPLIED"),
                    "a refusal must not carry the success token: " + res.body());
        }
    }

    @Test
    @DisplayName("a wired gate store is not refused — the fail-closed is scoped, not blanket")
    void approveIsNotRefusedWhenStoreIsDeclaredAuthoritative() throws Exception {
        var gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.init(new GateRow("p1", "acct1", GateState.HALTED, 0, "boot", "h0",
                null, null, null, null, 0L, null, null, null));

        try (GatewayHttpServer wired =
                new GatewayHttpServer(config(), new GatewayReadiness(), n -> {}, gates, true)) {
            wired.start();
            // Exactly what is asserted is that the authority gate does not fire here; the
            // approve outcome itself is the existing suite's business. Without this, a
            // blanket 501 would pass the test above while breaking the feature entirely.
            assertNotEquals(501, postApprove(wired).statusCode(),
                    "a declared-authoritative store must not hit the missing-authority refusal");
        }
    }
}
