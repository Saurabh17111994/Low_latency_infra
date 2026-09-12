package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A budget that nothing sets is not a budget. Every case in {@link RequestBudgetTest} still passes
 * if the HTTP handler forgets {@link RequestBudget#begin}/{@link RequestBudget#clear} - the stores
 * would simply fall back to their own per-site budget, and {@code /v1/events} would quietly return
 * to the 72.4s hold that C2 exists to remove. This is the test that fails when that happens.
 */
class RequestBudgetWiringTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String SECRET = "secret1234567890123456";

    private GatewayHttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    private static GatewayConfig config() {
        return new GatewayConfig(
                "localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 0, "http://127.0.0.1:9190/v1/intents",
                "execution-gateway.v1", SECRET,
                Duration.ofMillis(2000), Duration.ofMillis(250), "acct1", "p1",
                /* executionEnabled */ true, /* maxPendingProjectionRecords */ 16,
                GatewayConfig.defaultRequestBudget(Duration.ofMillis(2000)));
    }

    @Test
    void eventsHandlerRunsTheConsumerInsideTheRequestBudget() throws Exception {
        GatewayConfig cfg = config();
        GatewayReadiness readiness = new GatewayReadiness();
        readiness.fluss(true, "ok");
        readiness.protocol(true, "ok");
        readiness.durableWrites(true, "ok");

        AtomicLong remainingInsideConsumer = new AtomicLong(-1);
        Consumer<JsonNode> observingConsumer =
                payload -> remainingInsideConsumer.set(RequestBudget.remainingMillis());

        server = new GatewayHttpServer(cfg, readiness, observingConsumer, new InMemoryGateStateStore(),
                Executors.newFixedThreadPool(2), true);
        server.start();
        String base = "http://127.0.0.1:" + port(server);

        JsonNode payload = M.readTree("{\"postback_event_id\":\"pb-budget-wiring\"}");
        GatewayProtocol proto = new GatewayProtocol(SECRET);
        String envelope = proto.encode(new GatewayProtocol.Envelope(
                "execution-gateway.v1", "EXECUTION_EVENT", "req-budget-wiring", "acct1", "p1",
                GatewayProtocol.sha256(M.writeValueAsBytes(payload)), 1L, "fence-1",
                System.currentTimeMillis() + 60_000, payload, null));

        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(base + "/v1/events"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(envelope)).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(202, response.statusCode(), "expected the apply to be accepted: " + response.body());
        long remaining = remainingInsideConsumer.get();
        assertTrue(remaining > 0 && remaining <= cfg.requestBudget().toMillis(),
                "the consumer must run inside the configured budget (" + cfg.requestBudget().toMillis()
                        + "ms); saw " + remaining);
        assertEquals(Long.MAX_VALUE, RequestBudget.remainingMillis(),
                "the budget must be cleared after the request - this thread serves the next one");
    }

    private static int port(GatewayHttpServer s) throws Exception {
        var field = s.getClass().getDeclaredField("server");
        field.setAccessible(true);
        com.sun.net.httpserver.HttpServer http = (com.sun.net.httpserver.HttpServer) field.get(s);
        return http.getAddress().getPort();
    }
}
