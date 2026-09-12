package com.trading.execution.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Backpressure/projection-backlog flood soak (11-testing-and-release.md §required
 * fault coverage, line 534): flood {@code POST /v1/events} past {@code MAX_PENDING_PROJECTION_RECORDS} with a
 * blocked projection consumer. Expected: in-flight applies beyond the bound
 * shed with 503 and flip {@code durableWrites} readiness false; after the drain,
 * readiness restores and intake resumes 202.
 */
class ProjectionBacklogFloodSoakTest {
    private static final ObjectMapper M = new ObjectMapper();
    private GatewayHttpServer server;

    @AfterEach void tearDown() { if (server != null) server.close(); }

    @Test
    void floodPastMaxPendingShedsFlipsReadinessThenRecovers() throws Exception {
        GatewayConfig cfg = new GatewayConfig(
                "localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 0, "http://127.0.0.1:9190/v1/intents",
                "execution-gateway.v1", "secret1234567890123456",
                Duration.ofMillis(2000), Duration.ofMillis(250), "acct1", "p1",
                /* executionEnabled */ true, /* maxPendingProjectionRecords */ 4,
                GatewayConfig.defaultRequestBudget(Duration.ofMillis(2000)));
        GatewayReadiness readiness = new GatewayReadiness();
        // Main arms these once tables open; the soak isolates durableWrites dynamics.
        readiness.fluss(true, "ok");
        readiness.protocol(true, "ok");
        readiness.durableWrites(true, "ok");

        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        Consumer<JsonNode> blockedConsumer = n -> {
            concurrent.incrementAndGet();
            maxObserved.accumulateAndGet(concurrent.get(), Math::max);
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            concurrent.decrementAndGet();
        };
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.init(new GateRow("p1", "acct1", GateState.HALTED, 0, "boot", "h0",
                null, null, null, null, 0L, null, null, null));
        ExecutorService pool = Executors.newCachedThreadPool();
        server = new GatewayHttpServer(cfg, readiness, blockedConsumer, gates, pool, true);
        server.start();
        int port = serverPort(server);
        String base = "http://127.0.0.1:" + port;

        // Signed EXECUTION_EVENT envelope; payload hash must match canonical bytes.
        GatewayProtocol proto = new GatewayProtocol("secret1234567890123456");
        var payload = M.createObjectNode().put("instruction_id", "i-flood");

        // Flood 16 concurrent posts while every apply blocks on the latch.
        // P3-078: the replay cache keys on (request_id, payload_hash), so each
        // POST mints its own envelope — sharing one signed body 16x would be a
        // verbatim replay (401 "duplicate request"), not backpressure.
        HttpClient client = HttpClient.newHttpClient();
        java.util.concurrent.atomic.AtomicInteger seq = new java.util.concurrent.atomic.AtomicInteger();
        java.util.function.Supplier<String> freshEnvelope = () -> {
            try {
                int i = seq.incrementAndGet();
                return proto.encode(new GatewayProtocol.Envelope(
                        "execution-gateway.v1", "EXECUTION_EVENT", "req-flood-" + i, "acct1", "p1",
                        GatewayProtocol.sha256(M.writeValueAsBytes(payload)), 1L, "fence-1",
                        System.currentTimeMillis() + 60_000, payload, null));
            } catch (Exception e) { throw new RuntimeException(e); }
        };
        List<Future<HttpResponse<String>>> futures = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            futures.add(pool.submit(() -> client.send(HttpRequest.newBuilder(
                            URI.create(base + "/v1/events"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(freshEnvelope.get())).build(),
                    HttpResponse.BodyHandlers.ofString())));
        }
        // Give the flood time to peak, then assert MID-FLOOD: the accepted
        // applies are still blocked in the consumer until release.countDown(),
        // so the readiness assertions below run while the backlog is live.
        Thread.sleep(700);
        assertTrue(!readiness.snapshot().durableWriteReady(),
                "during flood durableWrites readiness must be false; reason="
                        + readiness.snapshot().reason());
        assertTrue(readiness.snapshot().reason().contains("backlog"),
                "reason: " + readiness.snapshot().reason());

        // Drain: release the blocked applies, then collect every response.
        release.countDown();
        List<HttpResponse<String>> responses = new ArrayList<>();
        for (var f : futures) responses.add(f.get(15, TimeUnit.SECONDS));

        long accepted = responses.stream().filter(r -> r.statusCode() == 202).count();
        long shed = responses.stream().filter(r -> r.statusCode() == 503).count();
        long rejected401 = responses.stream().filter(r -> r.statusCode() == 401).count();
        System.out.println("[soak] statuses: 202=" + accepted + " 503=" + shed
                + " 401=" + rejected401
                + " other=" + (responses.size() - accepted - shed - rejected401)
                + " maxObservedConcurrency=" + maxObserved.get()
                + " durableWriteReady=" + readiness.snapshot().durableWriteReady()
                + " reason=" + readiness.snapshot().reason());
        // Bound is 4: at most 4 in flight can be accepted while blocked.
        assertTrue(accepted <= 4, "accepted=" + accepted + " shed=" + shed);
        assertTrue(shed > 0, "flood past MAX_PENDING must shed load (503)");
        // Full drain: all blocked applies finished, so readiness must restore.
        assertTrue(readiness.snapshot().durableWriteReady(),
                "after drain readiness must restore; reason="
                        + readiness.snapshot().reason());

        // Intake resumes: a fresh post is accepted again.
        var resumed = client.send(HttpRequest.newBuilder(URI.create(base + "/v1/events"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(freshEnvelope.get())).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(202, resumed.statusCode(), resumed.body());
    }

    /**
     * P3-063: a projection failure must not be cleared by the drain. The finally in
     * {@code GatewayHttpServer.events} used to restore durableWrites unconditionally, so the very
     * request that failed — in-flight back to 0 in that same finally — flipped the dimension back
     * to ready milliseconds later: /readyz hid the failure and a persistent failure flapped the
     * flag on every request. Only the shed path's own false is the drain's to undo.
     */
    @Test
    void consumerFailureIsNotClearedByTheDrain() throws Exception {
        GatewayConfig cfg = new GatewayConfig(
                "localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 0, "http://127.0.0.1:9190/v1/intents",
                "execution-gateway.v1", "secret1234567890123456",
                Duration.ofMillis(2000), Duration.ofMillis(250), "acct1", "p1",
                /* executionEnabled */ true, /* maxPendingProjectionRecords */ 4,
                GatewayConfig.defaultRequestBudget(Duration.ofMillis(2000)));
        GatewayReadiness readiness = new GatewayReadiness();
        readiness.fluss(true, "ok");
        readiness.protocol(true, "ok");
        readiness.durableWrites(true, "ok");

        Consumer<JsonNode> failingConsumer = n -> {
            throw new IllegalStateException("applier exploded");
        };
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.init(new GateRow("p1", "acct1", GateState.HALTED, 0, "boot", "h0",
                null, null, null, null, 0L, null, null, null));
        ExecutorService pool = Executors.newCachedThreadPool();
        server = new GatewayHttpServer(cfg, readiness, failingConsumer, gates, pool, true);
        server.start();
        String base = "http://127.0.0.1:" + serverPort(server);

        GatewayProtocol proto = new GatewayProtocol("secret1234567890123456");
        var payload = M.createObjectNode().put("instruction_id", "i-fail");
        String envelope = proto.encode(new GatewayProtocol.Envelope(
                "execution-gateway.v1", "EXECUTION_EVENT", "req-fail-1", "acct1", "p1",
                GatewayProtocol.sha256(M.writeValueAsBytes(payload)), 1L, "fence-1",
                System.currentTimeMillis() + 60_000, payload, null));

        HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                        URI.create(base + "/v1/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(envelope)).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(500, response.statusCode(), response.body());
        assertTrue(readiness.snapshot().reason().contains("applier exploded"),
                "the projection cause must survive; reason=" + readiness.snapshot().reason());
        // The request's finally has already run (in-flight is back to 0), which is exactly the
        // window where the unconditional restore used to resurrect the flag.
        assertTrue(!readiness.snapshot().durableWriteReady(),
                "a consumer failure must stay latched after the drain (P3-063); reason="
                        + readiness.snapshot().reason());
    }

    private int serverPort(GatewayHttpServer s) throws Exception {
        var f = s.getClass().getDeclaredField("server");
        f.setAccessible(true);
        com.sun.net.httpserver.HttpServer hs = (com.sun.net.httpserver.HttpServer) f.get(s);
        return hs.getAddress().getPort();
    }
}
