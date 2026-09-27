package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H2-5/D2 (CHG-334): the executor's durable gate-transition report endpoint.
 *
 * <p>The executor owns the gate decision; this endpoint owns the durable record. These tests pin
 * the mapping (BOOT_HALT/APPROVE/HALT/RENEW), the sanctioned enablement path, the returned row the
 * executor adopts, and every fail-closed arm: disabled 503, non-authoritative 501, bad signature
 * 401, stale epoch / fence conflict / wrong scope 409 with no mutation, bad lease 400.
 */
class GatewayGateReportTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String SECRET = "secret1234567890123456";
    private static final long NOW = System.currentTimeMillis();

    private InMemoryGateStateStore gates;
    private GatewayProtocol protocol;
    private GatewayHttpServer server;
    private String base;
    private final AtomicInteger reqSeq = new AtomicInteger();

    private static GatewayConfig config(boolean enabled) {
        return new GatewayConfig("localhost:9123", "default", "Execution_Intent", "Execution_Gate",
                "Execution_Attempts", "Order_Correlation", "Postback_Projection_Ledger",
                "Safety_Halt_Requests", "127.0.0.1", 0, "http://127.0.0.1:9190/v1/intents",
                "execution-gateway.v1", SECRET, Duration.ofMillis(2000), Duration.ofMillis(250),
                "acct1", "p1", enabled);
    }

    @BeforeEach
    void setUp() throws Exception {
        gates = new InMemoryGateStateStore(Set.of("saurabh"));
        protocol = new GatewayProtocol(SECRET);
        server = new GatewayHttpServer(config(true), new GatewayReadiness(), n -> {}, gates, true);
        server.start();
        base = "http://127.0.0.1:" + serverPort(server);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.close();
    }

    private static int serverPort(GatewayHttpServer s) throws Exception {
        var f = s.getClass().getDeclaredField("server");
        f.setAccessible(true);
        com.sun.net.httpserver.HttpServer hs = (com.sun.net.httpserver.HttpServer) f.get(s);
        return hs.getAddress().getPort();
    }

    private HttpResponse<String> post(String path, String json) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(base + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** Builds and signs a GATE_REPORT envelope. */
    private String report(String transition, long epoch, String fenceToken, ObjectNode payload)
            throws Exception {
        payload.put("transition", transition);
        return protocol.encode(new GatewayProtocol.Envelope(
                "execution-gateway.v1", "GATE_REPORT", "gate-req-" + reqSeq.incrementAndGet(),
                "acct1", "p1", null, epoch, fenceToken, System.currentTimeMillis() + 60_000,
                payload, null));
    }

    private static ObjectNode bootPayload() {
        return M.createObjectNode().put("reason", "executor boot");
    }

    private static ObjectNode approvePayload(String owner, String principal, String evidence,
            long leaseMs) {
        return M.createObjectNode()
                .put("owner_instance_id", owner)
                .put("principal", principal)
                .put("evidence_hash", evidence)
                .put("lease_ms", leaseMs);
    }

    private static ObjectNode haltPayload(String reason, String evidence) {
        return M.createObjectNode().put("reason", reason).put("evidence_hash", evidence);
    }

    private static ObjectNode renewPayload(String owner, long leaseMs) {
        return M.createObjectNode().put("owner_instance_id", owner).put("lease_ms", leaseMs);
    }

    private static void haltedRow(InMemoryGateStateStore s, long epoch) {
        s.install(new GateRow("p1", "acct1", GateState.HALTED, epoch, "boot", null, null, null,
                null, null, 0L, null, null, null, NOW, null));
    }

    private static void enabledRow(InMemoryGateStateStore s, long epoch, long token) {
        s.install(new GateRow("p1", "acct1", GateState.ENABLED, epoch, "enabled", "ev-1",
                "saurabh", null, "ev-1", "exec-1", token, NOW - 1000, NOW + 30_000, null, NOW,
                null));
    }

    @Test
    @DisplayName("BOOT_HALT creates the HALTED row when none exists")
    void bootHaltCreatesRow() throws Exception {
        HttpResponse<String> r = post("/v1/gate", report("BOOT_HALT", 0, "0", bootPayload()));

        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("HALTED"), r.body());
        assertNotNull(gates.read("p1"));
        assertEquals(GateState.HALTED, gates.read("p1").state());
        assertEquals(0L, gates.read("p1").epoch());
    }

    @Test
    @DisplayName("BOOT_HALT over an ENABLED row halts it: fence cleared, epoch +1, never resumes")
    void bootHaltOverEnabledRow() throws Exception {
        enabledRow(gates, 5, 7);

        HttpResponse<String> r = post("/v1/gate", report("BOOT_HALT", 0, "0", bootPayload()));

        assertEquals(200, r.statusCode(), r.body());
        GateRow row = gates.read("p1");
        assertEquals(GateState.HALTED, row.state());
        assertEquals(6L, row.epoch());
        assertNull(row.ownerInstanceId());
        assertNull(row.leaseExpiresTs());
        assertTrue(row.fenceToken() > 7, "halt mints a strictly greater ordering token");
    }

    @Test
    @DisplayName("APPROVE walks HALTED -> RECONCILING -> APPROVAL_PENDING -> ENABLED with a live fence")
    void approveEnablesThroughSanctionedPath() throws Exception {
        haltedRow(gates, 3);

        HttpResponse<String> r = post("/v1/gate", report("APPROVE", 3, "0",
                approvePayload("exec-1", "saurabh", "ev-1", 30_000)));

        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("ENABLED"), r.body());
        GateRow row = gates.read("p1");
        assertEquals(GateState.ENABLED, row.state());
        // two sanctioned steps (3->4 RECONCILING, 4->5 APPROVAL_PENDING); the promotion keeps 5
        assertEquals(5L, row.epoch());
        assertEquals("exec-1", row.ownerInstanceId());
        assertTrue(row.fenceToken() > 0);
        assertNotNull(row.leaseExpiresTs());
        assertEquals("saurabh", row.approval1());
        assertEquals("ev-1", row.approvedEvidenceHash());
        assertTrue(row.fenceValidFor("exec-1", row.fenceToken(), System.currentTimeMillis()));
    }

    @Test
    @DisplayName("APPROVE with a stale epoch is 409 and mutates nothing")
    void approveStaleEpochIsRejected() throws Exception {
        haltedRow(gates, 3);

        HttpResponse<String> r = post("/v1/gate", report("APPROVE", 2, "0",
                approvePayload("exec-1", "saurabh", "ev-1", 30_000)));

        assertEquals(409, r.statusCode(), r.body());
        assertTrue(r.body().contains("EPOCH_MISMATCH"), r.body());
        assertEquals(GateState.HALTED, gates.read("p1").state());
        assertEquals(3L, gates.read("p1").epoch());
    }

    @Test
    @DisplayName("APPROVE when already ENABLED is idempotent and never re-mints the fence")
    void approveWhenEnabledIsIdempotent() throws Exception {
        enabledRow(gates, 5, 7);

        HttpResponse<String> r = post("/v1/gate", report("APPROVE", 5, "7",
                approvePayload("exec-1", "saurabh", "ev-1", 30_000)));

        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("ALREADY_ENABLED"), r.body());
        assertEquals(7L, gates.read("p1").fenceToken());
    }

    @Test
    @DisplayName("APPROVE by an unauthorized principal halts (fail-closed), never enables")
    void approveUnauthorizedHalts() throws Exception {
        haltedRow(gates, 3);

        HttpResponse<String> r = post("/v1/gate", report("APPROVE", 3, "0",
                approvePayload("exec-1", "mallory", "ev-1", 30_000)));

        assertEquals(409, r.statusCode(), r.body());
        assertTrue(r.body().contains("UNAUTHORIZED"), r.body());
        assertEquals(GateState.HALTED, gates.read("p1").state());
    }

    @Test
    @DisplayName("HALT clears the fence, bumps the epoch, and is idempotent")
    void haltClearsFence() throws Exception {
        enabledRow(gates, 5, 7);

        HttpResponse<String> r = post("/v1/gate", report("HALT", 5, "7",
                haltPayload("operator halt", "ev-1")));

        assertEquals(200, r.statusCode(), r.body());
        GateRow row = gates.read("p1");
        assertEquals(GateState.HALTED, row.state());
        assertEquals(6L, row.epoch());
        assertNull(row.ownerInstanceId());
        assertNull(row.leaseExpiresTs());
        assertTrue(row.fenceToken() > 7);

        // a second HALT (e.g. an escalation repeat) records evidence without a second epoch bump
        HttpResponse<String> again = post("/v1/gate", report("HALT", 6, "0",
                haltPayload("operator halt repeat", "ev-1")));
        assertEquals(200, again.statusCode(), again.body());
        assertEquals(6L, gates.read("p1").epoch());
    }

    @Test
    @DisplayName("RENEW extends the lease without changing the token; a wrong token conflicts")
    void renewExtendsLease() throws Exception {
        enabledRow(gates, 5, 7);

        HttpResponse<String> r = post("/v1/gate", report("RENEW", 5, "7",
                renewPayload("exec-1", 30_000)));

        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("RENEWED"), r.body());
        GateRow row = gates.read("p1");
        assertEquals(7L, row.fenceToken());
        assertTrue(row.leaseExpiresTs() > System.currentTimeMillis() + 25_000);

        HttpResponse<String> wrong = post("/v1/gate", report("RENEW", 5, "6",
                renewPayload("exec-1", 30_000)));
        assertEquals(409, wrong.statusCode(), wrong.body());
    }

    @Test
    @DisplayName("fail-closed arms: disabled 503, non-authoritative 501, bad signature 401, bad lease 400")
    void failClosedArms() throws Exception {
        try (GatewayHttpServer disabled = new GatewayHttpServer(config(false),
                new GatewayReadiness(), n -> {}, gates, false)) {
            disabled.start();
            String dBase = "http://127.0.0.1:" + serverPort(disabled);
            HttpResponse<String> r = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(dBase + "/v1/gate"))
                            .POST(HttpRequest.BodyPublishers.ofString(report("BOOT_HALT", 0, "0",
                                    bootPayload())))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(503, r.statusCode(), r.body());
        }

        // non-authoritative store: fail closed even when execution is enabled
        try (GatewayHttpServer placeholder = new GatewayHttpServer(config(true),
                new GatewayReadiness(), n -> {}, gates, false)) {
            placeholder.start();
            String pBase = "http://127.0.0.1:" + serverPort(placeholder);
            HttpResponse<String> r = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(pBase + "/v1/gate"))
                            .POST(HttpRequest.BodyPublishers.ofString(report("BOOT_HALT", 0, "0",
                                    bootPayload())))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(501, r.statusCode(), r.body());
        }

        // tampered signature
        String signed = report("BOOT_HALT", 0, "0", bootPayload());
        String tampered = signed.replace("\"reason\":\"executor boot\"",
                "\"reason\":\"executor boot!\"");
        assertEquals(401, post("/v1/gate", tampered).statusCode());

        // lease beyond the cap is a config error, never clamped
        haltedRow(gates, 3);
        HttpResponse<String> lease = post("/v1/gate", report("APPROVE", 3, "0",
                approvePayload("exec-1", "saurabh", "ev-1", GatewayHttpServer.MAX_GATE_LEASE_MS + 1)));
        assertEquals(400, lease.statusCode(), lease.body());
        assertEquals(GateState.HALTED, gates.read("p1").state());
    }

    @Test
    @DisplayName("the endpoint checks its own message type and scope")
    void typeAndScopeArePinned() throws Exception {
        String notReport = protocol.encode(new GatewayProtocol.Envelope(
                "execution-gateway.v1", "EXECUTION_EVENT", "evt-1", "acct1", "p1", null, 0, "0",
                System.currentTimeMillis() + 60_000, bootPayload(), null));
        assertEquals(400, post("/v1/gate", notReport).statusCode());

        String wrongScope = protocol.encode(new GatewayProtocol.Envelope(
                "execution-gateway.v1", "GATE_REPORT", "gate-req-other", "other-scope", "p1", null,
                0, "0", System.currentTimeMillis() + 60_000, bootPayload(), null));
        assertEquals(409, post("/v1/gate", wrongScope).statusCode());
    }
}
