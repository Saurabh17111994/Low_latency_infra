package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import org.junit.jupiter.api.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class GatewayHttpServerTest {
    private static final ObjectMapper M = new ObjectMapper();
    private GatewayHttpServer server;
    private InMemoryGateStateStore gates;
    private GatewayConfig cfg;
    private String base;

    @BeforeEach void setUp() throws Exception {
        gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.init(new GateRow("p1","acct1",GateState.HALTED,0,"boot","h0",null,null,null,null,0L,null,null,null));
        // P3-015: approve binds the exact (epoch, evidenceHash) the gate holds —
        // the reconciled package "h1" is what the approve tests must present.
        gates.install(new GateRow("p1","acct1",GateState.APPROVAL_PENDING,1,"reconciled","h1",null,null,null,"owner1",1L,1000L, 900000L,null));
        cfg = new GatewayConfig("localhost:9123","default","Execution_Intent","Execution_Gate","Execution_Attempts","Order_Correlation","Postback_Projection_Ledger","Safety_Halt_Requests","127.0.0.1",0,"http://127.0.0.1:9190/v1/intents","execution-gateway.v1","secret1234567890123456",Duration.ofMillis(2000),Duration.ofMillis(250),"acct1","p1", true);
        server = new GatewayHttpServer(cfg,new GatewayReadiness(), n-> {}, gates, true);
        server.start();
        int port = serverPort(server);
        base = "http://127.0.0.1:"+port;
    }
    @AfterEach void tearDown(){ if(server!=null) server.close(); }
    private int serverPort(GatewayHttpServer s) throws Exception {
        var f=s.getClass().getDeclaredField("server"); f.setAccessible(true);
        com.sun.net.httpserver.HttpServer hs=(com.sun.net.httpserver.HttpServer)f.get(s);
        return hs.getAddress().getPort();
    }
    private static final String BEARER = "Bearer secret1234567890123456";
    private HttpResponse<String> post(String path, String json) throws Exception {
        var c=HttpClient.newHttpClient();
        var req=HttpRequest.newBuilder(URI.create(base+path)).header("Content-Type","application/json").header("Authorization",BEARER).POST(HttpRequest.BodyPublishers.ofString(json)).build();
        return c.send(req, HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> postNoAuth(String path, String json) throws Exception {
        var c=HttpClient.newHttpClient();
        var req=HttpRequest.newBuilder(URI.create(base+path)).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(json)).build();
        return c.send(req, HttpResponse.BodyHandlers.ofString());
    }
    @Test void approve_SaurabhCorrectEpochHash_Enables() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","h1")));
        assertEquals(200,r.statusCode(),r.body());
        assertTrue(r.body().contains("ENABLED"));
        assertEquals(GateState.ENABLED, gates.read("p1").state());
        assertTrue(gates.read("p1").approvalsComplete());
        assertTrue(gates.read("p1").approvalsCover("h1"));
    }
    @Test void approve_UnauthorizedPrincipal_403AndHalted() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","alice","executionPartitionId","p1","epoch",1,"evidenceHash","h1")));
        assertEquals(403,r.statusCode());
        assertEquals(GateState.HALTED, gates.read("p1").state());
    }
    @Test void approve_EpochMismatch_409AndHalted() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",99,"evidenceHash","h1")));
        assertEquals(409,r.statusCode());
        assertEquals(GateState.HALTED, gates.read("p1").state());
    }
    @Test void approve_WrongEvidencePackage_409() throws Exception {
        // P3-015: exact (epoch, evidenceHash) binding — "other" is not the
        // reconciled "h1" package, so approve must mismatch, never APPLY.
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","other")));
        assertEquals(409,r.statusCode(),r.body());
        assertEquals(GateState.HALTED, gates.read("p1").state());
    }
    @Test void approve_MissingEvidence_400() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1)));
        assertEquals(400,r.statusCode());
    }
    @Test void approve_WrongMethod_405() throws Exception {
        var c=HttpClient.newHttpClient();
        var req=HttpRequest.newBuilder(URI.create(base+"/control/approve")).GET().build();
        assertEquals(405,c.send(req, HttpResponse.BodyHandlers.ofString()).statusCode());
    }
    @Test void approve_DuplicateAlreadyApplied_Idempotent200() throws Exception {
        post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","h1")));
        var r2=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","h1")));
        assertEquals(200,r2.statusCode());
        assertTrue(r2.body().contains("ALREADY_APPLIED")||r2.body().contains("ENABLED"));
    }
    @Test void approve_NotFound_404() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","nope","epoch",0,"evidenceHash","h")));
        assertEquals(404,r.statusCode());
    }
    @Test void approve_MissingBearer_401WithoutStoreSideEffect() throws Exception {
        var r=postNoAuth("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","h1")));
        assertEquals(401,r.statusCode());
        // P3-004: no halt side effect for unauthenticated probes — gate untouched.
        assertEquals(GateState.APPROVAL_PENDING, gates.read("p1").state());
    }
    @Test void approve_WrongBearer_401WithoutStoreSideEffect() throws Exception {
        var c=HttpClient.newHttpClient();
        var req=HttpRequest.newBuilder(URI.create(base+"/control/approve")).header("Content-Type","application/json").header("Authorization","Bearer wrong").POST(HttpRequest.BodyPublishers.ofString(M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","h1")))).build();
        assertEquals(401,c.send(req, HttpResponse.BodyHandlers.ofString()).statusCode());
        assertEquals(GateState.APPROVAL_PENDING, gates.read("p1").state());
    }
    @Test void approve_OversizedBody_413() throws Exception {
        var c=HttpClient.newHttpClient();
        var big=M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","x".repeat(300*1024)));
        var req=HttpRequest.newBuilder(URI.create(base+"/control/approve")).header("Content-Type","application/json").header("Authorization",BEARER).POST(HttpRequest.BodyPublishers.ofString(big)).build();
        assertEquals(413,c.send(req, HttpResponse.BodyHandlers.ofString()).statusCode());
    }
    @Test void approve_MissingPartitionId_400() throws Exception {
        // P3-292: omitting the field must 400, never approve the default.
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","epoch",1,"evidenceHash","h1")));
        assertEquals(400,r.statusCode());
        assertEquals(GateState.APPROVAL_PENDING, gates.read("p1").state());
    }
    @Test void haltedDefault_NoApproveRemainsHalted() {
        InMemoryGateStateStore fresh=new InMemoryGateStateStore(Set.of("saurabh"));
        fresh.init(new GateRow("p2","acct1",GateState.HALTED,0,"boot","h0",null,null,null,null,0L,null,null,null));
        assertEquals(GateState.HALTED, fresh.read("p2").state());
    }
}
