package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.GateStateStore;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

/** Minimal private endpoint; no Arrow route or broker client exists in this JVM. */
public final class GatewayHttpServer implements AutoCloseable {
    private static final String SINGLE_OPERATOR = "saurabh";
    /**
     * P3-290: inbound HTTP bodies are attacker-controlled — an unbounded
     * readAllBytes() lets one large POST OOM the gateway before auth/validation.
     * Both handlers share this cap; oversized bodies fail 413 before parsing.
     */
    static final int MAX_BODY_BYTES = 256 * 1024;
    private final HttpServer server;
    private final GatewayProtocol protocol;
    private final GatewayConfig config;
    private final GatewayReadiness readiness;
    private final Consumer<JsonNode> eventConsumer;
    private final GateStateStore gateStore;
    private final ObjectMapper mapper = new ObjectMapper();
    /** Concurrent in-flight projection applies (WP-2 MAX_PENDING_PROJECTION_RECORDS). */
    private final java.util.concurrent.atomic.AtomicInteger projectionInFlight =
            new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.Executor httpExecutor;

    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer) throws IOException {
        this(config, readiness, eventConsumer, new InMemoryGateStateStore(java.util.Set.of(SINGLE_OPERATOR)));
    }
    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer, GateStateStore gateStore) throws IOException {
        this(config, readiness, eventConsumer, gateStore, null);
    }
    /**
     * Full constructor. {@code httpExecutor} {@code null} keeps the platform default
     * (serial handler dispatch); a pool is how the flood soak drives real concurrency.
     */
    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer, GateStateStore gateStore,
            java.util.concurrent.Executor httpExecutor) throws IOException {
        this.config = config; this.readiness = readiness; this.eventConsumer = eventConsumer;
        this.gateStore = java.util.Objects.requireNonNull(gateStore, "gateStore");
        this.httpExecutor = httpExecutor;
        this.protocol = new GatewayProtocol(config.sharedSecret());
        this.server = HttpServer.create(new InetSocketAddress(config.bindHost(), config.bindPort()), 16);
        if (httpExecutor != null) server.setExecutor(httpExecutor);
        server.createContext("/healthz", this::health);
        server.createContext("/readyz", this::ready);
        server.createContext("/v1/events", this::events);
        server.createContext("/control/approve", this::approve);
    }
    public void start() { server.start(); }
    private void health(HttpExchange x) throws IOException { reply(x, 200, "{\"healthy\":true}"); }
    private void ready(HttpExchange x) throws IOException {
        GatewayReadiness.Snapshot s = readiness.snapshot();
        reply(x, s.executionReady() ? 200 : 503, mapper.writeValueAsString(s));
    }
    // POST /control/approve {principal,executionPartitionId,epoch,evidenceHash}
    private void approve(HttpExchange x) throws IOException {
        if (!"POST".equalsIgnoreCase(x.getRequestMethod())) { reply(x, 405, "{\"error\":\"method not allowed\"}"); return; }
        // P3-291: the events path fails closed when EXECUTION_ENABLED=false —
        // approve must too, or a caller enables the durable gate the events
        // path will never serve (fail-closed HALTED default contradicted).
        if (!config.executionEnabled()) { reply(x, 503, "{\"error\":\"execution disabled via EXECUTION_ENABLED\"}"); return; }
        // P3-004: the principal is transport identity, not body content — a
        // self-asserted {"principal":"saurabh"} drove HALTED->ENABLED with no
        // secret check, and anyone could force a safety HALT by sending a wrong
        // principal/epoch. Require the shared-secret bearer before touching
        // any store. P3-290: cap first — unauthenticated bytes are the DoS leg.
        String bearer = x.getRequestHeaders().getFirst("Authorization");
        if (!protocol.authorizedBearer(bearer)) {
            // No store access on auth failure: no halt side effect for probes
            // (the old wrong-principal halt was itself an unauthenticated DoS).
            reply(x, 401, "{\"error\":\"missing or invalid authorization\"}"); return;
        }
        String body = readCapped(x);
        if (body == null) return;
        JsonNode n;
        try { n = mapper.readTree(body); } catch (Exception e) { reply(x, 400, "{\"error\":\"malformed json\"}"); return; }
        String principal = n.path("principal").asText("");
        // P3-292: no default fallback — approving the configured partition by
        // omitting the field made the "required" 400 below unreachable.
        String partitionId = n.path("executionPartitionId").asText("");
        long epoch = n.path("epoch").isNumber() ? n.path("epoch").asLong() : Long.MIN_VALUE;
        String evidenceHash = n.path("evidenceHash").asText("");
        if (principal.isBlank() || evidenceHash.isBlank() || epoch == Long.MIN_VALUE || partitionId.isBlank()) {
            reply(x, 400, "{\"error\":\"principal, epoch, evidenceHash, executionPartitionId required\"}"); return;
        }
        long now = System.currentTimeMillis();
        GateRow cur = gateStore.read(partitionId);
        if (cur == null) { reply(x, 404, "{\"error\":\"no gate row for partition\"}"); return; }
        if (!SINGLE_OPERATOR.equals(principal)) {
            // Bearer already proved shared-secret possession; store-level
            // authorization still decides, and a wrong principal halts.
            haltUnlessHalted(partitionId, "unauthorized approver "+principal, evidenceHash, now);
            reply(x, 403, "{\"error\":\"single-operator saurabh only\",\"outcome\":\"UNAUTHORIZED\"}"); return;
        }
        // P3-075: one atomic store transition — never approve() then a
        // separate instanceof+install() (concurrent halt() between the two was
        // overwritten, and non-InMemory impls no-op'd while we replied ENABLED).
        GateStateStore.ApprovalResult res = gateStore.approveAndEnableIfComplete(
                partitionId, principal, epoch, evidenceHash, now);
        switch (res.outcome()) {
            case APPLIED -> {
                GateRow approved = res.row();
                reply(x, 200, mapper.writeValueAsString(Map.of("status",approved.state().name(),"epoch",approved.epoch(),"outcome","APPLIED")));
            }
            case ALREADY_APPLIED -> reply(x, 200, mapper.writeValueAsString(Map.of("status",res.row().state().name(),"epoch",res.row().epoch(),"outcome","ALREADY_APPLIED")));
            case EPOCH_MISMATCH -> {
                // P3-293: never halt on the stale pre-approve read — re-read
                // fresh so a concurrent approve/halt between is not clobbered
                // via a stale expected (P3-157 CAS witness semantics).
                GateRow fresh = gateStore.read(partitionId);
                if (fresh != null && fresh.state() != GateState.HALTED)
                    gateStore.halt(partitionId, fresh, "epoch mismatch approve "+epoch+" != "+fresh.epoch(), evidenceHash, now);
                reply(x, 409, "{\"error\":\"epoch mismatch\",\"outcome\":\"EPOCH_MISMATCH\"}");
            }
            case UNAUTHORIZED -> {
                GateRow fresh = gateStore.read(partitionId);
                if (fresh != null && fresh.state() != GateState.HALTED)
                    gateStore.halt(partitionId, fresh, "unauthorized "+principal, evidenceHash, now);
                reply(x, 403, "{\"error\":\"unauthorized\",\"outcome\":\"UNAUTHORIZED\"}");
            }
            case EVIDENCE_MISMATCH -> {
                // P3-151: the approval carried evidence the gate has not bound —
                // a stale/unauthorized package. Halt (fail closed) and surface the
                // mismatch; it must never be recorded as APPLIED.
                GateRow fresh = gateStore.read(partitionId);
                if (fresh != null && fresh.state() != GateState.HALTED)
                    gateStore.halt(partitionId, fresh, "evidence mismatch approve", evidenceHash, now);
                reply(x, 409, "{\"error\":\"evidence mismatch\",\"outcome\":\"EVIDENCE_MISMATCH\"}");
            }
            case SAME_PRINCIPAL -> reply(x, 409, "{\"error\":\"same principal already approved\",\"outcome\":\"SAME_PRINCIPAL\"}");
            case NOT_FOUND -> reply(x, 404, "{\"error\":\"no gate row\",\"outcome\":\"NOT_FOUND\"}");
            default -> reply(x, 409, mapper.writeValueAsString(Map.of("error",res.reason()==null?"rejected":res.reason(),"outcome",res.outcome().name())));
        }
    }

    /** P3-293 helper: halt only when the FRESH row is not already HALTED. */
    private void haltUnlessHalted(String partitionId, String reason, String evidenceHash, long now) {
        GateRow fresh = gateStore.read(partitionId);
        if (fresh != null && fresh.state() != GateState.HALTED)
            gateStore.halt(partitionId, fresh, reason, evidenceHash, now);
    }

    /** P3-290: one capped read for both handlers; 413 before JSON/verify. */
    private String readCapped(HttpExchange x) throws IOException {
        byte[] raw = x.getRequestBody().readNBytes(MAX_BODY_BYTES + 1);
        if (raw.length > MAX_BODY_BYTES) {
            reply(x, 413, "{\"error\":\"payload too large\"}");
            return null;
        }
        return new String(raw, StandardCharsets.UTF_8);
    }
    private void events(HttpExchange x) throws IOException {
        if (!"POST".equalsIgnoreCase(x.getRequestMethod())) { reply(x, 405, "method not allowed"); return; }
        // Fail-closed: when execution is disabled the bridge is disabled and the gateway remains HALTED.
        // Offline, no FLUSS_BOOTSTRAP / Arrow deps are required to evaluate this gate.
        if (!config.executionEnabled()) { reply(x, 503, "execution disabled via EXECUTION_ENABLED"); return; }
        // P3-290: same cap as approve — the 401 HMAC below must never see
        // unbounded bytes (single large POST OOMs before validation).
        String body = readCapped(x);
        if (body == null) return;
        GatewayProtocol.Verification v = protocol.verify(body, config.protocolVersion(), System.currentTimeMillis());
        // P3-076: a per-request auth failure is request state, not service
        // state — flipping protocolReady=false here bricked intake for every
        // later valid envelope until restart (one probe = global DoS).
        if (!v.accepted()) { reply(x, 401, v.reason()); return; }
        GatewayReadiness.Snapshot ready = readiness.snapshot();
        if (!ready.healthy() || !ready.flussReady() || !ready.protocolReady() || !ready.durableWriteReady()) {
            reply(x, 503, "gateway not ready"); return;
        }
        if (eventConsumer == null) { reply(x, 503, "projection consumer unavailable"); return; }
        // WP-2 backpressure bound: concurrent in-flight applies beyond
        // MAX_PENDING_PROJECTION_RECORDS flip durableWrites readiness false and
        // shed load (503) instead of queueing without bound.
        int inFlightNow = projectionInFlight.incrementAndGet();
        int maxPending = config.maxPendingProjectionRecords();
        try {
            if (inFlightNow > maxPending) {
                readiness.durableWrites(false,
                        "projection backlog " + inFlightNow + " > " + maxPending);
                reply(x, 503, "projection backlog exceeds MAX_PENDING_PROJECTION_RECORDS");
                return;
            }
            eventConsumer.accept(v.envelope().payload());
            reply(x, 202, "accepted");
        } catch (Exception consumerFailure) {
            // P3-077: production wiring throws IllegalStateException on
            // applier failure — without this catch the exception escapes the
            // HttpExchange handler and the client sees an aborted exchange.
            readiness.durableWrites(false, String.valueOf(consumerFailure.getMessage()));
            reply(x, 500, "{\"error\":\"projection failed\"}");
        } finally {
            // P3-294: the decrement-then-separate-snapshot-then-set is a
            // cross-atomic check-then-act — a concurrent shed can set false
            // after our snapshot, and this blind set(true) clears a live
            // backlog. Restore via the atomic readiness gate instead.
            if (projectionInFlight.decrementAndGet() == 0) {
                readiness.restoreIfDrained(projectionInFlight::get,
                        "projection backlog drained");
            }
        }
    }
    private static void reply(HttpExchange x, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        x.getResponseHeaders().set("Content-Type", "application/json");
        x.sendResponseHeaders(code, bytes.length);
        try (var out = x.getResponseBody()) { out.write(bytes); }
    }
    // P3-295: caller-owned pool, server-owned lifecycle — a pooled executor
    // outlives server.stop(0), leaking non-daemon threads after close (the
    // flood soak passes a cached pool). Shut it down here; a non-service
    // Executor (e.g. direct) is untouched.
    @Override public void close() {
        server.stop(0);
        if (httpExecutor instanceof java.util.concurrent.ExecutorService es) es.shutdownNow();
    }
}
