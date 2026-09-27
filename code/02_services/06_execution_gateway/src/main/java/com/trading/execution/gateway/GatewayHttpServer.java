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
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(GatewayHttpServer.class);
    static final String SINGLE_OPERATOR = "saurabh";
    /**
     * H2-5/D2: the largest lease a gate report may ask the durable row to carry. The executor's
     * own TTL is 30 s (renew every 10 s); a request beyond two minutes is a misconfiguration, and
     * honoring it would leave a dead executor's fence authorizing forwards for that long. Reject,
     * never clamp: a wrong config must be visible, not silently rounded.
     */
    static final long MAX_GATE_LEASE_MS = 120_000L;
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
    /**
     * P3-063: whether the current durableWrites=false is this server's own shed. Only that false is
     * the drain's to undo — an unreadable reason ("backlog drained") overwriting a projection
     * failure is how /readyz hid the failure, and a persistent failure flapped the flag on every
     * request. Any other writer of false clears this, so the drain can no longer clear their state.
     */
    private final java.util.concurrent.atomic.AtomicBoolean shedActive =
            new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.Executor httpExecutor;

    /**
     * Whether {@code /control/approve} may mutate {@link #gateStore}. False means the store is
     * a placeholder rather than durable authority, and the endpoint fails closed instead of
     * reporting a success it did not achieve.
     */
    private final boolean gateStoreAuthoritative;

    /**
     * Production default: no durable gate store is wired, so the server runs against a
     * per-process placeholder. {@code /control/approve} therefore FAILS CLOSED — it used to
     * return {@code 200 APPLIED} against state that approves nothing, which is worse than a
     * refusal because it reads as success.
     */
    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer) throws IOException {
        this(config, readiness, eventConsumer,
                new InMemoryGateStateStore(java.util.Set.of(SINGLE_OPERATOR)), null, false);
    }
    /**
     * Authority is a REQUIRED declaration, never inferred: a caller that wires a gate store
     * must also state whether it is durable authority. There is deliberately no overload that
     * supplies this silently — that implicit default was itself the defect, because it meant
     * the same constructor produced different answers in tests and in production.
     */
    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer, GateStateStore gateStore,
            boolean gateStoreAuthoritative) throws IOException {
        this(config, readiness, eventConsumer, gateStore, null, gateStoreAuthoritative);
    }
    /**
     * Full constructor. {@code httpExecutor} {@code null} keeps the platform default
     * (serial handler dispatch); a pool is how the flood soak drives real concurrency.
     * {@code gateStoreAuthoritative} false makes {@code /control/approve} fail closed.
     *
     * <p><b>Recorded debt - production dispatch is serial.</b> Production passes no executor, so
     * the platform runs each handler on the dispatcher thread: one request at a time, and a request
     * that holds also holds {@code /healthz}. Measured on the dev cluster (2026-09-12): with two
     * {@code /v1/events} in flight (6019ms and 3051ms - queued, not overlapped), {@code /healthz},
     * which answers in 45ms idle, took 5620ms. C2 bounds how long one request may hold its thread
     * (one retry budget across all eleven Fluss calls, instead of one per call); it does not remove
     * the serialisation itself.
     *
     * <p><b>Ordering analysis (2026-09-13): a same-process pool is admissible — concurrency is
     * gated on operations, not on ordering.</b> Both questions this javadoc used to leave open are
     * answered by inspection. <i>Interleaving:</i> {@code ProjectionApplier.apply} takes a
     * {@code ReentrantLock} stripe keyed on {@code postbackEventId} before it walks
     * RECEIVED -> writeAudit -> AUDIT_WRITTEN -> writeLifecycle -> LIFECYCLE_APPLIED ->
     * writePosition, so one stream's steps are serialised, and distinct eventIds are independent by
     * design. <i>The shed path:</i> {@code projectionInFlight} is incremented and the bound checked
     * before {@code eventConsumer.accept(...)}, so a rejected request has written nothing, and the
     * decrement runs in a {@code finally} guarded by {@code readiness.restoreIfDrained}, so it
     * cannot clear a live backlog.
     *
     * <p>What actually gates a pool is elsewhere. {@code MAX_PENDING_PROJECTION_RECORDS} is
     * unreachable in production today — one dispatcher thread keeps in-flight at 1 — so enabling a
     * pool turns shedding (503 plus {@code durableWrites(false)}) into a real intake behaviour
     * instead of a soak-only path, and that is a decision to take deliberately rather than inherit.
     * And the ledger's last-writer-wins is safe only under the single-writer-per-{@code eventId}
     * deployment contract: the stripe fences that inside one process, and nothing fences it across
     * processes — {@link ProjectionLedgerStore} names that gap. That gap blocks horizontal scaling,
     * not this pool. Until the shed bound's production value is chosen and a test proves two
     * distinct eventIds apply concurrently without touching each other's steps, keep the thread
     * count at one and keep the hold bounded.
     *
     * <p>Verified 2026-09-13: that is not an accident of this class. {@link ExecutionGatewayMain}
     * builds the server through the constructors that pass {@code null} for the executor, and no
     * pool is constructed anywhere in this service's main sources, so the platform default (the
     * thread created by {@code start()}, per {@code HttpServer.setExecutor}) serves every request
     * and in-flight stays at 1. The deployed value is the default 1000 — neither the compose file
     * nor the stack sets {@code MAX_PENDING_PROJECTION_RECORDS}.
     *
     * <p>Decision 2026-09-13 (B12-ii): keep that default and keep the single-threaded dispatch the
     * closing sentence above already requires — the change that wires an executor owns both the
     * value and that concurrency test.
     */
    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer, GateStateStore gateStore,
            java.util.concurrent.Executor httpExecutor, boolean gateStoreAuthoritative)
            throws IOException {
        this.config = config; this.readiness = readiness; this.eventConsumer = eventConsumer;
        this.gateStore = java.util.Objects.requireNonNull(gateStore, "gateStore");
        this.httpExecutor = httpExecutor;
        this.gateStoreAuthoritative = gateStoreAuthoritative;
        this.protocol = new GatewayProtocol(config.sharedSecret());
        this.server = HttpServer.create(new InetSocketAddress(config.bindHost(), config.bindPort()), 16);
        if (httpExecutor != null) server.setExecutor(httpExecutor);
        server.createContext("/healthz", this::health);
        server.createContext("/readyz", this::ready);
        server.createContext("/v1/events", this::events);
        server.createContext("/v1/gate", this::gateReport);
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
        // Readiness honesty: with no durable gate store wired (the 3-arg production form),
        // this endpoint used to return 200 APPLIED against a per-process map — a success
        // response that approves nothing. That is the most dangerous answer available,
        // because an operator reads it as "the gate is now open for business" and the
        // durable gate the events path consults is untouched. Refuse instead. 501 is
        // deliberately distinct from the 503 above (a deployment choice, reversible) and
        // from 200 (actually done).
        if (!gateStoreAuthoritative) {
            reply(x, 501, "{\"error\":\"no durable gate store wired; approvals are not "
                    + "available in this deployment\"}");
            return;
        }
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

    // POST /v1/gate — the executor's durable gate-transition report (H2-5/D2, CHG-334).
    //
    // The executor owns the gate DECISION (its DEC-044 envelope is verified there, before it
    // reports); this endpoint owns the durable RECORD. It walks the sanctioned enablement path
    // on the store and returns the persisted row, which the executor adopts — so the row, the
    // epoch the forward leg sends, and the executor's control epoch cannot drift. Fail-closed:
    // disabled gateway 503, non-authoritative store 501, bad signature 401, stale/illegal
    // transition 409 with no mutation, store failure 503 (the executor stays HALTED).
    private void gateReport(HttpExchange x) throws IOException {
        if (!"POST".equalsIgnoreCase(x.getRequestMethod())) {
            reply(x, 405, "{\"error\":\"method not allowed\"}"); return;
        }
        if (!config.executionEnabled()) {
            reply(x, 503, "{\"error\":\"execution disabled via EXECUTION_ENABLED\"}"); return;
        }
        if (!gateStoreAuthoritative) {
            reply(x, 501, "{\"error\":\"no durable gate store wired; gate reports are not "
                    + "available in this deployment\"}");
            return;
        }
        String body = readCapped(x);
        if (body == null) return;
        GatewayProtocol.Verification v =
                protocol.verify(body, config.protocolVersion(), System.currentTimeMillis());
        if (!v.accepted()) { reply(x, 401, v.reason()); return; }
        GatewayProtocol.Envelope e = v.envelope();
        // Endpoint/type pin: the shared allowlist admits intents/events too, so a report must
        // declare itself here — never be consumed as a projection event by mistake.
        if (!"GATE_REPORT".equals(e.messageType())) {
            reply(x, 400, "{\"error\":\"message_type must be GATE_REPORT\"}"); return;
        }
        if (!config.executionPartitionId().equals(e.executionPartitionId())
                || !config.accountScopeId().equals(e.accountScopeId())) {
            reply(x, 409, "{\"error\":\"partition/scope mismatch\",\"outcome\":\"SCOPE_MISMATCH\"}");
            return;
        }
        JsonNode p = e.payload();
        String transition = p.path("transition").asText("");
        String owner = p.path("owner_instance_id").asText("");
        String principal = p.path("principal").asText("");
        String reason = p.path("reason").asText("");
        String evidence = p.path("evidence_hash").asText("");
        long leaseMs = p.path("lease_ms").isNumber() ? p.path("lease_ms").asLong() : 0L;
        long now = System.currentTimeMillis();
        try {
            switch (transition) {
                case "BOOT_HALT" -> bootHalt(x, reason, now);
                case "APPROVE" -> approveReported(x, e, owner, principal, evidence, leaseMs, now);
                case "HALT" -> haltReported(x, reason, evidence, now);
                case "RENEW" -> renewReported(x, owner, e.fenceToken(), leaseMs, now);
                default -> reply(x, 400, "{\"error\":\"unknown transition\"}");
            }
        } catch (RuntimeException storeFailure) {
            // The durable write is the commit (P3-139/P3-145): a store failure must never be
            // reported as an applied transition. 503 is retryable; the executor stays HALTED
            // or keeps renewing only after this answers success.
            LOG.warn("gate report {} failed for partition {}", transition, e.executionPartitionId(),
                    storeFailure);
            reply(x, 503, "{\"error\":\"gate store unavailable\"}");
        }
    }

    /** Boot: the row must exist and be HALTED. Never authorizes anything by itself. */
    private void bootHalt(HttpExchange x, String reason, long now) throws IOException {
        String partition = config.executionPartitionId();
        GateRow row = gateStore.read(partition);
        if (row == null) {
            row = gateStore.init(new GateRow(partition, config.accountScopeId(), GateState.HALTED,
                    1L, reason.isBlank() ? "executor boot" : reason, null, null, null, null, null,
                    0L, null, null, null, now, null));
        }
        if (row.state() != GateState.HALTED) {
            // A restart must never resume: transition the durable row to HALTED (fence retained
            // as the ordering version, owner/lease cleared, epoch +1) — DEC-044, no auto-resume.
            row = gateStore.halt(partition, row, reason.isBlank() ? "executor boot" : reason,
                    null, now);
        }
        if (row == null) { reply(x, 404, "{\"error\":\"no gate row\"}"); return; }
        replyRow(x, 200, "HALTED", row);
    }

    /** Approve: the sanctioned path, then the existing single-operator promotion to ENABLED. */
    private void approveReported(HttpExchange x, GatewayProtocol.Envelope e, String owner,
            String principal, String evidence, long leaseMs, long now) throws IOException {
        String partition = config.executionPartitionId();
        if (owner.isBlank() || principal.isBlank() || evidence.isBlank()) {
            reply(x, 400, "{\"error\":\"owner_instance_id, principal, evidence_hash required\"}");
            return;
        }
        if (leaseMs <= 0 || leaseMs > MAX_GATE_LEASE_MS) {
            reply(x, 400, "{\"error\":\"lease_ms out of range (1.." + MAX_GATE_LEASE_MS + ")\"}");
            return;
        }
        GateRow row = gateStore.read(partition);
        if (row == null) {
            row = gateStore.init(new GateRow(partition, config.accountScopeId(), GateState.HALTED,
                    1L, "gate report before boot", null, null, null, null, null, 0L, null, null,
                    null, now, null));
        }
        if (row.state() == GateState.ENABLED) {
            // Idempotent: this approved generation is already durable. Never re-mint the fence —
            // a retried report must not invalidate the live lease.
            replyRow(x, 200, "ALREADY_ENABLED", row);
            return;
        }
        if (row.epoch() != e.gateEpoch()) {
            reply(x, 409, "{\"error\":\"stale report: epoch " + e.gateEpoch() + " != durable "
                    + row.epoch() + "\",\"outcome\":\"EPOCH_MISMATCH\"}");
            return;
        }
        GateStateStore.FenceResult fence = gateStore.acquire(partition, owner, leaseMs, now);
        if (fence.conflict()) {
            reply(x, 409, "{\"error\":\"" + fence.reason() + "\",\"outcome\":\"FENCE_CONFLICT\"}");
            return;
        }
        row = gateStore.read(partition);
        if (row.state() == GateState.HALTED) {
            row = gateStore.transition(partition, row, GateState.RECONCILING, "executor approval",
                    null, now);
        }
        if (row.state() == GateState.RECONCILING) {
            row = gateStore.transition(partition, row, GateState.APPROVAL_PENDING,
                    "executor approval", null, now);
        }
        if (row.state() != GateState.APPROVAL_PENDING) {
            reply(x, 409, "{\"error\":\"cannot approve from " + row.state() + "\",\"outcome\":\""
                    + row.state() + "\"}");
            return;
        }
        // The approval names the CURRENT durable epoch — the sanctioned path advanced it.
        GateStateStore.ApprovalResult res = gateStore.approveAndEnableIfComplete(
                partition, principal, row.epoch(), evidence, now);
        switch (res.outcome()) {
            case APPLIED -> {
                GateRow enabled = res.row();
                if (enabled.state() != GateState.ENABLED) {
                    // Defensive: promotion is the only APPLIED outcome from APPROVAL_PENDING.
                    reply(x, 409, "{\"error\":\"approval recorded but not enabled\",\"outcome\":\""
                            + enabled.state() + "\"}");
                    return;
                }
                replyRow(x, 200, "ENABLED", enabled);
            }
            case ALREADY_APPLIED -> replyRow(x, 200, "ALREADY_ENABLED", res.row());
            case UNAUTHORIZED, EVIDENCE_MISMATCH, EPOCH_MISMATCH -> {
                // Fail-closed parity with /control/approve: a wrong principal or a package the
                // gate has not bound halts the row rather than leaving a fenced partial state.
                haltUnlessHalted(partition, "gate report " + res.outcome().name().toLowerCase(),
                        evidence, now);
                reply(x, 409, "{\"error\":\"" + res.reason() + "\",\"outcome\":\""
                        + res.outcome().name() + "\"}");
            }
            default -> reply(x, 409, "{\"error\":\"" + res.reason() + "\",\"outcome\":\""
                    + res.outcome().name() + "\"}");
        }
    }

    /** Halt: unconditional (a delayed halt must still fence a live gate) and idempotent. */
    private void haltReported(HttpExchange x, String reason, String evidence, long now)
            throws IOException {
        String partition = config.executionPartitionId();
        GateRow row = gateStore.read(partition);
        if (row == null) {
            row = gateStore.init(new GateRow(partition, config.accountScopeId(), GateState.HALTED,
                    1L, reason.isBlank() ? "executor halt" : reason, null, null, null, null, null,
                    0L, null, null, null, now, null));
        }
        row = gateStore.halt(partition, null, reason.isBlank() ? "executor halt" : reason,
                evidence.isBlank() ? null : evidence, now);
        if (row == null) { reply(x, 404, "{\"error\":\"no gate row\"}"); return; }
        replyRow(x, 200, "HALTED", row);
    }

    /** Renew: extends the current holder's lease; a conflict is the executor's cue to halt. */
    private void renewReported(HttpExchange x, String owner, String fenceToken, long leaseMs, long now)
            throws IOException {
        String partition = config.executionPartitionId();
        if (owner.isBlank() || leaseMs <= 0 || leaseMs > MAX_GATE_LEASE_MS) {
            reply(x, 400, "{\"error\":\"owner_instance_id and lease_ms (1.." + MAX_GATE_LEASE_MS
                    + ") required\"}");
            return;
        }
        long token;
        try {
            token = Long.parseLong(fenceToken);
        } catch (NumberFormatException ex) {
            reply(x, 400, "{\"error\":\"fence_token must be numeric\"}"); return;
        }
        GateStateStore.FenceResult res = gateStore.renew(partition, owner, token, leaseMs, now);
        if (res.conflict()) {
            reply(x, 409, "{\"error\":\"" + res.reason() + "\",\"outcome\":\"FENCE_CONFLICT\"}");
            return;
        }
        replyRow(x, 200, "RENEWED", res.row());
    }

    /** The persisted row the executor adopts (state/epoch/fence/lease), plus the outcome. */
    private void replyRow(HttpExchange x, int code, String outcome, GateRow row) throws IOException {
        java.util.LinkedHashMap<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("outcome", outcome);
        out.put("state", row.state().name());
        out.put("epoch", row.epoch());
        out.put("fence_token", row.fenceToken());
        out.put("owner_instance_id", row.ownerInstanceId());
        out.put("lease_expires_ts", row.leaseExpiresTs());
        out.put("transition_ts", row.transitionTs());
        reply(x, code, mapper.writeValueAsString(out));
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
                shedActive.set(true);
                readiness.durableWrites(false,
                        "projection backlog " + inFlightNow + " > " + maxPending);
                reply(x, 503, "projection backlog exceeds MAX_PENDING_PROJECTION_RECORDS");
                return;
            }
            // C2: the request's retry budget starts here and covers every Fluss call the
            // consumer makes, instead of each of those calls getting its own. Cleared in a
            // finally because this thread will serve the next request too.
            RequestBudget.begin(config.requestBudget());
            try {
                eventConsumer.accept(v.envelope().payload());
            } finally {
                RequestBudget.clear();
            }
            reply(x, 202, "accepted");
        } catch (Exception consumerFailure) {
            // P3-077: production wiring throws IllegalStateException on
            // applier failure — without this catch the exception escapes the
            // HttpExchange handler and the client sees an aborted exchange.
            readiness.durableWrites(false, String.valueOf(consumerFailure.getMessage()));
            // P3-063: the failing write owns this false, not the shed.
            shedActive.set(false);
            reply(x, 500, "{\"error\":\"projection failed\"}");
        } finally {
            // P3-294: the decrement-then-separate-snapshot-then-set is a
            // cross-atomic check-then-act — a concurrent shed can set false
            // after our snapshot, and this blind set(true) clears a live
            // backlog. Restore via the atomic readiness gate instead.
            // P3-063: and only OUR false is restorable. The compareAndSet makes "was this
            // ours?" atomic with the drain; without it a consumer failure was cleared by the
            // same finally that ran for the failing request.
            if (projectionInFlight.decrementAndGet() == 0 && shedActive.compareAndSet(true, false)) {
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
