package com.trading.execution.gateway;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

/** Explicit serializers for normalized execution images and independent writes. */
public final class FlussProjectionWriter implements ProjectionWriter {
    private final Connection connection;
    private final GatewayConfig config;
    private final Duration timeout;
    private final Map<String, Table> tables = new HashMap<>();
    private final PostbackQuarantineStore quarantineStore;

    public static FlussProjectionWriter open(GatewayConfig config) {
        try {
            Configuration c = new Configuration(); c.setString("bootstrap.servers", config.flussBootstrap());
            return new FlussProjectionWriter(ConnectionFactory.createConnection(c), config,
                    config.requestTimeout());
        } catch (Exception e) { throw new IllegalStateException("cannot open projection writer", e); }
    }

    public static FlussProjectionWriter open(GatewayConfig config, PostbackQuarantineStore quarantineStore) {
        try {
            Configuration c = new Configuration(); c.setString("bootstrap.servers", config.flussBootstrap());
            return new FlussProjectionWriter(ConnectionFactory.createConnection(c), config,
                    config.requestTimeout(), quarantineStore);
        } catch (Exception e) { throw new IllegalStateException("cannot open projection writer", e); }
    }

    FlussProjectionWriter(Connection connection, GatewayConfig config, Duration timeout) {
        this(connection, config, timeout, null);
    }

    FlussProjectionWriter(Connection connection, GatewayConfig config, Duration timeout,
                          PostbackQuarantineStore quarantineStore) {
        this.connection = connection; this.config = config; this.timeout = timeout;
        this.quarantineStore = quarantineStore;
    }

    @Override public void writeAudit(NormalizedExecutionEvent e) throws Exception {
        if (e.audit() != null) append("Execution_Audit", auditRow(e));
    }
    @Override public void writeLifecycle(NormalizedExecutionEvent e) throws Exception {
        if (e.fill() != null) append("Fills", fillRow(e));
        if (e.lifecycle() != null) upsert("Order_Lifecycle", lifecycleRow(e));
        if (e.correlation() != null) upsert("Order_Correlation", correlationRow(e));
    }
    @Override public void writePosition(NormalizedExecutionEvent e) throws Exception {
        if (e.position() != null) {
            upsert("Positions", positionRow(e));
            upsert("Position_State", positionStateRow(e));
        }
    }

    /**
     * Tier 0 #6 quarantine path — fail-closed bijective guard.
     * Appends an immutable row to Postback_Quarantine LOG.
     * Caller must halt the affected scope after calling this (no further
     * lifecycle/position writes for the same postbackEventId).
     *
     * <p>If a {@link PostbackQuarantineStore} was supplied, delegates to it;
     * otherwise uses the same inline Fluss append pattern as {@link #writeAudit}.
     */
    public void writeQuarantine(NormalizedExecutionEvent e, String reason) throws Exception {
        String evidenceSummary = reason;
        byte[] rawPayload = e.fill() != null && e.fill().originalPayload() != null
                ? e.fill().originalPayload() : new byte[0];
        if (quarantineStore != null) {
            quarantineStore.quarantine(e.postbackEventId(), reason, evidenceSummary, rawPayload);
            return;
        }
        append("Postback_Quarantine", quarantineRow(e, reason, evidenceSummary, rawPayload));
    }

    /**
     * Direct quarantine API for callers that already have the four store fields.
     * Mirrors {@link PostbackQuarantineStore#quarantine} and also appends to
     * Postback_Quarantine via the same Fluss append path when no store is wired.
     */
    public void writeQuarantine(String postbackEventId, String reason, String evidenceSummary, byte[] rawPayload) throws Exception {
        if (quarantineStore != null) {
            quarantineStore.quarantine(postbackEventId, reason, evidenceSummary, rawPayload);
            return;
        }
        // Build a minimal quarantine row when only the four store fields are known.
        // Fabricate a minimal NormalizedExecutionEvent envelope for row mapping.
        NormalizedExecutionEvent minimal = new NormalizedExecutionEvent(
                postbackEventId, config.accountScopeId(), config.executionPartitionId(),
                0L, "gateway", "QUARANTINE", System.currentTimeMillis(),
                null,
                new NormalizedExecutionEvent.Fill("unknown", "2", null, null, null, null, "UNKNOWN",
                        0L, 0L, null, null, null, null, 0L, 0L, rawPayload, "unknown", "QUARANTINED", reason, "2"),
                null, null, null);
        append("Postback_Quarantine", quarantineRow(minimal, reason, evidenceSummary, rawPayload));
    }

    private void append(String name, GenericRow row) throws Exception {
        AppendWriter writer = table(name).newAppend().createWriter();
        try { writer.append(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        finally { writer.flush(); }
    }
    private void upsert(String name, GenericRow row) throws Exception {
        UpsertWriter writer = table(name).newUpsert().createWriter();
        try { writer.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        finally { writer.flush(); }
    }

    private GenericRow auditRow(NormalizedExecutionEvent e) {
        var a = e.audit();
        return GenericRow.of(bs(a.auditEventId()), bs(e.eventType()), bs(nullable(e.correlation(), true)),
                bs(nullableAttempt(e)), bs(e.executionPartitionId()), bs(e.accountScopeId()), e.gateEpoch(),
                bs(e.actorId()), bs(a.evidenceHash()), bs(a.evidenceSummary()), e.eventTs(), bs("2"));
    }
    private GenericRow fillRow(NormalizedExecutionEvent e) {
        var f = e.fill();
        return GenericRow.of(bs(e.postbackEventId()), bs(f.postbackFingerprint()), bs(f.fingerprintVersion()),
                bs(e.accountScopeId()), bs(f.brokerOrderId()), bs(f.instructionId()), bs(f.executionAttemptId()),
                bs(f.tradeContextId()), bs(f.orderStatus()), f.cumulativeQty(), f.pendingQty(), f.fillQty(),
                f.fillPricePaise(), bs(f.fillId()), f.brokerEventTime(), f.receiveTime(), f.ingestTs(),
                f.originalPayload() == null ? new byte[0] : f.originalPayload(), bs(f.payloadHash()),
                bs(f.correlationState()), bs(f.correlationReason()), bs(f.decoderVersion()), bs("2"));
    }
    private GenericRow lifecycleRow(NormalizedExecutionEvent e) {
        var l = e.lifecycle();
        return GenericRow.of(bs(e.accountScopeId()), bs(l.brokerOrderId()), bs(l.instructionId()),
                bs(l.executionAttemptId()), bs(l.tradeContextId()), bs(l.normalizedState()), l.cumulativeQty(),
                l.pendingQty(), l.averageFillPricePaise(), bs(e.postbackEventId()), l.sourceVersion(),
                l.sourceEventTime(), l.lastReceiveTime(), bs(l.correlationState()), bs("2"));
    }
    private GenericRow positionRow(NormalizedExecutionEvent e) {
        var p = e.position();
        return GenericRow.of(bs(p.positionId()), bs(p.tradeContextId()), bs(e.accountScopeId()), p.instrumentToken(),
                bs(p.exchange()), bs(p.symbol()), bs(p.side()), bs(p.state()), p.openQuantity(), p.closedQuantity(),
                p.averageEntryPaise(), p.averageExitPaise(), bs(e.postbackEventId()), p.sourceVersion(),
                p.createdTs(), p.lastUpdateTs(), bs("2"));
    }
    private GenericRow correlationRow(NormalizedExecutionEvent e) {
        var c = e.correlation();
        return GenericRow.of(bs(c.instructionId()), bs(c.executionAttemptId()), bs(e.accountScopeId()),
                bs(c.clientOrderRef()), bs(c.brokerOrderId()), bs(c.tradeContextId()), bs(c.positionId()),
                bs(c.verificationState()), bs(c.verificationEvidence()), c.correlatedTs(), bs("2"));
    }

    /**
     * Postback_Quarantine LOG row (13 cols, 16_postback_quarantine.sql).
     * Uses BinaryString for STRING cols and BYTES for payload, same pattern as auditRow/fillRow.
     */
    private GenericRow quarantineRow(NormalizedExecutionEvent e, String reason, String evidenceSummary, byte[] rawPayload) {
        String quarantineId = "q-" + e.postbackEventId();
        String payloadHash = null;
        String brokerOrderId = null;
        String instructionId = null;
        String correlationAttempt = null;
        if (e.fill() != null) {
            payloadHash = e.fill().payloadHash();
            brokerOrderId = e.fill().brokerOrderId();
            instructionId = e.fill().instructionId();
            correlationAttempt = e.fill().executionAttemptId();
        }
        if (brokerOrderId == null && e.correlation() != null) brokerOrderId = e.correlation().brokerOrderId();
        if (instructionId == null && e.correlation() != null) instructionId = e.correlation().instructionId();
        if (correlationAttempt == null && e.correlation() != null) correlationAttempt = e.correlation().executionAttemptId();
        if (payloadHash == null && e.audit() != null) payloadHash = e.audit().evidenceHash();
        if (payloadHash == null) payloadHash = "unknown";
        long quarantinedTs = e.eventTs() != 0 ? e.eventTs() : System.currentTimeMillis();
        Object[] v = new Object[13];
        v[0] = bs(quarantineId);
        v[1] = bs(e.postbackEventId());
        v[2] = bs(reason);
        v[3] = rawPayload == null ? new byte[0] : rawPayload;
        v[4] = bs(payloadHash);
        v[5] = bs(brokerOrderId);
        v[6] = bs(instructionId);
        v[7] = bs(correlationAttempt);
        v[8] = bs("OPEN");
        v[9] = bs(evidenceSummary);
        v[10] = quarantinedTs;
        v[11] = null;
        v[12] = bs("2");
        return GenericRow.of(v);
    }

    /**
     * Position_State handshake (Option B, max-one-active): per-instrument
     * lifecycle signal for Flink's ActiveSignalFeedbackFunction. Sole
     * steady-state writer is this gateway (Nautilus feedback); ops ADMIN_CLEAR
     * break-glass (runbook) is the only authorized second writer (P4-238).
     * Maps Positions state to OPEN/CLOSED: FLAT/CLOSED -> CLOSED, else OPEN.
     * No TTL — Flink clears only on CLOSED/ADMIN_CLEAR.
     *
     * <p>P4-088: unknown Positions states are NOT defaulted to OPEN (a typo
     * must not block an instrument forever) — the caller must quarantine +
     * halt. This method throws on any state outside OPEN/CLOSED/FLAT (the
     * gateway's known vocabulary); ADMIN_CLEAR never originates here.
     */
    private GenericRow positionStateRow(NormalizedExecutionEvent e) {
        var p = e.position();
        String raw = p.state() == null ? null : p.state().trim().toUpperCase();
        if (!"OPEN".equals(raw) && !"CLOSED".equals(raw) && !"FLAT".equals(raw)) {
            throw new IllegalArgumentException(
                    "Position_State: unknown Positions state '" + p.state()
                            + "' — quarantine + halt, never default to OPEN (P4-088)");
        }
        boolean isClosed = "CLOSED".equals(raw) || "FLAT".equals(raw);
        String status = isClosed ? "CLOSED" : "OPEN";
        Long closedTs = isClosed ? p.lastUpdateTs() : null;
        String closedReason = isClosed ? raw : null;
        // GenericRow.of with boxed Long nulls needs explicit null handling: use Object[] path
        // For Fluss GenericRow, null boxed is okay — upsert handles nullable BIGINT.
        // P4-005 v2: (account_scope_id, instrument_token, ...) — PK prefix first.
        return GenericRow.of(bs(e.accountScopeId()), p.instrumentToken(),
                bs(status), bs(p.positionId()), p.lastUpdateTs(),
                closedTs, bs(closedReason), p.sourceVersion(), bs("2"));
    }
    private static String nullable(NormalizedExecutionEvent.Correlation c, boolean instruction) {
        return c == null ? null : c.instructionId();
    }
    private static String nullableAttempt(NormalizedExecutionEvent e) {
        return e.correlation() == null ? null : e.correlation().executionAttemptId();
    }
    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }
    private Table table(String name) { return tables.computeIfAbsent(name,
            n -> connection.getTable(TablePath.of(config.flussDatabase(), n))); }
    @Override public void close() throws Exception { for (Table t : tables.values()) t.close(); connection.close(); }
}
