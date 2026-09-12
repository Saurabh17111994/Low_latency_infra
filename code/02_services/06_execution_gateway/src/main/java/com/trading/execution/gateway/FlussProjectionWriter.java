package com.trading.execution.gateway;

import com.trading.common.schema.fluss.BoundedRetry;
import com.trading.common.schema.fluss.FlussHandlePool;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    // P3-070: ConcurrentHashMap — the public ProjectionWriter API has no
    // single-thread confinement; GatewayHttpServer drives concurrent applies.
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final PostbackQuarantineStore quarantineStore;
    // P3-071: in-run guard for Fills append idempotency — retry after a crash
    // between the Fills append and the ledger put must not duplicate the LOG
    // row. Fluss LOG has no PK, so dedup by fingerprint before re-appending.
    private final Set<String> appendedFingerprints = ConcurrentHashMap.newKeySet();
    // P3-072: per-table pools of reusable writers. Same concurrent reasoning as `tables`
    // (P3-070) — a single cached writer would be a cross-call hazard on this path.
    private final Map<String, FlussHandlePool<AppendWriter>> appendPools = new ConcurrentHashMap<>();
    private final Map<String, FlussHandlePool<UpsertWriter>> upsertPools = new ConcurrentHashMap<>();

    public static FlussProjectionWriter open(GatewayConfig config) {
        try {
            Configuration c = new Configuration(); c.setString("bootstrap.servers", config.flussBootstrap());
            // D1: bulk-path linger via FlussWriteProfiles (throughput-leaning, not order-critical).
            com.trading.common.schema.fluss.FlussWriteProfiles.bulkPath(c);
            return new FlussProjectionWriter(ConnectionFactory.createConnection(c), config,
                    config.requestTimeout());
        } catch (Exception e) { throw new IllegalStateException("cannot open projection writer", e); }
    }

    public static FlussProjectionWriter open(GatewayConfig config, PostbackQuarantineStore quarantineStore) {
        try {
            Configuration c = new Configuration(); c.setString("bootstrap.servers", config.flussBootstrap());
            // D1: bulk-path linger via FlussWriteProfiles (throughput-leaning, not order-critical).
            com.trading.common.schema.fluss.FlussWriteProfiles.bulkPath(c);
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
        // P3-071: Fills is an append-only LOG with no PK — re-appending after the
        // ledger put failed would duplicate the row on retry. Skip when this
        // writer already appended this fingerprint in-run; cross-restart replay
        // is absorbed downstream by fingerprint-version dedup on Fills.
        if (e.fill() != null && e.fill().postbackFingerprint() != null
                && !appendedFingerprints.add(e.fill().postbackFingerprint())) {
            // Already appended this run — still apply the idempotent upserts.
        } else if (e.fill() != null) {
            append("Fills", fillRow(e));
        }
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
        Objects.requireNonNull(e.postbackEventId(), "postbackEventId");
        Objects.requireNonNull(reason, "reason");
        // P3-284: keep the human evidence, not the machine reason — the store
        // contract expects distinct (reason, evidenceSummary) fields.
        String evidenceSummary = e.audit() != null && e.audit().evidenceSummary() != null
                ? e.audit().evidenceSummary() : reason;
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
        // P3-285: fail fast — "q-null" masks null as a colliding key and null
        // reason violates Postback_Quarantine.reason NOT NULL only at the server.
        Objects.requireNonNull(postbackEventId, "postbackEventId");
        Objects.requireNonNull(reason, "reason");
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
        // Note this connection holds a Map<String,Table> (projection + quarantine + others),
        // so a per-record flush here would also force and await its sibling tables'.
        // D1: no per-record flush — see FlussWriteProfiles.
        // P3-072: pooled per-table writer instead of one per call. Writers are @NotThreadSafe
        // with no single-thread contract here, so a single cached field would be a cross-call
        // hazard; the pool lends one per caller and drops a handle whose call failed.
        var pool = appendPools.computeIfAbsent(name,
                n -> new FlussHandlePool<>(() -> table(n).newAppend().createWriter()));
        if ("Fills".equals(name)) {
            // retry-exempt: P3-268 Fills is deliberately NOT retried. It is a LOG table whose
            // only duplicate guard is the in-process appendedFingerprints set; the claimed
            // downstream
            // fingerprint-version dedup is UNVERIFIED in this repo, so a retry after a
            // TimeoutException could duplicate a fill. Retrying Execution_Audit and
            // Postback_Quarantine is acceptable — duplicates there are detectable evidence
            // records, not order state.
            pool.with(writer -> {
                // retry-exempt: see the P3-268 note above — Fills duplicates are not
                // tolerably deduped yet, so this site must NOT be retried.
                writer.append(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                return null;
            });
            return;
        }
        // C5: ride out the first-write-after-CREATE window (2.2-3.7s measured; see BoundedRetry)
        // rather than failing on the caller's 2s budget. The pool drops a failed handle, so
        // each attempt gets a fresh writer.
        RequestBudget.run(() -> pool.with(writer -> {
            writer.append(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return null;
        }));
    }
    private void upsert(String name, GenericRow row) throws Exception {
        // P3-072: pooled per-table writer — see append().
        var pool = upsertPools.computeIfAbsent(name,
                n -> new FlussHandlePool<>(() -> table(n).newUpsert().createWriter()));
        // P3-268: KV upserts are idempotent by primary key, so a retry cannot duplicate state.
        // C5: this is what lets a first write to a freshly created table ride out the measured
        // 2.2-3.7s window (see BoundedRetry) instead of failing on the caller's 2s budget.
        RequestBudget.run(() -> pool.with(writer -> {
            writer.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return null;
        }));
    }

    private GenericRow auditRow(NormalizedExecutionEvent e) {
        var a = e.audit();
        return GenericRow.of(bs(a.auditEventId()), bs(e.eventType()), bs(nullableInstruction(e.correlation())),
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
        // P3-285: fail fast here too — this row builder is also reachable from
        // the (event, reason) overload, not only the guarded 4-arg overload.
        Objects.requireNonNull(e.postbackEventId(), "postbackEventId");
        Objects.requireNonNull(reason, "reason");
        String quarantineId = "q-" + e.postbackEventId() + "-" + java.util.UUID.randomUUID();
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
        // P3-286: already fail-closed — unknown Positions states throw here
        // (quarantine + halt upstream) instead of defaulting to OPEN, which
        // would let a typo block Flink signals forever (no TTL). Pinned, not
        // re-fixed.
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
    // P3-287: single-purpose accessor — the old (c, boolean) selector ignored
    // its flag and always returned instructionId, so a future false-call would
    // silently map the wrong column. Only call site needs instruction_id.
    private static String nullableInstruction(NormalizedExecutionEvent.Correlation c) {
        return c == null ? null : c.instructionId();
    }
    private static String nullableAttempt(NormalizedExecutionEvent e) {
        return e.correlation() == null ? null : e.correlation().executionAttemptId();
    }
    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }
    private Table table(String name) { return tables.computeIfAbsent(name,
            n -> connection.getTable(TablePath.of(config.flussDatabase(), n))); }

    /** Log tables this writer appends to. Mirrors the {@code append(...)} call sites below. */
    private static final List<String> APPEND_TABLES =
            List.of("Fills", "Execution_Audit");

    /** Keyed tables this writer upserts into. Mirrors the {@code upsert(...)} call sites below. */
    private static final List<String> UPSERT_TABLES =
            List.of("Positions", "Position_State", "Order_Lifecycle", "Order_Correlation");

    /**
     * C1: pay the post-CREATE window HERE, at startup, instead of on the first user request.
     *
     * <p>Row-free by construction. Resolving each table's handle and minting its writer is
     * everything the request path does before it touches a row, so this warms exactly the work
     * the first request would otherwise pay for, and writes nothing: pre-warm must not put
     * probe rows into authoritative tables.
     *
     * <p>Measured basis: the first write to a freshly created table costs 2.2-3.7s against the
     * caller's 2s bound, while steady state is ~120ms and every write after the first is
     * 9-29ms. That window is why the retry in {@link BoundedRetry} is load-bearing on the
     * request path; moving the cost to startup is what lets it go back to being insurance.
     *
     * <p>A table added to the append/upsert call sites without being added to the lists above
     * is simply not pre-warmed - a missed optimisation, not a correctness gap.
     */
    public void prewarm() throws Exception {
        for (String name : APPEND_TABLES) {
            appendPools.computeIfAbsent(name,
                    n -> new FlussHandlePool<>(() -> table(n).newAppend().createWriter()))
                    .with(writer -> null);
        }
        for (String name : UPSERT_TABLES) {
            upsertPools.computeIfAbsent(name,
                    n -> new FlussHandlePool<>(() -> table(n).newUpsert().createWriter()))
                    .with(writer -> null);
        }
        // Handle warming alone leaves a residual: the window is consumed by the first RPC that
        // actually reaches the bucket, and a read does that as well as a write (C1 probe:
        // read 507-510ms then a 109ms write, on the same fresh shape). One row-free lookup of a
        // key that cannot exist is therefore part of the warm-up - it writes nothing.
        //
        // Retried deliberately: a read that FAILS returns in ~100ms and consumes nothing, so an
        // unretried pre-warm can report success having warmed nothing at all (same probe, 3/8
        // iterations). BoundedRetry is the existing tool for exactly that transient.
        Table kv = table("Positions");
        // Lookuper is not Closeable in Fluss 0.9.1 (FlussProjectionLedgerStore notes the same),
        // so there is no handle to close - this is a one-shot startup read.
        RequestBudget.run(() -> kv.newLookup().createLookuper()
                .lookup(GenericRow.of(BinaryString.fromString(ABSENT_PREWARM_KEY)))
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS));
    }

    /**
     * Cannot collide with a real projection row: positions are keyed by ids minted downstream,
     * and this lookup is a read - the value returned (null) is discarded.
     */
    private static final String ABSENT_PREWARM_KEY = "__prewarm_absent_key__";
    // P3-288: collect, don't abort — snapshot under concurrency (P3-070),
    // always close the connection, and clear so close is idempotent.
    @Override public void close() throws Exception {
        // P3-072: drop pooled writers first — TableWriter is not Closeable (only flush()),
        // so there is nothing to close, but holding them past the table close would leave
        // dead references behind.
        appendPools.values().forEach(FlussHandlePool::clear);
        appendPools.clear();
        upsertPools.values().forEach(FlussHandlePool::clear);
        upsertPools.clear();
        Exception failure = null;
        for (Table t : List.copyOf(tables.values())) {
            try { t.close(); } catch (Exception e) {
                if (failure == null) failure = e; else failure.addSuppressed(e);
            }
        }
        tables.clear();
        try { connection.close(); } catch (Exception e) {
            if (failure == null) failure = e; else failure.addSuppressed(e);
        }
        if (failure != null) throw failure;
    }
}
