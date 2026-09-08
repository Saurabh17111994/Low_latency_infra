package com.trading.common.schema.execution;

import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

/**
 * Fluss-backed {@link AttemptStore} — production writer for Execution_Attempts v3 (CHG-044, T5).
 * Delegates protocol to {@link InMemoryAttemptStore} and persists rows to Fluss. Offline
 * crash-window tests run against the InMemory store; this writer shares the same column
 * mapping (20 cols, schema_version v3, gate_fence_token persisted at PREPARED).
 */
public final class FlussAttemptStore implements AttemptStore, AutoCloseable {
    private final Connection connection;
    private final Table table;
    private final long timeoutMs;
    private final InMemoryAttemptStore delegate;

    private FlussAttemptStore(Connection connection, Table table, long timeoutMs, Runnable haltCallback) {
        this.connection = connection;
        this.table = table;
        this.timeoutMs = timeoutMs;
        this.delegate = new InMemoryAttemptStore(haltCallback);
    }

    public static FlussAttemptStore open(String bootstrap, String database, String tableName, Duration timeout, Runnable haltCallback) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection conn = ConnectionFactory.createConnection(conf);
        try {
            Table t = conn.getTable(TablePath.of(database, tableName));
            return new FlussAttemptStore(conn, t, timeout.toMillis(), haltCallback);
        } catch (Exception e) { conn.close(); throw e; }
    }

    @Override public void close() throws Exception { connection.close(); }

    /** Raw durable lookup by the deterministic execution_attempt_id. */
    private AttemptRecord lookup(String executionAttemptId) {
        try {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow r = lookuper.lookup(GenericRow.of(BinaryString.fromString(executionAttemptId)))
                    .get(timeoutMs, TimeUnit.MILLISECONDS).getSingletonRow();
            return r == null ? null : fromRow(r);
        } catch (Exception e) { return null; }
    }

    /** If the durable store already holds this attempt but this process has not seen it,
     *  hydrate it so identity / duplicate / transition checks observe it. */
    private void hydrateIfAbsent(String executionAttemptId) {
        if (delegate.attemptById(executionAttemptId) == null) {
            AttemptRecord durable = lookup(executionAttemptId);
            if (durable != null) delegate.hydrate(durable);
        }
    }

    private static AttemptRecord fromRow(InternalRow r) {
        String aid = r.getString(ExecutionAttemptsColumns.EXECUTION_ATTEMPT_ID).toString();
        String acct = r.getString(ExecutionAttemptsColumns.ACCOUNT_SCOPE_ID).toString();
        String instr = r.getString(ExecutionAttemptsColumns.INSTRUCTION_ID).toString();
        String action = r.isNullAt(ExecutionAttemptsColumns.ACTION_ID)
                ? null : r.getString(ExecutionAttemptsColumns.ACTION_ID).toString();
        String part = r.getString(ExecutionAttemptsColumns.EXECUTION_PARTITION_ID).toString();
        String reqHash = r.getString(ExecutionAttemptsColumns.REQUEST_HASH).toString();
        String cref = r.getString(ExecutionAttemptsColumns.CLIENT_ORDER_REF).toString();
        String brokerId = r.isNullAt(ExecutionAttemptsColumns.BROKER_ORDER_ID)
                ? null : r.getString(ExecutionAttemptsColumns.BROKER_ORDER_ID).toString();
        long gateEpoch = r.getLong(ExecutionAttemptsColumns.GATE_EPOCH);
        String phase = r.getString(ExecutionAttemptsColumns.PHASE).toString();
        long phaseEpoch = r.getLong(ExecutionAttemptsColumns.PHASE_EPOCH);
        String outcome = r.isNullAt(ExecutionAttemptsColumns.OUTCOME)
                ? null : r.getString(ExecutionAttemptsColumns.OUTCOME).toString();
        String outcomeDetail = r.isNullAt(ExecutionAttemptsColumns.OUTCOME_DETAIL)
                ? null : r.getString(ExecutionAttemptsColumns.OUTCOME_DETAIL).toString();
        long preparedTs = r.getLong(ExecutionAttemptsColumns.PREPARED_TS);
        Long submittedTs = r.isNullAt(ExecutionAttemptsColumns.SUBMITTED_TS)
                ? null : r.getLong(ExecutionAttemptsColumns.SUBMITTED_TS);
        Long terminalTs = r.isNullAt(ExecutionAttemptsColumns.TERMINAL_TS)
                ? null : r.getLong(ExecutionAttemptsColumns.TERMINAL_TS);
        String summary = r.isNullAt(ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY)
                ? null : r.getString(ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY).toString();
        int retry = r.getInt(ExecutionAttemptsColumns.RETRY_ATTEMPT);
        // P4-042 fail-closed: v3 declares gate_fence_token NOT NULL, so a null
        // here is a legacy/pre-v3 row — fail rather than decode fence 0 (every
        // other nullable column above guards with isNullAt; this one must not
        // silently mint a never-acquired fence).
        if (r.isNullAt(ExecutionAttemptsColumns.GATE_FENCE_TOKEN)) {
            throw new IllegalStateException("legacy Execution_Attempts row without gate_fence_token: " + aid);
        }
        long fence = r.getLong(ExecutionAttemptsColumns.GATE_FENCE_TOKEN);
        String sv = r.getString(ExecutionAttemptsColumns.SCHEMA_VERSION).toString();
        return new AttemptRecord(aid, acct, instr, action, part, reqHash, cref, fence, brokerId,
                gateEpoch, phase, phaseEpoch, outcome, outcomeDetail, preparedTs, submittedTs,
                terminalTs, summary, retry, sv);
    }

    @Override public PrepareResult prepare(PrepareRequest request) {
        // Restart-refresh: if this (deterministic) attempt already persisted durably, hydrate it
        // first so a restarted process rebuilds its duplicate index and returns DUPLICATE instead
        // of minting a second PREPARED (the crash-window exactly-once guarantee on the durable store).
        hydrateIfAbsent(request.executionAttemptId());
        PrepareResult r = delegate.prepare(request);
        if (r.status() == AttemptStore.Status.CREATED) persist(r.record());
        return r;
    }
    @Override public TransitionResult transition(String id, long epoch, String phase) {
        hydrateIfAbsent(id);
        TransitionResult r = delegate.transition(id, epoch, phase);
        if (r.outcome() == AttemptStore.TransitionOutcome.APPLIED) persist(r.record());
        return r;
    }
    @Override public TransitionResult resolveUnknown(String id, long epoch, String phase) {
        hydrateIfAbsent(id);
        TransitionResult r = delegate.resolveUnknown(id, epoch, phase);
        if (r.outcome() == AttemptStore.TransitionOutcome.APPLIED) persist(r.record());
        return r;
    }

    private void persist(AttemptRecord rec) {
        try {
            Object[] v = new Object[ExecutionAttemptsColumns.FIELD_COUNT];
            v[ExecutionAttemptsColumns.EXECUTION_ATTEMPT_ID] = BinaryString.fromString(rec.executionAttemptId());
            v[ExecutionAttemptsColumns.ACCOUNT_SCOPE_ID] = BinaryString.fromString(rec.accountScopeId());
            v[ExecutionAttemptsColumns.INSTRUCTION_ID] = BinaryString.fromString(rec.instructionId());
            v[ExecutionAttemptsColumns.ACTION_ID] = rec.actionId() == null ? null : BinaryString.fromString(rec.actionId());
            v[ExecutionAttemptsColumns.EXECUTION_PARTITION_ID] = BinaryString.fromString(rec.executionPartitionId());
            v[ExecutionAttemptsColumns.REQUEST_HASH] = BinaryString.fromString(rec.requestHash());
            v[ExecutionAttemptsColumns.CLIENT_ORDER_REF] = BinaryString.fromString(rec.clientOrderRef());
            v[ExecutionAttemptsColumns.BROKER_ORDER_ID] = rec.brokerOrderId() == null ? null : BinaryString.fromString(rec.brokerOrderId());
            v[ExecutionAttemptsColumns.GATE_EPOCH] = rec.gateEpoch();
            v[ExecutionAttemptsColumns.PHASE] = BinaryString.fromString(rec.phase());
            v[ExecutionAttemptsColumns.PHASE_EPOCH] = rec.phaseEpoch();
            v[ExecutionAttemptsColumns.OUTCOME] = rec.outcome() == null ? null : BinaryString.fromString(rec.outcome());
            v[ExecutionAttemptsColumns.OUTCOME_DETAIL] = rec.outcomeDetail() == null ? null : BinaryString.fromString(rec.outcomeDetail());
            v[ExecutionAttemptsColumns.PREPARED_TS] = rec.preparedTs();
            v[ExecutionAttemptsColumns.SUBMITTED_TS] = rec.submittedTs() == null ? null : rec.submittedTs();
            v[ExecutionAttemptsColumns.TERMINAL_TS] = rec.terminalTs() == null ? null : rec.terminalTs();
            v[ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY] = rec.brokerResponseSummary() == null ? null : BinaryString.fromString(rec.brokerResponseSummary());
            v[ExecutionAttemptsColumns.RETRY_ATTEMPT] = rec.retryAttempt();
            v[ExecutionAttemptsColumns.GATE_FENCE_TOKEN] = rec.gateFenceToken();
            v[ExecutionAttemptsColumns.SCHEMA_VERSION] = BinaryString.fromString(ExecutionAttemptsColumns.SCHEMA_VERSION_V3);
            UpsertWriter w = table.newUpsert().createWriter();
            try { w.upsert(GenericRow.of(v)).get(timeoutMs, TimeUnit.MILLISECONDS); } finally { w.flush(); }
        } catch (Exception ignored) {}
    }

    // Delegated test seams
    public AttemptRecord attemptById(String id) { return delegate.attemptById(id); }
    public int size() { return delegate.size(); }
}
