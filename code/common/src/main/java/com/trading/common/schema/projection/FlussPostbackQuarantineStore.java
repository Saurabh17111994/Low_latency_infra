package com.trading.common.schema.projection;

import com.trading.common.schema.fluss.FlussHandlePool;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

/**
 * Fluss-backed immutable {@link PostbackQuarantineStore} for Postback_Quarantine (T1, closes in
 * WP-4 step 3). Mirrors {@link FlussProjectionLedgerStore}: keeps the in-memory view for
 * {@link #all()} but persists each append to the LOG table via {@link AppendWriter}. Row layout
 * follows 16_postback_quarantine.sql (13 cols). Live durability is proven by the env-gated
 * {@code FlussPostbackQuarantineStoreIntegrationTest} (log-scan read-back).
 *
 * <p>P3-169: {@link #all()} is a process-local view — open() does not hydrate
 * the delegate, so rows appended by earlier processes stay invisible until a
 * scan-backed read path exists. Restart recovery must log-scan
 * Postback_Quarantine (BatchScanner/LogScanner over all buckets, same drain
 * shape as the gateway ledger stores) rather than trust all(); do not
 * auto-hydrate the unbounded LOG into heap on open (bounded-state rule).
 */
public final class FlussPostbackQuarantineStore implements PostbackQuarantineStore, AutoCloseable {
    private final Connection connection;
    private final Table table;
    private final long timeoutMs;
    private final InMemoryPostbackQuarantineStore delegate = new InMemoryPostbackQuarantineStore();
    // P3-502: reuse the append writer rather than minting one per quarantine write.
    // Created in the constructor: the factory closes over `table`, which must be assigned first.
    private final FlussHandlePool<AppendWriter> appenders;

    private FlussPostbackQuarantineStore(Connection connection, Table table, long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.timeoutMs = timeoutMs;
        this.appenders = new FlussHandlePool<>(() -> this.table.newAppend().createWriter());
    }

    public static FlussPostbackQuarantineStore open(String bootstrap, String database,
                                                    String tableName, Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        // D1: bulk-path linger via FlussWriteProfiles (quarantine appends are not order-critical).
        com.trading.common.schema.fluss.FlussWriteProfiles.bulkPath(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            Table table = connection.getTable(TablePath.of(database, tableName));
            return new FlussPostbackQuarantineStore(connection, table, timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    // P3-396: close the Table handle too — connection-only close leaks tablets.
    @Override public void close() throws Exception {
        // P3-502: drop the pooled writer first — nothing to close (AppendWriter is not
        // Closeable), but retaining it past the table close leaves a dead reference.
        appenders.clear();
        try { if (table != null) table.close(); }
        finally { connection.close(); }
    }

    @Override public void append(QuarantinedPostback row) throws Exception {
        Object[] v = new Object[13];
        // P3-167: build the row and persist FIRST — delegate only on success.
        // Memory-first diverges all() from durable state on timeout/failure
        // (retry then double-adds to memory while Fluss has 0 or 1 row).
        // P3-168: DDL marks postback/broker/instruction/attempt/reason/...
        // nullable — BinaryString.fromString(null) NPEs, so null-check first.
        v[0] = bs(row.quarantineId());
        v[1] = bs(row.postbackEventId());
        v[2] = row.reason() == null ? null : bs(row.reason().name());
        v[3] = row.originalPayload() == null ? new byte[0] : row.originalPayload();
        v[4] = bs(row.payloadHash());
        v[5] = bs(row.brokerOrderId());
        v[6] = bs(row.instructionId());
        v[7] = bs(row.correlationAttempt());
        v[8] = bs(row.disposition());
        v[9] = bs(row.dispositionReason());
        v[10] = row.quarantinedTs();
        v[11] = row.dispositionTs() == null ? null : row.dispositionTs();
        v[12] = bs(row.schemaVersion());
        // P3-502: pooled writer instead of one per append. TableWriter is not Closeable in
        // Fluss 0.9.1 (flush only), so there is no handle to close — only one to reuse, and
        // the pool never lends the same handle to two callers.
        // D1: no per-record flush — see FlussWriteProfiles; .get() still returns only
        // on a durable ack.
        appenders.with(writer -> {
            writer.append(GenericRow.of(v)).get(timeoutMs, TimeUnit.MILLISECONDS);
            return null;
        });
        delegate.append(row);
    }

    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }

    @Override public List<QuarantinedPostback> all() { return delegate.all(); }
}
