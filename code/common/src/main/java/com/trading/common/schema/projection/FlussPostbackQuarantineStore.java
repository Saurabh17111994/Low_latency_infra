package com.trading.common.schema.projection;

import com.trading.common.schema.fluss.BoundedRetry;

import com.trading.common.schema.fluss.FlussHandlePool;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Fluss-backed immutable {@link PostbackQuarantineStore} for Postback_Quarantine (T1, closes in
 * WP-4 step 3). Persists each append to the LOG table via {@link AppendWriter}; row layout follows
 * 16_postback_quarantine.sql (13 cols). Live durability is proven by the env-gated
 * {@code FlussPostbackQuarantineStoreIntegrationTest}.
 *
 * <p>P3-410/P3-169: this class used to keep an in-memory {@link InMemoryPostbackQuarantineStore}
 * mirror of every append and serve reads from it. That made the process heap grow with the
 * quarantine LOG for the life of the process, and the mirror was process-local — rows appended by
 * an earlier process stayed invisible until a restart, so the read could disagree with durable
 * truth in exactly the situation (post-incident review) where it matters. The mirror is gone:
 * {@link #scan} reads the table itself, bounded by the caller's limit, and nothing is hydrated
 * on open.
 */
public final class FlussPostbackQuarantineStore implements PostbackQuarantineStore, AutoCloseable {
    private final Connection connection;
    private final Table table;
    private final long timeoutMs;
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
        // P3-502 / CHG-223: close the pool first — nothing to close on the handle
        // (AppendWriter is not Closeable), but retaining it past the table close leaves a dead
        // reference, and a handle returned after close must not be pooled for a later borrower.
        appenders.close();
        try { if (table != null) table.close(); }
        finally { connection.close(); }
    }

    @Override public void append(QuarantinedPostback row) throws Exception {
        Object[] v = new Object[13];
        // P3-167: build the row and persist FIRST, and expose nothing afterwards —
        // there is no process-local view left that could disagree with durable state.
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
        // Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
        // Fluss 0.9.1 (flush only), so there is no handle to close — only one to reuse, and
        // the pool never lends the same handle to two callers.
        // D1: no per-record flush — see FlussWriteProfiles; .get() still returns only
        // on a durable ack.
        // C5: retry the quarantine append. This is a LOG append, so a retry CAN duplicate a
        // row — acceptable here because quarantine records are detectable evidence rather
        // than order state (contrast Fills, deliberately left un-retried until its claimed
        // downstream fingerprint dedup is verified).
        BoundedRetry.await(() -> appenders.with(writer -> {
            writer.append(GenericRow.of(v)).get(timeoutMs, TimeUnit.MILLISECONDS);
            return null;
        }));
    }

    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }

    /**
     * P3-410/P3-169: a bounded scan of the durable LOG — see the interface contract.
     *
     * <p>Drains each bucket with the same page-until-empty shape as the gateway ledger stores
     * (P3-003: one {@code pollBatch} is a time-bounded PAGE, not the whole bucket, so stopping at
     * the first page would silently truncate). Collection stops as soon as {@code limit} rows are
     * read, so memory is bounded by the limit even though the LOG is not.
     */
    @Override
    public List<QuarantinedPostback> scan(int limit) throws Exception {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive, got " + limit);
        List<QuarantinedPostback> out = new ArrayList<>(Math.min(limit, 64));
        TableInfo info = table.getTableInfo();
        for (int b = 0; b < info.getNumBuckets() && out.size() < limit; b++) {
            // Fluss 0.9.1 requires an explicit scan limit for a BatchScanner ("only available
            // when limit is set"). Setting it to the caller's bound is the honest value: the
            // scanner can never hand back more than the contract allows.
            try (BatchScanner scanner = table.newScan().limit(limit)
                    .createBatchScanner(new TableBucket(info.getTableId(), b))) {
                while (out.size() < limit) {
                    boolean any = false;
                    try (CloseableIterator<InternalRow> it =
                                 scanner.pollBatch(Duration.ofMillis(timeoutMs))) {
                        while (it != null && it.hasNext() && out.size() < limit) {
                            out.add(decode(it.next()));
                            any = true;
                        }
                    }
                    if (!any) break;
                }
            }
        }
        return out;
    }

    /** Row layout 16_postback_quarantine.sql (13 cols) — the exact inverse of {@link #append}. */
    private static QuarantinedPostback decode(InternalRow r) {
        String reason = text(r, 2);
        return new QuarantinedPostback(
                text(r, 0),
                text(r, 1),
                reason == null ? null : QuarantineReason.valueOf(reason),
                r.isNullAt(3) ? null : r.getBytes(3),
                text(r, 4),
                text(r, 5),
                text(r, 6),
                text(r, 7),
                text(r, 8),
                text(r, 9),
                r.getLong(10),
                r.isNullAt(11) ? null : r.getLong(11),
                text(r, 12));
    }

    private static String text(InternalRow r, int i) {
        return r.isNullAt(i) ? null : r.getString(i).toString();
    }
}
