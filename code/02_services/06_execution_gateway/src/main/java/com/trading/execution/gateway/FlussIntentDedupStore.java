package com.trading.execution.gateway;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/** Fluss raw-client implementation of {@link IntentDedupStore}. */
public final class FlussIntentDedupStore implements IntentDedupStore {
    private static final String TABLE = "Execution_Intent_Processed";
    private final Connection connection;
    private final Table table;
    private final Duration timeout;

    public static FlussIntentDedupStore open(GatewayConfig config) {
        try {
            Configuration c = new Configuration();
            c.setString("bootstrap.servers", config.flussBootstrap());
            Connection connection = ConnectionFactory.createConnection(c);
            try {
                Table table = connection.getTable(TablePath.of(config.flussDatabase(), TABLE));
                return new FlussIntentDedupStore(connection, table, config.requestTimeout());
            } catch (Exception e) {
                // P3-278: getTable failure must not leak the connection.
                try { connection.close(); } catch (Exception closeFailure) { e.addSuppressed(closeFailure); }
                throw e;
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot open Execution_Intent_Processed", e);
        }
    }
    FlussIntentDedupStore(Connection connection, Table table, Duration timeout) {
        this.connection = connection; this.table = table; this.timeout = timeout;
    }

    @Override public Map<String, String> hydrate() throws Exception {
        Map<String, String> out = new HashMap<>();
        var info = table.getTableInfo();
        for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
            // P3-002: pollBatch returns one time-bounded page, not the bucket —
            // a single poll truncates wide buckets (fail-open: replayed intents
            // re-forward as FIRST). Drain per bucket until null/empty.
            try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE)
                    .createBatchScanner(new TableBucket(info.getTableId(), bucket))) {
                while (true) {
                    try (CloseableIterator<InternalRow> it = scanner.pollBatch(timeout)) {
                        // P3-067: pollBatch may return null; skip null columns/tombstones.
                        if (it == null || !it.hasNext()) break;
                        while (it.hasNext()) {
                            InternalRow r = it.next();
                            if (r.isNullAt(0) || r.isNullAt(1)) continue;
                            out.put(r.getString(0).toString(),
                                    r.getString(1).toString());
                        }
                    }
                }
            }
        }
        return out;
    }

    @Override public void record(String instructionId, String requestHash, Long logOffset) throws Exception {
        // P3-279: per-call writer is intentional, not a leak — UpsertWriter is
        // flush-only in Fluss 0.9.1 (javap: TableWriter exposes only flush(),
        // not Closeable), and a shared writer would be an unsynchronized
        // cross-call hazard (no single-thread contract on this store).
        GenericRow row = GenericRow.of(BinaryString.fromString(instructionId),
                BinaryString.fromString(requestHash), System.currentTimeMillis(),
                logOffset == null ? null : logOffset, BinaryString.fromString("1"));
        UpsertWriter writer = table.newUpsert().createWriter();
        try { writer.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        finally { writer.flush(); }
    }

    // P3-280: collect, don't abort — table failure must not leak the connection.
    @Override public void close() throws Exception {
        try { table.close(); } finally { connection.close(); }
    }
}
