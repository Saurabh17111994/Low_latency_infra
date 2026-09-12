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
            // D1: money path — the dedup commit gates the intent disposition, so
            // use the 1ms linger rather than Fluss's 100ms default.
            com.trading.common.schema.fluss.FlussWriteProfiles.moneyPath(c);
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
        // D2: per-call writer is intentional, not a leak — see FlussWriteProfiles.
        // D1: no per-record flush — see FlussWriteProfiles. A pre-ack flush measures
        // ~1ms faster here, but it awaits unboundedly and would run before the
        // bounded get() below, moving the worst case outside the commit budget.
        // Must match Execution_Intent_Processed exactly
        // (28_execution_intent_processed.sql): instruction_id, request_hash,
        // handed_off_ts, source_log_offset, schema_version — all five values, in
        // that order. A short row still compiles here and only fails at the
        // server, which is how a dropped handed_off_ts silently disabled the
        // durable dedup (replayed intents re-forwarded after restart).
        GenericRow row = GenericRow.of(BinaryString.fromString(instructionId),
                BinaryString.fromString(requestHash), System.currentTimeMillis(),
                logOffset == null ? null : logOffset, BinaryString.fromString("1"));
        // C5/P3-268: retry the durable dedup commit. This is a KV upsert keyed by
        // instruction_id, so idempotent by construction and safe to repeat. Without retry a
        // single transient timeout here fails the commit, which the reader's violation
        // handler escalates to fail() — latching the whole gateway HALTED until restart.
        // That is far more disruptive than the transient it reacted to. A fresh writer per
        // attempt mirrors FlussHandlePool's rule that a failed handle may be poisoned.
        RequestBudget.run(() -> {
            UpsertWriter attempt = table.newUpsert().createWriter();
            attempt.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return null;
        });
    }

    // P3-280: collect, don't abort — table failure must not leak the connection.
    @Override public void close() throws Exception {
        try { table.close(); } finally { connection.close(); }
    }
}
