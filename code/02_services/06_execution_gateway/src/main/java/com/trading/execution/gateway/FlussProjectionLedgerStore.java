package com.trading.execution.gateway;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/** Full-row, awaited KV writes for Postback_Projection_Ledger. */
public final class FlussProjectionLedgerStore implements ProjectionLedgerStore {
    private final Connection connection;
    private final Table table;
    private final Duration timeout;

    public static FlussProjectionLedgerStore open(GatewayConfig c) {
        try {
            Configuration conf = new Configuration(); conf.setString("bootstrap.servers", c.flussBootstrap());
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TablePath.of(c.flussDatabase(), c.ledgerTable()));
            return new FlussProjectionLedgerStore(connection, table, c.requestTimeout());
        } catch (Exception e) { throw new IllegalStateException("cannot open projection ledger", e); }
    }
    FlussProjectionLedgerStore(Connection connection, Table table, Duration timeout) {
        this.connection = connection; this.table = table; this.timeout = timeout;
    }
    @Override public Entry lookup(String eventId) throws Exception {
        Lookuper l = table.newLookup().createLookuper();
        // C5 guard: same transient-lookup retry as FlussControlStateStore (5s settle vs 2s timeout).
        InternalRow r = BoundedRetry.run(() -> l.lookup(GenericRow.of(bs(eventId)))
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow());
        return r == null ? null : decode(r);
    }
    @Override public void put(Entry e) throws Exception {
        GenericRow row = GenericRow.of(bs(e.eventId()), bs(e.state().name()), bs(e.expectedPriorState()),
                e.retryCount(), bs(e.lastError()), bs(e.disposition()), e.stepTs(), e.completedTs(), bs("2"));
        UpsertWriter w = table.newUpsert().createWriter();
        try {
            // C5 guard: transient upsert-ack timeouts are retried with the same bounded budget;
            // upsert is idempotent by eventId (last-write-wins), so a retried write is safe.
            BoundedRetry.run(() -> {
                w.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                return null;
            });
        } finally { w.flush(); }
    }
    @Override public List<Entry> incomplete() throws Exception {
        List<Entry> out = new ArrayList<>();
        var info = table.getTableInfo();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE)
                    .createBatchScanner(new TableBucket(info.getTableId(), b));
                 CloseableIterator<InternalRow> it = scanner.pollBatch(timeout)) {
                while (it.hasNext()) { Entry e = decode(it.next()); if (ProjectionLedger.recoverable(e.state())) out.add(e); }
            }
        }
        return out;
    }
    private static Entry decode(InternalRow r) {
        return new Entry(text(r, 0), ProjectionLedger.State.valueOf(text(r, 1)), nullableText(r, 2),
                r.getInt(3), nullableText(r, 4), nullableText(r, 5), r.getLong(6), nullableLong(r, 7));
    }
    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }
    private static String text(InternalRow r, int i) { return r.isNullAt(i) ? "" : r.getString(i).toString(); }
    private static String nullableText(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getString(i).toString(); }
    private static Long nullableLong(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getLong(i); }
    @Override public void close() throws Exception { table.close(); connection.close(); }
}
