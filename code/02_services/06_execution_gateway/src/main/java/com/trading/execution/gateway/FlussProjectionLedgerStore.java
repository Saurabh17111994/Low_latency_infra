package com.trading.execution.gateway;

import com.trading.common.schema.fluss.BoundedRetry;
import com.trading.common.schema.fluss.FlussHandlePool;
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
    // P3-068/P3-326: reuse Lookupers and writers instead of minting one per call. They are
    // @NotThreadSafe and this store has no single-thread contract, so a cached field would be
    // an unsynchronized cross-call hazard — the pool lends one handle per caller instead, and
    // drops (rather than reuses) any handle whose call failed.
    // Created in the constructor: the factories close over `table`, assigned there.
    private final FlussHandlePool<Lookuper> lookupers;
    private final FlussHandlePool<UpsertWriter> writers;

    public static FlussProjectionLedgerStore open(GatewayConfig c) {
        try {
            Configuration conf = new Configuration(); conf.setString("bootstrap.servers", c.flussBootstrap());
            // D1: bulk-path linger via FlussWriteProfiles (ledger writes are not order-critical).
            com.trading.common.schema.fluss.FlussWriteProfiles.bulkPath(conf);
            Connection connection = ConnectionFactory.createConnection(conf);
            try {
                Table table = connection.getTable(TablePath.of(c.flussDatabase(), c.ledgerTable()));
                return new FlussProjectionLedgerStore(connection, table, c.requestTimeout());
            } catch (Exception e) {
                // P3-281: getTable failure must not leak the connection.
                try { connection.close(); } catch (Exception closeFailure) { e.addSuppressed(closeFailure); }
                throw e;
            }
        } catch (Exception e) { throw new IllegalStateException("cannot open projection ledger", e); }
    }
    FlussProjectionLedgerStore(Connection connection, Table table, Duration timeout) {
        this.connection = connection; this.table = table; this.timeout = timeout;
        this.lookupers = new FlussHandlePool<>(() -> this.table.newLookup().createLookuper());
        this.writers = new FlussHandlePool<>(() -> this.table.newUpsert().createWriter());
    }
    @Override public Entry lookup(String eventId) throws Exception {
        // P3-068: borrow a pooled Lookuper — reuse without an unsynchronized cross-call hazard
        // (Lookuper is @NotThreadSafe and this store has no single-thread contract). It is not
        // Closeable in Fluss 0.9.1, so there is no handle to close, only one to reuse.
        // C5 guard: same transient-lookup retry as FlussControlStateStore (first write after
        // CREATE, 2.2-3.7s measured, vs the 2s timeout).
        InternalRow r = lookupers.with(l -> BoundedRetry.run(() -> l.lookup(GenericRow.of(bs(eventId)))
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow()));
        return r == null ? null : decode(r);
    }
    @Override public void put(Entry e) throws Exception {
        GenericRow row = GenericRow.of(bs(e.eventId()), bs(e.state().name()), bs(e.expectedPriorState()),
                e.retryCount(), bs(e.lastError()), bs(e.disposition()), e.stepTs(), e.completedTs(), bs("2"));
        // D1: no per-record flush — see FlussWriteProfiles; the bulk-path linger
        // bounds the send delay and .get() still awaits the durable ack.
        // P3-068: pooled writer; a failed write drops its handle rather than pooling a
        // possibly-poisoned one.
        // C5 guard: transient upsert-ack timeouts are retried with the same bounded budget;
        // upsert is idempotent by eventId (last-write-wins), so a retried write is safe.
        writers.with(w -> {
            BoundedRetry.run(() -> {
                w.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                return null;
            });
            return null;
        });
    }
    @Override public IncompletePage incomplete(int limit) throws Exception {
        // P3-102: bounded page. Collecting stops at `limit`, but the scan cannot stop with it —
        // there is no cursor for this table (see the interface javadoc), and this read drives
        // recovery, so a page that quietly omitted work would skip it forever (until TTL). The
        // scan therefore runs on only as far as one further recoverable entry, which makes
        // `truncated` exact rather than an inference from a full page.
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive, got " + limit);
        List<Entry> out = new ArrayList<>(Math.min(limit, 1024));
        boolean truncated = false;
        var info = table.getTableInfo();
        scan:
        for (int b = 0; b < info.getNumBuckets(); b++) {
            // P3-003: pollBatch returns one time-bounded page, not the bucket —
            // a single poll truncates wide ledgers (recoverable entries silently
            // skipped). Drain per bucket until null/empty (same shape as P3-002).
            try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE)
                    .createBatchScanner(new TableBucket(info.getTableId(), b))) {
                while (true) {
                    boolean any = false;
                    try (CloseableIterator<InternalRow> it = scanner.pollBatch(timeout)) {
                        while (it != null && it.hasNext()) {
                            Entry e = decode(it.next());
                            any = true;
                            if (!ProjectionLedger.recoverable(e.state())) continue;
                            if (out.size() < limit) { out.add(e); continue; }
                            truncated = true;
                            break scan;
                        }
                    }
                    if (!any) break;
                }
            }
        }
        return new IncompletePage(out, truncated);
    }
    private static Entry decode(InternalRow r) {
        // P3-069: one corrupt/unknown-state row must not abort the whole
        // recovery scan — isolate with the eventId for ops diagnosis.
        String rawState = nullableText(r, 1);
        final ProjectionLedger.State state;
        try { state = ProjectionLedger.State.valueOf(rawState); }
        catch (Exception e) {
            throw new IllegalStateException("corrupt ledger row eventId="
                    + nullableText(r, 0) + " state=" + rawState, e);
        }
        return new Entry(text(r, 0), state, nullableText(r, 2),
                r.getInt(3), nullableText(r, 4), nullableText(r, 5), r.getLong(6), nullableLong(r, 7));
    }
    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }
    private static String text(InternalRow r, int i) { return r.isNullAt(i) ? "" : r.getString(i).toString(); }
    private static String nullableText(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getString(i).toString(); }
    private static Long nullableLong(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getLong(i); }
    // P3-283: collect, don't abort — a table failure must not leak the rest or the connection.
    @Override public void close() throws Exception {
        // P3-068: drop pooled handles first — nothing to close (Lookuper/TableWriter are not
        // Closeable), but retaining them past the table close leaves dead references.
        lookupers.clear();
        writers.clear();
        Exception first = null;
        try { table.close(); } catch (Exception e) { first = e; }
        try { connection.close(); } catch (Exception e) { if (first == null) first = e; else first.addSuppressed(e); }
        if (first != null) throw first;
    }
}
