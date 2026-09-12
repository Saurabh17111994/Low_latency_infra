package com.trading.execution.gateway;

import com.trading.common.schema.fluss.FlussHandlePool;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/** Real Fluss raw-client implementation for gateway control reads.
 *
 * <p>Resource note (P3-065): Lookupers are <b>pooled per table</b>, not created per call.
 * They cannot be closed — in Fluss 0.9.1 {@code Lookuper} exposes only {@code lookup()}, so
 * try-with-resources does not compile — and they cannot simply be cached either, because this
 * is a concurrent request path (P3-066, pooled httpExecutor) and {@code Lookuper} is
 * {@code @NotThreadSafe}. {@link FlussHandlePool} lends one handle per caller: steady-state
 * reuse without an unsynchronized cross-call hazard. Only Table/Connection are closed.
 */
public final class FlussControlStateStore implements ControlStateStore {
    private final Connection connection;
    private final GatewayConfig config;
    private final Duration timeout;
    // P3-066: concurrent request path (pooled httpExecutor) — plain HashMap
    // computeIfAbsent corrupts; iteration in close() would CME.
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    // P3-065: one bounded handle pool per table, same concurrent-map reasoning as
    // `tables` (computeIfAbsent from the request path, snapshot on close).
    private final Map<String, FlussHandlePool<Lookuper>> lookuperPools = new ConcurrentHashMap<>();

    public static FlussControlStateStore open(GatewayConfig config) throws Exception {
        Configuration c = new Configuration();
        c.setString("bootstrap.servers", config.flussBootstrap());
        // D1: money/safety path — 1ms linger instead of the 100ms default; a halt
        // row must become visible to the tail reader immediately.
        com.trading.common.schema.fluss.FlussWriteProfiles.moneyPath(c);
        return new FlussControlStateStore(ConnectionFactory.createConnection(c), config,
                config.requestTimeout());
    }

    FlussControlStateStore(Connection connection, GatewayConfig config, Duration timeout) {
        this.connection = connection; this.config = config; this.timeout = timeout;
    }

    @Override
    public Lookup lookup(String tableName, List<Object> keyFields) {
        // P3-273: explicit validation — keyFields.stream() on a null List
        // throws a bare NPE with no param name (varargs path already guards,
        // direct List calls bypass it).
        if (tableName == null) {
            throw new IllegalArgumentException("tableName must not be null");
        }
        if (keyFields == null) {
            throw new IllegalArgumentException("keyFields must not be null");
        }
        for (Object key : keyFields) {
            if (key == null) {
                throw new IllegalArgumentException("key field must not be null");
            }
        }
        try {
            Table table = table(tableName);
            Object[] key = keyFields.stream().map(FlussControlStateStore::value).toArray();
            // P3-065: borrow a pooled Lookuper for this table instead of minting one per
            // lookup. Steady-state reuse; the pool never hands the same handle to two callers.
            FlussHandlePool<Lookuper> pool = lookuperPools.computeIfAbsent(tableName,
                    n -> new FlussHandlePool<>(() -> table.newLookup().createLookuper()));
            // C5 guard: transient Fluss lookups (first write after CREATE, 2.2-3.7s measured,
            // vs the 2s timeout) previously surfaced as intermittent TimeoutException /
            // UNAVAILABLE even though the RPC would have recovered; retry a bounded budget,
            // fail fast after.
            InternalRow row = pool.with(lookuper -> RequestBudget.run(() -> lookuper.lookup(GenericRow.of(key))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow()));
            return row == null ? new Lookup(Status.NOT_FOUND, null, "key not found")
                    : new Lookup(Status.FOUND, row, "ok");
        } catch (Exception e) {
            return new Lookup(Status.UNAVAILABLE, null, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @Override
    public void replaySafetyHalts(Consumer<InternalRow> consumer) {
        // P3-001: pollBatch returns one time-bounded page, not the bucket —
        // a single poll silently misses rows (fail-open on halts) and a null
        // batch NPEs. Drain per bucket until null/empty, same shape as
        // FlussIntentDedupStore.hydrate / FlussProjectionLedgerStore.
        try {
            Table table = table(config.haltTable());
            var info = table.getTableInfo();
            for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
                TableBucket tb = new TableBucket(info.getTableId(), bucket);
                try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE).createBatchScanner(tb)) {
                    while (true) {
                        try (CloseableIterator<InternalRow> it = scanner.pollBatch(timeout)) {
                            if (it == null || !it.hasNext()) {
                                break;
                            }
                            while (it.hasNext()) consumer.accept(it.next());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot replay safety halt state", e);
        }
    }

    private Table table(String name) {
        // P3-066: ConcurrentHashMap path — connection.getTable failure must
        // not poison the map entry (a failed mapping retries next call).
        if (name == null) {
            throw new IllegalArgumentException("table name must not be null");
        }
        return tables.computeIfAbsent(name, n -> connection.getTable(TablePath.of(config.flussDatabase(), n)));
    }
    private static Object value(Object value) {
        return value instanceof String s ? BinaryString.fromString(s) : value;
    }
    @Override public void close() throws Exception {
        // P3-065: drop pooled handles first — nothing to close (Lookuper is not Closeable),
        // but holding them past the table close would leave dead references behind.
        for (FlussHandlePool<Lookuper> pool : List.copyOf(lookuperPools.values())) {
            pool.clear();
        }
        lookuperPools.clear();
        // P3-066: collect, don't abort — one table failing must not leak the
        // rest or the connection; snapshot first (concurrent map).
        Exception failure = null;
        for (Table table : List.copyOf(tables.values())) {
            try { table.close(); } catch (Exception e) {
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
