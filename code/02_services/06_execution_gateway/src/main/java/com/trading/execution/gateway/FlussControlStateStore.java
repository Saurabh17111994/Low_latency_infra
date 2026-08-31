package com.trading.execution.gateway;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

/** Real Fluss raw-client implementation for gateway control reads. */
public final class FlussControlStateStore implements ControlStateStore {
    private final Connection connection;
    private final GatewayConfig config;
    private final Duration timeout;
    private final Map<String, Table> tables = new HashMap<>();

    public static FlussControlStateStore open(GatewayConfig config) throws Exception {
        Configuration c = new Configuration();
        c.setString("bootstrap.servers", config.flussBootstrap());
        return new FlussControlStateStore(ConnectionFactory.createConnection(c), config,
                config.requestTimeout());
    }

    FlussControlStateStore(Connection connection, GatewayConfig config, Duration timeout) {
        this.connection = connection; this.config = config; this.timeout = timeout;
    }

    @Override
    public Lookup lookup(String tableName, List<Object> keyFields) {
        try {
            Table table = table(tableName);
            Object[] key = keyFields.stream().map(FlussControlStateStore::value).toArray();
            Lookuper lookuper = table.newLookup().createLookuper();
            // C5 guard: transient Fluss lookups (leader-election settle ~5s vs 2s timeout)
            // previously surfaced as intermittent TimeoutException / UNAVAILABLE even
            // though the RPC would have recovered; retry a bounded budget, fail fast after.
            InternalRow row = BoundedRetry.run(() -> lookuper.lookup(GenericRow.of(key))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow());
            return row == null ? new Lookup(Status.NOT_FOUND, null, "key not found")
                    : new Lookup(Status.FOUND, row, "ok");
        } catch (Exception e) {
            return new Lookup(Status.UNAVAILABLE, null, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @Override
    public void replaySafetyHalts(Consumer<InternalRow> consumer) {
        try {
            Table table = table(config.haltTable());
            var info = table.getTableInfo();
            for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
                TableBucket tb = new TableBucket(info.getTableId(), bucket);
                try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE).createBatchScanner(tb);
                     CloseableIterator<InternalRow> it = scanner.pollBatch(timeout)) {
                    while (it.hasNext()) consumer.accept(it.next());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot replay safety halt state", e);
        }
    }

    private Table table(String name) {
        return tables.computeIfAbsent(name, n -> connection.getTable(TablePath.of(config.flussDatabase(), n)));
    }
    private static Object value(Object value) {
        return value instanceof String s ? BinaryString.fromString(s) : value;
    }
    @Override public void close() throws Exception {
        for (Table table : tables.values()) table.close();
        connection.close();
    }
}
