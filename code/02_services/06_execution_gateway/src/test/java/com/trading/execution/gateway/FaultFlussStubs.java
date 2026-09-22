package com.trading.execution.gateway;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.TypedLookuper;
import org.apache.fluss.client.table.MultiTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.DeleteResult;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.row.InternalRow;

/**
 * Gateway-side fault-injection stubs for the raw-client write paths — the same
 * real Fluss 0.9.1 hazard as {@code com.trading.common.schema.fluss.FaultFlussStubs}:
 * a write future that never completes (a batch whose leader is unknown is never
 * completed) and a {@code flush()} that blocks indefinitely (Fluss awaits it
 * through a bare {@code latch.await()} with no delivery timeout).
 *
 * <p>Duplicated from the common module rather than shared because the repo
 * publishes no test-jars, and a test-jar would only resolve under
 * {@code mvn package}/{@code install} — the guards must keep working under a
 * plain {@code mvn test}, which is where they actually run. Covers both write
 * primitives here: {@code newAppend()} (LOG) and {@code newUpsert()} (KV).
 */
final class FaultFlussStubs {

    private FaultFlussStubs() {}

    /** A writer whose every write is unacked and whose {@code flush()} blocks forever. */
    static final class FaultWriter implements UpsertWriter, AppendWriter {

        private final AtomicInteger flushCalls = new AtomicInteger();

        /** How many times {@code flush()} was called — must stay 0 on the bounded paths. */
        int flushCalls() {
            return flushCalls.get();
        }

        @Override
        public CompletableFuture<UpsertResult> upsert(InternalRow record) {
            return new CompletableFuture<>(); // never completes — unackable batch
        }

        @Override
        public CompletableFuture<DeleteResult> delete(InternalRow record) {
            return new CompletableFuture<>();
        }

        @Override
        public CompletableFuture<AppendResult> append(InternalRow record) {
            return new CompletableFuture<>();
        }

        @Override
        public void flush() {
            flushCalls.incrementAndGet();
            blockIndefinitely();
        }
    }

    /**
     * Model Fluss's unbounded await. Interruptible only so a preemptive test
     * timeout can reclaim the thread — production has nothing to interrupt it.
     */
    private static void blockIndefinitely() {
        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** A {@link Table} whose writers are faulty and whose lookup finds nothing. */
    static final class FaultTable implements Table {

        private final FaultWriter writer;
        private final TableInfo tableInfo;

        FaultTable(FaultWriter writer) {
            this(writer, null);
        }

        /** With a {@link TableInfo} the table also reports its deployed kind, for pre-warm. */
        FaultTable(FaultWriter writer, TableInfo tableInfo) {
            this.writer = writer;
            this.tableInfo = tableInfo;
        }

        @Override
        public Upsert newUpsert() {
            return new Upsert() {
                @Override
                public Upsert partialUpdate(int[] targetColumns) {
                    return this;
                }

                @Override
                public Upsert partialUpdate(String... targetColumnNames) {
                    return this;
                }

                @Override
                public Upsert mergeMode(MergeMode mode) {
                    return this;
                }

                @Override
                public UpsertWriter createWriter() {
                    return writer;
                }

                @Override
                public <T> org.apache.fluss.client.table.writer.TypedUpsertWriter<T>
                        createTypedWriter(Class<T> pojoClass) {
                    throw new UnsupportedOperationException("not used by the write paths");
                }
            };
        }

        @Override
        public Append newAppend() {
            return new Append() {
                @Override
                public AppendWriter createWriter() {
                    return writer;
                }

                @Override
                public <T> org.apache.fluss.client.table.writer.TypedAppendWriter<T>
                        createTypedWriter(Class<T> pojoClass) {
                    throw new UnsupportedOperationException("not used by the write paths");
                }
            };
        }

        @Override
        public Lookup newLookup() {
            return new Lookup() {
                @Override
                public Lookup lookupBy(List<String> lookupColumnNames) {
                    return this;
                }

                @Override
                public Lookup enableInsertIfNotExists() {
                    return this;
                }

                @Override
                public Lookuper createLookuper() {
                    return lookupKey ->
                            CompletableFuture.completedFuture(new LookupResult((InternalRow) null));
                }

                @Override
                public <T> TypedLookuper<T> createTypedLookuper(Class<T> pojoClass) {
                    throw new UnsupportedOperationException("not used by the write paths");
                }
            };
        }

        @Override
        public TableInfo getTableInfo() {
            if (tableInfo != null) {
                return tableInfo;
            }
            throw new UnsupportedOperationException("not used by the write paths");
        }

        @Override
        public Scan newScan() {
            throw new UnsupportedOperationException("not used by the write paths");
        }

        @Override
        public void close() {
            // nothing to release in the stub
        }
    }

    /** A {@link Connection} handing out {@link FaultTable}s, for the by-name paths. */
    static final class FaultConnection implements Connection {

        private final FaultTable table;

        FaultConnection(FaultWriter writer) {
            this(new FaultTable(writer));
        }

        FaultConnection(FaultTable table) {
            this.table = table;
        }

        @Override
        public Table getTable(TablePath tablePath) {
            return table;
        }

        @Override
        public Configuration getConfiguration() {
            throw new UnsupportedOperationException("not used by the write paths");
        }

        @Override
        public Admin getAdmin() {
            throw new UnsupportedOperationException("not used by the write paths");
        }

        // Added to the Connection interface in Fluss 1.0; the write paths under
        // test are single-table, so this stays unimplemented by design.
        @Override
        public MultiTable getMultiTable() {
            throw new UnsupportedOperationException("not used by the write paths");
        }

        @Override
        public void close() {
            // nothing to release in the stub
        }
    }
}
