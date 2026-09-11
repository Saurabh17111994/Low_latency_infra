package com.trading.common.schema.fluss;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.TypedLookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.DeleteResult;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.row.InternalRow;

/**
 * Fault-injection stubs for the raw-client write paths, modelling the real
 * Fluss 0.9.1 hazard rather than a synthetic one:
 *
 * <ul>
 *   <li>{@link FaultWriter#upsert} returns a future that <b>never completes</b>
 *       — a batch whose leader is unknown is never completed
 *       ({@code Sender} refreshes metadata and retries forever;
 *       {@code RecordAccumulator} carries a {@code TODO add deliveryTimeoutMs});</li>
 *   <li>{@link FaultWriter#flush} <b>blocks indefinitely</b> — Fluss awaits it
 *       through a bare {@code latch.await()} with no delivery timeout
 *       ({@code WriteBatch.RequestFuture.await()}).</li>
 * </ul>
 *
 * <p>Both are the real semantics, so a test driving these can pin the property a
 * live cluster cannot: that an unackable write fails within the caller's budget
 * instead of hanging. On a healthy cluster the flush-present and flush-absent
 * versions behave identically, which is exactly why fault injection is needed
 * (see {@code flush_guard.sh} for the same reasoning applied statically).
 *
 * <p>Lives in test sources and is {@code public} so tests in the sibling store
 * packages can share one stub set (the repo publishes no test-jars, so this
 * cannot live in {@code common} for the gateway module to reuse).
 */
public final class FaultFlussStubs {

    private FaultFlussStubs() {}

    /**
     * A writer whose every write is unacked and whose {@code flush()} blocks
     * forever. Implements both write primitives so KV and LOG paths share it.
     */
    public static final class FaultWriter implements UpsertWriter, AppendWriter {

        private final AtomicInteger flushCalls = new AtomicInteger();

        /** How many times {@code flush()} was called — must stay 0 on the bounded paths. */
        public int flushCalls() {
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

    /**
     * A {@link Table} whose {@code newUpsert()} returns {@code writer} and whose
     * lookup finds nothing (so a read-before-write path proceeds to its write).
     */
    public static final class FaultTable implements Table {

        private final FaultWriter writer;

        public FaultTable(FaultWriter writer) {
            this.writer = writer;
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
            throw new UnsupportedOperationException("not used by the write paths");
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
        public Scan newScan() {
            throw new UnsupportedOperationException("not used by the write paths");
        }

        @Override
        public void close() {
            // nothing to release in the stub
        }
    }
}
