package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.DeleteResult;
import org.apache.fluss.client.table.writer.TypedUpsertWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.junit.jupiter.api.Test;

/**
 * CHG-223 (§5.7): the durable dedup commit borrows from the shared handle pool, and a failed
 * attempt's writer is never reused.
 *
 * <p>What this replaced: {@code record()} created a writer per attempt. On a timeout that writer
 * could not be closed — Fluss 0.9.1 gives {@code TableWriter} no close, only {@code flush()} — so
 * every retry abandoned one, each holding a {@code Sender} with a pending record: the run-4b
 * storm shape, on the handoff path rather than the drill's.
 *
 * <p>These pins are behavioural, not structural: they assert what the table sees (how many
 * writers were created), so they would also fail if someone reintroduced per-attempt creation.
 */
class FlussIntentDedupStorePoolingTest {

    @Test
    void twoSuccessfulWritesReuseOnePooledWriter() throws Exception {
        CountingTable table = new CountingTable(new AckingWriter());
        FlussIntentDedupStore store = new FlussIntentDedupStore(null, table, Duration.ofMillis(200));

        store.record("instr-1", "hash-1", null);
        store.record("instr-2", "hash-2", null);

        assertThat(table.writerCreates())
                .as("the second write must borrow the first one's writer, not create another")
                .isEqualTo(1);
    }

    @Test
    void aTimedOutAttemptsWriterIsNotReused() throws Exception {
        // FaultFlussStubs.FaultWriter's upsert future never completes, so every attempt times
        // out — the shape that abandoned a writer per retry before this change.
        CountingTable table = new CountingTable(new FaultFlussStubs.FaultWriter());
        FlussIntentDedupStore store = new FlussIntentDedupStore(null, table, Duration.ofMillis(50));

        assertThatThrownBy(() -> store.record("instr-1", "hash-1", null))
                .as("an unacked write must fail, not be reported as committed")
                .isInstanceOf(TimeoutException.class);
        int afterFailures = table.writerCreates();
        assertThat(afterFailures)
                .as("each retry attempts with its own writer")
                .isGreaterThanOrEqualTo(1);

        // Now make the table healthy again: the next write must create a NEW writer. If the
        // poisoned handle had been pooled, this would silently reuse it and no writer would be
        // created — which is exactly the defect this test exists to catch.
        table.writer(new AckingWriter());
        store.record("instr-2", "hash-2", null);

        assertThat(table.writerCreates())
                .as("a writer whose call timed out must never be handed to a later caller")
                .isEqualTo(afterFailures + 1);
    }

    /** An UpsertWriter whose writes complete at once — the healthy path. */
    private static final class AckingWriter implements UpsertWriter {

        @Override
        public CompletableFuture<UpsertResult> upsert(InternalRow record) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<DeleteResult> delete(InternalRow record) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void flush() {
            // nothing buffered
        }
    }

    /** A Table that counts writer creations; only its upsert half is implemented. */
    private static final class CountingTable implements Table {

        private volatile UpsertWriter writer;
        private final AtomicInteger writerCreates = new AtomicInteger();

        CountingTable(UpsertWriter writer) {
            this.writer = writer;
        }

        void writer(UpsertWriter next) {
            this.writer = next;
        }

        int writerCreates() {
            return writerCreates.get();
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
                    writerCreates.incrementAndGet();
                    return writer;
                }

                @Override
                public <T> TypedUpsertWriter<T> createTypedWriter(Class<T> pojoClass) {
                    throw new UnsupportedOperationException("the dedup commit writes rows");
                }
            };
        }

        @Override
        public TableInfo getTableInfo() {
            throw new UnsupportedOperationException("not on the write path");
        }

        @Override
        public Scan newScan() {
            throw new UnsupportedOperationException("not on the write path");
        }

        @Override
        public Lookup newLookup() {
            throw new UnsupportedOperationException("not on the write path");
        }

        @Override
        public Append newAppend() {
            throw new UnsupportedOperationException("not on the write path");
        }

        @Override
        public void close() {
            // nothing held
        }
    }
}
