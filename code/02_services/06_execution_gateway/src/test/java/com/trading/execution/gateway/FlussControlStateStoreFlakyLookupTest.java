package com.trading.execution.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.trading.common.schema.fluss.BoundedRetry;
import com.trading.execution.gateway.ControlStateStore.Lookup;
import com.trading.execution.gateway.ControlStateStore.Status;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

/**
 * C5 guard test: FlussControlStateStore.lookup must retry transient Fluss lookups through
 * a bounded budget (recovering when the cluster settles) and fail fast to UNAVAILABLE when
 * the transient outlasts the budget — instead of surfacing a spurious UNAVAILABLE on the
 * first transient as before.
 */
class FlussControlStateStoreFlakyLookupTest {

    /** Stub Lookuper whose lookup() completes exceptionally for the first {@code failures} calls. */
    private static final class StubLookuper implements Lookuper {
        final AtomicInteger calls = new AtomicInteger();
        final int failures;
        final InternalRow row;

        StubLookuper(int failures, InternalRow row) {
            this.failures = failures;
            this.row = row;
        }

        @Override
        public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
            int n = calls.incrementAndGet();
            if (n <= failures) {
                // Complete with the raw retriable exception: CompletableFuture.get() wraps
                // it once in ExecutionException, which BoundedRetry unwraps to see
                // NetworkException (a RetriableException). Wrapping here again would
                // double-wrap and defeat the transient detection.
                return CompletableFuture.failedFuture(
                        new NetworkException("leader election", new Exception("sim")));
            }
            return CompletableFuture.completedFuture(new LookupResult(row));
        }
    }

    private static final class StubLookup implements org.apache.fluss.client.lookup.Lookup {
        final StubLookuper lookuper;
        StubLookup(StubLookuper lookuper) { this.lookuper = lookuper; }
        @Override public Lookuper createLookuper() { return lookuper; }
        @Override public <T> org.apache.fluss.client.lookup.TypedLookuper<T> createTypedLookuper(Class<T> clazz) {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public org.apache.fluss.client.lookup.Lookup lookupBy(List<String> lookupColumnNames) {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public org.apache.fluss.client.lookup.Lookup enableInsertIfNotExists() {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public org.apache.fluss.client.lookup.Lookup lookupBy(String... prefixKeys) {
            throw new UnsupportedOperationException("not used in this test");
        }
    }

    private static final class StubTable implements Table {
        final StubLookuper lookuper;
        StubTable(StubLookuper lookuper) { this.lookuper = lookuper; }
        @Override public org.apache.fluss.client.lookup.Lookup newLookup() { return new StubLookup(lookuper); }
        @Override public org.apache.fluss.metadata.TableInfo getTableInfo() {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public org.apache.fluss.client.table.scanner.Scan newScan() {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public org.apache.fluss.client.table.writer.Append newAppend() {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public org.apache.fluss.client.table.writer.Upsert newUpsert() {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public void close() {}
    }

    private static final class StubConnection implements Connection {
        final StubLookuper lookuper;
        StubConnection(StubLookuper lookuper) { this.lookuper = lookuper; }
        @Override public Configuration getConfiguration() {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public org.apache.fluss.client.admin.Admin getAdmin() {
            throw new UnsupportedOperationException("not used in this test");
        }
        @Override public Table getTable(TablePath tablePath) { return new StubTable(lookuper); }
        @Override public void close() {}
    }

    private InternalRow row(long token) {
        return org.apache.fluss.row.GenericRow.of(
                BinaryString.fromString("evt-" + token),
                BinaryString.fromString("SEEN"));
    }

    private static GatewayConfig testConfig() {
        return new GatewayConfig(
                "localhost:9123", "test_db", "intent", "gate", "attempts", "correlation",
                "ledger", "halt", "127.0.0.1", 0, "http://nautilus:9500", "v1", "secret",
                Duration.ofMillis(2000), Duration.ofMillis(100), "scope", "partition");
    }

    private FlussControlStateStore store(StubLookuper lookuper, Duration timeout) {
        return new FlussControlStateStore(new StubConnection(lookuper), testConfig(), timeout);
    }

    @Test
    void lookupRecoversAfterTransientFailures() {
        StubLookuper lookuper = new StubLookuper(2, row(7L));
        Lookup result = store(lookuper, Duration.ofMillis(50)).lookup("some_table", List.of(7L));
        assertEquals(Status.FOUND, result.status(), "must recover once the cluster settles");
        assertNotNull(result.row());
        assertEquals(3, lookuper.calls.get(), "guard must have retried the two transients");
    }

    @Test
    void transientBeyondBudgetFailsFastToUnavailable() {
        StubLookuper lookuper = new StubLookuper(Integer.MAX_VALUE, row(7L)); // always failing
        Lookup result = store(lookuper, Duration.ofMillis(50)).lookup("some_table", List.of(7L));
        assertEquals(Status.UNAVAILABLE, result.status(),
                "transient that outlasts the budget must fail fast to UNAVAILABLE");
        assertEquals(BoundedRetry.ATTEMPTS, lookuper.calls.get(),
                "exactly the bounded budget — no infinite retry loop");
    }
}
