package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.trading.common.schema.fluss.FaultFlussStubs;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Boundedness of the Postback_Quarantine LOG append — the same guarantee the
 * other raw-client stores have, for the one store that had none. See
 * {@link FaultFlussStubs} for the injected fault and why a live drill cannot
 * cover this (on a healthy cluster the flush-present and flush-absent versions
 * behave identically).
 *
 * <p>This store is already correct — the append awaits a bounded
 * {@code .get()} and never flushes. The test is regression insurance: quarantine
 * is the exception path (a postback that could not be correlated), so a stall
 * here is exactly the case where an operator is already investigating, and the
 * {@code delegate.append(row)} that follows only runs on a durable ack.
 */
@DisplayName("FlussPostbackQuarantineStore: an unackable quarantine append fails within budget")
class FlussPostbackQuarantineStoreWriteBoundednessTest {

    private static final long WRITE_TIMEOUT_MS = 250;

    /** Hang detector. Must comfortably exceed the budget and the preemptive wait. */
    private static final Duration TEST_BUDGET = Duration.ofSeconds(10);

    private static final long BOUNDED_MS = 5_000L;

    @Test
    @DisplayName("append is bounded, never flushes, and does not cache the row in memory")
    void quarantineAppendIsBounded() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        FlussPostbackQuarantineStore store = storeWith(writer);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> store.append(row()))
                    .as("an unacked quarantine append must surface as a bounded timeout")
                    .isInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start)).isLessThan(BOUNDED_MS);
        });

        assertThat(writer.flushCalls())
                .as("append must never call flush(): unbounded in Fluss 0.9.1 (see flush_guard.sh)")
                .isZero();
        // P3-167/P3-410: the process-local mirror is gone, so a failed append has no memory to
        // leave a row in. The read path consults the durable TABLE — against this stub that
        // surfaces as the table's own UnsupportedOperationException, which is itself the proof
        // that no in-process cache answered instead of the table.
        assertThatThrownBy(() -> store.scan(10))
                .as("reads must go to the durable table, never to a local cache")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static long elapsedMsSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /**
     * The constructor is private and {@code open()} builds its own connection, so
     * reflection is the only seam that injects a fault without adding test-only
     * production API. The connection argument is unused by the append path.
     */
    private static FlussPostbackQuarantineStore storeWith(FaultFlussStubs.FaultWriter writer)
            throws Exception {
        Table table = new FaultFlussStubs.FaultTable(writer);
        Constructor<FlussPostbackQuarantineStore> ctor = FlussPostbackQuarantineStore.class
                .getDeclaredConstructor(Connection.class, Table.class, long.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, table, WRITE_TIMEOUT_MS);
    }

    private static QuarantinedPostback row() {
        return new QuarantinedPostback(
                "q-1", "pb-1", QuarantineReason.MISSING_BROKER_ID, new byte[] {1, 2, 3},
                "hash-1", null, "instr-1", null, null, null,
                1_700_000_000_000L, null, "2");
    }
}
