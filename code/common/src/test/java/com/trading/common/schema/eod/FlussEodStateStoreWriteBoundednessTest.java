package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.trading.common.schema.fluss.FaultFlussStubs;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.LocalDate;
import java.util.concurrent.TimeoutException;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.TableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The write path's guarantee, pinned without a cluster: <b>a Fluss writer that
 * awaits an unsendable batch must fail within the caller's budget, never
 * hang</b>. See {@link FaultFlussStubs} for why the live drills cannot test
 * this (on a healthy cluster the flush-present and flush-absent versions behave
 * identically) and what real 0.9.1 behaviour the stubs reproduce.
 *
 * <p>Pins, for {@link FlussEodStateStore#upsert} (the state write) and
 * {@link FlussEodStateStore#acquireLease} (the single-writer fencing path — the
 * one that must not hang, since a hung lease holder blocks every other
 * controller):
 *
 * <ol>
 *   <li>the write throws {@link TimeoutException} within the budget — a
 *       re-added {@code flush()} instead blocks forever and this test times out
 *       and fails;</li>
 *   <li>{@code flush()} is never called at all — the positive statement of the
 *       same invariant, failing fast if a future edit adds one that happens not
 *       to block.</li>
 * </ol>
 */
@DisplayName("FlussEodStateStore: an unsendable batch fails within budget, it never hangs")
class FlussEodStateStoreWriteBoundednessTest {

    private static final long WRITE_TIMEOUT_MS = 250;

    /** Hang detector. Must comfortably exceed the budget and the preemptive wait. */
    private static final Duration TEST_BUDGET = Duration.ofSeconds(10);

    private static final EodOffloadRecord RECORD = EodOffloadRecord.initial(
            LocalDate.of(2026, 8, 13), "feature_candles_15s", "2", 1_700_000_000_000L);

    @Test
    @DisplayName("upsert fails with TimeoutException within budget and never calls flush()")
    void upsertIsBoundedAndNeverFlushes() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        FlussEodStateStore store = storeWith(writer);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> store.upsert(RECORD))
                    .as("an unacked write must surface as a bounded timeout, not a hang")
                    .isInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start))
                    .as("must fail within the budget, not just eventually")
                    .isLessThan(5_000L);
        });

        assertThat(writer.flushCalls())
                .as("the write path must never call flush(): unbounded, and a no-op after the "
                        + "bounded get() on the success path (see flush_guard.sh)")
                .isZero();
    }

    @Test
    @DisplayName("acquireLease (fencing path) is bounded too — a hang here blocks every controller")
    void leaseWriteIsBoundedAndNeverFlushes() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        FlussEodStateStore store = storeWith(writer);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> store.acquireLease("token-a", 1_700_000_000_000L, 60_000L))
                    .as("an unacked lease write must fail loudly, never hold the lease indefinitely")
                    .isInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start))
                    .as("must fail within the budget, not just eventually")
                    .isLessThan(5_000L);
        });

        assertThat(writer.flushCalls())
                .as("the fencing path must never call flush() either")
                .isZero();
    }

    private static long elapsedMsSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /**
     * Build a store around a fake {@link Table}. The constructor is private and
     * the store's own {@code open()} builds its connection, so reflection is the
     * only seam that injects a fault without adding test-only production API.
     * The connection/TableInfo arguments are unused by the write paths.
     */
    private static FlussEodStateStore storeWith(FaultFlussStubs.FaultWriter writer) throws Exception {
        Table table = new FaultFlussStubs.FaultTable(writer);
        Constructor<FlussEodStateStore> ctor = FlussEodStateStore.class.getDeclaredConstructor(
                Connection.class, Table.class, TableInfo.class, long.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, table, null, WRITE_TIMEOUT_MS);
    }
}
