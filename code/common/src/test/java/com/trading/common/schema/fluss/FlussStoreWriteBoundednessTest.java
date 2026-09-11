package com.trading.common.schema.fluss;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.trading.common.model.GateState;
import com.trading.common.model.PositionState;
import com.trading.common.schema.execution.AttemptStore;
import com.trading.common.schema.execution.FlussAttemptStore;
import com.trading.common.schema.execution.FlussGateStateStore;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.position.FlussPositionsStateStore;
import com.trading.common.schema.position.PositionSnapshot;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The same boundedness guarantee {@code FlussEodStateStore} has, pinned for the
 * other raw-client stores: <b>a Fluss writer waiting on an unsendable batch
 * must fail within the caller's budget, never hang</b>. See
 * {@link FaultFlussStubs} for the injected fault and why a live drill cannot
 * cover this.
 *
 * <p>These are the money-adjacent paths — the gate's durable commit, the
 * attempt store's exactly-once prepare — plus the positions projection write.
 * A hang here is worse than a timeout: the caller is holding the gate lock, so
 * an unbounded wait stalls every order behind it. The static rule in
 * {@code flush_guard.sh} stops a flush() being <i>added</i>; this stops one
 * being <i>tolerated</i>.
 *
 * <p>Each test asserts two things: the failure surfaces within budget, and
 * {@code flush()} was never called. The second is the positive form of the
 * invariant — it fails fast if someone adds a flush that happens not to block
 * in the stub, which the first assertion would not notice.
 */
@DisplayName("raw-client stores: unackable writes fail within budget and never call flush()")
class FlussStoreWriteBoundednessTest {

    private static final long WRITE_TIMEOUT_MS = 250;

    /** Hang detector. Must comfortably exceed the budget and the preemptive wait. */
    private static final Duration TEST_BUDGET = Duration.ofSeconds(10);

    /** Upper bound on "within budget" — the same generous slack the EOD test uses. */
    private static final long BOUNDED_MS = 5_000L;

    @Test
    @DisplayName("FlussPositionsStateStore.upsert is bounded and never flushes")
    void positionsUpsertIsBounded() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        FlussPositionsStateStore store = positionsStore(writer);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> store.upsert(snapshot()))
                    .as("an unacked position write must surface as a bounded timeout")
                    .isInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start)).isLessThan(BOUNDED_MS);
        });

        assertThat(writer.flushCalls()).isZero();
    }

    @Test
    @DisplayName("FlussGateStateStore.init (gate commit) is bounded and never flushes")
    void gateInitIsBounded() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        FlussGateStateStore store = gateStore(writer);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> store.init(bootRow()))
                    .as("a failed durable gate commit must fail loud, not hold the gate lock forever")
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start)).isLessThan(BOUNDED_MS);
        });

        assertThat(writer.flushCalls()).isZero();
    }

    @Test
    @DisplayName("FlussAttemptStore.prepare (exactly-once prepare) is bounded and never flushes")
    void attemptPrepareIsBounded() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        FlussAttemptStore store = attemptStore(writer);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> store.prepare(prepareRequest()))
                    .as("a failed durable prepare must fail closed, never report CREATED on a hang")
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start)).isLessThan(BOUNDED_MS);
        });

        assertThat(writer.flushCalls()).isZero();
    }

    private static long elapsedMsSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    // ── fixtures under test ───────────────────────────────────────────────

    /**
     * Each store's constructor is private and its own {@code open()} builds the
     * connection, so reflection is the only seam that injects a fault without
     * adding test-only production API. The connection argument is unused by the
     * write paths.
     */
    private static FlussPositionsStateStore positionsStore(FaultFlussStubs.FaultWriter writer)
            throws Exception {
        Constructor<FlussPositionsStateStore> ctor = FlussPositionsStateStore.class
                .getDeclaredConstructor(Connection.class, Table.class, long.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, new FaultFlussStubs.FaultTable(writer), WRITE_TIMEOUT_MS);
    }

    private static FlussGateStateStore gateStore(FaultFlussStubs.FaultWriter writer)
            throws Exception {
        Constructor<FlussGateStateStore> ctor = FlussGateStateStore.class
                .getDeclaredConstructor(Connection.class, Table.class, long.class, Set.class, boolean.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, new FaultFlussStubs.FaultTable(writer), WRITE_TIMEOUT_MS,
                Set.of(), true);
    }

    private static FlussAttemptStore attemptStore(FaultFlussStubs.FaultWriter writer)
            throws Exception {
        Constructor<FlussAttemptStore> ctor = FlussAttemptStore.class
                .getDeclaredConstructor(Connection.class, Table.class, long.class, Runnable.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, new FaultFlussStubs.FaultTable(writer), WRITE_TIMEOUT_MS,
                (Runnable) () -> { });
    }

    // ── inputs ────────────────────────────────────────────────────────────

    private static PositionSnapshot snapshot() {
        return new PositionSnapshot("pos-1", "tc-1", "acc-1", 123L, "NSE", "RELIANCE", "BUY",
                PositionState.OPEN, 100L, 0L, 10050L, 0L, "pb-1", 1L,
                1_700_000_000_000L, 1_700_000_000_000L, "2");
    }

    private static GateRow bootRow() {
        return new GateRow("partition-1", "acc-1", GateState.HALTED, 0L, "boot", null,
                null, null, null, null, 0L, null, null, null);
    }

    private static AttemptStore.PrepareRequest prepareRequest() {
        return new AttemptStore.PrepareRequest(
                "attempt-" + System.nanoTime(), "acc-1", "instr-1", "action-1", "partition-1",
                "hash-1", "corr-1", 0L, 0L, 1_700_000_000_000L);
    }
}
