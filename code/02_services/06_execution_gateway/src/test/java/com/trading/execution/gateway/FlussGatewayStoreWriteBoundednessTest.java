package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The gateway half of the bounded-write guarantee: <b>a Fluss writer waiting on
 * an unsendable batch must fail within the caller's budget, never hang</b>. See
 * {@link FaultFlussStubs} for the injected fault and why a live drill cannot
 * cover this (on a healthy cluster the flush-present and flush-absent versions
 * behave identically).
 *
 * <p>These are the three gateway write paths that had a per-record {@code
 * flush()} removed on 2026-09-11. All three sit in front of live order flow, so
 * an unbounded wait is not a slow request — it is a stalled gateway: the
 * dedup record is the restart-replay guard, the projection append is the audit
 * trail, and the ledger put is the cross-table workflow's commit point.
 *
 * <p>Pins two things per path: the failure surfaces within budget, and
 * {@code flush()} was never called. The second is the positive form of the
 * invariant — it fails fast if someone adds a flush that happens not to block
 * in the stub, which the first assertion would not notice.
 */
@DisplayName("gateway raw-client stores: unackable writes fail within budget, never flush()")
class FlussGatewayStoreWriteBoundednessTest {

    private static final Duration WRITE_TIMEOUT = Duration.ofMillis(250);

    /** Hang detector. Must comfortably exceed the budget (and BoundedRetry's 3 attempts). */
    private static final Duration TEST_BUDGET = Duration.ofSeconds(10);

    /** Upper bound on "within budget" — the same generous slack the other stores' tests use. */
    private static final long BOUNDED_MS = 5_000L;

    @Test
    @DisplayName("FlussIntentDedupStore.record (replay guard) is bounded and never flushes")
    void dedupRecordIsBounded() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        // record() writes through the Table it was handed; the Connection is unused.
        FlussIntentDedupStore store = new FlussIntentDedupStore(
                null, new FaultFlussStubs.FaultTable(writer), WRITE_TIMEOUT);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> store.record("instr-1", "hash-1", 42L))
                    .as("an unacked dedup record must fail loud — a silent success here "
                            + "re-forwards replayed intents after restart")
                    .isInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start)).isLessThan(BOUNDED_MS);
        });

        assertThat(writer.flushCalls()).isZero();
    }

    @Test
    @DisplayName("FlussProjectionWriter append (LOG path) is bounded and never flushes")
    void projectionAppendIsBounded() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        FlussProjectionWriter projectionWriter = new FlussProjectionWriter(
                new FaultFlussStubs.FaultConnection(writer), testConfig(), WRITE_TIMEOUT);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            // The 4-arg overload appends to Postback_Quarantine through the same
            // private append() the projection LOG writes use — the by-name table()
            // lookup also gets exercised.
            assertThatThrownBy(() -> projectionWriter.writeQuarantine(
                    "pb-1", "reason", "evidence", new byte[0]))
                    .as("an unacked LOG append must fail within budget, not stall the gateway")
                    .isInstanceOf(TimeoutException.class);
            assertThat(elapsedMsSince(start)).isLessThan(BOUNDED_MS);
        });

        assertThat(writer.flushCalls()).isZero();
    }

    @Test
    @DisplayName("FlussProjectionLedgerStore.put (KV path, C5 retry) stays bounded and never flushes")
    void projectionLedgerPutIsBounded() throws Exception {
        FaultFlussStubs.FaultWriter writer = new FaultFlussStubs.FaultWriter();
        // put() writes through the Table it was handed; the Connection is unused.
        FlussProjectionLedgerStore ledger = new FlussProjectionLedgerStore(
                null, new FaultFlussStubs.FaultTable(writer), WRITE_TIMEOUT);

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            // put() wraps the await in BoundedRetry (C5): a TimeoutException is
            // transient, so it retries ATTEMPTS times with backoff and then
            // rethrows. Bounded, not infinite — this is the assertion that keeps
            // it that way.
            assertThatThrownBy(() -> ledger.put(entry()))
                    .as("the bounded retry must exhaust and rethrow, never hang the ledger")
                    .isInstanceOf(TimeoutException.class);
            long elapsed = elapsedMsSince(start);
            assertThat(elapsed)
                    .as("3 attempts + backoff must still land inside the budget")
                    .isLessThan(BOUNDED_MS);
        });

        assertThat(writer.flushCalls()).isZero();
    }

    private static long elapsedMsSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private static ProjectionLedgerStore.Entry entry() {
        return new ProjectionLedgerStore.Entry("evt-" + System.nanoTime(),
                ProjectionLedger.State.RECEIVED, null, 0, null, null,
                1_700_000_000_000L, null);
    }

    /** Same shape the existing gateway tests use for a minimal live config. */
    private static GatewayConfig testConfig() {
        return new GatewayConfig(
                "localhost:9123", "test_db", "intent", "gate", "attempts", "correlation",
                "ledger", "halt", "127.0.0.1", 0, "http://nautilus:9500", "v1", "secret",
                Duration.ofMillis(2000), Duration.ofMillis(100), "scope", "partition");
    }
}
