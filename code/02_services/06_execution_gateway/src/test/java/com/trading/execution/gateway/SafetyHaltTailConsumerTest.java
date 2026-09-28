package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import com.trading.common.schema.execution.SafetyHaltRequest;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

/**
 * H1-1: the durable Safety_Halt_Requests consumer, offline (in-memory control + gate stores).
 *
 * <p>Pins the contract the gateway now relies on: boot replay applies OPEN UNSAFE rows, records
 * the outcome on the row, only UNSAFE rows halt, scope is account AND partition, the epoch fence
 * is rebuilt from APPLIED rows on every pass (restart-safe), a crash between halt and audit
 * re-applies idempotently, and a dead poll loop is loud.
 */
class SafetyHaltTailConsumerTest {

    private static final String PARTITION = "p-1";
    private static final String ACCOUNT = "acct-1";
    private static final String SOURCE = "signal-job";

    private static GateRow haltedBoot() {
        return new GateRow(PARTITION, ACCOUNT, GateState.HALTED, 0, "boot", "ev0",
                null, null, null, null, 0L, null, null, null);
    }

    private static GateRow enabled(long epoch) {
        return new GateRow(PARTITION, ACCOUNT, GateState.ENABLED, epoch, "approved", "ev0",
                "saurabh", null, "approved-ev", "owner", 1L, 1000L, 10_000L, null);
    }

    private static SafetyHaltRequest halt(
            String account, String partition, String reason, long ts, long epoch, String state) {
        return SafetyHaltRequest.createValidated(account, partition, SOURCE, "i-1", reason, "detail",
                ts, epoch, "abc123", "v3", null, 0L, null, state);
    }

    private static InternalRow tamperedRow() {
        Object[] v = new Object[21];
        v[0] = BinaryString.fromString("bad-id-not-sha256");
        v[1] = BinaryString.fromString(ACCOUNT);
        v[2] = null;
        v[3] = BinaryString.fromString(PARTITION);
        v[4] = BinaryString.fromString(SOURCE);
        v[5] = BinaryString.fromString("i-1");
        v[6] = BinaryString.fromString("CLOCK_DRIFT");
        v[7] = BinaryString.fromString("detail");
        v[8] = 2000L;
        v[9] = 1L;
        v[10] = BinaryString.fromString("abc123");
        v[11] = BinaryString.fromString("OPEN");
        v[12] = null;
        v[13] = BinaryString.fromString("v3");
        v[14] = null;
        v[15] = 0L;
        v[16] = null;
        v[17] = BinaryString.fromString("abc123");
        v[18] = BinaryString.fromString("UNSAFE");
        v[19] = null;
        v[20] = 2;
        return GenericRow.of(v);
    }

    private static SafetyHaltTailConsumer consumer(
            ControlStateStore store, InMemoryGateStateStore gates) {
        return new SafetyHaltTailConsumer(store, gates, PARTITION, 1000L, null);
    }

    @Test
    void bootReplayAppliesAnOpenUnsafeRowAndAuditsIt() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        SafetyHaltRequest row = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 2000L, 1L, "UNSAFE");
        store.add(row);

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(gates.read(PARTITION).state()).isEqualTo(GateState.HALTED);
        assertThat(store.applicationResult(row.haltRequestId())).isEqualTo("APPLIED");
        assertThat(store.appliedTs(row.haltRequestId())).isNotNull();
        assertThat(consumer.appliedCount()).isEqualTo(1);
        assertThat(consumer.rejectedCount()).isZero();
    }

    @Test
    void aSecondPassSkipsTheAppliedRowAndDoesNotBumpTheEpochAgain() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        store.add(halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 2000L, 1L, "UNSAFE"));

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();
        long epochAfterApply = gates.read(PARTITION).epoch();
        consumer.replayOnce();

        assertThat(gates.read(PARTITION).epoch())
                .as("the durable APPLIED result must stop the 1 s replay from re-halting")
                .isEqualTo(epochAfterApply);
        assertThat(consumer.appliedCount()).isEqualTo(1);
    }

    @Test
    void aRecoveredRowIsAuditedWithoutAnyGateEffect() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.install(enabled(3L));
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        SafetyHaltRequest row = halt(ACCOUNT, PARTITION, "SIGNAL_RECOVERED", 3000L, 2L, "RECOVERED");
        store.add(row);

        consumer(store, gates).replayOnce();

        assertThat(gates.read(PARTITION).state())
                .as("RECOVERED is audit-only and never auto-enables or halts")
                .isEqualTo(GateState.ENABLED);
        assertThat(gates.read(PARTITION).epoch()).isEqualTo(3L);
        assertThat(store.applicationResult(row.haltRequestId())).isEqualTo("APPLIED");
    }

    @Test
    void aForeignPartitionHaltIsRejectedWithoutTouchingThisGate() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.install(enabled(3L));
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        SafetyHaltRequest row = halt(ACCOUNT, "p-OTHER", "CLOCK_DRIFT", 2000L, 1L, "UNSAFE");
        store.add(row);

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(gates.read(PARTITION).state()).isEqualTo(GateState.ENABLED);
        assertThat(gates.read(PARTITION).epoch()).isEqualTo(3L);
        assertThat(store.applicationResult(row.haltRequestId())).isEqualTo("REJECTED");
        assertThat(consumer.rejectedCount()).isEqualTo(1);
    }

    @Test
    void anAccountScopeMismatchIsRejectedByTheGateRowCheck() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        SafetyHaltRequest row = halt("acct-OTHER", PARTITION, "CLOCK_DRIFT", 2000L, 1L, "UNSAFE");
        store.add(row);

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(gates.read(PARTITION).epoch()).isZero();
        assertThat(store.applicationResult(row.haltRequestId())).isEqualTo("REJECTED");
        assertThat(consumer.rejectedCount()).isEqualTo(1);
    }

    @Test
    void theGreatestEpochOfASourceWinsAndTheStaleOneIsRejected() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        SafetyHaltRequest newer = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 5000L, 7L, "UNSAFE");
        SafetyHaltRequest older = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 4000L, 5L, "UNSAFE");
        store.add(older);
        store.add(newer);

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(store.applicationResult(newer.haltRequestId())).isEqualTo("APPLIED");
        assertThat(store.applicationResult(older.haltRequestId())).isEqualTo("REJECTED");
        assertThat(consumer.appliedCount()).isEqualTo(1);
        assertThat(consumer.rejectedCount()).isEqualTo(1);
    }

    @Test
    void aRestartRebuildsTheEpochFenceFromDurableAppliedRows() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        SafetyHaltRequest applied = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 5000L, 7L, "UNSAFE");
        store.add(applied);
        consumer(store, gates).replayOnce();
        assertThat(store.applicationResult(applied.haltRequestId())).isEqualTo("APPLIED");

        // A new process (fresh consumer, fresh processor dedup set) sees only durable truth: the
        // APPLIED row rebuilds the per-source max, so a lower epoch is stale.
        SafetyHaltRequest stale = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 6000L, 5L, "UNSAFE");
        store.add(stale);
        SafetyHaltTailConsumer restarted = consumer(store, gates);
        restarted.replayOnce();

        assertThat(store.applicationResult(stale.haltRequestId())).isEqualTo("REJECTED");
        assertThat(restarted.rejectedCount()).isEqualTo(1);
        assertThat(restarted.appliedCount()).isZero();
    }

    @Test
    void theEpochFenceIsPerSourceInstanceNotJustComponent() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        // Same component, different instances (a restarted signal job) with independent epoch
        // streams: instance B's epoch 2 must not be fenced by instance A's epoch 9.
        SafetyHaltRequest instanceA = SafetyHaltRequest.createValidated(ACCOUNT, PARTITION, SOURCE,
                "i-A", "CLOCK_DRIFT", "detail", 2000L, 9L, "abc123", "v3", null, 0L, null, "UNSAFE");
        SafetyHaltRequest instanceB = SafetyHaltRequest.createValidated(ACCOUNT, PARTITION, SOURCE,
                "i-B", "CLOCK_DRIFT", "detail", 3000L, 2L, "abc123", "v3", null, 0L, null, "UNSAFE");
        store.add(instanceA);
        store.add(instanceB);

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(store.applicationResult(instanceA.haltRequestId())).isEqualTo("APPLIED");
        assertThat(store.applicationResult(instanceB.haltRequestId())).isEqualTo("APPLIED");
        assertThat(consumer.rejectedCount()).isZero();
    }

    @Test
    void aForeignPartitionsAppliedRowDoesNotFenceThisPartitionsEpochs() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        // A sibling gateway already applied epoch 9 for the SAME component+instance tuple.
        SafetyHaltRequest foreign = SafetyHaltRequest.createValidated(ACCOUNT, "p-OTHER", SOURCE,
                "i-1", "CLOCK_DRIFT", "detail", 2000L, 9L, "abc123", "v3", null, 0L, null, "UNSAFE");
        store.add(foreign);
        store.recordApplication(InMemoryControlStateStore.toRow(foreign), "APPLIED", 100L);
        // Our own halt for the same source instance at epoch 2 must NOT read as stale.
        SafetyHaltRequest ours = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 3000L, 2L, "UNSAFE");
        store.add(ours);

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(store.applicationResult(ours.haltRequestId())).isEqualTo("APPLIED");
        assertThat(consumer.rejectedCount()).isZero();
    }

    @Test
    void aMissingGateRowDefersAndLeavesTheRowOpenForTheNextPass() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore(); // no row for the partition
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        SafetyHaltRequest row = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 2000L, 1L, "UNSAFE");
        store.add(row);

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(store.applicationResult(row.haltRequestId()))
                .as("a deferred halt must stay OPEN, never be dropped as rejected")
                .isNull();
        assertThat(consumer.deferredCount()).isEqualTo(1);
    }

    @Test
    void anInvalidIdRowIsLeftUntouchedAndCounted() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        store.addRawRow(tamperedRow());

        SafetyHaltTailConsumer consumer = consumer(store, gates);
        consumer.replayOnce();

        assertThat(consumer.invalidCount()).isEqualTo(1);
        assertThat(gates.read(PARTITION).epoch()).isZero();
        assertThat(consumer.appliedCount()).isZero();
    }

    @Test
    void aWriteFailureThrowsAndTheRestartReappliesThenAudits() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.install(enabled(3L));
        InMemoryControlStateStore real = new InMemoryControlStateStore();
        SafetyHaltRequest row = halt(ACCOUNT, PARTITION, "CLOCK_DRIFT", 2000L, 1L, "UNSAFE");
        real.add(row);

        // Crash window: the gate halt lands, the audit write fails.
        ControlStateStore failing = new FailingWriteback(real);
        SafetyHaltTailConsumer first = consumer(failing, gates);
        assertThatThrownBy(first::replayOnce).isInstanceOf(IllegalStateException.class);
        assertThat(gates.read(PARTITION).state())
                .as("the halt is applied first — fail-closed even when the audit cannot be written")
                .isEqualTo(GateState.HALTED);
        assertThat(real.applicationResult(row.haltRequestId())).isNull();

        // Restart: the row is still OPEN, so it re-applies idempotently and is audited.
        SafetyHaltTailConsumer restart = consumer(real, gates);
        restart.replayOnce();
        assertThat(real.applicationResult(row.haltRequestId())).isEqualTo("APPLIED");
        assertThat(gates.read(PARTITION).state()).isEqualTo(GateState.HALTED);
    }

    @Test
    void aDeadPollLoopCallsOnDeath() throws Exception {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        gates.init(haltedBoot());
        InMemoryControlStateStore real = new InMemoryControlStateStore();
        ControlStateStore flaky = new FailOnSecondReplay(real);

        CountDownLatch died = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        SafetyHaltTailConsumer consumer =
                new SafetyHaltTailConsumer(flaky, gates, PARTITION, 10L, t -> {
                    failure.set(t);
                    died.countDown();
                });
        consumer.replayOnce(); // first pass is clean
        consumer.start();
        try {
            assertThat(died.await(2, TimeUnit.SECONDS))
                    .as("a store failure must stop the loop and notify the service")
                    .isTrue();
            assertThat(failure.get()).isInstanceOf(IllegalStateException.class);
            assertThat(consumer.failed()).isTrue();
        } finally {
            consumer.close();
        }
    }

    @Test
    void pollPeriodMustBePositive() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore();
        InMemoryControlStateStore store = new InMemoryControlStateStore();
        assertThatThrownBy(() -> new SafetyHaltTailConsumer(store, gates, PARTITION, 0L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Delegates to a real in-memory store, but fails the first application write (H1-1 crash). */
    private static final class FailingWriteback implements ControlStateStore {
        private final InMemoryControlStateStore delegate;
        private boolean failed;

        FailingWriteback(InMemoryControlStateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Lookup lookup(String tableName, List<Object> keyFields) {
            return delegate.lookup(tableName, keyFields);
        }

        @Override
        public void replaySafetyHalts(Consumer<InternalRow> consumer) {
            delegate.replaySafetyHalts(consumer);
        }

        @Override
        public void recordApplication(InternalRow row, String applicationResult, long appliedTs) {
            if (!failed) {
                failed = true;
                throw new IllegalStateException("simulated disk failure between halt and audit");
            }
            delegate.recordApplication(row, applicationResult, appliedTs);
        }

        @Override
        public void close() {}
    }

    /** Delegates to a real in-memory store, but fails the second replay (daemon-death drill). */
    private static final class FailOnSecondReplay implements ControlStateStore {
        private final InMemoryControlStateStore delegate;
        private int replays;

        FailOnSecondReplay(InMemoryControlStateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Lookup lookup(String tableName, List<Object> keyFields) {
            return delegate.lookup(tableName, keyFields);
        }

        @Override
        public void replaySafetyHalts(Consumer<InternalRow> consumer) {
            if (++replays > 1) {
                throw new IllegalStateException("simulated scan failure");
            }
            delegate.replaySafetyHalts(consumer);
        }

        @Override
        public void recordApplication(InternalRow row, String applicationResult, long appliedTs) {
            delegate.recordApplication(row, applicationResult, appliedTs);
        }

        @Override
        public void close() {}
    }
}
