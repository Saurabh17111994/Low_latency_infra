package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.EodControllerState;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * EOD controller orchestration unit tests (SCH-23): the planning/status
 * decisions, the due-day selection, the full offload drive (success, fail-
 * closed, retry with backoff, verify-failure, crash-resume, reconcile,
 * manual reset), and the single-writer lease — all against the in-memory
 * store with the mock/drill executor.
 */
class EodControllerTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    private static final Duration LIVE_TTL = Duration.ofDays(2);
    private static final Duration FLOOR = Duration.ofDays(7);

    private static final LocalDate D1 = LocalDate.of(2026, 8, 13);
    private static final LocalDate D2 = LocalDate.of(2026, 8, 14);

    private static final Instant NOW = Instant.parse("2026-08-14T15:00:00Z");
    private static final long NOW_MS = NOW.toEpochMilli();

    private static final String TABLE = "candle_closed";

    private static EodOffloadRecord verifiedDay(LocalDate date) {
        return EodOffloadRecord.initial(date, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS)
                .transition(EodControllerState.COMMITTED, NOW_MS)
                .transition(EodControllerState.VERIFYING, NOW_MS)
                .transition(EodControllerState.VERIFIED, NOW_MS);
    }

    // ── planning / status ─────────────────────────────────────────────────

    @Test
    void planTablesRequiresLiveTtlPerScopedTable() {
        assertThatThrownBy(() -> EodController.planTables(
                List.of(), List.of("mystery_table"), Map.of(), KOLKATA, FLOOR, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no live TTL");
    }

    @Test
    void statusIsOkWhenAllDaysVerifiedAndMarginSafe() {
        Instant safe = Instant.parse("2026-08-05T12:00:00Z");
        List<EodOffloadRecord> days = List.of(
                verifiedDay(LocalDate.of(2026, 8, 10)),
                verifiedDay(LocalDate.of(2026, 8, 11)),
                verifiedDay(LocalDate.of(2026, 8, 12)),
                verifiedDay(LocalDate.of(2026, 8, 13)));
        List<EodController.TablePlan> plans = EodController.planTables(
                days, List.of(TABLE), Map.of(TABLE, LIVE_TTL), KOLKATA, FLOOR, safe);
        assertThat(plans).hasSize(1);
        assertThat(plans.get(0).verifiedDays()).isEqualTo(4);
        assertThat(plans.get(0).unverifiedDays()).isZero();
        assertThat(EodController.statusOf(plans, FLOOR)).isEqualTo(EodController.Status.OK);
    }

    @Test
    void statusIsPendingWorkWithUnverifiedDaysWhenMarginIsSafe() {
        // a future pending day whose source-expiry bound is far out — unverified
        // work exists, but neither its bound nor the 3-day floor has collapsed
        // the safety floor, so this is PENDING_WORK, not EXTENSION_REQUIRED.
        LocalDate future = D2.plusDays(10);
        List<EodOffloadRecord> days = List.of(
                EodOffloadRecord.initial(future, TABLE, "2", NOW_MS));
        List<EodController.TablePlan> plans = EodController.planTables(
                days, List.of(TABLE), Map.of(TABLE, LIVE_TTL), KOLKATA, FLOOR, NOW);
        assertThat(plans.get(0).unverifiedDays()).isEqualTo(1);
        assertThat(plans.get(0).plan().requiresExtension()).isFalse();
        assertThat(EodController.statusOf(plans, FLOOR))
                .isEqualTo(EodController.Status.PENDING_WORK);
    }

    @Test
    void statusIsExtensionRequiredWhenMarginCollapses() {
        List<EodOffloadRecord> days = List.of(
                verifiedDay(LocalDate.of(2026, 8, 10)),
                verifiedDay(LocalDate.of(2026, 8, 11)),
                verifiedDay(LocalDate.of(2026, 8, 12)),
                verifiedDay(LocalDate.of(2026, 8, 13)));
        // floor bound = D2 (third-most-recent) expiry: 2026-08-14T00:00 IST + 2d;
        // a now 6h before it collapses the 7d floor.
        Instant tight = EodRetentionPolicy.sourceExpiryBound(
                LocalDate.of(2026, 8, 11), KOLKATA, LIVE_TTL).minus(Duration.ofHours(6));
        List<EodController.TablePlan> plans = EodController.planTables(
                days, List.of(TABLE), Map.of(TABLE, LIVE_TTL), KOLKATA,
                Duration.ofDays(30), tight);
        assertThat(plans.get(0).plan().requiresExtension()).isTrue();
        assertThat(EodController.statusOf(plans, Duration.ofDays(30)))
                .isEqualTo(EodController.Status.EXTENSION_REQUIRED);
    }

    @Test
    void noDaysTableIsReportedNotPlanned() {
        List<EodController.TablePlan> plans = EodController.planTables(
                List.of(), List.of(TABLE), Map.of(TABLE, LIVE_TTL), KOLKATA, FLOOR, NOW);
        assertThat(plans.get(0).noDays()).isTrue();
        assertThat(EodController.statusOf(plans, FLOOR)).isEqualTo(EodController.Status.OK);
    }

    // ── due-day selection ──────────────────────────────────────────────────

    @Test
    void dueDaysSelectsPendingInFlightAndElapsedRetriesOnly() {
        EodOffloadRecord pendingPast = EodOffloadRecord.initial(D1, TABLE, "2", NOW_MS);
        EodOffloadRecord pendingFuture = EodOffloadRecord.initial(D2.plusDays(1), TABLE, "2", NOW_MS);
        EodOffloadRecord writing = EodOffloadRecord.initial(D1, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS);
        EodOffloadRecord verifying = EodOffloadRecord.initial(D1, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS)
                .transition(EodControllerState.COMMITTED, NOW_MS)
                .transition(EodControllerState.VERIFYING, NOW_MS);
        // retry due (backoff elapsed) vs retry future (backoff not elapsed)
        EodOffloadRecord retryDue = new EodOffloadRecord("2026-08-13", TABLE, "2",
                -1, -1, 0, 0, "", "", "", EodControllerState.FAILED_RETRYABLE, 1,
                NOW_MS - 1_000L, Long.MAX_VALUE, NOW_MS);
        EodOffloadRecord retryFuture = new EodOffloadRecord("2026-08-13", TABLE, "2",
                -1, -1, 0, 0, "", "", "", EodControllerState.FAILED_RETRYABLE, 1,
                NOW_MS + Duration.ofDays(1).toMillis(), Long.MAX_VALUE, NOW_MS);
        EodOffloadRecord manual = EodOffloadRecord.initial(D1, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS)
                .transition(EodControllerState.FAILED_MANUAL, NOW_MS);

        List<EodOffloadRecord> due = EodController.dueDays(
                List.of(pendingPast, pendingFuture, writing, verifying, retryDue,
                        retryFuture, manual, verifiedDay(D1)),
                D2, NOW_MS);

        assertThat(due).extracting(EodOffloadRecord::state)
                .contains(EodControllerState.PENDING,
                        EodControllerState.WRITING,
                        EodControllerState.VERIFYING,
                        EodControllerState.FAILED_RETRYABLE);
        assertThat(due).doesNotContain(pendingFuture, retryFuture, manual, verifiedDay(D1));
    }

    @Test
    void futureDatedInFlightDaysWaitForTheirBoundary() {
        // P4-271: a future WRITING day (clock skew / pre-seeded) must not
        // advance before its EOD boundary, even though it is in-flight.
        EodOffloadRecord futureWriting = EodOffloadRecord.initial(D2.plusDays(1), TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS);
        List<EodOffloadRecord> due = EodController.dueDays(
                List.of(futureWriting), D2, NOW_MS);
        assertThat(due).isEmpty();
    }

    @Test
    void verifyThrowSurfacesRootCause() throws Exception {
        // P4-273: a throwing verify must land FAILED_RETRYABLE with the cause
        // in the note, not a generic "did not reconcile".
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        EodOffloadExecutor throwing = new EodOffloadExecutor() {
            @Override
            public OffloadResult offload(EodOffloadRecord record) {
                return new OffloadResult(true, 0, 0, 1, 1, "s", "t", "snap", null);
            }

            @Override
            public boolean verify(EodOffloadRecord committedOrVerifying) {
                throw new IllegalStateException("R2 SigV4 timeout");
            }
        };
        store.upsert(EodOffloadRecord.initial(D1, TABLE, "2", NOW_MS));
        List<EodController.RunOutcome> outcomes = EodController.runOnce(
                store, throwing, D1, List.of(TABLE), "2", NOW);
        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).to()).isEqualTo(EodControllerState.FAILED_RETRYABLE);
        assertThat(outcomes.get(0).note()).contains("R2 SigV4 timeout");
    }

    @Test
    void runOnceIgnoresOutOfScopeTables() throws Exception {
        // P4-272: a store holding another table's due day must not have it
        // mutated by this run.
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        store.upsert(EodOffloadRecord.initial(D1, "OTHER_TABLE", "2", NOW_MS));
        MockEodOffloadExecutor executor = new MockEodOffloadExecutor(true, true);
        List<EodController.RunOutcome> outcomes = EodController.runOnce(
                store, executor, D1, List.of(TABLE), "2", NOW);
        assertThat(outcomes).extracting(EodController.RunOutcome::table)
                .doesNotContain("OTHER_TABLE");
        assertThat(store.raw().get(EodOffloadStateColumns.recordId(
                D1.toString(), "OTHER_TABLE")).state())
                .isEqualTo(EodControllerState.PENDING);
    }

    // ── run drive ──────────────────────────────────────────────────────────

    @Test
    void runOnceAdvancesRunDateToVerifiedWithMockOffload() throws Exception {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        MockEodOffloadExecutor executor = new MockEodOffloadExecutor(true, true);

        List<EodController.RunOutcome> outcomes = EodController.runOnce(
                store, executor, D2, List.of(TABLE), "2", NOW);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).to()).isEqualTo(EodControllerState.VERIFIED);
        assertThat(outcomes.get(0).verified()).isTrue();
        assertThat(executor.offloadCalls()).isEqualTo(1);
        assertThat(executor.verifyCalls()).isEqualTo(1);

        EodOffloadRecord record = store.raw().get(EodOffloadStateColumns.recordId(
                D2.toString(), TABLE));
        assertThat(record.state()).isEqualTo(EodControllerState.VERIFIED);
        assertThat(record.permitsSourceExpiry()).isTrue();
        assertThat(record.earliestAllowedSourceExpiryMs()).isEqualTo(NOW_MS);
        assertThat(record.rowCount()).isEqualTo(1_000L);
        assertThat(record.sourceHash()).isEqualTo("mock-source-hash");
        assertThat(record.targetHash()).isEqualTo("mock-target-hash");
        assertThat(record.icebergSnapshotId()).isEqualTo("mock-iceberg-snapshot-1");
    }

    @Test
    void runOnceFailsClosedWithNotConfiguredExecutor() throws Exception {
        InMemoryEodStateStore store = new InMemoryEodStateStore();

        List<EodController.RunOutcome> outcomes = EodController.runOnce(
                store, NotConfiguredEodOffloadExecutor.INSTANCE, D2, List.of(TABLE), "2", NOW);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).to()).isEqualTo(EodControllerState.FAILED_RETRYABLE);
        assertThat(outcomes.get(0).verified()).isFalse();
        assertThat(outcomes.get(0).note()).contains("not configured");

        EodOffloadRecord record = store.raw().get(EodOffloadStateColumns.recordId(
                D2.toString(), TABLE));
        assertThat(record.state()).isEqualTo(EodControllerState.FAILED_RETRYABLE);
        assertThat(record.permitsSourceExpiry())
                .as("an unverified day must never permit source expiry").isFalse();
        assertThat(record.retryCount()).isEqualTo(1);
        assertThat(record.nextRetryAtMs()).isGreaterThan(NOW_MS);
    }

    @Test
    void retryAfterBackoffAdvancesFailedDayToVerified() throws Exception {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        MockEodOffloadExecutor failing = new MockEodOffloadExecutor(false, true);
        EodController.runOnce(store, failing, D2, List.of(TABLE), "2", NOW);

        EodOffloadRecord failed = store.raw().get(EodOffloadStateColumns.recordId(
                D2.toString(), TABLE));
        assertThat(failed.state()).isEqualTo(EodControllerState.FAILED_RETRYABLE);

        // next pass, well after the backoff window, with a healthy executor
        Instant later = NOW.plus(Duration.ofHours(1));
        MockEodOffloadExecutor healthy = new MockEodOffloadExecutor(true, true);
        List<EodController.RunOutcome> outcomes = EodController.runOnce(
                store, healthy, D2, List.of(TABLE), "2", later);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).to()).isEqualTo(EodControllerState.VERIFIED);
        EodOffloadRecord verified = store.raw().get(EodOffloadStateColumns.recordId(
                D2.toString(), TABLE));
        assertThat(verified.state()).isEqualTo(EodControllerState.VERIFIED);
        assertThat(verified.retryCount()).isEqualTo(1); // the failed attempt is recorded
    }

    @Test
    void verifyFailureLandsRetryableThenVerifiesOnRetry() throws Exception {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        MockEodOffloadExecutor badVerify = new MockEodOffloadExecutor(true, false);

        EodController.runOnce(store, badVerify, D2, List.of(TABLE), "2", NOW);
        EodOffloadRecord failed = store.raw().get(EodOffloadStateColumns.recordId(
                D2.toString(), TABLE));
        assertThat(failed.state()).isEqualTo(EodControllerState.FAILED_RETRYABLE);
        assertThat(failed.targetHash())
                .as("the offload content evidence must survive a verify failure")
                .isEqualTo("mock-target-hash");

        Instant later = NOW.plus(Duration.ofHours(1));
        MockEodOffloadExecutor healthy = new MockEodOffloadExecutor(true, true);
        EodController.runOnce(store, healthy, D2, List.of(TABLE), "2", later);
        assertThat(store.raw().get(EodOffloadStateColumns.recordId(D2.toString(), TABLE)).state())
                .isEqualTo(EodControllerState.VERIFIED);
    }

    @Test
    void crashedInFlightDayIsReDrivenToVerified() throws Exception {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        // a crash left the day in WRITING (offload evidence absent)
        store.upsert(EodOffloadRecord.initial(D2, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS));

        List<EodController.RunOutcome> outcomes = EodController.runOnce(
                store, new MockEodOffloadExecutor(true, true), D2, List.of(TABLE), "2", NOW);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).to()).isEqualTo(EodControllerState.VERIFIED);
    }

    @Test
    void advanceRefusesVerifiedAndManualDays() {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        MockEodOffloadExecutor executor = new MockEodOffloadExecutor(true, true);
        assertThatThrownBy(() -> EodController.advance(store, executor, verifiedDay(D1), NOW_MS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not advance");
        EodOffloadRecord manual = EodOffloadRecord.initial(D1, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS)
                .transition(EodControllerState.FAILED_MANUAL, NOW_MS);
        assertThatThrownBy(() -> EodController.advance(store, executor, manual, NOW_MS))
                .isInstanceOf(IllegalStateException.class);
    }

    // ── reconcile / reset ──────────────────────────────────────────────────

    @Test
    void throwingExecutorFailsClosedToRetryable() throws Exception {
        EodOffloadExecutor throwing = new EodOffloadExecutor() {
            @Override
            public OffloadResult offload(EodOffloadRecord record) throws Exception {
                throw new java.io.IOException("lake unreachable");
            }

            @Override
            public boolean verify(EodOffloadRecord committedOrVerifying) {
                return true;
            }
        };
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        store.upsert(EodOffloadRecord.initial(D2, TABLE, "2", NOW_MS));
        java.util.List<EodController.RunOutcome> outcomes = EodController.runOnce(
                store, throwing, D2, List.of(TABLE), "2", NOW);
        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).to()).isEqualTo(EodControllerState.FAILED_RETRYABLE);
        assertThat(outcomes.get(0).note()).contains("lake unreachable");
        assertThat(store.raw().get(EodOffloadStateColumns.recordId(D2.toString(), TABLE)).state())
                .isEqualTo(EodControllerState.FAILED_RETRYABLE);
    }

    @Test
    void offloadResultInvariantsFailFast() {
        assertThatThrownBy(() -> OffloadResult.failure("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OffloadResult(true, 0, 1, 1, 1, "s", "t", "snap", "boom"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OffloadResult(false, 0, 1, 1, 1, "s", "t", "snap", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OffloadResult(true, 2, 1, 1, 1, "s", "t", "snap", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void reconcileVerifiesCommittedDay() throws Exception {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        EodOffloadRecord committed = EodOffloadRecord.initial(D2, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS)
                .transition(EodControllerState.COMMITTED, NOW_MS);
        store.upsert(committed);

        List<EodController.RunOutcome> outcomes = EodController.reconcile(
                store, new MockEodOffloadExecutor(true, true), List.of(TABLE), NOW);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).to()).isEqualTo(EodControllerState.VERIFIED);
        assertThat(store.raw().get(EodOffloadStateColumns.recordId(D2.toString(), TABLE)).state())
                .isEqualTo(EodControllerState.VERIFIED);
    }

    @Test
    void resetManualReturnsManualDaysToPendingWithApproval() throws Exception {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        EodOffloadRecord manual = EodOffloadRecord.initial(D1, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS)
                .transition(EodControllerState.FAILED_MANUAL, NOW_MS);
        store.upsert(manual);

        int reset = EodController.resetManual(store, NOW, null, null);
        assertThat(reset).isEqualTo(1);
        assertThat(store.raw().get(EodOffloadStateColumns.recordId(D1.toString(), TABLE)).state())
                .isEqualTo(EodControllerState.PENDING);

        // scoped reset matches only the requested day
        store.upsert(EodOffloadRecord.initial(D2, TABLE, "2", NOW_MS)
                .transition(EodControllerState.WRITING, NOW_MS)
                .transition(EodControllerState.FAILED_MANUAL, NOW_MS));
        assertThat(EodController.resetManual(store, NOW, D1.toString(), TABLE)).isZero();
        assertThat(EodController.resetManual(store, NOW, D2.toString(), TABLE)).isEqualTo(1);
    }

    // ── lease (single-writer fencing) ──────────────────────────────────────

    @Test
    void leaseBlocksSecondControllerUntilExpiry() {
        InMemoryEodStateStore store = new InMemoryEodStateStore();
        Lease a = store.acquireLease("controller-a", NOW_MS, Duration.ofMinutes(30).toMillis());
        assertThat(a.isHeldBy("controller-a", NOW_MS)).isTrue();

        Lease b = store.acquireLease("controller-b", NOW_MS,
                Duration.ofMinutes(30).toMillis());
        assertThat(b.isHeldBy("controller-b", NOW_MS))
                .as("controller-b must be refused while controller-a holds the lease")
                .isFalse();
        assertThat(b.token()).isEqualTo("controller-a");

        // after the lease expires the same token re-acquires
        long after = NOW_MS + Duration.ofMinutes(31).toMillis();
        assertThat(store.acquireLease("controller-a", after,
                Duration.ofMinutes(30).toMillis()).isHeldBy("controller-a", after)).isTrue();
    }
}
