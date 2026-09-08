package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.EodControllerState;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/** Unit tests for the durable per-day offload record + its state machine (SCH-23). */
class EodOffloadRecordTest {

    private static final long NOW = 1_752_000_000_000L;
    private static final LocalDate DAY = LocalDate.of(2026, 8, 14);

    @Test
    void initialRecordIsPendingWithSourceExpiryNever() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW);
        assertThat(r.tradingDate()).isEqualTo("2026-08-14");
        assertThat(r.state()).isEqualTo(EodControllerState.PENDING);
        assertThat(r.retryCount()).isZero();
        assertThat(r.nextRetryAtMs()).isZero();
        assertThat(r.earliestAllowedSourceExpiryMs()).isEqualTo(Long.MAX_VALUE);
        assertThat(r.permitsSourceExpiry()).isFalse();
        assertThat(r.requiresRetentionExtension())
                .as("a PENDING day must extend live retention").isTrue();
    }

    @Test
    void happyPathReachesVerifiedAndReleasesSourceExpiry() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.COMMITTED, NOW)
                .transition(EodControllerState.VERIFYING, NOW)
                .transition(EodControllerState.VERIFIED, NOW);
        assertThat(r.state()).isEqualTo(EodControllerState.VERIFIED);
        assertThat(r.earliestAllowedSourceExpiryMs()).isEqualTo(NOW);
        assertThat(r.permitsSourceExpiry()).isTrue();
        assertThat(r.requiresRetentionExtension()).isFalse();
        assertThat(r.nextRetryAtMs()).isZero();
    }

    @Test
    void retryableFailureIncrementsRetryCountAndSchedulesBackoff() {
        EodOffloadRecord failed = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_RETRYABLE, NOW);
        assertThat(failed.state()).isEqualTo(EodControllerState.FAILED_RETRYABLE);
        assertThat(failed.retryCount()).isEqualTo(1);
        assertThat(failed.nextRetryAtMs()).isGreaterThan(NOW);
        assertThat(failed.nextRetryAtMs() - NOW).isBetween(1_600L, 2_399L);
        assertThat(failed.permitsSourceExpiry())
                .as("a retryable day must never permit source expiry").isFalse();

        // retry clears the schedule; a second failure backoffs harder
        EodOffloadRecord retrying = failed.transition(EodControllerState.WRITING, NOW);
        assertThat(retrying.nextRetryAtMs()).isZero();
        EodOffloadRecord failedAgain = retrying
                .transition(EodControllerState.FAILED_RETRYABLE, NOW);
        assertThat(failedAgain.retryCount()).isEqualTo(2);
        assertThat(failedAgain.nextRetryAtMs() - NOW).isBetween(3_200L, 4_799L);
    }

    @Test
    void manualFailureRequiresExplicitResetToPending() {
        EodOffloadRecord manual = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_MANUAL, NOW);
        assertThat(manual.state()).isEqualTo(EodControllerState.FAILED_MANUAL);
        assertThat(manual.requiresRetentionExtension()).isTrue();
        EodOffloadRecord reset = manual.transition(EodControllerState.PENDING, NOW);
        assertThat(reset.state()).isEqualTo(EodControllerState.PENDING);
    }

    @Test
    void manualResetClearsRetryBudgetButVerifiedKeepsHistory() {
        // P4-341/345: FAILED_MANUAL → PENDING resets retryCount + schedule
        // (next failure backs off from a clean slate); VERIFIED retains the
        // count as monotonic per-day history (terminal — no future backoff).
        EodOffloadRecord failed = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_RETRYABLE, NOW);
        assertThat(failed.retryCount()).isEqualTo(1);
        EodOffloadRecord reset = failed
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_MANUAL, NOW)
                .transition(EodControllerState.PENDING, NOW);
        assertThat(reset.retryCount()).isZero();
        assertThat(reset.nextRetryAtMs()).isZero();
        EodOffloadRecord refailed = reset
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_RETRYABLE, NOW);
        assertThat(refailed.retryCount()).isEqualTo(1);

        EodOffloadRecord verified = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_RETRYABLE, NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.COMMITTED, NOW)
                .transition(EodControllerState.VERIFYING, NOW)
                .transition(EodControllerState.VERIFIED, NOW);
        assertThat(verified.retryCount()).isEqualTo(1);
        assertThat(verified.nextRetryAtMs()).isZero();
    }

    @Test
    void tunedTransitionOverloadIsDeterministic() {
        // P4-342/344: explicit base/max/rng — a seeded rng replays exactly.
        java.util.Random seedA = new java.util.Random(7);
        java.util.Random seedB = new java.util.Random(7);
        EodOffloadRecord base = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW);
        EodOffloadRecord first = base.transition(
                EodControllerState.FAILED_RETRYABLE, NOW, 1_000L, 300_000L, seedA);
        EodOffloadRecord second = base.transition(
                EodControllerState.FAILED_RETRYABLE, NOW, 1_000L, 300_000L, seedB);
        assertThat(first.nextRetryAtMs()).isEqualTo(second.nextRetryAtMs());
        assertThat(first.nextRetryAtMs() - NOW).isBetween(1_600L, 2_399L);
    }

    @Test
    void illegalAndRegressiveTransitionsThrow() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW);
        assertThatThrownBy(() -> r.transition(EodControllerState.COMMITTED, NOW))
                .as("PENDING → COMMITTED skips WRITING").isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> r.transition(EodControllerState.VERIFIED, NOW))
                .as("PENDING → VERIFIED skips the pipeline").isInstanceOf(IllegalStateException.class);

        EodOffloadRecord verified = r.transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.COMMITTED, NOW)
                .transition(EodControllerState.VERIFYING, NOW)
                .transition(EodControllerState.VERIFIED, NOW);
        assertThatThrownBy(() -> verified.transition(EodControllerState.PENDING, NOW))
                .as("a VERIFIED day must never silently regress")
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> verified.transition(EodControllerState.FAILED_RETRYABLE, NOW))
                .as("VERIFIED is terminal").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void legalTransitionMapCoversTheMachineEdges() {
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.PENDING,
                EodControllerState.WRITING)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.VERIFYING,
                EodControllerState.VERIFIED)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.FAILED_RETRYABLE,
                EodControllerState.WRITING)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.FAILED_MANUAL,
                EodControllerState.PENDING)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.VERIFIED,
                EodControllerState.PENDING)).isFalse();
    }

    @Test
    void retryReEntersWritingOnly() {
        // P4-287/289: the FAILED_RETRYABLE -> VERIFYING bypass is cut — a
        // WRITING failure must re-commit before it can verify.
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.FAILED_RETRYABLE,
                EodControllerState.VERIFYING)).isFalse();
    }

    @Test
    void compactCtorFailsFast() {
        // P4-286/288: null identity/state, malformed date, negative counts.
        EodOffloadRecord good = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW);
        assertThatThrownBy(() -> new EodOffloadRecord(null, good.tableName(),
                good.schemaVersion(), -1, -1, 0, 0, "", "", "", good.state(), 0, 0, 0, NOW))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new EodOffloadRecord("2026/08/31", good.tableName(),
                good.schemaVersion(), -1, -1, 0, 0, "", "", "", good.state(), 0, 0, 0, NOW))
                .isInstanceOf(java.time.DateTimeException.class);
        assertThatThrownBy(() -> new EodOffloadRecord(good.tradingDate(), good.tableName(),
                good.schemaVersion(), -1, -1, -1, 0, "", "", "", good.state(), 0, 0, 0, NOW))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dateFormattingRoundTrips() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW);
        assertThat(r.tradingDateAsLocalDate()).isEqualTo(DAY);
        assertThat(EodOffloadRecord.parseTradingDate(EodOffloadRecord.formatTradingDate(DAY)))
                .isEqualTo(DAY);
    }
}
