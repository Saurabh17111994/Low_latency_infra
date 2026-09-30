package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.EodControllerState;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the per-table EOD retention plan (SCH-23). */
class EodPlannerTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    private static final Duration LIVE_TTL = Duration.ofDays(2);
    private static final Duration SAFETY_FLOOR = Duration.ofDays(7);

    private static final LocalDate D1 = LocalDate.of(2026, 8, 10);
    private static final LocalDate D2 = LocalDate.of(2026, 8, 11);
    private static final LocalDate D3 = LocalDate.of(2026, 8, 12);
    private static final LocalDate D4 = LocalDate.of(2026, 8, 13);

    private static final long NOW = 1_752_000_000_000L;

    private static EodOffloadRecord verified(LocalDate date) {
        EodOffloadRecord r = EodOffloadRecord.initial(date, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.COMMITTED, NOW)
                .transition(EodControllerState.VERIFYING, NOW);
        return r.transition(EodControllerState.VERIFIED, NOW);
    }

    private static EodOffloadRecord pending(LocalDate date) {
        return EodOffloadRecord.initial(date, "feature_candles_15s", "2", NOW);
    }

    @Test
    void allVerifiedDaysNeverExtend() {
        // The 3-complete-trading-day floor is retired (2026-09-30, operator
        // Q36: live replay window <= 24h, backfills from the lake). Verified
        // days carry no protected bound and never force an extension.
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(verified(D1), verified(D2), verified(D3), verified(D4)),
                KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW));
        assertThat(plan.allVerified()).isTrue();
        assertThat(plan.earliestUnverifiedDate()).isNull();
        assertThat(plan.protectedExpiryBound()).isNull();
        assertThat(plan.marginMs()).isEqualTo(Long.MAX_VALUE);
        assertThat(plan.requiresExtension()).isFalse();
    }

    @Test
    void unverifiedDayIsTheOnlyProtectedBound() {
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(verified(D1), verified(D2), verified(D3), pending(D4)),
                KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW));
        assertThat(plan.allVerified()).isFalse();
        assertThat(plan.earliestUnverifiedDate()).isEqualTo(D4);
        assertThat(plan.protectedExpiryBound())
                .isEqualTo(EodRetentionPolicy.sourceExpiryBound(D4, KOLKATA, LIVE_TTL));
    }

    @Test
    void oldestUnverifiedDayWins() {
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(pending(D1), verified(D2), verified(D3), verified(D4)),
                KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW));
        assertThat(plan.earliestUnverifiedDate()).isEqualTo(D1);
        assertThat(plan.protectedExpiryBound())
                .isEqualTo(EodRetentionPolicy.sourceExpiryBound(D1, KOLKATA, LIVE_TTL));
    }

    @Test
    void weekendWithThreeDayRetentionStaysQuietWhenVerified() {
        // Monday 23:30 IST, only Friday and Monday on file, all verified, 3d
        // TTL. Under the retired floor rule Friday's bound (Tue 00:00) left a
        // 30-minute margin and forced a weekly extension; now: quiet.
        LocalDate friday = LocalDate.of(2026, 8, 14);
        LocalDate monday = LocalDate.of(2026, 8, 17);
        Instant mondayLate = monday.atTime(23, 30).atZone(KOLKATA).toInstant();
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(verified(friday), verified(monday)),
                KOLKATA, Duration.ofDays(3), Duration.ofDays(1), mondayLate);
        assertThat(plan.allVerified()).isTrue();
        assertThat(plan.requiresExtension()).isFalse();
        assertThat(plan.protectedExpiryBound()).isNull();
    }

    @Test
    void stuckUnverifiedDayExtendsWhenTheRunwayCollapses() {
        // Unverified Friday, 3d TTL: bound = Tue 00:00. With a 24h runway, 48h
        // before the bound is quiet; 12h before it forces the extension.
        LocalDate friday = LocalDate.of(2026, 8, 14);
        Instant bound = EodRetentionPolicy.sourceExpiryBound(
                friday, KOLKATA, Duration.ofDays(3));
        EodPlanner.Plan roomy = EodPlanner.plan(
                List.of(pending(friday)), KOLKATA, Duration.ofDays(3),
                Duration.ofDays(1), bound.minus(Duration.ofHours(48)));
        assertThat(roomy.requiresExtension()).isFalse();
        EodPlanner.Plan tight = EodPlanner.plan(
                List.of(pending(friday)), KOLKATA, Duration.ofDays(3),
                Duration.ofDays(1), bound.minus(Duration.ofHours(12)));
        assertThat(tight.requiresExtension()).isTrue();
        assertThat(tight.marginMs()).isLessThan(Duration.ofDays(1).toMillis());
    }

    @Test
    void extensionFiresOnlyWhenMarginCollapsesBelowTheFloor() {
        Instant bound = EodRetentionPolicy.sourceExpiryBound(D4, KOLKATA, LIVE_TTL);
        Instant closeToBound = bound.minus(Duration.ofHours(6));

        EodPlanner.Plan tight = EodPlanner.plan(
                List.of(pending(D4)), KOLKATA, LIVE_TTL, Duration.ofDays(1), closeToBound);
        assertThat(tight.requiresExtension()).isTrue();
        assertThat(tight.marginMs()).isLessThan(Duration.ofDays(1).toMillis());

        EodPlanner.Plan roomy = EodPlanner.plan(
                List.of(pending(D4)), KOLKATA, LIVE_TTL, Duration.ofHours(1), closeToBound);
        assertThat(roomy.requiresExtension()).isFalse();
        assertThat(roomy.marginMs()).isGreaterThanOrEqualTo(Duration.ofHours(1).toMillis());
    }

    @Test
    void emptyDayListIsRejected() {
        assertThatThrownBy(() -> EodPlanner.plan(
                List.of(), KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodPlanner.plan(
                null, KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
