package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

/** Unit tests for the EOD retention margin/extension arithmetic (SCH-23). */
class EodRetentionPolicyTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata"); // UTC+5:30
    private static final Duration TWO_DAYS = Duration.ofDays(2);

    @Test
    void contractKeepsAtLeastThreeCompleteTradingDaysLive() {
        assertThat(EodRetentionPolicy.MIN_COMPLETE_TRADING_DAYS).isEqualTo(3);
    }

    @Test
    void sourceExpiryBoundIsEndOfTradingDayPlusLiveTtl() {
        // 2026-08-14 trading day ends 2026-08-15T00:00 IST == 2026-08-14T18:30Z;
        // + 2d live TTL == 2026-08-16T18:30Z.
        Instant bound = EodRetentionPolicy.sourceExpiryBound(
                LocalDate.of(2026, 8, 14), KOLKATA, TWO_DAYS);
        assertThat(bound).isEqualTo(Instant.parse("2026-08-16T18:30:00Z"));
    }

    @Test
    void marginIsTimeRemainingUntilTheProtectedBound() {
        Instant bound = Instant.parse("2026-08-16T18:30:00Z");
        assertThat(EodRetentionPolicy.marginMs(Instant.parse("2026-08-13T12:00:00Z"), bound))
                .isEqualTo(Duration.ofDays(3).plusHours(6).plusMinutes(30).toMillis());
        assertThat(EodRetentionPolicy.marginMs(bound, bound)).isZero();
        assertThat(EodRetentionPolicy.marginMs(Instant.parse("2026-08-17T00:00:00Z"), bound))
                .isNegative();
    }

    @Test
    void extensionRequiredOnlyWhenMarginCollapsesBelowTheFloor() {
        assertThat(EodRetentionPolicy.requiresExtension(6_999L, 7_000L)).isTrue();
        assertThat(EodRetentionPolicy.requiresExtension(7_000L, 7_000L))
                .as("margin == floor does not yet require extension").isFalse();
        assertThat(EodRetentionPolicy.requiresExtension(7_001L, 7_000L)).isFalse();
    }

    @Test
    void extendedTtlIsBasePlusExtension() {
        Duration extended = EodRetentionPolicy.extendedTtl(TWO_DAYS, Duration.ofDays(30));
        assertThat(extended).isEqualTo(Duration.ofDays(32));
    }

    @Test
    void safetyGatesFailFastOnNull() {
        // P4-348/349/350: mis-wired callers get a named argument error at the
        // gate, not a bare NPE deep in java.time.
        assertThatThrownBy(() -> EodRetentionPolicy.sourceExpiryBound(null, KOLKATA, TWO_DAYS))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("tradingDate");
        assertThatThrownBy(() -> EodRetentionPolicy.sourceExpiryBound(
                LocalDate.of(2026, 8, 14), null, TWO_DAYS))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("zone");
        assertThatThrownBy(() -> EodRetentionPolicy.sourceExpiryBound(
                LocalDate.of(2026, 8, 14), KOLKATA, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("liveTtl");
        assertThatThrownBy(() -> EodRetentionPolicy.marginMs(null,
                Instant.parse("2026-08-16T18:30:00Z")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("now");
        assertThatThrownBy(() -> EodRetentionPolicy.marginMs(
                Instant.parse("2026-08-13T12:00:00Z"), null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("protectedExpiryBound");
        assertThatThrownBy(() -> EodRetentionPolicy.extendedTtl(null, Duration.ofDays(30)))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("baseLiveTtl");
        assertThatThrownBy(() -> EodRetentionPolicy.extendedTtl(TWO_DAYS, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("extension");
    }

    @Test
    void parseTtlHandlesFlussOptionUnits() {
        assertThat(EodRetentionPolicy.parseTtl("2d")).isEqualTo(Duration.ofDays(2));
        assertThat(EodRetentionPolicy.parseTtl("7d")).isEqualTo(Duration.ofDays(7));
        assertThat(EodRetentionPolicy.parseTtl("1h")).isEqualTo(Duration.ofHours(1));
        assertThat(EodRetentionPolicy.parseTtl("30m")).isEqualTo(Duration.ofMinutes(30));
        assertThat(EodRetentionPolicy.parseTtl("15s")).isEqualTo(Duration.ofSeconds(15));
        assertThat(EodRetentionPolicy.parseTtl("5000ms")).isEqualTo(Duration.ofMillis(5000));
    }

    @Test
    void parseTtlIsCaseInsensitiveButRejectsCompounds() {
        // P4-351: live metadata variants like "7D" parse; compound "1d12h"
        // stays unsupported (documented, rejected — never silently defaulted).
        assertThat(EodRetentionPolicy.parseTtl("7D")).isEqualTo(Duration.ofDays(7));
        assertThat(EodRetentionPolicy.parseTtl("1H")).isEqualTo(Duration.ofHours(1));
        assertThatThrownBy(() -> EodRetentionPolicy.parseTtl("1d12h"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseTtlRejectsBlankGarbageAndNonPositive() {
        assertThatThrownBy(() -> EodRetentionPolicy.parseTtl(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodRetentionPolicy.parseTtl("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodRetentionPolicy.parseTtl("bogus"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodRetentionPolicy.parseTtl("0d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodRetentionPolicy.parseTtl("-1h"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
