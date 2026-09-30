package com.trading.common.schema.eod;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Retention margin/extension arithmetic for the EOD controller (SCH-23;
 * docs/08_implementation/02-schema-storage.md "EOD controller and offload
 * gate", docs/04_contracts/02-storage.md "Retention and lake").
 *
 * <p><b>3d retention + 1d runway (2026-09-30, Wave A/A2)</b>: the live DDL TTL
 * moves to 3d with the block-delete-unverified guard — source data for a
 * trading day cannot expire while its iceberg manifest is unverified;
 * unverified days extend (one table.log.ttl ALTER) and fire a critical alert
 * once their source-expiry margin falls below the runway
 * (EOD_SAFETY_FLOOR, default 1d; the earlier 7d TTL / 7d-floor pair assumed a
 * 9d table).
 *
 * <p>Load-bearing rules encoded here:
 *
 * <ul>
 *   <li>Source data for a trading day cannot expire while its manifest is
 *       unverified — unverified days always extend;</li>
 *   <li>verified days never force an extension (the 3-complete-trading-day
 *       floor is retired — Q36: live replay window &lt;= 24h, lake backfills);</li>
 *   <li>retention extension is automatic while the manifest is unverified,
 *       retryable, or under reconciliation.</li>
 * </ul>
 *
 * <p>Fluss 1.0.0 {@code table.log.ttl} is alterable and enforced (A1 probe
 * GREEN 2026-09-25: accepted + in force in ZK; A2 probe GREEN 2026-09-25:
 * enforced expiry follows the ALTER) — {@link #extendedTtl} computes the new
 * TTL for that single ALTER.
 */
public final class EodRetentionPolicy {

    private EodRetentionPolicy() {}

    /**
     * The instant a trading day's live data expires: records are written
     * throughout the day, so the last record expires at end-of-day (start of
     * the next day) plus the live {@code table.log.ttl}.
     */
    public static Instant sourceExpiryBound(LocalDate tradingDate, ZoneId zone,
            Duration liveTtl) {
        java.util.Objects.requireNonNull(tradingDate, "tradingDate must not be null");
        java.util.Objects.requireNonNull(zone, "zone must not be null");
        java.util.Objects.requireNonNull(liveTtl, "liveTtl must not be null");
        Instant dayEnd = tradingDate.plusDays(1).atStartOfDay(zone).toInstant();
        return dayEnd.plus(liveTtl);
    }

    /** Margin = time remaining until the protected source-expiry bound. */
    public static long marginMs(Instant now, Instant protectedExpiryBound) {
        java.util.Objects.requireNonNull(now, "now must not be null");
        java.util.Objects.requireNonNull(protectedExpiryBound, "protectedExpiryBound must not be null");
        return Duration.between(now, protectedExpiryBound).toMillis();
    }

    /**
     * Extension is required when the margin collapses below the safety floor.
     * T8 block-guard: this is the verified-guard check — when true, Fluss
     * delete is BLOCKED until the iceberg manifest is VERIFIED; caller must
     * extend and alert CRITICAL.
     */
    public static boolean requiresExtension(long marginMs, long safetyFloorMs) {
        return marginMs < safetyFloorMs;
    }

    /** New TTL for the retention ALTER: base live TTL + extension. */
    public static Duration extendedTtl(Duration baseLiveTtl, Duration extension) {
        java.util.Objects.requireNonNull(baseLiveTtl, "baseLiveTtl must not be null");
        java.util.Objects.requireNonNull(extension, "extension must not be null");
        return baseLiveTtl.plus(extension);
    }

    private static final Pattern TTL_PATTERN =
            Pattern.compile("(\\d+)(ms|s|m|h|d)", Pattern.CASE_INSENSITIVE);

    /**
     * Parse a Fluss TTL option value (e.g. {@code "2d"}, {@code "7d"},
     * {@code "1h"}, {@code "30m"}, {@code "15s"}, {@code "5000ms"}) into a
     * {@link Duration}. Used to read a live table's effective
     * {@code table.log.ttl} from cluster metadata (the EOD controller plans
     * against the table's ACTUAL create-time TTL, never an assumed one).
     * Matching is case-insensitive ({@code "7D"} parses); compound values
     * like {@code "1d12h"} are NOT supported and are rejected.
     *
     * @throws IllegalArgumentException when the value is blank, unparseable,
     *         or non-positive
     */
    public static Duration parseTtl(String ttl) {
        if (ttl == null) {
            throw new IllegalArgumentException("ttl must not be null");
        }
        String t = ttl.trim();
        if (t.isEmpty()) {
            throw new IllegalArgumentException("ttl must not be blank");
        }
        Matcher m = TTL_PATTERN.matcher(t);
        if (!m.matches()) {
            throw new IllegalArgumentException("unparseable ttl '" + ttl
                    + "' (expected e.g. 2d, 7d, 1h, 30m, 15s, 5000ms)");
        }
        long value = Long.parseLong(m.group(1));
        if (value <= 0) {
            throw new IllegalArgumentException("ttl must be positive, got '" + ttl + "'");
        }
        // P4-292: Duration.ofDays/ofHours/ofMinutes use multiplyExact — a
        // huge value that passes parseLong throws ArithmeticException, not
        // IAE, misclassifying a usage error as FATAL/EXIT=1 instead of EXIT=4.
        try {
            return switch (m.group(2).toLowerCase(java.util.Locale.ROOT)) {
                case "ms" -> Duration.ofMillis(value);
                case "s" -> Duration.ofSeconds(value);
                case "m" -> Duration.ofMinutes(value);
                case "h" -> Duration.ofHours(value);
                case "d" -> Duration.ofDays(value);
                default -> throw new IllegalStateException("unhandled ttl unit " + m.group(2));
            };
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("ttl out of range, got '" + ttl + "'", overflow);
        }
    }
}
