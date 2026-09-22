package com.trading.common.schema.eod;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;

// Version note (2026-09-23): the 0.9.1 references in this file record the pre-1.0.0 baseline this code was written against, not a constraint of the running Fluss 1.0.0 — re-check them (DEC-052).
/**
 * EOD planner (SCH-23): computes, per table, the earliest unverified trading
 * date, the protected source-expiry bound, and the retention-extension
 * decision — the "tested control mechanism" the storage contract demands
 * ("Unverified or retryable state extends retention through a tested control
 * mechanism; a fixed DDL TTL comment is insufficient").
 *
 * <p>Protected bound = the later of:
 *
 * <ol>
 *   <li>the source-expiry bound of the <b>earliest unverified</b> day —
 *       source data for a trading day must not expire while its manifest is
 *       unverified, retryable, or under reconciliation;</li>
 *   <li>the source-expiry bound of the <b>third-most-recent</b> trading day —
 *       at least three complete trading days remain live even after
 *       successful offload (the 3-day floor; with fewer than three days on
 *       file the floor is the oldest day).</li>
 * </ol>
 *
 * <p>If the margin between now and that bound is below the safety floor, the
 * controller must extend live retention (in Fluss 0.9.1: a controlled table
 * rewrite with {@link EodRetentionPolicy#extendedTtl}). All logic is pure —
 * no cluster, no clock.
 */
public final class EodPlanner {

    private EodPlanner() {}

    /** Output of one planning pass for one table. */
    public record Plan(LocalDate earliestUnverifiedDate, Instant protectedExpiryBound,
                       long marginMs, boolean requiresExtension) {
        /** True when every trading day on file is VERIFIED. */
        public boolean allVerified() {
            return earliestUnverifiedDate == null;
        }
    }

    /**
     * Plan retention for a table's per-day records.
     *
     * @param days per-day offload records (any order; sorted by trading date here)
     * @param zone trading-day timezone (e.g. Asia/Kolkata)
     * @param liveTtl the table's effective live {@code table.log.ttl}
     * @param safetyFloor minimum acceptable margin before extension fires
     * @param now the planner clock
     * @throws IllegalArgumentException when {@code days} is empty
     */
    public static Plan plan(List<EodOffloadRecord> days, ZoneId zone, Duration liveTtl,
            Duration safetyFloor, Instant now) {
        if (days == null || days.isEmpty()) {
            throw new IllegalArgumentException("no EOD days to plan for a table");
        }
        List<EodOffloadRecord> sorted = days.stream()
                .sorted(Comparator.comparing(EodOffloadRecord::tradingDate))
                .toList();

        LocalDate earliestUnverified = sorted.stream()
                .filter(day -> !day.permitsSourceExpiry())
                .map(EodOffloadRecord::tradingDateAsLocalDate)
                .min(Comparator.naturalOrder())
                .orElse(null);

        // 3-day floor: the third-most-recent trading day (the oldest when the
        // table has fewer than three days on file) must remain live.
        LocalDate floorDate = sorted.get(Math.max(0, sorted.size()
                - EodRetentionPolicy.MIN_COMPLETE_TRADING_DAYS)).tradingDateAsLocalDate();
        Instant floorBound = EodRetentionPolicy.sourceExpiryBound(floorDate, zone, liveTtl);

        Instant unverifiedBound = earliestUnverified == null ? null
                : EodRetentionPolicy.sourceExpiryBound(earliestUnverified, zone, liveTtl);
        Instant protectedBound = unverifiedBound != null && unverifiedBound.isAfter(floorBound)
                ? unverifiedBound : floorBound;

        long margin = EodRetentionPolicy.marginMs(now, protectedBound);
        boolean requiresExtension =
                EodRetentionPolicy.requiresExtension(margin, safetyFloor.toMillis());
        return new Plan(earliestUnverified, protectedBound, margin, requiresExtension);
    }
}
