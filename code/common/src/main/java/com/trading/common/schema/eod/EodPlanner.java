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
 * <p>Protected bound = the source-expiry bound of the <b>earliest unverified</b>
 * day — source data for a trading day must not expire while its manifest is
 * unverified, retryable, or under reconciliation. When every day on file is
 * verified there is nothing to protect: no bound, no extension (the retired
 * 3-complete-trading-day floor, 2026-09-30 — operator Q36 re-scoped the live
 * replay window to &lt;= 24h with backfills from the lake; what stays live is
 * the table's own TTL).
 *
 * <p>If the margin between now and that bound is below the safety floor (the
 * extension runway; default 1d since 2026-09-30), the controller must extend
 * live retention with one {@code table.log.ttl} ALTER
 * ({@link EodRetentionPolicy#extendedTtl}). All logic is pure — no cluster,
 * no clock.
 */
public final class EodPlanner {

    private EodPlanner() {}

    /**
     * Output of one planning pass for one table.
     *
     * <p>{@code protectedExpiryBound} and {@code marginMs} describe the
     * earliest unverified day's remaining source life; when every day on file
     * is verified both are absent ({@code null} / {@link Long#MAX_VALUE}) —
     * verified days never force an extension (the retired 3-day floor).
     */
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
     * @param safetyFloor extension runway — minimum acceptable margin before
     *        an unverified day's extension fires (verified days never fire)
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

        if (earliestUnverified == null) {
            // All days verified: nothing to protect. The 3-complete-trading-day
            // floor is retired (2026-09-30, operator Q36: live replay window
            // <= 24h, backfills from the lake); extension is reserved for
            // unverified days.
            return new Plan(null, null, Long.MAX_VALUE, false);
        }
        Instant protectedBound = EodRetentionPolicy.sourceExpiryBound(
                earliestUnverified, zone, liveTtl);
        long margin = EodRetentionPolicy.marginMs(now, protectedBound);
        boolean requiresExtension =
                EodRetentionPolicy.requiresExtension(margin, safetyFloor.toMillis());
        return new Plan(earliestUnverified, protectedBound, margin, requiresExtension);
    }
}
