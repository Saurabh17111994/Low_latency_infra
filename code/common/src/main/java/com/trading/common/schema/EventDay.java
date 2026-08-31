package com.trading.common.schema;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * EventDay (2026-08-31, daily-partition migration): maps an event timestamp
 * to the raw-table partition value — yyyyMMdd in Asia/Kolkata (trading day).
 * MUST match Fluss auto-partition DAY naming exactly (yyyyMMdd), or rows
 * land in a second, non-auto partition (smoke GUARD E catches this).
 */
public final class EventDay {

    private EventDay() {}

    /** Trading-day zone: Asia/Kolkata (market day boundary = IST midnight). */
    public static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");

    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZONE);

    public static String of(Instant eventTime) {
        return FORMAT.format(eventTime);
    }

    public static String of(long eventTimeEpochMilli) {
        return FORMAT.format(Instant.ofEpochMilli(eventTimeEpochMilli));
    }
}
