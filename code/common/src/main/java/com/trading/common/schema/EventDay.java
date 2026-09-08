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

    /**
     * Fail-fast guard for the raw-table partition key (P4-014): the DDL's
     * {@code event_day} is a plain STRING (Fluss has no CHECK/REGEXP), so a
     * wrongly formatted value silently lands rows in a second, non-auto
     * partition. Both row converters call this on the freshly derived value —
     * a derivation bug fails at the bridge instead of corrupting the table.
     */
    public static String validate(String eventDay) {
        if (eventDay == null || eventDay.length() != 8) {
            throw new IllegalArgumentException(
                    "event_day must be yyyyMMdd, got '" + eventDay + "'");
        }
        for (int i = 0; i < 8; i++) {
            char c = eventDay.charAt(i);
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException(
                        "event_day must be yyyyMMdd, got '" + eventDay + "'");
            }
        }
        return eventDay;
    }

    /** Derive + validate in one call (the converter path). */
    public static String ofValidated(Instant eventTime) {
        return validate(of(eventTime));
    }

    /** Derive + validate in one call (the converter path). */
    public static String ofValidated(long eventTimeEpochMilli) {
        return validate(of(eventTimeEpochMilli));
    }
}
