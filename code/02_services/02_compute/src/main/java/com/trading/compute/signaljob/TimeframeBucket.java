package com.trading.compute.signaljob;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Pure bucket math for NSE 09:15-15:30 IST (Asia/Kolkata, fixed +05:30, no DST).
 *
 * <p>Zero Flink imports — static helpers only. All params are epoch-millis
 * (UTC). Session alignment anchors to 09:15 IST of the event's own IST date
 * via java.time (ZoneId Asia/Kolkata is allowed per spec).
 *
 * <p>Half-open [start, start+window). Epoch-aligned: floor(t/window)*window.
 * Session-aligned: floor((t-openMs)/window)*window + openMs.
 * Filtering (pre/post session) happens BEFORE bucketing — helpers expose
 * isInSession / isPreOpen / sessionOpenMs / sessionCloseMs.
 */
public final class TimeframeBucket {

    public static final int SESSION_OPEN_IST_SECONDS = 9 * 3600 + 15 * 60;

    public static final int SESSION_CLOSE_IST_SECONDS = 15 * 3600 + 30 * 60;

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private static final LocalTime OPEN_TIME = LocalTime.of(9, 15);

    private static final LocalTime CLOSE_TIME = LocalTime.of(15, 30);

    private TimeframeBucket() {
        // static only
    }

    /**
     * Bucket start for the given timeframe and eventTime (epoch-millis).
     * Half-open [start, start+window).
     */
    public static long bucketStart(Timeframe tf, long eventTimeMs) {
        long window = tf.windowMs();
        if (!tf.isSessionAligned()) {
            return Math.floorDiv(eventTimeMs, window) * window;
        }
        long open = sessionOpenMs(eventTimeMs);
        long elapsed = eventTimeMs - open;
        return Math.floorDiv(elapsed, window) * window + open;
    }

    /**
     * Half-open session filter: 09:15:00.000 inclusive to 15:30:00.000 exclusive
     * in IST for the event's own IST date.
     */
    public static boolean isInSession(long eventTimeMs) {
        long open = sessionOpenMs(eventTimeMs);
        long close = sessionCloseMs(eventTimeMs);
        return eventTimeMs >= open && eventTimeMs < close;
    }

    /**
     * 09:15 IST boundary (inclusive) for the event's IST date.
     */
    public static long sessionOpenMs(long eventTimeMs) {
        ZonedDateTime zdt = Instant.ofEpochMilli(eventTimeMs).atZone(IST);
        LocalDate date = zdt.toLocalDate();
        return ZonedDateTime.of(date, OPEN_TIME, IST).toInstant().toEpochMilli();
    }

    /**
     * 15:30 IST boundary (exclusive) for the event's IST date.
     */
    public static long sessionCloseMs(long eventTimeMs) {
        ZonedDateTime zdt = Instant.ofEpochMilli(eventTimeMs).atZone(IST);
        LocalDate date = zdt.toLocalDate();
        return ZonedDateTime.of(date, CLOSE_TIME, IST).toInstant().toEpochMilli();
    }

    /**
     * Pre-open: t < sessionOpen of its IST date.
     */
    public static boolean isPreOpen(long t) {
        return t < sessionOpenMs(t);
    }

    /**
     * Gap threshold per timeframe for restart/gap handling (§E).
     * Defined as max(2 * window, 10_000 ms).
     */
    public static long gapThresholdMs(Timeframe tf) {
        return Math.max(2L * tf.windowMs(), 10_000L);
    }
}
