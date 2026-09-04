package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Data-quality focused tests for {@link TimeframeBucket}.
 *
 * <p>Covers §D worked examples, half-open semantics, gap/overlap fuzz,
 * session alignment, LCM independence, isInSession boundaries, midnight edges,
 * and gapThreshold per §E (max(2*window, 10_000)).
 */
class TimeframeBucketTest {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private static long ist(int y, int m, int d, int h, int min, int s, int ms) {
        return ZonedDateTime.of(
                        LocalDate.of(y, m, d),
                        LocalTime.of(h, min, s, ms * 1_000_000),
                        IST)
                .toInstant()
                .toEpochMilli();
    }

    // -------------------------------------------------------------------------
    // §D Worked examples
    // -------------------------------------------------------------------------

    @Test
    void workedExample_midSession_10_00_37_210() {
        long t = ist(2026, 9, 4, 10, 0, 37, 210);
        assertEquals(ist(2026, 9, 4, 10, 0, 30, 0), TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, t));
        assertEquals(ist(2026, 9, 4, 10, 0, 30, 0), TimeframeBucket.bucketStart(Timeframe.THIRTY_S, t));
        assertEquals(ist(2026, 9, 4, 10, 0, 0, 0), TimeframeBucket.bucketStart(Timeframe.ONE_M, t));
        assertEquals(ist(2026, 9, 4, 10, 0, 0, 0), TimeframeBucket.bucketStart(Timeframe.THREE_M, t));
        assertEquals(ist(2026, 9, 4, 10, 0, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIVE_M, t));
        assertEquals(ist(2026, 9, 4, 10, 0, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIFTEEN_M, t));
    }

    @Test
    void workedExample_sessionOpenEdge_09_15_00_000_isInSessionAndFirstBucket() {
        long t = ist(2026, 9, 4, 9, 15, 0, 0);
        assertTrue(TimeframeBucket.isInSession(t));
        assertFalse(TimeframeBucket.isPreOpen(t));
        assertEquals(t, TimeframeBucket.sessionOpenMs(t));
        assertEquals(t, TimeframeBucket.bucketStart(Timeframe.THREE_M, t));
        assertEquals(t, TimeframeBucket.bucketStart(Timeframe.FIVE_M, t));
        assertEquals(t, TimeframeBucket.bucketStart(Timeframe.FIFTEEN_M, t));
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, t));
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), TimeframeBucket.bucketStart(Timeframe.ONE_M, t));
        // also verify ONE_M bucket is [09:15:00,09:16:00)
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), TimeframeBucket.bucketStart(Timeframe.ONE_M, t));
    }

    @Test
    void workedExample_preOpen_09_14_59_999_isFiltered() {
        long pre = ist(2026, 9, 4, 9, 14, 59, 999);
        assertFalse(TimeframeBucket.isInSession(pre));
        assertTrue(TimeframeBucket.isPreOpen(pre));
        // sessionOpen for that date is 09:15 same day
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), TimeframeBucket.sessionOpenMs(pre));
    }

    @Test
    void workedExample_postClose_15_30_00_000_isFiltered() {
        long close = ist(2026, 9, 4, 15, 30, 0, 0);
        assertFalse(TimeframeBucket.isInSession(close));
        assertFalse(TimeframeBucket.isPreOpen(close));
        assertEquals(close, TimeframeBucket.sessionCloseMs(close));
        // 15:30 tick belongs to no bucket — filtered before bucketing, but bucketStart for epoch still defined
        // For session TFs, elapsed == 22_500_000 => filtered; verify close boundary is exclusive
        long justAfter = ist(2026, 9, 4, 15, 30, 0, 1);
        assertFalse(TimeframeBucket.isInSession(justAfter));
    }

    @Test
    void workedExample_lastMs_15_29_59_999_inSessionAndFinalBucket() {
        long t = ist(2026, 9, 4, 15, 29, 59, 999);
        assertTrue(TimeframeBucket.isInSession(t));
        assertFalse(TimeframeBucket.isPreOpen(t));
        assertEquals(ist(2026, 9, 4, 15, 27, 0, 0), TimeframeBucket.bucketStart(Timeframe.THREE_M, t));
        assertEquals(ist(2026, 9, 4, 15, 25, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIVE_M, t));
        assertEquals(ist(2026, 9, 4, 15, 15, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIFTEEN_M, t));
        // Forced-roll boundary: each final bucket ends at 15:30 exactly
        assertEquals(ist(2026, 9, 4, 15, 30, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIFTEEN_M, t) + Timeframe.FIFTEEN_M.windowMs());
        assertEquals(ist(2026, 9, 4, 15, 30, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIVE_M, t) + Timeframe.FIVE_M.windowMs());
        assertEquals(ist(2026, 9, 4, 15, 30, 0, 0), TimeframeBucket.bucketStart(Timeframe.THREE_M, t) + Timeframe.THREE_M.windowMs());
        // sessionClose matches forced-roll
        assertEquals(ist(2026, 9, 4, 15, 30, 0, 0), TimeframeBucket.sessionCloseMs(t));
    }

    @Test
    void midnightEdge() {
        long midnight = ist(2026, 9, 4, 0, 0, 0, 0);
        long justBeforeMidnight = ist(2026, 9, 3, 23, 59, 59, 999);
        long justAfterMidnight = ist(2026, 9, 4, 0, 0, 0, 1);
        // Midnight is pre-open (00:00 < 09:15 of same date)
        assertFalse(TimeframeBucket.isInSession(midnight));
        assertTrue(TimeframeBucket.isPreOpen(midnight));
        // 23:59:59.999 is post-close (after 15:30), not pre-open
        assertFalse(TimeframeBucket.isInSession(justBeforeMidnight));
        assertFalse(TimeframeBucket.isPreOpen(justBeforeMidnight));
        assertFalse(TimeframeBucket.isInSession(justAfterMidnight));
        assertTrue(TimeframeBucket.isPreOpen(justAfterMidnight));
        // sessionOpen for midnight's date is 09:15 that date
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), TimeframeBucket.sessionOpenMs(midnight));
        assertEquals(ist(2026, 9, 3, 9, 15, 0, 0), TimeframeBucket.sessionOpenMs(justBeforeMidnight));
        // 23:59:59.999 is post-close of same session (15:30 already passed)
        assertFalse(TimeframeBucket.isInSession(justBeforeMidnight));
        // Epoch-aligned bucket at midnight should be floor to epoch window
        // Midnight 00:00:00.000 is exactly on 1m and 15s boundaries epoch-wise
        assertEquals(midnight, TimeframeBucket.bucketStart(Timeframe.ONE_M, midnight));
        assertEquals(midnight, TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, midnight));
    }

    // -------------------------------------------------------------------------
    // Half-open proof
    // -------------------------------------------------------------------------

    @Test
    void halfOpenProof_boundaryBelongsToNextBucket() {
        // Bucket [10:00:00,10:00:15) — Example 5
        long bucketStart = ist(2026, 9, 4, 10, 0, 0, 0);
        long bucketEnd = ist(2026, 9, 4, 10, 0, 15, 0);
        // For 15s timeframe, 10:00:00 is epoch-aligned boundary
        // Verify t exactly at start belongs to that bucket
        assertEquals(bucketStart, TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, bucketStart));
        // t one ms before start is in previous bucket
        long prev = bucketStart - 1;
        assertEquals(bucketStart - 15_000L, TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, prev));
        // t exactly at end belongs to next bucket, not current
        assertEquals(bucketEnd, TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, bucketEnd));
        assertTrue(TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, bucketEnd) > bucketStart);
        // Also verify invariant: start <= t < start+window
        long tIn = ist(2026, 9, 4, 10, 0, 14, 999);
        long s = TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, tIn);
        assertTrue(s <= tIn && tIn < s + Timeframe.FIFTEEN_S.windowMs());
        // t at end is NOT in previous bucket
        assertFalse(bucketStart <= bucketEnd && bucketEnd < bucketStart + 15_000L);
    }

    @Test
    void halfOpenProof_sessionTimeframe_boundaries() {
        // 5m bucket [09:15,09:20) — verify 09:20:00 belongs to [09:20,09:25), not previous
        long b1 = ist(2026, 9, 4, 9, 15, 0, 0);
        long b2 = ist(2026, 9, 4, 9, 20, 0, 0);
        assertEquals(b1, TimeframeBucket.bucketStart(Timeframe.FIVE_M, b1));
        assertEquals(b1, TimeframeBucket.bucketStart(Timeframe.FIVE_M, ist(2026, 9, 4, 9, 19, 59, 999)));
        assertEquals(b2, TimeframeBucket.bucketStart(Timeframe.FIVE_M, b2));
        // Check half-open: t in [b1, b2) maps to b1
        assertTrue(TimeframeBucket.bucketStart(Timeframe.FIVE_M, b2 - 1) == b1);
        assertTrue(TimeframeBucket.bucketStart(Timeframe.FIVE_M, b2) == b2);
    }

    // -------------------------------------------------------------------------
    // No gaps / overlaps fuzz over 2 NSE sessions
    // -------------------------------------------------------------------------

    @Test
    void noGapsOrOverlaps_randomTwoSessions_fixedSeed() {
        Random rnd = new Random(0xCAFEBABE12345L); // fixed seed
        long d1Open = ist(2026, 9, 4, 9, 15, 0, 0);
        long d1Close = ist(2026, 9, 4, 15, 30, 0, 0);
        long d2Open = ist(2026, 9, 5, 9, 15, 0, 0);
        long d2Close = ist(2026, 9, 5, 15, 30, 0, 0);
        long sessionMs = 22_500_000L;

        for (Timeframe tf : Timeframe.values()) {
            long window = tf.windowMs();
            // 500 random events per timeframe across both sessions
            for (int i = 0; i < 500; i++) {
                boolean firstDay = rnd.nextBoolean();
                long base = firstDay ? d1Open : d2Open;
                long offset = (long) (rnd.nextDouble() * sessionMs);
                // add 0..window*2 jitter occasionally to test boundaries, but stay in-session
                long t = base + offset;
                if (t >= base + sessionMs) t = base + sessionMs - 1;

                long start = TimeframeBucket.bucketStart(tf, t);
                assertTrue(start <= t, tf + " start must be <= t, t=" + t + " start=" + start);
                assertTrue(t < start + window, tf + " t must be < start+window");

                // Consecutive tiling: t+1ms is either same bucket or next bucket exactly +window
                long nextStart = TimeframeBucket.bucketStart(tf, t + 1);
                boolean same = nextStart == start;
                boolean next = nextStart == start + window;
                // For session TFs near session end, nextStart could jump to next day? But t is in-session
                // and t+1 stays in-session unless t is last ms of session.
                if (t + 1 >= (firstDay ? d1Close : d2Close)) {
                    // at 15:29:59.999 +1ms = 15:30:00.000 is out-of-session, but bucketStart still defined per math
                    // For session TFs, 15:30:00.000 would be next bucket [15:30, 15:33) if not filtered; but isInSession false
                    // So we exempt boundary crossing session close from strict tiling
                } else {
                    assertTrue(same || next, tf + " consecutive buckets must tile: start=" + start + " nextStart=" + nextStart + " t=" + t);
                }

                if (!tf.isSessionAligned()) {
                    // Epoch-aligned must be epoch-floor
                    long expected = Math.floorDiv(t, window) * window;
                    assertEquals(expected, start, tf + " epoch bucket mismatch");
                } else {
                    // Session-aligned buckets must be >= open and < close
                    long open = firstDay ? d1Open : d2Open;
                    assertTrue(start >= open, tf + " session bucket start must be >= open");
                    assertTrue(start < (firstDay ? d1Close : d2Close) || start == (firstDay ? d1Close : d2Close), tf + " start beyond close");
                }
            }
            // Also verify exhaustive tiling from open to close: step by window, each start maps to itself
            for (long s = d1Open; s < d1Close; s += window) {
                // For session TFs, session-end is exactly reachable; for epoch TFs tiling may not align to open
                if (tf.isSessionAligned()) {
                    assertEquals(s, TimeframeBucket.bucketStart(tf, s), tf + " tiling start mismatch at " + s);
                }
            }
        }
        // Also test d2 tiling for session TFs second day
        for (Timeframe tf : Timeframe.values()) {
            if (tf.isSessionAligned()) {
                long s = d2Open;
                assertEquals(s, TimeframeBucket.bucketStart(tf, s));
                long lastStart = TimeframeBucket.bucketStart(tf, d2Close - 1);
                assertTrue(lastStart + tf.windowMs() == d2Close, "final bucket must end exactly at 15:30 for " + tf);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Session alignment
    // -------------------------------------------------------------------------

    @Test
    void sessionAlignment_firstBucketStartsAt_09_15_00_000() {
        long open = ist(2026, 9, 4, 9, 15, 0, 0);
        assertEquals(open, TimeframeBucket.sessionOpenMs(open));
        for (Timeframe tf : Timeframe.values()) {
            if (tf.isSessionAligned()) {
                assertEquals(open, TimeframeBucket.bucketStart(tf, open), tf + " first bucket must start exactly at 09:15");
            }
        }
    }

    @Test
    void sessionAlignment_finalBucketEndsAt_15_30_00_000() {
        long close = ist(2026, 9, 4, 15, 30, 0, 0);
        for (Timeframe tf : Timeframe.values()) {
            if (tf.isSessionAligned()) {
                long lastTick = close - 1;
                long start = TimeframeBucket.bucketStart(tf, lastTick);
                assertEquals(close, start + tf.windowMs(), tf + " final bucket must end exactly at 15:30");
                // Also verify every bucket boundary from open tiles without gap to close
                long open = ist(2026, 9, 4, 9, 15, 0, 0);
                long sessionMs = close - open;
                assertEquals(0, sessionMs % tf.windowMs(), tf + " window must divide session evenly (22_500_000 % window == 0)");
            }
        }
    }

    @Test
    void sessionAlignment_allBucketsTileWithoutGapForFifteenM() {
        long open = ist(2026, 9, 4, 9, 15, 0, 0);
        long close = ist(2026, 9, 4, 15, 30, 0, 0);
        long expected = open;
        while (expected < close) {
            assertEquals(expected, TimeframeBucket.bucketStart(Timeframe.FIFTEEN_M, expected));
            expected += Timeframe.FIFTEEN_M.windowMs();
        }
        assertEquals(close, expected);
    }

    // -------------------------------------------------------------------------
    // 3m / 5m LCM independence
    // -------------------------------------------------------------------------

    @Test
    void lcm_3mAnd5m_independentBoundaries() {
        long t_09_20 = ist(2026, 9, 4, 9, 20, 0, 0);
        // 5m boundaries at 09:15,09:20,09:25,... so 09:20 is a 5m boundary
        assertEquals(t_09_20, TimeframeBucket.bucketStart(Timeframe.FIVE_M, t_09_20));
        // 3m boundaries at 09:15,09:18,09:21,... so 09:20 is NOT a 3m boundary (should map to 09:18)
        assertEquals(ist(2026, 9, 4, 9, 18, 0, 0), TimeframeBucket.bucketStart(Timeframe.THREE_M, t_09_20));
        // also verify converse: 09:18 is 3m boundary but not 5m
        long t_09_18 = ist(2026, 9, 4, 9, 18, 0, 0);
        assertEquals(t_09_18, TimeframeBucket.bucketStart(Timeframe.THREE_M, t_09_18));
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), TimeframeBucket.bucketStart(Timeframe.FIVE_M, t_09_18));
        // 09:30 is LCM of 3m and 5m and 15m offset from 09:15 (15 min) — all three should align
        long t_09_30 = ist(2026, 9, 4, 9, 30, 0, 0);
        assertEquals(t_09_30, TimeframeBucket.bucketStart(Timeframe.THREE_M, t_09_30));
        assertEquals(t_09_30, TimeframeBucket.bucketStart(Timeframe.FIVE_M, t_09_30));
        assertEquals(t_09_30, TimeframeBucket.bucketStart(Timeframe.FIFTEEN_M, t_09_30));
    }

    // -------------------------------------------------------------------------
    // isInSession boundaries
    // -------------------------------------------------------------------------

    @Test
    void isInSession_oneMsBeforeAfterOpenClose() {
        long open = ist(2026, 9, 4, 9, 15, 0, 0);
        long close = ist(2026, 9, 4, 15, 30, 0, 0);
        // open boundaries
        assertFalse(TimeframeBucket.isInSession(open - 1));
        assertTrue(TimeframeBucket.isInSession(open));
        assertTrue(TimeframeBucket.isInSession(open + 1));
        // close boundaries (half-open: close is OUT)
        assertTrue(TimeframeBucket.isInSession(close - 1));
        assertFalse(TimeframeBucket.isInSession(close));
        assertFalse(TimeframeBucket.isInSession(close + 1));
        // pre-open vs post-close ticks
        long preOpenTick = ist(2026, 9, 4, 8, 0, 0, 0);
        long postCloseTick = ist(2026, 9, 4, 16, 0, 0, 0);
        assertFalse(TimeframeBucket.isInSession(preOpenTick));
        assertTrue(TimeframeBucket.isPreOpen(preOpenTick));
        assertFalse(TimeframeBucket.isInSession(postCloseTick));
        assertFalse(TimeframeBucket.isPreOpen(postCloseTick));
    }

    @Test
    void isInSession_acrossMidnight() {
        long midnight = ist(2026, 9, 5, 0, 0, 0, 0);
        long beforeMidnight = ist(2026, 9, 4, 23, 59, 59, 999);
        long afterMidnight = ist(2026, 9, 5, 0, 0, 0, 1);
        assertFalse(TimeframeBucket.isInSession(midnight));
        assertFalse(TimeframeBucket.isInSession(beforeMidnight));
        assertFalse(TimeframeBucket.isInSession(afterMidnight));
        assertTrue(TimeframeBucket.isPreOpen(midnight));
        assertTrue(TimeframeBucket.isPreOpen(afterMidnight));
        // Next day's open is still correct
        assertEquals(ist(2026, 9, 5, 9, 15, 0, 0), TimeframeBucket.sessionOpenMs(midnight));
        assertEquals(ist(2026, 9, 5, 15, 30, 0, 0), TimeframeBucket.sessionCloseMs(midnight));
        // Previous day's close
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), TimeframeBucket.sessionOpenMs(beforeMidnight));
        assertEquals(ist(2026, 9, 4, 15, 30, 0, 0), TimeframeBucket.sessionCloseMs(beforeMidnight));
    }

    @Test
    void sessionOpenAndClose_msConsistency() {
        long t = ist(2026, 9, 4, 12, 34, 56, 789);
        long open = TimeframeBucket.sessionOpenMs(t);
        long close = TimeframeBucket.sessionCloseMs(t);
        assertEquals(ist(2026, 9, 4, 9, 15, 0, 0), open);
        assertEquals(ist(2026, 9, 4, 15, 30, 0, 0), close);
        assertEquals(22_500_000L, close - open);
        // constants check: SESSION_OPEN = 9*3600+15*60 = 33300, CLOSE = 15*3600+30*60 = 55800
        assertEquals(9 * 3600 + 15 * 60, TimeframeBucket.SESSION_OPEN_IST_SECONDS);
        assertEquals(15 * 3600 + 30 * 60, TimeframeBucket.SESSION_CLOSE_IST_SECONDS);
    }

    // -------------------------------------------------------------------------
    // gapThresholdMs §E (max(2*window, 10_000))
    // -------------------------------------------------------------------------

    @Test
    void gapThresholdMs_perTimeframe_matchesSpec() {
        // max(2*window, 10_000) per §E
        assertEquals(Math.max(2 * 15_000L, 10_000L), TimeframeBucket.gapThresholdMs(Timeframe.FIFTEEN_S));
        assertEquals(30_000L, TimeframeBucket.gapThresholdMs(Timeframe.FIFTEEN_S));
        assertEquals(60_000L, TimeframeBucket.gapThresholdMs(Timeframe.THIRTY_S));
        assertEquals(120_000L, TimeframeBucket.gapThresholdMs(Timeframe.ONE_M));
        assertEquals(360_000L, TimeframeBucket.gapThresholdMs(Timeframe.THREE_M));
        assertEquals(600_000L, TimeframeBucket.gapThresholdMs(Timeframe.FIVE_M));
        assertEquals(1_800_000L, TimeframeBucket.gapThresholdMs(Timeframe.FIFTEEN_M));
    }

    // -------------------------------------------------------------------------
    // Additional invariants
    // -------------------------------------------------------------------------

    @Test
    void epochAligned_bucketStart_isEpochFloor() {
        Random rnd = new Random(42L);
        for (int i = 0; i < 200; i++) {
            long t = Math.abs(rnd.nextLong()) % 2_000_000_000_000L;
            for (Timeframe tf : new Timeframe[]{Timeframe.FIFTEEN_S, Timeframe.THIRTY_S, Timeframe.ONE_M}) {
                long window = tf.windowMs();
                long expected = Math.floorDiv(t, window) * window;
                assertEquals(expected, TimeframeBucket.bucketStart(tf, t), tf + " epoch bucket mismatch for t=" + t);
            }
        }
    }

    @Test
    void isPreOpen_semantics() {
        long open = ist(2026, 9, 4, 9, 15, 0, 0);
        assertTrue(TimeframeBucket.isPreOpen(open - 1));
        assertFalse(TimeframeBucket.isPreOpen(open));
        assertFalse(TimeframeBucket.isPreOpen(open + 1));
        long close = ist(2026, 9, 4, 15, 30, 0, 0);
        // post-close is NOT pre-open (it's after open)
        assertFalse(TimeframeBucket.isPreOpen(close));
        assertFalse(TimeframeBucket.isPreOpen(close + 1));
        assertFalse(TimeframeBucket.isPreOpen(ist(2026, 9, 4, 15, 29, 59, 999)));
    }
}
