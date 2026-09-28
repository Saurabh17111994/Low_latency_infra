package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P1: the ZERO-ACK watchdog must distinguish a slow COLD START from a wedged
 * sender.
 *
 * <p>Observed live: a healthy cold start was killed ~1s after its first accepted
 * tick with "accepted ticks flowing for 82ms but no Fluss append ack". Cause: with
 * no append ever acked, the age was {@code Long.MAX_VALUE}, so the guard was already
 * overdue on its next 1s tick - the first append never got its full window (it was
 * still creating the day partition).
 *
 * <p>The watchdog is an inline lambda inside a large constructor, so the decision is
 * tested through {@link IngestionService#zeroAckAgeMs} - same shape as
 * {@code classifyFreshness} in {@code StaleDataTradeGuardTest}.
 */
@DisplayName("ZERO-ACK watchdog: cold-start grace vs genuine wedge")
class ZeroAckWatchdogColdStartTest {

    /** INGESTION_ZERO_ACK_TIMEOUT_MS default. */
    private static final long TIMEOUT = 10_000L;

    @Test
    @DisplayName("never acked: age is measured from the FIRST accepted tick, not infinity")
    void coldStartGetsTheWholeWindow() {
        long now = 1_000_000L;
        long age = IngestionService.zeroAckAgeMs(now, 0L, now - 3_000L);
        assertEquals(3_000L, age, "cold start must measure from the first accepted tick");
        assertTrue(age < TIMEOUT,
                "3s into a cold start must not be overdue - Long.MAX_VALUE fired here (the live bug)");
    }

    @Test
    @DisplayName("never acked, window genuinely elapsed: still trips, guard keeps its purpose")
    void neverAckedStillTripsAfterTheWindow() {
        long now = 1_000_000L;
        long age = IngestionService.zeroAckAgeMs(now, 0L, now - TIMEOUT - 1);
        assertTrue(age >= TIMEOUT, "a sender that never acks must still fail fast");
    }

    @Test
    @DisplayName("once an ack exists the baseline is that ack - wedge detection unchanged")
    void ackBaselineIsUnchanged() {
        long now = 1_000_000L;
        assertEquals(2_000L, IngestionService.zeroAckAgeMs(now, now - 2_000L, now - 500_000L),
                "an old firstAccepted must not matter once an ack exists");
        assertEquals(TIMEOUT, IngestionService.zeroAckAgeMs(now, now - TIMEOUT, now - 500_000L),
                "wedge threshold measured from the last ack, exactly as before");
    }

    @Test
    @DisplayName("nothing accepted yet: MAX_VALUE, so a never-started writer cannot false-trip")
    void noAcceptedTickYetIsNeverOverdue() {
        assertEquals(Long.MAX_VALUE, IngestionService.zeroAckAgeMs(1_000_000L, 0L, 0L));
    }

    @Test
    @DisplayName("cold start that never acks: the window is the full zeroAckTimeoutMs, not 1s")
    void coldStartWindowIsTheConfiguredTimeout() {
        long firstAccepted = 1_000_000L;
        long justUnder = IngestionService.zeroAckAgeMs(firstAccepted + TIMEOUT - 1, 0L, firstAccepted);
        long exactly = IngestionService.zeroAckAgeMs(firstAccepted + TIMEOUT, 0L, firstAccepted);
        assertTrue(justUnder < TIMEOUT, "the first append gets almost the whole window");
        assertTrue(exactly >= TIMEOUT, "and is caught at the window boundary");
    }

    @Test
    @DisplayName("CHG-326: the write startup grace suppresses the watchdog while no ack exists")
    void writeGraceSuppressesColdStart() {
        assertTrue(IngestionService.zeroAckSuppressed(30_000L),
                "inside the grace with no ack ever, a no-ack window is not a wedge");
    }

    @Test
    @DisplayName("CHG-356: an early ack must not re-arm the watchdog inside the startup grace")
    void earlyAckDoesNotShrinkTheGrace() {
        // 2026-09-28 cold start (reproduced in the container log): the first
        // append was acked while the Fluss tablet was still replaying; the old
        // rule treated that single ack as "startup over", the next 10.9s ack
        // gap killed a healthy container, and on-failure:3 exhausted into a
        // crash loop. The whole bounded window is the grace.
        assertTrue(IngestionService.zeroAckSuppressed(30_000L),
                "one early ack must not turn the rest of the startup window into a wedge");
    }

    @Test
    @DisplayName("CHG-326: once the grace expires the watchdog keeps its full authority")
    void expiredGraceDoesNotSuppress() {
        assertFalse(IngestionService.zeroAckSuppressed(0L),
                "the grace is bounded: expiry restores fail-fast wedge detection");
    }
}
