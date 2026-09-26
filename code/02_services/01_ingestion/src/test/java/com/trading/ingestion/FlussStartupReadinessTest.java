package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import javax.naming.AuthenticationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * CHG-322: bounded, leader-aware Fluss readiness wait for the ingestion startup
 * path. Failing-first red under the CHG-322 evidence: these tests pin
 *
 * <ul>
 *   <li>retryable-only retries with capped exponential backoff,</li>
 *   <li>fail-fast on fatal failures and fail-closed on unrecognized ones,</li>
 *   <li>a missing table is not a readiness problem (schema step decides),</li>
 *   <li>budget exhaustion fails closed with the last error in the message,</li>
 *   <li>zero budget = one attempt, no sleep (rollback semantics).</li>
 * </ul>
 *
 * <p>Uses an injected clock and sleeper so the tests never wait in real time.
 */
@DisplayName("CHG-322: bounded Fluss startup readiness wait")
class FlussStartupReadinessTest {

    /** Class-name shape the classifier recognizes as table-absent (FATAL today). */
    static final class FakeTableNotExistException extends RuntimeException {}

    /** Deterministic clock + sleep recorder + log collector. */
    private static final class Harness {
        final long[] now = {0};
        final List<Long> sleeps = new ArrayList<>();
        final List<String> logs = new ArrayList<>();
        final LongSupplier clock = () -> now[0];
        final FlussStartupReadiness.Sleeper sleeper = ms -> {
            sleeps.add(ms);
            now[0] += ms;
        };
        final Consumer<String> log = logs::add;
    }

    @Test
    @DisplayName("ready on the first probe: no sleep, no retry")
    void readyImmediately() throws Exception {
        Harness h = new Harness();
        int[] calls = {0};
        FlussStartupReadiness.Probe probe = () -> calls[0]++;
        FlussStartupReadiness.awaitWith(probe, 60_000, h.clock, h.sleeper, h.log);
        assertEquals(1, calls[0]);
        assertTrue(h.sleeps.isEmpty());
    }

    @Test
    @DisplayName("retryable failures retry with exponential backoff, then succeed")
    void retriesThenSucceeds() throws Exception {
        Harness h = new Harness();
        int[] calls = {0};
        FlussStartupReadiness.Probe probe = () -> {
            if (++calls[0] <= 2) {
                throw new RuntimeException("Leader not available for bucket 0 of raw_table_1");
            }
        };
        FlussStartupReadiness.awaitWith(probe, 60_000, h.clock, h.sleeper, h.log);
        assertEquals(3, calls[0]);
        assertEquals(List.of(250L, 500L), h.sleeps);
    }

    @Test
    @DisplayName("budget exhaustion fails closed with the last error in the message")
    void timesOutWithLastError() {
        Harness h = new Harness();
        int[] calls = {0};
        FlussStartupReadiness.Probe probe = () -> {
            calls[0]++;
            throw new RuntimeException("Leader not available for bucket 0");
        };
        FlussStartupReadiness.StartupWaitTimeoutException e = assertThrows(
                FlussStartupReadiness.StartupWaitTimeoutException.class,
                () -> FlussStartupReadiness.awaitWith(probe, 1_000, h.clock, h.sleeper, h.log));
        assertTrue(e.getMessage().contains("1000"), "message names the budget: " + e.getMessage());
        assertTrue(e.getMessage().contains("Leader not available"),
                "message carries the last error: " + e.getMessage());
        assertTrue(h.sleeps.stream().mapToLong(Long::longValue).sum() <= 1_000,
                "never sleeps past the budget: " + h.sleeps);
        assertTrue(calls[0] >= 3, "expected several attempts, got " + calls[0]);
    }

    @Test
    @DisplayName("zero budget = one attempt, no sleep, fail closed on retryable")
    void zeroBudgetDoesNotRetry() {
        Harness h = new Harness();
        int[] calls = {0};
        FlussStartupReadiness.Probe probe = () -> {
            calls[0]++;
            throw new RuntimeException("connection refused");
        };
        assertThrows(FlussStartupReadiness.StartupWaitTimeoutException.class,
                () -> FlussStartupReadiness.awaitWith(probe, 0, h.clock, h.sleeper, h.log));
        assertEquals(1, calls[0]);
        assertTrue(h.sleeps.isEmpty());
    }

    @Test
    @DisplayName("fatal failures fail fast with the original exception (no retry)")
    void fatalFailsFast() {
        Harness h = new Harness();
        AuthenticationException fatal = new AuthenticationException("bad credentials");
        FlussStartupReadiness.Probe probe = () -> {
            throw fatal;
        };
        AuthenticationException thrown = assertThrows(AuthenticationException.class,
                () -> FlussStartupReadiness.awaitWith(probe, 60_000, h.clock, h.sleeper, h.log));
        assertSame(fatal, thrown);
        assertTrue(h.sleeps.isEmpty());
    }

    @Test
    @DisplayName("table missing is not a readiness problem: wait ends, schema step decides")
    void tableMissingProceeds() throws Exception {
        Harness h = new Harness();
        int[] calls = {0};
        FlussStartupReadiness.Probe probe = () -> {
            calls[0]++;
            throw new FakeTableNotExistException();
        };
        FlussStartupReadiness.awaitWith(probe, 60_000, h.clock, h.sleeper, h.log);
        assertEquals(1, calls[0]);
        assertTrue(h.sleeps.isEmpty());
    }

    @Test
    @DisplayName("unknown failures fail closed per R-285 (no retry)")
    void unknownFailsClosed() {
        Harness h = new Harness();
        IllegalStateException unknown = new IllegalStateException("something new");
        FlussStartupReadiness.Probe probe = () -> {
            throw unknown;
        };
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> FlussStartupReadiness.awaitWith(probe, 60_000, h.clock, h.sleeper, h.log));
        assertSame(unknown, thrown);
        assertTrue(h.sleeps.isEmpty());
    }

    @Test
    @DisplayName("failure action mapping: RETRY / PROCEED / FAIL")
    void failureActionMapping() {
        assertEquals(FlussStartupReadiness.Action.RETRY,
                FlussStartupReadiness.actionFor(new RuntimeException("leader not available")));
        assertEquals(FlussStartupReadiness.Action.RETRY,
                FlussStartupReadiness.actionFor(new java.net.ConnectException("connection refused")));
        assertEquals(FlussStartupReadiness.Action.PROCEED,
                FlussStartupReadiness.actionFor(new FakeTableNotExistException()));
        assertEquals(FlussStartupReadiness.Action.FAIL,
                FlussStartupReadiness.actionFor(new AuthenticationException("denied")));
        assertEquals(FlussStartupReadiness.Action.FAIL,
                FlussStartupReadiness.actionFor(new IllegalStateException("unclassified")));
    }

    @Test
    @DisplayName("backoff is capped at 5 s")
    void backoffIsCapped() {
        assertEquals(250L, FlussStartupReadiness.backoffMs(1));
        assertEquals(500L, FlussStartupReadiness.backoffMs(2));
        assertEquals(4_000L, FlussStartupReadiness.backoffMs(5));
        assertEquals(5_000L, FlussStartupReadiness.backoffMs(6));
        assertEquals(5_000L, FlussStartupReadiness.backoffMs(50));
    }
}
