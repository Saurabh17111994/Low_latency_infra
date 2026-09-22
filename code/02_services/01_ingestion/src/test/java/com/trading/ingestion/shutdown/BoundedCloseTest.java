package com.trading.ingestion.shutdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

// Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
/**
 * The evidence writers' release path is bounded (2026-09-12).
 *
 * <p>Why this test exists: the three shutdown writers used to release with a
 * bare {@code flush()} and then close their Table/Connection — and every one of
 * those calls is unbounded in Fluss 0.9.1 ({@code flush()} waits on a bare
 * {@code latch.await()}; {@code FlussConnection.close()} passes
 * {@code Duration.ofMillis(Long.MAX_VALUE)}). On the shutdown-hook path a block
 * there means the JVM never exits. A live drill cannot prove the fix — a healthy
 * cluster never blocks, so both the bounded and unbounded versions behave
 * identically — so the bound is pinned here with a release that blocks forever.
 *
 * <p>Two legs:
 *
 * <ol>
 *   <li>behaviour: a blocking release returns within the budget, is counted as
 *       abandoned, and never throws; a fast release is not counted;</li>
 *   <li>wiring: each writer's {@code close()} actually delegates to
 *       {@link BoundedClose} — the helper being bounded is worthless if a
 *       writer reverts to a bare flush, which is exactly the regression this
 *       pins.</li>
 * </ol>
 */
@DisplayName("evidence writers: release is bounded, a blocking flush cannot hang shutdown")
class BoundedCloseTest {

    /** Small budget so the bounded property is provable in-test, not waited out. */
    private static final Duration BUDGET = Duration.ofMillis(300);

    /** Generous "did not hang" ceiling — the point is bounded, not fast. */
    private static final Duration TEST_BUDGET = Duration.ofSeconds(10);

    /**
     * The writers whose {@code close()} must delegate to {@link BoundedClose}.
     * Paths are relative to the ingestion module root.
     */
    private static final List<String> BOUNDED_CLOSE_WRITERS = List.of(
            "src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java",
            "src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java",
            "src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java",
            "src/main/java/com/trading/ingestion/TypedFlussRowConverter.java",
            "src/main/java/com/trading/ingestion/InstrumentManifestWriter.java");

    @BeforeEach
    void resetCounters() {
        BoundedClose.resetAbandonedCountForTest();
    }

    @Test
    @DisplayName("a release that blocks forever is abandoned within the budget, never hangs")
    void blockingReleaseIsAbandonedWithinBudget() {
        CountDownLatch neverReleased = new CountDownLatch(1);
        AtomicBoolean ran = new AtomicBoolean();

        assertTimeoutPreemptively(TEST_BUDGET, () -> {
            long start = System.nanoTime();
            boolean completed = BoundedClose.run("test-blocking", BUDGET, () -> {
                ran.set(true);
                try {
                    // The real hazard: flush() / Connection.close() waiting on a
                    // latch that an un-acking cluster never releases.
                    neverReleased.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertFalse(completed, "an overrunning release must report as abandoned");
            assertTrue(elapsedMs >= BUDGET.toMillis(),
                    "must actually wait the budget before abandoning (waited " + elapsedMs + " ms)");
            assertTrue(elapsedMs < 5_000,
                    "must return within the budget, not hang (took " + elapsedMs + " ms)");
        });

        assertTrue(ran.get(), "the release must have been attempted, not skipped");
        assertEquals(1, BoundedClose.abandonedCount(),
                "the overrun must be counted so it is visible, not silent");

        // Let the abandoned daemon thread finish so it does not accumulate.
        neverReleased.countDown();
    }

    @Test
    @DisplayName("a release that completes inside the budget is not counted as abandoned")
    void fastReleaseIsNotAbandoned() {
        AtomicBoolean ran = new AtomicBoolean();

        boolean completed = BoundedClose.run("test-fast", BUDGET, () -> ran.set(true));

        assertTrue(completed, "a release that finishes in time must report success");
        assertTrue(ran.get(), "the release must run");
        assertEquals(0, BoundedClose.abandonedCount(), "no overrun — nothing abandoned");
    }

    @Test
    @DisplayName("a throwing release is swallowed — the shutdown path must not abort")
    void throwingReleaseDoesNotPropagate() {
        boolean completed = BoundedClose.run("test-throwing", BUDGET,
                () -> {
                    throw new IllegalStateException("flush failed");
                });

        assertTrue(completed, "the thread finished, so the release completed (failed, but done)");
        assertEquals(0, BoundedClose.abandonedCount(), "a failure is not an overrun");
    }

    @Test
    @DisplayName("interrupting the waiter abandons the release and restores the interrupt flag")
    void interruptedWaiterAbandonsAndRestoresFlag() throws Exception {
        CountDownLatch neverReleased = new CountDownLatch(1);
        AtomicReference<Boolean> completed = new AtomicReference<>();

        Thread waiter = new Thread(() ->
                completed.set(BoundedClose.run("test-interrupt", Duration.ofSeconds(30),
                        () -> {
                            try {
                                neverReleased.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        })), "bounded-close-test-waiter");
        waiter.setDaemon(true);
        waiter.start();
        Thread.sleep(50); // let it reach the join
        waiter.interrupt();
        waiter.join(5_000);

        assertFalse(waiter.isAlive(), "an interrupted waiter must return promptly, not hang");
        assertEquals(Boolean.FALSE, completed.get(),
                "an interrupted wait must report the release as abandoned");
        assertEquals(1, BoundedClose.abandonedCount(), "the abandonment must be counted");

        neverReleased.countDown();
    }

    @Test
    @DisplayName("a malformed/silly budget override falls back instead of breaking shutdown")
    void budgetOverrideParsingIsFailSafe() {
        Duration fallback = Duration.ofSeconds(2);

        assertEquals(Duration.ofSeconds(5),
                BoundedClose.parseBudgetSeconds("5", fallback), "a plain number is seconds");
        assertEquals(Duration.ofSeconds(7),
                BoundedClose.parseBudgetSeconds(" 7 ", fallback),
                "whitespace is tolerated around the value");
        assertEquals(fallback, BoundedClose.parseBudgetSeconds(null, fallback), "absent");
        assertEquals(fallback, BoundedClose.parseBudgetSeconds("", fallback), "blank");
        assertEquals(fallback, BoundedClose.parseBudgetSeconds("abc", fallback), "not a number");
        assertEquals(fallback, BoundedClose.parseBudgetSeconds("0", fallback), "zero is not a budget");
        assertEquals(fallback, BoundedClose.parseBudgetSeconds("-3", fallback), "negative");
        // Never throws from the static initializer that reads the env var.
        assertEquals(fallback, BoundedClose.parseBudgetSeconds("1.5", fallback), "fractional");
    }

    @Test
    @DisplayName("wiring: every evidence writer's close() releases through BoundedClose")
    void writersDelegateToBoundedClose() throws Exception {
        Path moduleRoot = moduleRoot();
        for (String relative : BOUNDED_CLOSE_WRITERS) {
            Path file = moduleRoot.resolve(relative);
            assertTrue(Files.isRegularFile(file), "missing writer source " + file);
            String body = closeMethodBody(Files.readString(file, StandardCharsets.UTF_8));
            assertTrue(body.contains("BoundedClose.run("),
                    relative + ": close() does not release through BoundedClose — a bare "
                            + "flush()/connection close is unbounded in Fluss 1.0.0 (unchanged since 0.9.1) and would "
                            + "hang shutdown again:\n" + body);
        }
    }

    /**
     * Slice out {@code public void close()} by brace matching, so the assertion
     * cannot be satisfied by a {@code BoundedClose} call somewhere else in the
     * file.
     */
    private static String closeMethodBody(String source) {
        int start = source.indexOf("public void close()");
        assertTrue(start >= 0, "no close() method found in source");
        int open = source.indexOf('{', start);
        assertTrue(open > 0, "malformed close() — no opening brace");
        int depth = 0;
        for (int i = open; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return source.substring(open, i + 1);
                }
            }
        }
        throw new AssertionError("malformed close() — unbalanced braces");
    }

    /**
     * Surefire runs with the module basedir as cwd; also accept a repo-root
     * working directory (IDE runs). Fail loudly if neither is found — a scan of
     * nothing must never pass.
     */
    private static Path moduleRoot() {
        for (String candidate : List.of(".", "02_services/01_ingestion")) {
            Path root = Path.of(candidate).toAbsolutePath().normalize();
            if (Files.isRegularFile(root.resolve(BOUNDED_CLOSE_WRITERS.get(0)))) {
                return root;
            }
        }
        throw new AssertionError("cannot locate the ingestion module root (no "
                + BOUNDED_CLOSE_WRITERS.get(0) + " under cwd or cwd/02_services/01_ingestion)");
    }
}
