package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P3-086/P3-091 (D4): a {@code DEFERRED} handoff used to be dropped on the first attempt and
 * recovered only by replay-from-zero after a restart, so a valid, money-moving intent could sit
 * unexecuted indefinitely as long as the gateway stayed up. It is now retried in-run within a
 * bounded budget.
 *
 * <p>These exercise the retry policy directly (it is package-private for exactly this reason) —
 * no Fluss connection, so the coverage is cheap enough to always run.
 */
class IntentReaderDeferRetryTest {

    private static final long NO_DELAY = 0L;
    private static final int BUDGET = 3;

    private static IntentRecord intent() {
        return new IntentRecord("i-1", "c-1", "t-1", "acct-1", "part-1", 1L, "NSE", "RELIANCE",
                "BUY", 10L, "LIMIT", 100L, "MIS", "DAY", "strat-1", "v1", "cfg-1", 1L, null,
                "hash-1", null, "3", 0L);
    }

    @Test
    @DisplayName("a defer that clears within the budget is forwarded in-run, no restart needed")
    void deferThenForwardIsRecoveredInRun() throws Exception {
        // Defers twice, then succeeds. The old "drop on first defer" behaviour returns DEFERRED
        // after a single attempt, so this assertion is what detects a regression to it.
        AtomicInteger attempts = new AtomicInteger();
        IntentSink sink = i -> switch (attempts.incrementAndGet()) {
            case 1, 2 -> IntentSink.Result.DEFERRED;
            default -> IntentSink.Result.FORWARDED;
        };

        IntentSink.Result result = IntentReader.retryWhileDeferred(sink, intent(), BUDGET, NO_DELAY);

        assertThat(result).isEqualTo(IntentSink.Result.FORWARDED);
        assertThat(attempts.get()).as("must retry in-run, not defer to a restart").isEqualTo(3);
    }

    @Test
    @DisplayName("a permanently deferred intent stops at the budget instead of looping forever")
    void permanentlyDeferredStopsAtBudget() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        IntentSink sink = i -> {
            attempts.incrementAndGet();
            return IntentSink.Result.DEFERRED;
        };

        IntentSink.Result result = IntentReader.retryWhileDeferred(sink, intent(), BUDGET, NO_DELAY);

        assertThat(result).isEqualTo(IntentSink.Result.DEFERRED);
        assertThat(attempts.get())
                .as("budget is exact — exceeding it would stall the single-writer loop")
                .isEqualTo(BUDGET);
    }

    @Test
    @DisplayName("a rejecting sink is returned immediately — rejection is not retried")
    void rejectionIsNotRetried() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        IntentSink sink = i -> {
            attempts.incrementAndGet();
            return IntentSink.Result.REJECTED;
        };

        assertThat(IntentReader.retryWhileDeferred(sink, intent(), BUDGET, NO_DELAY))
                .isEqualTo(IntentSink.Result.REJECTED);
        assertThat(attempts.get())
                .as("REJECTED is a decision, not a transient condition")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a throwing sink is retried, then propagates once the budget is spent")
    void throwingSinkIsRetriedThenPropagates() {
        AtomicInteger attempts = new AtomicInteger();
        IntentSink alwaysThrows = i -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("broker down");
        };

        assertThatThrownBy(() -> IntentReader.retryWhileDeferred(alwaysThrows, intent(), BUDGET, NO_DELAY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broker down");
        assertThat(attempts.get()).isEqualTo(BUDGET);
    }

    @Test
    @DisplayName("a sink that throws then recovers is forwarded, not failed")
    void throwingThenRecoveringIsForwarded() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        IntentSink flaky = i -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("transient");
            }
            return IntentSink.Result.FORWARDED;
        };

        assertThat(IntentReader.retryWhileDeferred(flaky, intent(), BUDGET, NO_DELAY))
                .isEqualTo(IntentSink.Result.FORWARDED);
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("an interrupt during the retry wait stops the loop instead of being swallowed")
    void interruptDuringRetryPropagates() {
        IntentSink sink = i -> IntentSink.Result.DEFERRED;
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> IntentReader.retryWhileDeferred(sink, intent(), BUDGET, 5_000L))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("interrupted");
        } finally {
            // Clear the flag so it cannot leak into another test.
            Thread.interrupted();
        }
    }
}
