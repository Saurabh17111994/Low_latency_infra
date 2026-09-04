package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Single-read snapshot rule checks (perf trim options 1+2, 2026-09-05).
 *
 * <p>The breakout rule must give byte-identical answers whether it is
 * evaluated from a one-shot {@link SignalLookbackState.Snapshot} or from
 * repeated state reads: bullish (close &gt; open, strict), breakout
 * (close &gt; maxHigh, strict), trend (close*n &gt; sum, strict), and
 * never before {@code lookback} candles are buffered.
 */
@DisplayName("SignalLookbackState snapshot rule — identical answers, boundary strictness")
class SignalLookbackStateTest {

    private static SignalLookbackState.Snapshot snap(int count, long maxHigh, long sumCloses) {
        return new SignalLookbackState.Snapshot(count, maxHigh, sumCloses);
    }

    @Test
    void coldNeverFires() {
        assertFalse(SignalLookbackState.ruleHolds(100, 10_000, snap(2, 50, 100), 3));
        assertFalse(SignalLookbackState.ruleHolds(100, 10_000, snap(0, 0, 0), 3));
    }

    @Test
    void allThreeConditionsRequired() {
        // bullish fails (flat)
        assertFalse(SignalLookbackState.ruleHolds(128, 128, snap(3, 120, 330), 3));
        // breakout fails (close below max high)
        assertFalse(SignalLookbackState.ruleHolds(100, 119, snap(3, 120, 300), 3));
        // trend fails (close*n below sum)
        assertFalse(SignalLookbackState.ruleHolds(100, 121, snap(3, 120, 400), 3));
        // all hold
        assertTrue(SignalLookbackState.ruleHolds(115, 128, snap(3, 120, 330), 3));
    }

    @Test
    void strictBoundariesDoNotFire() {
        // close == maxHigh is NOT a breakout (strict >)
        assertFalse(SignalLookbackState.ruleHolds(100, 120, snap(3, 120, 300), 3));
        // close*n == sum is NOT a trend (strict >)
        assertFalse(SignalLookbackState.ruleHolds(100, 120, snap(3, 110, 360), 3));
        // close == open is NOT bullish (strict >)
        assertFalse(SignalLookbackState.ruleHolds(120, 120, snap(3, 110, 300), 3));
    }

    @Test
    void randomizedMatchesReferenceComputation() {
        Random rnd = new Random(20260905L);
        for (int i = 0; i < 500; i++) {
            int n = 1 + rnd.nextInt(6);
            long maxHigh = 0;
            long sum = 0;
            for (int k = 0; k < n; k++) {
                long c = 90 + rnd.nextInt(50);
                sum += c;
                maxHigh = Math.max(maxHigh, c + rnd.nextInt(10));
            }
            long open = 90 + rnd.nextInt(50);
            long close = 90 + rnd.nextInt(50);
            int lookback = 3;
            boolean expected = n >= lookback
                    && close > open
                    && close > maxHigh
                    && close * (long) n > sum;
            assertEquals(expected,
                    SignalLookbackState.ruleHolds(open, close, snap(n, maxHigh, sum), lookback),
                    "open=" + open + " close=" + close + " n=" + n
                            + " maxHigh=" + maxHigh + " sum=" + sum);
        }
    }
}
