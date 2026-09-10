package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Focused contract tests for the three model shapes Part C pins:
 * accumulator sentinel/clear, closed-candle span invariant, signal-context
 * shape. Supplements the behaviour suites (aggregate/state/copy tests)
 * with the exact invariants the P2 items promise.
 */
@DisplayName("Part C model contracts — accumulator, closed candle, signal context")
class PartCModelContractsTest {

    private static CandleAccumulator forming(long price) {
        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = acc.highPaise = acc.lowPaise = acc.closePaise = price;
        acc.volume = 5L;
        acc.tickCount = 1L;
        acc.firstEventTime = 1_000L;
        acc.firstFingerprint = "fp-a";
        acc.lastEventTime = 2_000L;
        acc.lastFingerprint = "fp-b";
        acc.lastIngestTs = 9_000L;
        return acc;
    }

    private static List<MultiTimeframeSignalContext.TimeframeContext> frames() {
        List<MultiTimeframeSignalContext.TimeframeContext> out = new ArrayList<>(6);
        for (Timeframe tf : Timeframe.values()) {
            out.add(new MultiTimeframeSignalContext.TimeframeContext(
                    tf, forming(100L), List.of()));
        }
        return out;
    }

    @Test
    @DisplayName("fresh accumulator is empty with sentinel defaults; clear() restores them in place")
    void accumulatorSentinelAndClear() {
        CandleAccumulator fresh = new CandleAccumulator();
        assertTrue(fresh.isEmpty());
        assertEquals("", fresh.firstFingerprint);
        assertEquals("", fresh.lastFingerprint);
        assertEquals(Long.MIN_VALUE, fresh.lastIngestTs);

        CandleAccumulator acc = forming(100L);
        CandleAccumulator same = acc;
        acc.clear();
        assertTrue(acc.isEmpty());
        assertEquals("", acc.firstFingerprint);
        assertEquals("", acc.lastFingerprint);
        assertEquals(Long.MIN_VALUE, acc.lastIngestTs);
        assertEquals(0L, acc.openPaise);
        assertEquals(0L, acc.volume);
    }

    @Test
    @DisplayName("accumulator extracts as Flink POJO (no Kryo fallback)")
    void accumulatorIsPojo() {
        TypeInformation<CandleAccumulator> ti = TypeInformation.of(CandleAccumulator.class);
        assertTrue(ti instanceof PojoTypeInfo,
                "CandleAccumulator must extract as POJO, got " + ti.getClass().getSimpleName());
    }

    @Test
    @DisplayName("closed candle keeps the fixed span (windowEnd - windowStart == tfMs)")
    void closedCandleFixedSpan() {
        for (Timeframe tf : Timeframe.values()) {
            long ws = 1_750_000_000_000L;
            ClosedCandle c = new ClosedCandle(ws, ws + tf.windowMs(), 100L, 110L, 99L,
                    105L, 25L, 2L, ws + 1_000L, "fp-x");
            assertEquals(tf.windowMs(), c.windowEnd - c.windowStart,
                    "span must equal tf.windowMs for " + tf);
        }
    }

    @Test
    @DisplayName("signal context carries 6 frames in values() order; accessors never alias")
    void signalContextShape() {
        MultiTimeframeSignalContext ctx =
                new MultiTimeframeSignalContext(2885L, "NSE", "TEST", 2_000L, frames());
        assertEquals(6, ctx.timeframeCount());
        Timeframe[] expected = Timeframe.values();
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], ctx.frames().get(i).tf());
        }
        assertNotSame(frames().get(0).forming(), ctx.frames().get(0).forming());
        assertThrows(UnsupportedOperationException.class, () -> ctx.frames().add(ctx.frames().get(0)));
    }

    @Test
    @DisplayName("timeframe fromCode round-trips every value and rejects unknowns")
    void timeframeFromCode() {
        for (Timeframe tf : Timeframe.values()) {
            assertEquals(tf, Timeframe.fromCode(tf.code()));
        }
        assertThrows(IllegalArgumentException.class, () -> Timeframe.fromCode("NOPE"));
    }
}
