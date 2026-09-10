package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P2-044/045/046: the single defensive copy lives inside
 * {@link MultiTimeframeSignalContext} — the caller passes live refs.
 *
 * <p>Proves both sides: (1) mutating the original tray/accumulator/closed
 * objects after snapshot creation does not move the snapshot; (2) stored
 * refs are never {@code ==} the passed-in refs, so the copy is at the
 * boundary rather than relying on the caller to pre-copy.
 */
@DisplayName("SignalContext single-copy boundary — isolation + identity")
class MultiTimeframeSignalContextCopyTest {

    private static CandleAccumulator forming(long price, long volume) {
        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = acc.highPaise = acc.lowPaise = acc.closePaise = price;
        acc.volume = volume;
        acc.tickCount = 2L;
        acc.firstEventTime = 1_000L;
        acc.firstFingerprint = "fp-a";
        acc.lastEventTime = 2_000L;
        acc.lastFingerprint = "fp-b";
        acc.lastIngestTs = 9_000L;
        return acc;
    }

    private static ClosedCandle closed(long ws, long price) {
        return new ClosedCandle(ws, ws + 15_000L, price, price + 10, price - 10,
                price, 5L, 1L, ws + 1_000L, "fp-c");
    }

    private static List<MultiTimeframeSignalContext.TimeframeContext> frames(
            CandleAccumulator[] formings, List<ClosedCandle>[] closeds) {
        List<MultiTimeframeSignalContext.TimeframeContext> out = new ArrayList<>(6);
        Timeframe[] tfs = Timeframe.values();
        for (int i = 0; i < tfs.length; i++) {
            out.add(new MultiTimeframeSignalContext.TimeframeContext(tfs[i], formings[i], closeds[i]));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Object[] liveInputs() {
        CandleAccumulator[] formings = new CandleAccumulator[6];
        List<ClosedCandle>[] closeds = new List[6];
        for (int i = 0; i < 6; i++) {
            formings[i] = forming(100_00L + i, 10L + i);
            closeds[i] = new ArrayList<>(List.of(closed(1_000L * (i + 1), 99_00L + i)));
        }
        return new Object[] {formings, closeds};
    }

    @Test
    @DisplayName("mutating originals after snapshot does not move the snapshot")
    void mutateOriginalsAfterSnapshot() {
        Object[] in = liveInputs();
        CandleAccumulator[] formings = (CandleAccumulator[]) in[0];
        @SuppressWarnings("unchecked")
        List<ClosedCandle>[] closeds = (List<ClosedCandle>[]) in[1];

        MultiTimeframeSignalContext ctx = new MultiTimeframeSignalContext(
                2885L, "NSE", "TEST", 2_000L, frames(formings, closeds));

        // Mutate everything the caller still holds: tray list, frames, accumulators, candles.
        formings[0].closePaise = 888888L;
        formings[0].volume = 777777L;
        closeds[0].get(0).closePaise = 999999L;
        closeds[0].add(closed(555_000L, 111L));

        var f0 = ctx.frames().get(0);
        assertEquals(100_00L, f0.forming().closePaise);
        assertEquals(10L, f0.forming().volume);
        assertEquals(1, f0.closedNewestFirst().size());
        assertEquals(99_00L, f0.closedNewestFirst().get(0).closePaise);
    }

    @Test
    @DisplayName("stored refs are never the passed-in refs — copy is at the boundary")
    void copyLivesAtBoundary() {
        Object[] in = liveInputs();
        CandleAccumulator[] formings = (CandleAccumulator[]) in[0];
        @SuppressWarnings("unchecked")
        List<ClosedCandle>[] closeds = (List<ClosedCandle>[]) in[1];
        List<MultiTimeframeSignalContext.TimeframeContext> tray = frames(formings, closeds);

        MultiTimeframeSignalContext ctx = new MultiTimeframeSignalContext(
                2885L, "NSE", "TEST", 2_000L, tray);

        for (int i = 0; i < 6; i++) {
            var stored = ctx.frames().get(i);
            // Outer rebuild: never the caller's frame object.
            assertNotSame(tray.get(i), stored);
            // Inner copies: never the caller's accumulator or candle objects.
            assertNotSame(formings[i], stored.forming());
            assertNotSame(closeds[i].get(0), stored.closedNewestFirst().get(0));
            // Values still match — it is a copy, not a blank.
            assertEquals(formings[i].closePaise, stored.forming().closePaise);
            assertEquals(closeds[i].get(0).closePaise, stored.closedNewestFirst().get(0).closePaise);
        }
    }

    @Test
    @DisplayName("wrong-order / null frames fail before copying (P2-150)")
    void frameOrderValidated() {
        Object[] in = liveInputs();
        CandleAccumulator[] formings = (CandleAccumulator[]) in[0];
        @SuppressWarnings("unchecked")
        List<ClosedCandle>[] closeds = (List<ClosedCandle>[]) in[1];
        List<MultiTimeframeSignalContext.TimeframeContext> tray = frames(formings, closeds);

        // Swap two frames — ordinal 0 must be FIFTEEN_S.
        var tmp = tray.get(0);
        tray.set(0, tray.get(1));
        tray.set(1, tmp);
        assertThrows(IllegalArgumentException.class, () -> new MultiTimeframeSignalContext(
                2885L, "NSE", "TEST", 2_000L, tray));

        // Null element fails fast.
        List<MultiTimeframeSignalContext.TimeframeContext> withNull = frames(formings, closeds);
        withNull.set(3, null);
        assertThrows(NullPointerException.class, () -> new MultiTimeframeSignalContext(
                2885L, "NSE", "TEST", 2_000L, withNull));
    }
}
