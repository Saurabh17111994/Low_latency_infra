package com.trading.compute.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class FeatureComputersTest {

    private static void close(RsiComputer rsi, long closePaise) {
        rsi.onClose(0L, 0L, 0L, 0L, 0L, closePaise, 0L, 0L);
    }

    private static void close(SmaComputer sma, long closePaise) {
        sma.onClose(0L, 0L, 0L, 0L, 0L, closePaise, 0L, 0L);
    }

    @Test
    void lastPriceTracksTicks() {
        LastPriceComputer last = new LastPriceComputer();
        assertTrue(Double.isNaN(last.value()));
        last.onTick(1L, 1_234L, 10L, 2L);
        assertEquals(1_234.0, last.value(), 1e-9);
        last.onTick(2L, 1_235L, 11L, 3L);
        assertEquals(1_235.0, last.value(), 1e-9);
    }

    @Test
    void smaIsNaNUntilFilledThenRolls() {
        SmaComputer sma = new SmaComputer(3);
        close(sma, 10L);
        assertTrue(Double.isNaN(sma.value()), "SMA before its period is not a value");
        close(sma, 20L);
        close(sma, 30L);
        assertEquals(20.0, sma.value(), 1e-9);
        close(sma, 40L);
        assertEquals(30.0, sma.value(), 1e-9);
    }

    @Test
    void rsiAllGainsIs100() {
        RsiComputer rsi = new RsiComputer(14);
        for (int i = 1; i <= 15; i++) {
            close(rsi, i * 100L);
        }
        assertEquals(100.0, rsi.value(), 1e-9);
    }

    @Test
    void rsiAllLossesIs0() {
        RsiComputer rsi = new RsiComputer(14);
        for (int i = 15; i >= 1; i--) {
            close(rsi, i * 100L);
        }
        assertEquals(0.0, rsi.value(), 1e-9);
    }

    @Test
    void rsiIsNaNUntilTheFirstPeriodOfChanges() {
        RsiComputer rsi = new RsiComputer(14);
        for (int i = 1; i <= 14; i++) {
            close(rsi, i * 100L);
        }
        assertTrue(Double.isNaN(rsi.value()), "14 closes are 13 changes — one short");
    }

    @Test
    void smaPreviewIsTheValueIfTheFormingCandleClosedNow() {
        SmaComputer sma = new SmaComputer(3);
        close(sma, 10L);
        close(sma, 20L);
        assertTrue(Double.isNaN(sma.value()));
        assertEquals(20.0, sma.previewOnForming(0L, 0L, 0L, 30L, 0L, 0L), 1e-9, "mean(10,20,30)");
        assertEquals(
                30.0,
                sma.previewOnForming(0L, 0L, 0L, 60L, 0L, 0L),
                1e-9,
                "the preview tracks the forming close");
        assertTrue(Double.isNaN(sma.value()), "the preview must not mutate the closed state");

        close(sma, 30L);
        assertEquals(20.0, sma.value(), 1e-9);
        assertEquals(
                110.0 / 3.0,
                sma.previewOnForming(0L, 0L, 0L, 60L, 0L, 0L),
                1e-9,
                "mean(20,30,60) — the oldest close rolls out");
        close(sma, 60L);
        assertEquals(110.0 / 3.0, sma.value(), 1e-9, "preview == the value after the same close");
    }

    @Test
    void rsiPreviewIsTheValueIfTheFormingCandleClosedNow() {
        RsiComputer rsi = new RsiComputer(14);
        long[] closes = {1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 8, 7, 9, 8};
        for (int i = 0; i < closes.length - 1; i++) {
            close(rsi, closes[i] * 100L);
        }
        assertTrue(Double.isNaN(rsi.value()), "14 closes are 13 changes — one short");

        double preview = rsi.previewOnForming(0L, 0L, 0L, closes[14] * 100L, 0L, 0L);
        assertFalse(Double.isNaN(preview), "the forming candle completes the period");
        assertTrue(Double.isNaN(rsi.value()), "the preview must not mutate the closed state");
        close(rsi, closes[14] * 100L);
        assertEquals(rsi.value(), preview, 1e-9, "preview == the value after the same close");
    }

    @Test
    void rsiMixedMatchesWilderSeed() {
        RsiComputer rsi = new RsiComputer(14);
        // 7 gains of +2 and 7 losses of -1: avgGain=1, avgLoss=0.5, rs=2
        long[] closes = {1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 8, 7, 9, 8};
        for (long c : closes) {
            close(rsi, c * 100L);
        }
        assertEquals(100.0 - 100.0 / 3.0, rsi.value(), 1e-9);
    }
}
