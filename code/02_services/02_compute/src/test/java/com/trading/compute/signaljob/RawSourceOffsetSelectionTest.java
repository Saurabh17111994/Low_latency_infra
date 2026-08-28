package com.trading.compute.signaljob;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Guard for the 2026-08-29 offset-selection fix: the raw source must replay
 * from offset 0 ONLY in FULL_REPLAY mode; RESTORE and LATEST modes must start
 * from LATEST so a restored/clean job skips the accumulated LOG backlog (which
 * previously replayed at ~124k/s and contaminated steady-state p99s).
 */
class RawSourceOffsetSelectionTest {

    @Test
    void fullReplayRequestsOffsetZero() {
        assertEquals(OffsetsInitializer.full().getClass(),
                SignalJob.rawSourceOffsets(SignalJobConfig.StartupMode.FULL_REPLAY).getClass());
    }

    @Test
    void latestModeRequestsLatest() {
        assertEquals(OffsetsInitializer.latest().getClass(),
                SignalJob.rawSourceOffsets(SignalJobConfig.StartupMode.LATEST).getClass());
    }

    @Test
    void restoreModeRequestsLatest() {
        assertEquals(OffsetsInitializer.latest().getClass(),
                SignalJob.rawSourceOffsets(SignalJobConfig.StartupMode.RESTORE).getClass());
    }
}
