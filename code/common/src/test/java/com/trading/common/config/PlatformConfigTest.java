package com.trading.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-127/R-199/R-263 — PlatformConfig hardening.
 */
@DisplayName("R-199: PlatformConfig guards and de-duplicates")
class PlatformConfigTest {

    @Test
    @DisplayName("non-positive container limit fails fast (R-199)")
    void nonPositiveLimitFails() {
        assertThrows(IllegalArgumentException.class,
                () -> PlatformConfig.maxPendingAppendBytes(0));
        assertThrows(IllegalArgumentException.class,
                () -> PlatformConfig.maxPendingAppendBytes(-1));
    }

    @Test
    @DisplayName("positive limit derives min(192MiB, 10%) — T2 3k tunable")
    void positiveLimitDerives() {
        // 512 MiB container → floor(536870912 * 0.10) = 53687091 < 192 MiB.
        assertEquals(53_687_091L, PlatformConfig.maxPendingAppendBytes(512L << 20));
        // 4 GiB container → floor > 192 MiB → capped at 192 MiB (201326592).
        assertEquals(201_326_592L, PlatformConfig.maxPendingAppendBytes(4L << 30));
        // 2 GiB container → floor 214748364 > 192 MiB → capped.
        assertEquals(201_326_592L, PlatformConfig.maxPendingAppendBytes(2L << 30));
    }

    @Test
    @DisplayName("BROKER_MAX delegates to FixedScope (R-263)")
    void maxTicksSingleSource() {
        assertEquals(com.trading.common.config.FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC,
                PlatformConfig.BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC);
        assertEquals(20, PlatformConfig.BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC);
    }
}
