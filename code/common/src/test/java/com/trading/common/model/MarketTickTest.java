package com.trading.common.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-128/R-163 — MarketTick validity-state semantics and array safety.
 */
@DisplayName("R-128: MarketTick validity + rawPayload safety")
class MarketTickTest {

    private static MarketTick tick(String validityState, byte[] payload) {
        return new MarketTick(
                "fp", "v1", "c1", 1L, 1000L, "NSE", "SYM", "EQ",
                null, null, null,
                1000L, 1001L, 1002L, "TRADE",
                100L, 1L, 101L, 1L, 102L, 1L,
                payload, "hash", "go-arrow-sdk", "v1",
                validityState, null, "1");
    }

    @Test
    @DisplayName("validity_state carries the enum name, not the literal VALID (R-128)")
    void validitySemantics() {
        assertTrue(tick("VALID_TRADE", null).isValid());
        assertTrue(tick("VALID_NON_TRADE", null).isValid());
        assertTrue(tick("VALID_TRADE", null).isValidTrade());
        assertFalse(tick("INVALID_VALUES", null).isValid());
        assertFalse(tick(null, null).isValid());
    }

    @Test
    @DisplayName("future VALID-prefixed states fail closed (P2-097)")
    void futureValidPrefixedStatesRejected() {
        assertFalse(tick("VALID_QUARANTINED", null).isValid());
        assertFalse(tick("VALIDATION_PENDING", null).isValid());
        assertFalse(tick("VALID", null).isValid());
    }

    @Test
    @DisplayName("rawPayloadUnsafe exposes the live array without copying (P2-211)")
    void rawPayloadUnsafeIsZeroCopy() {
        byte[] a = {1, 2, 3};
        MarketTick t = tick("VALID_TRADE", a);
        assertNotSame(a, t.rawPayload());
        t.rawPayloadUnsafe()[0] = 99;
        assertArrayEquals(new byte[]{99, 2, 3}, t.rawPayload());
    }

    @Test
    @DisplayName("rawPayload is defensive-copied so records compare by value (R-163)")
    void rawPayloadValueSemantics() {
        byte[] a = {1, 2, 3};
        byte[] b = {1, 2, 3};
        MarketTick t1 = tick("VALID_TRADE", a);
        MarketTick t2 = tick("VALID_TRADE", b);
        assertEquals(t1, t2, "equal bytes must make records equal");
        assertEquals(t1.hashCode(), t2.hashCode());

        // Mutating the input array after construction must not change the record.
        a[0] = 99;
        assertArrayEquals(new byte[]{1, 2, 3}, t1.rawPayload());

        // The accessor returns a copy — mutating it must not corrupt the record.
        t1.rawPayload()[0] = 77;
        assertArrayEquals(new byte[]{1, 2, 3}, t1.rawPayload());
        assertNotSame(t1.rawPayload(), t1.rawPayload(), "accessor must return fresh copies");
    }
}
