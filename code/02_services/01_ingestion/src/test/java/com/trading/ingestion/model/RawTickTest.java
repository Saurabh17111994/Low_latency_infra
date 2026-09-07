package com.trading.ingestion.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** P1-087/P1-088: RawTick immutability — no input aliasing, no output exposure. */
@DisplayName("P1-087/088: RawTick byte[] ownership")
class RawTickTest {

    private static RawTick.Builder base(byte[] payload) {
        return new RawTick.Builder()
                .rawPayload(payload)
                .payloadHash("abc123")
                .protocolVersion("go-arrow-v0")
                .decoderVersion("test")
                .receiveTime(Instant.now())
                .receiveTimeNanos(System.nanoTime());
    }

    @Test
    @DisplayName("P1-088: mutating the caller buffer between rawPayload(v) and build() cannot corrupt the tick")
    void builderCopiesInputDefensively() {
        byte[] buf = {1, 2, 3, 4};
        RawTick.Builder b = base(buf);
        buf[0] = 99; // caller reuses/mutates its buffer before build (TOCTOU)
        RawTick tick = b.build();
        assertArrayEquals(new byte[]{1, 2, 3, 4}, tick.rawPayload());
    }

    @Test
    @DisplayName("P1-088: retained bytes survive later caller mutation (hash invariant holds)")
    void builtTickIsolatedFromCallerBuffer() {
        byte[] buf = {5, 6, 7};
        RawTick tick = base(buf).build();
        buf[1] = 100;
        assertArrayEquals(new byte[]{5, 6, 7}, tick.rawPayload());
    }

    @Test
    @DisplayName("P1-087: rawPayload() returns a copy — mutating it cannot corrupt the tick")
    void accessorReturnsDefensiveCopy() {
        RawTick tick = base(new byte[]{8, 9}).build();
        tick.rawPayload()[0] = 100;
        assertArrayEquals(new byte[]{8, 9}, tick.rawPayload());
    }

    @Test
    @DisplayName("P1-087: rawPayloadLength() reports the size without exposing the array")
    void lengthWithoutExposure() {
        assertEquals(4, base(new byte[]{1, 2, 3, 4}).build().rawPayloadLength());
    }

    @Test
    @DisplayName("P1-088: null rawPayload still fails fast with a message, not a deferred NPE")
    void nullPayloadFailsFast() {
        assertThrows(NullPointerException.class, () -> base(null).build());
    }

    @Test
    @DisplayName("P1-250: negative System.nanoTime() instants are legitimate (arbitrary origin)")
    void negativeNanoTimeAccepted() {
        RawTick tick = base(new byte[]{1, 2, 3}).receiveTimeNanos(-5L).build();
        assertEquals(-5L, tick.receiveTimeNanos());
    }

    @Test
    @DisplayName("P1-250: zero receiveTimeNanos still fails fast (unset sentinel)")
    void zeroNanoTimeRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> base(new byte[]{1, 2, 3}).receiveTimeNanos(0L).build());
    }
}
