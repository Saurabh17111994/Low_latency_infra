package com.trading.ingestion.model;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** P1-089: TickPacket monetary invariants — corrupt values never reach append. */
@DisplayName("P1-089: TickPacket monetary validation")
class TickPacketValidationTest {

    private static TickPacket.Builder valid() {
        return new TickPacket.Builder()
                .raw(new RawTick.Builder()
                        .rawPayload(new byte[]{1, 2, 3})
                        .payloadHash("abc123")
                        .protocolVersion("go-arrow-v0")
                        .decoderVersion("test")
                        .receiveTime(Instant.now())
                        .receiveTimeNanos(System.nanoTime())
                        .build())
                .validity(ValidityClassification.VALID_TRADE)
                .lastPricePaise(12345L)
                .instrumentToken(100000L)
                .tradingSymbol("SYM-EQ")
                .exchange("NSE")
                .eventTime(Instant.now().minusMillis(100))
                .ingestTs(Instant.now())
                .eventFingerprint("fp_1")
                .fingerprintVersion(1)
                .connectionId("test")
                .connectionEpoch(0L);
    }

    @Test
    @DisplayName("fully valid packet builds")
    void validPacketBuilds() {
        assertDoesNotThrow(() -> valid().build());
    }

    @Test
    @DisplayName("negative prices rejected (every classification)")
    void rejectsNegativePrice() {
        assertThrows(IllegalArgumentException.class, () -> valid().lastPricePaise(-1L).build());
        assertThrows(IllegalArgumentException.class, () -> valid().ohlcHighPaise(-5L).build());
        assertThrows(IllegalArgumentException.class, () -> valid().averagePricePaise(-5L).build());
    }

    @Test
    @DisplayName("negative volume/openInterest rejected")
    void rejectsNegativeVolume() {
        assertThrows(IllegalArgumentException.class, () -> valid().volume(-1L).build());
        assertThrows(IllegalArgumentException.class, () -> valid().openInterest(-1L).build());
    }

    @Test
    @DisplayName("NaN/Infinite change rejected")
    void rejectsNonFiniteChange() {
        assertThrows(IllegalArgumentException.class, () -> valid().change(Double.NaN).build());
        assertThrows(IllegalArgumentException.class,
                () -> valid().change(Double.POSITIVE_INFINITY).build());
    }

    @Test
    @DisplayName("VALID_TRADE requires a positive price")
    void validTradeRequiresPositivePrice() {
        assertThrows(IllegalArgumentException.class, () -> valid().lastPricePaise(0L).build());
    }

    @Test
    @DisplayName("zero price stays legal for non-trade quotes")
    void zeroPriceLegalForNonTrade() {
        assertDoesNotThrow(() -> valid()
                .validity(ValidityClassification.VALID_NON_TRADE)
                .lastPricePaise(0L).build());
    }
}
