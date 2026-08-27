// T1 contract tests — Java side of the Go ↔ protobuf ↔ Java serialization
// contract (plan §7.3). Mirrors the Go market_data_test.go so both languages
// lock the same proto schema.
package com.trading.ingestion.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** T1-R1/T1-X1: Java round-trip + bit-exact raw payload. */
@DisplayName("T1: TickEvent Java contract tests")
class TickEventContractTest {

    private static final byte[][] RAW_PAYLOADS = {
        {0x00, 0x01, 0x02, 0x03},           // leading zero byte
        {(byte) 0xFF, (byte) 0xFE, (byte) 0x80, 0x7F}, // high-bit bytes
        {0x00, 0x00, 0x00, 0x00},           // all zeros
        {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF, 0x00, (byte) 0xFF},
        "not-json-not-base64-{}[]\\".getBytes(StandardCharsets.UTF_8),
        {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
    };

    private TickEvent sampleEvent(byte[] raw) {
        return TickEvent.newBuilder()
            .setSlotId("slot-0")
            .setMode("full")
            .setToken(123456)
            .setFeed("hft")
            .setTsMs(1_720_000_000_000L)
            .setReceivedMs(1_720_000_000_123L)
            .setFeedSequenceLocal(7)
            .setLtpPaise(98765)
            .setClosePaise(98000)
            .setOpenPaise(97000)
            .setHighPaise(99500)
            .setLowPaise(96500)
            .setVwapPaise(98123)
            .setLtq(42)
            .setVolume(1_000_000)
            .setTotalBuyQty(5_000_000)
            .setTotalSellQty(4_000_000)
            .setOpenInterest(999)
            .addBidPx(100).addBidPx(99).addBidPx(98).addBidPx(97).addBidPx(96)
            .addAskPx(101).addAskPx(102).addAskPx(103).addAskPx(104).addAskPx(105)
            .addBidQty(10).addBidQty(20).addBidQty(30).addBidQty(40).addBidQty(50)
            .addAskQty(11).addAskQty(21).addAskQty(31).addAskQty(41).addAskQty(51)
            .addBidOrders(1).addBidOrders(2).addBidOrders(3).addBidOrders(4).addBidOrders(5)
            .addAskOrders(6).addAskOrders(7).addAskOrders(8).addAskOrders(9).addAskOrders(10)
            .setRawPayload(ByteString.copyFrom(raw))
            .build();
    }

    @Test
    @DisplayName("T1-R1: Java round-trip preserves every field")
    void roundTripPreservesFields() throws Exception {
        TickEvent ev = sampleEvent(RAW_PAYLOADS[0]);
        byte[] bytes = ev.toByteArray();
        TickEvent back = TickEvent.parseFrom(bytes);
        assertEquals(ev, back, "round-trip must be field-identical");
        assertEquals(5, back.getBidPxCount(), "depth arrays fixed 5");
        assertEquals(123456, back.getToken());
        assertEquals("hft", back.getFeed());
    }

    @Test
    @DisplayName("T1-X1: bit-exact raw payload survives Java round-trip")
    void bitExactRawPayload() throws Exception {
        for (int i = 0; i < RAW_PAYLOADS.length; i++) {
            TickEvent ev = sampleEvent(RAW_PAYLOADS[i]);
            byte[] bytes = ev.toByteArray();
            TickEvent back = TickEvent.parseFrom(bytes);
            assertArrayEquals(RAW_PAYLOADS[i], back.getRawPayload().toByteArray(),
                "payload " + i + " bit-exact failed");
        }
    }

    @Test
    @DisplayName("T1-P2: integer fidelity — no float, extreme values preserved")
    void integerFidelity() {
        TickEvent ev = TickEvent.newBuilder()
            .setToken(Integer.MAX_VALUE)
            .setLtpPaise(Integer.MAX_VALUE)
            .setVolume(Long.MAX_VALUE)
            .setTsMs(Long.MAX_VALUE)
            .setOpenInterest(Long.MAX_VALUE)
            .addBidPx(Integer.MAX_VALUE).addBidPx(0).addBidPx(-1).addBidPx(Integer.MIN_VALUE).addBidPx(5)
            .addBidOrders(Integer.MAX_VALUE).addBidOrders(0).addBidOrders(1).addBidOrders(2).addBidOrders(3)
            .build();
        assertEquals(Integer.MAX_VALUE, ev.getToken());
        assertEquals(Integer.MAX_VALUE, ev.getLtpPaise());
        assertEquals(Long.MAX_VALUE, ev.getVolume());
        assertEquals(Integer.MAX_VALUE, ev.getBidPx(0));
        assertEquals(Integer.MIN_VALUE, ev.getBidPx(3));
        assertEquals(-1, ev.getBidPx(2));
        assertEquals(Integer.MAX_VALUE, ev.getBidOrders(0));
        // zero depth values preserved
        assertEquals(0, ev.getBidPx(1));
        assertEquals(0, ev.getBidOrders(1));
    }

    @Test
    @DisplayName("T1-C1: batch carries multiple events")
    void batchRoundTrip() throws Exception {
        MarketDataBatch batch = MarketDataBatch.newBuilder()
            .setConnectionId("ingestion-local/hft-0")
            .setConnectionEpoch(1)
            .setBatchSeq(3)
            .setCreatedMs(1_720_000_000_000L)
            .addEvents(sampleEvent(RAW_PAYLOADS[0]))
            .addEvents(sampleEvent(RAW_PAYLOADS[1]))
            .setBatchPayloadHash(ByteString.copyFrom(new byte[]{1, 2}))
            .build();
        MarketDataBatch back = MarketDataBatch.parseFrom(batch.toByteArray());
        assertEquals(batch, back);
        assertEquals(2, back.getEventsCount());
        assertTrue(back.getBatchPayloadHash().size() == 2);
    }
}
