package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.ingestion.transport.ControlRecord;
import com.trading.ingestion.transport.MarketDataBatch;
import com.trading.ingestion.transport.TransportFrame;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P1-269: a truncated bridge stream (1-3 trailing header bytes) must fail as
 * an error — never look like a clean EOF. Only a zero-byte read at a frame
 * boundary is a clean exit.
 */
@DisplayName("P1-269: short header is a failure, not a clean exit")
class ProtoFrameReaderTruncationTest {

    private static ProtoFrameReader reader(byte[] bytes, AtomicInteger batches) {
        return new ProtoFrameReader(new ByteArrayInputStream(bytes),
                new ProtoFrameReader.FrameHandler() {
                    @Override
                    public void onMarketBatch(MarketDataBatch batch) {
                        batches.incrementAndGet();
                    }

                    @Override
                    public void onControl(ControlRecord control) {}
                });
    }

    @Test
    @DisplayName("2 trailing header bytes throw EOFException")
    void twoByteTrailerThrows() {
        AtomicInteger batches = new AtomicInteger();
        ProtoFrameReader r = reader(new byte[] {0x05, 0x00}, batches);
        assertThrows(EOFException.class, r::readLoop,
                "partial header must not read as a clean exit");
        assertEquals(0, batches.get());
    }

    @Test
    @DisplayName("1 and 3 trailing header bytes throw EOFException")
    void oneAndThreeByteTrailersThrow() {
        assertThrows(EOFException.class,
                () -> reader(new byte[] {0x01}, new AtomicInteger()).readLoop());
        assertThrows(EOFException.class,
                () -> reader(new byte[] {0x01, 0x02, 0x03}, new AtomicInteger()).readLoop());
    }

    @Test
    @DisplayName("empty stream is still a clean EOF")
    void emptyStreamIsCleanEof() {
        AtomicInteger batches = new AtomicInteger();
        assertDoesNotThrow(() -> reader(new byte[0], batches).readLoop());
        assertEquals(0, batches.get());
    }

    @Test
    @DisplayName("one valid frame then EOF delivers once and exits clean")
    void validFrameThenCleanEof() throws Exception {
        byte[] body = TransportFrame.newBuilder()
                .setProtocolVersion(ProtoFrameReader.PROTOCOL_VERSION)
                .setMarketBatch(MarketDataBatch.newBuilder().build())
                .build()
                .toByteArray();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(body.length & 0xFF);
        bos.write((body.length >> 8) & 0xFF);
        bos.write((body.length >> 16) & 0xFF);
        bos.write((body.length >> 24) & 0xFF);
        bos.write(body);
        AtomicInteger batches = new AtomicInteger();
        assertDoesNotThrow(() -> reader(bos.toByteArray(), batches).readLoop());
        assertEquals(1, batches.get(), "valid frame still delivered exactly once");
    }
}
