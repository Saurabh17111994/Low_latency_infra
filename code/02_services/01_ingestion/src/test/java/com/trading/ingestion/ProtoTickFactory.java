package com.trading.ingestion;

import com.google.protobuf.ByteString;
import com.trading.ingestion.transport.TickEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Test helper: builds a valid proto TickEvent the way the arrow-bridge's
 * proto emitter does (transport contract T6). Replaces the removed NDJSON
 * tickLine helpers — the NDJSON pipe transport was removed 2026-08-29 and
 * the only ingestion path is processTickEvent(TickEvent, connId, epoch).
 */
public final class ProtoTickFactory {

    private ProtoTickFactory() {}

    public static final byte[] FRAME_PAYLOAD = new byte[]{
            0x28, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A
    };

    /** Build a full-mode tick with the given token/ts/price and seq. */
    public static TickEvent tick(long token, long tsMs, long ltpPaise, long seq, long epoch) {
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken((int) token)
                .setFeed("hft")
                .setTsMs(tsMs)
                .setReceivedMs(System.currentTimeMillis())
                .setFeedSequenceLocal(seq)
                .setLtpPaise(ltpPaise)
                .setVolume(100)
                .setOpenPaise(ltpPaise - 10)
                .setHighPaise(ltpPaise + 10)
                .setLowPaise(ltpPaise - 20)
                .setClosePaise(ltpPaise)
                .setRawPayload(ByteString.copyFrom(FRAME_PAYLOAD))
                .setPayloadHash(ByteString.copyFrom(sha256(FRAME_PAYLOAD)))
                .build();
    }

    /** Build a tick with an explicit mode (full/ltpc/ltp). */
    public static TickEvent tick(long token, long tsMs, long ltpPaise, String mode, long seq, long epoch) {
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode(mode)
                .setToken((int) token)
                .setFeed("hft")
                .setTsMs(tsMs)
                .setReceivedMs(System.currentTimeMillis())
                .setFeedSequenceLocal(seq)
                .setLtpPaise(ltpPaise)
                .setVolume(100)
                .setRawPayload(ByteString.copyFrom(FRAME_PAYLOAD))
                .setPayloadHash(ByteString.copyFrom(sha256(FRAME_PAYLOAD)))
                .build();
    }

    /** Build a tick with a custom payload hash (for hash-mismatch tests). */
    public static TickEvent tickWithHash(long token, long tsMs, long ltpPaise, long seq, byte[] payload, byte[] hash) {
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken((int) token)
                .setFeed("hft")
                .setTsMs(tsMs)
                .setReceivedMs(System.currentTimeMillis())
                .setFeedSequenceLocal(seq)
                .setLtpPaise(ltpPaise)
                .setVolume(100)
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom(hash))
                .build();
    }

    public static byte[] sha256(byte[] in) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** UTF-8 bytes of the sha256 hex (the wire contract's payload_hash). */
    public static ByteString sha256HexBytes(byte[] in) {
        StringBuilder sb = new StringBuilder();
        for (byte b : sha256(in)) {
            sb.append(String.format("%02x", b));
        }
        return ByteString.copyFrom(sb.toString().getBytes(StandardCharsets.UTF_8));
    }
}
