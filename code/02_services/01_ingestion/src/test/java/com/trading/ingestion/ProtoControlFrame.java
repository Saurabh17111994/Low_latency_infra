package com.trading.ingestion;

import com.trading.ingestion.transport.ControlRecord;
import com.trading.ingestion.transport.TransportFrame;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Test helper (proto-only transport, 2026-08-29): builds the length-prefixed
 * proto {@code TransportFrame} bytes for a control record, base64-encoded so
 * scripted fake bridges can embed them with {@code base64 -d | head -c N}.
 *
 * <p>The NDJSON pipe transport was removed; fake bridges must now emit proto
 * frames. The 4-byte little-endian length prefix + TransportFrame payload
 * matches {@code ProtoFrameReader} and the Go bridge's framing.
 */
final class ProtoControlFrame {

    private ProtoControlFrame() {
    }

    /** 64 lowercase hex chars — satisfies BridgeEvent's wire contract. */
    static final String HASH_64 = "a".repeat(64);

    /** Base64 of a length-prefixed TransportFrame wrapping the given control record. */
    static String controlFrameB64(
            String event, String reason, String state, long nowMs) throws Exception {
        ControlRecord control = ControlRecord.newBuilder()
                .setRecordType("bridge_event")
                .setContractVersion(2)
                .setEvent(event)
                .setSlotId("hft-0")
                .setConnectionId("conn-1")
                .setConnectionEpoch(1)
                .setState(state)
                .setAssignedTokens(0)
                .setAcknowledgedTokens(0)
                .setRejectedTokens(0)
                .setReason(reason == null ? "" : reason)
                .setReceivedTsMs(nowMs)
                .setManifestFingerprint(HASH_64)
                .setAssignedTokenSetHash(HASH_64)
                .build();
        TransportFrame frame = TransportFrame.newBuilder()
                .setProtocolVersion(1)
                .setControl(control)
                .build();
        byte[] payload = frame.toByteArray();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(payload.length + 4);
        bos.write(ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(payload.length).array());
        bos.write(payload);
        return Base64.getEncoder().encodeToString(bos.toByteArray());
    }

    /** Shell fragment: decode base64 to stdout. */
    static String shellDecode(String b64) {
        return "printf '%s' '" + b64 + "' | base64 -d\n";
    }
}
