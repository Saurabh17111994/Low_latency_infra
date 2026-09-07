// ProtoFrameReader — T6 proto transport reader (contract §6 T6).
//
// Reads length-prefixed TransportFrame protobuf records from an InputStream:
//
//	[4-byte little-endian length][protobuf TransportFrame bytes]
//
// Supports transport sniffing: probe() reads the first 4 bytes; if they
// form a plausible frame length AND the first frame parses as a
// TransportFrame, the stream is proto. Otherwise the bytes are pushed back
// and the caller treats the bridge as failed (proto-only since 2026-08-29;
// the NDJSON fallback was removed).
//
// Frames are delivered to a callback. MarketDataBatch frames carry market
// ticks (batched); ControlRecord frames carry bridge_event / bridge_metrics
// / broker_quarantine (never interleaved into market batches, Q20).

package com.trading.ingestion.write;

import com.trading.ingestion.transport.ControlRecord;
import com.trading.ingestion.transport.MarketDataBatch;
import com.trading.ingestion.transport.TransportFrame;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads length-prefixed {@link TransportFrame} protobuf records.
 * Thread-confined: one reader per bridge process stream.
 */
public final class ProtoFrameReader {

    /** Framing protocol version (must match Go TransportVersion = 1). */
    public static final int PROTOCOL_VERSION = 1;

    /** Max plausible frame length (64 MiB — matches Go maxFrameLen). */
    static final int MAX_FRAME_LEN = 64 << 20;

    /** Callback for one decoded frame. */
    public interface FrameHandler {
        void onMarketBatch(MarketDataBatch batch) throws IOException;
        void onControl(ControlRecord control) throws IOException;
    }

    private final BufferedInputStream in;
    private final FrameHandler handler;

    public ProtoFrameReader(InputStream in, FrameHandler handler) {
        this.in = new BufferedInputStream(in);
        this.handler = handler;
    }

    /**
     * The buffered stream with any sniffed-then-restored bytes available.
     * After {@link #sniffProto()} returns false, the caller must read from
     * THIS stream (not the raw input) — the sniff consumed nothing net.
     */
    public InputStream stream() {
        return in;
    }

    /**
     * Sniff whether the stream starts with a proto frame. Reads up to 4 bytes;
     * on a miss they are pushed back so the stream is un-consumed.
     *
     * @return true if the first 4 bytes form a plausible length prefix whose
     *         frame parses as a TransportFrame; false → not proto (bridge failure).
     */
    public boolean sniffProto() throws IOException {
        // P1-107/108/109: mark BEFORE consuming anything. Every miss path
        // resets to this mark, so hdr+BODY (up to 64 MiB) are restored for
        // the stream() fallback — the old 4-byte pushback drained the body
        // and corrupted any stream with a plausible length prefix.
        // BufferedInputStream grows its buffer only for bytes actually read.
        in.mark(4 + MAX_FRAME_LEN);
        byte[] hdr = new byte[4];
        int got = readFullyOrEof(hdr);
        if (got < 4) {
            // stream too short to be proto — could be an empty stream or a
            // trailing partial frame. Report not-proto (never auto-quarantine).
            in.reset();
            return false;
        }
        long len = ((hdr[0] & 0xFFL)) | ((hdr[1] & 0xFFL) << 8)
                | ((hdr[2] & 0xFFL) << 16) | ((hdr[3] & 0xFFL) << 24);
        if (len <= 0 || len > MAX_FRAME_LEN) {
            in.reset();
            return false;
        }
        // Try to parse the first frame. If it fails, the stream is not proto —
        // reset (hdr+body restored) and report not-proto.
        byte[] body = new byte[(int) len];
        try {
            readFully(body);
        } catch (EOFException e) {
            in.reset();
            return false;
        }
        try {
            TransportFrame frame = TransportFrame.parseFrom(body);
            if (frame.getProtocolVersion() != PROTOCOL_VERSION
                    || !(frame.hasMarketBatch() || frame.hasControl())) {
                in.reset();
                return false;
            }
            // The first frame is consumed — deliver it so no data is lost.
            deliver(frame);
            return true;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            in.reset();
            return false;
        }
    }

    /**
     * Read and deliver frames until EOF or IOException. Returns on EOF.
     */
    public void readLoop() throws IOException {
        while (true) {
            byte[] hdr = new byte[4];
            int got = readFullyOrEof(hdr);
            if (got == 0) {
                return; // clean EOF at a frame boundary
            }
            if (got < 4) {
                // P1-269: a 1-3 byte trailing header is truncation, never a
                // clean exit — fail loud so a crashed/truncated bridge is
                // not mistaken for a normal EOF.
                throw new EOFException("stream ended mid-header (" + got + "/4 bytes)");
            }
            long len = ((hdr[0] & 0xFFL)) | ((hdr[1] & 0xFFL) << 8)
                    | ((hdr[2] & 0xFFL) << 16) | ((hdr[3] & 0xFFL) << 24);
            if (len <= 0 || len > MAX_FRAME_LEN) {
                throw new IOException("invalid frame length: " + len);
            }
            byte[] body = new byte[(int) len];
            readFully(body);
            TransportFrame frame;
            try {
                frame = TransportFrame.parseFrom(body);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new IOException("invalid frame payload", e);
            }
            if (frame.getProtocolVersion() != PROTOCOL_VERSION) {
                throw new IOException("unsupported protocol_version: " + frame.getProtocolVersion());
            }
            deliver(frame);
        }
    }

    private void deliver(TransportFrame frame) throws IOException {
        if (frame.hasMarketBatch()) {
            handler.onMarketBatch(frame.getMarketBatch());
        } else if (frame.hasControl()) {
            handler.onControl(frame.getControl());
        } else {
            // empty frame — ignore (Go never emits one)
        }
    }

    private int readFullyOrEof(byte[] b) throws IOException {
        int off = 0;
        while (off < b.length) {
            int n = in.read(b, off, b.length - off);
            if (n < 0) {
                return off;
            }
            off += n;
        }
        return off;
    }

    private void readFully(byte[] b) throws IOException {
        int off = 0;
        while (off < b.length) {
            int n = in.read(b, off, b.length - off);
            if (n < 0) {
                throw new EOFException("stream ended mid-frame (" + off + "/" + b.length + " bytes)");
            }
            off += n;
        }
    }
}
