package com.trading.ingestion.health;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks local clock offset against an NTP server.
 *
 * <p>Uses the standard NTP (SNTP) protocol: sends a 48-byte request packet
 * to a configured NTP server and computes the offset from the response.
 * Falls back to a basic sanity check if the NTP server is unreachable
 * (clock must not be before 2024-01-01 UTC).
 *
 * <p>Contract: clock offset &lt;= {@code CLOCK_OFFSET_LIMIT_MS} (default 2000ms / 2s, T10)
 * for ingestion readiness per {@code docs/08_implementation/03-ingestion.md}.
 */
public final class NtpClockChecker {

    private static final Logger LOG = LoggerFactory.getLogger(NtpClockChecker.class);

    // NTP protocol constants
    private static final int NTP_PORT = 123;
    private static final int NTP_PACKET_SIZE = 48;
    private static final int NTP_MODE_CLIENT = 3;
    private static final long NTP_EPOCH_OFFSET = 2_208_988_800L; // seconds from 1900 to 1970
    private static final int SOCKET_TIMEOUT_MS = 5_000;

    // Fallback: clock must be after this epoch ms (2024-01-01)
    private static final long MIN_WALL_CLOCK_EPOCH_MS = 1_704_067_200_000L;

    private final String[] ntpServers;
    private final long offsetLimitMs;
    private final boolean required;
    // P1-248: ONE volatile snapshot instead of four — measureOffsetMs
    // updated the four fields non-atomically, so HealthProbe.diagnostics()
    // could read offset from check N with pass/fail from check N+1 (torn
    // snapshot). Every update publishes a complete immutable record at
    // once; readers take one reference and see a consistent check.
    /** Immutable single-check outcome. Null checkTime = never checked. */
    record ClockSnapshot(long offsetMs, Instant checkTime, boolean passed, boolean verified) {}
    private volatile ClockSnapshot snapshot = new ClockSnapshot(0, null, false, false);

    private void publish(long offsetMs, boolean passed, boolean verified) {
        snapshot = new ClockSnapshot(offsetMs, Instant.now(), passed, verified);
    }

    /** The latest complete check outcome — a single atomic read (P1-248). */
    ClockSnapshot snapshot() {
        return snapshot;
    }

    /** @param ntpServer      NTP server hostname (e.g. "pool.ntp.org")
     *  @param offsetLimitMs  maximum allowed clock offset in millis */
    public NtpClockChecker(String ntpServer, long offsetLimitMs) {
        this(ntpServer, offsetLimitMs, false);
    }

    /**
     * @param ntpServer      NTP server hostname (or comma-separated list, tried in order)
     * @param offsetLimitMs  maximum allowed clock offset in millis
     * @param required       when true, an unreachable NTP server fails the check
     *                       instead of falling back to a wall-clock sanity check
     */
    public NtpClockChecker(String ntpServer, long offsetLimitMs, boolean required) {
        this.ntpServers = splitServers(ntpServer);
        this.offsetLimitMs = offsetLimitMs;
        this.required = required;
    }

    private static String[] splitServers(String value) {
        if (value == null || value.isBlank()) {
            return new String[] {"ntp.ubuntu.com", "time.google.com", "in.pool.ntp.org"};
        }
        String[] parts = value.split(",");
        List<String> cleaned = new java.util.ArrayList<>();
        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty()) cleaned.add(t);
        }
        return cleaned.isEmpty()
                ? new String[] {"ntp.ubuntu.com", "time.google.com", "in.pool.ntp.org"}
                : cleaned.toArray(new String[0]);
    }

    /**
     * Query the NTP server and return the clock offset in milliseconds.
     * Positive = local clock is ahead of NTP. Negative = local clock is behind.
     *
     * @throws NtpException if the server is unreachable or the response is invalid
     */
    public long measureOffsetMs() throws NtpException {
        NtpException lastError = null;
        for (String server : ntpServers) {
            try {
                long offset = queryNtp(server);
                boolean passed = Math.abs(offset) <= offsetLimitMs;
                publish(offset, passed, true);

                if (!passed) {
                    LOG.warn("ntp-clock: offset {}ms exceeds limit {}ms (server={})",
                            offset, offsetLimitMs, server);
                }
                return offset;
            } catch (NtpException e) {
                lastError = e;
                LOG.warn("ntp-clock: server {} unreachable; trying next", server);
            }
        }

        if (required) {
            // Strict mode: an unreachable time source is a failure.
            publish(0, false, false);
            LOG.error("ntp-clock: all servers unreachable and CLOCK_CHECK_REQUIRED=true — clock check FAILED");
            throw lastError != null ? lastError : new NtpException("all NTP servers unreachable", null);
        }
        // Fallback (R-032): FAIL-CLOSED. Without a real NTP response the offset
        // is unverified — a clock skewed by hours would pass the old wall-clock
        // sanity check and write wrong timestamps. Readiness now fails (clock
        // unverified) unless an operator explicitly overrides, and the degraded
        // state is surfaced via isVerified().
        long now = System.currentTimeMillis();
        if (now < MIN_WALL_CLOCK_EPOCH_MS) {
            publish(0, false, false);
            LOG.error("ntp-clock: wall clock before {} — clock check FAILED",
                    Instant.ofEpochMilli(MIN_WALL_CLOCK_EPOCH_MS));
            throw lastError != null ? lastError : new NtpException("all NTP servers unreachable", null);
        }
        publish(0, false, false);   // fail-closed: unverified != within limit
        // P1-083: THROW instead of returning 0 — a 0 return masqueraded an
        // unverified clock as perfect sync for return-value-only callers
        // (Math.abs(measureOffsetMs()) <= limit). Fail-closed state is
        // unchanged (isWithinLimit stays false, and HealthProbe readiness keys
        // off isWithinLimit — so an unverified clock blocks readiness); this
        // only makes the failure LOUD.
        // Batch-1 #31 CLOSED 2026-09-07: all 3 default servers
        // (ntp.ubuntu.com, time.google.com, in.pool.ntp.org) answer UDP/123
        // from this host with valid 48-byte responses — no loud-degraded
        // revisit needed. If a future network blocks NTP with
        // CLOCK_CHECK_REQUIRED=false, startup still continues via the
        // IngestionService catch (warn + wall-clock sanity); only readiness
        // stays red until a server responds.
        LOG.error("ntp-clock: all servers unreachable and CLOCK_CHECK_REQUIRED=false — clock UNVERIFIED "
                + "(fail-closed; readiness blocked until a server responds)");
        throw lastError != null ? lastError : new NtpException("all NTP servers unreachable", null);
    }

    /** True if the last check passed (offset within limit). Never true when unverified. */
    public boolean isWithinLimit() {
        return snapshot.passed();
    }

    /** True if the last offset was verified against a real NTP server response. */
    public boolean isVerified() {
        return snapshot.verified();
    }

    /** Offset in ms from last check. Positive = local ahead. */
    public long lastOffsetMs() {
        return snapshot.offsetMs();
    }

    /** When the last check happened, or null if never checked. */
    public Instant lastCheckTime() {
        return snapshot.checkTime();
    }

    /** The configured limit in ms. */
    public long offsetLimitMs() {
        return offsetLimitMs;
    }

    // ---- NTP query (RFC 5905 SNTP client) ----

    private static long queryNtp(String host) throws NtpException {
        byte[] buffer = new byte[NTP_PACKET_SIZE];

        // Build NTP request (SNTP client, version 4, mode 3)
        buffer[0] = (byte) (0x23); // LI=0, VN=4, Mode=3
        // R-064: the client MUST set its transmit timestamp (bytes 40-47) so the
        // server's origin timestamp can be validated against it.
        long nowSeconds = System.currentTimeMillis() / 1000L + NTP_EPOCH_OFFSET;
        writeUint32(buffer, 40, nowSeconds);
        writeUint32(buffer, 44, 0); // fraction — 0 is acceptable for SNTP

        long sendNanos;
        long receiveNanos;

        // P1-084: snapshot the request BEFORE receive() overwrites the
        // shared buffer — passing the same array as request+response made the
        // origin-echo check dead code (any off-path mode-4 48-byte packet
        // accepted). P1-085: validate the DATAGRAM length, not the backing
        // array (always 48) — a truncated packet left stale request bytes
        // (client Tx at 40-47) parsed as the server transmit timestamp,
        // typically a near-zero offset false-pass.
        byte[] requestBytes = buffer.clone();
        int responseLength;
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            InetAddress address = InetAddress.getByName(host);

            DatagramPacket request = new DatagramPacket(buffer, buffer.length, address, NTP_PORT);

            sendNanos = System.nanoTime();
            socket.send(request);

            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            socket.receive(response);
            responseLength = response.getLength();
            receiveNanos = System.nanoTime();

        } catch (Exception e) {
            throw new NtpException("NTP query to " + host + " failed", e);
        }

        // R-064: validate the datagram is a genuine NTP server response before
        // trusting any timestamp: 48-byte packet, server mode (4), and the
        // origin timestamp echoing our transmit timestamp.
        validateResponse(requestBytes, buffer, responseLength);

        // Parse NTP response
        // Transmit timestamp: bytes 40-47 (seconds + fraction)
        long transmitSeconds = readUint32(buffer, 40);
        long transmitFraction = readUint32(buffer, 44);

        // Convert NTP timestamp (1900 epoch) to milliseconds since 1970
        long ntpMillis = (transmitSeconds - NTP_EPOCH_OFFSET) * 1000L
                + (transmitFraction * 1000L) / 0x1_0000_0000L;

        // Local time when response arrived (system time)
        // Account for half the round-trip to approximate the offset
        long rttNanos = receiveNanos - sendNanos;
        long localMillis = System.currentTimeMillis() - (rttNanos / 2_000_000); // half RTT

        return localMillis - ntpMillis;
    }

    /**
     * Validate a received datagram is a genuine NTP server response (R-064):
     * exactly {@link #NTP_PACKET_SIZE} bytes, LI/VN/Mode byte with server mode
     * (bits 0-2 == 4), and the origin timestamp (bytes 24-31) echoing the
     * client's transmit timestamp (bytes 40-47 of the request).
     *
     * <p>Package-private static for direct unit testing.
     *
     * @throws NtpException if any check fails
     */
    static void validateResponse(byte[] request, byte[] response, int length) throws NtpException {
        if (response == null || length != NTP_PACKET_SIZE) {
            throw new NtpException("invalid NTP response: expected " + NTP_PACKET_SIZE
                    + " bytes, got " + length, null);
        }
        int mode = response[0] & 0x07;
        if (mode != 4) {
            throw new NtpException("invalid NTP response: mode " + mode
                    + " (expected 4 = server)", null);
        }
        // Server must echo our transmit timestamp in its origin timestamp.
        if (request != null && request.length == NTP_PACKET_SIZE) {
            long sentTx = readUint32(request, 40);
            long echoedOrigin = readUint32(response, 24);
            if (sentTx != 0 && echoedOrigin != sentTx) {
                throw new NtpException("invalid NTP response: origin timestamp does not echo "
                        + "the request transmit timestamp", null);
            }
        }
    }

    private static long readUint32(byte[] buf, int offset) {
        return ((buf[offset] & 0xFFL) << 24)
                | ((buf[offset + 1] & 0xFFL) << 16)
                | ((buf[offset + 2] & 0xFFL) << 8)
                | (buf[offset + 3] & 0xFFL);
    }

    private static void writeUint32(byte[] buf, int offset, long value) {
        buf[offset] = (byte) (value >>> 24);
        buf[offset + 1] = (byte) (value >>> 16);
        buf[offset + 2] = (byte) (value >>> 8);
        buf[offset + 3] = (byte) value;
    }

    // ---- Exception ----

    public static final class NtpException extends Exception {
        public NtpException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
