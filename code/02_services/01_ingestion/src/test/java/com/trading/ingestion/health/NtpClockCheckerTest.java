package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-032 / R-064 — NTP clock checker fail-closed fallback and response validation.
 */
@DisplayName("ING-UNIT-013: NTP clock checker hardening")
class NtpClockCheckerTest {

    // ---- R-064: response validation ----

    @Test
    @DisplayName("genuine server response passes validation")
    void validResponsePasses() throws Exception {
        byte[] request = new byte[48];
        request[0] = 0x23; // VN=4, mode=3
        writeUint32(request, 40, 0x10203040L); // transmit ts

        byte[] response = new byte[48];
        response[0] = 0x24; // VN=4, mode=4 (server)
        writeUint32(response, 24, 0x10203040L); // origin echoes transmit

        NtpClockChecker.validateResponse(request, response, response.length); // no throw
    }

    @Test
    @DisplayName("P1-085: truncated datagram throws even when the backing array is a full 48 bytes")
    void truncatedDatagramWithFullBackingArrayThrows() {
        // The old validator read response.length (always 48 for the shared
        // buffer) instead of the datagram length — a 20-byte datagram left
        // stale request bytes (client Tx at 40-47) parsed as the server
        // transmit timestamp, typically a near-zero offset false-pass.
        byte[] request = new byte[48];
        request[0] = 0x23;
        writeUint32(request, 40, 0x10203040L); // client transmit
        writeUint32(request, 44, 0x50607080L);
        byte[] backing = request.clone(); // receive() overwrote the shared buffer
        backing[0] = 0x24; // mode 4 forged by off-path sender
        // Mirror the client Tx into the origin window (24-31): the OLD code
        // compared the buffer against ITSELF, so the echo check trivially
        // passed and validation returned void (false-pass). New code throws
        // on the datagram length before any timestamp parsing.
        writeUint32(backing, 24, 0x10203040L);
        writeUint32(backing, 28, 0x50607080L);
        assertThrows(NtpClockChecker.NtpException.class, () ->
                NtpClockChecker.validateResponse(request, backing, 20));
    }

    @Test
    @DisplayName("short datagram is rejected")
    void shortDatagramRejected() {
        byte[] response = new byte[20];
        assertThrows(NtpClockChecker.NtpException.class,
                () -> NtpClockChecker.validateResponse(null, response, response.length));
    }

    @Test
    @DisplayName("non-server mode is rejected")
    void wrongModeRejected() {
        byte[] response = new byte[48];
        response[0] = 0x23; // mode 3 (client) — not a server response
        assertThrows(NtpClockChecker.NtpException.class,
                () -> NtpClockChecker.validateResponse(null, response, response.length));
    }

    @Test
    @DisplayName("origin timestamp not echoing our transmit is rejected")
    void originMismatchRejected() throws Exception {
        byte[] request = new byte[48];
        writeUint32(request, 40, 0x11111111L);
        byte[] response = new byte[48];
        response[0] = 0x24;
        writeUint32(response, 24, 0x22222222L); // wrong echo
        assertThrows(NtpClockChecker.NtpException.class,
                () -> NtpClockChecker.validateResponse(request, response, response.length));
    }

    // ---- R-032: fail-closed fallback ----

    @Test
    @DisplayName("unreachable servers fail closed when not required (R-032)")
    void unreachableServersFailClosed() {
        // Unroutable address with the hardcoded NTP port (123) — the query
        // fails fast and reaches the fallback path.
        NtpClockChecker checker = new NtpClockChecker(
                "10.255.255.1", 100, false);
        // P1-083: the fallback now THROWS instead of returning 0 (a 0 return
        // masqueraded an unverified clock as perfect sync for callers using
        // only the return value). Fail-closed state is unchanged.
        assertThrows(NtpClockChecker.NtpException.class, checker::measureOffsetMs);
        assertFalse(checker.isWithinLimit(),
                "unverified clock must NOT be within limit (fail-closed, R-032)");
        assertFalse(checker.isVerified(),
                "unreachable servers must leave the check unverified (R-032)");
    }

    @Test
    @DisplayName("required mode throws when all servers unreachable")
    void requiredModeThrows() {
        NtpClockChecker checker = new NtpClockChecker(
                "10.255.255.1", 100, true);
        assertThrows(NtpClockChecker.NtpException.class, checker::measureOffsetMs);
        assertFalse(checker.isWithinLimit());
    }

    // P1-248 guard: the published snapshot is one atomic record — offset,
    // pass/fail, verified and timestamp always belong to the SAME check, so
    // diagnostics() can never pair offset-N with ok-N+1. A failed check
    // publishes fail-closed values, stamped.
    @Test
    @DisplayName("failed check publishes a complete fail-closed snapshot (P1-248)")
    void failedCheckPublishesAtomicSnapshot() {
        NtpClockChecker checker = new NtpClockChecker(
                "10.255.255.1", 100, false);
        assertThrows(NtpClockChecker.NtpException.class, checker::measureOffsetMs);
        NtpClockChecker.ClockSnapshot snap = checker.snapshot();
        assertNotNull(snap, "every check — even a failed one — publishes a snapshot");
        assertEquals(0, snap.offsetMs());
        assertFalse(snap.passed());
        assertFalse(snap.verified());
        assertNotNull(snap.checkTime(), "the failed check is stamped");
        // Getters delegate to the same record — no drift between the two APIs.
        assertEquals(snap.offsetMs(), checker.lastOffsetMs());
        assertEquals(snap.passed(), checker.isWithinLimit());
        assertEquals(snap.verified(), checker.isVerified());
        assertEquals(snap.checkTime(), checker.lastCheckTime());
    }

    private static void writeUint32(byte[] buf, int offset, long value) {
        buf[offset] = (byte) (value >>> 24);
        buf[offset + 1] = (byte) (value >>> 16);
        buf[offset + 2] = (byte) (value >>> 8);
        buf[offset + 3] = (byte) value;
    }
}
