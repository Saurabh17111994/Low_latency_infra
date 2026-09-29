package com.trading.mockarrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Wire-contract integration test for R-040: the server writes strictly
 * newline-delimited JSON — one tick object per line, never a blank line and
 * never a JSON array.
 */
class MockArrowServerTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int READ_TIMEOUT_MS = 5_000;

    @Test
    void firstBatchIsOneJsonObjectPerLineWithNoBlankLines() throws Exception {
        int tickRatePerSec = 20;
        List<Long> instruments = LongStream.range(0, 10).map(i -> 100000L + i).boxed().toList();
        // batchSize = max(1, ceil(instruments * tickRatePerSec / 100)), so the
        // first batch arrives on the scheduler's first 10 ms tick.
        int batchSize = Math.max(1, (int) Math.ceil(instruments.size() * tickRatePerSec / 100.0));

        int port = freePort();
        MockArrowServer server = new MockArrowServer(port, tickRatePerSec, instruments, 42L);
        Socket client = null;
        try {
            server.start();
            client = new Socket();
            client.connect(new InetSocketAddress("127.0.0.1", port), READ_TIMEOUT_MS);
            client.setSoTimeout(READ_TIMEOUT_MS);
            var reader = new BufferedReader(
                    new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));

            // Read one full batch plus the first line of the next batch: the
            // extra newLine() bug shows up as a blank line right after a batch.
            List<String> lines = readAtMost(reader, batchSize + 1);
            assertEquals(batchSize + 1, lines.size(),
                    "expected " + batchSize + " tick lines plus one more within " + READ_TIMEOUT_MS
                            + " ms; got " + lines);
            assertFalse(lines.stream().anyMatch(String::isBlank),
                    "R-040: every line must carry a tick, blank line received: " + lines);

            List<JsonNode> parsed = new ArrayList<>(lines.size());
            for (String line : lines) {
                parsed.add(MAPPER.readTree(line));
            }
            for (int i = 0; i < parsed.size(); i++) {
                assertTrue(parsed.get(i).isObject(), "line is not a JSON object: " + lines.get(i));
            }
            assertTrue(parsed.stream()
                            .anyMatch(node -> node.has("instrument_token") && node.has("last_price_paise")),
                    "no line carried the expected tick keys: " + lines);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException ignored) {
                }
            }
            server.stop();
        }
    }

    /**
     * H5-3: the retired shared pool had an unbounded queue and no write deadline.
     * Each session now owns a bounded outbox (64 batches) and exactly one daemon
     * writer; the pool field must stay gone, and the healthy-peer path must keep
     * flowing while a stalled peer is evicted.
     */
    @Test
    void sessionDeliveryIsPerClientAndBounded() throws Exception {
        int port = freePort();
        var server = new MockArrowServer(port, 20, List.of(100000L), 7L);
        Socket client = null;
        try {
            server.start();
            client = new Socket();
            client.connect(new InetSocketAddress("127.0.0.1", port), READ_TIMEOUT_MS);
            long deadline = System.currentTimeMillis() + 3_000;
            while (server.sessionCountForTest() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, server.sessionCountForTest(), "the session must be registered");
            assertEquals(64, MockArrowServer.SESSION_OUTBOX_CAPACITY,
                    "H5-3: the outbox bound is part of the contract");
            assertThrows(NoSuchFieldException.class,
                    () -> MockArrowServer.class.getDeclaredField("deliveryPool"),
                    "the shared delivery pool must stay deleted");
            long writers = Thread.getAllStackTraces().keySet().stream()
                    .filter(t -> "mock-arrow-session-writer".equals(t.getName())).count();
            assertEquals(1, writers, "one session = exactly one writer thread");
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException ignored) {
                }
            }
            server.stop();
        }
    }

    @Test
    void stalledClientIsEvictedAndHealthyPeerKeepsReceiving() throws Exception {
        int port = freePort();
        var instruments = LongStream.range(0, 10).map(i -> 100000L + i).boxed().toList();
        var server = new MockArrowServer(port, MockArrowServer.PER_INSTRUMENT_CAP, instruments,
                42L, SyntheticWorkload.Profile.PEAK);
        Socket stalled = null;
        Socket healthy = null;
        try {
            server.start();
            stalled = new Socket();
            stalled.connect(new InetSocketAddress("127.0.0.1", port), READ_TIMEOUT_MS);
            healthy = new Socket();
            healthy.connect(new InetSocketAddress("127.0.0.1", port), READ_TIMEOUT_MS);
            healthy.setSoTimeout(READ_TIMEOUT_MS);
            var reader = new BufferedReader(
                    new InputStreamReader(healthy.getInputStream(), StandardCharsets.UTF_8));

            // Both sessions must register before the eviction can be observed.
            long deadline = System.currentTimeMillis() + 5_000;
            while (server.sessionCountForTest() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(2, server.sessionCountForTest(), "both sessions must register");
            assertNotNull(reader.readLine(), "the healthy client receives before the eviction");

            // Stop ONLY the stalled session's writer: the real 1 s sweep must evict it
            // after SESSION_STALLED_AFTER_MS of no write progress.
            server.pauseSessionWritesForTest(stalled);
            deadline = System.currentTimeMillis() + 10_000;
            while (server.sessionCountForTest() > 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
            assertEquals(1, server.sessionCountForTest(),
                    "a session with no write progress past "
                            + MockArrowServer.SESSION_STALLED_AFTER_MS + " ms must be evicted");
            assertNotNull(reader.readLine(), "the healthy client must keep receiving after "
                    + "the stalled peer is evicted");
        } finally {
            if (stalled != null) {
                try {
                    stalled.close();
                } catch (IOException ignored) {
                }
            }
            if (healthy != null) {
                try {
                    healthy.close();
                } catch (IOException ignored) {
                }
            }
            server.stop();
        }
    }

    /**
     * P3-468: the startup log must not advertise a WebSocket endpoint — the
     * wire format is raw TCP NDJSON (R-039/R-040), so the log must say so.
     */
    @Test
    void startupLogAdvertisesTcpNdjsonNotWebSocket() throws Exception {
        var logger = (Logger) LoggerFactory.getLogger(MockArrowServer.class);
        var events = new ListAppender<ILoggingEvent>();
        events.start();
        logger.addAppender(events);
        var previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        var server = new MockArrowServer(freePort(), 20, List.of(100000L), 7L);
        try {
            server.start();
        } finally {
            server.stop();
            logger.detachAppender(events);
            logger.setLevel(previousLevel);
        }
        List<String> startup = events.list.stream()
                .map(ILoggingEvent::getFormattedMessage).toList();
        assertTrue(startup.stream().noneMatch(m -> m.contains("ws://") || m.contains("WebSocket")),
                "P3-468: startup log advertises a WebSocket server: " + startup);
        assertTrue(startup.stream().anyMatch(m -> m.contains("tcp://") && m.contains("NDJSON")),
                "P3-468: startup log must advertise the raw-TCP NDJSON endpoint: " + startup);
    }

    /**
     * H5-2 (DEC-045): the mock must never exceed the 20/s-per-instrument cap the
     * production profile is validated at. The retired mapping gave PEAK 30/s and
     * re-derived the profile from the rate, so the workload shape and the wire cap
     * were the same knob.
     */
    @Test
    void constructorRejectsARateAboveTheDec045Cap() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new MockArrowServer(freePort(), 30, List.of(100000L), 7L));
        assertTrue(ex.getMessage().contains("DEC-045"), ex.getMessage());
        assertTrue(ex.getMessage().contains("20"), ex.getMessage());
    }

    @Test
    void peakKeepsTheCapAndItsArrivalShape() throws Exception {
        var server = new MockArrowServer(freePort(), MockArrowServer.PER_INSTRUMENT_CAP,
                List.of(100000L), 7L, SyntheticWorkload.Profile.PEAK);
        assertEquals(20, server.configuredRate(),
                "PEAK must keep the DEC-045 wire cap; it selects the arrival shape, not the rate");
        assertEquals(SyntheticWorkload.Profile.PEAK, server.configuredProfile());
    }

    @Test
    void measuredAggregateStaysWithinTheCap() throws Exception {
        int instruments = 10;
        int port = freePort();
        var server = new MockArrowServer(port, MockArrowServer.PER_INSTRUMENT_CAP,
                LongStream.range(0, instruments).map(i -> 100000L + i).boxed().toList(),
                42L, SyntheticWorkload.Profile.PEAK);
        Socket client = null;
        try {
            server.start();
            client = new Socket();
            client.connect(new InetSocketAddress("127.0.0.1", port), READ_TIMEOUT_MS);
            client.setSoTimeout(500);
            var reader = new BufferedReader(
                    new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
            long start = System.nanoTime();
            int lines = 0;
            while ((System.nanoTime() - start) < 1_100_000_000L) {
                try {
                    if (reader.readLine() != null) {
                        lines++;
                    }
                } catch (SocketTimeoutException e) {
                    // keep counting until the measurement window ends
                }
            }
            // ~1.1 s at 10 instruments × 20/s ≈ 220 lines; the retired PEAK=30 gave
            // ~330. 25% headroom over the cap keeps the assertion stable while still
            // catching the old mapping.
            int bound = (int) (instruments * MockArrowServer.PER_INSTRUMENT_CAP * 1.1 * 1.25);
            assertTrue(lines <= bound,
                    "measured " + lines + " lines in ~1.1 s (bound " + bound
                            + ") — above the DEC-045 per-instrument cap");
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException ignored) {
                }
            }
            server.stop();
        }
    }

    /** Bounded read: never blocks past the socket read timeout, never hangs the suite. */
    private static List<String> readAtMost(BufferedReader reader, int maxLines) throws IOException {
        List<String> lines = new ArrayList<>(maxLines);
        try {
            while (lines.size() < maxLines) {
                String line = reader.readLine();
                if (line == null) break;
                lines.add(line);
            }
        } catch (SocketTimeoutException e) {
            // Return what arrived; the caller's assertions report the shortfall.
        }
        return lines;
    }

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }
}
