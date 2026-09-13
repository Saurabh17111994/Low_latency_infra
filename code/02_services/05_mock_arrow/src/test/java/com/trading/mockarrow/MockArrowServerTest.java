package com.trading.mockarrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
