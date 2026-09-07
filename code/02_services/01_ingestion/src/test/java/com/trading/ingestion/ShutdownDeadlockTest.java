package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ING-FAIL-010: shutdown with a Fluss ack that never completes must not hang,
 * and the uncertainty journal must pin the exact bytes still pending (R-260).
 *
 * <p>Scenario: valid ticks are fed into the real pipeline against a
 * {@link FlussRowConverter} whose append future never completes AND ignores
 * cancellation (an {@code UncancelableFuture} — a real wedge would surface as
 * a never-acking or synchronously-throwing Fluss client). Every tick is
 * ACCEPTED and its tracker reservation stays pending. {@code shutdown()} is
 * then called TWICE:
 *
 * <pre>{@code
 *   1st shutdown  — journal.write (pre-drain, pins the exact pending bytes),
 *                   writer.close() → drain() waits at most DRAIN_DEADLINE_SECONDS
 *                   then R-260 releases the exact tracked bytes (no hang)
 *   2nd shutdown  — a no-op: the shutdownStarted latch holds, so no second
 *                   journal entry and no second drain
 * }</pre>
 *
 * <p>The drain deadline is configurable ({@code DRAIN_DEADLINE_SECONDS}, here
 * 2 s) so the "exits within the drain deadline" property is provable in-test
 * instead of waiting out the production 30 s default.
 */
@DisplayName("ING-FAIL-010: shutdown deadlock — ack never completes, no hang, journal pins pending bytes")
class ShutdownDeadlockTest {

    private static final long TOKEN_A = 100_000L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("stuck ack: shutdown returns within the drain deadline and the journal records the exact pending bytes")
    void shutdownReturnsWithinDrainDeadlineAndJournalPinsPendingBytes() throws Exception {
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");
        IngestionConfig config = buildConfig(journalPath);
        StuckConverter converter = new StuckConverter();
        IngestionService service = new IngestionService(
                "ing-fail-010", instruments(), converter, config, null,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        // 1. Feed 3 valid ticks — each is ACCEPTED, each reservation stays
        //    pending because the ack future never completes.
        long now = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            service.processTickEvent(protoTick(TOKEN_A, now, 100 + i), "hft-0", 1L);
        }
        // The proto path enqueues asynchronously — wait for acceptance.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (service.tracker().totalAccepted() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(3, service.tracker().totalAccepted(), "all 3 ticks must be accepted");
        long expectedPendingBytes = 3L * converter.estimatedRowSize(null);
        assertEquals(3, service.tracker().pendingRecords(), "all 3 must stay pending (ack never completes)");
        assertEquals(expectedPendingBytes, service.tracker().pendingBytes(),
                "pending bytes must equal the exact accepted row bytes");

        // 2. First shutdown — must return within the drain deadline (no hang).
        Method shutdown = IngestionService.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        long startNanos = System.nanoTime();
        shutdown.invoke(service);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        // Drain deadline 2 s + append timeout 1 s + journal/close overhead.
        assertTrue(elapsedMs < 15_000,
                "shutdown must return within the drain deadline, not hang (took " + elapsedMs + " ms)");

        // 3. The journal has EXACTLY one entry, recording the exact remaining
        //    pending bytes (R-260) — written before the drain.
        List<String> lines = Files.readAllLines(journalPath);
        assertEquals(1, lines.size(), "first shutdown writes exactly one journal entry");
        JsonNode entry = MAPPER.readTree(lines.get(0));
        assertEquals(3, entry.get("pending_records").asLong(), "journal pins the pending record count");
        assertEquals(expectedPendingBytes, entry.get("pending_bytes").asLong(),
                "journal pins the exact remaining pending bytes (R-260)");
        assertEquals("shutdown", entry.get("reason").asText());

        // 4. The drain released the exact bytes — tracker does not leak.
        assertEquals(0, service.tracker().pendingBytes(), "drain must release the exact pending bytes");
        assertTrue(service.tracker().totalFailed() >= 1, "R-260: the un-flushable pending is recorded as failed");

        // 5. Second shutdown is a no-op — still exactly one journal entry.
        shutdown.invoke(service);
        assertEquals(1, Files.readAllLines(journalPath).size(),
                "duplicate shutdown must not write a second journal entry");
    }

    // ---- fixtures (mirror IngestionNoSilentDropTest) ----

    private static IngestionConfig buildConfig(Path journalPath) throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        env.put("UNCERTAINTY_JOURNAL_PATH", journalPath.toString());
        env.put("DRAIN_DEADLINE_SECONDS", "2");   // provable deadline instead of 30 s
        env.put("APPEND_TIMEOUT_SECONDS", "1");   // per-attempt timeout still fires (cancel is a no-op)
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static List<com.trading.ingestion.model.Instrument> instruments() {
        return List.of(new com.trading.ingestion.model.Instrument.Builder()
                .instrumentToken(TOKEN_A).tradingSymbol("SYM1-EQ")
                .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build());
    }

    private static final byte[] FRAME_PAYLOAD =
            new byte[] {0x01, 0x02, 0x03, 0x04, (byte) 0xFF, 0x00, 0x10};

    private static com.trading.ingestion.transport.TickEvent protoTick(long token, long tsMs, long ltpPaise) {
        return com.trading.ingestion.transport.TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("ltpc")
                .setToken((int) token)
                .setFeed("hft")
                .setTsMs(tsMs)
                .setReceivedMs(System.currentTimeMillis())
                .setFeedSequenceLocal(1)
                .setLtpPaise(ltpPaise)
                .setVolume(100)
                .setRawPayload(com.google.protobuf.ByteString.copyFrom(FRAME_PAYLOAD))
                .setPayloadHash(com.google.protobuf.ByteString.copyFrom(sha256Hex(FRAME_PAYLOAD).getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .build();
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Fake converter whose ack future never completes and ignores cancellation. */
    static final class StuckConverter implements FlussRowConverter {
        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(
                com.trading.ingestion.model.TickPacket packet) {
            return new UncancelableFuture<>();
        }

        @Override
        public int estimatedRowSize(com.trading.ingestion.model.TickPacket packet) {
            return 256;
        }

        @Override
        public void close() {
        }
    }

    /**
     * A CompletableFuture whose {@code cancel()} is a no-op — the writer's
     * per-attempt timeout calls {@code cancel(true)}, and a real wedge
     * (un-acking Fluss) would leave the append in-flight the same way: the
     * tracker reservation stays until the drain deadline.
     */
    static final class UncancelableFuture<T> extends CompletableFuture<T> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false; // never completes, never cancels — ack is lost forever
        }
    }

    // ---- no-op evidence sinks (ING-DQ-010 seam) ----

    private static QuarantineSink noopQuarantine() {
        return new QuarantineSink() {
            @Override
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
            }

            @Override
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                              Long instrumentToken, String exchange, String symbol) {
            }

            @Override
            public void close() {
            }
        };
    }

    private static DiscontinuitySink noopDiscontinuity() {
        return new DiscontinuitySink() {
            @Override
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before) {
            }

            @Override
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before,
                              Long instrumentToken, String exchange, String symbol) {
            }

            @Override
            public void writeBridgeEvent(com.trading.ingestion.bridge.BridgeEvent event,
                                         DiscontinuityWriter.LastTickSnapshot before) {
            }

            @Override
            public void close() {
            }
        };
    }

    private static SafetySink noopSafety() {
        return new SafetySink() {
            @Override
            public String write(String slotId, long connectionEpoch,
                                SafetyHaltWriter.SafetyState state,
                                SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                                String evidenceReference, long detectedTsMs) {
                return "noop";
            }

            @Override
            public void close() {
            }
        };
    }
}
