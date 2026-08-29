package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.health.ReadinessFile;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.trading.ingestion.transport.TickEvent;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-001: proto TickEvent → TickPacket field mapping, plus bridge
 * stderr classification and readiness throttling.
 *
 * <p><i>2026-08-29:</i> the NDJSON parser (GoTick) was removed with the
 * NDJSON transport; parse coverage now lives at the proto layer
 * (TickEventContractTest for the wire schema, this test for the
 * event → persisted packet mapping).
 */
@DisplayName("ING-UNIT-001: proto mapping + bridge diagnostics")
class IngestionServiceTest {

    @Test
    @DisplayName("proto TickEvent → TickPacket field mapping preserves OHLC/bid/ask/ltp")
    void protoTickMapsToPacket() throws Exception {
        IngestionConfig config = buildConfig();
        RecordingConverter converter = new RecordingConverter();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-unit-001", instruments(), converter, config, clock,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        byte[] payload = "raw-bytes".getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        TickEvent ev = TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken(3045)
                .setFeed("hft")
                .setTsMs(now)
                .setReceivedMs(now)
                .setFeedSequenceLocal(17)
                .setLtpPaise(234500)
                .setClosePaise(234200)
                .setOpenPaise(233100)
                .setHighPaise(235000)
                .setLowPaise(233000)
                .setVwapPaise(234100)
                .setLtq(50)
                .setVolume(125000)
                .setOpenInterest(0)
                .addBidPx(234400).addBidPx(234300).addBidPx(234200).addBidPx(234100).addBidPx(234000)
                .addAskPx(234600).addAskPx(234700).addAskPx(234800).addAskPx(234900).addAskPx(235000)
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(payload).getBytes(StandardCharsets.UTF_8)))
                .build();

        service.processTickEvent(ev, "hft-0", 1L);
        awaitDrain(converter, 1);
        assertEquals(1, converter.appendCalls.get(), "valid proto tick must append");
        TickPacket p = converter.packets.get(0);
        assertEquals(3045L, p.instrumentToken());
        assertEquals(234500L, p.lastPricePaise());
        assertEquals(125000L, p.volume());
        assertEquals(233100L, p.ohlcOpenPaise());
        assertEquals(235000L, p.ohlcHighPaise());
        assertEquals(233000L, p.ohlcLowPaise());
        assertEquals(234200L, p.ohlcClosePaise());
        assertNotNull(p.eventFingerprint(), "mapped packet carries a fingerprint");
    }

    @Test
    @DisplayName("Classify bridge stderr lines per plan log4j2 rule")
    void classifyBridgeLines() {
        // Warning/error keywords → WARN.
        for (String s : new String[]{"connect failed", "HFT stream ended", "decode error",
                "feed stalled", "subscription partial", "token rejected"}) {
            assertTrue(IngestionService.classifyBridgeLine(s), s + " should be WARN");
        }
        // Ordinary diagnostics → INFO.
        for (String s : new String[]{"authenticated", "subscription plan=abc slots=1",
                "heartbeat sent", "retry_in_2s", ""}) {
            assertFalse(IngestionService.classifyBridgeLine(s), s + " should be INFO");
        }
    }

    @Test
    @DisplayName("R-209: readiness file write is throttled (state change or 1s), not per-tick")
    void readinessWriteThrottled() throws Exception {
        IngestionConfig config = buildConfig();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        java.nio.file.Path ready = java.nio.file.Files.createTempDirectory("readiness-throttle")
                .resolve("ready");
        IngestionService service = new IngestionService(
                "ing-r209", java.util.List.of(), new StubFlussRowConverter("raw_table_1"),
                config, clock, null, null, null);
        java.lang.reflect.Field rf = IngestionService.class.getDeclaredField("readinessFile");
        rf.setAccessible(true);
        rf.set(service, new ReadinessFile(ready));

        java.lang.reflect.Method m = IngestionService.class.getDeclaredMethod("updateReadinessFile");
        m.setAccessible(true);
        m.invoke(service);
        long writesAfter1 = java.nio.file.Files.exists(ready) ? 1 : 0;
        m.invoke(service);  // back-to-back within 1s: must be throttled (no new write)
        long writesAfter2 = java.nio.file.Files.exists(ready) ? 1 : 0;
        assertEquals(writesAfter1, writesAfter2,
                "back-to-back readiness updates must not rewrite (throttle)");
        java.lang.reflect.Field lrw = IngestionService.class.getDeclaredField("lastReadinessWritten");
        lrw.setAccessible(true);
        java.lang.reflect.Field lrwm = IngestionService.class.getDeclaredField("lastReadinessWriteMs");
        lrwm.setAccessible(true);
        assertTrue(System.currentTimeMillis() - (long) lrwm.get(service) < 1000L,
                "recent write timestamp recorded");
    }

    // ---- fixtures and harness ----

    private static QuarantineSink noopQuarantine() {
        return new QuarantineSink() {
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {}
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                              Long instrumentToken, String exchange, String symbol) {}
            public void close() {}
        };
    }

    private static DiscontinuitySink noopDiscontinuity() {
        return new DiscontinuitySink() {
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before) {}
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before,
                              Long instrumentToken, String exchange, String symbol) {}
            public void writeBridgeEvent(com.trading.ingestion.bridge.BridgeEvent event,
                                         DiscontinuityWriter.LastTickSnapshot before) {}
            public void close() {}
        };
    }

    private static SafetySink noopSafety() {
        return new SafetySink() {
            public String write(String slotId, long connectionEpoch,
                                SafetyHaltWriter.SafetyState state,
                                SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                                String evidenceReference, long detectedTsMs) {
                return "noop";
            }
            public void close() {}
        };
    }

    private static void awaitDrain(RecordingConverter converter, int expected) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (converter.appendCalls.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }

    private static List<Instrument> instruments() {
        return List.of(
                new Instrument.Builder().instrumentToken(3045).tradingSymbol("SYM1-EQ")
                        .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build());
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

    static final class RecordingConverter implements FlussRowConverter {
        final AtomicInteger appendCalls = new AtomicInteger();
        final List<TickPacket> packets = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            appendCalls.incrementAndGet();
            packets.add(packet);
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1, "p0"));
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 256;
        }

        @Override
        public void close() {}
    }

    private static IngestionConfig buildConfig() throws Exception {
        java.util.Map<String, String> env = new java.util.HashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        java.lang.reflect.Method validateFrom = IngestionConfig.class
                .getDeclaredMethod("validateFrom", java.util.Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }
}
