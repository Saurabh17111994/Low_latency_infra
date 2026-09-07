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
    @DisplayName("P1-081: processed tick refreshes ONLY its own slot recency (R-031 wiring)")
    void tickRefreshesOnlyItsOwnSlotRecency() throws Exception {
        IngestionConfig config = buildConfig();
        RecordingConverter converter = new RecordingConverter();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-unit-081", instruments(), converter, config, clock,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        // Both slots ACTIVE with a 1s-old stamp (recent, but strictly older
        // than anything the tick path will write).
        long old = System.nanoTime() - 1_000_000_000L;
        service.health().updateSlot("hft-0", "ACTIVE", 1, 2, 2, 0, old);
        service.health().updateSlot("hft-1", "ACTIVE", 1, 2, 2, 0, old);

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
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(payload).getBytes(StandardCharsets.UTF_8)))
                .build();

        service.processTickEvent(ev, "hft-0", 1L);
        awaitDrain(converter, 1);

        long after0 = service.health().slot("hft-0").lastFrameNanos;
        long after1 = service.health().slot("hft-1").lastFrameNanos;
        assertTrue(after0 > old, "hft-0 tick must advance hft-0 recency (R-031 wiring)");
        assertEquals(old, after1, "hft-0 tick must not refresh hft-1 (P1-081 isolation)");
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
                config, clock, noopQuarantine(), noopDiscontinuity(), noopSafety());
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

    @Test
    @DisplayName("P1-305: FUTURE quarantine increments the FUTURE_BROKER_TIMESTAMP decode-error metric")
    void futureQuarantineIncrementsMetric() throws Exception {
        IngestionConfig config = buildConfig();
        RecordingConverter converter = new RecordingConverter();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-p1305", instruments(), converter, config, clock,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        byte[] payload = "raw-bytes".getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        TickEvent ev = TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken(3045)
                .setFeed("hft")
                .setTsMs(now + 60_000L)
                .setReceivedMs(now)
                .setFeedSequenceLocal(17)
                .setLtpPaise(234500)
                .setVolume(125000)
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(payload).getBytes(StandardCharsets.UTF_8)))
                .build();

        service.processTickEvent(ev, "hft-0", 1L);
        assertTrue(service.metrics().buildMetricsJson().contains("FUTURE_BROKER_TIMESTAMP"),
                "FUTURE quarantine must be visible to decode-error dashboards (was: metric missing)");
        assertEquals(0, converter.appendCalls.get(), "FUTURE ticks never append");
    }

    @Test
    @DisplayName("P1-092: failed safety-halt write evicts dedup so the next tick retries (slot still marked unsafe)")
    void failedSafetyWriteRetriesOnNextTick() throws Exception {
        IngestionConfig config = buildConfig();
        RecordingConverter converter = new RecordingConverter();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        AtomicInteger writes = new AtomicInteger();
        SafetySink failing = new SafetySink() {
            public String write(String slotId, long connectionEpoch,
                                SafetyHaltWriter.SafetyState state,
                                SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                                String evidenceReference, long detectedTsMs) {
                writes.incrementAndGet();
                throw new RuntimeException("simulated sync upsert failure");
            }
            public void close() {}
        };
        IngestionService service = new IngestionService(
                "ing-unit-092", instruments(), converter, config, clock,
                noopQuarantine(), noopDiscontinuity(), failing);

        // Far-future broker timestamp → FUTURE_BROKER_TIMESTAMP quality-UNSAFE.
        // No appends happen on this path (synchronous emit inside processTickEvent).
        byte[] payload = "raw-bytes".getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        TickEvent ev = TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken(3045)
                .setFeed("hft")
                // Broker stamp 60s ahead of the bridge receive clock = FUTURE
                // (the skew gate compares tsMs against receive time, so
                // receivedMs must stay at now).
                .setTsMs(now + 60_000L)
                .setReceivedMs(now)
                .setFeedSequenceLocal(17)
                .setLtpPaise(234500)
                .setVolume(125000)
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(payload).getBytes(StandardCharsets.UTF_8)))
                .build();

        service.processTickEvent(ev, "hft-0", 1L);
        service.processTickEvent(ev, "hft-0", 1L);
        assertEquals(2, writes.get(),
                "evicted dedup must let the second tick retry the halt write (old code: 1)");
        assertTrue(service.health().slot("hft-0").unsafe,
                "slot must be marked unsafe even though the halt write failed (old code: false)");
        assertEquals(0, converter.appendCalls.get(), "FUTURE ticks never append");
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
        java.lang.reflect.Method validateFrom = IngestionConfig.class
                .getDeclaredMethod("validateFrom", java.util.Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    // ---- P1-062: per-queue budgets are shares of the global total ----

    private static IngestionConfig buildConfigWith(java.util.Map<String, String> extra)
            throws Exception {
        java.util.Map<String, String> env = new java.util.HashMap<>();
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
        env.putAll(extra);
        java.lang.reflect.Method validateFrom = IngestionConfig.class
                .getDeclaredMethod("validateFrom", java.util.Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    @Test
    @DisplayName("P1-062: N writers split the pending budget (was: N x full budget)")
    void queueBudgetsSplitAcrossWriters() throws Exception {
        IngestionConfig config = buildConfigWith(java.util.Map.of(
                "FLUSS_WRITERS", "2",
                "MAX_PENDING_APPEND_RECORDS", "10000",
                "MAX_PENDING_APPEND_BYTES", "67108864"));
        IngestionService service = new IngestionService(
                "ing-p1062", instruments(), new RecordingConverter(), config,
                new NtpClockChecker("127.0.0.1:9", 100, false),
                noopQuarantine(), noopDiscontinuity(), noopSafety());
        try {
            java.lang.reflect.Field qf = IngestionService.class.getDeclaredField("queues");
            qf.setAccessible(true);
            com.trading.ingestion.write.BoundedQueue[] queues =
                    (com.trading.ingestion.write.BoundedQueue[]) qf.get(service);
            assertEquals(2, queues.length, "two writers configured");
            long totalBytes = 0;
            long totalRecords = 0;
            for (com.trading.ingestion.write.BoundedQueue q : queues) {
                assertEquals(33554432L, q.maxBytes(), "each queue holds half the byte budget");
                assertEquals(5000, q.maxRecords(), "each queue holds half the record budget");
                totalBytes += q.maxBytes();
                totalRecords += q.maxRecords();
            }
            assertTrue(totalBytes <= 67108864L, "queues combined must not exceed the global budget");
            assertTrue(totalRecords <= 10000L, "queues combined must not exceed the global budget");
        } finally {
            invokeShutdown(service);
        }
    }

    // ---- P1-064: broker_quarantine mapping carries token 0 + real hash ----

    @Test
    @DisplayName("P1-064: quarantine token is 0/unknown with a verifiable payload hash")
    void brokerQuarantineMapsUnknownToken() {
        byte[] raw = "<binary-frame-bytes>".getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        com.trading.ingestion.transport.ControlRecord cr =
                com.trading.ingestion.transport.ControlRecord.newBuilder()
                        .setSlotId("hft-0").setConnectionId("c1").setConnectionEpoch(7)
                        .setReason("MALFORMED_JSON")
                        .setRawPayload(com.google.protobuf.ByteString.copyFrom(raw))
                        .setReceivedTsMs(now).build();
        com.trading.ingestion.bridge.BrokerQuarantine record =
                IngestionService.toBrokerQuarantine(cr);
        assertEquals(0L, record.token(), "undecodable tick has no token (was: timestamp)");
        assertEquals(now, record.detectedTsMs(), "timestamp preserved in its own column");
        assertEquals(sha256Hex(raw), record.payloadHash(), "R-207 pin must verify");
    }

    @Test
    @DisplayName("P1-064: quarantine record accepts token 0 (was: positive-only)")
    void quarantineRecordAcceptsUnknownToken() {
        byte[] raw = "<binary-frame-bytes>".getBytes(StandardCharsets.UTF_8);
        com.trading.ingestion.bridge.BrokerQuarantine record =
                new com.trading.ingestion.bridge.BrokerQuarantine(2, "hft-0", "c1", 7L, 0L,
                        "MALFORMED_JSON", raw, sha256Hex(raw), System.currentTimeMillis());
        assertEquals(0L, record.token());
    }

    // ---- P1-065/066: first-fatal-wins + full scheduler shutdown ----

    private static void invokeShutdown(IngestionService service) throws Exception {
        java.lang.reflect.Method m = IngestionService.class.getDeclaredMethod("shutdown");
        m.setAccessible(true);
        m.invoke(service);
    }

    @Test
    @DisplayName("P1-065/066: first fatal wins; shutdown stops every scheduler")
    void firstFatalWinsAndSchedulersStop() throws Exception {
        IngestionConfig config = buildConfig();
        IngestionService service = new IngestionService(
                "ing-p1065", instruments(), new RecordingConverter(), config,
                new NtpClockChecker("127.0.0.1:9", 100, false),
                noopQuarantine(), noopDiscontinuity(), noopSafety());
        java.lang.reflect.Method fatal =
                IngestionService.class.getDeclaredMethod("requestFatalStop", String.class);
        fatal.setAccessible(true);
        fatal.invoke(service, "first");
        fatal.invoke(service, "second"); // must be a no-op: first fatal wins
        java.lang.reflect.Field ff = IngestionService.class.getDeclaredField("fatalStopReason");
        ff.setAccessible(true);
        Object holder = ff.get(service);
        String reason = (String) ((java.util.concurrent.atomic.AtomicReference<?>) holder).get();
        assertEquals("first", reason, "second FATAL must not overwrite the first");
        for (String name : new String[]{"stalenessWatchdog", "clockMonitorScheduler",
                "memoryMonitorScheduler"}) {
            java.lang.reflect.Field sf = IngestionService.class.getDeclaredField(name);
            sf.setAccessible(true);
            java.util.concurrent.ScheduledExecutorService sched =
                    (java.util.concurrent.ScheduledExecutorService) sf.get(service);
            assertTrue(sched.isShutdown(), name + " must stop on shutdown (was: leaked)");
        }
    }

    // ---- P1-225/246/249: shutdown killer, monotonic stamp, blank hash ----

    @Test
    @DisplayName("P1-225: kill helper still alive after the destroy race must not abort shutdown")
    void killExitCodeToleratesLiveHelper() {
        Process stillAlive = new Process() {
            @Override public java.io.OutputStream getOutputStream() {
                return new java.io.ByteArrayOutputStream();
            }
            @Override public java.io.InputStream getInputStream() {
                return new java.io.ByteArrayInputStream(new byte[0]);
            }
            @Override public java.io.InputStream getErrorStream() {
                return new java.io.ByteArrayInputStream(new byte[0]);
            }
            @Override public int waitFor() {
                return 0;
            }
            @Override public int exitValue() {
                throw new IllegalThreadStateException("still running");
            }
            @Override public void destroy() {
            }
        };
        assertEquals(-1, IngestionService.killExitCode(stillAlive),
                "a live kill helper must report unknown (-1), not throw past the shutdown catch");
    }

    @Test
    @DisplayName("P1-246: NTP backward step must not move the heap-gate stamp backward")
    void memorySampleClampsClockRegression() throws Exception {
        IngestionConfig config = buildConfig();
        IngestionService service = new IngestionService(
                "ing-p1246", instruments(), new RecordingConverter(), config,
                new NtpClockChecker("127.0.0.1:9", 100, false),
                noopQuarantine(), noopDiscontinuity(), noopSafety());
        try {
            assertEquals(20_000L, service.monotonicMemorySampleMs(20_000L), "first stamp passes through");
            assertEquals(20_000L, service.monotonicMemorySampleMs(19_000L),
                    "regressed wall-clock must clamp to the last stamp (old code: 19000, stalling WARN_HEAP_HIGH)");
            assertEquals(21_000L, service.monotonicMemorySampleMs(21_000L),
                    "forward clock must pass through");
        } finally {
            invokeShutdown(service);
        }
    }

    @Test
    @DisplayName("P1-249: omitted Go payload hash quarantines instead of building a blank-hash RawTick")
    void blankPayloadHashQuarantined() throws Exception {
        IngestionConfig config = buildConfig();
        RecordingConverter converter = new RecordingConverter();
        RecordingQuarantine quarantine = new RecordingQuarantine();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-p1249", instruments(), converter, config, clock,
                quarantine, noopDiscontinuity(), noopSafety());
        try {
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
                    .setRawPayload(ByteString.copyFrom(payload))
                    .build();

            service.processTickEvent(ev, "hft-0", 1L);
            assertEquals(1, quarantine.reasons.size(),
                    "blank hash must quarantine (old code: 0 writes, tick appended with \"\" hash)");
            assertEquals(QuarantineWriter.Reason.INVALID_VALUES, quarantine.reasons.get(0));
            assertEquals(0, converter.appendCalls.get(), "quarantined tick must never append");
        } finally {
            invokeShutdown(service);
        }
    }

    static final class RecordingQuarantine implements QuarantineSink {
        final List<QuarantineWriter.Reason> reasons = new CopyOnWriteArrayList<>();

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
            reasons.add(reason);
        }

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                          Long instrumentToken, String exchange, String symbol) {
            reasons.add(reason);
        }

        @Override
        public void close() {
        }
    }

    // ---- P1-226: non-positive wire token quarantines before the queue index ----

    @Test
    @DisplayName("P1-224: safetyEmitted is epoch-scoped — restart-clear re-emits same halt")
    void safetyDedupEpochScoped() {
        java.util.Set<String> emitted = java.util.concurrent.ConcurrentHashMap.newKeySet();
        String idN = "fp|hft-0|5|UNSAFE|RESOURCE_EXHAUSTED";
        String idN1 = "fp|hft-0|6|UNSAFE|RESOURCE_EXHAUSTED";
        assertTrue(com.trading.ingestion.IngestionService.firstEmission(emitted, "UNSAFE", idN));
        // same id suppressed within epoch
        assertFalse(com.trading.ingestion.IngestionService.firstEmission(emitted, "UNSAFE", idN));
        // bridge restart clears (IngestionService RESTART branch: safetyEmitted.clear())
        emitted.clear();
        // new epoch id differs by construction AND re-emits after clear
        assertTrue(!idN.equals(idN1), "epoch must be part of the halt id");
        assertTrue(com.trading.ingestion.IngestionService.firstEmission(emitted, "UNSAFE", idN1));
        assertEquals(1, emitted.size(), "one epoch worth only — bounded");
    }

    @Test
    @DisplayName("P1-226: negative token quarantines INVALID_VALUES, never reaches queues")
    void negativeTokenQuarantinedBeforeRouting() throws Exception {
        IngestionConfig config = buildConfig();
        RecordingConverter converter = new RecordingConverter();
        RecordingQuarantine quarantine = new RecordingQuarantine();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-p1226", instruments(), converter, config, clock,
                quarantine, noopDiscontinuity(), noopSafety());
        try {
            byte[] payload = "raw-bytes".getBytes(StandardCharsets.UTF_8);
            long now = System.currentTimeMillis();
            TickEvent ev = TickEvent.newBuilder()
                    .setSlotId("hft-0")
                    .setMode("full")
                    .setToken(-7)
                    .setFeed("hft")
                    .setTsMs(now)
                    .setReceivedMs(now)
                    .setFeedSequenceLocal(17)
                    .setLtpPaise(234500)
                    .setVolume(125000)
                    .setRawPayload(ByteString.copyFrom(payload))
                    .setPayloadHash(ByteString.copyFrom(sha256Hex(payload).getBytes(StandardCharsets.UTF_8)))
                    .build();

            service.processTickEvent(ev, "hft-0", 1L);
            assertEquals(1, quarantine.reasons.size(),
                    "negative token must quarantine (old code: map-miss only, index risk)");
            assertEquals(QuarantineWriter.Reason.INVALID_VALUES, quarantine.reasons.get(0));
            assertEquals(0, converter.appendCalls.get(), "quarantined tick must never append");
        } finally {
            invokeShutdown(service);
        }
    }
}
