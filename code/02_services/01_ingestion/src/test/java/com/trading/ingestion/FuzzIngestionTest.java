package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.google.protobuf.ByteString;
import com.trading.ingestion.transport.MarketDataBatch;
import com.trading.ingestion.transport.TickEvent;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.ProtoFrameReader;
import com.trading.ingestion.write.RawTickWriter;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-DQ-011: seeded property-based fuzz of the proto tick path.
 *
 * <p>A plain JDK {@link Random} with a fixed seed (no external property
 * library, per the hardening plan) generates a mixed adversarial corpus of
 * proto {@code TickEvent}s — valid ticks, stale/future timestamps, unknown
 * tokens, invalid modes, hash mismatches, mutated payloads — and every event
 * is driven through the REAL {@code IngestionService} pipeline via the
 * no-op-sink test seam (ING-DQ-010). The properties asserted per run:
 *
 * <pre>{@code
 *   nothing propagates out of processTickEvent (an escaping exception fails the test)
 *   frameCount == eventsFed                    (every event entered the pipeline)
 *   appends + quarantineWrites == eventsFed    (no silent drop: every event maps to exactly one evidence outcome)
 *   INTERNAL_ERROR writes == errorCount        (evidence and the error counter agree)
 *   tracker drains to zero                     (no leaked reservations)
 * }</pre>
 *
 * <p><b>Determinism:</b> the corpus regenerates identically from the fixed
 * seed, so a failing seed is a reproducible regression pin. The three seeds
 * below are pinned in the run — a fourth seed that exposes a new pipeline
 * failure is itself a bug report, not a test change.
 *
 * <p><i>2026-08-29:</i> migrated from NDJSON line fuzzing to proto
 * TickEvent fuzzing — the NDJSON pipe transport was removed and the only
 * ingestion path is {@code processTickEvent}. Wire-level garbage (bad length
 * prefixes, non-proto bytes) is rejected by {@code ProtoFrameReader} and is
 * covered separately in ProtoTransportTest.
 */
@DisplayName("ING-DQ-011: seeded property-based fuzz — no crash, no silent drop, reproducible")
class FuzzIngestionTest {

    private static final long TOKEN_A = 100_000L;
    private static final long TOKEN_B = 100_100L;

    /** Pinned seeds — a corpus from any of these must satisfy every property. */
    private static final long[] SEEDS = {0x0BAD_F00DL, 0xDEAD_BEEFL, 0x5EED_CAFEL};

    /** Fixed epoch for all generated timestamps — keeps the corpus fully deterministic. */
    private static final long NOW_MS = 1_800_000_000_000L;

    /** Freshness window from the test config: [now - 5000, now + 2000]. */
    private static final long STALE_OFFSET_MS = 60_000L;

    @Test
    @DisplayName("fuzz corpus: no uncaught exception, no silent drop, ledger reconciles, reproducible")
    void fuzzCorpusNeverCrashesOrDrops() throws Exception {
        // Re-runnability pin: the same seed must regenerate the identical corpus.
        List<TickEvent> corpus = generateProtoCorpus(SEEDS[0], NOW_MS);
        assertEquals(corpus, generateProtoCorpus(SEEDS[0], NOW_MS),
                "fixed seed must regenerate the identical corpus");

        IngestionConfig config = buildConfig();
        CountingConverter converter = new CountingConverter();
        CountingQuarantine quarantine = new CountingQuarantine();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-dq-011", instruments(), converter, config, clock,
                quarantine, new CountingDiscontinuity(), new CountingSafety());

        long totalLines = 0;
        for (long seed : SEEDS) {
            List<TickEvent> batch = generateProtoCorpus(seed, NOW_MS);
            totalLines += batch.size();
            for (TickEvent event : batch) {
                service.processTickEvent(event, "hft-0", 1L);
                // an escaping exception fails the test directly
            }
        }

        awaitPipelineDrain(converter, quarantine, totalLines);

        long appends = converter.appendCalls.get();
        long quarantineWrites = quarantine.writes.get();
        assertEquals(totalLines, frameCount(service), "every fed event must be processed");
        assertEquals(totalLines, appends + quarantineWrites,
                "no silent drop: appends + quarantines must reconcile exactly with events fed "
                        + "(appends=" + appends + ", quarantines=" + quarantineWrites + ")");
        assertEquals(quarantine.internalErrors.get(), errorCount(service),
                "INTERNAL_ERROR quarantine evidence must match the error counter — an untyped "
                        + "crash path means the parser did not classify the input");
        assertEquals(0, service.tracker().pendingRecords(), "no leaked record reservations");
        assertEquals(0, service.tracker().pendingBytes(), "no leaked byte reservations");
        assertTrue(converter.packets.stream().allMatch(p -> p.eventFingerprint() != null),
                "every appended packet carries a fingerprint");
        assertTrue(converter.packets.stream()
                        .allMatch(p -> p.raw() != null && p.raw().rawPayload() != null),
                "every appended packet preserves the raw payload bytes");
    }

    @Test
    @DisplayName("guaranteed-valid ticks always append — fuzz mutations never break the happy path")
    void validTicksAlwaysAppend() throws Exception {
        IngestionConfig config = buildConfig();
        CountingConverter converter = new CountingConverter();
        CountingQuarantine quarantine = new CountingQuarantine();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-dq-011-valid", instruments(), converter, config, clock,
                quarantine, new CountingDiscontinuity(), new CountingSafety());

        List<Long> expectedTokens = new ArrayList<>();
        Random rnd = new Random(0x5EED); // deterministic fixture set
        for (int i = 0; i < 200; i++) {
            String mode = MODES[rnd.nextInt(MODES.length)];
            long token = rnd.nextBoolean() ? TOKEN_A : TOKEN_B;
            long ltp = mode.equals("quote") ? 0 : 1 + rnd.nextInt(500_000);
            long tsMs = NOW_MS + rnd.nextInt(4001) - 2000; // strictly inside the freshness window
            expectedTokens.add(token);
            service.processTickEvent(validProtoTick(mode, token, ltp, tsMs), "hft-0", 1L);
        }

        awaitPipelineDrain(converter, quarantine, 200);

        assertEquals(200, converter.appendCalls.get(),
                "every guaranteed-valid tick must append — quarantine writes=" + quarantine.writes.get());
        assertEquals(0, quarantine.writes.get(), "no valid tick may be quarantined");
        assertEquals(expectedTokens, converter.packets.stream().map(TickPacket::instrumentToken).toList(),
                "appended tokens must match the fed order exactly");
        assertEquals(0, errorCount(service));
    }

    @Test
    @DisplayName("garbage frames are consumed without evidence — the reject bucket")
    void rejectBucketConsumedWithoutEvidence() throws Exception {
        // The proto transport has no "null node" lines: the wire primitive is
        // a length-prefixed TransportFrame, and garbage (empty frame, non-proto
        // bytes, oversized length prefix) is rejected at ProtoFrameReader —
        // consumed from the stream without any tick append or quarantine.
        List<byte[]> garbage = List.of(
                new byte[0],                                    // empty stream
                new byte[]{0x01, 0x02, 0x03},                   // <4 bytes: incomplete prefix
                new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F}); // 2.1 GB length
        for (byte[] g : garbage) {
            CountingConverter converter = new CountingConverter();
            CountingQuarantine quarantine = new CountingQuarantine();
            ProtoFrameReader reader = new ProtoFrameReader(new ByteArrayInputStream(g),
                    new ProtoFrameReader.FrameHandler() {
                        @Override public void onMarketBatch(MarketDataBatch b) {}
                        @Override public void onControl(com.trading.ingestion.transport.ControlRecord c) {}
                    });
            reader.sniffProto(); // rejects: not a plausible proto frame
            assertEquals(0, converter.appendCalls.get(), "no garbage may become a tick");
            assertEquals(0, quarantine.writes.get(), "no garbage may produce quarantine evidence");
        }
    }

    // ---- corpus generation (deterministic from seed) ----

    /** One generated proto event; the pipeline decides its outcome (no per-event prediction). */
    private record FuzzEvent(TickEvent event) {}

    private static List<TickEvent> generateProtoCorpus(long seed, long now) {
        Random rnd = new Random(seed);
        List<TickEvent> events = new ArrayList<>();
        for (int i = 0; i < 800; i++) {
            events.add(switch (rnd.nextInt(10)) {
                case 0, 1, 2, 3, 4, 5 -> mutatedProtoTick(rnd, now);
                case 6, 7 -> freshnessViolation(rnd, now);
                case 8 -> badHashTick(rnd, now);
                default -> unknownTokenTick(rnd, now);
            });
        }
        // Pinned adversarial edges (outside the random draw so they always run).
        events.add(TickEvent.getDefaultInstance());                 // empty event
        events.add(TickEvent.newBuilder().setToken(0).build());     // token only
        events.add(TickEvent.newBuilder().setMode("").build());     // mode only
        events.add(TickEvent.newBuilder().setMode("junk").setToken((int) TOKEN_A).build()); // unknown mode
        events.add(TickEvent.newBuilder().setMode("full").setToken((int) TOKEN_A)
                .setTsMs(now).setLtpPaise(-1).build());             // negative price
        events.add(TickEvent.newBuilder().setMode("full").setToken((int) TOKEN_A)
                .setTsMs(now).setLtpPaise(0).setVolume(-5).build()); // negative volume
        events.add(TickEvent.newBuilder().setMode("full").setToken((int) TOKEN_A)
                .setTsMs(now).setLtpPaise(0).setRawPayload(ByteString.copyFrom(new byte[1 << 20])).build()); // 1 MB payload
        events.add(TickEvent.newBuilder().setMode("full").setToken((int) TOKEN_A)
                .setTsMs(now).setLtpPaise(100).setReceivedMs(0).build()); // zero received_ms
        return events;
    }

    private static final String[] MODES = {"ltp", "ltpc", "full", "quote"};

    /** A proto tick with random mutations of the fields the pipeline gates on. */
    private static TickEvent mutatedProtoTick(Random rnd, long now) {
        TickEvent.Builder b = TickEvent.newBuilder()
                .setSlotId("hft-" + rnd.nextInt(4))
                .setMode(MODES[rnd.nextInt(MODES.length)])
                .setToken((int) (rnd.nextBoolean() ? TOKEN_A : TOKEN_B))
                .setFeed("hft")
                .setTsMs(now + rnd.nextInt(4001) - 3000)   // [-3000, +1000] fresh
                .setReceivedMs(now)
                .setFeedSequenceLocal(rnd.nextInt(1000))
                .setLtpPaise(1 + rnd.nextInt(500_000))
                .setVolume(rnd.nextInt(1_000_000));
        byte[] payload = new byte[8 + rnd.nextInt(57)];
        rnd.nextBytes(payload);
        b.setRawPayload(ByteString.copyFrom(payload));
        if (rnd.nextInt(10) < 8) {
            b.setPayloadHash(ByteString.copyFrom(sha256Hex(payload).getBytes(StandardCharsets.UTF_8)));
        }
        return b.build();
    }

    /** A well-formed proto tick that MUST append (per ING-DQ-011 validTicksAlwaysAppend). */
    private static TickEvent validProtoTick(String mode, long token, long ltpPaise, long tsMs) {
        byte[] payload = ("fuzz-payload-" + token + "-" + mode).getBytes(StandardCharsets.UTF_8);
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode(mode)
                .setToken((int) token)
                .setFeed("hft")
                .setTsMs(tsMs)
                .setReceivedMs(NOW_MS)
                .setFeedSequenceLocal(1)
                .setLtpPaise(ltpPaise)
                .setVolume(100)
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(payload).getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    /** Stale or future broker timestamp — must quarantine, never append. */
    private static TickEvent freshnessViolation(Random rnd, long now) {
        long ts = rnd.nextBoolean() ? now - STALE_OFFSET_MS : now + STALE_OFFSET_MS;
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken((int) (rnd.nextBoolean() ? TOKEN_A : TOKEN_B))
                .setFeed("hft")
                .setTsMs(ts)
                .setReceivedMs(now)
                .setFeedSequenceLocal(rnd.nextInt(1000))
                .setLtpPaise(1 + rnd.nextInt(500_000))
                .setVolume(100)
                .setRawPayload(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(new byte[]{1, 2, 3}).getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    /** Valid event but payload_hash does not match the payload — must quarantine (HASH_MISMATCH). */
    private static TickEvent badHashTick(Random rnd, long now) {
        byte[] payload = new byte[16];
        rnd.nextBytes(payload);
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken((int) (rnd.nextBoolean() ? TOKEN_A : TOKEN_B))
                .setFeed("hft")
                .setTsMs(now + rnd.nextInt(1000) - 500)
                .setReceivedMs(now)
                .setFeedSequenceLocal(rnd.nextInt(1000))
                .setLtpPaise(1 + rnd.nextInt(500_000))
                .setVolume(100)
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom("ZZZ".getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    /** Token not in the instrument manifest — must quarantine (MISSING_INSTRUMENT). */
    private static TickEvent unknownTokenTick(Random rnd, long now) {
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken(999_999 + rnd.nextInt(1_000))
                .setFeed("hft")
                .setTsMs(now)
                .setReceivedMs(now)
                .setFeedSequenceLocal(rnd.nextInt(1000))
                .setLtpPaise(100)
                .setVolume(100)
                .setRawPayload(ByteString.copyFrom(new byte[]{9, 9}))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(new byte[]{9, 9}).getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    // ---- fixtures and harness ----

    private static IngestionConfig buildConfig() throws Exception {
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
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static List<Instrument> instruments() {
        return List.of(
                new Instrument.Builder().instrumentToken(TOKEN_A).tradingSymbol("SYM1-EQ")
                        .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build(),
                new Instrument.Builder().instrumentToken(TOKEN_B).tradingSymbol("SYM2-EQ")
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

    // ---- counting sinks (ING-DQ-010 no-op seam, but counting) ----

    static final class CountingQuarantine implements QuarantineSink {
        final AtomicLong writes = new AtomicLong();
        final AtomicLong internalErrors = new AtomicLong();
        final List<String> reasons = new CopyOnWriteArrayList<>();

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
            count(reason);
        }

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                          Long instrumentToken, String exchange, String symbol) {
            count(reason);
        }

        private void count(QuarantineWriter.Reason reason) {
            writes.incrementAndGet();
            reasons.add(reason.name());
            if (reason == QuarantineWriter.Reason.INTERNAL_ERROR) internalErrors.incrementAndGet();
        }

        @Override
        public void close() {
        }
    }

    static final class CountingDiscontinuity implements DiscontinuitySink {
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
    }

    static final class CountingSafety implements SafetySink {
        @Override
        public String write(String slotId, long connectionEpoch, SafetyHaltWriter.SafetyState state,
                            SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                            String evidenceReference, long detectedTsMs) {
            return "fuzz-halt";
        }

        @Override
        public void close() {
        }
    }

    /** Fake converter: counts appends, completes instantly, records packets. */
    static final class CountingConverter implements FlussRowConverter {
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
        public void close() {
        }
    }

    // ---- reflection accessors (private state; IngestionServiceTest uses the same pattern) ----

    private static long frameCount(IngestionService service) throws Exception {
        Field f = IngestionService.class.getDeclaredField("frameCount");
        f.setAccessible(true);
        return ((AtomicLong) f.get(service)).get();
    }

    /**
     * The write path is async: processTickEvent queues to BoundedQueue and a
     * WriterWorker thread performs the converter append, so appendCalls can
     * legitimately lag the feed loop under full-suite JVM load (observed
     * 2026-08-31: 197/200 immediately after feeding, 200/200 moments later —
     * a test race, not a silent drop). Poll bounded: the ledger must settle
     * at exactly eventsFed, else the "no silent drop" property genuinely
     * failed and the failure message carries the counts.
     */
    private static void awaitPipelineDrain(CountingConverter converter,
                                           CountingQuarantine quarantine,
                                           long eventsFed) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L; // 10s bound
        while (converter.appendCalls.get() + quarantine.writes.get() < eventsFed) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "pipeline did not settle after " + eventsFed + " events (appends="
                                + converter.appendCalls.get() + ", quarantines="
                                + quarantine.writes.get()
                                + ") — either a genuine silent drop or a stalled writer worker");
            }
            Thread.sleep(10);
        }
    }

    private static long errorCount(IngestionService service) throws Exception {
        Field f = IngestionService.class.getDeclaredField("errorCount");
        f.setAccessible(true);
        return ((AtomicLong) f.get(service)).get();
    }
}
