package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker 14 P6 (docs/08_implementation/04-signal-job.md §Absorbed documents) —
 * end-to-end harness for the replay contract, re-scoped 2026-08-13 (DEC-035 +
 * user requirement: candle tables are KV-only): candle KV + signal LOG/KV.
 *
 * <p>The harness runs the <b>actual production graph</b>
 * ({@link SignalJob#buildTopology(SignalJobConfig)}: Fluss source → raw
 * validation → fingerprint dedup → 15s event-time window → candle KV upsert
 * sink → breakout detection → signal dual-sink: {@code Signal_Candidates} LOG append
 * + {@code Signal_Candidates_current} KV upsert behind the canonical-signal
 * filter) inside a Flink
 * {@link MiniClusterWithClientResource} against <b>scratch Fluss tables</b>
 * (unique per test: {@code p6_<epoch>_raw|_log|_cand|_cur}, created and
 * dropped by this test). Production tables and the live dev signaljob are
 * never touched.
 *
 * <p>Feed (deterministic, event-time ordered): tokens 1000/1001 × 23 windows
 * × 4 ticks (window {@code w}: prices open=10000+w, high=10050+w,
 * low=9950+w, close=10000+w), plus:
 * <ul>
 *   <li>window 4: one extra tick appended after the window's own ticks but
 *       before the window closes — must fold into the accumulator
 *       (close(w4)=7004, arrival-order semantics);</li>
 *   <li>window 5: one tick whose event time is inside w5 appended AFTER the
 *       watermark passed w5.end+lateness — must be dropped, candle
 *       uncorrected (close(w5)=10005, tick_count=4);</li>
 *   <li>window 17: token 1000 gets only 3 ticks — its window must still close
 *       on the shared watermark advanced by token 1001 (idle bucket does not
 *       block window close);</li>
 *   <li>window 22: breakout candle (open=11000, close=15000 &gt;
 *       max(high of previous 20) = 10071) — fires exactly one candidate per
 *       token ({@code breakout-20-bullish-trend-<token>-<windowEnd>});</li>
 *   <li>window 23: a pusher tick advances the watermark past w22's end so the
 *       final breakout window closes;</li>
 *   <li>5 invalid rows on token 2000 (schema version "3", price 0, qty −5,
 *       blank fingerprint, non-VALID validity) — all dropped by validation,
 *       no candle for token 2000.</li>
 * </ul>
 *
 * <p>Assertions:
 * <ul>
 *   <li><b>P6.1</b> — phase 1: candle KV=46 rows (one per key),
 *       window-driven signal LOG=2 rows (one per token), signal KV=2 keys.
 *       Phase 2 (full replay from the same raw LOG, fresh state): candle KV
 *       stays FROZEN at 46 (upsert convergence — replay re-emits the same
 *       keys, no duplicate rows), window-driven signal LOG grows to 4 (replay
 *       re-emits the two signals — the LOG twin is audit, it may grow), and
 *       the signal KV key count stays FROZEN at 2 with canonical w22 content
 *       (either of the two w22 candidates — see below). Phase 3 (restore from
 *       the last completed checkpoint of phase 2 + 4
 *       new ticks per token): candle KV grows to exactly 50 (46 old keys +
 *       the two pending w23 pusher ticks held in checkpointed window state +
 *       two w24 candles); window-driven signal LOG stays 4 and KV stays 2
 *       (no new signal fires). An offset-0 fallback would re-emit the
 *       original 46 windows and reach 92 KV rows (46 keys re-upserted — no
 *       row growth, content unchanged).</li>
 *
 * <p>Since 2026-08-16 (Slice 2.2 forming-bar handoff) the SAME dual-sink
 * also carries the tick-driven forming-bar placeholder candidates
 * ({@code CANONICAL_FORMING_RULE_ID}): they land in the LOG and, being
 * canonical, upsert the KV current-state as well. P6.1's signal counts and
 * candidate-id assertions are therefore scoped to the window-driven
 * {@code CANONICAL_RULE_ID} rows only; the forming-bar branch is covered by
 * its own unit tests (FormingBarDetectionFunctionTest).
 *   <li><b>P6.2</b> — {@link SignalJob#preflightTableContracts} fails closed
 *       on: candle KV missing, candle KV schema drift (20-col raw as candle
 *       target), signal LOG missing/schema drift, signal KV current table
 *       missing, unreachable coordinator; and passes for the three scratch
 *       tables.</li>
 *   <li><b>P6.3</b> — invalid rows produce no candle; in-window late arrival
 *       folds in; beyond-lateness arrival is dropped and never corrects the
 *       emitted candle; per-token idle windows still close; emitted rows carry
 *       the canonical algorithm/configuration/schema versions.</li>
 * </ul>
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6=true)} —
 * skipped (with no MiniCluster startup) in the normal suite. Requires the dev
 * Fluss cluster ({@code FLUSS_BOOTSTRAP}, default {@code localhost:9123});
 * the test skips when the cluster is unreachable. Run:
 * {@code mvn -o test -Dtest=CandleGraphReplayIntegrationTest} with the gate
 * set.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("CANDLE-KV-REPLAY-001 P6: candle KV + signal dual-sink replay + failure + data-quality")
class CandleGraphReplayIntegrationTest {


    private static final Logger LOG = LoggerFactory.getLogger(CandleGraphReplayIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    /** 15000-aligned epoch anchor: floor(BASE/15000)*15000 == BASE (window alignment). */
    private static final long BASE = 1_699_999_995_000L;
    private static final long WINDOW_MS = 15_000L;
    private static final long TOKEN_A = 1000L;
    private static final long TOKEN_B = 1001L;
    private static final long TOKEN_BAD = 2000L;
    private static final int WINDOWS = 23; // w0..w22
    private static final String RULE_ID = "breakout-20-bullish-trend";

    private static final String CANONICAL_ALGORITHM = "candle-15s-v1";
    private static final String CANONICAL_CONFIGURATION = "1.0.0";

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    /**
     * MiniCluster resource. MiniClusterWithClientResource is a JUnit 4
     * ExternalResource (flink-runtime tests-jar), not a JUnit 5 extension —
     * its {@code before()}/{@code after()} are invoked explicitly from
     * {@link #connect()} / {@link #cleanup()}. {@code before()} registers the
     * TestStreamEnvironment that {@link SignalJob#buildTopology}'s
     * {@code getExecutionEnvironment(Configuration)} resolves to, so jobs
     * submit to this MiniCluster.
     */
    private static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeAll
    static void connect() throws Exception {
        MINI_CLUSTER.before();
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("p6: connected to Fluss at {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("p6: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            ScratchTables.dropCreated(admin, TIMEOUT);
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
        MINI_CLUSTER.after();
    }

    // ── P6.2 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("P6.2 preflight fails closed on missing/wrong-kind/drifted/unreachable")
    void preflightFailureInjection() throws Exception {
        ScratchSet s = createSet();

        // Happy path: the three scratch tables satisfy the write contracts.
        SignalJob.preflightTableContracts(SignalJobConfig.from(envFor(s, null)));

        // Candle LOG missing.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "CANDLE_TABLE",
                        "p6_" + s.suffix() + "_missing_log"))),
                "missing candle KV must fail preflight");
        // Schema drift: the 20-column raw LOG must never pass the 15-column check.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "CANDLE_TABLE", s.rawName()))),
                "schema-drifted candle target must fail preflight");
        // Signal LOG missing.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "SIGNAL_CANDIDATES_TABLE",
                        "p6_" + s.suffix() + "_missing_sig"))),
                "missing signal LOG must fail preflight");
        // Signal LOG schema drift: the 20-column raw LOG must never pass the 22-column check.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "SIGNAL_CANDIDATES_TABLE", s.rawName()))),
                "schema-drifted signal LOG must fail preflight");
        // Signal KV current table missing.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "SIGNAL_CURRENT_TABLE",
                        "p6_" + s.suffix() + "_missing_cur"))),
                "missing signal KV current table must fail preflight");
        // Unreachable coordinator — fail closed, never build a degraded graph.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "FLUSS_BOOTSTRAP_SERVERS", "127.0.0.1:1"))),
                "unreachable Fluss cluster must fail preflight");
    }

    // ── P6.1 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("P6.1 replay + restore: LOG grows deterministically; restore resumes at "
            + "offsets (no offset-0)")
    void logReplayAndRestoreIdempotency() throws Exception {
        ScratchSet s = createSet();
        appendFeed(s);

        // ── Phase 1: first pass ────────────────────────────────────────────
        JobClient job1 = startJob(envFor(s, null), "p6-phase1");
        awaitLogCount(s, 46, "phase-1 KV = 46 rows", 180);
        awaitSignalLogCount(s, 2, "phase-1 window-driven signal LOG = 2 rows", 120);
        awaitCurrentKeyCount(s, 2, "phase-1 signal KV = 2 keys", 120);
        Map<Long, SignalCurrent> cur1 = awaitStableCurrent(s, 120);
        assertCanonicalCurrent(s, cur1, "phase-1");
        Map<CandleKey, List<CandleRow>> log1 = readLogMap(s);
        assertFirstPass(s, log1);
        cancelAndFinish(job1, "phase1");
        LOG.info("p6: phase 1 done — LOG={} rows, signalLog={}, signalKv={}",
                log1.size(), signalLogRows(s), currentKeyCount(s));

        // ── Phase 2: full replay (fresh state, same raw LOG) ───────────────
        JobClient job2 = startJob(envFor(s, null), "p6-phase2");
        awaitTrue(() -> safe(() -> logCount(s) == 46), "phase-2 KV stays at 46 rows (replay upserts converge)", 180);
        awaitSignalLogCount(s, 4, "phase-2 window-driven signal LOG = 4 rows (2 re-emitted)", 120);
        awaitCurrentKeyCount(s, 2, "phase-2 signal KV stays at 2 keys", 120);
        // Replay convergence: same two keys, canonical w22 content only. The
        // two candidate producers (window-driven + forming-bar) union into the
        // KV sink, so the final last-upsert per token is interleaving-dependent
        // — content is scoped to the two w22 candidates, not pinned to an order.
        Map<Long, SignalCurrent> cur2 = awaitStableCurrent(s, 120);
        assertCanonicalCurrent(s, cur2, "phase-2");
        Map<CandleKey, List<CandleRow>> log2 = readLogMap(s);
        assertReplayConverges(s, log1, log2);
        String restore = awaitStableCheckpoint(s, job2.getJobID(), 180);
        cancelAndFinish(job2, "phase2");
        LOG.info("p6: phase 2 done — LOG={} rows, signalLog={}, signalKv={}, restore={}",
                log2.size(), signalLogRows(s), currentKeyCount(s), restore);

        // The phase-2 checkpoint retains the valid pusher tick in w23. New w24
        // data closes that pending window, and the w25 pusher closes w24.
        appendWindow(s, 24, 10024L); // 4 ticks per token in w24 (flat, no signal)
        appendPusher(s, 25, 10025L); // advance the watermark past w24's end
        JobClient job3 = startJob(envFor(s, restore), "p6-phase3");
        awaitLogCount(s, 50, "phase-3 restore must upsert pending w23 and new w24 (46 old + 4 new keys)", 180);
        Map<CandleKey, List<CandleRow>> log3 = readLogMap(s);
        assertEquals(50, log3.size(),
                "phase-3 LOG must hold 46 old keys + pending w23 + new w24 for both tokens");
        assertExistingKeysAndBusinessFields(log2, log3,
                "restore must not change the business fields of already-written keys");
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            List<CandleRow> w23 = log3.get(new CandleKey(token, BASE + 23 * WINDOW_MS));
            assertTrue(w23 != null && w23.size() == 1, "w23 must close exactly once for " + token);
            assertEquals(1, w23.get(0).tickCount(), "restored pending pusher tick closes w23 alone");
            List<CandleRow> w24 = log3.get(new CandleKey(token, BASE + 24 * WINDOW_MS));
            assertTrue(w24 != null && w24.size() == 1, "w24 must close exactly once for " + token);
            assertEquals(4, w24.get(0).tickCount(), "post-restore w24 must contain its four ticks");
        }
        awaitSignalLogCount(s, 4, "phase-3 window-driven signal LOG must stay at 4 rows", 120);
        awaitCurrentKeyCount(s, 2, "phase-3 signal KV must stay at 2 keys", 120);
        assertEquals(2, candidateIds(s).size(),
                "the pending and flat windows must not fire a new signal");
        cancelAndFinish(job3, "phase3");
        LOG.info("p6: phase 3 done — LOG={} rows, signalLog={}, signalKv={}",
                logCount(s), signalLogRows(s), currentKeyCount(s));
    }


    // ── P6.3 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("P6.3 data quality: invalid rows dropped, in-window late folds, "
            + "beyond-lateness dropped, idle window closes, canonical versions")
    void dataQualityRejectionsAndWindowSemantics() throws Exception {
        ScratchSet s = createSet();
        appendFeed(s);
        JobClient job = startJob(envFor(s, null), "p6-data-quality");
        awaitLogCount(s, 46, "first-pass LOG = 46 rows", 180);
        awaitSignalLogCount(s, 2, "data-quality window-driven signal LOG = 2 rows", 120);
        awaitCurrentKeyCount(s, 2, "data-quality signal KV = 2 keys", 120);

        Map<CandleKey, List<CandleRow>> log = readLogMap(s);
        assertFirstPass(s, log);

        // Invalid rows never become candles — token 2000 absent from LOG.
        assertTrue(log.keySet().stream().noneMatch(k -> k.token() == TOKEN_BAD),
                "invalid rows must not produce LOG candles");
        // LOG first pass: exactly one row per key (no duplicates yet).
        for (Map.Entry<CandleKey, List<CandleRow>> e : log.entrySet()) {
            assertEquals(1, e.getValue().size(), "first-pass LOG must hold one row per key: " + e.getKey());
        }
        // Emitted rows carry the canonical version triple.
        CandleRow sample = log.get(new CandleKey(TOKEN_A, BASE + 10 * WINDOW_MS)).get(0);
        assertEquals(CANONICAL_ALGORITHM, sample.algorithmVersion());
        assertEquals(CANONICAL_CONFIGURATION, sample.configurationVersion());
        assertEquals("2", sample.schemaVersion());

        cancelAndFinish(job, "data-quality");
        LOG.info("p6: data-quality done — LOG={} rows, signalLog={}, signalKv={}",
                log.size(), signalLogRows(s), currentKeyCount(s));
    }

    // ── feed ───────────────────────────────────────────────────────────────

    /** Appends the full deterministic feed (see class javadoc) to the scratch raw LOG. */
    private static void appendFeed(ScratchSet s) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            for (int w = 0; w < WINDOWS; w++) {
                for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                    int ticks = (w == 17 && token == TOKEN_A) ? 3 : 4;
                    appendWindowTicks(writer, token, w, ticks);
                }
                // In-window late arrival for window 4: appended after w4's own ticks
                // but before w5 (watermark still inside w4) — must fold in.
                if (w == 4) {
                    for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                        appendTick(writer, token, w, 14_900L, 7_000L + w, "L");
                    }
                }
            }
            // Pusher: closes window 22 (watermark = BASE+345000 = w22 end).
            appendPusher(writer, 23, 10_023L);
            // Beyond-lateness arrivals for window 5: event time inside w5, appended
            // after the watermark passed w5.end + allowedLateness — must be dropped.
            for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                appendTick(writer, token, 5, 14_900L, 8_000L + 5, "B");
            }
            appendInvalidRows(writer);
        } finally {
            writer.flush();
        }
    }

    /** Window w ticks at offsets [0, 5000, 10000, 14900]; w22 is the breakout window. */
    private static void appendWindowTicks(AppendWriter writer, long token, int w, int tickCount)
            throws Exception {
        if (w == 22) {
            appendTick(writer, token, w, 0L, 11_000L, "");
            appendTick(writer, token, w, 5_000L, 11_050L, "");
            appendTick(writer, token, w, 10_000L, 10_950L, "");
            appendTick(writer, token, w, 14_900L, 15_000L, "");
            return;
        }
        long base = 10_000L + w;
        appendTick(writer, token, w, 0L, base, "");
        appendTick(writer, token, w, 5_000L, base + 50L, "");
        appendTick(writer, token, w, 10_000L, base - 50L, "");
        if (tickCount > 3) {
            appendTick(writer, token, w, 14_900L, base, "");
        }
    }

    /** 4 in-order ticks in window 24 (flat candle — never fires a signal). */
    private static void appendWindow(ScratchSet s, int w, long base) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                appendTick(writer, token, w, 0L, base, "");
                appendTick(writer, token, w, 5_000L, base + 50L, "");
                appendTick(writer, token, w, 10_000L, base - 50L, "");
                appendTick(writer, token, w, 14_900L, base, "");
            }
        } finally {
            writer.flush();
        }
    }

    /** Advances the watermark past {@code w}'s end: event at w.start+5000, price base. */
    private static void appendPusher(AppendWriter writer, int w, long base) throws Exception {
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            appendTick(writer, token, w, 5_000L, base, "P");
        }
    }

    /** Standalone pusher (phase 3 feed) — opens its own writer. */
    private static void appendPusher(ScratchSet s, int w, long base) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            appendPusher(writer, w, base);
        } finally {
            writer.flush();
        }
    }

    private static void appendTick(AppendWriter writer, long token, int w, long offset, long price,
            String suffix) throws Exception {
        long eventTime = BASE + w * WINDOW_MS + offset;
        writer.append(toRawRow(token, eventTime, "fp-" + token + "-" + w + "-" + offset + suffix,
                "TRADE", price, 100L)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 5 invalid raw rows on token 2000 — every validation reason must drop them. */
    private static void appendInvalidRows(AppendWriter writer) throws Exception {
        writer.append(toRawRow(TOKEN_BAD, BASE + 1_000L, "fp-bad-schema", "TRADE", 10_000L, 100L,
                "VALID_TRADE", "3")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 2_000L, "fp-bad-price", "TRADE", 0L, 100L,
                "VALID_TRADE", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 3_000L, "fp-bad-qty", "TRADE", 10_000L, -5L,
                "VALID_TRADE", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 4_000L, "   ", "TRADE", 10_000L, 100L,
                "VALID_TRADE", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 5_000L, "fp-bad-validity", "TRADE", 10_000L, 100L,
                "INVALID", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 20-column raw row mirroring DDL 02 / DdlBootstrap.RAW_TABLE_1_SCHEMA exactly. */
    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty) {
        return toRawRow(token, eventTime, fingerprint, tickType, price, qty, "VALID_TRADE", "2");
    }

    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty, String validity, String schemaVersion) {
        return GenericRow.of(
                bs(fingerprint), bs("v2"), bs("p6-conn"), 1L, token, bs("NSE"), bs("TEST"),
                eventTime, eventTime, eventTime, bs(tickType), price, qty, null,
                bs("h-" + fingerprint), bs("v2"), bs("1.0"), bs(validity), null, bs(schemaVersion));
    }

    // ── job control ────────────────────────────────────────────────────────

    /** Env map for the pinned SignalJobConfig; {@code recovery} null ⇒ explicit full replay. */
    private static Map<String, String> envFor(ScratchSet s, String recovery) {
        return envFor(s, recovery, null, null);
    }

    private static Map<String, String> envFor(ScratchSet s, String recovery,
            String overrideKey, String overrideValue) {
        Map<String, String> e = new HashMap<>();
        e.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        e.put("FLUSS_DATABASE", "default");
        e.put("RAW_TABLE", s.rawName());
        e.put("CANDLE_TABLE", s.logName());
        e.put("SIGNAL_CANDIDATES_TABLE", s.candName());
        e.put("SIGNAL_CURRENT_TABLE", s.curName());
        e.put("QUARANTINE_TABLE", s.quarName());
        e.put("DEDUP_TTL_MS", "60000");
        e.put("CANDLE_WINDOW_MS", "15000");
        e.put("WATERMARK_OUT_OF_ORDER_MS", "5000");
        e.put("ALLOWED_LATENESS_MS", "5000");
        e.put("SOURCE_IDLE_MS", "15000");
        e.put("CHECKPOINT_INTERVAL_MS", "10000");
        e.put("CHECKPOINT_TIMEOUT_MS", "30000");
        e.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        e.put("RESTART_MAX_ATTEMPTS", "1");
        e.put("RESTART_DELAY_MS", "1000");
        e.put("DEPLOYMENT_ENV", "dev");
        e.put("STATE_BACKEND", "hashmap");
        e.put("CHECKPOINT_DIR", "file://" + s.checkpointDir().toAbsolutePath());
        e.put("PARALLELISM", "1");
        e.put("OTEL_COLLECTOR_HOST", "localhost:1");
        if (recovery == null) {
            e.put("ALLOW_FULL_REPLAY", "true");
        } else {
            e.put("STATE_RECOVERY_PATH", recovery);
        }
        if (overrideKey != null) {
            e.put(overrideKey, overrideValue);
        }
        return e;
    }

    /** Builds the production topology and submits it to the MiniCluster. */
    private static JobClient startJob(Map<String, String> env, String name) throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env);
        StreamExecutionEnvironment senv = SignalJob.buildTopology(config);
        JobClient client = senv.executeAsync(name);
        JobID jobId = client.getJobID();
        JobStatus status = awaitJobStatus(client, 90);
        if (status != JobStatus.RUNNING) {
            // Dump where the job is stuck from the MiniCluster execution graph.
            StringBuilder detail = new StringBuilder("job " + name + " stuck at " + status);
            try {
                AccessExecutionGraph graph =
                        MINI_CLUSTER.getMiniCluster().getExecutionGraph(jobId).get();
                for (AccessExecutionJobVertex v : graph.getVerticesTopologically()) {
                    detail.append("\n  vertex ").append(v.getName())
                            .append(" parallel=").append(v.getParallelism());
                    for (AccessExecutionVertex ev : v.getTaskVertices()) {
                        detail.append("\n    task ").append(ev.getParallelSubtaskIndex())
                                .append(" state=").append(ev.getExecutionState())
                                .append(" failure=")
                                .append(ev.getFailureInfo()
                                        .map(ErrorInfo::getExceptionAsString)
                                        .orElse("none"));
                    }
                }
            } catch (Exception e) {
                detail.append("\n  (graph dump failed: ").append(e).append(')');
            }
            assertTrue(false, detail.toString());
        }
        LOG.info("p6: job {} RUNNING", name);
        return client;
    }

    private static JobStatus awaitJobStatus(JobClient client, int timeoutSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            if (last == JobStatus.RUNNING || last == JobStatus.FINISHED || last == JobStatus.FAILED
                    || last == JobStatus.CANCELED) {
                return last;
            }
            Thread.sleep(2000);
        }
        return last;
    }

    private static void cancelAndFinish(JobClient client, String phase) throws Exception {
        client.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        awaitTrue(() -> {
            try {
                return client.getJobStatus().get(5, TimeUnit.SECONDS) == JobStatus.CANCELED;
            } catch (Exception e) {
                return false;
            }
        }, "phase " + phase + " job CANCELED", 60);
    }

    // ── assertions ─────────────────────────────────────────────────────────

    /** P6.3 (first pass) + the phase-1 half of P6.1. */
    private static void assertFirstPass(ScratchSet s, Map<CandleKey, List<CandleRow>> log)
            throws Exception {
        assertEquals(46, log.size(), "first pass: LOG must hold one row per key");
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            // In-window late arrival folds in (arrival-order close semantics).
            assertEquals(7004, log.get(new CandleKey(token, BASE + 4 * WINDOW_MS)).get(0).close(),
                    "in-window late tick must fold into the w4 candle");
            // Beyond-lateness arrival dropped, candle uncorrected.
            CandleRow w5 = log.get(new CandleKey(token, BASE + 5 * WINDOW_MS)).get(0);
            assertEquals(10005, w5.close(), "beyond-lateness tick must be dropped (close uncorrected)");
            assertEquals(4, w5.tickCount(), "beyond-lateness tick must not reach the accumulator");
            // Breakout window 22 fires.
            CandleRow w22 = log.get(new CandleKey(token, BASE + 22 * WINDOW_MS)).get(0);
            assertEquals(11000, w22.open());
            assertEquals(15000, w22.close());
        }
        // Token 1000's w17 has only 3 ticks — its window still closes on the
        // shared watermark advanced by token 1001 (idle bucket does not block).
        assertNotNull(log.get(new CandleKey(TOKEN_A, BASE + 17 * WINDOW_MS)),
                "idle token's window must close on the shared watermark");
        // Exactly one candidate per token, deterministic id.
        Set<String> ids = candidateIds(s);
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            assertTrue(ids.contains(RULE_ID + "-" + token + "-" + (BASE + 23 * WINDOW_MS)),
                    "expected candidate " + RULE_ID + "-" + token + "-" + (BASE + 23 * WINDOW_MS)
                            + ", got " + ids);
        }
        assertEquals(2, ids.size(), "exactly one candidate per token");
        assertEquals(2, currentKeyCount(s),
                "first pass: exactly one KV current row per token");
    }

    /** P6.1 phase-2 replay convergence: KV upserts re-emit each window once with equal fields (no row growth). */
    private static void assertReplayConverges(ScratchSet s, Map<CandleKey, List<CandleRow>> log1,
            Map<CandleKey, List<CandleRow>> log2) throws Exception {
        assertEquals(46, log1.size());
        // KV: row count == distinct-key count; replay upserts must not grow it.
        int firstPassRows = log1.values().stream().mapToInt(List::size).sum();
        int replayRows = log2.values().stream().mapToInt(List::size).sum();
        assertEquals(firstPassRows, replayRows,
                "full replay must not grow the candle KV — same-key upserts converge");
        for (Map.Entry<CandleKey, List<CandleRow>> e : log1.entrySet()) {
            List<CandleRow> replayed = log2.get(e.getKey());
            assertNotNull(replayed, "replay must re-upsert key " + e.getKey());
            assertEquals(1, replayed.size(), "replay must not add a row for key " + e.getKey());
            assertBusinessFieldsEqual(e.getValue().get(0), replayed.get(0),
                    "replayed KV row must carry the same business fields");
        }
        assertEquals(2, candidateIds(s).size(), "replay must not create extra candidates");
        assertEquals(4, signalLogRows(s),
                "replay must re-emit the two signals — the signal LOG twin grows (audit)");
        assertEquals(2, currentKeyCount(s),
                "replay must NOT grow the signal KV — same-instrument upserts converge");
    }

    /** Existing KV rows survive a restore unchanged; new post-restore keys may be added. */
    private static void assertExistingKeysAndBusinessFields(Map<CandleKey, List<CandleRow>> before,
            Map<CandleKey, List<CandleRow>> after, String what) {
        assertTrue(after.keySet().containsAll(before.keySet()),
                what + " — restored LOG must retain every existing key");
        for (Map.Entry<CandleKey, List<CandleRow>> e : before.entrySet()) {
            List<CandleRow> afterRows = after.get(e.getKey());
            assertNotNull(afterRows, what + " — key " + e.getKey());
            assertTrue(afterRows.size() >= e.getValue().size(),
                    what + " — key " + e.getKey() + " must keep every prior row");
            for (int i = 0; i < e.getValue().size(); i++) {
                assertBusinessFieldsEqual(e.getValue().get(i), afterRows.get(i),
                        what + " — key " + e.getKey() + " row " + i);
            }
        }
    }

    private static void assertBusinessFieldsEqual(CandleRow a, CandleRow b, String what) {
        assertEquals(a.exchange(), b.exchange(), what);
        assertEquals(a.symbol(), b.symbol(), what);
        assertEquals(a.windowStart(), b.windowStart(), what);
        assertEquals(a.windowEnd(), b.windowEnd(), what);
        assertEquals(a.open(), b.open(), what);
        assertEquals(a.high(), b.high(), what);
        assertEquals(a.low(), b.low(), what);
        assertEquals(a.close(), b.close(), what);
        assertEquals(a.volume(), b.volume(), what);
        assertEquals(a.tickCount(), b.tickCount(), what);
        assertEquals(a.algorithmVersion(), b.algorithmVersion(), what);
        assertEquals(a.configurationVersion(), b.configurationVersion(), what);
        assertEquals(a.schemaVersion(), b.schemaVersion(), what);
    }

    // ── polling / reading ──────────────────────────────────────────────────

    private static void awaitTrue(BooleanSupplier cond, String what, long timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(1_000L);
        }
        fail("Timed out after " + timeoutSeconds + "s waiting for " + what);
    }

    private static void awaitLogCount(ScratchSet s, long expected, String what, long timeoutSeconds)
            throws Exception {
        long latest = -1;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            latest = logCount(s);
            if (latest == expected) {
                return;
            }
            if (latest > expected) {
                fail(what + ": expected " + expected + " LOG rows but observed " + latest);
            }
            Thread.sleep(1_000L);
        }
        fail("Timed out after " + timeoutSeconds + "s waiting for " + what
                + " (expected " + expected + " LOG rows, observed " + latest + ")");
    }

    private static void awaitSignalLogCount(ScratchSet s, long expected, String what,
            long timeoutSeconds) throws Exception {
        long latest = -1;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            latest = signalLogRows(s);
            if (latest == expected) {
                return;
            }
            if (latest > expected) {
                fail(what + ": expected " + expected + " signal LOG rows but observed " + latest);
            }
            Thread.sleep(1_000L);
        }
        fail("Timed out after " + timeoutSeconds + "s waiting for " + what
                + " (expected " + expected + " signal LOG rows, observed " + latest + ")");
    }

    private static void awaitCurrentKeyCount(ScratchSet s, long expected, String what,
            long timeoutSeconds) throws Exception {
        long latest = -1;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            latest = currentKeyCount(s);
            if (latest == expected) {
                return;
            }
            if (latest > expected) {
                fail(what + ": expected " + expected + " KV keys but observed " + latest);
            }
            Thread.sleep(1_000L);
        }
        fail("Timed out after " + timeoutSeconds + "s waiting for " + what
                + " (expected " + expected + " KV keys, observed " + latest + ")");
    }

    private static boolean safe(ThrowingBooleanSupplier c) {
        try {
            return c.getAsBoolean();
        } catch (Exception e) {
            return false;
        }
    }

    @FunctionalInterface
    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    /** Waits until the LOG is stable for ≥3 polls AND a completed chk-N exists; returns it. */
    private static String awaitStableCheckpoint(ScratchSet s, JobID jobId, long timeoutSeconds)
            throws Exception {
        long last = -1;
        int stablePolls = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        String latest = null;
        while (System.nanoTime() < deadline) {
            long count = logCount(s);
            latest = latestCompletedCheckpoint(s, jobId);
            if (count == last) {
                stablePolls++;
            } else {
                stablePolls = 0;
            }
            last = count;
            if (stablePolls >= 3 && latest != null) {
                LOG.info("p6: LOG stable at {} rows for {} polls; restore target {}", count, stablePolls, latest);
                return latest;
            }
            Thread.sleep(5_000L);
        }
        String checkpointFailure = "none";
        try {
            AccessExecutionGraph graph = MINI_CLUSTER.getMiniCluster()
                    .getExecutionGraph(jobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            var checkpointStats = graph.getCheckpointStatsSnapshot();
            var counts = checkpointStats.getCounts();
            var failed = checkpointStats.getHistory().getLatestFailedCheckpoint();
            checkpointFailure = "enabled="
                    + graph.getCheckpointCoordinatorConfiguration().isCheckpointingEnabled()
                    + ", interval=" + graph.getCheckpointCoordinatorConfiguration().getCheckpointInterval()
                    + ", total=" + counts.getTotalNumberOfCheckpoints()
                    + ", completed=" + counts.getNumberOfCompletedCheckpoints()
                    + ", failed=" + counts.getNumberOfFailedCheckpoints()
                    + ", last=" + (failed == null ? "none" : failed.getFailureMessage());
        } catch (Exception e) {
            checkpointFailure = "unavailable: " + e;
        }
        fail("Timed out waiting for a stable checkpoint (LOG=" + last + ", latest chk=" + latest
                + ", latest failure=" + checkpointFailure + ")");
        return null; // unreachable
    }

    /** Newest completed checkpoint for {@code jobId}, as a {@code file://} URI. */
    private static String latestCompletedCheckpoint(ScratchSet s, JobID jobId) {
        Path dir = s.checkpointDir().resolve(jobId.toHexString());
        if (!Files.isDirectory(dir)) {
            return null;
        }
        long best = -1;
        try (Stream<Path> entries = Files.list(dir)) {
            for (Path p : (Iterable<Path>) entries::iterator) {
                String name = p.getFileName().toString();
                if (!name.startsWith("chk-") || !Files.isRegularFile(p.resolve("_metadata"))) {
                    continue;
                }
                try {
                    best = Math.max(best, Long.parseLong(name.substring("chk-".length())));
                } catch (NumberFormatException ignored) {
                    // not a checkpoint dir
                }
            }
        } catch (Exception e) {
            return null;
        }
        return best < 0 ? null : "file://" + dir.resolve("chk-" + best).toAbsolutePath();
    }

    private static long logCount(ScratchSet s) throws Exception {
        return scanAll(s.log(), s.logInfo()).size();
    }


    private static Map<CandleKey, List<CandleRow>> readLogMap(ScratchSet s) throws Exception {
        Map<CandleKey, List<CandleRow>> map = new HashMap<>();
        for (InternalRow r : scanAll(s.log(), s.logInfo())) {
            CandleKey k = new CandleKey(r.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                    r.getLong(CandleTableColumns.WINDOW_START));
            map.computeIfAbsent(k, ignored -> new ArrayList<>()).add(candleRow(r));
        }
        return map;
    }

    /** Window-driven candidate ids only (forming-bar placeholder candidates excluded). */
    private static Set<String> candidateIds(ScratchSet s) throws Exception {
        Set<String> ids = new HashSet<>();
        for (InternalRow r : scanAll(s.cand(), s.candInfo())) {
            if (windowDriven(r)) {
                ids.add(r.getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString());
            }
        }
        return ids;
    }

    /** True iff the signal row was emitted by the window-driven breakout rule. */
    private static boolean windowDriven(InternalRow r) {
        BinaryString ruleId = r.getString(SignalCandidatesTableColumns.RULE_ID);
        return ruleId != null
                && SignalCandidatesTableColumns.CANONICAL_RULE_ID.equals(ruleId.toString());
    }

    /** Scans every bucket of a table (CountCandles probe precedent). */
    private static List<InternalRow> scanAll(Table table, TableInfo info) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofMillis(250))) {
                while (it.hasNext()) {
                    rows.add(it.next());
                }
            }
        }
        return rows;
    }

    /** Window-driven signal LOG rows only (forming-bar placeholder candidates excluded). */
    private static long signalLogRows(ScratchSet s) throws Exception {
        long count = 0;
        for (InternalRow r : scanAll(s.cand(), s.candInfo())) {
            if (windowDriven(r)) {
                count++;
            }
        }
        return count;
    }

    private static long currentKeyCount(ScratchSet s) throws Exception {
        return scanAll(s.cur(), s.curInfo()).size();
    }

    /** Current-state projection: instrument_token -> (candidate_id, detection_ts). */
    private static Map<Long, SignalCurrent> readCurrentMap(ScratchSet s) throws Exception {
        Map<Long, SignalCurrent> map = new HashMap<>();
        for (InternalRow r : scanAll(s.cur(), s.curInfo())) {
            map.put(r.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN),
                    new SignalCurrent(
                            r.getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString(),
                            r.getLong(SignalCandidatesTableColumns.DETECTION_TS)));
        }
        return map;
    }

    /**
     * P6.1 KV convergence: one row per token, each the canonical w22 candidate
     * (the window-driven {@code breakout-20-bullish-trend} id at w22 end, or
     * the forming-bar placeholder id fired mid-w22). The two candidate
     * producers union into the KV sink, so the final last-upsert per token is
     * interleaving-dependent — the assertion scopes to canonical w22 content
     * (no growth, no stale windows) instead of pinning one ordering.
     */
    private static void assertCanonicalCurrent(ScratchSet s, Map<Long, SignalCurrent> cur,
            String phase) {
        assertEquals(2, cur.size(), phase + ": KV must hold one row per token");
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            SignalCurrent row = cur.get(token);
            assertNotNull(row, phase + ": KV row missing for token " + token);
            String windowId = RULE_ID + "-" + token + "-" + (BASE + 23 * WINDOW_MS);
            String formingId = SignalCandidatesTableColumns.CANONICAL_FORMING_RULE_ID
                    + "-" + token + "-" + (BASE + 22 * WINDOW_MS);
            assertTrue(row.candidateId().equals(windowId) || row.candidateId().equals(formingId),
                    phase + ": token " + token + " must hold a canonical w22 candidate, got "
                            + row.candidateId());
            if (row.candidateId().equals(windowId)) {
                assertEquals(BASE + 23 * WINDOW_MS, row.detectionTs(),
                        phase + ": window-driven detection_ts must be w22 end");
            } else {
                assertTrue(row.detectionTs() >= BASE + 22 * WINDOW_MS
                                && row.detectionTs() <= BASE + 23 * WINDOW_MS,
                        phase + ": forming-bar detection_ts must fall inside w22, got "
                                + row.detectionTs());
            }
        }
    }

    /**
     * Reads the KV current-state until two consecutive polls (1 s apart) are
     * identical — the last-upsert-wins projection must be fully settled before
     * content is read (forming-bar candidates upsert the same PK rows, so
     * the key count alone cannot pin the content).
     */
    private static Map<Long, SignalCurrent> awaitStableCurrent(ScratchSet s, long timeoutSeconds)
            throws Exception {
        Map<Long, SignalCurrent> prev = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            Map<Long, SignalCurrent> cur = readCurrentMap(s);
            if (cur.equals(prev)) {
                return cur;
            }
            prev = cur;
            Thread.sleep(1_000L);
        }
        fail("KV current-state did not settle within " + timeoutSeconds + "s (last=" + prev + ")");
        return null; // unreachable
    }

    private static CandleRow candleRow(InternalRow r) {
        return new CandleRow(
                r.getString(CandleTableColumns.EXCHANGE).toString(),
                r.getString(CandleTableColumns.SYMBOL).toString(),
                r.getLong(CandleTableColumns.WINDOW_START),
                r.getLong(CandleTableColumns.WINDOW_END),
                r.getLong(CandleTableColumns.OPEN_PAISE),
                r.getLong(CandleTableColumns.HIGH_PAISE),
                r.getLong(CandleTableColumns.LOW_PAISE),
                r.getLong(CandleTableColumns.CLOSE_PAISE),
                r.getLong(CandleTableColumns.VOLUME),
                r.getInt(CandleTableColumns.TICK_COUNT),
                r.getString(CandleTableColumns.ALGORITHM_VERSION).toString(),
                r.getString(CandleTableColumns.CONFIGURATION_VERSION).toString(),
                r.getString(CandleTableColumns.SCHEMA_VERSION).toString(),
                r.getLong(CandleTableColumns.OUTPUT_TS));
    }

    // ── scratch tables ─────────────────────────────────────────────────────

    private static ScratchSet createSet() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        Path cpDir = Path.of(System.getProperty("java.io.tmpdir"), "p6-cp-" + suffix);
        String rawName = "p6_" + suffix + "_raw";
        String logName = "p6_" + suffix + "_log";
        String candName = "p6_" + suffix + "_cand";
        String curName = "p6_" + suffix + "_cur";
        String quarName = "p6_" + suffix + "_quar";
        // One raw bucket gives this event-time test one source watermark; Fluss's
        // production 16-bucket routing is covered independently by bucket tests.
        Table raw = ScratchTables.create(connection, admin, rawName, rawSchema(), null, 1,
                "raw LOG", TIMEOUT);
        Table log = ScratchTables.create(connection, admin, logName, ScratchTables.candleSchema(),
                List.of("instrument_token", "window_start"), 16, "candle KV", TIMEOUT);
        Table cand = ScratchTables.create(connection, admin, candName,
                ScratchTables.signalLogSchema(), null, 16, "signal LOG", TIMEOUT);
        Table cur = ScratchTables.create(connection, admin, curName,
                ScratchTables.signalCurrentSchema(), List.of("instrument_token"), 16,
                "signal current KV", TIMEOUT);
        Table quar = ScratchTables.create(connection, admin, quarName,
                ScratchTables.ingestionQuarantineSchema(), null, 16,
                "quarantine LOG", TIMEOUT);
        TableInfo rawInfo = tableInfo(rawName);
        TableInfo logInfo = tableInfo(logName);
        TableInfo candInfo = tableInfo(candName);
        TableInfo curInfo = tableInfo(curName);
        TableInfo quarInfo = tableInfo(quarName);
        return new ScratchSet(suffix, rawName, logName, candName, curName, quarName,
                raw, log, cand, cur, quar, rawInfo, logInfo, candInfo, curInfo, quarInfo, cpDir);
    }

    private static TableInfo tableInfo(String name) throws Exception {
        return admin.getTableInfo(TablePath.of("default", name))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 20-column raw LOG schema mirroring DDL 02 (DdlBootstrap.RAW_TABLE_1_SCHEMA). */
    private static Schema rawSchema() {
        return Schema.newBuilder()
                .column("event_fingerprint", DataTypes.STRING())
                .column("fingerprint_version", DataTypes.STRING())
                .column("connection_id", DataTypes.STRING())
                .column("connection_epoch", DataTypes.BIGINT())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("event_time", DataTypes.BIGINT())
                .column("ingest_ts", DataTypes.BIGINT())
                .column("ack_ts", DataTypes.BIGINT())
                .column("tick_type", DataTypes.STRING())
                .column("last_price_paise", DataTypes.BIGINT())
                .column("last_qty", DataTypes.BIGINT())
                .column("raw_payload", DataTypes.BYTES())
                .column("payload_hash", DataTypes.STRING())
                .column("decoder_version", DataTypes.STRING())
                .column("protocol_version", DataTypes.STRING())
                .column("validity_state", DataTypes.STRING())
                .column("validity_reason", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING())
                .build();
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }

    /** One scratch table set — never shared between tests. */
    private record ScratchSet(
            String suffix,
            String rawName,
            String logName,
            String candName,
            String curName,
            String quarName,
            Table raw,
            Table log,
            Table cand,
            Table cur,
            Table quar,
            TableInfo rawInfo,
            TableInfo logInfo,
            TableInfo candInfo,
            TableInfo curInfo,
            TableInfo quarInfo,
            Path checkpointDir) {}

    /** Candle row identity = (instrument_token, window_start). */
    private record CandleKey(long token, long windowStart) {}

    /** KV current-state row content (identity + emit instant). */
    private record SignalCurrent(String candidateId, long detectionTs) {}

    /** Business fields + output_ts of a candle row (output_ts excluded from equality checks). */
    private record CandleRow(
            String exchange,
            String symbol,
            long windowStart,
            long windowEnd,
            long open,
            long high,
            long low,
            long close,
            long volume,
            int tickCount,
            String algorithmVersion,
            String configurationVersion,
            String schemaVersion,
            long outputTs) {}
}
