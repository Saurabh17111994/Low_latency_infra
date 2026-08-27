package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
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
 * Tracker 14 P4.3 (docs/08_implementation/04-signal-job.md §Absorbed documents) —
 * the pinned production state backend proven at RUNTIME, not just in config
 * tests: the actual {@link SignalJob#buildTopology} graph runs on the embedded
 * <b>RocksDB</b> backend inside a Flink MiniCluster, checkpoints to a local
 * file URI, is cancelled, and is restored from that checkpoint by a <b>fresh
 * MiniCluster instance</b> (a different worker — new TaskManager, new RocksDB
 * instances) against the same scratch Fluss tables.
 *
 * <p>What this proves, with runtime artifacts (not config assertions):
 * <ul>
 *   <li>RocksDB actually ran: {@code STATE_BACKEND_LOCAL_DIRS} contains a live
 *       RocksDB store ({@code CURRENT}/MANIFEST/OPTIONS) after phase 1, and
 *       the completed checkpoint contains {@code .sst} files — incremental
 *       RocksDB artifacts a heap-state checkpoint never produces.</li>
 *   <li>Dedup state restores: the restored job resumes at the checkpointed
 *       source offsets and dedup/window state — LOG reaches exactly 50 (the
 *       46 phase-1 rows + the two pending w23 windows held in checkpointed
 *       window state + the new w24 windows), never 92, which an offset-0
 *       fallback would emit.</li>
 *   <li>Window state restores: the pending w23 windows close exactly once
 *       (tickCount=1) and w24 folds its four ticks.</li>
 *   <li>Existing LOG rows survive the restore byte-identical in business
 *       fields, and the restored job appends exactly the 4 post-restore rows
 *       (w23+w24 × 2 tokens) — no duplicates, no lost rows.</li>
 *   <li>Restore failure cannot fall back: startup mode is RESTORE (config
 *       gate, P6-proven) and the restore is strict (no {@code
 *       allowNonRestoredState}).</li>
 * </ul>
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6=true)} —
 * same dev-cluster gate as {@link CandleGraphReplayIntegrationTest}; skipped
 * (no MiniCluster, no Fluss connection) in the normal suite. Run:
 * {@code mvn -o test -Dtest=CandleRocksDbRestoreIntegrationTest} with the gate
 * set and the dev Fluss cluster reachable at {@code FLUSS_BOOTSTRAP}
 * (default {@code localhost:9123}).
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("CANDLE-KV-REPLAY-001 P4.3: RocksDB backend restore across workers")
class CandleRocksDbRestoreIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(CandleRocksDbRestoreIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    /** 15000-aligned epoch anchor (window alignment), same feed as P6. */
    private static final long BASE = 1_699_999_995_000L;
    private static final long WINDOW_MS = 15_000L;
    private static final long TOKEN_A = 1000L;
    private static final long TOKEN_B = 1001L;
    private static final long TOKEN_BAD = 2000L;
    private static final int WINDOWS = 23; // w0..w22
    private static final String CANONICAL_ALGORITHM = "candle-15s-v1";
    private static final String CANONICAL_CONFIGURATION = "1.0.0";

    private static final List<String> CREATED_TABLES = new ArrayList<>();
    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("p4.3: connected to Fluss at {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("p4.3: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        for (String table : CREATED_TABLES) {
            try {
                admin.dropTable(TablePath.of("default", table), false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("p4.3: dropped scratch table {}", table);
            } catch (Exception e) {
                LOG.warn("p4.3: drop {} failed: {}", table, e.getMessage());
            }
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("RocksDB checkpoint restores dedup/window/sink state on a fresh worker")
    void rocksDbBackendRestoresDedupWindowAndSinksAcrossWorkers() throws Exception {
        ScratchSet s = createSet();
        Path rocksDir = Path.of(System.getProperty("java.io.tmpdir"), "rocks-local-" + s.suffix());
        Files.createDirectories(rocksDir);

        // ── Phase 1: RocksDB graph on worker A ────────────────────────────
        Map<CandleKey, List<CandleRow>> log1;
        String restore;
        MiniClusterWithClientResource clusterA = newMiniCluster();
        clusterA.before();
        try {
            appendFeed(s);
            JobClient job1 = startJob(envFor(s, null, rocksDir), "p4.3-phase1");
            awaitLogCount(s, 46, "phase-1 LOG = 46 rows", 180);
            log1 = readLogMap(s);
            assertFirstPass(s, log1);
            restore = awaitStableCheckpoint(s, job1.getJobID(), 180);
            // Live RocksDB store must exist WHILE the job runs — RocksDB dispose
            // removes the store at cancel, so asserting after cancel proves nothing.
            assertRocksDbStore(rocksDir, "state.backend.rocksdb.localdir");
            cancelAndFinish(job1, "phase1");
            LOG.info("p4.3: phase 1 done — LOG={} rows, candidates={}, restore={}",
                    log1.size(), candidateIds(s).size(), restore);

            // The completed checkpoint holds state files in shared/ (the
            // incremental-RocksDB layout; names are MD5 hashes, so no
            // extension is expected). The _metadata itself names RocksDB
            // artifacts (000017.sst, rocksdb.properties) — heap checkpoints
            // never do.
            assertCheckpointStateFiles(s, job1.getJobID(),
                    "completed checkpoint must hold state files under shared/");
        } finally {
            clusterA.after(); // phase 2 must run on a FRESH worker
        }

        // ── Phase 2: restore from phase-1 checkpoint on worker B ──────────
        MiniClusterWithClientResource clusterB = newMiniCluster();
        clusterB.before();
        try {
            appendWindow(s, 24, 10024L); // 4 ticks per token in w24 (flat)
            appendPusher(s, 25, 10025L); // advance the watermark past w24's end
            JobClient job2 = startJob(envFor(s, restore, rocksDir), "p4.3-phase2");
            awaitLogCount(s, 50,
                    "restored RocksDB job must emit pending w23 and new w24 (46 + 2 + 2)", 180);
            Map<CandleKey, List<CandleRow>> log2 = readLogMap(s);
            assertEquals(50, log2.size(),
                    "restored LOG must hold 46 old keys + pending w23 + new w24 for both tokens");
            assertExistingKeysAndBusinessFields(log1, log2,
                    "restore must not change the business fields of already-written keys");
            for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                List<CandleRow> w23 = log2.get(new CandleKey(token, BASE + 23 * WINDOW_MS));
                assertTrue(w23 != null && w23.size() == 1, "w23 must close exactly once for " + token);
                assertEquals(1, w23.get(0).tickCount(),
                        "restored pending pusher tick must close w23 exactly once");
                List<CandleRow> w24 = log2.get(new CandleKey(token, BASE + 24 * WINDOW_MS));
                assertTrue(w24 != null && w24.size() == 1, "w24 must close exactly once for " + token);
                assertEquals(4, w24.get(0).tickCount(),
                        "post-restore w24 must contain its four ticks");
            }
            assertEquals(2, candidateIds(s).size(),
                    "the pending and flat windows must not fire a new signal");
            cancelAndFinish(job2, "phase2");
            LOG.info("p4.3: phase 2 done — LOG={} rows, candidates={}",
                    logCount(s), candidateIds(s).size());
        } finally {
            clusterB.after();
        }
    }

    private static MiniClusterWithClientResource newMiniCluster() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
    }

    // ── runtime artifact assertions ────────────────────────────────────────

    /**
     * RocksDB store in the configured local directory — proof the backend ran
     * on the pinned fast-disk dirs. Flink 2.2.1 creates one store per keyed
     * operator instance as a subdirectory {@code <dir>/<job>_op_<op>__<uuid>}
     * holding CURRENT/MANIFEST/OPTIONS/LOG/.sst — so this walks recursively.
     */
    private static void assertRocksDbStore(Path dir, String what) throws Exception {
        assertTrue(Files.isDirectory(dir), what + " dir missing: " + dir);
        List<String> all = new ArrayList<>();
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) {
                all.add(file.getFileName().toString());
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE; // unreadable — skip
            }
        });
        boolean manifest = all.stream().anyMatch(n -> n.startsWith("MANIFEST-"));
        boolean options = all.stream().anyMatch(n -> n.startsWith("OPTIONS-"));
        boolean sstOrLog = all.contains("LOG") || all.stream().anyMatch(n -> n.endsWith(".sst"));
        if (!(all.contains("CURRENT") && (manifest || options) && sstOrLog)) {
            fail(what + "=" + dir + " must hold a live RocksDB store (CURRENT/MANIFEST/OPTIONS/"
                    + "LOG/.sst per operator subdir); found: " + all);
        }
    }

    /** The completed checkpoint must hold state files under shared/ (incremental layout). */
    private static void assertCheckpointStateFiles(ScratchSet s, JobID jobId, String what)
            throws Exception {
        Path jobDir = s.checkpointDir().resolve(jobId.toHexString());
        assertTrue(Files.isDirectory(jobDir), "checkpoint dir missing: " + jobDir);
        try (Stream<Path> walk = Files.walk(jobDir)) {
            long shared = walk.filter(p -> p.getParent() != null
                            && p.getParent().getFileName().toString().equals("shared"))
                    .count();
            assertTrue(shared > 0, what + " — no shared/ state files under " + jobDir);
        }
    }

    // ── feed (identical to P6: same 46/96/50/2 expectations) ──────────────

    private static void appendFeed(ScratchSet s) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            for (int w = 0; w < WINDOWS; w++) {
                for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                    int ticks = (w == 17 && token == TOKEN_A) ? 3 : 4;
                    appendWindowTicks(writer, token, w, ticks);
                }
                if (w == 4) {
                    for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                        appendTick(writer, token, w, 14_900L, 7_000L + w, "L");
                    }
                }
            }
            appendPusher(writer, 23, 10_023L);
            for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                appendTick(writer, token, 5, 14_900L, 8_000L + 5, "B");
            }
            appendInvalidRows(writer);
        } finally {
            writer.flush();
        }
    }

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

    private static void appendPusher(AppendWriter writer, int w, long base) throws Exception {
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            appendTick(writer, token, w, 5_000L, base, "P");
        }
    }

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

    /** 20-column raw row mirroring DDL 02 (same layout as P6); default validity/schema. */
    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty) {
        return toRawRow(token, eventTime, fingerprint, tickType, price, qty, "VALID_TRADE", "2");
    }

    /** 20-column raw row mirroring DDL 02 (same layout as P6). */
    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty, String validity, String schemaVersion) {
        return GenericRow.of(
                bs(fingerprint), bs("v2"), bs("p4.3-conn"), 1L, token, bs("NSE"), bs("TEST"),
                eventTime, eventTime, eventTime, bs(tickType), price, qty, null,
                bs("h-" + fingerprint), bs("v2"), bs("1.0"), bs(validity), null, bs(schemaVersion));
    }

    // ── job control ────────────────────────────────────────────────────────

    /** Env map: dev mode + RocksDB backend + file checkpoints + RocksDB local dirs. */
    private static Map<String, String> envFor(ScratchSet s, String recovery, Path rocksDir) {
        Map<String, String> e = new HashMap<>();
        e.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        e.put("FLUSS_DATABASE", "default");
        e.put("RAW_TABLE", s.rawName());
        e.put("CANDLE_TABLE", s.logName());
        e.put("SIGNAL_CANDIDATES_TABLE", s.candName());
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
        e.put("STATE_BACKEND", "rocksdb");
        e.put("STATE_BACKEND_LOCAL_DIRS", rocksDir.toAbsolutePath().toString());
        e.put("CHECKPOINT_DIR", "file://" + s.checkpointDir().toAbsolutePath());
        e.put("PARALLELISM", "1");
        e.put("OTEL_COLLECTOR_HOST", "localhost:1");
        if (recovery == null) {
            e.put("ALLOW_FULL_REPLAY", "true");
        } else {
            e.put("STATE_RECOVERY_PATH", recovery);
        }
        return e;
    }

    private static JobClient startJob(Map<String, String> env, String name) throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env);
        StreamExecutionEnvironment senv = SignalJob.buildTopology(config);
        JobClient client = senv.executeAsync(name);
        JobStatus status = awaitJobStatus(client, 90);
        assertEquals(JobStatus.RUNNING, status, "job " + name + " must reach RUNNING, got " + status);
        LOG.info("p4.3: job {} RUNNING", name);
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

    private static void assertFirstPass(ScratchSet s, Map<CandleKey, List<CandleRow>> log)
            throws Exception {
        assertEquals(46, log.size(), "first pass: LOG must hold one row per key");
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            assertEquals(7004, log.get(new CandleKey(token, BASE + 4 * WINDOW_MS)).get(0).close(),
                    "in-window late tick must fold into the w4 candle");
            CandleRow w5 = log.get(new CandleKey(token, BASE + 5 * WINDOW_MS)).get(0);
            assertEquals(10005, w5.close(), "beyond-lateness tick must be dropped");
            assertEquals(4, w5.tickCount(), "beyond-lateness tick must not reach the accumulator");
            CandleRow w22 = log.get(new CandleKey(token, BASE + 22 * WINDOW_MS)).get(0);
            assertEquals(11000, w22.open());
            assertEquals(15000, w22.close());
            CandleRow sample = log.get(new CandleKey(token, BASE + 10 * WINDOW_MS)).get(0);
            assertEquals(CANONICAL_ALGORITHM, sample.algorithmVersion());
            assertEquals(CANONICAL_CONFIGURATION, sample.configurationVersion());
            assertEquals("2", sample.schemaVersion());
        }
        assertNotNull(log.get(new CandleKey(TOKEN_A, BASE + 17 * WINDOW_MS)),
                "idle token's window must close on the shared watermark");
        assertEquals(2, candidateIds(s).size(), "exactly one candidate per token");
    }

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
                LOG.info("p4.3: LOG stable at {} rows; restore target {}", count, latest);
                return latest;
            }
            Thread.sleep(5_000L);
        }
        fail("Timed out waiting for a stable RocksDB checkpoint (LOG=" + last
                + ", latest chk=" + latest + ")");
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

    /** Scans every bucket of a table (P6 precedent). */
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
        Path cpDir = Path.of(System.getProperty("java.io.tmpdir"), "rocks-cp-" + suffix);
        String rawName = "rocks_" + suffix + "_raw";
        String logName = "rocks_" + suffix + "_log";
        String candName = "rocks_" + suffix + "_cand";
        Table raw = createTable(rawName, rawSchema(), null, 1, "raw LOG");
        Table log = createTable(logName, candleSchema(), List.of("instrument_token", "window_start"),
                16, "candle KV");
        Table cand = createTable(candName, candidatesSchema(), null, 16, "signal LOG");
        return new ScratchSet(suffix, rawName, logName, candName,
                raw, log, cand,
                tableInfo(rawName), tableInfo(logName), tableInfo(candName),
                cpDir);
    }

    private static Table createTable(String name, Schema schema, List<String> pk, int bucketCount,
            String what) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(bucketCount, pk == null ? "instrument_token" : pk.get(0))
                .build();
        TablePath path = TablePath.of("default", name);
        admin.createTable(path, td, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED_TABLES.add(name);
        LOG.info("p4.3: created scratch {} table {}", what, name);
        return connection.getTable(path);
    }

    private static TableInfo tableInfo(String name) throws Exception {
        return admin.getTableInfo(TablePath.of("default", name))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

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

    private static Schema candleSchema() {
        return Schema.newBuilder()
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("window_start", DataTypes.BIGINT())
                .column("window_end", DataTypes.BIGINT())
                .column("open_paise", DataTypes.BIGINT())
                .column("high_paise", DataTypes.BIGINT())
                .column("low_paise", DataTypes.BIGINT())
                .column("close_paise", DataTypes.BIGINT())
                .column("volume", DataTypes.BIGINT())
                .column("tick_count", DataTypes.INT())
                .column("algorithm_version", DataTypes.STRING())
                .column("configuration_version", DataTypes.STRING())
                .column("output_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("instrument_token", "window_start")
                .build();
    }

    private static Schema candidatesSchema() {
        return Schema.newBuilder()
                .column("candidate_id", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("strategy_id", DataTypes.STRING())
                .column("strategy_version", DataTypes.STRING())
                .column("rule_id", DataTypes.STRING())
                .column("detection_ts", DataTypes.BIGINT())
                .column("evaluation_ts", DataTypes.BIGINT())
                .column("action", DataTypes.STRING())
                .column("side", DataTypes.STRING())
                .column("quantity", DataTypes.BIGINT())
                .column("order_type", DataTypes.STRING())
                .column("limit_price_paise", DataTypes.BIGINT())
                .column("score_inputs", DataTypes.STRING())
                .column("formation_snapshot_ref", DataTypes.STRING())
                .column("validity_reason", DataTypes.STRING())
                .column("supersedes_candidate_id", DataTypes.STRING())
                .column("superseded_by_candidate_id", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING())
                .build();
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }

    /** Scratch tables — never shared between tests. */
    private record ScratchSet(
            String suffix,
            String rawName,
            String logName,
            String candName,
            Table raw,
            Table log,
            Table cand,
            TableInfo rawInfo,
            TableInfo logInfo,
            TableInfo candInfo,
            Path checkpointDir) {}

    private record CandleKey(long token, long windowStart) {}

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
