package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
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
 * Tracker 14 P6.2/P6.3 failure injection against the REAL {@link SignalJob#buildTopology}
 * graph on a Flink MiniCluster + scratch Fluss tables (same harness as P4.3). No live
 * cluster is disturbed: injections are job-scoped (read-only checkpoint dir)
 * or feed-scoped (watermark stall, overflow event time).
 *
 * <ul>
 *   <li><b>P6.2 616</b> — checkpoint failure triggers the CONFIGURED fixed-delay restart
 *       behavior: with {@code RESTART_MAX_ATTEMPTS=2} the job is observed RESTARTING
 *       (JobStatus) after each checkpoint failure, then FAILED when attempts are
 *       exhausted; a control run with a writable checkpoint dir completes checkpoints
 *       on the same feed. The pinned {@code CHECKPOINT_TIMEOUT_MS} cannot be injected
 *       via env (config gate), so the failure is provoked by a read-only checkpoint
 *       directory — a checkpoint failure mode, the same path Flink's timeout uses
 *       (timeout == checkpoint failure from the restart-strategy perspective).</li>
 *   <li><b>P6.3 618</b> — watermark stall: with the feed stopped (under the 15s
 *       {@code SOURCE_IDLE_MS}), the watermark freezes and NOTHING closes (no phantom
 *       candles); resuming the feed closes exactly the windows whose end the watermark
 *       now passes — clean recovery, no duplicates. The candidate assertion is
 *       scoped to the window-driven breakout rule: the tick-driven forming-bar
 *       placeholder detector (2026-08-16) fires independently of the watermark
 *       and is covered by its own unit tests.</li>
 *   <li><b>P6.3 623</b> — an event time at {@code Long.MAX_VALUE} is REJECTED by the
 *       validation gate end-to-end (no candle for token 2000) — the
 *       {@code event-time-overflow-window} guard that keeps Flink's
 *       {@code window.maxTimestamp + allowedLateness} trigger arithmetic from
 *       overflowing.</li>
 * </ul>
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6=true)} — same
 * dev-cluster gate as the P6/P4.3 tests. Run:
 * {@code mvn -o test -Dtest=CandleFailureInjectionIntegrationTest} with the gate set and
 * the dev Fluss cluster reachable at {@code FLUSS_BOOTSTRAP} (default {@code localhost:9123}).
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("CANDLE-KV-REPLAY-001 P6.2/P6.3: failure injection on the real graph")
class CandleFailureInjectionIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(CandleFailureInjectionIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    /** 15000-aligned epoch anchor (window alignment), same feed as P6/P4.3. */
    private static final long BASE = 1_699_999_995_000L;
    private static final long WINDOW_MS = 15_000L;
    private static final long TOKEN_A = 1000L;
    private static final long TOKEN_B = 1001L;
    private static final long TOKEN_BAD = 2000L;
    private static final int WINDOWS = 23; // w0..w22

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
            LOG.info("p63: connected to Fluss at {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("p63: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        for (String table : CREATED_TABLES) {
            try {
                admin.dropTable(TablePath.of("default", table), false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                LOG.warn("p63: drop {} failed (already dropped mid-test?): {}", table, e.getMessage());
            }
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    // ── P6.2 616: checkpoint failure -> configured restart -> FAILED ─────────

    @Test
    @DisplayName("checkpoint failure triggers the configured fixed-delay restart, then FAILED")
    void checkpointFailureTriggersConfiguredRestartThenFails() throws Exception {
        // Healthy path FIRST: the job completes a real checkpoint on the writable
        // dir (same feed/backend) — this is the control. Then the injection strips
        // write permission from the job-id checkpoint dir, so the NEXT checkpoint
        // (10 s pinned interval) cannot create its location -> checkpoint failure
        // -> the CONFIGURED fixed-delay restart (attempts=2, delay=1s) -> FAILED
        // once attempts are exhausted. The pinned CHECKPOINT_TIMEOUT_MS cannot be
        // injected via env (config gate), but a timeout and a location-creation
        // failure are the same checkpoint-failure path from the restart-strategy
        // perspective.
        ScratchSet s = createSet();
        Path jobDir = null;
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            appendFeed(s);
            JobClient job = startJob(baseEnv(s, null, "rocksdb",
                    "file://" + s.checkpointDir().toAbsolutePath(), "2"), "p63-cp-failing");
            String chk = awaitStableCheckpoint(s, job.getJobID(), 180);
            assertTrue(chk != null && !chk.isBlank(),
                    "healthy path: a checkpoint must complete before the injection");
            LOG.info("p63: healthy checkpoint {} complete; now stripping write from the checkpoint dir", chk);
            jobDir = s.checkpointDir().resolve(job.getJobID().toHexString());
            Files.setPosixFilePermissions(jobDir, PosixFilePermissions.fromString("r-xr-xr-x"));

            Set<JobStatus> seen = new HashSet<>();
            JobStatus terminal = awaitTerminal(job, seen, 180);
            assertEquals(JobStatus.FAILED, terminal,
                    "exhausted restart attempts must end FAILED, seen=" + seen);
            assertTrue(seen.contains(JobStatus.RESTARTING),
                    "checkpoint failure must drive the job through the configured "
                            + "RESTARTING state (fixed-delay restart), seen=" + seen);
            String cause = jobFailureCause(job);
            assertTrue(cause.toLowerCase().contains("checkpoint"),
                    "failure cause must be the checkpoint injection, got: " + cause);
            LOG.info("p63: checkpoint-failure job FAILED after RESTARTING; cause={}; statuses={}",
                    cause, seen);
        } finally {
            if (jobDir != null) {
                try {
                    Files.setPosixFilePermissions(jobDir, PosixFilePermissions.fromString("rwxr-xr-x"));
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
            cluster.after();
        }
    }

    // ── P6.3 618 + 623: watermark stall freezes output; overflow event time rejected ──

    @Test
    @DisplayName("watermark stall freezes output, resume closes exactly the passed windows")
    void watermarkStallFreezesOutputAndResumesCleanly() throws Exception {
        ScratchSet s = createSet();
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            JobClient job = startJob(baseEnv(s, null, "hashmap",
                    "file://" + s.checkpointDir().toAbsolutePath(), "3"), "p63-stall");
            // Stall phase: ticks ONLY in w0 (offsets 0..300). Max event time
            // BASE+300 -> watermark BASE+300-5001 = BASE-4701, below w0's end
            // (BASE+14999) -> NOTHING can close, and the event-driven generator
            // emits nothing on the periodic timer, so the stall must freeze output.
            AppendWriter writer = s.raw().newAppend().createWriter();
            try {
                for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                    appendTick(writer, token, 0, 0L, 10_000L, "");
                    appendTick(writer, token, 0, 100L, 10_050L, "");
                    appendTick(writer, token, 0, 300L, 9_950L, "");
                }
            } finally {
                writer.flush();
            }
            Thread.sleep(3_000L);
            assertEquals(0, logCount(s),
                    "before the stall the watermark is below w0 — nothing may close");

            // Stall: ~4s real time, well under SOURCE_IDLE_MS (15s) — the watermark
            // stays frozen and NO window may close (no phantom candles on a stall).
            Thread.sleep(4_000L);
            assertEquals(0, logCount(s),
                    "watermark stall must freeze output — no phantom candle closes");

            // Resume: ticks into w1..w8 (offsets 0/100/300) then a pusher at w9
            // offset 5000 -> watermark end(w8)-1, closing exactly w0..w8 (9 windows
            // x 2 tokens = 18 rows), each with its 3 pre-stall ticks.
            AppendWriter resume = s.raw().newAppend().createWriter();
            try {
                for (int w = 1; w <= 8; w++) {
                    for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                        appendTick(resume, token, w, 0L, 10_000L + w, "");
                        appendTick(resume, token, w, 100L, 10_050L + w, "");
                        appendTick(resume, token, w, 300L, 9_950L + w, "");
                    }
                }
                appendPusher(resume, 9, 10_009L);
            } finally {
                resume.flush();
            }
            awaitLogCount(s, 18,
                    "resume must close exactly the windows whose end the watermark passes", 120);
            Map<CandleKey, List<CandleRow>> log = readLogMap(s);
            assertEquals(18, log.size(), "LOG must hold exactly the 18 closed windows");
            for (int w = 0; w <= 8; w++) {
                for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                    List<CandleRow> rows = log.get(new CandleKey(token, BASE + w * WINDOW_MS));
                    assertTrue(rows != null && rows.size() == 1,
                            "window " + w + " token " + token + " must have closed exactly once");
                    CandleRow row = rows.get(0);
                    assertEquals(3, row.tickCount(),
                            "window " + w + " must fold exactly its 3 pre-stall ticks");
                    assertEquals(9_950L + w, row.close(),
                            "window " + w + " close = last pre-stall tick");
                }
            }
            assertEquals(0, candidateIds(s).size(),
                    "flat windows must not fire a window-driven (breakout) signal");

            // P6.3 623, end-to-end: an event time at Long.MAX_VALUE must be REJECTED
            // by the validation gate (event-time-overflow-window guard) — it can
            // never produce a TOKEN_BAD candle. The watermark it carries is emitted
            // at the source (before validation), so it legitimately closes the
            // still-open w9 (1 pusher tick -> LOG 18 -> 20); the row itself is
            // dropped and leaves no trace.
            AppendWriter overflow = s.raw().newAppend().createWriter();
            try {
                overflow.append(toRawRow(TOKEN_BAD, Long.MAX_VALUE, "fp-max-event-time",
                        "TRADE", 10_000L, 100L)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } finally {
                overflow.flush();
            }
            awaitLogCount(s, 20, "overflow watermark must close w9 (single pusher tick)", 60);
            Map<CandleKey, List<CandleRow>> log2 = readLogMap(s);
            assertEquals(20, log2.size(), "LOG = 18 pre-overflow + w9 for both tokens");
            for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                List<CandleRow> w9 = log2.get(new CandleKey(token, BASE + 9 * WINDOW_MS));
                assertTrue(w9 != null && w9.size() == 1, "w9 must close for token " + token);
                assertEquals(1, w9.get(0).tickCount(), "w9 must contain only the pusher tick");
                assertEquals(10_009L, w9.get(0).close(), "w9 close = the pusher tick price");
            }
            assertTrue(log2.keySet().stream().noneMatch(k -> k.token() == TOKEN_BAD),
                    "the overflow row must never produce a TOKEN_BAD candle");
            Thread.sleep(3_000L);
            assertEquals(20, logCount(s),
                    "post-resume LOG must stay at 20 — no duplicates, no continued growth");
            LOG.info("p63: stall freeze + resume + overflow-rejection verified — KV=20, "
                    + "window-driven candidates=0, no TOKEN_BAD candle");
            cancelAndFinish(job, "stall");
        } finally {
            cluster.after();
        }
    }

    // ── harness (same shape as P4.3) ────────────────────────────────────────

    private static MiniClusterWithClientResource newMiniCluster() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
    }

    /**
     * Dev-mode env for the real {@link SignalJob} graph. {@code checkpointDirOverride}
     * lets tests inject the read-only dir; {@code restartAttempts} drives the
     * configured restart behavior (0 = fail fast, no restart).
     */
    private static Map<String, String> baseEnv(ScratchSet s, String recovery, String backend,
            String checkpointDirOverride, String restartAttempts) {
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
        e.put("RESTART_MAX_ATTEMPTS", restartAttempts);
        e.put("RESTART_DELAY_MS", "1000");
        e.put("DEPLOYMENT_ENV", "dev");
        e.put("STATE_BACKEND", backend);
        if ("rocksdb".equals(backend)) {
            e.put("STATE_BACKEND_LOCAL_DIRS", s.rocksDir().toAbsolutePath().toString());
        }
        e.put("CHECKPOINT_DIR", checkpointDirOverride);
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
        LOG.info("p63: job {} RUNNING", name);
        return client;
    }

    private static JobStatus awaitJobStatus(JobClient client, int timeoutSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                Thread.sleep(300);
                continue;
            }
            if (last == JobStatus.RUNNING || last == JobStatus.FINISHED || last == JobStatus.FAILED
                    || last == JobStatus.CANCELED) {
                return last;
            }
            Thread.sleep(2_000L);
        }
        return last;
    }

    /**
     * Polls until FAILED (records every observed status). 50 ms cadence: with
     * RESTART_MAX_ATTEMPTS=0 the job's FAILING window is just the failover
     * handshake (cancel + wait, ~100-400 ms on the in-process MiniCluster), so
     * the 300 ms cadence used to miss it and the shared-fate assertion saw
     * only [RUNNING, FAILED].
     */
    private static JobStatus awaitTerminal(JobClient client, Set<JobStatus> seen, int timeoutSeconds)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                Thread.sleep(50);
                continue;
            }
            seen.add(last);
            if (last == JobStatus.FAILED || last == JobStatus.CANCELED || last == JobStatus.FINISHED) {
                return last;
            }
            Thread.sleep(50);
        }
        fail("job did not reach a terminal state in " + timeoutSeconds + "s; last=" + last
                + " seen=" + seen);
        return null; // unreachable
    }

    /** Root-cause chain of a failed job (or the timeout note when not yet terminal). */
    private static String jobFailureCause(JobClient client) {
        try {
            client.getJobExecutionResult().get(5, TimeUnit.SECONDS);
            return "no failure (job succeeded?)";
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            Throwable c = e;
            int depth = 0;
            while (c != null && depth++ < 12) {
                sb.append(c).append('\n');
                c = c.getCause();
            }
            return sb.toString().trim();
        }
    }

    private static void cancelAndFinish(JobClient client, String phase) throws Exception {
        try {
            client.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.warn("p63: cancel({}) failed (already terminal?): {}", phase, e.getMessage());
        }
        awaitTrue(() -> {
            try {
                JobStatus st = client.getJobStatus().get(5, TimeUnit.SECONDS);
                return st == JobStatus.CANCELED || st == JobStatus.FAILED || st == JobStatus.FINISHED;
            } catch (Exception e) {
                return false;
            }
        }, "phase " + phase + " job terminal", 60);
    }

    // ── feed ────────────────────────────────────────────────────────────────

    /** 46-row canonical feed (same as P6/P4.3): 23 windows x 2 tokens + late/beyond/invalid rows. */
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

    private static void appendPusher(AppendWriter writer, int w, long base) throws Exception {
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            appendTick(writer, token, w, 5_000L, base, "P");
        }
    }

    private static void appendTick(AppendWriter writer, long token, int w, long offset, long price,
            String suffix) throws Exception {
        long eventTime = BASE + w * WINDOW_MS + offset;
        writer.append(toRawRow(token, eventTime, "fp-" + token + "-" + w + "-" + offset + suffix,
                "TRADE", price, 100L)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 20-column raw row mirroring DDL 02; default validity/schema (same layout as P6). */
    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty) {
        return GenericRow.of(
                bs(fingerprint), bs("v2"), bs("p63-conn"), 1L, token, bs("NSE"), bs("TEST"),
                eventTime, eventTime, eventTime, bs(tickType), price, qty, null,
                bs("h-" + fingerprint), bs("v2"), bs("1.0"), bs("VALID_TRADE"), null, bs("2"));
    }

    // ── reads ───────────────────────────────────────────────────────────────

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

    /** Waits until the LOG is stable for >=3 polls AND a completed chk-N exists; returns it. */
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
                LOG.info("p63: LOG stable at {} rows for {} polls; restore target {}", count, stablePolls, latest);
                return latest;
            }
            Thread.sleep(5_000L);
        }
        fail("Timed out waiting for a stable checkpoint (LOG=" + last + ", latest chk=" + latest + ")");
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

    /** Every closed candle in the LOG keyed by (token, window_start). */
    private static Map<CandleKey, List<CandleRow>> readLogMap(ScratchSet s) throws Exception {
        Map<CandleKey, List<CandleRow>> map = new HashMap<>();
        for (InternalRow r : scanAll(s.log(), s.logInfo())) {
            map.computeIfAbsent(new CandleKey(r.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                    r.getLong(CandleTableColumns.WINDOW_START)), k -> new ArrayList<>())
                    .add(candleRow(r));
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

    // ── scratch tables ──────────────────────────────────────────────────────

    private static ScratchSet createSet() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        Path cpDir = Path.of(System.getProperty("java.io.tmpdir"), "p63-cp-" + suffix);
        Path rocksDir = Path.of(System.getProperty("java.io.tmpdir"), "p63-rocks-" + suffix);
        String rawName = "p63_" + suffix + "_raw";
        String logName = "p63_" + suffix + "_log";
        String candName = "p63_" + suffix + "_cand";
        Table raw = createTable(rawName, rawSchema(), null, 1, "raw LOG");
        Table log = createTable(logName, candleSchema(), List.of("instrument_token", "window_start"),
                16, "candle KV");
        Table cand = createTable(candName, candidatesSchema(), null, 16, "signal LOG");
        return new ScratchSet(suffix, rawName, logName, candName,
                raw, log, cand,
                tableInfo(rawName), tableInfo(logName), tableInfo(candName),
                cpDir, rocksDir);
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
        LOG.info("p63: created scratch {} table {}", what, name);
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
            Path checkpointDir,
            Path rocksDir) {}

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
