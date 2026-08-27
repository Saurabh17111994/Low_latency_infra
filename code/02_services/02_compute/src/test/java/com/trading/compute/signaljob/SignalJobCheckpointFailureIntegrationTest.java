package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * SIG-FAIL-001 (feature-path half): checkpoint/continuity failure → safe halt —
 * the HOST-RUNNABLE shell-level checkpoint-failure test (the live-cluster P6.2
 * leg of {@link CandleFailureInjectionIntegrationTest} covers the same failure
 * path against the real Fluss topology; this class proves the same behavior on
 * an embedded MiniCluster with {@code file://} checkpoints — no dev cluster, no
 * Fluss — and asserts the FULL ×3 restart count, not just "RESTARTING seen").
 *
 * <p><b>What is proven.</b> A checkpoint failure (injected the P6.2 way: the
 * job-id checkpoint directory is stripped of write permission after a healthy
 * checkpoint completes, so the next checkpoint cannot create its location —
 * the same checkpoint-failure path Flink's timeout uses from the restart
 * strategy's perspective) must drive the job through the CONFIGURED fixed-delay
 * restart policy — exactly {@code RESTART_MAX_ATTEMPTS} restart episodes, each
 * with the configured {@code RESTART_DELAY_MS} — and then FAIL the job. The job
 * never continues in a degraded state: no unsafe continuation.
 *
 * <p><b>Restart-strategy wiring is the production route.</b> The restart
 * strategy is applied through the same declarative {@code RestartStrategyOptions}
 * keys {@code SignalJob.buildTopology} sets (Flink 2.2.1 removed the
 * programmatic {@code RestartStrategies} API), and the checkpoint directory is
 * the same {@code CheckpointingOptions.CHECKPOINTS_DIRECTORY} route. Dev-mode
 * env override keeps the test fast: {@code RESTART_MAX_ATTEMPTS=3} ×
 * {@code RESTART_DELAY_MS=1000} (dev is explicitly overridable for the
 * failure-injection ITs per SignalJobConfig; production pins 3 × 30000 and the
 * pin itself is covered by {@code SignalJobConfigTest}).
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_SIG_FAIL=true)}
 * — skipped in the normal suite (MiniCluster). Host-runnable: embedded
 * MiniCluster + {@code file://} checkpoints — no external cluster, no Fluss, no
 * S3. Run:
 * {@code COMPUTE_INT_TEST_SIG_FAIL=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SignalJobCheckpointFailureIntegrationTest}
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_SIG_FAIL", matches = "true")
@DisplayName("SIG-FAIL-001: checkpoint failure -> exactly N fixed-delay restarts -> FAILED, never degraded")
class SignalJobCheckpointFailureIntegrationTest {

    private static final Duration POLL = Duration.ofMillis(50);
    private static final Duration TERMINAL_TIMEOUT = Duration.ofSeconds(180);

    /** Serializable row spec — GenericRowData is not serializable. */
    private record RowSpec(long token, long eventTime, String fingerprint)
            implements java.io.Serializable {}

    /** Emits rows once, then parks so the job stays RUNNING for checkpoints. */
    private static final class EmitOnceThenPark implements SourceFunction<RowSpec> {
        private static final long serialVersionUID = 1L;
        private final List<RowSpec> rows;
        private volatile boolean cancelled;

        EmitOnceThenPark(List<RowSpec> rows) {
            this.rows = rows;
        }

        @Override
        public void run(SourceContext<RowSpec> ctx) {
            for (RowSpec row : rows) {
                ctx.collectWithTimestamp(row, row.eventTime);
            }
            while (!cancelled) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    /** Static collect sink (sink2 API) for the main output. */
    private static final class CollectingSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(RowData element, Context context) {
                    // accepted fingerprints collected nowhere — the failure path
                    // is what this test asserts, not the dedup output
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private static RowData toRow(RowSpec spec) {
        return TestRawRows.row(spec.token, spec.eventTime, spec.fingerprint, "TRADE", 100, 1);
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        // Dev-mode overrides for the failure-injection IT (SignalJobConfig:
        // dev keeps restart tuning overridable; production pins 3 × 30000).
        env.put("RESTART_MAX_ATTEMPTS", "3");
        env.put("RESTART_DELAY_MS", "1000");
        return env;
    }

    private static List<RowSpec> rows(String prefix, int count, long token, long startEventTime) {
        List<RowSpec> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(new RowSpec(token, startEventTime + i * 1_000L, prefix + "-" + i));
        }
        return out;
    }

    private static Configuration config(Path workDir, int restartAttempts, long restartDelayMs) {
        Configuration flinkConfig = new Configuration();
        // The SAME declarative route SignalJob.buildTopology uses for the
        // restart strategy and checkpoint storage (Flink 2.2.1: programmatic
        // RestartStrategies API removed — declarative Configuration only).
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.FIXED_DELAY.getMainValue());
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                restartAttempts);
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(restartDelayMs));
        flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + workDir);
        flinkConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file://" + workDir);
        flinkConfig.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        flinkConfig.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        return flinkConfig;
    }

    private static MiniClusterWithClientResource cluster(Configuration config, int parallelism) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberSlotsPerTaskManager(Math.max(4, parallelism))
                        .setNumberTaskManagers(1)
                        .build());
    }

    /**
     * The real dedup sub-graph (same shape as the compact-restore IT): source →
     * toRow → keyBy(token) → FingerprintDedupFunction → sink. The dedup set is
     * keyed state that must be checkpointed atomically; a checkpoint failure is
     * therefore a continuity failure of the whole job — the SIG-FAIL-001 target.
     */
    private static JobClient submit(Configuration config, List<RowSpec> feed, int parallelism)
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(parallelism);
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<RowData> deduped =
                env.addSource(new EmitOnceThenPark(feed)).uid("src")
                        .map(SignalJobCheckpointFailureIntegrationTest::toRow).uid("map")
                        .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                        .process(new FingerprintDedupFunction(SignalJobConfig.from(env())))
                        .uid("dedup");
        deduped.sinkTo(new CollectingSink()).uid("out");
        return env.executeAsync();
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TERMINAL_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL.toMillis());
        }
        assertTrue(condition.getAsBoolean(), "timed out waiting for " + what);
    }

    /** Newest completed checkpoint dir for the job, or null. */
    private static Path latestCompletedCheckpoint(Path cpRoot) {
        if (!Files.isDirectory(cpRoot)) {
            return null;
        }
        long best = -1;
        try (Stream<Path> entries = Files.list(cpRoot)) {
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
        return best < 0 ? null : cpRoot.resolve("chk-" + best);
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

    @Test
    @DisplayName("checkpoint failure -> exactly RESTART_MAX_ATTEMPTS fixed-delay restarts -> FAILED")
    void checkpointFailureDrivesConfiguredRestartsThenFails() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.WARN);
        int attempts = SignalJobConfig.from(env()).restartMaxAttempts();
        long delayMs = SignalJobConfig.from(env()).restartDelayMs();
        assertEquals(3, attempts, "fixture: dev override drives exactly 3 restart attempts");
        Path workDir = Files.createTempDirectory("sig-fail-001-");
        try {
            Path jobRoot = null;
            MiniClusterWithClientResource cluster =
                    cluster(config(workDir, attempts, delayMs), 2);
            cluster.before();
            try {
                JobClient job = submit(config(workDir, attempts, delayMs),
                        rows("fp", 50, 1L, System.currentTimeMillis()), 2);
                // Poll until RUNNING (a single getJobStatus() snapshot can catch
                // INITIALIZING on the in-process MiniCluster).
                awaitTrue("job to reach RUNNING", () -> {
                    try {
                        return job.getJobStatus().get(10, TimeUnit.SECONDS)
                                == JobStatus.RUNNING;
                    } catch (Exception e) {
                        return false;
                    }
                });

                // Healthy path FIRST: a checkpoint must complete on the writable
                // dir — the control proving the injection (not the job) is what
                // fails below. The job-id dir only appears once the first
                // checkpoint writes.
                String jobId = job.getJobID().toHexString();
                awaitTrue("healthy checkpoint on the writable dir",
                        () -> latestCompletedCheckpoint(
                                workDir.resolve(jobId)) != null);
                Path jobDir = workDir.resolve(jobId);
                assertTrue(Files.isDirectory(jobDir), "job checkpoint dir must exist");
                jobRoot = jobDir;

                // Injection: strip write from the job-id checkpoint dir so the
                // NEXT checkpoint (10 s interval) cannot create its location →
                // checkpoint failure → the CONFIGURED fixed-delay restart
                // (attempts=3, delay=1000 ms) → FAILED once attempts exhausted.
                // Same injection as the live P6.2 leg; a location-creation
                // failure and a timeout are the same checkpoint-failure path.
                Files.setPosixFilePermissions(jobDir,
                        PosixFilePermissions.fromString("r-xr-xr-x"));

                List<JobStatus> seen = Collections.synchronizedList(new ArrayList<>());
                int restartEpisodes = awaitRestartsThenTerminal(job, attempts, seen);
                assertEquals(attempts, restartEpisodes,
                        "exactly RESTART_MAX_ATTEMPTS restart episodes, seen=" + seen);
                assertEquals(JobStatus.FAILED, seen.get(seen.size() - 1),
                        "exhausted restart attempts must end FAILED, seen=" + seen);
                String cause = jobFailureCause(job);
                assertTrue(cause.toLowerCase().contains("checkpoint"),
                        "failure cause must be the checkpoint injection, got: " + cause);
                System.out.println("SIG-FAIL-001[checkpoint-failure] attempts=" + attempts
                        + " delayMs=" + delayMs + " restartEpisodes=" + restartEpisodes
                        + " terminal=FAILED cause~checkpoint — PASS");
            } finally {
                if (jobRoot != null) {
                    try {
                        Files.setPosixFilePermissions(jobRoot,
                                PosixFilePermissions.fromString("rwxr-xr-x"));
                    } catch (Exception ignored) {
                        // best-effort cleanup
                    }
                }
                cluster.after();
            }
        } finally {
            deleteRecursively(workDir);
        }
    }

    /**
     * Polls at 50 ms and records every observed status; counts RESTARTING
     * episodes (each restart: RUNNING → RESTARTING → RUNNING). Returns when the
     * job is terminal; asserts the terminal state is FAILED and that the
     * RESTARTING episodes match the configured attempts.
     */
    private static int awaitRestartsThenTerminal(JobClient client, int attempts,
            List<JobStatus> seen) throws Exception {
        long deadline = System.nanoTime() + TERMINAL_TIMEOUT.toNanos();
        JobStatus prev = null;
        int restartEpisodes = 0;
        while (System.nanoTime() < deadline) {
            JobStatus status;
            try {
                status = client.getJobStatus().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                Thread.sleep(POLL.toMillis());
                continue;
            }
            seen.add(status);
            // A restart episode starts when we observe RESTARTING after a
            // non-RESTARTING state (each episode is RUNNING→RESTARTING→RUNNING).
            if (status == JobStatus.RESTARTING && prev != JobStatus.RESTARTING) {
                restartEpisodes++;
            }
            prev = status;
            if (status == JobStatus.FAILED || status == JobStatus.CANCELED
                    || status == JobStatus.FINISHED) {
                assertTrue(status == JobStatus.FAILED,
                        "SIG-FAIL-001: terminal state must be FAILED (safe halt), got "
                                + status + ", seen=" + seen);
                return restartEpisodes;
            }
            Thread.sleep(POLL.toMillis());
        }
        fail("job did not reach a terminal state in " + TERMINAL_TIMEOUT.getSeconds()
                + "s; seen=" + seen);
        return -1; // unreachable
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // best-effort temp cleanup
                }
            });
        }
    }
}
