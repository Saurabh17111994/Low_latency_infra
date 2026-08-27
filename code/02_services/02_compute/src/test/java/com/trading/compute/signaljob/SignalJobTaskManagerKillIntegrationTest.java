package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * SIG-TMKILL-001 (failure chaos suite, T13 test 2): TaskManager death →
 * job restores from the last checkpoint and the first-write-wins dedup
 * markers survive, so the post-restore replay emits NO duplicate candle/
 * signal seed (no duplicate {@code event_fingerprint} ever leaves the dedup
 * boundary).
 *
 * <p><b>What is proven.</b> TaskManager loss must NOT lose the dedup state:
 * the job is deployed on a 2-TM MiniCluster (parallelism 2), a healthy
 * checkpoint completes, BOTH TaskManagers are terminated abruptly
 * ({@code MiniCluster#terminateTaskManager}, the same failover path a
 * container SIGKILL takes — with both TMs gone no task can survive, so the
 * failover is deterministic regardless of slot placement), a fresh
 * TaskManager joins ({@code MiniCluster#startTaskManager}, the cluster-side
 * replacement Flink performs on slot loss), and the fixed-delay restart
 * strategy re-deploys the job from the last completed checkpoint. Because a
 * legacy source re-emits its whole feed from scratch after restore, the
 * replayed rows must be suppressed by the restored first-write-wins markers.
 * Any marker loss would surface as a duplicate fingerprint in the collected
 * output, which is exactly the plan's "no dup window / no dup signal"
 * invariant ({@code FingerprintDedupFunction}
 * is the gate in front of candle emission and signal detection in
 * {@link SignalJob}; the KV-side first-write-wins layer,
 * {@link CandleKvFirstWriteWinsFunction}, is a second, restore-surviving
 * guard covered by the live-cluster leg of chaos-02).
 *
 * <p><b>Restart-strategy wiring is the production route.</b> The same
 * declarative {@code RestartStrategyOptions} keys {@code SignalJob.buildTopology}
 * sets, plus the same {@code CheckpointingOptions.CHECKPOINTS_DIRECTORY}
 * route, on an embedded MiniCluster with {@code file://} checkpoints — no
 * external cluster, no Fluss, no S3. Dev-mode overrides keep the test fast:
 * {@code RESTART_MAX_ATTEMPTS=10} × {@code RESTART_DELAY_MS=1000} ×
 * {@code CHECKPOINT_INTERVAL_MS=10000} (F005-pinned).
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_TM_KILL=true)}
 * — skipped in the normal suite. Run (host, offline):
 * {@code COMPUTE_INT_TEST_TM_KILL=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SignalJobTaskManagerKillIntegrationTest}
 * or via {@code code/01_platform/04_scripts/chaos/chaos-02-tm-kill.sh}.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_TM_KILL", matches = "true")
@DisplayName("SIG-TMKILL-001: TM kill -> restore from last checkpoint -> no duplicate fingerprint")
class SignalJobTaskManagerKillIntegrationTest {

    private static final Duration POLL = Duration.ofMillis(50);
    private static final Duration TERMINAL_TIMEOUT = Duration.ofSeconds(240);

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

    /**
     * Collecting sink (sink2 API): records the fingerprint of every row that
     * passes the dedup boundary, so the per-run duplicate check has ground
     * truth from the operator graph (not from a mocked collector).
     */
    private static final class FingerprintCollectingSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;
        private static final List<String> EMITTED =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(RowData element, Context context) {
                    EMITTED.add(element.getString(RawTableColumns.EVENT_FINGERPRINT).toString());
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
        // CHECKPOINT_INTERVAL_MS is F005-pinned to 10000 (fixed scope) — an
        // override is rejected by SignalJobConfig, which is itself a gate.
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        // Dev-mode overrides for the failure-injection IT (SignalJobConfig:
        // dev keeps restart tuning overridable; production pins 3 x 30000 —
        // a single TM kill must recover well within the budget here).
        env.put("RESTART_MAX_ATTEMPTS", "10");
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
        // The SAME declarative route SignalJob.buildTopology uses (Flink
        // 2.2.1: programmatic RestartStrategies API removed).
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
        // 2 TMs x 4 slots: both TMs are killed (zero surviving tasks -> the
        // job MUST fail), then a fresh TM joins to host the restore.
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberSlotsPerTaskManager(Math.max(4, parallelism))
                        .setNumberTaskManagers(2)
                        .build());
    }

    /** The dedup sub-graph: source -> toRow -> keyBy(token) -> dedup -> sink. */
    private static JobClient submit(Configuration config, List<RowSpec> feed, int parallelism)
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(parallelism);
        env.enableCheckpointing(5_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        FingerprintCollectingSink.EMITTED.clear();
        env.addSource(new EmitOnceThenPark(feed)).uid("src")
                .map(SignalJobTaskManagerKillIntegrationTest::toRow).uid("map")
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                .process(new FingerprintDedupFunction(SignalJobConfig.from(env())))
                .uid("dedup")
                .sinkTo(new FingerprintCollectingSink()).uid("out");
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

    /** Highest completed checkpoint id under the job root, or -1. */
    private static long latestCompletedCheckpointId(Path cpRoot) {
        if (!Files.isDirectory(cpRoot)) {
            return -1;
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
            return -1;
        }
        return best;
    }

    @Test
    @DisplayName("TM kill -> restart from last checkpoint -> replay suppressed (no duplicate fingerprints)")
    void taskManagerKillRestoresWithoutDuplicateOutput() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.WARN);
        int attempts = SignalJobConfig.from(env()).restartMaxAttempts();
        long delayMs = SignalJobConfig.from(env()).restartDelayMs();
        Path workDir = Files.createTempDirectory("sig-tmkill-001-");
        try {
            MiniClusterWithClientResource cluster =
                    cluster(config(workDir, attempts, delayMs), 2);
            cluster.before();
            try {
                List<RowSpec> feed = rows("tmk", 50, 1L, System.currentTimeMillis());
                JobClient job = submit(config(workDir, attempts, delayMs), feed, 2);

                awaitTrue("job to reach RUNNING", () -> {
                    try {
                        return job.getJobStatus().get(10, TimeUnit.SECONDS)
                                == JobStatus.RUNNING;
                    } catch (Exception e) {
                        return false;
                    }
                });
                String jobId = job.getJobID().toHexString();
                Path jobRoot = workDir.resolve(jobId);

                // Healthy path FIRST: a completed checkpoint + the dedup
                // markers actually emitted (control for the injection).
                awaitTrue("healthy checkpoint before the kill",
                        () -> latestCompletedCheckpointId(jobRoot) > 0);
                awaitTrue("dedup output before the kill",
                        () -> FingerprintCollectingSink.EMITTED.size() >= feed.size());
                long cpBeforeKill = latestCompletedCheckpointId(jobRoot);
                int emittedBeforeKill = FingerprintCollectingSink.EMITTED.size();
                System.out.println("SIG-TMKILL-001[before-kill] checkpoint=" + cpBeforeKill
                        + " emitted=" + emittedBeforeKill);

                // Injection: hard-kill BOTH TaskManagers (ContainerKilled-
                // class failover — the same path as `docker kill` on the TM).
                // With zero TMs no task can survive, so the job MUST fail and
                // the restore path is exercised deterministically (a single-
                // TM kill is placement-dependent: MiniCluster may schedule
                // both subtasks on the surviving TM).
                cluster.getMiniCluster().terminateTaskManager(0)
                        .get(30, TimeUnit.SECONDS);
                cluster.getMiniCluster().terminateTaskManager(1)
                        .get(30, TimeUnit.SECONDS);

                // The job must leave RUNNING (RESTARTING/reconciling).
                List<JobStatus> seen = Collections.synchronizedList(new ArrayList<>());
                awaitTrue("job to leave RUNNING after TM kill", () -> {
                    try {
                        JobStatus s = job.getJobStatus().get(10, TimeUnit.SECONDS);
                        seen.add(s);
                        return s != JobStatus.RUNNING;
                    } catch (Exception e) {
                        return false;
                    }
                });
                // A fresh TM joins (the cluster-side replacement Flink runs
                // when slot resources disappear), so the restart re-deploys.
                cluster.getMiniCluster().startTaskManager();

                // Job restores from the last completed checkpoint and comes
                // back RUNNING on the replacement TM.
                awaitTrue("job to restart after TM kill", () -> {
                    try {
                        JobStatus s = job.getJobStatus().get(10, TimeUnit.SECONDS);
                        seen.add(s);
                        return s == JobStatus.RUNNING;
                    } catch (Exception e) {
                        return false;
                    }
                });
                assertTrue(seen.contains(JobStatus.RESTARTING) || seen.contains(JobStatus.RECONCILING)
                                || seen.stream().filter(s -> s != JobStatus.RUNNING).count() > 0,
                        "job must have left RUNNING after the TM kills, seen=" + seen);

                // Restore from the last checkpoint, then a FRESH completed
                // checkpoint must follow (the job is live, not stuck).
                awaitTrue("fresh checkpoint after restore",
                        () -> latestCompletedCheckpointId(jobRoot) > cpBeforeKill);

                // The legacy source re-emits its entire feed after restore;
                // restored first-write-wins markers must suppress every
                // replayed row. No duplicate fingerprint may ever reach the
                // sink — the plan's "no dup candle / no dup signal" invariant
                // at the dedup boundary.
                awaitTrue("replay to drain through the restored pipeline", () -> {
                    int n = FingerprintCollectingSink.EMITTED.size();
                    return n >= emittedBeforeKill && latestCompletedCheckpointId(jobRoot)
                            > cpBeforeKill + 1;
                });

                List<String> emitted = new ArrayList<>(FingerprintCollectingSink.EMITTED);
                Set<String> distinct = new HashSet<>(emitted);
                assertEquals(distinct.size(), emitted.size(),
                        "duplicate fingerprint after TM kill + restore (first-write-wins"
                                + " markers must be restored from the checkpoint): " + emitted);
                assertEquals(feed.size(), distinct.size(),
                        "every feed fingerprint must pass the dedup boundary exactly once");
                assertEquals(JobStatus.RUNNING,
                        job.getJobStatus().get(10, TimeUnit.SECONDS),
                        "job must stay RUNNING after the TM-kill recovery (no restart-budget exhaustion)");
                System.out.println("SIG-TMKILL-001[after-kill] checkpoint="
                        + latestCompletedCheckpointId(jobRoot) + " emitted=" + emitted.size()
                        + " distinct=" + distinct.size() + " job="
                        + job.getJobStatus().get(10, TimeUnit.SECONDS));
            } finally {
                try {
                    cluster.after();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        } catch (Exception e) {
            fail("SIG-TMKILL-001 failed: " + e, e);
        }
        System.out.println("TM-KILL-CHAOS-02: RESULT=PASS EXIT=0 (MiniCluster leg)");
    }
}
