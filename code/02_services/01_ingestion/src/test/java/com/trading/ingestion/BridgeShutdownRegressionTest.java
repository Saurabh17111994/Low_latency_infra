package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ING-UNIT-023: the bridge's final tick-count report must be drained from
 * stderr after the graceful SIGTERM path — the CHG-015 regression (revived
 * from the superseded {@code a980a33} draft and reconciled with the landed
 * M3 design: a scripted fake bridge + the ING-DQ-010 no-op-sink seam,
 * in-process, no Fluss and no Go binaries).
 *
 * <p>Scenario: {@code runWithBridge} launches a scripted fake bridge whose
 * stderr carries the per-second {@code arrow-tick-counts} reports (the real
 * bridge's report channel). While the loop is live, {@code shutdown()} is
 * invoked exactly as the JVM shutdown hook would: {@code signalBridge()}
 * sends {@code kill -TERM} — NOT {@code Process.destroy()}, which would close
 * the parent-side pipes and kill the stderr drain thread mid-read. The fake
 * bridge's TERM trap writes its FINAL report ({@code total=99}) to stderr and
 * exits 0; the pipes stay open, so the drain thread re-logs the final report
 * <b>after</b> the signal, and the main loop unwinds cleanly on EOF.
 *
 * <p>Assertions: the final report appears in the captured logs after
 * {@code signaling arrow-bridge (SIGTERM)} (it was drained from stderr, not
 * just persisted to the counter file); the drain never hits
 * {@code stderr drain error} (the destroy()-closes-pipe artifact); the loop
 * unwinds with {@code bridge loop ended}; the uncertainty journal records
 * exactly one shutdown entry.
 */
@DisplayName("ING-UNIT-023: bridge final tick-count report drained from stderr on SIGTERM (CHG-015 regression)")
class BridgeShutdownRegressionTest {

    private static final long TOKEN_A = 100_000L;

    private static final String SIGNAL_LINE = "signaling arrow-bridge (SIGTERM)";
    private static final String FINAL_REPORT = "arrow-tick-counts: total=99";
    private static final String PERIODIC_REPORT = "arrow-tick-counts: total=";
    private static final String DRAIN_ERROR = "stderr drain error";

    private static final long STARTUP_TIMEOUT_MS = 30_000;
    private static final long SHUTDOWN_TIMEOUT_MS = 45_000;
    private static final long POLL_INTERVAL_MS = 250;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("final arrow-tick-counts report is drained from stderr after the SIGTERM signal")
    void finalReportDrainedFromStderrOnSigterm() throws Exception {
        Path bridge = writeGracefulBridge();
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");
        IngestionConfig config = buildConfig(journalPath);
        IngestionService service = new IngestionService(
                "ing-unit-023", instruments(), new NoopConverter(), config, null,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        CapturingAppender capture = attachLogCapture("ing-unit-023-capture");
        try {
            // The real bridge loop, in-process: the fake bridge keeps stdout
            // open and feeds the periodic tick-count reports to stderr.
            Thread bridgeLoop = new Thread(() -> service.runWithBridge(bridge.toString()),
                    "bridge-loop");
            bridgeLoop.start();

            // 1. The drain must be live before we tear down: wait for the first
            //    periodic report to be drained from the bridge's stderr.
            awaitLog(capture, PERIODIC_REPORT, STARTUP_TIMEOUT_MS,
                    "the bridge's per-second tick-count reports must be drained and re-logged");

            // 2. Graceful shutdown — the same path the JVM shutdown hook runs
            //    on SIGTERM: signalBridge (kill -TERM, pipes preserved), wait
            //    for the bridge to exit, join the stderr drain.
            Method shutdown = IngestionService.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(service);

            bridgeLoop.join(SHUTDOWN_TIMEOUT_MS);
            assertFalse(bridgeLoop.isAlive(),
                    "the bridge loop must unwind cleanly after shutdown; log tail:\n"
                            + tail(capture));

            List<String> logs = List.copyOf(capture.messages);

            // 3. The signal must be logged…
            int signalIdx = indexOf(logs, SIGNAL_LINE);
            assertTrue(signalIdx >= 0,
                    "the shutdown path must log '" + SIGNAL_LINE + "'; log tail:\n"
                            + tail(capture));

            // 4. …and the FINAL report must be drained from stderr AFTER the
            //    signal (the pipe-preserving kill -TERM keeps the drain thread
            //    alive long enough to re-log it).
            boolean finalReportDrained = false;
            for (int i = signalIdx + 1; i < logs.size(); i++) {
                if (logs.get(i).contains(FINAL_REPORT)) {
                    finalReportDrained = true;
                    break;
                }
            }
            assertTrue(finalReportDrained,
                    "the bridge's final tick-count report (" + FINAL_REPORT
                            + ") must be drained from stderr after the SIGTERM signal "
                            + "(a Process.destroy() regression would close the pipes first); "
                            + "log around the signal:\n" + around(logs, signalIdx));

            // 5. The destroy()-closes-pipe artifact must be absent: the drain
            //    thread saw EOF, not a mid-read exception.
            assertFalse(logs.stream().anyMatch(l -> l.contains(DRAIN_ERROR)),
                    "the bridge-stderr drain must not error on the graceful path "
                            + "(kill -TERM preserves the pipe); log tail:\n" + tail(capture));

            // 6. The main loop unwound cleanly (EOF after the bridge exited),
            //    not via the exception path.
            assertTrue(logs.stream().anyMatch(l -> l.contains("bridge loop ended")),
                    "the bridge loop must unwind to its end-of-loop report; log tail:\n"
                            + tail(capture));

            // 7. Exactly one shutdown: one uncertainty-journal entry.
            assertEquals(1, Files.readAllLines(journalPath).size(),
                    "the graceful path must shut down exactly once");
        } finally {
            detachLogCapture(capture);
        }
    }

    // ---- fixtures (mirror BridgeCrashLoopE2ETest / ShutdownDeadlockTest) ----

    /**
     * A scripted fake bridge: writes a per-second {@code arrow-tick-counts}
     * report to stderr; on SIGTERM writes the FINAL report ({@code total=99})
     * to stderr and exits 0 — the real bridge's graceful drain behavior, no
     * Go binary required.
     */
    private Path writeGracefulBridge() throws Exception {
        Path script = tempDir.resolve("fake-bridge.sh");
        String body = """
                #!/bin/sh
                # CHG-015 regression fixture: per-second tick-count reports on
                # stderr; on TERM, the FINAL report (total=99) then exit 0.
                trap 'echo "arrow-tick-counts: total=99" >&2; exit 0' TERM
                i=0
                while :; do
                  i=$((i + 1))
                  echo "arrow-tick-counts: total=$i" >&2
                  sleep 1
                done
                """;
        Files.writeString(script, body);
        Set<PosixFilePermission> perms = EnumSet.copyOf(PosixFilePermissions.fromString("rwxr-xr-x"));
        Files.setPosixFilePermissions(script, perms);
        return script;
    }

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
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static List<com.trading.ingestion.model.Instrument> instruments() {
        return List.of(new com.trading.ingestion.model.Instrument.Builder()
                .instrumentToken(TOKEN_A).tradingSymbol("SYM1-EQ")
                .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build());
    }

    private static int indexOf(List<String> logs, String needle) {
        for (int i = 0; i < logs.size(); i++) {
            if (logs.get(i).contains(needle)) {
                return i;
            }
        }
        return -1;
    }

    private static String around(List<String> logs, int idx) {
        int from = Math.max(0, idx - 3);
        int to = Math.min(logs.size(), idx + 6);
        return String.join("\n", logs.subList(from, to));
    }

    private static String tail(CapturingAppender capture) {
        List<String> messages = capture.messages;
        int from = Math.max(0, messages.size() - 40);
        return String.join("\n", messages.subList(from, messages.size()));
    }

    /** Poll the captured log until {@code needle} appears or the deadline passes. */
    private static void awaitLog(CapturingAppender capture, String needle, long timeoutMs,
                                 String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (capture.messages.stream().anyMatch(l -> l.contains(needle))) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        fail(message + " — marker '" + needle + "' not seen within " + timeoutMs
                + "ms; captured log tail:\n" + tail(capture));
    }

    // ---- log4j2 capture (same minimal AbstractAppender as BridgeCrashLoopE2ETest) ----

    static final class CapturingAppender extends AbstractAppender {
        final List<String> messages = new CopyOnWriteArrayList<>();

        CapturingAppender(String name) {
            super(name, null, (Layout<?>) null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage() == null
                    ? "" : event.getMessage().getFormattedMessage());
        }
    }

    private static CapturingAppender attachLogCapture(String name) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration cfg = ctx.getConfiguration();
        CapturingAppender appender = new CapturingAppender(name);
        appender.start();
        cfg.getRootLogger().addAppender(appender, null, null);
        ctx.updateLoggers();
        return appender;
    }

    private static void detachLogCapture(CapturingAppender appender) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration cfg = ctx.getConfiguration();
        cfg.getRootLogger().removeAppender(appender.getName());
        ctx.updateLoggers();
        appender.stop();
    }

    // ---- no-op sinks / converter (ING-DQ-010 seam) ----

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

    /** No-op converter — the SIGTERM-drain scenario never reaches the writer. */
    static final class NoopConverter implements FlussRowConverter {
        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(
                com.trading.ingestion.model.TickPacket packet) {
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1, "p0"));
        }

        @Override
        public int estimatedRowSize(com.trading.ingestion.model.TickPacket packet) {
            return 256;
        }

        @Override
        public void close() {
        }
    }
}
