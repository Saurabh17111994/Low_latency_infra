package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * ING-FAIL-008: bridge crash-loop E2E with a scripted fake bridge binary.
 *
 * <p>The bridge is replaced by a shell script that exits non-zero immediately
 * (the scripted-bridge pattern from {@code FullStackE2ETest}, minus the live
 * Fluss cluster — the evidence sinks are the ING-DQ-010 no-op seam). Driving
 * the REAL {@code runWithBridge} subprocess loop proves the production
 * lifecycle end-to-end:
 *
 * <pre>{@code
 *   crash #1 → BRIDGE_CRASH logged, DROP discontinuity written,
 *              restart exactly once (BRIDGE_RESTART_WAIT_MS=1 s)
 *   crash #2 → BRIDGE_CRASH logged, DROP written, TERMINAL — the loop ends
 *              and the service shuts down cleanly (the "then exits 0" path
 *              in-process: runWithBridge returns without error)
 * }</pre>
 *
 * <p>Readiness marker: {@code READINESS_FILE_PATH} points at a temp file; the
 * marker is cleared by {@code shutdown()} — the container probe must not see
 * READY after the bridge is gone. The uncertainty journal records exactly one
 * shutdown entry.
 *
 * <p>Log capture: a minimal capturing log4j2 appender on the root logger
 * asserts the {@code BRIDGE_CRASH}, restart, and terminal lines the plan's
 * pass result names. The observable evidence (DROP rows, readiness file,
 * journal) is asserted independently of the logging backend.
 */
@DisplayName("ING-FAIL-008: bridge crash-loop — BRIDGE_CRASH + DROP evidence, restart once, then terminal")
class BridgeCrashLoopE2ETest {

    private static final long TOKEN_A = 100_000L;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("scripted bridge crashes twice: BRIDGE_CRASH + DROP per crash, one restart, then clean terminal shutdown")
    void crashLoopRestartsOnceThenTerminalWithEvidenceAndReadinessCleared() throws Exception {
        Path bridgeScript = writeCrashingBridge("exit 3");
        Path readinessPath = tempDir.resolve("ready.marker");
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");

        RecordingDiscontinuitySink discontinuities = new RecordingDiscontinuitySink();
        IngestionConfig config = buildConfig(journalPath);
        IngestionService service = new IngestionService(
                "ing-fail-008", instruments(), new NoopConverter(), config, null,
                noopQuarantine(), discontinuities, noopSafety());

        // READINESS_FILE_PATH is read from the environment at construction —
        // it was null above; set it before the service runs the bridge loop.
        java.lang.reflect.Field readinessField = IngestionService.class
                .getDeclaredField("readinessFile");
        readinessField.setAccessible(true);
        readinessField.set(service,
                new com.trading.ingestion.health.ReadinessFile(readinessPath));
        // Seed the marker as READY (as a healthy service would have it) so
        // the assertion below proves shutdown() CLEARS a present marker rather
        // than trivially observing an absent one.
        new com.trading.ingestion.health.ReadinessFile(readinessPath).setReady(true);
        assertTrue(Files.exists(readinessPath), "marker seeded before the crash loop");

        CapturingAppender capture = attachLogCapture("ing-fail-008-capture");
        try {
            // The full bridge loop: crash → restart → crash → terminal → shutdown.
            service.runWithBridge(bridgeScript.toString());
        } finally {
            detachLogCapture(capture);
        }

        String logs = String.join("\n", capture.messages);

        // ---- plan pass result: Java logs BRIDGE_CRASH, writes DROP per crash ----
        assertEquals(2, count(logs, "BRIDGE_CRASH exitCode=3"),
                "each unexpected bridge exit must log BRIDGE_CRASH");
        assertEquals(2, discontinuities.writes.size(),
                "each crash must write a DROP discontinuity (restart once = 2 exits)");
        assertTrue(discontinuities.writes.stream()
                        .allMatch(w -> w.reason == DiscontinuityWriter.Reason.DROP),
                "crash evidence is DROP-class");
        assertTrue(discontinuities.writes.stream()
                        .allMatch(w -> w.note.contains("crashed") && w.note.contains("code 3")),
                "DROP notes must name the crash and its exit code");

        // ---- plan pass result: restarts exactly once, then exits ----
        assertTrue(logs.contains("restarting bridge after unexpected exit (attempt 1 of 2)"),
                "first crash must restart (attempt 1 of 2)");
        assertTrue(logs.contains("bridge exited unexpectedly 2 time(s) — terminal"),
                "second crash must be terminal");
        assertTrue(logs.contains("bridge loop ended (ticks=0, errors=0, restarts=1)"),
                "the loop report must show exactly one restart");
        assertTrue(logs.contains("ingestion: drained"), "shutdown must complete the drain report");

        // ---- plan pass result: readiness marker cleared ----
        assertFalse(Files.exists(readinessPath),
                "shutdown must clear the readiness marker (probe sees no READY)");

        // ---- single shutdown: exactly one journal entry ----
        List<String> journalLines = Files.readAllLines(journalPath);
        assertEquals(1, journalLines.size(), "the terminal path shuts down exactly once");
    }

    // ---- fixtures ----

    /** A scripted fake bridge: runs then exits with the given shell line. */
    private Path writeCrashingBridge(String exitLine) throws Exception {
        Path script = tempDir.resolve("fake-bridge.sh");
        Files.writeString(script, "#!/bin/sh\n" + exitLine + "\n");
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

    private static long count(String haystack, String needle) {
        int idx = 0, n = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            n++;
            idx += needle.length();
        }
        return n;
    }

    // ---- log4j2 capture (ListAppender is stripped from the slimmed
    //      log4j-core on the classpath — use a minimal AbstractAppender) ----

    /** Captures every log message the root logger emits, in order. */
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

    // ---- recording discontinuity sink ----

    private static final class RecordingDiscontinuitySink implements DiscontinuitySink {
        final List<Write> writes = new CopyOnWriteArrayList<>();

        record Write(DiscontinuityWriter.Reason reason, String note) {}

        @Override
        public void write(DiscontinuityWriter.Reason reason, String note,
                          DiscontinuityWriter.LastTickSnapshot before) {
            writes.add(new Write(reason, note));
        }

        @Override
        public void write(DiscontinuityWriter.Reason reason, String note,
                          DiscontinuityWriter.LastTickSnapshot before,
                          Long instrumentToken, String exchange, String symbol) {
            writes.add(new Write(reason, note));
        }

        @Override
        public void writeBridgeEvent(com.trading.ingestion.bridge.BridgeEvent event,
                                     DiscontinuityWriter.LastTickSnapshot before) {
            writes.add(new Write(DiscontinuityWriter.Reason.DROP, event.reason()));
        }

        @Override
        public void close() {
        }
    }

    // ---- no-op sinks / converter ----

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

    /** No-op converter — the crash-loop scenario never reaches the writer. */
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
