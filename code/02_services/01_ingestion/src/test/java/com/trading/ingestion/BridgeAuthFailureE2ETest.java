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
 * ING-FAIL-009: auth-failure path E2E with a scripted fake bridge (bad
 * {@code ARROW_TOKEN} / revoked creds scenario).
 *
 * <p>The fake bridge mirrors the real Go bridge's terminal-auth drain
 * sequence exactly: emit an {@code auth_failure} bridge event (reason
 * {@code authentication_refresh_exhausted}), emit the {@code bridge_shutdown}
 * drain event, then exit with the FATAL code 2 ({@code exitFatalStart}).
 *
 * <p>Java must respond with the documented contract:
 * <ul>
 *   <li>the {@code auth_failure} event is discontinuity evidence — a DROP row
 *       with the auth reason is written (never silent)</li>
 *   <li>zero partial appends — nothing reached the writer before the failure</li>
 *   <li>the pipe close (bridge exit) is treated as a requested exit, so the
 *       bridge is NOT restarted — a revoked credential is terminal, not
 *       retryable — and the service unwinds to a clean shutdown</li>
 * </ul>
 *
 * <p>Default-run: the evidence sinks are the ING-DQ-010 no-op seam, so no
 * live Fluss is required; the bridge is a temp shell script.
 */
@DisplayName("ING-FAIL-009: auth failure — bridge exits 2, zero appends, auth_failure evidence, no restart")
class BridgeAuthFailureE2ETest {

    private static final long TOKEN_A = 100_000L;
    private static final String HASH_64 = "a".repeat(64);

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("revoked creds: auth_failure evidence written, zero appends, terminal exit 2 without restart")
    void authFailureWritesEvidenceAndNeverAppends() throws Exception {
        long now = System.currentTimeMillis();
        Path bridgeScript = writeAuthFailingBridge(now);
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");

        RecordingDiscontinuitySink discontinuities = new RecordingDiscontinuitySink();
        IngestionConfig config = buildConfig(journalPath);
        IngestionService service = new IngestionService(
                "ing-fail-009", instruments(), new NoopConverter(), config, null,
                noopQuarantine(), discontinuities, noopSafety());

        CapturingAppender capture = attachLogCapture("ing-fail-009-capture");
        try {
            service.runWithBridge(bridgeScript.toString());
        } finally {
            detachLogCapture(capture);
        }

        String logs = String.join("\n", capture.messages);

        // ---- zero partial appends ----
        assertEquals(0, service.tracker().totalAccepted(),
                "a failed-auth bridge must never contribute an accepted tick");

        // ---- auth_failure evidence row written (DROP-class, reason preserved) ----
        assertTrue(discontinuities.writes.stream().anyMatch(w ->
                        w.reason == DiscontinuityWriter.Reason.DROP
                                && w.note.contains("authentication_refresh_exhausted")),
                "the auth_failure event must produce DROP evidence naming the reason");

        // ---- Java logs the authentication failure ----
        assertTrue(logs.contains("bridge authentication failed"),
                "Java must log the authentication failure");

        // ---- bridge exits 2 and Java treats the pipe close as terminal ----
        assertTrue(logs.contains("bridge exited normally (code=2"),
                "the FATAL exit (code 2) must be recorded as a requested/terminal exit");
        assertFalse(logs.contains("restarting bridge"),
                "a revoked credential must NOT be restarted");
        assertTrue(logs.contains("bridge loop ended"), "the loop must unwind to the final report");

        // ---- clean single shutdown ----
        assertEquals(1, Files.readAllLines(journalPath).size(),
                "the auth-failure terminal path shuts down exactly once");
    }

    // ---- fixtures ----

    /**
     * A scripted fake bridge for the revoked-creds scenario: emit the
     * auth_failure bridge event, the bridge_shutdown drain event, then exit 2.
     * Proto-only transport (2026-08-29): control records are length-prefixed
     * TransportFrame protobufs (see {@link ProtoControlFrame}).
     */
    private Path writeAuthFailingBridge(long now) throws Exception {
        String authFailure = ProtoControlFrame.controlFrameB64(
                "auth_failure", "authentication_refresh_exhausted", "TERMINAL", now);
        String bridgeShutdown = ProtoControlFrame.controlFrameB64(
                "bridge_shutdown", "drain_complete", "TERMINAL", now);
        Path script = tempDir.resolve("fake-bridge-auth.sh");
        Files.writeString(script,
                "#!/bin/sh\n"
                        + ProtoControlFrame.shellDecode(authFailure)
                        + ProtoControlFrame.shellDecode(bridgeShutdown)
                        + "exit 2\n");
        Set<PosixFilePermission> perms = EnumSet.copyOf(PosixFilePermissions.fromString("rwxr-xr-x"));
        Files.setPosixFilePermissions(script, perms);
        return script;
    }

    private static IngestionConfig buildConfig(Path journalPath) throws Exception {
        Map<String, String> env = new HashMap<>();
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

    /** No-op converter — the auth-failure scenario never reaches the writer. */
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
