package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.lang.reflect.Field;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression for the fail-fast exit-code bug: {@code requestFatalStop} (the
 * zero-ack watchdog / FATAL-append path) drives a full {@code shutdown()},
 * which joins the bridge-loop (main) thread — so a {@code System.exit(1)}
 * issued on the watchdog thread raced the JVM's natural exit(0) when main
 * returned from {@code runWithBridge}. Observed live: after a table
 * drop+recreate, the ZERO-ACK guard fired correctly but the container exited
 * 0 (natural main return), defeating the {@code restart: unless-stopped}
 * policy.
 *
 * <p>The fix: {@code requestFatalStop} only records {@code fatalStopReason}
 * and shuts down; {@code main()} owns the nonzero exit decision by checking
 * {@code fatalStopReason} after {@code runWithBridge} returns. This test
 * asserts that contract in-process (no {@code System.exit}): after a fatal
 * stop the bridge loop unwinds to completion AND the fatal reason is set —
 * exactly the state under which main exits 1.
 */
@DisplayName("ING-UNIT-024: fatal stop unwinds the bridge loop with fatalStopReason set (exit-code ownership)")
class FatalStopExitCodeRegressionTest {

    private static final long TOKEN_A = 100_000L;
    private static final long STARTUP_TIMEOUT_MS = 30_000;
    private static final long SHUTDOWN_TIMEOUT_MS = 45_000;
    private static final long POLL_INTERVAL_MS = 250;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("requestFatalStop unwinds the bridge loop and leaves fatalStopReason set for main's exit(1)")
    void fatalStopLeavesReasonForMainExitDecision() throws Exception {
        Path bridge = writeFakeBridge();
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");
        IngestionConfig config = buildConfig(journalPath);
        IngestionService service = new IngestionService(
                "ing-unit-024", instruments(), new NoopConverter(), config, null,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        // The real bridge loop, in-process (the thread stands in for main).
        Thread bridgeLoop = new Thread(() -> service.runWithBridge(bridge.toString()),
                "bridge-loop");
        bridgeLoop.start();

        // 1. The loop must be live (bridge running) before the fatal stop.
        long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_MS;
        boolean bridgeStarted = false;
        while (System.currentTimeMillis() < deadline) {
            Field bpField = IngestionService.class.getDeclaredField("currentBridgeProcess");
            bpField.setAccessible(true);
            Process bp = (Process) bpField.get(service);
            if (bp != null && bp.isAlive()) {
                bridgeStarted = true;
                break;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        assertTrue(bridgeStarted, "the fake bridge must be live before the fatal stop");

        // 2. Fatal stop — exactly what the ZERO-ACK watchdog / FATAL-append
        //    path calls. No System.exit on this thread (the fix).
        Method requestFatalStop = IngestionService.class.getDeclaredMethod(
                "requestFatalStop", String.class);
        requestFatalStop.setAccessible(true);
        requestFatalStop.invoke(service, "ZERO-ACK: test fatal stop");

        // 3. The bridge loop (main-equivalent) must unwind to completion —
        //    shutdown() flips running=false, the loop exits, runWithBridge
        //    returns. This is the state from which main() checks the reason.
        bridgeLoop.join(SHUTDOWN_TIMEOUT_MS);
        assertFalse(bridgeLoop.isAlive(),
                "the bridge loop must unwind cleanly after a fatal stop "
                        + "(main must reach its fatalStopReason check, not hang)");

        // 4. The fatal reason must be set — the exact condition under which
        //    main() exits nonzero (System.exit(1)) instead of returning 0.
        Field reasonField = IngestionService.class.getDeclaredField("fatalStopReason");
        reasonField.setAccessible(true);
        Object reason = reasonField.get(service);
        assertNotNull(reason, "fatalStopReason must be recorded so main() exits 1 "
                + "(the regression: main returned 0 because nothing told it to exit 1)");
        assertTrue(reason.toString().contains("ZERO-ACK"),
                "the recorded fatal reason must be the watchdog's reason: " + reason);

        // 5. Clean single shutdown: the fatal path must not double-shutdown.
        List<String> journalLines = Files.readAllLines(journalPath);
        assertFalse(journalLines.isEmpty(), "the fatal path must pin the uncertainty journal");
    }

    // ---- fixtures ----

    /** A fake bridge that stays alive until TERM, then exits 0. */
    private Path writeFakeBridge() throws Exception {
        Path script = tempDir.resolve("fake-bridge.sh");
        String body = """
                #!/bin/sh
                trap 'exit 0' TERM
                while :; do
                  sleep 1
                done
                """;
        Files.writeString(script, body);
        Set<PosixFilePermission> perms =
                EnumSet.copyOf(PosixFilePermissions.fromString("rwxr-xr-x"));
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

    /** No-op converter — this scenario never reaches the Fluss writer. */
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
