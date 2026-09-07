package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ING-UNIT-024: the REAL JVM shutdown-hook path — a spawned driver JVM
 * ({@link BridgeShutdownDriver}, mirroring {@code IngestionService.main()}'s
 * wiring with the ING-DQ-010 seam + a scripted fake bridge) is SIGTERMed, and
 * the hook runs the real {@code shutdown()} on the hook thread: signal the
 * bridge via {@code kill -TERM}, drain its stderr, write the uncertainty
 * journal, and bounded-join the main thread.
 *
 * <p>This is the layer the in-process ING-UNIT-023 skips: {@code mainThread}
 * capture, hook registration, and the main-thread join that lets the loop
 * flush its final {@code bridge loop ended} report before the JVM halts. A
 * broken join (or a hook that hangs) surfaces here as a JVM that never halts
 * (30 s {@code waitFor} fails) or halts before the main thread's final report
 * is flushed.
 *
 * <p>Assertions: the driver exits 143 (128 + SIGTERM); the final
 * {@code arrow-tick-counts} report is drained from the bridge's stderr AFTER
 * {@code signaling arrow-bridge (SIGTERM)}; no {@code stderr drain error}; the
 * main loop unwound to {@code bridge loop ended} before the halt; exactly one
 * uncertainty-journal entry. POSIX-only (SIGTERM semantics; exit 143).
 */
@DisplayName("ING-UNIT-024: JVM shutdown hook — real SIGTERM, main-thread join, exit 143, final report drained")
class BridgeShutdownHookTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SIGNAL_LINE = "signaling arrow-bridge (SIGTERM)";
    private static final String FINAL_REPORT = "arrow-tick-counts: total=99";
    private static final String PERIODIC_REPORT = "arrow-tick-counts: total=";
    private static final String DRAIN_ERROR = "stderr drain error";

    private static final long STARTUP_TIMEOUT_MS = 60_000;
    private static final long HALT_TIMEOUT_MS = 30_000;
    private static final long POLL_INTERVAL_MS = 250;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("SIGTERM to the driver JVM runs the hook: main thread joined, final report drained, exit 143")
    void realSigtermRunsHookAndDrainsFinalReport() throws Exception {
        Path bridge = writeGracefulBridge();
        Path logDir = tempDir.resolve("logs");
        Files.createDirectories(logDir);
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");

        // 1. Driver JVM with a controlled env (no Fluss — stub writer, and a
        //    scripted fake bridge instead of the Go binary).
        List<String> cmd = List.of(
                "java",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                "com.trading.ingestion.BridgeShutdownDriver");
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("ARROW_BRIDGE_BIN", bridge.toString());
        // P1-077: child must declare its env — blank refuses startup.
        pb.environment().put("DEPLOYMENT_ENV", "dev");
        pb.environment().put("ARROW_APP_ID", "test-app");
        pb.environment().put("ARROW_APP_SECRET", "test-secret");
        pb.environment().put("ARROW_USER_ID", "test-user");
        pb.environment().put("ARROW_PASSWORD", "test-pass");
        pb.environment().put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        pb.environment().put("FLUSS_BOOTSTRAP", "localhost:9123");
        pb.environment().put("RAW_TABLE_NAME", "raw_table_1");
        pb.environment().put("ARROW_MAX_EVENT_AGE_MS", "5000");
        pb.environment().put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        pb.environment().put("UNCERTAINTY_JOURNAL_PATH", journalPath.toString());
        pb.environment().put("LOG_DIR", logDir.toString());
        // Files, not pipes: the JVM is SIGTERMed, so parent-side pipes would
        // close at destroy() — the driver's own stderr must survive for
        // diagnostics (CHG-014 pattern).
        pb.redirectOutput(tempDir.resolve("driver.stdout.log").toFile());
        pb.redirectError(tempDir.resolve("driver.stderr.log").toFile());
        Process proc = pb.start();
        try {
            // P1-132: the service names its log ingestion-${HOST}-${VM_ID}.json —
            // resolve via glob, not the old fixed name.
            FileLog log = new FileLog(findIngestionJson(logDir));

            // 2. Wait for the bridge's per-second tick reports (the service
            //    drains them from the bridge's stderr and re-logs them).
            waitForMarker(log, PERIODIC_REPORT, STARTUP_TIMEOUT_MS,
                    "the bridge's per-second tick-count reports must be drained and re-logged");

            // 3. Real SIGTERM: the JVM runs the shutdown hook (not just the
            //    shutdown() call) — signalBridge, drain join, journal write,
            //    bounded main-thread join, then halt.
            proc.destroy();
            boolean exited = proc.waitFor(HALT_TIMEOUT_MS, TimeUnit.SECONDS);
            if (!exited) {
                proc.destroyForcibly();
            }
            assertTrue(exited,
                    "the driver JVM must halt within " + HALT_TIMEOUT_MS
                            + " ms of SIGTERM (a hung shutdown hook or join would stall it); "
                            + "stderr tail:\n" + tail(tempDir.resolve("driver.stderr.log")));
            // SIGTERM's conventional exit status is 128 + 15 = 143.
            assertEquals(143, proc.exitValue(),
                    "the driver JVM must exit via SIGTERM (143), got " + proc.exitValue()
                            + "; stderr tail:\n" + tail(tempDir.resolve("driver.stderr.log")));

            // 4. Read the service's JSON log (log4j2 JSON_FILE appender,
            //    immediateFlush=true — complete once the JVM halted).
            String all = log.full();
            List<Event> events = parseEvents(all);

            int signalIdx = indexOfMessage(events, Pattern.compile(Pattern.quote(SIGNAL_LINE)));
            assertTrue(signalIdx >= 0,
                    "the hook must log '" + SIGNAL_LINE + "'; JSON tail:\n" + tail(all));

            // 5. The final report must have been drained from the bridge's
            //    stderr AFTER the shutdown signal — through the real hook.
            boolean finalReportDrained = false;
            for (int i = signalIdx + 1; i < events.size(); i++) {
                if (events.get(i).message().contains(FINAL_REPORT)) {
                    finalReportDrained = true;
                    break;
                }
            }
            assertTrue(finalReportDrained,
                    "the bridge's final tick-count report (" + FINAL_REPORT
                            + ") must be drained from stderr after the SIGTERM signal "
                            + "(a Process.destroy() regression would close the pipes first); "
                            + "JSON around the signal:\n" + around(all, signalIdx));

            // 6. No destroy()-closes-pipe artifact.
            assertFalse(all.contains(DRAIN_ERROR),
                    "the bridge-stderr drain must not error on the graceful path; JSON tail:\n"
                            + tail(all));

            // 7. The main thread finished its loop before the JVM halted — the
            //    hook's bounded join let the final report flush (without it,
            //    the halt races the main thread).
            assertTrue(events.stream().anyMatch(e -> e.message().contains("bridge loop ended")),
                    "the main loop must unwind to its end-of-loop report before the JVM halts "
                            + "(the hook's main-thread join); JSON tail:\n" + tail(all));

            // 8. Exactly one shutdown: one uncertainty-journal entry.
            assertEquals(1, Files.readAllLines(journalPath).size(),
                    "the graceful path must shut down exactly once");
        } finally {
            proc.destroyForcibly();
        }
    }

    // ---- fixtures ----

    /** Same scripted fake bridge as ING-UNIT-023: per-second stderr reports; on TERM the FINAL report then exit 0. */
    private Path writeGracefulBridge() throws Exception {
        Path script = tempDir.resolve("fake-bridge.sh");
        String body = """
                #!/bin/sh
                # CHG-015 fixture: per-second tick-count reports on stderr; on
                # TERM, the FINAL report (total=99) then exit 0.
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

    // ---- JSON-log helpers (same pattern as the WIP draft / E2E harness) ----

    private record Event(long epochSecond, long nanoOfSecond, String message) {
    }

    private static List<Event> parseEvents(String jsonLog) {
        List<Event> events = new ArrayList<>();
        for (String line : jsonLog.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            try {
                JsonNode node = MAPPER.readTree(line);
                JsonNode instant = node.path("instant");
                events.add(new Event(instant.path("epochSecond").asLong(),
                        instant.path("nanoOfSecond").asLong(),
                        node.path("message").asText("")));
            } catch (IOException ignored) {
                // Partial trailing line — not a complete event yet.
            }
        }
        return events;
    }

    private static int indexOfMessage(List<Event> events, Pattern pattern) {
        for (int i = 0; i < events.size(); i++) {
            if (pattern.matcher(events.get(i).message()).find()) {
                return i;
            }
        }
        return -1;
    }

    private static String around(String jsonLog, int eventIdx) {
        String[] lines = jsonLog.split("\n");
        int from = Math.max(0, eventIdx - 3);
        int to = Math.min(lines.length, eventIdx + 6);
        return String.join("\n", java.util.Arrays.copyOfRange(lines, from, to));
    }

    private static void waitForMarker(FileLog log, String marker, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (log.buffer().toString().contains(marker)) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        if (!log.buffer().toString().contains(marker)) {
            fail(message + " — marker '" + marker + "' not seen within " + timeoutMs
                    + "ms; captured JSON tail:\n" + tail(log.buffer().toString()));
        }
    }

    private static String tail(String s) {
        return s.length() > 4000 ? s.substring(s.length() - 4000) : s;
    }

    private static String tail(Path p) {
        try {
            return tail(Files.readString(p, StandardCharsets.UTF_8));
        } catch (IOException e) {
            return "(unreadable: " + e + ")";
        }
    }

    /** Append-only view of a growing log file (same pattern as the E2E harness). */
    /** P1-132: locate the service's JSON log (identity-bearing name). */
    private static Path findIngestionJson(Path logDir) throws Exception {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (java.util.stream.Stream<Path> s = Files.list(logDir)) {
                java.util.Optional<Path> hit = s
                        .filter(p -> p.getFileName().toString().startsWith("ingestion-")
                                && p.getFileName().toString().endsWith(".json"))
                        .findFirst();
                if (hit.isPresent()) return hit.get();
            }
            Thread.sleep(200);
        }
        throw new AssertionError("service JSON log (ingestion-*.json) never appeared in " + logDir);
    }

    private static final class FileLog {
        private final Path path;
        private final StringBuilder buffer = new StringBuilder();
        private long offset;

        FileLog(Path path) {
            this.path = path;
        }

        synchronized StringBuilder buffer() {
            try {
                long size = Files.size(path);
                if (size < offset) {
                    offset = 0;
                }
                if (size > offset) {
                    byte[] all = Files.readAllBytes(path);
                    int newStart = (int) Math.min(offset, all.length);
                    buffer.append(new String(all, newStart, all.length - newStart,
                            StandardCharsets.UTF_8));
                    offset = size;
                }
            } catch (IOException ignored) {
            }
            return buffer;
        }

        String full() {
            return buffer().toString();
        }
    }
}
