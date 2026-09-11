package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.shutdown.BoundedClose;
import com.trading.ingestion.telemetry.OtlpMetricsEmitter;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The container stop grace period must exceed ingestion's worst-case shutdown,
 * or Docker SIGKILLs mid-sequence and discards the pending evidence the bounded
 * closes exist to flush (2026-09-12).
 *
 * <p>This is the invariant the bounded-close work depends on. Bounding each step
 * is worthless if their SUM overruns the grace period: the process is killed
 * before the later steps run, and the last writers — the safety-halt evidence —
 * are the ones that lose. Docker's default is 10s and the sequence is
 * deliberately allowed to take longer, so an explicit
 * {@code stop_grace_period} is load-bearing, not decoration.
 *
 * <p>Every term is read from the real source, not restated here:
 *
 * <ul>
 *   <li>drain phase — the actual {@code DRAIN_DEADLINE_SECONDS} default, parsed
 *       by running the real config loader with the variable absent;</li>
 *   <li>evidence writers — {@link BoundedClose#budget()} x the number of writers
 *       the shutdown hook closes (counted from the hook, not assumed);</li>
 *   <li>metrics — {@link OtlpMetricsEmitter#SHUTDOWN_SCHEDULER_WAIT_SECONDS} +
 *       {@link OtlpMetricsEmitter#FINAL_FLUSH_WAIT_SECONDS};</li>
 *   <li>main-thread join and late-bridge reap —
 *       {@link IngestionService#SHUTDOWN_MAIN_JOIN_MS} /
 *       {@link IngestionService#LATE_BRIDGE_REAP_MS}.</li>
 * </ul>
 *
 * <p>So raising any budget without raising the grace period fails here, which is
 * the drift this pins. Two legs are deliberately shape-checked rather than
 * summed — that the drain phase shares ONE deadline across workers, and that the
 * evidence writers close BEFORE metrics — because both are ordering properties
 * that no arithmetic can catch.
 */
@DisplayName("shutdown budget: the container stop grace period covers the worst-case sequence")
class ShutdownBudgetGraceTest {

    /** Compose file declaring the ingestion service's stop grace period. */
    private static final String COMPOSE_REL = "code/01_platform/01_docker/docker-compose.yml";

    /**
     * The writers the shutdown hook releases through {@link BoundedClose},
     * counted from the hook itself so adding a fourth writer cannot silently
     * under-count the budget.
     */
    private static int evidenceWriterCount() throws Exception {
        Matcher m = Pattern.compile(
                        // \w+Writer requires a camelCase evidence writer
                        // (safetyHaltWriter, quarantineWriter, discontinuityWriter)
                        // and excludes the raw tick writer, whose field is plain
                        // `writer` and is NOT released through BoundedClose (it
                        // drains via RawTickWriter.close under drainDeadline).
                        "if \\((\\w+Writer) != null\\) \\1\\.close\\(\\);",
                        Pattern.MULTILINE)
                .matcher(shutdownMethodBody());
        int count = 0;
        while (m.find()) {
            count++;
        }
        assertTrue(count > 0, "no evidence-writer closes found in shutdown()");
        return count;
    }

    @Test
    @DisplayName("stop_grace_period exists on the ingestion service and covers the worst case")
    void gracePeriodCoversWorstCaseShutdown() throws Exception {
        long graceSeconds = ingestionStopGraceSeconds();
        long worstCaseSeconds = worstCaseShutdownSeconds();

        assertTrue(graceSeconds > 0,
                "ingestion must declare an explicit stop_grace_period — Docker's 10s default is "
                        + "shorter than the shutdown sequence, so a SIGKILL would land mid-sequence");
        assertTrue(graceSeconds >= worstCaseSeconds,
                "stop_grace_period (" + graceSeconds + "s) must cover the worst-case shutdown ("
                        + worstCaseSeconds + "s), or the process is SIGKILLed before the later "
                        + "bounded steps run — losing the safety-halt / quarantine / discontinuity "
                        + "evidence those steps flush. Raise stop_grace_period or lower a budget.");
    }

    /**
     * The worst case, summed from the real constants. Named per phase so a
     * failure says which one moved.
     */
    private static long worstCaseShutdownSeconds() throws Exception {
        long drain = drainDeadlineDefaultSeconds();
        long evidence = BoundedClose.budget().toSeconds() * evidenceWriterCount();
        long metrics = OtlpMetricsEmitter.SHUTDOWN_SCHEDULER_WAIT_SECONDS
                + OtlpMetricsEmitter.FINAL_FLUSH_WAIT_SECONDS;
        long join = IngestionService.SHUTDOWN_MAIN_JOIN_MS / 1000;
        long reap = IngestionService.LATE_BRIDGE_REAP_MS / 1000;

        System.out.println("shutdown budget: drain=" + drain + "s evidence=" + evidence
                + "s metrics=" + metrics + "s join=" + join + "s reap=" + reap + "s");
        return drain + evidence + metrics + join + reap;
    }

    /**
     * The real default: run the actual config loader with
     * {@code DRAIN_DEADLINE_SECONDS} absent, so this tracks the code rather than
     * a restated number. Mirrors {@code ShutdownDeadlockTest}'s fixture.
     */
    private static long drainDeadlineDefaultSeconds() throws Exception {
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
        // DRAIN_DEADLINE_SECONDS deliberately absent — we want the default.
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        IngestionConfig cfg = (IngestionConfig) validateFrom.invoke(null, env);
        return cfg.drainDeadline.toSeconds();
    }

    @Test
    @DisplayName("drain phase shares ONE deadline across workers (not one full budget each)")
    void drainPhaseSharesOneDeadline() throws Exception {
        String body = shutdownMethodBody();
        String loop = between(body, "if (writerWorkers != null)", "if (writer != null)");

        assertTrue(loop.contains("drainDeadlineNanos"),
                "the worker-close loop must derive one shared deadline for the whole phase — "
                        + "a fresh budget per worker makes the drain scale with FLUSS_WRITERS "
                        + "(N x DRAIN_DEADLINE_SECONDS), which no grace period covers:\n" + loop);
        assertTrue(loop.contains("w.close(Duration.ofNanos("),
                "each worker must be closed with the remaining share of the shared deadline:\n" + loop);
        assertTrue(!loop.contains("w.close();"),
                "w.close() grants a fresh full budget per worker — the sequential-full-deadlines "
                        + "bug B130 fixed inside WriterWorker, lifted back to the shutdown path:\n" + loop);
    }

    @Test
    @DisplayName("evidence writers close BEFORE metrics, so a slow flush cannot starve them")
    void evidenceWritersCloseBeforeMetrics() throws Exception {
        String body = shutdownMethodBody();
        int quarantine = body.indexOf("quarantineWriter.close()");
        int safety = body.indexOf("safetyHaltWriter.close()");
        int metrics = body.indexOf("metrics.close()");

        assertTrue(quarantine > 0 && safety > 0 && metrics > 0,
                "expected the evidence-writer and metrics closes in the shutdown method");
        assertTrue(quarantine < metrics && safety < metrics,
                "the evidence writers must close BEFORE metrics: metrics.close() can consume "
                        + "11s (6s scheduler + 5s single-flight), and with the writers after it a "
                        + "SIGKILL at the grace period lands first and the pending safety-halt "
                        + "evidence is never flushed. P1-067 only requires metrics to outlive the "
                        + "DRAIN, which is already complete above.");
    }

    /** The shutdown method's body, comments stripped, by brace matching from its signature. */
    private static String shutdownMethodBody() throws Exception {
        Path file = sourceRoot().resolve("IngestionService.java");
        String source = stripComments(Files.readString(file, StandardCharsets.UTF_8));
        int start = source.indexOf("void shutdown()");
        assertTrue(start >= 0, "no shutdown() method found in " + file);
        int open = source.indexOf('{', start);
        int depth = 0;
        for (int i = open; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return source.substring(open, i + 1);
                }
            }
        }
        throw new AssertionError("malformed shutdown() — unbalanced braces");
    }

    /**
     * Remove {@code //} and {@code /* *}{@code /} comments before any source scan.
     * Without this, prose describing a call (e.g. a comment explaining why
     * {@code metrics.close()} must come last) is indistinguishable from the call
     * itself — which is exactly how the first version of this test passed its own
     * comment as the real ordering.
     */
    private static String stripComments(String source) {
        StringBuilder out = new StringBuilder(source.length());
        boolean line = false;
        boolean block = false;
        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            char next = i + 1 < source.length() ? source.charAt(i + 1) : '\0';
            if (line) {
                if (c == '\n') {
                    line = false;
                    out.append(c);
                }
                continue;
            }
            if (block) {
                if (c == '*' && next == '/') {
                    block = false;
                    i++;
                } else if (c == '\n') {
                    out.append(c);
                }
                continue;
            }
            if (c == '/' && next == '/') {
                line = true;
                continue;
            }
            if (c == '/' && next == '*') {
                block = true;
                i++;
                continue;
            }
            out.append(c);
        }
        return out.toString();
    }

    private static String between(String haystack, String from, String to) {
        int a = haystack.indexOf(from);
        int b = haystack.indexOf(to);
        assertTrue(a >= 0 && b > a, "cannot slice between '" + from + "' and '" + to + "'");
        return haystack.substring(a, b);
    }

    /** Parse the ingestion service's stop_grace_period from the compose file. */
    private static long ingestionStopGraceSeconds() throws Exception {
        Path compose = repoRoot().resolve(COMPOSE_REL);
        assertTrue(Files.isRegularFile(compose), "missing compose file " + compose);
        String text = Files.readString(compose, StandardCharsets.UTF_8);

        String service = serviceBlock(text, "ingestion");
        Matcher m = Pattern.compile("^\\s*stop_grace_period:\\s*\"?(\\d+)s?\"?\\s*$", Pattern.MULTILINE)
                .matcher(service);
        assertTrue(m.find(),
                "ingestion has no stop_grace_period — Docker's 10s default would SIGKILL the "
                        + "shutdown sequence mid-way (see this test's javadoc)");
        return Long.parseLong(m.group(1));
    }

    /** Slice one top-level compose service (2-space indent) out of the file. */
    private static String serviceBlock(String text, String name) {
        Matcher m = Pattern.compile("^  " + Pattern.quote(name) + ":\\s*$", Pattern.MULTILINE)
                .matcher(text);
        assertTrue(m.find(), "no '" + name + ":' service in the compose file");
        int start = m.end();
        Matcher next = Pattern.compile("^  [A-Za-z0-9_-]+:\\s*$", Pattern.MULTILINE).matcher(text);
        int end = next.find(start) ? next.start() : text.length();
        return text.substring(start, end);
    }

    private static Path sourceRoot() {
        for (String candidate : List.of(
                "src/main/java/com/trading/ingestion",
                "02_services/01_ingestion/src/main/java/com/trading/ingestion")) {
            Path p = Path.of(candidate);
            if (Files.isRegularFile(p.resolve("IngestionService.java"))) {
                return p.toAbsolutePath().normalize();
            }
        }
        throw new AssertionError("cannot locate the ingestion source root");
    }

    /**
     * Repo root (the directory containing {@code code/}). Found by walking up
     * until the compose file appears, so it does not depend on counting {@code ..}
     * segments from the test class — which is off by one the moment the package
     * or module layout moves.
     */
    private static Path repoRoot() {
        Path dir = sourceRoot();
        for (int i = 0; i < 12 && dir != null; i++) {
            if (Files.isRegularFile(dir.resolve(COMPOSE_REL))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new AssertionError("walked up from " + sourceRoot()
                + " without finding " + COMPOSE_REL);
    }
}
