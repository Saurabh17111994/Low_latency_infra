package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ING-E2E-001: full-stack fake-broker → bridge → IngestionService → Fluss.
 *
 * <p>Requires a fake HFT broker URL (provided by a fixture script that starts
 * the Go fake broker) and a live Fluss at localhost:9123. The Java service is
 * launched as a subprocess with ARROW_HFT_URL pointing at the fake broker, the
 * built bridge binary as ARROW_BRIDGE_BIN, and the normal env. The test asserts
 * the service starts, the bridge connects, ticks flow, the bridge survives a
 * forced disconnect by reconnecting in-process (a second ACTIVE
 * subscription_ack), and the service's final bridge-loop report shows the tick
 * count grew after the reconnect — i.e. appends resumed on the fresh epoch.
 *
 * <p>Set {@code INGESTION_INT_TEST_E2E=true} (the fake broker is started by
 * this test itself — the historical {@code FAKE_HFT_URL} env var is unused).
 *
 * <p><b>Harness hardening (2026-08-15):</b>
 * <ul>
 *   <li>{@code LOG_DIR} is pointed at a writable JUnit temp dir — the
 *       log4j2 {@code JSON_FILE} appender defaults to
 *       {@code /data/ingestion/logs}, and when that path cannot be created the
 *       root logger's events are dropped, leaving the assertions blind to the
 *       service's startup logs.</li>
 *   <li>Startup/disconnect markers are <b>polled</b> (250 ms interval, bounded
 *       deadlines) instead of a fixed 8 s sleep, so a slow JVM/clock-check no
 *       longer fails a healthy run and a fast run finishes as soon as the
 *       evidence appears.</li>
 *   <li>Cluster pre-flight (2026-08-15): before launching the service the test
 *       verifies the dev Fluss stack serves data — TCP 9123 (coordinator) and
 *       TCP 9124 (tablet) must accept. The tablet only binds after every log
 *       segment recovers, so a missing tablet listener with a crash-looping
 *       container means truncated segments from an unclean shutdown; the
 *       failure message points at the surgical repair
 *       ({@code code/01_platform/04_scripts/fluss-repair/repair-tablet.sh},
 *       runbook section in 11-testing-and-release.md). Without this, the test
 *       burned the full 90 s startup window polling for {@code Fluss
 *       connected} and failed with a confusing stderr tail.</li>
 *   <li>Append-success proof (CHG-014): {@code ARROW_TICK_COUNTS=1} makes the
 *       bridge report its cumulative emitted-tick total every second
 *       ({@code arrow-tick-counts: total=N}, drained and re-logged by the
 *       service). After the forced disconnect the test polls for a <b>second</b>
 *       {@code subscription_ack state=ACTIVE} (the reconnect), then waits for
 *       the bridge's reported total to grow past the pre-reconnect baseline,
 *       then asserts the service's final {@code bridge loop ended (ticks=B, …)}
 *       report exceeds that baseline with zero errors — the reconnect epoch
 *       demonstrably appended ticks to Fluss.</li>
 *   <li><b>File-based stderr capture (root cause fix, CHG-014):</b> the
 *       service's stderr/stdout are redirected to files and the poll loop
 *       reads the files. The original pipe-based reader was killed by
 *       {@code Process.destroy()}: on this JDK (17.0.19) {@code destroy()}
 *       closes the parent-side process input streams with
 *       {@code IOException("Stream closed")} while the child is still running
 *       its shutdown hooks (verified with a slow-exit probe), so every console
 *       line the service wrote during the graceful shutdown — including the
 *       final {@code bridge loop ended} report — entered a pipe with no reader
 *       and was discarded. The file appender (own fd) always had the full
 *       shutdown, which is why the JSON log was complete while the buffer
 *       froze. Files survive {@code destroy()}, so the final report is now
 *       reliably captured.</li>
 *   <li>No-orphan guard (CHG-016): after the service exits, the test asserts
 *       the bridge pid (parsed from {@code arrow-bridge started (pid=N)},
 *       last match) is no longer alive — the shutdown hook's final reaping
 *       sweep must have taken it down — and the fake broker is force-reaped
 *       and asserted dead, so nothing the service or the test spawned
 *       survives.</li>
 * </ul>
 */
@DisplayName("ING-E2E-001: full-stack fake broker → Fluss")
class FullStackE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(FullStackE2ETest.class);

    /** Startup deadline — the poll returns as soon as the markers appear. */
    private static final long STARTUP_TIMEOUT_MS = 90_000;

    /** Dev-cluster ports: coordinator client RPC and tablet server. */
    private static final int FLUSS_COORDINATOR_PORT = 9123;
    private static final int FLUSS_TABLET_PORT = 9124;
    /** Pre-flight TCP connect timeout — success is sub-ms on a healthy stack. */
    private static final int PREFLIGHT_TCP_TIMEOUT_MS = 5_000;
    /** Bounded window to observe the forced-disconnect evidence after startup. */
    private static final long DISCONNECT_WINDOW_MS = 30_000;
    /** Bounded window for the reconnect (second ACTIVE) and tick growth. */
    private static final long RECONNECT_WINDOW_MS = 60_000;
    /** Bounded window for the graceful shutdown to flush the final
     *  bridge-loop report after SIGTERM (the hook signals the bridge, joins
     *  the stderr drain and the main thread, then drains writers — under load
     *  this takes longer than a fixed few-second sleep). */
    private static final long SHUTDOWN_TIMEOUT_MS = 45_000;

    /** Tick growth margin: wait for this many ticks past the reconnect-time
     *  baseline before tearing down, so the Java-side frame count has drained
     *  well past the baseline when the final report is compared. */
    private static final int TICK_GROWTH_MARGIN = 5;

    /** Matches the bridge's per-second emitted-tick reports as re-logged by
     *  the service: {@code arrow-bridge: arrow-tick-counts: total=N …}. */
    private static final Pattern TICK_TOTAL = Pattern.compile("arrow-tick-counts: total=(\\d+)");

    /** Matches a full-ack ACTIVE subscription event in the service's lifecycle
     *  log: {@code bridge lifecycle event=subscription_ack … state=ACTIVE …}. */
    private static final Pattern ACTIVE_ACK = Pattern.compile(
            "event=subscription_ack[^\\n]*state=ACTIVE");

    /** Matches the service's final bridge-loop report. */
    private static final Pattern BRIDGE_LOOP_ENDED = Pattern.compile(
            "bridge loop ended \\(ticks=(\\d+), errors=(\\d+), restarts=(\\d+)\\)");

    /** Matches the service's bridge-launch log line, whose pid the no-orphan
     *  guard reaps against: {@code arrow-bridge started (pid=N)}. The last
     *  match is the bridge current at shutdown (restarts spawn newer pids). */
    private static final Pattern BRIDGE_PID = Pattern.compile(
            "arrow-bridge started \\(pid=(\\d+)\\)");
    private static final long POLL_INTERVAL_MS = 250;

    @TempDir
    Path logDir;

    @Test
    @DisplayName("bridge ingests fake ticks into Fluss and survives a forced disconnect")
    void fullStackFakeBrokerToFluss() throws Exception {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("INGESTION_INT_TEST_E2E", "false")),
                "Skipping — set INGESTION_INT_TEST_E2E=true");

        // 0. Pre-flight: the dev Fluss stack must serve data before we burn
        //    the startup window (see the cluster-health javadoc note).
        preflightCluster();

        // 1. Start the standalone fake HFT broker on a free port.
        int port = freePort();
        String faketoolBin = System.getenv().getOrDefault(
                "FAKETOOL_BIN", "go-bridge/faketool/faketool");
        Process faketool = new ProcessBuilder(faketoolBin, "--port", String.valueOf(port), "--disconnect-after", "1")
                .redirectErrorStream(true)
                .redirectOutput(logDir.resolve("faketool.log").toFile())
                .redirectError(logDir.resolve("faketool.log").toFile())
                .start();
        try {
            Thread.sleep(500); // let the fake broker bind
            String fakeUrl = "ws://127.0.0.1:" + port;

            String bridgeBin = System.getenv().getOrDefault(
                    "ARROW_BRIDGE_BIN",
                    "go-bridge/arrow-bridge");

            List<String> cmd = new ArrayList<>(List.of(
                    "java",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "-cp", System.getProperty("java.class.path"),
                    "com.trading.ingestion.IngestionService"));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            java.util.Map<String, String> env = new java.util.HashMap<>();
            env.put("ARROW_HFT_URL", fakeUrl);
            env.put("ARROW_BRIDGE_BIN", bridgeBin);
            env.put("DEPLOYMENT_ENV", "dev");
            // This test is the deployment surface for the fake credentials below,
            // so it declares the env-file mechanism exactly as the sibling
            // harnesses do (pipeline-lib.sh, stage-soak-e2e.sh, tiering-smoke.sh,
            // loadtest-run.sh). Without it SecretGuard fails closed on any
            // secret key present in the env map (S3, 2026-08-29 decision A) and
            // the service refuses to start — which is what step 9 hit.
            env.put("SECRETS_VIA_ENV_FILE", "1");
            env.put("ARROW_APP_ID", "e2e");
            env.put("ARROW_APP_SECRET", "e2esecret");
            env.put("ARROW_USER_ID", "e2e-user");
        env.put("ARROW_PASSWORD", "e2e-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
            // TOTP migration (2026-08-24): the bridge AutoLogins against the real
            // API. E2E must use the fake-broker auth mode so no login API call
            // happens (e2e-user is not a real account — "user not found" 400).
            env.put("ARROW_FAKE_BROKER", "1");
            env.put("FLUSS_BOOTSTRAP", "localhost:9123");
            env.put("RAW_TABLE_NAME", "raw_table_1");
            env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
            env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
            env.put("GO_ARROW_SDK_VERSION", "v0.0.0-20260622-7cce1630");
            env.put("ARROW_HFT_CONNECTIONS", "1");
            // Count-based losslessness evidence: the bridge reports its
            // cumulative emitted-tick total every second on stderr, which the
            // service drains and re-logs — the mid-run tick signal the
            // append-growth assertion reads.
            env.put("ARROW_TICK_COUNTS", "1");
            // Keep the bridge's report file inside the temp dir (the default
            // /tmp/arrow-tick-counts.txt is shared and could race other runs).
            env.put("ARROW_TICK_COUNTS_FILE", logDir.resolve("arrow-tick-counts.txt").toString());
            // log4j2's JSON_FILE appender needs a writable LOG_DIR; without one
            // the root logger drops every event and the assertions go blind.
            env.put("LOG_DIR", logDir.toString());
            // Manifest: the fake broker emits token 757614 (present in the
            // approved CSV) so the tick is actually ingested.
            String manifestPath = System.getenv().getOrDefault("INSTRUMENT_MANIFEST_PATH",
                    "../../../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv");
            env.put("INSTRUMENT_MANIFEST_PATH", manifestPath);
            env.put("ARROW_INSTRUMENT_MANIFEST", manifestPath);
            pb.environment().putAll(env);
            // stderr/stdout go to files, NOT pipes: Process.destroy() closes
            // the parent-side process input streams with IOException("Stream
            // closed") while the child is still running its shutdown hooks, so
            // a pipe reader dies at SIGTERM and every shutdown console line is
            // lost. Files survive destroy() — the poll loop reads appended
            // bytes (see FileLog).
            Path stderrLog = logDir.resolve("service.stderr.log");
            Path stdoutLog = logDir.resolve("service.stdout.log");
            pb.redirectOutput(stdoutLog.toFile());
            pb.redirectError(stderrLog.toFile());
            Process proc = pb.start();
            // The log file must exist (the child inherits the fd at spawn; a
            // missing file means spawn failed). Poll reads append-only content.
            FileLog serviceLog = new FileLog(stderrLog);

            // 2. Poll for the startup markers (no fixed sleep): the service
            //    must validate config, pass the schema check, connect to Fluss,
            //    construct the real evidence writers, and launch the bridge.
            waitForMarker(serviceLog, "Fluss connected", STARTUP_TIMEOUT_MS,
                    "service must connect to Fluss");
            waitForMarker(serviceLog, "arrow-bridge started", STARTUP_TIMEOUT_MS,
                    "service must start the bridge");
            // 3. faketool --disconnect-after 1 force-closes the first
            //    connection; the bridge must emit the disconnect lifecycle
            //    event (→ DROP discontinuity evidence) and keep running.
            waitForMarker(serviceLog, "event=disconnect", DISCONNECT_WINDOW_MS,
                    "bridge must survive the forced disconnect (discontinuity evidence)");
            // 4. Append-success proof: the supervisor must reconnect in-process
            //    (a second ACTIVE subscription_ack on a fresh epoch) and the
            //    bridge's emitted-tick total must keep growing — the fresh
            //    epoch's ticks are flowing into the service again. faketool's
            //    connection 2 is not the forced-close connection, so it keeps
            //    serving ticks indefinitely.
            waitForMarkerCount(serviceLog, ACTIVE_ACK, 2, RECONNECT_WINDOW_MS,
                    "bridge must reconnect after the forced disconnect (second ACTIVE subscription_ack)");
            int baseline = maxTickTotal(serviceLog);
            waitForTickTotal(serviceLog, baseline + TICK_GROWTH_MARGIN, RECONNECT_WINDOW_MS,
                    "bridge tick total must grow past the reconnect-time baseline");

            proc.destroy();
            // Poll for the final bridge-loop report instead of a fixed sleep:
            // the graceful SIGTERM shutdown runs the hook (signal the bridge
            // for its final tick-count report, join the stderr drain, join the
            // main thread so it logs "bridge loop ended", then drain writers).
            // Because stderr is a FILE, the report survives destroy() (a pipe
            // reader would have been closed by destroy() — see the class
            // javadoc). destroyForcibly after a too-short fixed wait would
            // SIGKILL the JVM mid-flush and lose the report this assertion
            // needs.
            waitForMarker(serviceLog, "bridge loop ended", SHUTDOWN_TIMEOUT_MS,
                    "service must flush its final bridge-loop report on shutdown");
            boolean exited = proc.waitFor(15, TimeUnit.SECONDS);
            if (!exited) {
                proc.destroyForcibly();
            }

            String log = serviceLog.full();
            LOG.info("e2e service stderr ({} chars):\n{}", log.length(),
                    log.length() > 4000 ? log.substring(0, 4000) : log);
            assertTrue(log.contains("Fluss connected"),
                    "service must connect to Fluss");
            assertTrue(log.contains("arrow-bridge started") || log.contains("bridge"),
                    "service must start the bridge");

            // 5. The service's final bridge-loop report (flushed during the
            //    graceful SIGTERM shutdown) must show the tick count grew past
            //    the reconnect-time baseline with zero errors — the reconnect
            //    epoch appended ticks to Fluss end-to-end.
            Matcher m = BRIDGE_LOOP_ENDED.matcher(log);
            assertTrue(m.find(),
                    "service must flush its final bridge-loop report on shutdown; "
                            + "service stderr tail:\n" + tail(log));
            long finalTicks = Long.parseLong(m.group(1));
            long finalErrors = Long.parseLong(m.group(2));
            long finalRestarts = Long.parseLong(m.group(3));
            LOG.info("e2e append-growth: baseline={} finalTicks={} errors={} restarts={}",
                    baseline, finalTicks, finalErrors, finalRestarts);
            assertTrue(finalTicks > baseline,
                    "final bridge-loop report ticks (" + finalTicks
                            + ") must exceed the reconnect-time baseline (" + baseline
                            + ") — the reconnect epoch produced no appends; service stderr tail:\n"
                            + tail(log));
            assertTrue(finalErrors == 0,
                    "final bridge-loop report must have zero errors, got " + finalErrors
                            + "; service stderr tail:\n" + tail(log));
            assertTrue(finalRestarts == 0,
                    "reconnect must be in-process (no bridge process restarts), got "
                            + finalRestarts + "; service stderr tail:\n" + tail(log));
            assertTrue(!log.contains("FATAL append error"),
                    "no append failures may occur; service stderr tail:\n" + tail(log));

            // 5b. No-orphan guard (CHG-016): the bridge the service spawned
            //     must not survive the service's exit — the shutdown hook
            //     signals it and reaps it (a survivor would keep emitting
            //     into a dead pipe forever). The report above was flushed by
            //     the hook, so the bridge was signaled; assert its pid (the
            //     last one launched) is gone.
            Matcher bm = BRIDGE_PID.matcher(log);
            long bridgePid = -1;
            while (bm.find()) {
                bridgePid = Long.parseLong(bm.group(1));
            }
            if (bridgePid > 0) {
                boolean bridgeAlive = ProcessHandle.of(bridgePid)
                        .map(ProcessHandle::isAlive).orElse(false);
                assertTrue(!bridgeAlive,
                        "the bridge process (pid=" + bridgePid
                                + ") must not survive the service's exit — the shutdown hook "
                                + "should have reaped it; service stderr tail:\n" + tail(log));
            }
        } finally {
            // Reap the fake broker too: nothing the test spawned may survive.
            if (faketool.isAlive()) {
                faketool.destroyForcibly();
            }
            try {
                faketool.waitFor(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            assertTrue(!faketool.isAlive(),
                    "faketool (pid=" + faketool.pid()
                            + ") must not survive the test — no orphaned broker");
        }
    }

    /**
     * Pre-flight cluster-health check: the dev Fluss stack must actually serve
     * data before the test launches the service.
     *
     * <p>The check is cheap and precise: TCP 9123 (coordinator client RPC) and
     * TCP 9124 (tablet) must accept. The tablet only binds after <b>every</b>
     * log segment recovers, so a missing tablet listener while its container
     * crash-loops means truncated segments from an unclean shutdown — the
     * docker status probe distinguishes that from a stopped stack and points
     * at the surgical repair (fluss-repair/repair-tablet.sh; runbook section
     * in 11-testing-and-release.md).
     */
    private static void preflightCluster() {
        if (!tcpReachable("127.0.0.1", FLUSS_COORDINATOR_PORT)) {
            fail("ING-E2E-001 pre-flight: Fluss coordinator not reachable on 127.0.0.1:"
                    + FLUSS_COORDINATOR_PORT + " — start the dev stack "
                    + "(code/01_platform/01_docker) before running this test");
        }
        if (!tcpReachable("127.0.0.1", FLUSS_TABLET_PORT)) {
            String diag = dockerTabletStatus();
            if (diag != null && diag.contains("Restarting")) {
                fail("ING-E2E-001 pre-flight: Fluss tablet is crash-looping (" + diag + ") — "
                        + "likely truncated log segments from an unclean shutdown. Run the "
                        + "surgical repair: code/01_platform/04_scripts/fluss-repair/repair-tablet.sh "
                        + "(procedure in the ING-E2E-001 runbook section of 11-testing-and-release.md) "
                        + "before retrying");
            }
            fail("ING-E2E-001 pre-flight: Fluss tablet not reachable on 127.0.0.1:"
                    + FLUSS_TABLET_PORT + " — start the dev stack (code/01_platform/01_docker) "
                    + "before running this test"
                    + (diag == null ? "" : " (docker status: " + diag + ")"));
        }
    }

    /** True if a TCP connect to {@code host}:{@code port} succeeds within the pre-flight timeout. */
    private static boolean tcpReachable(String host, int port) {
        try (java.net.Socket s = new java.net.Socket()) {
            s.connect(new java.net.InetSocketAddress(host, port), PREFLIGHT_TCP_TIMEOUT_MS);
            return true;
        } catch (java.io.IOException e) {
            return false;
        }
    }

    /**
     * Best-effort docker status line for the tablet container via
     * {@code docker ps -a} (e.g. {@code Restarting (5) 3 seconds ago}), or
     * {@code null} when the CLI is unavailable or no matching container
     * exists. Several compose projects can leave fluss-tablet containers
     * around (this host also has an Exited one from another stack), so the
     * match prefers the crash-loop symptom (Restarting), then an Up
     * container, then the most recently stopped match — mirroring
     * repair-tablet.sh's discovery. Recency is ranked by the "ago" unit in
     * the status line (seconds/minutes &lt; hours &lt; days &lt; weeks+), since
     * CreatedAt is the container's original creation time and is not the
     * right signal.
     */
    private static String dockerTabletStatus() {
        try {
            String[] cmd = {"docker", "ps", "-a", "--format", "{{.Names}}\t{{.Status}}",
                    "--filter", "name=fluss-tablet"};
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            p.waitFor(5, TimeUnit.SECONDS);
            String restarting = null, up = null;
            String recentStatus = null;
            int recentRank = Integer.MAX_VALUE;
            for (String line : out.split("\n")) {
                String[] parts = line.split("\t");
                if (parts.length < 2 || !parts[0].contains("fluss-tablet")) {
                    continue;
                }
                String status = parts[1];
                if (restarting == null && status.startsWith("Restarting")) {
                    restarting = status;
                }
                if (up == null && status.startsWith("Up")) {
                    up = status;
                }
                int rank = agoRank(status);
                if (rank < recentRank) {
                    recentRank = rank;
                    recentStatus = status;
                }
            }
            if (restarting != null) {
                return restarting;
            }
            if (up != null) {
                return up;
            }
            return recentStatus;
        } catch (Exception e) {
            return null;
        }
    }

    /** Recency rank of a docker status line by its "ago" unit: 0 = seconds/
     *  minutes, 1 = hours, 2 = days, 3 = weeks or longer (unparseable = 3). */
    private static int agoRank(String status) {
        String s = status.toLowerCase();
        if (s.contains("second") || s.contains("minute")) {
            return 0;
        }
        if (s.contains("hour")) {
            return 1;
        }
        if (s.contains("day")) {
            return 2;
        }
        return 3;
    }

    /** Poll {@code log} for {@code marker} until it appears or the deadline passes. */
    private static void waitForMarker(FileLog log, String marker, long timeoutMs,
                                      String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (log.buffer().indexOf(marker) >= 0) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        if (log.buffer().indexOf(marker) < 0) {
            String tail = log.buffer().length() > 4000
                    ? log.buffer().substring(log.buffer().length() - 4000) : log.buffer().toString();
            fail(message + " — marker '" + marker + "' not seen within " + timeoutMs + "ms; "
                    + "service stderr tail:\n" + tail);
        }
    }

    /** Poll until {@code pattern} matches at least {@code minCount} times. */
    private static void waitForMarkerCount(FileLog log, Pattern pattern, int minCount,
                                           long timeoutMs, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int seen = 0;
        while (System.currentTimeMillis() < deadline) {
            seen = countMatches(log, pattern);
            if (seen >= minCount) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        fail(message + " — matched " + seen + " of " + minCount + " within " + timeoutMs
                + "ms; service stderr tail:\n" + tail(log));
    }

    /** Poll until the largest {@code arrow-tick-counts: total=N} seen reaches {@code floor}. */
    private static void waitForTickTotal(FileLog log, int floor, long timeoutMs,
                                         String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int seen = 0;
        while (System.currentTimeMillis() < deadline) {
            seen = maxTickTotal(log);
            if (seen >= floor) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        fail(message + " — tick total reached " + seen + ", wanted " + floor + " within "
                + timeoutMs + "ms; service stderr tail:\n" + tail(log));
    }

    /** Largest {@code arrow-tick-counts: total=N} seen (0 if none yet). */
    private static int maxTickTotal(FileLog log) {
        int max = 0;
        Matcher m = TICK_TOTAL.matcher(log.buffer().toString());
        while (m.find()) {
            max = Math.max(max, Integer.parseInt(m.group(1)));
        }
        return max;
    }

    /** Count matches of {@code pattern} in the log so far. */
    private static int countMatches(FileLog log, Pattern pattern) {
        Matcher m = pattern.matcher(log.buffer().toString());
        int count = 0;
        while (m.find()) {
            count++;
        }
        return count;
    }

    /** Last 4000 chars of the log (for failure diagnostics). */
    private static String tail(FileLog log) {
        return tail(log.buffer().toString());
    }

    /** Last 4000 chars of a snapshot (for failure diagnostics). */
    private static String tail(String s) {
        return s.length() > 4000 ? s.substring(s.length() - 4000) : s;
    }

    private static int freePort() throws IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /**
     * Append-only view of the service's redirected stderr file. The child's
     * fd 2 (a regular file, not a pipe) keeps receiving writes for the whole
     * process lifetime — including the graceful shutdown after
     * {@code Process.destroy()} — so polling appended bytes yields every
     * console line, which a pipe reader cannot (destroy() closes parent-side
     * process pipes with IOException("Stream closed"); see the class javadoc).
     *
     * <p>{@link #buffer()} drains newly appended bytes into the in-memory log
     * and returns it; the poll helpers call it on every iteration. A partial
     * trailing line (a write caught mid-line) stays in the file until the next
     * drain, so markers — always within complete lines — are never missed.
     */
    private static final class FileLog {
        private final Path path;
        private final StringBuilder buffer = new StringBuilder();
        private long offset;

        FileLog(Path path) {
            this.path = path;
        }

        /** Append new file content to the buffer and return it. */
        synchronized StringBuilder buffer() {
            try {
                long size = Files.size(path);
                if (size < offset) {
                    offset = 0; // file rotated/truncated — restart from the top
                }
                if (size > offset) {
                    byte[] all = Files.readAllBytes(path);
                    int newStart = (int) Math.min(offset, all.length);
                    buffer.append(new String(all, newStart, all.length - newStart,
                            StandardCharsets.UTF_8));
                    offset = size;
                }
            } catch (IOException ignored) {
                // File not created yet (spawn just happened) or being written.
            }
            return buffer;
        }

        /** Complete captured content (for final assertions and diagnostics). */
        String full() {
            return buffer().toString();
        }
    }
}
