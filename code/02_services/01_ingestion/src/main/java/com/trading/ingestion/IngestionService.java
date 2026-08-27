package com.trading.ingestion;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.bridge.BridgeEvent;
import com.trading.ingestion.bridge.BridgeEventParser;
import com.trading.ingestion.bridge.BridgeMetrics;
import com.trading.ingestion.bridge.BrokerQuarantine;
import com.trading.ingestion.bridge.PayloadHashValidator;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.SequenceGapMonitor;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.discontinuity.TimeJumpMonitor;
import com.trading.ingestion.fingerprint.FingerprintBuilder;
import com.trading.common.config.ContainerMemoryGuard;
import com.trading.ingestion.health.HealthProbe;
import com.trading.ingestion.health.JvmHeapReadinessGate;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.health.ReadinessFile;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.model.ValidityClassification;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetySink;
import com.trading.ingestion.shutdown.UncertaintyJournal;
import com.trading.ingestion.telemetry.OtlpAlertLogs;
import com.trading.ingestion.telemetry.OtlpMetricsEmitter;
import com.trading.ingestion.write.AppendTracker;
import com.trading.ingestion.write.BoundedQueue;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.ProtoFrameReader;
import com.trading.ingestion.write.RawTickWriter;
import com.trading.ingestion.write.WriterWorker;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingestion main entry point — consumes NDJSON ticks from the Arrow Go bridge
 * (stdin), normalizes, fingerprints, and appends to Fluss {@code raw_table_1}.
 *
 * <h3>Pipeline</h3>
 * <pre>{@code
 * Go arrow-bridge stdout (NDJSON)
 *   → IngestionService.main() reads stdin line-at-a-time
 *   → parse Tick JSON → RawTick → valid? → TickPacket
 *   → FingerprintBuilder.build(…) → fingerprint
 *   → RawTickWriter.write(…) → Fluss append
 * }</pre>
 *
 * <p>The Go arrow-bridge process uses the official Arrow Go SDK for:
 * authentication (AutoLogin or static token), WebSocket connect, binary
 * frame decode (all 4 modes — LTP/LTPC/QUOTE/FULL including 5-level depth),
 * zstd decompression (HFT), subscription management, keepalive, and
 * reconnection. Java owns only the platform path: normalize, fingerprint,
 * Fluss append, backpressure, and observability.
 *
 * <p>See {@code docs/04_contracts/01-ingestion.md},
 * {@code docs/08_implementation/03-ingestion.md}.
 */
public final class IngestionService {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionService.class);

    private static final String VERSION = "0.2.0";
    private static final String FINGERPRINT_ALGO = "SHA-256";
    /**
     * ING-DQ-001: static detail for malformed-JSON quarantine rows. Never
     * interpolated with the offending line — Jackson's message embeds the
     * input snippet, which would leak raw line content into quarantine/logs.
     */
    private static final String MALFORMED_JSON_DETAIL =
            "NDJSON line is not valid JSON — quarantined per REQ-ING";
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    private static final long FRAME_STALE_MS = 15_000L;
    private static final long SUBSCRIPTION_COMPLETENESS_TIMEOUT_MS = 30_000L;
    /** Clock re-measurement cadence (ING-FAIL-007). NTP queries are cheap but
     *  not free; 60 s keeps the readiness clock dimension fresh without
     *  hammering the time servers. */
    private static final long CLOCK_MONITOR_INITIAL_DELAY_MS = 60_000L;
    private static final long CLOCK_MONITOR_INTERVAL_MS = 60_000L;
    /** Container-memory gate cadence (SIGNAL-warn-jvm-heap-high). Left small so
     *  the sustained 60 s breach window is sampled finely (avoid timestamp gaps);
     *  reading cgroup is cheap and local. */
    private static final long MEMORY_MONITOR_INITIAL_DELAY_MS = 5_000L;
    private static final long MEMORY_MONITOR_INTERVAL_MS = 5_000L;
    /** T2 tunable backpressure (G2 Ingest): pending limits are env-tunable via
     *  IngestionConfig / AppendTracker — 80% WARN / 100% HALT (fail-closed until
     *  restart, never silent drop). Streaming-3000 3k defaults: 150k records /
     *  192 MiB (vs 50k/64M for 1k). Slow-Fluss subscription pause stays at 90% with
     *  50% resume — conservative early-throttle above the 80% warning. */
    private static final double SLOW_FLUSS_PAUSE_PERCENT = 0.90;
    private static final double SLOW_FLUSS_RESUME_PERCENT = 0.50;
    /** Bridge restarts after an unexpected process exit (plan: restart exactly once). */
    private static final int MAX_BRIDGE_RESTARTS = 1;
    /** Wait before restarting a crashed bridge (plan: wait 1 second). */
    private static final long BRIDGE_RESTART_WAIT_MS = 1_000L;
    /** Bounded join on the main thread from the shutdown hook: the JVM halts as
     *  soon as every hook returns and does not wait for the main thread, so the
     *  final {@code bridge loop ended} report (logged by main as it unwinds)
     *  could race the halt. Join long enough for the unwind to flush it. */
    private static final long SHUTDOWN_MAIN_JOIN_MS = 10_000L;

    private final String instanceId;
    private final AppendTracker tracker;
    private final HealthProbe health;
    private final RawTickWriter writer;
    /** T6-3: bounded byte-budgeted queue (proto path) + single writer worker. */
    private final BoundedQueue queue;
    /** T7-F11: per-connection sequence-gap detection (never halts path). */
    private final SequenceGapMonitor sequenceGapMonitor = new SequenceGapMonitor();
    private final WriterWorker writerWorker;
    private final Map<Long, Instrument> instrumentMap;
    private final AtomicLong frameCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);
    private volatile boolean running;
    private volatile boolean subscriptionPaused;
    private volatile long lastFrameNanos;
    private volatile long lastResourceRefreshNanos;

    /** Watchdog for broker-staleness detection during feed outages (R-108). */
    private final java.util.concurrent.ScheduledExecutorService stalenessWatchdog =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ingestion-staleness-watchdog");
                t.setDaemon(true);
                return t;
            });

    /** ING-FAIL-007: one TIME_JUMP discontinuity per clock-violation episode. */
    private final TimeJumpMonitor timeJumpMonitor;
    /** Re-measures the NTP offset; a violation crossing CLOCK_OFFSET_LIMIT_MS
     *  emits TIME_JUMP evidence and refreshes the readiness clock dimension. */
    private final java.util.concurrent.ScheduledExecutorService clockMonitorScheduler;
    /** Sustained JVM/container memory readiness gate (SIGNAL-warn-jvm-heap-high). */
    private final JvmHeapReadinessGate heapGate;
    private final java.util.concurrent.ScheduledExecutorService memoryMonitorScheduler;
    private final String otelHost;

    private final IngestionConfig config;
    private final NtpClockChecker clock;
    private final UncertaintyJournal journal;
    private final OtlpMetricsEmitter metrics;
    private final QuarantineSink quarantineWriter;
    private final DiscontinuitySink discontinuityWriter;
    private final SafetySink safetyHaltWriter;
    private final String manifestFingerprint;
    private final String assignedTokenSetHash;
    private final java.util.Set<String> safetyEmitted = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final ReadinessFile readinessFile;
    private final BridgeEventParser bridgeEventParser = new BridgeEventParser(MAPPER);
    private final AtomicLong connectionEpoch = new AtomicLong(0);
    private volatile DiscontinuityWriter.LastTickSnapshot lastTickSnapshot;
    /** Current arrow-bridge subprocess, so shutdown can signal it (SIGTERM) and
     *  let it emit its final ARROW_TICK_COUNTS report before the pipe closes. */
    private volatile Process currentBridgeProcess;
    /** stderr drain thread for the current bridge, joined at shutdown so the
     *  final tick-count report is flushed into the log before JVM halt. */
    private volatile Thread currentBridgeStderrThread;
    /** The process's main thread (set by {@link #main}); the shutdown hook
     *  joins it (bounded) so the final bridge-loop report flushes before the
     *  JVM halts. Null outside main (unit tests) — the join is skipped. */
    private volatile Thread mainThread;

    public IngestionService(String instanceId,
                             List<Instrument> instruments,
                             FlussRowConverter flussWriter,
                             IngestionConfig config,
                             NtpClockChecker clock) {
        this(instanceId, instruments, flussWriter, config, clock, null, null, null);
    }

    /**
     * Test seam: accepts substitute evidence sinks so the service can be
     * constructed and driven without a reachable Fluss (ING-DQ-010). A
     * {@code null} sink falls back to the production Fluss-backed writer.
     */
    IngestionService(String instanceId,
                     List<Instrument> instruments,
                     FlussRowConverter flussWriter,
                     IngestionConfig config,
                     NtpClockChecker clock,
                     QuarantineSink quarantineSink,
                     DiscontinuitySink discontinuitySink,
                     SafetySink safetySink) {
        this.config = config;
        this.clock = clock;
        if (config.uncertaintyJournalPath != null && !config.uncertaintyJournalPath.isBlank()) {
            this.journal = new UncertaintyJournal(java.nio.file.Paths.get(config.uncertaintyJournalPath));
        } else {
            this.journal = new UncertaintyJournal();
        }
        this.instanceId = instanceId;
        this.instrumentMap = new ConcurrentHashMap<>();
        for (Instrument inst : instruments) {
            instrumentMap.put(inst.instrumentToken(), inst);
        }
        // T2 G2 Ingest: tunable pending limits from IngestionConfig (env-driven 80%/100%).
        // Defaults 150k/192M (3k); 50k/64M (1k) via env override. Halt stays fail-closed.
        this.tracker = new AppendTracker(config.maxPendingRecords, config.maxPendingBytes,
                config.pendingWarningPercent);
        this.health = new HealthProbe(tracker, clock);
        this.timeJumpMonitor = new TimeJumpMonitor(config.clockOffsetLimitMs);
        this.clockMonitorScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ingestion-clock-monitor");
            t.setDaemon(true);
            return t;
        });
        this.heapGate = new JvmHeapReadinessGate();
        this.memoryMonitorScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ingestion-memory-monitor");
            t.setDaemon(true);
            return t;
        });
        this.writer = new RawTickWriter(flussWriter, tracker, config.rawTableName,
                config.appendTimeout,
                config.drainDeadline); // drain deadline (DRAIN_DEADLINE_SECONDS)
        // Async append completions (throughput plan Phase 2): metrics, error
        // counters, and discontinuity evidence are driven by the writer's
        // completion callback — write() no longer blocks on the Fluss ack.
        writer.setOutcomeListener(this::onAppendOutcome);

        // T6-3: bounded queue + single writer worker (O-1). The proto path
        // submits via queue.offer; the worker drains to RawTickWriter →
        // AppendWriter (client-side batch-timeout=1ms coalesces). The NDJSON
        // path keeps the direct write (existing regression suite is
        // synchronous). Queue budgets mirror AppendTracker (Q17: total 192MiB).
        this.queue = new BoundedQueue(config.maxPendingBytes, (int) Math.min(config.maxPendingRecords, Integer.MAX_VALUE));
        this.writerWorker = new WriterWorker(queue, writer, config.drainDeadline);
        writerWorker.start();

        // Telemetry emitter — flushes every 10s to otel-collector:4318
        String otelHost = System.getenv().getOrDefault(
                "OTEL_COLLECTOR_HOST", "otel-collector:4318");
        this.otelHost = otelHost;
        this.metrics = new OtlpMetricsEmitter(otelHost, instanceId);
        metrics.setHealthCallback(health::setOtlpHealthy);
        metrics.start();
        // R-245: report the actual manifest version, not a hardcoded 1. All
        // instruments share the loaded manifest version (SCH-22).
        long manifestVersion = instruments.stream()
                .mapToLong(Instrument::manifestVersion)
                .max().orElse(0L);
        metrics.setManifestVersion(manifestVersion);

        // Quarantine + discontinuity writers (Phase 2b). Test seam: substitute
        // sinks (ING-DQ-010) bypass the Fluss-connection requirement.
        this.quarantineWriter = quarantineSink != null
                ? quarantineSink : new QuarantineWriter(config.flussBootstrap, instanceId);
        this.discontinuityWriter = discontinuitySink != null
                ? discontinuitySink : new DiscontinuityWriter(
                        config.flussBootstrap, instanceId, "arrow-bridge", connectionEpoch);

        // Safety writer (Phase 6A — slot-scoped safety propagation).
        this.manifestFingerprint = InstrumentManifestLoader.computeFingerprint(instruments);
        this.assignedTokenSetHash = com.trading.ingestion.safety.SafetyHaltWriter
                .computeAssignedTokenHash(instruments.stream()
                        .map(Instrument::instrumentToken).toList());
        String accountScope = System.getenv().getOrDefault("ACCOUNT_SCOPE_ID", "QP3796");
        this.safetyHaltWriter = safetySink != null
                ? safetySink : new com.trading.ingestion.safety.SafetyHaltWriter(
                        config.flussBootstrap, instanceId, manifestFingerprint, accountScope);
        String readinessPath = System.getenv("READINESS_FILE_PATH");
        this.readinessFile = readinessPath == null || readinessPath.isBlank()
                ? null : new ReadinessFile(java.nio.file.Paths.get(readinessPath));

        tracker.setListener((level, recs, byt, mr, mb, now) -> {
            if (level == AppendTracker.BackpressureListener.Level.WARNING) {
                LOG.warn("ingestion: backpressure warning (pending={} records, {} bytes)", recs, byt);
            } else {
                LOG.error("ingestion: backpressure critical — halting (pending={} records, {} bytes)", recs, byt);
            }
            // R-246: keep the readiness marker in sync during silent degradation
            // (backpressure/append failures) even when no bridge event arrives.
            updateReadinessFile();
        });
    }

    // ---- main entry point ----

    public static void main(String[] args) throws Exception {
        // 1. Validate ALL configuration keys (throws on failure)
        IngestionConfig config = IngestionConfig.validate();
        LOG.info("ingestion: config validated ({} keys, fluss={})",
                config.toMap().size(), config.flussBootstrap);

        String instanceId = "ingestion-" + System.getenv().getOrDefault("HOSTNAME", "local");

        LOG.info("ingestion: starting (instance={}, build={}, fluss={})",
                instanceId, VERSION, config.flussBootstrap);

        // 2. Check clock offset (C21, I9)
        NtpClockChecker clock = new NtpClockChecker(
                System.getenv().getOrDefault("NTP_SERVER",
                        "ntp.ubuntu.com,time.google.com,in.pool.ntp.org"),
                config.clockOffsetLimitMs,
                config.clockCheckRequired);
        try {
            long offset = clock.measureOffsetMs();
            LOG.info("ingestion: clock offset {}ms (limit {}ms, passed={})",
                    offset, config.clockOffsetLimitMs, clock.isWithinLimit());
            if (!clock.isWithinLimit() && config.clockCheckRequired) {
                LOG.error("ingestion: FATAL — clock offset outside limit and CLOCK_CHECK_REQUIRED=true");
                System.exit(1);
            }
        } catch (NtpClockChecker.NtpException e) {
            if (config.clockCheckRequired) {
                LOG.error("ingestion: FATAL — NTP clock check failed and CLOCK_CHECK_REQUIRED=true: {}",
                        e.getMessage());
                System.exit(1);
            }
            LOG.warn("ingestion: clock check failed: {} — falling back to wall-clock sanity", e.getMessage());
        }

        // 3. Schema verification — read-only by default; DDL mutation only when
        //    ALLOW_RUNTIME_DDL=true (local development only).
        boolean schemaOk = config.allowRuntimeDdl
                ? DdlBootstrap.ensureTables(config.flussBootstrap)
                : DdlBootstrap.verifyTables(config.flussBootstrap);
        if (!schemaOk) {
            LOG.error("ingestion: FATAL — Fluss schema verification failed (allowRuntimeDdl={}); "
                    + "run DDL first or set ALLOW_RUNTIME_DDL=true for local dev",
                    config.allowRuntimeDdl);
            System.exit(1);
        }

        // 4. Connect to Fluss and verify schema version (D3)
        FlussRowConverter converter = FlussClientAdapter.connect(
                config.flussBootstrap, config.rawTableName,
                config.flussWriterBatchTimeoutMs);
        LOG.info("ingestion: Fluss connected (bootstrap={}, table={})",
                config.flussBootstrap, config.rawTableName);

        // 4. Load instrument manifest (D4) — validate version + fingerprint (SCH-22)
        InstrumentManifestLoader.ManifestResult manifestResult =
                InstrumentManifestLoader.loadDefault();
        if (manifestResult.instruments().isEmpty()) {
            LOG.error("ingestion: FATAL — manifest load returned empty instrument set");
            System.exit(1);
        }
        LOG.info("ingestion: manifest loaded (instruments={}, approved={}, version={}, fingerprint={})",
                manifestResult.instrumentCount(), manifestResult.approved(),
                manifestResult.version(),
                manifestResult.fingerprint().substring(0, Math.min(12, manifestResult.fingerprint().length())));
        List<Instrument> instruments = manifestResult.instruments();

        // 5. Create the service
        IngestionService service = new IngestionService(
                instanceId, instruments, converter, config, clock);

        // 5a. Fluss schema verified + connected → mark Fluss readiness dimension.
        service.health().setFlussReady(true);

        // 5b. Uncertainty journal must be writable before we accept data.
        if (!service.journal.ensureWritable()) {
            LOG.error("ingestion: FATAL — uncertainty journal not writable; "
                    + "set UNCERTAINTY_JOURNAL_PATH to a writable path");
            System.exit(1);
        }

        // Capture the main thread so the shutdown hook can join it (bounded) —
        // without the join, the JVM halts the moment the hook returns and the
        // main thread's final "bridge loop ended" report races the halt.
        service.mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("ingestion: shutdown requested");
            service.shutdown();
        }));

        // 6. Launch Go arrow-bridge and pipe NDJSON through Java (D7, I1, I6)
        String bridgeBin = System.getenv().getOrDefault("ARROW_BRIDGE_BIN", "/app/arrow-bridge");
        service.runWithBridge(bridgeBin);
    }

    // ---- bridge subprocess loop ----

    /**
     * Validates that the Go arrow-bridge binary exists and is runnable before
     * launch (startup step 6). Throws {@link IllegalStateException} with a
     * clear message if the binary is missing or not executable — a FATAL
     * startup error that propagates out of {@link #main} to a non-zero exit.
     */
    static void requireBridgeBinary(String bridgeBinary) {
        if (bridgeBinary == null || bridgeBinary.isBlank()) {
            throw new IllegalStateException(
                    "arrow-bridge binary path is empty (set ARROW_BRIDGE_BIN)");
        }
        java.nio.file.Path bin = java.nio.file.Paths.get(bridgeBinary);
        if (!java.nio.file.Files.exists(bin)) {
            throw new IllegalStateException(
                    "arrow-bridge binary not found: " + bridgeBinary
                            + " (set ARROW_BRIDGE_BIN to the correct path)");
        }
        if (!java.nio.file.Files.isRegularFile(bin) || !java.nio.file.Files.isExecutable(bin)) {
            throw new IllegalStateException(
                    "arrow-bridge binary is not runnable: " + bridgeBinary
                            + " (expected a regular executable file; set ARROW_BRIDGE_BIN)");
        }
    }

    /**
     * Launch the Go arrow-bridge as a subprocess, read NDJSON from its stdout,
     * and pipe its stderr into SLF4J. This replaces the shell pipe
     * ({@code arrow-bridge | java}) with Java-managed lifecycle.
     *
     * <p>On bridge exit, records the exit code and (on non-zero) calls
     * {@link #shutdown()}.
     */
    public void runWithBridge(String bridgeBinary) {
        requireBridgeBinary(bridgeBinary);
        running = true;
        subscriptionPaused = false;

        LOG.info("ingestion: launching arrow-bridge (binary={}, instance={})",
                bridgeBinary, instanceId);

        // R-108: broker-staleness must be detected even while readLine() blocks
        // during a genuine feed outage — the ING-1 inline check only runs after
        // a new frame arrives. A watchdog thread evaluates staleness every 5s.
        stalenessWatchdog.scheduleAtFixedRate(() -> {
            if (!running) return;
            long nowNanos = System.nanoTime();
            long msSinceLastFrame = lastFrameNanos > 0
                    ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nowNanos - lastFrameNanos)
                    : 0;
            if (lastFrameNanos > 0 && msSinceLastFrame > FRAME_STALE_MS
                    && health.isBrokerConnected()) {
                LOG.warn("ingestion: broker data stale ({}ms since last frame) — marking disconnected",
                        msSinceLastFrame);
                health.setBrokerConnected(false);
                metrics.setBridgeConnected(false);
                updateReadinessFile();
            }
        }, 5, 5, java.util.concurrent.TimeUnit.SECONDS);

        // ---- Subscription completeness tracking (ING-2) ----
        java.util.Set<Long> seenTokens = java.util.concurrent.ConcurrentHashMap.newKeySet();
        long lastSubscriptionWarningNanos = System.nanoTime();

        // ING-FAIL-007: periodic clock-offset re-measurement — a violation
        // crossing CLOCK_OFFSET_LIMIT_MS emits a TIME_JUMP discontinuity
        // (once per episode) and keeps the readiness clock dimension fresh.
        startClockMonitor();
        startMemoryMonitor();

        // Start with broker not connected — set to true on first frame
        health.setBrokerConnected(false);
        health.setSubscriptionComplete(false);
        metrics.setBridgeConnected(false);

        int restartCount = 0;
        Process bridgeProcess = null;
        boolean bridgeLoopDone = false;

        while (running && restartCount <= MAX_BRIDGE_RESTARTS) {
            long bridgeStartNanos = System.nanoTime();
            lastSubscriptionWarningNanos = bridgeStartNanos;

            try {
                bridgeProcess = startBridge(bridgeBinary);
                currentBridgeProcess = bridgeProcess;
                final Process proc = bridgeProcess;

                Thread stderrThread = new Thread(() -> drainStderr(proc),
                        "bridge-stderr");
                stderrThread.setDaemon(true);
                stderrThread.start();
                currentBridgeStderrThread = stderrThread;

                // T6-2: sniff the transport. If the bridge emits proto frames
                // (TRANSPORT=proto|grpc), route binary frames; else NDJSON
                // (TRANSPORT=pipe fallback / rollback, T6-RB1).
                java.io.InputStream bridgeIn = bridgeProcess.getInputStream();
                ProtoFrameReader protoReader = new ProtoFrameReader(bridgeIn,
                        new ProtoFrameReader.FrameHandler() {
                            @Override
                            public void onMarketBatch(com.trading.ingestion.transport.MarketDataBatch batch) {
                                for (com.trading.ingestion.transport.TickEvent ev : batch.getEventsList()) {
                                    handleFrameArrival();
                                    processTickEvent(ev, batch.getConnectionId(), batch.getConnectionEpoch());
                                }
                            }

                            @Override
                            public void onControl(com.trading.ingestion.transport.ControlRecord control) {
                                handleFrameArrival();
                                handleControlRecord(control);
                            }
                        });
                boolean protoMode = protoReader.sniffProto();
                if (protoMode) {
                    LOG.info("ingestion: bridge transport = proto frames (T6)");
                    // First frame was already delivered by sniffProto.
                    protoReader.readLoop();
                } else {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(protoReader.stream(), StandardCharsets.UTF_8))) {

                    String line;
                    while (running && (line = reader.readLine()) != null) {
                        line = line.strip();
                        if (line.isEmpty()) continue;

                        long nowNanos = System.nanoTime();

                        // ---- ING-1: Broker disconnect detection via frame staleness ----
                        long msSinceLastFrame = (lastFrameNanos > 0)
                                ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nowNanos - lastFrameNanos)
                                : 0;
                        if (lastFrameNanos > 0 && msSinceLastFrame > FRAME_STALE_MS && health.isBrokerConnected()) {
                            LOG.warn("ingestion: broker data stale ({}ms since last frame) — marking disconnected",
                                    msSinceLastFrame);
                            health.setBrokerConnected(false);
                            metrics.setBridgeConnected(false);
                        }

                        // Record frame arrival
                        lastFrameNanos = nowNanos;
                        if (!health.isBrokerConnected()) {
                            String userInfo = config.arrowUserId.isBlank() ? "token" : config.arrowUserId;
                            LOG.info("ingestion: ✅ CONNECTED to Arrow Trade as user {} — broker data flowing", userInfo);
                            health.setBrokerConnected(true);
                            metrics.setBridgeConnected(true);
                        }
                        health.setLastFrameReceived(nowNanos);

                        // ---- ING-2: Subscription completeness check ----
                        if (!health.isSubscriptionComplete()) {
                            long msSinceStart = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                    nowNanos - bridgeStartNanos);
                            // Try to extract token from JSON (lightweight parse before full processLine)
                            try {
                                int idx = line.indexOf("\"token\"");
                                if (idx >= 0) {
                                    int colon = line.indexOf(':', idx);
                                    if (colon >= 0) {
                                        int start = colon + 1;
                                        while (start < line.length() && Character.isWhitespace(line.charAt(start))) start++;
                                        int end = start;
                                        while (end < line.length() && Character.isDigit(line.charAt(end))) end++;
                                        if (end > start) {
                                            long token = Long.parseLong(line.substring(start, end));
                                            seenTokens.add(token);
                                        }
                                    }
                                }
                            } catch (Exception ignore) { /* lightweight parse failure — skip */ }

                            if (msSinceStart > SUBSCRIPTION_COMPLETENESS_TIMEOUT_MS) {
                                if (seenTokens.size() >= instrumentMap.size()) {
                                    LOG.info("ingestion: subscription complete ({} of {} tokens seen in {}ms)",
                                            seenTokens.size(), instrumentMap.size(), msSinceStart);
                                    health.setSubscriptionComplete(true);
                                } else {
                                    long msSinceLastWarning = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                            nowNanos - lastSubscriptionWarningNanos);
                                    if (msSinceLastWarning >= SUBSCRIPTION_COMPLETENESS_TIMEOUT_MS) {
                                        LOG.warn("ingestion: subscription incomplete after {}ms — {} of {} tokens seen",
                                                msSinceStart, seenTokens.size(), instrumentMap.size());
                                        lastSubscriptionWarningNanos = nowNanos;
                                    }
                                    health.setSubscriptionComplete(false);
                                }
                            }
                        }

                        // ---- ING-3: Slow-Fluss controlled pause ----
                        long pendingRecs = tracker.pendingRecords();
                        // R-109: the pause threshold must use the CONFIGURED max pending
                        // records (tracker.maxPendingRecords()), not the static 10,000 — a
                        // smaller configured limit would never reach the 90% pause.
                        long maxRecs = tracker.maxPendingRecords();
                        double pendingPct = (double) pendingRecs / maxRecs;

                        if (!subscriptionPaused && pendingPct >= SLOW_FLUSS_PAUSE_PERCENT) {
                            subscriptionPaused = true;
                            LOG.warn("ingestion: Slow-Fluss backpressure — pending at {}% ({} of {} records), "
                                    + "pausing subscription reads to protect memory",
                                    String.format("%.0f", pendingPct * 100), pendingRecs, maxRecs);
                            metrics.incrementAcknowledgedLoss();
                        }

                        if (subscriptionPaused) {
                            if (pendingPct <= SLOW_FLUSS_RESUME_PERCENT) {
                                subscriptionPaused = false;
                                LOG.info("ingestion: Slow-Fluss backpressure resolved — pending at {}% ({} records), "
                                        + "resuming subscription reads",
                                        String.format("%.0f", pendingPct * 100), pendingRecs);
                            } else {
                                // Skip processing this frame — queue is still draining
                                continue;
                            }
                        }

                        processLine(line);
                    }
                }
                } // else NDJSON path (T6-2)

                int exitCode = bridgeProcess.waitFor();
                // A shutdown-begun exit is a requested exit: the hook is
                // tearing the bridge down, so a non-zero code (e.g. the
                // forced-kill fallback, exit 137) must not be logged as a
                // crash nor trigger a restart.
                boolean requested = exitCode == 0 || !running || shutdownStarted.get();
                recordBridgeExit(exitCode, requested, restartCount);
                stderrThread.join(5_000);

                switch (bridgeRestartDecision(running, shutdownStarted.get(), exitCode, restartCount)) {
                case RESTART:
                    restartCount++;
                    LOG.warn("ingestion: restarting bridge after unexpected exit (attempt {} of {})",
                            restartCount, MAX_BRIDGE_RESTARTS + 1);
                    // Wait 1 second before restart (plan).
                    try {
                        Thread.sleep(BRIDGE_RESTART_WAIT_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        bridgeLoopDone = true;
                        break;
                    }
                    // Reset slot states to AUTHENTICATING for the fresh process.
                    health.resetSlotsToAuthenticating();
                    // R-110: the fresh bridge process must re-establish
                    // subscription completeness from zero — tokens seen by the
                    // previous process must not count toward the new one.
                    seenTokens.clear();
                    health.setSubscriptionComplete(false);
                    metrics.setBridgeConnected(false);
                    break;
                case TERMINAL:
                    LOG.error("ingestion: bridge exited unexpectedly {} time(s) — terminal (exitCode={})",
                            restartCount + 1, exitCode);
                    bridgeLoopDone = true;
                    break;
                default: // NO_RESTART — normal shutdown
                    bridgeLoopDone = true;
                    break;
                }
                if (bridgeLoopDone) {
                    break;
                }

            } catch (Exception e) {
                LOG.error("ingestion: bridge process error", e);
                break;
            }
        }

        LOG.info("ingestion: bridge loop ended (ticks={}, errors={}, restarts={})",
                frameCount.get(), errorCount.get(), restartCount);
        shutdown();
    }

    /**
     * Start the Go arrow-bridge subprocess with the current environment.
     */
    private Process startBridge(String bridgeBinary) throws java.io.IOException {
        ProcessBuilder pb = new ProcessBuilder(bridgeBinary);
        pb.environment().putAll(System.getenv());
        pb.redirectErrorStream(false);
        Process process = pb.start();
        LOG.info("ingestion: arrow-bridge started (pid={})", process.pid());
        return process;
    }

    /**
     * Bridge restart policy (plan §IngestionService): an unexpected exit is
     * restarted once; a second unexpected exit in the same process is terminal;
     * a requested exit (code 0 or shutdown begun) is never restarted.
     */
    static BridgeRestartDecision bridgeRestartDecision(boolean running, boolean shutdownInProgress,
                                                       int exitCode, int restartCount) {
        if (!running || shutdownInProgress || exitCode == 0) return BridgeRestartDecision.NO_RESTART;
        return restartCount >= MAX_BRIDGE_RESTARTS
                ? BridgeRestartDecision.TERMINAL
                : BridgeRestartDecision.RESTART;
    }

    /** Outcome of the bridge restart policy. */
    enum BridgeRestartDecision { RESTART, TERMINAL, NO_RESTART }

    /**
     * Drain bridge stderr into SLF4J. Classifies each line per plan §log4j2:
     * lines containing `failed`, `error`, `ended`, `stalled`, `partial`, or
     * `rejected` log at WARN/ERROR; all other bridge diagnostics log at INFO.
     * Tick stdout is never logged.
     */
    private void drainStderr(Process process) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String safe = sanitizeLog(line);
                if (classifyBridgeLine(safe)) {
                    LOG.warn("arrow-bridge: {}", safe);
                } else {
                    LOG.info("arrow-bridge: {}", safe);
                }
            }
        } catch (java.io.IOException e) {
            // "Stream closed" is the JDK's Process.destroy() artifact: the
            // parent-side process pipes are closed the moment the bridge is
            // destroyed (destroy() itself, or the forced-kill fallback at
            // shutdown), while the child may still be running. There is
            // nothing left to read — the graceful path signals the bridge
            // without closing the pipes (see signalBridge), so this only
            // happens on the forced path. Not a drain failure; log at INFO.
            if ("Stream closed".equals(e.getMessage())) {
                LOG.info("arrow-bridge: stderr drain ended (stream closed — bridge being torn down)");
            } else {
                LOG.warn("arrow-bridge: stderr drain error: {}", sanitizeLog(e.getMessage()));
            }
        } catch (Exception e) {
            LOG.warn("arrow-bridge: stderr drain error: {}", sanitizeLog(e.getMessage()));
        }
    }

    /**
     * Signal the bridge process (SIGTERM) WITHOUT closing the parent-side
     * process pipes. {@link Process#destroy()} sends SIGTERM but on this JDK
     * also closes the parent's input streams for the child with
     * {@code IOException("Stream closed")} while the child is still running
     * its shutdown work — the bridge-stderr drain thread would die before the
     * bridge's final ARROW_TICK_COUNTS report arrives, the main loop would
     * take the exception path instead of reading the final {@code
     * bridge_shutdown} NDJSON event, and the authoritative per-token count for
     * ING-TCP-001 would be lost from the logs (only the report FILE survives).
     * Sending the signal via {@code kill -TERM <pid>} (POSIX) leaves the pipes
     * open: the bridge writes its final report to stderr, drains, EOFs, and
     * exits 0. Falls back to {@link Process#destroy()} if the signal cannot
     * be delivered (kill missing or non-zero exit).
     */
    private void signalBridge(Process bridgeProcess) {
        try {
            Process kill = new ProcessBuilder("kill", "-TERM", String.valueOf(bridgeProcess.pid()))
                    .redirectErrorStream(true).start();
            if (!kill.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                kill.destroyForcibly();
            }
            if (kill.exitValue() != 0) {
                LOG.warn("ingestion: kill -TERM failed (exit={}); falling back to Process.destroy()",
                        kill.exitValue());
                bridgeProcess.destroy();
            }
        } catch (java.io.IOException e) {
            LOG.warn("ingestion: cannot signal bridge via kill ({}); falling back to Process.destroy()",
                    sanitizeLog(e.getMessage()));
            bridgeProcess.destroy();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            bridgeProcess.destroy();
        }
    }

    /**
     * Plan §log4j2 classification: {@code true} → WARN/ERROR level, {@code false} → INFO.
     * Matches lines containing failed/error/ended/stalled/partial/rejected.
     */
    static boolean classifyBridgeLine(String line) {
        if (line == null || line.isBlank()) return false;
        String l = line.toLowerCase(java.util.Locale.ROOT);
        return l.contains("failed") || l.contains("error") || l.contains("ended")
                || l.contains("stalled") || l.contains("partial") || l.contains("rejected");
    }

    /**
     * Record a bridge process exit as discontinuity evidence (plan §IngestionService).
     *
     * @param exitCode      the process exit code
     * @param requested     true if the exit was requested (normal shutdown, code 0, or shutdown begun)
     * @param restartCount  how many restarts have already occurred
     */
    private void recordBridgeExit(int exitCode, boolean requested, int restartCount) {
        long newEpoch = connectionEpoch.incrementAndGet();
        if (requested) {
            LOG.info("ingestion: bridge exited normally (code={}, epoch={}, restarts={})",
                    exitCode, newEpoch, restartCount);
        } else {
            LOG.error("ingestion: BRIDGE_CRASH exitCode={} instance={} epoch={} restarts={}",
                    exitCode, instanceId, newEpoch, restartCount);
        }
        health.setBrokerConnected(false);
        metrics.setBridgeConnected(false);
        metrics.setChildProcessAlive(false);
        metrics.setActiveSockets(0);

        discontinuityWriter.write(
                DiscontinuityWriter.Reason.DROP,
                "arrow-bridge " + (requested ? "exited" : "crashed") + " with code " + exitCode,
                lastTickSnapshot
        );
    }

    /**
     * T7-F11: check a tick's per-connection sequence for a gap. Emits the
     * metric increment + a SEQUENCE_GAP discontinuity evidence row when a gap
     * is detected. NEVER halts the data path (contract §7.8 F11).
     */
    private void checkSequenceGap(String connectionKey, long seq, long token) {
        if (seq <= 0) {
            return;
        }
        if (sequenceGapMonitor.onTick(connectionKey, seq)) {
            metrics.incrementSequenceGaps();
            discontinuityWriter.write(
                    DiscontinuityWriter.Reason.SEQUENCE_GAP,
                    "feed_sequence_local gap on " + connectionKey + " at seq " + seq
                            + " (token=" + token + ")",
                    lastTickSnapshot
            );
        }
    }

    private void processLine(String jsonLine) {
        frameCount.incrementAndGet();
        try {
            // R-214: parse the NDJSON line exactly ONCE and route by
            // record_type. The old flow ran three full JSON parses per tick
            // (bridge-event readTree + quarantine readTree + GoTick readValue).
            com.fasterxml.jackson.databind.JsonNode jsonNode = MAPPER.readTree(jsonLine);
            if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
                return;
            }
            java.util.Optional<BridgeEvent> bridgeEvent = bridgeEventParser.parse(jsonNode);
            if (bridgeEvent.isPresent()) {
                handleBridgeEvent(bridgeEvent.get());
                return;
            }
            java.util.Optional<BrokerQuarantine> brokerQuarantine = bridgeEventParser.parseQuarantine(jsonNode);
            if (brokerQuarantine.isPresent()) {
                BrokerQuarantine record = brokerQuarantine.get();
                quarantineWriter.write(record.rawPayload(),
                        QuarantineWriter.Reason.valueOf(record.reason()),
                        "bridge broker quarantine",
                        record.token(), null, null);
                metrics.incrementDecodeError(record.reason());
                return;
            }
            // Supervisor health snapshot (additive v2 record). Must be routed
            // BEFORE the GoTick fall-through — a bridge_metrics line has no
            // feed/state and would otherwise be quarantined as INVALID_SCHEMA.
            java.util.Optional<BridgeMetrics> bridgeMetrics = bridgeEventParser.parseMetrics(jsonNode);
            if (bridgeMetrics.isPresent()) {
                BridgeMetrics m = bridgeMetrics.get();
                // The Go supervisor is authoritative for these (10s cadence);
                // the lifecycle-derived values are only pre-metrics fallbacks.
                metrics.setReconnectConsecutive(m.reconnectConsecutive());
                metrics.setActiveSockets(m.activeSockets());
                metrics.setGoGoroutines(m.goGoroutines());
                return;
            }
            // 1. Bind the tree → GoTick (no third parse)
            GoTick gt = MAPPER.treeToValue(jsonNode, GoTick.class);
            long receiveTsMs = gt.received_ts_ms > 0 ? gt.received_ts_ms : System.currentTimeMillis();
            byte[] rawBytes = jsonLine.getBytes(StandardCharsets.UTF_8);

            // T7-F11: per-connection sequence-gap detection (evidence only).
            checkSequenceGap(
                    (gt.slot_id == null ? "?" : gt.slot_id) + "/" + gt.connection_epoch,
                    gt.feed_sequence_local, gt.token);

            // 1a. Validate the original-bytes hash (plan §Tick / §Data Flow):
            //     raw_payload is the exact decompressed broker packet bytes
            //     (Base64); payload_hash is their SHA-256. Decoded JSON must not
            //     replace those bytes. A mismatch quarantines the record.
            //     A2 decision: INGEST_VALIDATE_PAYLOAD_HASH=false skips the
            //     per-tick recompute (proto path carries the hash inside the
            //     frame; validation is the T3/T6 job). Default true (safety).
            byte[] packetBytes;
            if (config.validatePayloadHash) {
                packetBytes = decodeAndValidatePayload(gt, rawBytes);
                if (packetBytes == null) {
                    return; // already quarantined
                }
            } else {
                // No SHA-256 round trip: decode only (validation disabled).
                // Malformed Base64 still quarantines — the payload must be
                // decodable regardless of hash policy.
                packetBytes = decodePayloadOnly(gt, rawBytes);
                if (packetBytes == null) {
                    return; // already quarantined
                }
            }

            if (gt.ts_ms <= 0) {
                quarantineWriter.write(rawBytes, QuarantineWriter.Reason.INVALID_VALUES,
                        "missing or non-positive broker timestamp", gt.token, null, null);
                return;
            }
            // Freshness gate (T10 NTP 2s gate): stale or future broker timestamps are quarantined
            // BEFORE any trade classification, so stale data can never become a
            // trade decision. Future ticks > ARROW_MAX_FUTURE_EVENT_SKEW_MS (2s, T10)
            // are quarantined per slot (slot_id + epoch) and emit slot-scoped safety
            // evidence; ticks <2s keep existing behavior. (Plan §Production hardening: 1376.)
            FreshnessDecision fd = classifyFreshness(gt.ts_ms, receiveTsMs,
                    config.arrowMaxFutureEventSkewMs, config.arrowMaxEventAgeMs);
            if (fd == FreshnessDecision.FUTURE) {
                quarantineWriter.write(rawBytes, QuarantineWriter.Reason.FUTURE_BROKER_TIMESTAMP,
                        "event timestamp exceeds receive time", gt.token, null, null);
                emitQualityUnsafe(gt.slot_id, gt.connection_epoch,
                        QuarantineWriter.Reason.FUTURE_BROKER_TIMESTAMP);
                return;
            }
            if (fd == FreshnessDecision.STALE) {
                quarantineWriter.write(rawBytes, QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP,
                        "event timestamp is older than configured age", gt.token, null, null);
                metrics.incrementDecodeError("STALE_BROKER_TIMESTAMP");
                emitQualityUnsafe(gt.slot_id, gt.connection_epoch,
                        QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP);
                return;
            }

            // 2. Resolve instrument
            Instrument instr = instrumentMap.get(gt.token);
            if (instr == null) {
                LOG.warn("ingestion: missing instrument token={}", gt.token);
                // E4, I3: Missing instrument → quarantine, don't silently skip
                quarantineWriter.write(rawBytes,
                        QuarantineWriter.Reason.MISSING_INSTRUMENT,
                        "token=" + gt.token + " not found in daily manifest",
                        gt.token, null, null);
                metrics.incrementDecodeError("MISSING_INSTRUMENT");
                return;
            }

            // 3. Validate
            ValidityClassification validity;
            String validityReason = null;
            // AC-ING-002: an unrecognized broker protocol version is quarantined
            // (UNKNOWN_VERSION) before any trade classification — the raw bytes
            // are preserved but the tick can never become a trade decision.
            // HFT is the only supported feed (the Standard feed was removed
            // 2026-08-14); any other feed value is rejected.
            if (!"hft".equals(gt.feed)) {
                quarantineWriter.write(rawBytes,
                        QuarantineWriter.Reason.INVALID_SCHEMA,
                        "unknown broker protocol version: " + (gt.feed == null ? "<null>" : gt.feed),
                        gt.token, null, null);
                metrics.incrementDecodeError("UNKNOWN_VERSION");
                return;
            }
            if (gt.ltp_paise <= 0 && (gt.mode.equals("ltp") || gt.mode.equals("ltpc"))) {
                validity = ValidityClassification.INVALID_VALUES;
                validityReason = "ltp_paise <= 0";
            } else if (gt.mode.equals("ltp") || gt.mode.equals("ltpc") || gt.mode.equals("full")) {
                validity = ValidityClassification.VALID_TRADE;
            } else {
                validity = ValidityClassification.VALID_NON_TRADE;
            }

            if (validity == ValidityClassification.INVALID_VALUES) {
                // E4: Invalid values → quarantine with instrument context
                quarantineWriter.write(rawBytes,
                        QuarantineWriter.Reason.INVALID_VALUES,
                        validityReason,
                        instr.instrumentToken(), instr.exchange(), instr.tradingSymbol());
                metrics.incrementDecodeError("INVALID_VALUES");
                return;
            }

            String tickType = (validity == ValidityClassification.VALID_TRADE) ? "TRADE" : "QUOTE";

            // 4. Fingerprint
            FingerprintBuilder.Result fp = FingerprintBuilder.build(
                    gt.connection_epoch,
                    gt.token,
                    gt.ts_ms,
                    tickType,
                    gt.ltp_paise,
                    gt.ltq,
                    gt.bid_px != null && gt.bid_px[0] != 0 ? gt.bid_px[0] : 0L,
                    gt.ask_px != null && gt.ask_px[0] != 0 ? gt.ask_px[0] : 0L
            );

            // 5. Build RawTick (raw_payload = exact decompressed broker packet
            //    bytes validated in step 1a, not the NDJSON line)
            RawTick raw = new RawTick.Builder()
                    .rawPayload(packetBytes)
                    .payloadHash(gt.payload_hash)
                    .hashAlgorithm(FINGERPRINT_ALGO)
                    .protocolVersion(gt.feed)
                    .decoderVersion("arrow-go-sdk")
                    .receiveTime(Instant.now())
                    .receiveTimeNanos(System.nanoTime())
                    .build();

            // 6. Build TickPacket
            TickPacket packet = new TickPacket.Builder()
                    .raw(raw)
                    .validity(validity)
                    .validityReason(validityReason)
                    .instrumentToken(gt.token)
                    .tradingSymbol(instr.tradingSymbol())
                    .exchange(instr.exchange())
                    .eventTime(Instant.ofEpochMilli(gt.ts_ms))
                    .ingestTs(Instant.now())
                    .lastPricePaise(gt.ltp_paise)
                    .volume(gt.volume)
                    .ohlcOpenPaise(gt.open_paise)
                    .ohlcHighPaise(gt.high_paise)
                    .ohlcLowPaise(gt.low_paise)
                    .ohlcClosePaise(gt.close_paise)
                    .eventFingerprint(fp.hash())
                    .fingerprintVersion(fp.version())
                    .connectionId(gt.connection_id == null || gt.connection_id.isBlank() ? "arrow-bridge" : gt.connection_id)
                    .connectionEpoch(gt.connection_epoch)
                    .instanceId(instanceId)
                    .build();

            // 7. Submit to bounded writer (async append — each tick is its
            //    own append call; the terminal outcome is delivered via
            //    onAppendOutcome when the Fluss ack completes)
            RawTickWriter.AppendOutcome outcome = writer.write(packet);

            // Record receive-side metrics
            metrics.recordTick(packetBytes.length);
            metrics.incrementFingerprint();

            if (outcome.status() == RawTickWriter.Status.REJECTED) {
                LOG.warn("ingestion: tick rejected (reason={})", outcome.detail());
                metrics.incrementAcknowledgedLoss();
            }

            // 8. Update health probe + gauge metrics + lastTickSnapshot
            health.setLastFrameReceived(System.nanoTime());
            metrics.setPendingRecords(tracker.pendingRecords());
            metrics.setPendingBytes(tracker.pendingBytes());
            metrics.setIngestionReady(health.isReady());
            metrics.setBridgeConnected(true);
            refreshResourceMetrics();
            // R-246: the readiness marker must track the tick-processing path
            // too, not just bridge lifecycle events.
            updateReadinessFile();

        } catch (JsonProcessingException e) {
            // ING-DQ-001: a malformed NDJSON line must produce quarantine
            // evidence — never a silent drop (REQ-ING: every accepted or
            // rejected packet SHALL produce audit evidence). The raw line
            // bytes are preserved as the quarantine payload; the detail is a
            // static constant because Jackson's message embeds the offending
            // input snippet (line content must not leak into logs).
            MalformedJsonDecision decision = malformedJsonDecision(rawLineBytes(jsonLine));
            quarantineWriter.write(decision.rawPayload(), decision.reason(), decision.detail());
            metrics.incrementDecodeError("MALFORMED_JSON");
        } catch (Exception e) {
            errorCount.incrementAndGet();
            metrics.incrementDecodeError(e.getClass().getSimpleName());
            // Write quarantine record for any unhandled processing failure
            try {
                byte[] rawBytes = jsonLine != null ? jsonLine.getBytes(StandardCharsets.UTF_8) : null;
                quarantineWriter.write(rawBytes,
                        QuarantineWriter.Reason.INTERNAL_ERROR,
                        e.getClass().getSimpleName() + ": " + e.getMessage());
            } catch (Exception nested) {
                LOG.error("ingestion: quarantine writer failed: {}", nested.getMessage());
            }
            LOG.warn("ingestion: line processing error: {}", e.getMessage());
        }
    }

    /**
     * Shared frame-arrival bookkeeping (proto path): ING-1 broker-staleness
     * detection + connection state. Mirrors the NDJSON loop's inline block.
     */
    private void handleFrameArrival() {
        long nowNanos = System.nanoTime();
        long msSinceLastFrame = (lastFrameNanos > 0)
                ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nowNanos - lastFrameNanos)
                : 0;
        if (lastFrameNanos > 0 && msSinceLastFrame > FRAME_STALE_MS && health.isBrokerConnected()) {
            LOG.warn("ingestion: broker data stale ({}ms since last frame) — marking disconnected",
                    msSinceLastFrame);
            health.setBrokerConnected(false);
            metrics.setBridgeConnected(false);
        }
        lastFrameNanos = nowNanos;
        if (!health.isBrokerConnected()) {
            String userInfo = config.arrowUserId.isBlank() ? "token" : config.arrowUserId;
            LOG.info("ingestion: ✅ CONNECTED to Arrow Trade as user {} — broker data flowing", userInfo);
            health.setBrokerConnected(true);
            metrics.setBridgeConnected(true);
        }
        health.setLastFrameReceived(nowNanos);
    }

    /**
     * T6-I2: route a ControlRecord (proto transport) to the existing control
     * handlers — bridge_event / bridge_metrics / broker_quarantine are
     * distinguished by record_type; a malformed or unknown record is
     * quarantined, never allowed to corrupt market-data processing.
     */
    private void handleControlRecord(com.trading.ingestion.transport.ControlRecord cr) {
        String rt = cr.getRecordType();
        try {
            switch (rt) {
                case "bridge_event" -> {
                    BridgeEvent event = new BridgeEvent(
                            cr.getEvent(),
                            BridgeEvent.CONTRACT_VERSION,
                            cr.getSlotId(),
                            cr.getConnectionId(),
                            cr.getConnectionEpoch(),
                            cr.getState(),
                            cr.getAssignedTokens(),
                            cr.getAcknowledgedTokens(),
                            cr.getRejectedTokens(),
                            cr.getReason(),
                            cr.getReceivedTsMs(),
                            cr.getManifestFingerprint(),
                            cr.getAssignedTokenSetHash());
                    handleBridgeEvent(event);
                }
                case "bridge_metrics" -> {
                    BridgeMetrics m = new BridgeMetrics(
                            cr.getTsMs(),
                            cr.getReconnectConsecutive(),
                            cr.getActiveSockets(),
                            cr.getGoGoroutines());
                    metrics.setReconnectConsecutive(m.reconnectConsecutive());
                    metrics.setActiveSockets(m.activeSockets());
                    metrics.setGoGoroutines(m.goGoroutines());
                }
                case "broker_quarantine" -> {
                    BrokerQuarantine record = new BrokerQuarantine(
                            BrokerQuarantine.CONTRACT_VERSION,
                            cr.getSlotId(),
                            cr.getConnectionId(),
                            cr.getConnectionEpoch(),
                            cr.getRawPayload() == null ? 0L : cr.getReceivedTsMs(),
                            cr.getReason(),
                            cr.getRawPayload() == null ? new byte[0] : cr.getRawPayload().toByteArray(),
                            "",
                            cr.getReceivedTsMs());
                    quarantineWriter.write(record.rawPayload(),
                            QuarantineWriter.Reason.valueOf(record.reason()),
                            "bridge broker quarantine (proto)",
                            record.token(), null, null);
                    metrics.incrementDecodeError(record.reason());
                }
                default -> {
                    // Unknown control record type — quarantine evidence, never
                    // a silent drop (fail-closed, Q18).
                    byte[] raw = cr.getRawPayload() == null ? new byte[0] : cr.getRawPayload().toByteArray();
                    quarantineWriter.write(raw, QuarantineWriter.Reason.INVALID_SCHEMA,
                            "unknown control record_type: " + rt, 0L, null, null);
                    metrics.incrementDecodeError("UNKNOWN_CONTROL_TYPE");
                }
            }
        } catch (Exception e) {
            // A malformed control record must not corrupt market-data
            // processing (T6-I2) — quarantine and continue.
            errorCount.incrementAndGet();
            LOG.warn("ingestion: control record error (type={}): {}", rt, e.getMessage());
            try {
                quarantineWriter.write(
                        cr.getRawPayload() == null ? new byte[0] : cr.getRawPayload().toByteArray(),
                        QuarantineWriter.Reason.INTERNAL_ERROR,
                        "control record " + rt + ": " + e.getClass().getSimpleName(),
                        0L, null, null);
            } catch (Exception nested) {
                LOG.error("ingestion: quarantine writer failed: {}", nested.getMessage());
            }
        }
    }

    /**
     * T6 proto-path per-tick processing (contract §6 T6). Mirrors the NDJSON
     * path gates EXACTLY (freshness, instrument, validity, fingerprint,
     * quarantine) but consumes a decoded {@code TickEvent} — the raw payload
     * is already bytes (no base64 round-trip, Q3/Q6) and the payload hash is
     * already validated at the frame level. Rows built here are byte-identical
     * to the NDJSON path (T6-I3 pipe parity).
     */
    void processTickEvent(com.trading.ingestion.transport.TickEvent ev,
                           String batchConnectionId, long batchConnectionEpoch) {
        frameCount.incrementAndGet();
        try {
            long receiveTsMs = ev.getReceivedMs() > 0 ? ev.getReceivedMs() : System.currentTimeMillis();
            byte[] packetBytes = ev.getRawPayload().toByteArray();

            // T7-F11: per-connection sequence-gap detection (evidence only).
            checkSequenceGap(
                    (ev.getSlotId().isEmpty() ? "?" : ev.getSlotId()) + "/" + batchConnectionEpoch,
                    ev.getFeedSequenceLocal(), ev.getToken());

            // Same gates as the NDJSON path (processLine steps 1b-8).
            if (ev.getTsMs() <= 0) {
                quarantineWriter.write(packetBytes, QuarantineWriter.Reason.INVALID_VALUES,
                        "missing or non-positive broker timestamp", (long) ev.getToken(), null, null);
                return;
            }
            FreshnessDecision fd = classifyFreshness(ev.getTsMs(), receiveTsMs,
                    config.arrowMaxFutureEventSkewMs, config.arrowMaxEventAgeMs);
            if (fd == FreshnessDecision.FUTURE) {
                quarantineWriter.write(packetBytes, QuarantineWriter.Reason.FUTURE_BROKER_TIMESTAMP,
                        "event timestamp exceeds receive time", (long) ev.getToken(), null, null);
                emitQualityUnsafe(ev.getSlotId(), batchConnectionEpoch,
                        QuarantineWriter.Reason.FUTURE_BROKER_TIMESTAMP);
                return;
            }
            if (fd == FreshnessDecision.STALE) {
                quarantineWriter.write(packetBytes, QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP,
                        "event timestamp is older than configured age", (long) ev.getToken(), null, null);
                metrics.incrementDecodeError("STALE_BROKER_TIMESTAMP");
                emitQualityUnsafe(ev.getSlotId(), batchConnectionEpoch,
                        QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP);
                return;
            }

            Instrument instr = instrumentMap.get((long) (long) ev.getToken());
            if (instr == null) {
                LOG.warn("ingestion: missing instrument token={}", (long) ev.getToken());
                quarantineWriter.write(packetBytes,
                        QuarantineWriter.Reason.MISSING_INSTRUMENT,
                        "token=" + ev.getToken() + " not found in daily manifest",
                        (long) ev.getToken(), null, null);
                metrics.incrementDecodeError("MISSING_INSTRUMENT");
                return;
            }

            ValidityClassification validity;
            String validityReason = null;
            if (!"hft".equals(ev.getFeed())) {
                quarantineWriter.write(packetBytes,
                        QuarantineWriter.Reason.INVALID_SCHEMA,
                        "unknown broker protocol version: " + (ev.getFeed().isEmpty() ? "<null>" : ev.getFeed()),
                        (long) ev.getToken(), null, null);
                metrics.incrementDecodeError("UNKNOWN_VERSION");
                return;
            }
            String mode = ev.getMode();
            if (ev.getLtpPaise() <= 0 && (mode.equals("ltp") || mode.equals("ltpc"))) {
                validity = ValidityClassification.INVALID_VALUES;
                validityReason = "ltp_paise <= 0";
            } else if (mode.equals("ltp") || mode.equals("ltpc") || mode.equals("full")) {
                validity = ValidityClassification.VALID_TRADE;
            } else {
                validity = ValidityClassification.VALID_NON_TRADE;
            }

            if (validity == ValidityClassification.INVALID_VALUES) {
                quarantineWriter.write(packetBytes,
                        QuarantineWriter.Reason.INVALID_VALUES,
                        validityReason,
                        instr.instrumentToken(), instr.exchange(), instr.tradingSymbol());
                metrics.incrementDecodeError("INVALID_VALUES");
                return;
            }

            String tickType = (validity == ValidityClassification.VALID_TRADE) ? "TRADE" : "QUOTE";
            FingerprintBuilder.Result fp = FingerprintBuilder.build(
                    batchConnectionEpoch,
                    (long) ev.getToken(),
                    ev.getTsMs(),
                    tickType,
                    ev.getLtpPaise(),
                    ev.getLtq(),
                    ev.getBidPxCount() > 0 && ev.getBidPx(0) != 0 ? (long) ev.getBidPx(0) : 0L,
                    ev.getAskPxCount() > 0 && ev.getAskPx(0) != 0 ? (long) ev.getAskPx(0) : 0L
            );

            RawTick raw = new RawTick.Builder()
                    .rawPayload(packetBytes)
                    .payloadHash(ev.getPayloadHash().isEmpty() ? "" : ev.getPayloadHash().toStringUtf8())
                    .hashAlgorithm(FINGERPRINT_ALGO)
                    .protocolVersion(ev.getFeed())
                    .decoderVersion("arrow-go-sdk")
                    .receiveTime(Instant.now())
                    .receiveTimeNanos(System.nanoTime())
                    .build();

            TickPacket packet = new TickPacket.Builder()
                    .raw(raw)
                    .validity(validity)
                    .validityReason(validityReason)
                    .instrumentToken((long) ev.getToken())
                    .tradingSymbol(instr.tradingSymbol())
                    .exchange(instr.exchange())
                    .eventTime(Instant.ofEpochMilli(ev.getTsMs()))
                    .ingestTs(Instant.now())
                    .lastPricePaise(ev.getLtpPaise())
                    .volume(ev.getVolume())
                    .ohlcOpenPaise(ev.getOpenPaise())
                    .ohlcHighPaise(ev.getHighPaise())
                    .ohlcLowPaise(ev.getLowPaise())
                    .ohlcClosePaise(ev.getClosePaise())
                    .eventFingerprint(fp.hash())
                    .fingerprintVersion(fp.version())
                    .connectionId(batchConnectionId == null || batchConnectionId.isEmpty() ? "arrow-bridge" : batchConnectionId)
                    .connectionEpoch(batchConnectionEpoch)
                    .instanceId(instanceId)
                    .build();

            // T6-3: submit via bounded queue → writer worker (proto path only).
            // The NDJSON path keeps the direct write so the existing regression
            // suite (synchronous processLine assertions) is unaffected.
            int rowBytes = packetBytes.length + 64; // conservative row estimate
            if (!queue.offer(packet, rowBytes)) {
                LOG.warn("ingestion: tick rejected (queue full) token={}", (long) ev.getToken());
                metrics.incrementAcknowledgedLoss();
            }

            metrics.recordTick(packetBytes.length);
            metrics.incrementFingerprint();
            health.setLastFrameReceived(System.nanoTime());
            metrics.setPendingRecords(tracker.pendingRecords());
            metrics.setPendingBytes(tracker.pendingBytes());
            metrics.setIngestionReady(health.isReady());
            metrics.setBridgeConnected(true);
            refreshResourceMetrics();
            updateReadinessFile();
        } catch (Exception e) {
            errorCount.incrementAndGet();
            metrics.incrementDecodeError(e.getClass().getSimpleName());
            try {
                quarantineWriter.write(ev.getRawPayload().toByteArray(),
                        QuarantineWriter.Reason.INTERNAL_ERROR,
                        e.getClass().getSimpleName() + ": " + e.getMessage());
            } catch (Exception nested) {
                LOG.error("ingestion: quarantine writer failed: {}", nested.getMessage());
            }
            LOG.warn("ingestion: proto tick processing error: {}", e.getMessage());
        }
    }

    /**
     * Async append completion (throughput plan Phase 2) — invoked by the
     * writer's background completion for every terminal outcome. Metrics,
     * error counters, and discontinuity evidence move here because
     * {@link #processLine} no longer blocks on the Fluss ack.
     *
     * <p>R-111: lastTickSnapshot is discontinuity evidence — it must only
     * reflect a tick that was actually persisted; only SUCCESS completes
     * with an ack, so only SUCCESS updates the snapshot.
     */
    private void onAppendOutcome(RawTickWriter.AppendOutcome outcome) {
        long latencyMs = outcome.ackTime() != null
                ? java.time.Duration.between(outcome.acceptTime(), outcome.ackTime()).toMillis()
                : -1;
        if (latencyMs >= 0) metrics.recordAppendLatencyMs(latencyMs);

        switch (outcome.status()) {
            case TIMEOUT -> LOG.warn("ingestion: append timeout (rowBytes={})",
                    outcome.rowBytes());
            case UNCERTAIN -> {
                LOG.warn("ingestion: append UNCERTAIN — Fluss may have persisted (rowBytes={}, detail={})",
                        outcome.rowBytes(), outcome.detail());
                errorCount.incrementAndGet();
            }
            case FAILED, FATAL -> {
                errorCount.incrementAndGet();
                metrics.incrementDecodeError("append_" + outcome.status().name().toLowerCase());
            }
            case SUCCESS -> lastTickSnapshot = new DiscontinuityWriter.LastTickSnapshot(
                    outcome.eventTime().toEpochMilli(),
                    outcome.fingerprint(),
                    outcome.instrumentToken(),
                    outcome.exchange(),
                    outcome.tradingSymbol());
            default -> {
                // REJECTED / SKIPPED / ACCEPTED are synchronous outcomes and
                // are never delivered through the listener.
            }
        }
    }

    /**
     * Route a bridge lifecycle event: update health/metrics and persist
     * discontinuity evidence per the plan's mapping.
     */
    private void processBridgeEvent(BridgeEvent event) {
        boolean active = "ACTIVE".equals(event.state())
                && event.assignedTokens() == event.acknowledgedTokens()
                && event.rejectedTokens() == 0;
        health.setBrokerConnected(active);
        health.setSubscriptionComplete(active);
        if (active) health.setLastFrameReceived(System.nanoTime());
        health.updateSlot(event.slotId(), event.state(), event.connectionEpoch(),
                event.assignedTokens(), event.acknowledgedTokens(), event.rejectedTokens(),
                active ? System.nanoTime() : 0L);
        metrics.setConnectionEpoch(event.connectionEpoch());
        // Slot gauges (plan §Monitoring — slot label only, no token/symbol).
        metrics.setSlotState(event.slotId(), active, event.assignedTokens(),
                event.acknowledgedTokens(), event.rejectedTokens(),
                active ? System.nanoTime() : 0L);
        metrics.setSlotCapacityUsedPercent(event.slotId(),
                event.assignedTokens() > 0
                        ? (100.0 * event.acknowledgedTokens()) / event.assignedTokens()
                        : 0.0);
        // Capacity headroom (plan ING-CAP-001): remaining = connection limit
        // − assigned. Negative never happens (assigned ≤ limit by plan
        // construction) but clamp for gauge safety.
        long capacityRemaining = Math.max(0,
                (long) config.arrowHftMaxTokensPerConnection - event.assignedTokens());
        metrics.setSlotCapacityRemaining(event.slotId(), capacityRemaining);
        health.setSlotCapacityRemaining(event.slotId(), capacityRemaining);
        metrics.setActiveSockets(active ? 1 : 0);
        metrics.setChildProcessAlive(true);

        // Slot-identity cross-check (plan §Slot-scoped safety propagation):
        // the bridge's manifest_fingerprint / assigned_token_set_hash must
        // match Java's manifest-derived digests. Warn-only — the event is
        // never rejected, because Go/Java token sets can legitimately differ
        // in dev synthetic mode. Both sides are hex digests (no secrets).
        if (!manifestFingerprint.equals(event.manifestFingerprint())) {
            LOG.warn("ingestion: bridge manifest_fingerprint mismatch (slot={}, epoch={}): got={} want={} — cross-check only, event not rejected",
                    event.slotId(), event.connectionEpoch(),
                    event.manifestFingerprint(), manifestFingerprint);
            metrics.incrementDecodeError("FINGERPRINT_MISMATCH");
        }
        if (!assignedTokenSetHash.equals(event.assignedTokenSetHash())) {
            LOG.warn("ingestion: bridge assigned_token_set_hash mismatch (slot={}, epoch={}): got={} want={} — cross-check only, event not rejected",
                    event.slotId(), event.connectionEpoch(),
                    event.assignedTokenSetHash(), assignedTokenSetHash);
            metrics.incrementDecodeError("TOKEN_HASH_MISMATCH");
        }

        // ---- Slot-scoped safety propagation (plan Amendment §Slot-scoped safety) ----
        emitSafetyTransition(event, active);

        // ---- Discontinuity evidence (plan §DiscontinuityWriter) ----
        // Reconnect evidence is written only when the slot reaches ACTIVE again.
        if ("reconnect".equals(event.event()) && active) {
            discontinuityWriter.writeBridgeEvent(event, lastTickSnapshot);
        } else if (isEvidenceEvent(event.event())) {
            discontinuityWriter.writeBridgeEvent(event, lastTickSnapshot);
        }

        if ("reconnect".equals(event.event())) {
            metrics.incrementSubscriptionRetry();
            if (event.reason() != null && event.reason().contains("authentication_refreshed")) {
                metrics.incrementAuthRefresh();
            }
        }
        if ("heartbeat_failed".equals(event.event())) metrics.incrementHeartbeatFailure();
        if ("feed_stalled".equals(event.event())) metrics.incrementFeedStall();
        if ("auth_failure".equals(event.event())) metrics.incrementAuthFailure();
        if ("subscription_ack".equals(event.event()) && event.rejectedTokens() > 0) {
            metrics.incrementPartialSubscription();
            // Partial acknowledgement is feed-health evidence.
            discontinuityWriter.writeBridgeEvent(event, lastTickSnapshot);
        }
        if ("bridge_shutdown".equals(event.event())) {
            LOG.info("ingestion: bridge requested shutdown (slot={}, epoch={})", event.slotId(), event.connectionEpoch());
            running = false;
        } else if ("auth_failure".equals(event.event())) {
            LOG.error("ingestion: bridge authentication failed (slot={}, epoch={}, reason={})",
                    event.slotId(), event.connectionEpoch(), sanitizeLog(event.reason()));
        } else if ("feed_stalled".equals(event.event()) || "heartbeat_failed".equals(event.event())) {
            health.setBrokerConnected(false);
            metrics.setBridgeConnected(false);
        }
        updateReadinessFile();
        LOG.info("bridge lifecycle event={} slot={} state={} epoch={} assigned={} acknowledged={} rejected={} reason={}",
                event.event(), event.slotId(), event.state(), event.connectionEpoch(),
                event.assignedTokens(), event.acknowledgedTokens(), event.rejectedTokens(),
                sanitizeLog(event.reason()));
    }

    /** Whether a lifecycle event should be recorded as discontinuity evidence. */
    private static boolean isEvidenceEvent(String eventName) {
        return switch (eventName) {
            case "disconnect", "bridge_exit", "auth_failure",
                 "heartbeat_failed", "feed_stalled" -> true;
            default -> false;
        };
    }

    /**
     * Slot-scoped safety transition classification (plan Amendment
     * §Slot-scoped safety propagation). Pure decision logic extracted from
     * {@link #emitSafetyTransition} for direct unit testing (ING-SAFE-001..003).
     *
     * @param event  validated bridge lifecycle event (never null)
     * @param active {@code ACTIVE} state with full acknowledgement and no
     *               rejected tokens (the caller's slot-readiness computation)
     * @return the unsafe reason code for this event, or {@code null} when the
     *         event carries no unsafe transition
     */
    static com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode unsafeReasonFor(
            BridgeEvent event, boolean active) {
        com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode unsafe = null;
        switch (event.event()) {
            case "feed_stalled" ->
                unsafe = "decode_error_burst".equals(event.reason())
                        ? com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.DECODE_ERROR_BURST
                        : com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.FEED_STALLED;
            case "heartbeat_failed" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.HEARTBEAT_FAILED;
            case "disconnect" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.READ_FAILURE;
            case "auth_failure" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.AUTH_FAILURE;
            case "bridge_shutdown", "bridge_exit" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.BRIDGE_EXIT;
            case "subscription_ack" -> {
                if (event.rejectedTokens() > 0 && !active) {
                    unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.SUBSCRIPTION_PARTIAL;
                } else if ("TERMINAL".equals(event.state())
                        && "subscription_response_timeout".equals(event.reason())) {
                    unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.SUBSCRIPTION_TIMEOUT;
                }
            }
            default -> { /* slot_state / reconnect carry no unsafe transition */ }
        }
        return unsafe;
    }

    /**
     * Whether a bridge event is a RECOVERED transition: the slot returns to
     * {@code ACTIVE} with full acknowledgement via {@code subscription_ack}
     * (plan ING-SAFE-003: recovery requires ACTIVE + full ack; the
     * post-recovery frame is confirmed separately by the reader loop).
     */
    static boolean isRecoveredTransition(BridgeEvent event, boolean active) {
        return active && "subscription_ack".equals(event.event());
    }

    /**
     * Emit slot-scoped safety requests (plan Amendment §Slot-scoped safety
     * propagation). One UNSAFE row exactly once per transition; a RECOVERED
     * row only when the slot returns to ACTIVE with full acknowledgement. Rows
     * are deduped by the computed halt_request_id (same tuple never re-emitted).
     */
    private void emitSafetyTransition(BridgeEvent event, boolean active) {
        if (safetyHaltWriter == null) return;
        String slotId = event.slotId();
        long epoch = event.connectionEpoch();

        com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode unsafe =
                unsafeReasonFor(event, active);

        if (unsafe != null) {
            String id = com.trading.ingestion.safety.SafetyHaltWriter.computeHaltRequestId(
                    manifestFingerprint, slotId, epoch, "UNSAFE", unsafe.name());
            if (firstEmission(safetyEmitted, "UNSAFE", id)) {
                safetyHaltWriter.write(slotId, epoch,
                        com.trading.ingestion.safety.SafetyHaltWriter.SafetyState.UNSAFE,
                        unsafe, assignedTokenSetHash, event.event(), System.currentTimeMillis());
                LOG.warn("safety: slot {} UNSAFE (reason={}, epoch={}, halt={})",
                        slotId, unsafe, epoch, id.substring(0, Math.min(8, id.length())));
            }
            markSlotUnsafe(slotId);
        } else if (isRecoveredTransition(event, active)) {
            // Full acknowledgement → RECOVERED (same or greater epoch, frame present).
            String id = com.trading.ingestion.safety.SafetyHaltWriter.computeHaltRequestId(
                    manifestFingerprint, slotId, epoch, "RECOVERED", "");
            if (firstEmission(safetyEmitted, "RECOVERED", id)) {
                safetyHaltWriter.write(slotId, epoch,
                        com.trading.ingestion.safety.SafetyHaltWriter.SafetyState.RECOVERED,
                        null, assignedTokenSetHash, event.event(), System.currentTimeMillis());
                LOG.info("safety: slot {} RECOVERED (epoch={}, halt={})",
                        slotId, epoch, id.substring(0, Math.min(8, id.length())));
            }
            // A RECOVERED transition is the only way a slot returns to SAFE —
            // bridge restarts (resetSlotsToAuthenticating) must not clear it.
            health.setSlotUnsafe(slotId, false);
            metrics.setSlotSafetyState(slotId, 0, 0);
        }
    }

    /**
     * Quality-class slot-unsafe evidence (plan §Market-data quality
     * classification): FUTURE_BROKER_TIMESTAMP and STALE_BROKER_TIMESTAMP
     * quarantine rows also emit one UNSAFE safety request per slot/epoch, so
     * Signal suppresses decisions for the slot while the broker's timestamps
     * are untrustworthy. Rows are deduped by halt_request_id (tuple =
     * fp|slot|epoch|state|reason); the plan's "once per
     * instrument/slot/epoch" collapses to once per slot/epoch because the
     * tuple carries no instrument.
     */
    private void emitQualityUnsafe(String slotId, long epoch,
                                   QuarantineWriter.Reason quarantineReason) {
        if (safetyHaltWriter == null || slotId == null || slotId.isBlank()) return;
        com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode code =
                qualityUnsafeReason(quarantineReason);
        if (code == null) return;
        long safeEpoch = epoch > 0 ? epoch : 1L;
        String id = com.trading.ingestion.safety.SafetyHaltWriter.computeHaltRequestId(
                manifestFingerprint, slotId, safeEpoch, "UNSAFE", code.name());
        if (firstEmission(safetyEmitted, "UNSAFE", id)) {
            safetyHaltWriter.write(slotId, safeEpoch,
                    com.trading.ingestion.safety.SafetyHaltWriter.SafetyState.UNSAFE,
                    code, assignedTokenSetHash, quarantineReason.name(),
                    System.currentTimeMillis());
            LOG.warn("safety: slot {} UNSAFE (reason={}, epoch={}, halt={})",
                    slotId, code, safeEpoch, id.substring(0, Math.min(8, id.length())));
        }
        markSlotUnsafe(slotId);
    }

    /**
     * Maps a quality-class quarantine reason to its safety reason code
     * (pure — unit-tested). Anything else maps to {@code null} so only the
     * plan's three quality classes emit slot-unsafe evidence.
     */
    static com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode qualityUnsafeReason(
            QuarantineWriter.Reason quarantineReason) {
        return switch (quarantineReason) {
            case FUTURE_BROKER_TIMESTAMP -> com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.FUTURE_BROKER_TIMESTAMP;
            case STALE_BROKER_TIMESTAMP -> com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.STALE_BROKER_TIMESTAMP;
            default -> null;
        };
    }

    /** Propagate slot-unsafe evidence to HealthProbe + the metrics emitter. */
    private void markSlotUnsafe(String slotId) {
        health.setSlotUnsafe(slotId, true);
        metrics.setSlotSafetyState(slotId, 1, health.slot(slotId).unsafeSinceNanos);
    }

    /**
     * R-298 safety-write dedup gate: {@code true} only for the FIRST emission
     * of this {@code (state, halt_request_id)} pair. Callers MUST gate the
     * actual {@code SafetyHaltWriter.write(...)} on this — the pre-R-298 code
     * wrote unconditionally and deduped only the log line, so every repeated
     * STALE tick still appended a KV upsert (table-89 growth + hot-path cost).
     * Backed by a {@link java.util.concurrent.ConcurrentHashMap} key set:
     * concurrency-safe, pure, unit-tested.
     */
    static boolean firstEmission(java.util.Set<String> emitted, String state, String haltRequestId) {
        return emitted.add(state + "|" + haltRequestId);
    }

    private void handleBridgeEvent(BridgeEvent event) {
        processBridgeEvent(event);
    }

    private static final java.util.regex.Pattern SECRET_LOG_PATTERN = java.util.regex.Pattern.compile(
            "(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|access_token|authorization|appID|token)([=:][^&\\s,}]+)");
    private static final java.util.regex.Pattern BEARER_LOG_PATTERN = java.util.regex.Pattern.compile(
            "(?i)\\bBearer[=:\\s]+[^\\s,}]+");

    private static String sanitizeLog(String value) {
        if (value == null) return "";
        // Two-pass: Bearer tokens first (space-separated), then name=value pairs.
        String sanitized = BEARER_LOG_PATTERN.matcher(value).replaceAll("Bearer=[REDACTED]");
        sanitized = SECRET_LOG_PATTERN.matcher(sanitized).replaceAll("$1=[REDACTED]");
        return sanitized.length() > 512 ? sanitized.substring(0, 512) : sanitized;
    }

    private void updateReadinessFile() {
        if (readinessFile == null) return;
        try {
            readinessFile.setReady(health.isReady());
        } catch (java.io.IOException e) {
            LOG.warn("ingestion: readiness marker update failed: {}", sanitizeLog(e.getMessage()));
        }
    }

    // ---- ING-FAIL-007: clock-jump monitoring ----

    /**
     * Start the periodic NTP clock re-measurement. Runs on its own daemon
     * scheduler so a slow NTP query can never delay the broker-staleness
     * watchdog or the read loop.
     */
    private void startClockMonitor() {
        clockMonitorScheduler.scheduleAtFixedRate(() -> {
            if (!running || clock == null) return;
            try {
                long offset = clock.measureOffsetMs();
                handleClockMeasurement(offset);
            } catch (NtpClockChecker.NtpException e) {
                LOG.warn("ingestion: clock monitor check failed: {}", sanitizeLog(e.getMessage()));
            }
        }, CLOCK_MONITOR_INITIAL_DELAY_MS, CLOCK_MONITOR_INTERVAL_MS,
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    // ---- SIGNAL-warn-jvm-heap-high: sustained container-memory readiness gate ----

    /**
     * Start the sustained JVM/container memory monitor. Every interval it reads
     * the cgroup memory limit + current usage, feeds the {@link JvmHeapReadinessGate}
     * state machine, and:
     *
     * <ul>
     *   <li>reflects the gate into the readiness probe
     *       ({@code health.setMemoryBlocked(...)}) so a sustained breach blocks
     *       readiness (never on a transient spike);</li>
     *   <li>emits exactly one {@code SIGNAL-warn-jvm-heap-high} WARN per breach
     *       episode and one {@code SIGNAL-info-jvm-heap-recovered} INFO on the
     *       sustained recovery (hysteresis at 75%).</li>
     * </ul>
     *
     * <p>No-op outside a bounded cgroup (bare JVM / dev / test): without a
     * container limit there is no budget to gate, matching
     * {@link ContainerMemoryGuard}'s contract. The alert is best-effort and off
     * the critical path (a dead collector can never fail the data path).
     */
    private void startMemoryMonitor() {
        memoryMonitorScheduler.scheduleAtFixedRate(() -> {
            if (!running) return;
            try {
                long limit = ContainerMemoryGuard.readContainerMemoryLimitBytes();
                if (limit <= 0) return;         // no bounded container budget — nothing to gate
                long used = ContainerMemoryGuard.readContainerMemoryUsedBytes();
                if (used < 0) return;
                JvmHeapReadinessGate.Signal signal =
                        heapGate.observe(limit, used, System.currentTimeMillis());
                health.setMemoryBlocked(heapGate.isBlocked());
                refreshResourceMetrics();
                if (signal == JvmHeapReadinessGate.Signal.WARN_HEAP_HIGH) {
                    LOG.error(
                        "ingestion: SIGNAL-warn-jvm-heap-high — container memory sustained at/above "
                        + "the 85% alert threshold (limit={}B used={}B pct={}%); blocking readiness so "
                        + "the mis-sized JVM stops being a live-data source instead of OOM-ing.",
                        limit, used, ContainerMemoryGuard.utilizedPercent(limit, used));
                    OtlpAlertLogs.emit(otelHost, "ingestion", "WARN",
                        "SIGNAL-warn-jvm-heap-high",
                        "container_memory_pct=" + ContainerMemoryGuard.utilizedPercent(limit, used));
                } else if (signal == JvmHeapReadinessGate.Signal.INFO_HEAP_RECOVERED) {
                    LOG.info(
                        "ingestion: SIGNAL-info-jvm-heap-recovered — container memory back below the "
                        + "75% hysteresis setpoint; clearing the readiness block (limit={}B used={}B).",
                        limit, used);
                    OtlpAlertLogs.emit(otelHost, "ingestion", "INFO",
                        "SIGNAL-info-jvm-heap-recovered",
                        "container_memory_pct=" + ContainerMemoryGuard.utilizedPercent(limit, used));
                }
            } catch (Exception e) {
                // A memory-probe failure must never take the data path down.
                LOG.warn("ingestion: memory monitor tick failed: {}", sanitizeLog(e.getMessage()));
            }
        }, MEMORY_MONITOR_INITIAL_DELAY_MS, MEMORY_MONITOR_INTERVAL_MS,
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Process one fresh clock-offset measurement. Emits exactly one
     * {@code TIME_JUMP} discontinuity row per violation episode (ING-FAIL-007)
     * and refreshes the readiness marker so the clock dimension tracks the
     * latest measurement, not just the startup check.
     */
    void handleClockMeasurement(long offsetMs) {
        if (timeJumpMonitor.onOffsetMeasured(offsetMs)) {
            LOG.error("ingestion: TIME_JUMP — clock offset {}ms exceeds limit {}ms; "
                            + "writing discontinuity evidence",
                    offsetMs, config.clockOffsetLimitMs);
            discontinuityWriter.write(
                    DiscontinuityWriter.Reason.TIME_JUMP,
                    "clock offset " + offsetMs + "ms exceeds limit "
                            + config.clockOffsetLimitMs + "ms",
                    lastTickSnapshot
            );
        }
        updateReadinessFile();
    }

    // ---- shutdown ----

    private void shutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            LOG.debug("ingestion: duplicate shutdown ignored");
            return;
        }
        // Capture whether the bridge loop is live BEFORE flipping running below
        // — only that case needs the main-thread join (a FATAL-startup System.exit
        // also runs this hook, and main is blocked in System.exit there, so the
        // join must be skipped or startup failures would stall the full bound).
        boolean bridgeLoopLive = running;
        // Signal the bridge (SIGTERM) so its ctx.Done handler runs the final
        // ARROW_TICK_COUNTS report before the pipe closes. Without this the
        // bridge dies of SIGPIPE (exit 141) on JVM halt and the shutdown report
        // — the authoritative per-token count for ING-TCP-001 — is lost.
        //
        // Ordering is critical: `running` must stay true until the bridge has
        // exited. The main read loop exits (closing the stdout pipe) as soon
        // as `running` flips false, and a still-writing bridge would then get
        // SIGPIPE before its final report. Signal first, wait for the bridge
        // to exit cleanly (it emits the report, then EOFs its stdout), THEN
        // flip running so the loop unwinds normally.
        Process bp = currentBridgeProcess;
        if (bp != null && bp.isAlive()) {
            LOG.info("ingestion: signaling arrow-bridge (SIGTERM) for final tick-count report");
            // Signal via kill -TERM, NOT Process.destroy(): destroy() closes
            // the parent-side pipes immediately (IOException "Stream closed")
            // so the bridge's final report and bridge_shutdown event are lost
            // from the logs. kill leaves the pipes open for the bridge's clean
            // exit (see signalBridge).
            signalBridge(bp);
            try {
                if (!bp.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    bp.destroyForcibly();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        // Let the stderr drain thread flush the bridge's final report into the
        // log before the JVM halts (the drain thread is daemon and would
        // otherwise be killed mid-read).
        Thread st = currentBridgeStderrThread;
        if (st != null) {
            try {
                st.join(5_000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        running = false;
        // Stop the staleness watchdog (R-108) and the clock monitor (ING-FAIL-007).
        stalenessWatchdog.shutdownNow();
        clockMonitorScheduler.shutdownNow();
        if (readinessFile != null) {
            try { readinessFile.clear(); }
            catch (java.io.IOException e) { LOG.warn("ingestion: readiness marker clear failed: {}", sanitizeLog(e.getMessage())); }
        }
        health.markNotAlive();

        // J3, I10: Persist uncertainty counters before drain. R-260: the
        // entry pins the EXACT bytes/records still pending at shutdown — the
        // drain (writer.close below) may time out on an un-acking Fluss, so
        // the journal must record what the drain was unable to flush.
        journal.write(new UncertaintyJournal.Entry(
                instanceId,
                Instant.now(),
                tracker.totalAccepted(),
                tracker.totalAppended(),
                tracker.totalFailed(),
                tracker.totalRejected(),
                tracker.totalBytesAccepted(),
                tracker.pendingRecords(),
                tracker.pendingBytes(),
                "shutdown"
        ));

        // Close metrics emitter (triggers final flush)
        metrics.close();

        // Close quarantine + discontinuity writers
        if (quarantineWriter != null) quarantineWriter.close();
        if (discontinuityWriter != null) discontinuityWriter.close();
        if (safetyHaltWriter != null) safetyHaltWriter.close();

        // J2: Drain pending writes with deadline — T6-3: close the queue
        // first (stops new offers), the worker drains everything queued,
        // then the writer closes (pending acks + drain deadline).
        if (writerWorker != null) writerWorker.close();
        if (writer != null) writer.close();
        // Join the main thread (bounded) when this is the shutdown hook and the
        // bridge loop was live: the loop logs the final "bridge loop ended
        // (ticks=…)" report after `running` flips false, and the JVM halts as
        // soon as this hook returns without waiting for main. (Skipped when the
        // bridge never launched — FATAL startup — or on a normal in-thread
        // shutdown, where main IS this thread or already finished.)
        if (bridgeLoopLive && mainThread != null
                && Thread.currentThread() != mainThread && mainThread.isAlive()) {
            try {
                mainThread.join(SHUTDOWN_MAIN_JOIN_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        // Final orphan sweep (CHG-016): a bridge the main loop restarted
        // between the signal step above (whose `bp` may already be dead or
        // null) and `running=false` was never signaled — re-read the current
        // process and take it down (graceful, then forced) so nothing survives
        // the JVM halt. Without this, a late-restart bridge would keep running
        // after the JVM exits, emitting into a dead pipe forever.
        Process latest = currentBridgeProcess;
        if (latest != null && latest != bp && latest.isAlive()) {
            LOG.warn("ingestion: reaping late bridge (pid={}) spawned during shutdown", latest.pid());
            signalBridge(latest);
            try {
                if (!latest.waitFor(3, java.util.concurrent.TimeUnit.SECONDS)) {
                    latest.destroyForcibly();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                latest.destroyForcibly();
            }
        }
        LOG.info("ingestion: drained (totalTicks={}, errors={})",
                frameCount.get(), errorCount.get());
    }

    // ---- health accessor ----

    public HealthProbe health() { return health; }
    public AppendTracker tracker() { return tracker; }

    /** Freshness classification for a broker timestamp vs. receive time. */
    enum FreshnessDecision { FRESH, STALE, FUTURE }

    /**
     * Pure freshness gate: a tick whose broker timestamp is older than
     * maxEventAgeMs (STALE) or ahead of receive time by more than
     * maxFutureSkewMs (FUTURE) is quarantined before any trade classification,
     * so stale data can never reach a trade decision. (Plan §Production
     * hardening 1376.)
     */
    static FreshnessDecision classifyFreshness(long tsMs, long receiveTsMs,
            long maxFutureSkewMs, long maxEventAgeMs) {
        if (receiveTsMs - tsMs > maxEventAgeMs) return FreshnessDecision.STALE;
        if (tsMs - receiveTsMs > maxFutureSkewMs) return FreshnessDecision.FUTURE;
        return FreshnessDecision.FRESH;
    }

    // ---- GoTick JSON model (matches arrow-bridge output) ----

    /**
     * Mirrors the NDJSON schema emitted by the Go arrow-bridge.
     * All prices in paise, all timestamps in epoch ms.
     */
    @SuppressWarnings("unused")
    private static final class GoTick {
        @com.fasterxml.jackson.annotation.JsonProperty("record_type")
        public String record_type;
        @com.fasterxml.jackson.annotation.JsonProperty("connection_id")
        public String connection_id;
        @com.fasterxml.jackson.annotation.JsonProperty("connection_epoch")
        public long connection_epoch;
        @com.fasterxml.jackson.annotation.JsonProperty("slot_id")
        public String slot_id;
        @com.fasterxml.jackson.annotation.JsonProperty("received_ts_ms")
        public long received_ts_ms;
        @com.fasterxml.jackson.annotation.JsonProperty("feed_sequence_local")
        public long feed_sequence_local;
        @com.fasterxml.jackson.annotation.JsonProperty("raw_payload")
        public String raw_payload;
        @com.fasterxml.jackson.annotation.JsonProperty("payload_hash")
        public String payload_hash;
        public String feed;
        public String mode;
        public long token;
        @com.fasterxml.jackson.annotation.JsonProperty("ltp_paise")
        public long ltp_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("close_paise")
        public long close_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("open_paise")
        public long open_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("high_paise")
        public long high_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("low_paise")
        public long low_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("vwap_paise")
        public long vwap_paise;
        public long ltq;
        public long volume;
        @com.fasterxml.jackson.annotation.JsonProperty("total_buy_qty")
        public long total_buy_qty;
        @com.fasterxml.jackson.annotation.JsonProperty("total_sell_qty")
        public long total_sell_qty;
        public long atv;
        public long btv;
        @com.fasterxml.jackson.annotation.JsonProperty("open_interest")
        public long open_interest;
        @com.fasterxml.jackson.annotation.JsonProperty("ts_ms")
        public long ts_ms;
        @com.fasterxml.jackson.annotation.JsonProperty("bid_px")
        public long[] bid_px;
        @com.fasterxml.jackson.annotation.JsonProperty("ask_px")
        public long[] ask_px;
        @com.fasterxml.jackson.annotation.JsonProperty("bid_qty")
        public long[] bid_qty;
        @com.fasterxml.jackson.annotation.JsonProperty("ask_qty")
        public long[] ask_qty;

    }

    // ---- helpers ----

    /** Raw NDJSON line bytes for quarantine evidence (null-safe). */
    static byte[] rawLineBytes(String jsonLine) {
        return jsonLine != null ? jsonLine.getBytes(StandardCharsets.UTF_8) : null;
    }

    /** Decision for a malformed NDJSON line (ING-DQ-001). */
    record MalformedJsonDecision(QuarantineWriter.Reason reason, byte[] rawPayload, String detail) {}

    /**
     * Classify a non-JSON line for quarantine (ING-DQ-001). The raw line bytes
     * are preserved verbatim as quarantine evidence; the detail is the static
     * {@link #MALFORMED_JSON_DETAIL} constant — never the Jackson message, which
     * embeds the offending input snippet.
     */
    static MalformedJsonDecision malformedJsonDecision(byte[] rawLine) {
        return new MalformedJsonDecision(
                QuarantineWriter.Reason.MALFORMED_JSON, rawLine, MALFORMED_JSON_DETAIL);
    }

    /** The static detail used for MALFORMED_JSON quarantine rows (ING-DQ-001). */
    static String malformedJsonDetail() {
        return MALFORMED_JSON_DETAIL;
    }

    /**
     * Refresh resource gauges (plan Amendment §Resource): FDs, RSS, JVM threads.
     * Go goroutines are reported by the bridge; child-process-alive is set by
     * lifecycle events. Bounded cost — called once per processed frame.
     */
    private void refreshResourceMetrics() {
        // R-140: /proc + ThreadMXBean reads are expensive at HFT tick rates —
        // throttle to at most one refresh every 5 seconds. The gauges are
        // slow-moving resources; a 5s staleness is irrelevant.
        long nowNanos = System.nanoTime();
        if (lastResourceRefreshNanos != 0
                && nowNanos - lastResourceRefreshNanos < 5_000_000_000L) {
            return;
        }
        lastResourceRefreshNanos = nowNanos;

        long open = countOpenFds();
        long limit = readFdLimit();
        metrics.setProcessOpenFds(open);
        metrics.setProcessFdLimit(limit);
        double usage = limit > 0 ? 100.0 * open / limit : 0.0;
        metrics.setProcessFdUsagePercent(usage);
        maybeEmitResourceExhausted(usage);
        metrics.setProcessRssBytes(readRssBytes());
        try {
            metrics.setJvmThreadsLive(
                    java.lang.management.ManagementFactory.getThreadMXBean().getThreadCount());
        } catch (Exception ignore) { /* skip */ }
    }

    /**
     * Critical resource condition (plan Amendment §Resource): process FD
     * usage at or above 90% of the soft limit triggers the RESOURCE_EXHAUSTED
     * safety path. Pure and defensive — /proc unavailable or negative counts
     * produce a false result so a broken platform never fabricates exhaustion.
     */
    static boolean isCriticalResourceCondition(double fdUsagePercent) {
        return fdUsagePercent >= 90.0;
    }

    /**
     * RESOURCE_EXHAUSTED emission: while the critical FD condition holds,
     * every tracked slot gets one UNSAFE request per slot/epoch (deduped by
     * halt_request_id — the 5s refresh throttle bounds re-checks). The
     * orphan-child and unsafe-duration critical conditions from the plan text
     * are covered by the child-process lifecycle gauges and the safety-state
     * tracking instead of separate halt requests.
     */
    private void maybeEmitResourceExhausted(double fdUsagePercent) {
        if (!isCriticalResourceCondition(fdUsagePercent) || safetyHaltWriter == null) return;
        for (String slotId : health.slotIds()) {
            HealthProbe.SlotHealth slot = health.slot(slotId);
            long epoch = slot.epoch > 0 ? slot.epoch : connectionEpoch.get();
            if (epoch <= 0) continue;
            String id = com.trading.ingestion.safety.SafetyHaltWriter.computeHaltRequestId(
                    manifestFingerprint, slotId, epoch, "UNSAFE", "RESOURCE_EXHAUSTED");
            if (firstEmission(safetyEmitted, "UNSAFE", id)) {
                safetyHaltWriter.write(slotId, epoch,
                        com.trading.ingestion.safety.SafetyHaltWriter.SafetyState.UNSAFE,
                        com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.RESOURCE_EXHAUSTED,
                        assignedTokenSetHash, "fd_usage_exhausted", System.currentTimeMillis());
                LOG.error("safety: slot {} UNSAFE (reason=RESOURCE_EXHAUSTED, fd_usage={}%, epoch={}, halt={})",
                        slotId, String.format("%.1f", fdUsagePercent), epoch,
                        id.substring(0, Math.min(8, id.length())));
            }
            markSlotUnsafe(slotId);
        }
    }

    /** Count open FDs via /proc/self/fd (Linux); -1 on non-Linux or failure. */
    private static long countOpenFds() {
        try (var stream = java.nio.file.Files.list(java.nio.file.Paths.get("/proc/self/fd"))) {
            return stream.count();
        } catch (Exception ignore) { /* non-Linux or unreadable */ }
        return -1L;
    }

    /**
     * Read the process's per-process RLIMIT_NOFILE soft limit from
     * {@code /proc/self/limits} (R-192) — NOT the system-wide
     * {@code /proc/sys/fs/file-max}, which made fd_usage_percent look near 0%
     * even at the real cap. Returns -1 on non-Linux or failure.
     */
    private static long readFdLimit() {
        try {
            java.nio.file.Path p = java.nio.file.Paths.get("/proc/self/limits");
            if (!java.nio.file.Files.isReadable(p)) return -1L;
            for (String line : java.nio.file.Files.readAllLines(p)) {
                if (line.startsWith("Max open files")) {
                    // Format: "Max open files  1024  1048576  files"
                    //            [0]    [1]   [2]   [3]     [4]
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 4 && !"unlimited".equals(parts[3])) {
                        return Long.parseLong(parts[3]); // soft limit
                    }
                }
            }
        } catch (Exception ignore) { /* non-Linux or unreadable */ }
        return -1L;
    }

    /** Read VmRSS from /proc/self/status (Linux); 0 on non-Linux or failure. */
    private static long readRssBytes() {
        try {
            java.nio.file.Path status = java.nio.file.Paths.get("/proc/self/status");
            if (!java.nio.file.Files.isReadable(status)) return 0L;
            for (String line : java.nio.file.Files.readAllLines(status)) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 2) {
                        return Long.parseLong(parts[1]) * 1024L; // kB → bytes
                    }
                }
            }
        } catch (Exception ignore) { /* non-Linux or unreadable */ }
        return 0L;
    }

    /**
     * Decode the Base64 {@code raw_payload} from a tick and verify its SHA-256
     * digest equals the bridge-provided {@code payload_hash}.
     *
     * @return the exact decompressed broker packet bytes, or {@code null} if the
     *         record must be quarantined (bad Base64, empty payload, or hash
     *         mismatch). Quarantining is performed here; the caller returns.
     */
    /** Base64-decode raw_payload without hash validation (A2 off path). */
    private byte[] decodePayloadOnly(GoTick gt, byte[] rawLine) {
        if (gt.raw_payload == null || gt.raw_payload.isBlank()) {
            quarantineWriter.write(rawLine, QuarantineWriter.Reason.HASH_MISMATCH,
                    "raw_payload missing or empty", gt.token, null, null);
            return null;
        }
        byte[] packet;
        try {
            packet = Base64.getDecoder().decode(gt.raw_payload);
        } catch (IllegalArgumentException e) {
            quarantineWriter.write(rawLine, QuarantineWriter.Reason.HASH_MISMATCH,
                    "raw_payload not valid Base64", gt.token, null, null);
            return null;
        }
        if (packet.length == 0) {
            quarantineWriter.write(rawLine, QuarantineWriter.Reason.HASH_MISMATCH,
                    "raw_payload decodes to empty", gt.token, null, null);
            return null;
        }
        return packet;
    }

    private byte[] decodeAndValidatePayload(GoTick gt, byte[] rawLine) {
        // R-248: result is returned directly (no caller-owned out-array).
        PayloadHashValidator.Result result = PayloadHashValidator.validate(gt.raw_payload, gt.payload_hash);
        if (result == PayloadHashValidator.Result.VALID) {
            return PayloadHashValidator.decodeValid(gt.raw_payload, gt.payload_hash);
        }
        String detail = switch (result) {
            case MALFORMED_PAYLOAD -> "raw_payload missing, not valid Base64, or empty";
            case MALFORMED_HASH -> "payload_hash missing or not a SHA-256 hex digest";
            case HASH_MISMATCH -> "payload hash mismatch: broker packet bytes do not match payload_hash";
            case VALID -> "unreachable";
        };
        quarantineWriter.write(rawLine, QuarantineWriter.Reason.HASH_MISMATCH,
                detail, gt.token, null, null);
        metrics.incrementDecodeError("HASH_MISMATCH");
        return null;
    }
}
