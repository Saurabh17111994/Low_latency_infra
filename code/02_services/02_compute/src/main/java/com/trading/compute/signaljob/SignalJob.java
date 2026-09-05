package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import com.trading.common.schema.CandleTableSchema;
import org.apache.flink.util.OutputTag;
import com.trading.compute.telemetry.ComputeAlertLogs;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Signal job — compute path (docs/08_implementation/04-signal-job.md).
 *
 * <p>Topology: {@code raw_table_1} (Fluss LOG source, full offsets) → raw
 * schema/validity gate → state-authoritative fingerprint dedup → 15-second event-time
 * tumbling window (OHLCV aggregate) → {@code feature_candles_15s} (Fluss KV
 * upsert sink — user requirement 2026-08-13: candle tables are KV-only, no
 * LOG+KV twin) → MVP signal detection (Slice 2.1, DEC-034) → signal dual-sink
 * (DEC-035): {@code Signal_Candidates} (Fluss LOG append, every signal) and
 * {@code Signal_Candidates_current} (Fluss KV upsert behind the
 * canonical-signal filter). Business Logic operator internals (candidate
 * lifecycle, max-one-active) and Ranking/decision sinks stay disabled at
 * those boundaries — postponed with the ranking phase.
 *
 * <p>Checkpointing: EXACTLY_ONCE, pinned interval/timeout/max-concurrent
 * (REQ-FC-006); fixed-delay restart 3 × 30s. All output sinks are plain
 * {@code FlussSink}s — the NATIVE stall-guard is the checkpoint timeout
 * (30 s) + fixed-delay restart failing the job, never hanging it (CHG-023
 * item 4 removed the StallGuardedSink watchdog; the Fluss client's own
 * {@code client.request-timeout} bounds each write). The
 * signal LOG sink uses {@code RowDataSerializationSchema(true, true)}
 * (append-only, ignore delete — correct for a LOG table); the candle KV and
 * signal current-state KV sinks use
 * {@code RowDataSerializationSchema(false, false)} (INSERT → UPSERT for the
 * KV writer). The source consumes from the earliest available offset on
 * first start.
 */
public final class SignalJob {

    private static final Logger LOG = LoggerFactory.getLogger(SignalJob.class);

    private SignalJob() {}

    public static void main(String[] args) throws Exception {
        SignalJobConfig config = SignalJobConfig.fromEnv();
        LOG.info("signal-job: starting compute path with {}", config);
        run(config);
    }

    public static void run(SignalJobConfig config) throws Exception {
        // Startup-mode gate (CANDLE-KV-REPLAY-001 A3.3/A3.4): the config was
        // already validated by fromEnv(); log the mode for the operator and
        // ship it as a gauge so the run's startup is observable post hoc.
        // FULL_REPLAY is a break-glass mode — surface it at WARN, not INFO.
        if (config.startupMode() == SignalJobConfig.StartupMode.FULL_REPLAY) {
            LOG.warn("signal-job: startup mode = {} (restore={}, fullReplay={}) "
                    + "— offset-0 full replay accepted via ALLOW_FULL_REPLAY (A3.4)",
                    config.startupMode(), config.stateRecoveryPath() != null, config.allowFullReplay());
        } else {
            LOG.info("signal-job: startup mode = {} (restore={}, fullReplay={})",
                    config.startupMode(), config.stateRecoveryPath() != null, config.allowFullReplay());
        }
        // Tracker 14 P8.0 box 828: ship the startup-mode event to the
        // trading_alerts stream. A synchronous emit is required because the
        // native OTel metric reporter (CHG-023 item 1) only runs INSIDE the
        // cluster and never sees these client-side lifecycle events under
        // `flink run -d`. Best-effort: collector outage never fails the job.
        ComputeAlertLogs.emitAlertLog(config.otelCollectorHost(),
                config.startupMode() == SignalJobConfig.StartupMode.FULL_REPLAY ? "WARN" : "INFO",
                "startup-mode",
                "mode=" + config.startupMode() + " restore=" + (config.stateRecoveryPath() != null)
                        + " fullReplay=" + config.allowFullReplay());

        // Tracker 14 P8.0/831 — resource attributes (environment, host,
        // deployment version, job name, execution mode) ride the alert-log
        // payload so OpenObserve queries can slice by env/host/version. Only
        // known-safe config fields + hostname; never credentials.
        ComputeAlertLogs.configureResourceAttributes(
                "deployment.environment", config.deploymentEnv(),
                "host.name", hostName(),
                "deployment.version", config.configurationVersion(),
                "job.name", "signal-job",
                "flink.execution.mode", "embedded");

        // The METRIC half of the retired ComputeOtlpEmitter is gone: the
        // schema-version rejection counter, dedup gauges, source metrics, and
        // late-drop counter are all Flink MetricGroup metrics exported by the
        // native flink-metrics-otel reporter wired in applyRuntimeOptions
        // (CHG-023 item 1, 2026-08-17). Nothing client-side to start here.
        StreamExecutionEnvironment env = buildTopology(config);
        env.execute("signal-job-compute");
    }

    /**
     * Builds the full compute topology (source → validation → dedup → window →
     * candle sinks → signal detection → candidates sink) <b>without
     * executing</b>.
     *
     * <p>Shared by {@link #run(SignalJobConfig)} and the offline
     * JobGraph/operator-ID comparison tool ({@code JobGraphDump}, P6 of
     * CANDLE-KV-REPLAY-001): the restore-compatibility evidence compares this
     * graph before and after the tracker change, so both dumps must be built
     * by the same code path the running job uses.
     */
    /**
     * Raw-source offset selection (2026-08-29): offset-0 full replay only when
     * explicitly requested (ALLOW_FULL_REPLAY=true, A3.4); otherwise LATEST so
     * a restored/clean job skips the accumulated LOG backlog instead of
     * replaying it at max speed and contaminating steady-state measurements.
     * Package-private for direct unit testing (RawSourceOffsetSelectionTest).
     */
    static OffsetsInitializer rawSourceOffsets(SignalJobConfig.StartupMode mode) {
        return mode == SignalJobConfig.StartupMode.FULL_REPLAY
                ? OffsetsInitializer.full()
                : OffsetsInitializer.latest();
    }

    public static StreamExecutionEnvironment buildTopology(SignalJobConfig config) {
        // Read-only metadata preflight (tracker 14 P1 / re-scoped P2): prove
        // the deployed tables (candle KV, signal LOG, signal current-state KV)
        // match the contracts the write paths rely on before any graph is
        // built — fail closed on contract drift, never write degraded.
        try {
            preflightTableContracts(config);
        } catch (TableContractValidator.ContractViolation e) {
            // Tracker 14 P8.0 box 828: record the fail-closed startup event on
            // the trading_alerts stream before rethrowing — the job exits
            // right after, so this is the only chance to ship it.
            ComputeAlertLogs.emitAlertLog(config.otelCollectorHost(), "ERROR",
                    "schema-preflight-failed",
                    String.valueOf(e.getMessage()));
            throw e;
        }

        // Flink 2.x configures restart strategies declaratively via Configuration —
        // the programmatic RestartStrategies API was removed (verified against
        // flink-core 2.2.1). Create the environment from that configuration.
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.FIXED_DELAY.getMainValue());
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                config.restartMaxAttempts());
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(config.restartDelayMs()));
        // Checkpoint storage is a deployment property (flink-conf.yaml state.checkpoints.dir
        // in the dist). Flink 2.2.1 removed CheckpointConfig.setCheckpointStorage; the
        // declarative Configuration route is the only way to point local/dev runs at a
        // durable directory instead of the 5 MiB-capped JobManager-heap default.
        if (config.checkpointDir() != null) {
            flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointDir());
        }
        // State restore (STATE_RECOVERY_PATH): without an explicit restore the
        // job starts from offset 0 and replays the whole LOG backlog, which blows
        // the pinned checkpoint contract (REQ-FC-006: 10 s interval / 30 s timeout /
        // 1 concurrent) and cascades to JobManager death. A restart MUST resume
        // from the last checkpoint of the previous run. Restore-from-checkpoint
        // works with the same path key as savepoints (StreamGraphGenerator reads
        // StateRecoveryOptions.SAVEPOINT_PATH -> SavepointRestoreSettings).
        if (config.stateRecoveryPath() != null) {
            flinkConfig.set(StateRecoveryOptions.SAVEPOINT_PATH, config.stateRecoveryPath());
        }
        // Production runtime options (tracker 14 P4.1/P4.2): state backend,
        // incremental checkpoints, RocksDB local dirs / managed memory, savepoint
        // directory, parallelism. Backend + storage are Configuration-driven in
        // Flink 2.2.1; applied before the environment is created.
        applyRuntimeOptions(config, flinkConfig);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.setParallelism(config.parallelism());

        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(config.maxConcurrentCheckpoints());
        // A deliberate cancel/restart must retain the completed checkpoint named by
        // STATE_RECOVERY_PATH; deleting it would silently force an unsafe offset-0 replay.
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        FlussSource<RowData> source = FlussSource.<RowData>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setDatabase(config.database())
                .setTable(config.rawTable())
                .setStartingOffsets(rawSourceOffsets(config.startupMode()))
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();

        WatermarkStrategy<RowData> watermarks = CandleWatermarkStrategy.of(config);
        DataStream<RowData> ticks = env.fromSource(source, watermarks, "raw-table-1")
                .uid("raw-table-1");

        DataStream<RowData> valid = ticks
                .flatMap(new RawValidationFunction(config))
                .returns(ticks.getType())
                .name("raw-validation")
                .uid("raw-validation");

        // State-authoritative dedup (2026-08-16, DEC-038 superseded): the
        // complete 5-minute dedup set lives in this operator's keyed state,
        // checkpointed atomically with the source offset — no Fluss store, no
        // write path. keyBy(instrument_token) keeps every record of a token on
        // one subtask (key-group hashing, rescale-safe).
        SingleOutputStreamOperator<RowData> deduped = valid
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                .process(new FingerprintDedupFunction(config))
                .returns(ticks.getType())
                .name("fingerprint-dedup")
                // G-DEDUP-3 (2026-09-03 redesign): new uid so Flink never
                // silently maps pre-redesign checkpointed MapState onto the
                // heap-window operator (which requests no managed state).
                // Restoring an old checkpoint fails closed via the
                // no-allowNonRestoredState rule — clean start required.
                .uid("fingerprint-dedup-v2");

        // Step-2 latency observability (2026-09-03): per-tick exact age.
        // Non-keyed identity map on the deduped stream — sees every accepted
        // tick exactly once, no redistribution, records ingest_ts -> now into
        // compute.latency.ingest_to_monitor (histogram, all ticks). No state,
        // no output change. Restoring a pre-monitor checkpoint is safe (the
        // operator holds no state and the uid is new only to avoid
        // false-mapping a vanished operator — actually a NEW operator slot is
        // created on restore with zero state, which is fine).
        DataStream<RowData> monitored = deduped
                .map(new IngestLatencyMonitorFunction())
                .returns(ticks.getType())
                .name("ingest-latency-monitor")
                .uid("ingest-latency-monitor");

        // ── Multi-timeframe aggregator branch (Phase 4, 2026-09-05) ──────────
        // Behind MULTITF_ENABLED (default false) the SAME deduped stream
        // (via monitored, which is an identity map) branches to the
        // multi-TF operator so the old 15s path is untouched and both
        // consume the same ticks. Heap-state operator (intentional amnesia)
        // — checkpoint-restore rebuilds from live ticks, no RocksDB touch.
        SingleOutputStreamOperator<RowData> multiTfClosed = null;
        DataStream<RowData> multiTfLive = null;
        if (config.multiTfEnabled()) {
            SingleOutputStreamOperator<RowData> aggregator = monitored
                    .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                    .process(new MultiTimeframeAggregateFunction(config.liveSnapshotIntervalMs(),
                            config.multiTfSessionBypass(),
                            config.multiTfSignalContextEnabled()))
                    .returns(CandleClosedColumns.ROW_TYPE_INFO)
                    .name("multi-tf-aggregator")
                    .uid("multi-tf-aggregator-v1");
            multiTfClosed = aggregator;
            multiTfLive = aggregator.getSideOutput(MultiTimeframeAggregateFunction.LIVE_TAG);

            // Closed rows: first-write-wins filter + KV sink
            MultiTimeframeSinks.sinkClosed(multiTfClosed, config, config.candleClosedTable());
            // Live rows: KV upsert sink (1s overwrite)
            MultiTimeframeSinks.sinkLive(multiTfLive, config, config.candleLiveTable());

            // N7 retired (2026-09-05 cutover, batch 2): the range-breakout
            // rule runs ONLY as a host strategy (N7RangeBreakoutStrategy,
            // STRATEGIES id n7-range-breakout-v1) with the same deterministic
            // candidate ids, so the KV current-state converges across the
            // cutover. The n7-signal-v1 operator, its managed n7-emitted-ids
            // state, and the multitf-* sinks are gone: restoring a checkpoint
            // carrying them fails closed instead of double-emitting. Never
            // re-add an emitter beside the host.

            // Strategy host (plug-and-play strategies, 2026-09-05): one fixed
            // operator runs every STRATEGIES-listed SignalStrategy for every
            // instrument. Gated by STRATEGY_HOST_ENABLED (default false) so
            // the topology stays byte-identical until switched on — adding a
            // strategy later is one file + one config id, never a wiring
            // change. Reads the multi-TF live/closed streams (read-only
            // fan-out); emits to the same LOG + KV dual-sink shape the
            // retired n7-signal branch used. The stub smoke id is LOG-only
            // by filter design.
            if (config.strategyHostEnabled()) {
                DataStream<RowData> strategySignals = multiTfLive
                        .connect(multiTfClosed)
                        .keyBy(
                                live -> live.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                                closed -> closed.getLong(CandleClosedColumns.INSTRUMENT_TOKEN))
                        .process(new StrategyHostFunction(config, config.strategyIds()))
                        .returns(SignalCandidatesTableColumns.ROW_TYPE_INFO)
                        .name("strategy-host")
                        .uid("strategy-host-v1");

                strategySignals
                        .sinkTo(FlussSink.<RowData>builder()
                                        .setBootstrapServers(config.bootstrapServers())
                                        .setDatabase(config.database())
                                        .setTable(config.signalCandidatesTable())
                                        .setSerializationSchema(new RowDataSerializationSchema(true, true))
                                        .setOption("client.request-timeout",
                                                config.sinkWriteStallTimeoutMs() + "ms")
                                        .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                                        .build())
                        .name("strategy-host-candidates-sink")
                        .uid("strategy-host-candidates-sink");

                strategySignals
                        .filter(new CanonicalSignalFilterFunction(
                                Set.copyOf(config.strategyIds())))
                        .name("canonical-signal-filter-strategy-host")
                        .uid("canonical-signal-filter-strategy-host")
                        .sinkTo(FlussSink.<RowData>builder()
                                        .setBootstrapServers(config.bootstrapServers())
                                        .setDatabase(config.database())
                                        .setTable(config.signalCurrentTable())
                                        .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                        .setOption("client.request-timeout",
                                                config.sinkWriteStallTimeoutMs() + "ms")
                                        .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                                        .build())
                        .name("strategy-host-candidates-current-sink")
                        .uid("strategy-host-candidates-current-sink");
            } else if (!config.strategyIds().isEmpty()) {
                LOG.warn("signal-job: STRATEGIES is set ({}) but STRATEGY_HOST_ENABLED=false — "
                        + "no strategy-host branch wired; listed strategies will NOT run",
                        config.strategyIds());
            }
        }

        // Chain-heap redesign (2026-09-03 plan, OP4): the tumbling window +
        // aggregate + emit operator did 2-3 RocksDB touches per tick (46.5%
        // busy at 4.9 k/s). HeapCandleEmitFunction keeps the same
        // epoch-aligned 15 s windows, the same end-timer fire time, the same
        // accumulation math, the same quarantine gate and the same late-drop
        // leg — only the window state moved to the heap (see its javadoc
        // for the one documented fresh-slot edge).
        SingleOutputStreamOperator<RowData> candles = monitored
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                .process(new HeapCandleEmitFunction(config))
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("candle-15s")
                // G-CHAIN-3 (chain-heap redesign): new uid so Flink never
                // silently maps pre-redesign checkpointed window state onto
                // the heap operator (which requests no managed window
                // state). Restoring an old checkpoint fails closed — clean
                // start required.
                .uid("candle-15s-v2");

            // Preview path retired (2026-09-05 cutover, batch 3):
            // candle_live serves evolving candles; the parallel 1s
            // preview window, its KV sink, and its classes are gone.

        // REQ-FC-006: raw ticks dropped as beyond-allowed-lateness are counted
        // (compute.candles.late.dropped) instead of vanishing silently. The
        // counter operator is observability-only — no keyed state, no output.
        candles.getSideOutput(CandleLateDrop.OUTPUT)
                .process(new CandleLateDrop.CounterFunction(config.candleWindowMs()))
                .returns(ticks.getType())
                .name("candle-late-drop-counter")
                .uid("candle-late-drop-counter");

        // Plain FlussSink (CHG-023 item 4, 2026-08-17): the StallGuardedSink
        // watchdog is REMOVED. The native stall-guard is the checkpoint
        // timeout (30 s) + fixed-delay restart failing the job, never hanging
        // it; the Fluss client's own request-timeout (client.request-timeout
        // below) + retries=2 bound the write path. The 2026-08-12 hang the
        // guard patched (deleted table -> flush latch never counts down) is
        // now bounded by the checkpoint timeout + restart policy, and the
        // checkpoint-1 zero-ack stall class was eliminated by Design B (no
        // RPC on the hot path, CHG-022).
        //
        // Streaming-3000 T5 (decision 25): the KV write path is first-write-
        // wins. CandleKvFirstWriteWinsFunction (keyed by the candle PK
        // instrument_token + window_start) forwards the first emission of a
        // window to the sink and drops + counts any second emission
        // (compute.candles.duplicate_window) — a re-emission after a
        // checkpoint-restore re-fire or a same-day rollback must never
        // overwrite an already-written candle row.
        candles.keyBy(CandleKvFirstWriteWinsFunction.keySelector(),
                        Types.TUPLE(Types.LONG, Types.LONG))
                .process(new CandleKvFirstWriteWinsFunction())
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("candle-kv-first-write-wins")
                .uid("candle-kv-first-write-wins")
                .sinkTo(FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.candleTable())
                                // KV upsert: feature_candles_15s is a KV table
                                // (PK instrument_token, window_start) — the
                                // (false, false) RowDataSerializationSchema maps
                                // INSERT RowKinds to UPSERTs so replay/restart
                                // re-emits converge instead of appending
                                // duplicates (user requirement 2026-08-13:
                                // candle tables are KV-only, no LOG+KV twin).
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                                .build())
                .name("feature-candles-15s-sink")
                .uid("feature-candles-15s-sink");

        // Streaming-3000 T6 (decision 24): candles that fail one of the five
        // CandleInvariantCheck OHLC invariants never reach the main output —
        // CandleEmitFunction routes them here, to the quarantine side output.
        // The counter operator counts each into compute.candles.invalid.total
        // + the per-reason compute.candles.invalid.<reason> counters and logs
        // the violation (one bounded WARN per candle); the FlussSink appends
        // the evidence row to ingestion_quarantine (LOG append — the same
        // table the ingestion pipeline uses for invalid raw ticks, DDL 21).
        candles.getSideOutput(CandleQuarantine.OUTPUT)
                .process(new CandleQuarantine.CounterFunction())
                .returns(CandleQuarantineColumns.ROW_TYPE_INFO)
                .name("candle-invalid-quarantine")
                .uid("candle-invalid-quarantine")
                .sinkTo(FlussSink.<RowData>builder()
                        .setBootstrapServers(config.bootstrapServers())
                        .setDatabase(config.database())
                        .setTable(config.quarantineTable())
                        // LOG append-only: ingestion_quarantine has no primary
                        // key — (true, true) serialization ignores deletes.
                        .setSerializationSchema(new RowDataSerializationSchema(true, true))
                        .setOption("client.request-timeout",
                                config.sinkWriteStallTimeoutMs() + "ms")
                        .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                        .build())
                .name("candle-invalid-quarantine-sink")
                .uid("candle-invalid-quarantine-sink");

        // Slice 2.2 forming-bar handoff (REQ-FC-007/AC-FC-014, REQ-FC-013):
        // the LIVE forming bar forks off the SAME deduped tick stream (the
        // candle window's input), updated in-process on every accepted tick
        // and handed straight to Business Logic — no Fluss round trip, no
        // database read/write, no new transport on the hot path. The
        // completed-candle pipeline above is untouched: the window operator
        // remains the sole producer of finalized candles.
        //
        // Topology (both branches coexist from the same deduped input):
        //   deduped ── keyBy(token) ── FormingBarBuilder (per-tick emit)
        //        └── (existing) keyBy(token) ── window ── candle sink / SignalDetection
        //   FormingBarBuilder ── connect(candles) ── FormingBarDetection ── union ──
        //        existing signal LOG + KV dual-sink (REQ-SS-003 + DEC-035)
        SingleOutputStreamOperator<FormingBar> formingBars = monitored
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                .process(new FormingBarBuilderFunction(config))
                .returns(FormingBarTypeInfo.INSTANCE)
                .name("forming-bar-builder")
                // G-CHAIN-3 (chain-heap redesign): uid bumped — the builder
                // now keeps forming slots on the heap (no managed state);
                // restoring a pre-redesign checkpoint fails closed.
                .uid("forming-bar-builder-v2");

        // The detector co-locates both inputs by instrument key: the live
        // forming-bar events (input 1) and the completed candles (input 2).
        // Candidate rows feed the signal dual-sink below.
        DataStream<RowData> formingSignals = formingBars
                .connect(candles)
                .keyBy(
                        bar -> bar.instrumentToken(),
                        candle -> candle.getLong(CandleTableColumns.INSTRUMENT_TOKEN))
                .process(new FormingBarDetectionFunction(config))
                .returns(SignalCandidatesTableColumns.ROW_TYPE_INFO)
                .name("forming-bar-detection")
                // G-CHAIN-3 (chain-heap redesign): uid bumped — heap slots,
                // no managed state; restoring a pre-redesign checkpoint
                // fails closed.
                .uid("forming-bar-detection-v2");

        // Forming-bar durable home (persistence phase, 2026-08-16): the
        // builder's PERSIST_OUTPUT carries every tick's snapshot to a
        // coalescing writer (keyed by instrument — one buffered row per
        // instrument, the LATEST forming bar) that flushes on the
        // FORMING_BAR_WRITE_BATCH_MS cadence into the forming_bar KV
        // current-state projection (PK instrument_token, INSERT→UPSERT).
        // Current-state only, never per-tick history; the finalized candle
        // remains the completed-candle pipeline's artifact. The hot path
        // (tick → builder → detector → Business Logic) is untouched — the
        // Fluss write is off the per-tick path.
        formingBars
                .getSideOutput(FormingBarBuilderFunction.PERSIST_OUTPUT)
                .keyBy(bar -> bar.instrumentToken())
                .process(new FormingBarWriterFunction(config))
                .returns(FormingBarTableColumns.ROW_TYPE_INFO)
                .name("forming-bar-writer")
                // G-CHAIN-3 (chain-heap redesign): uid bumped — the buffered
                // snapshot moved to the heap (only the flush timer remains
                // managed); restoring a pre-redesign checkpoint fails closed.
                .uid("forming-bar-writer-v2")
                .sinkTo(FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.formingBarTable())
                                // KV upsert: forming_bar is a KV table (PK
                                // instrument_token) — (false, false) maps
                                // INSERT RowKinds to UPSERTs so replay/re-
                                // flush re-emits converge on the same key.
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                                .build())
                .name("forming-bar-sink")
                .uid("forming-bar-sink");

        // The forming-bar rule (breakout-5-forming-bar) is the sole
        // completed-candle signal producer after the 20-candle breakout rule
        // (signal-detection, 2026-09-05) was deleted — N7 on the multi-TF
        // chain (multi-tf branch above) supersedes it as the entry method.
        // Active-signal indefinite + CLOSED handshake (max-one-active, Option B
        // strict, 2026-08-18): if instrument already ACTIVE, drop new candidate
        // — wait till Position_State(status=CLOSED or ADMIN_CLEAR) from
        // Nautilus/Execution Gateway. No TTL — block survives restarts
        // (checkpointed ValueState) and is cleared only on explicit CLOSED.
        // Stuck ACTIVE never auto-frees — needs CLOSED or admin clear.
        DataStream<RowData> allSignals = formingSignals;

        FlussSource<RowData> positionStateSource = FlussSource.<RowData>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setDatabase(config.database())
                .setTable(config.positionStateTable())
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();
        DataStream<RowData> positionStates = env.fromSource(
                        positionStateSource, WatermarkStrategy.noWatermarks(), "position-state")
                .uid("position-state");

        DataStream<RowData> filteredSignals = allSignals
                .connect(positionStates)
                .keyBy(
                        row -> row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN),
                        row -> row.getLong(PositionStateTableColumns.INSTRUMENT_TOKEN))
                .process(new ActiveSignalFeedbackFunction(config))
                .name("active-signal-feedback")
                .uid("active-signal-feedback");

        filteredSignals
                .sinkTo(FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.signalCandidatesTable())
                                .setSerializationSchema(new RowDataSerializationSchema(true, true))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                                .build())
                .name("signal-candidates-sink")
                .uid("signal-candidates-sink");

        filteredSignals
                .filter(new CanonicalSignalFilterFunction())
                .name("canonical-signal-filter")
                .uid("canonical-signal-filter")
                .sinkTo(FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.signalCurrentTable())
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                                .build())
                .name("signal-candidates-current-sink")
                .uid("signal-candidates-current-sink");

        // Execution intent is a separate, explicitly disabled-by-default
        // branch. It consumes the immutable candidate stream but does not
        // call a broker, gateway, or Arrow service. The gateway remains the
        // authoritative duplicate/quarantine boundary in T2.
        if (config.executionIntentEnabled()) {
            filteredSignals
                    .flatMap(new ExecutionIntentProducerFunction(config))
                    .returns(ExecutionIntentTableColumns.ROW_TYPE_INFO)
                    .name("execution-intent-producer")
                    .uid("execution-intent-producer")
                    .sinkTo(FlussSink.<RowData>builder()
                            .setBootstrapServers(config.bootstrapServers())
                            .setDatabase(config.database())
                            .setTable(config.executionIntentTable())
                            .setSerializationSchema(new RowDataSerializationSchema(true, true))
                            .setOption("client.request-timeout",
                                    config.sinkWriteStallTimeoutMs() + "ms")
                            .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                            .build())
                    .name("execution-intent-sink")
                    .uid("execution-intent-sink");
        }

        return env;
    }

    /**
     * Production runtime options (tracker 14 P4.1/P4.2), extracted from the
     * config into the Flink {@link Configuration}: state backend (rocksdb in
     * production, hashmap dev-only — validated by
     * {@code SignalJobConfig.from}), incremental checkpoints for RocksDB,
     * RocksDB local state dirs + managed memory, the savepoint directory, and
     * the explicit checkpoint directory (kept from the caller, above).
     *
     * <p>Never sets {@code allowNonRestoredState} — a restore that cannot
     * fully match the graph fails closed (P4.3, CHECKPOINT-RESTORE-001), it
     * does not degrade to a silent full replay.
     *
     * <p>Package-visible so {@code RuntimeOptionsTest} can assert the exact
     * Configuration a run would use, without a Flink cluster.
     */
    static void applyRuntimeOptions(SignalJobConfig config, Configuration flinkConfig) {
        if ("rocksdb".equals(config.stateBackend())) {
            // Shortcut names per StateBackendOptions: 'rocksdb' (or 'hashmap').
            flinkConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            // Incremental checkpoints are enabled only on the RocksDB backend
            // (Flink ignores them on heap state) and only for the keyed
            // MapState + timer state this graph uses — both fully supported.
            // Streaming-3000 T3 G3 Compute: RocksDB incremental is pinned for
            // the 3000-instrument envelope (16 Fluss buckets → 8 TaskSlots,
            // ~1 GB checkpoint at 15M entries).
            flinkConfig.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
            // Streaming-3000 T3 G3: SSD dirs — RocksDB local state on fast
            // disk (dev: /tmp/flink-rocksdb on host SSD, prod: SSD mount),
            // not on the container overlay. T3 default is explicit so p8 does
            // not silently use the overlay's limited disk.
            if (config.stateBackendLocalDirs() != null) {
                // RocksDBOptions.LOCAL_DIRECTORIES ("state.backend.rocksdb.localdir",
                // singular) is the live key in Flink 2.2.1; the older
                // "state.backend.rocksdb.local_directories" key is dead in this
                // version and would silently drop the fast-disk pin (tracker 14
                // P4.1 — verified against the pinned
                // flink-statebackend-rocksdb-2.2.1.jar).
                flinkConfig.setString(
                        "state.backend.rocksdb.localdir", config.stateBackendLocalDirs());
            } else {
                flinkConfig.setString("state.backend.rocksdb.localdir", "/tmp/flink-rocksdb");
            }
            if (!config.stateBackendManagedMemory()) {
                flinkConfig.setString("state.backend.rocksdb.memory.managed", "false");
            } else {
                // Streaming-3000 T3 G3: managed fraction for RocksDB block
                // cache/memtables. NOTE (CHG-120 2026-09-01): in cluster mode
                // the TM pool is sized at TM STARTUP from flink-conf
                // (docker-compose taskmanager.memory.managed.fraction — 0.6
                // there now); this job-level value only governs
                // embedded/local runs. Verified in
                // DedupRocksDbThroughputMemoryIT.
                flinkConfig.setString("taskmanager.memory.managed.fraction", "0.6");
            }
            // E2E root cause (2026-08-17): under LOCAL execution (no
            // flink-conf.yaml) Flink defaults taskmanager.memory.managed.size
            // to 128 MB TOTAL — the RocksDB block cache for the Design-B dedup
            // envelope (~628 MB at 20 480 t/s × 300 s) thrashes inside that
            // pool and throughput collapses to ≈ the feed rate, so the E2E job
            // never catches the backlog tail. Explicit passthrough
            // (TASK_MANAGER_MEMORY_MANAGED_SIZE) for embedded/local runs only;
            // unset → the deployment (flink-conf.yaml) stays authoritative.
            // T3 G3 keeps this passthrough for E2E (2048m) but the 0.4 fraction
            // above is the prod pin; an explicit size overrides the derived
            // managed size and must win when present.
            if (config.taskManagerMemoryManagedSize() != null) {
                flinkConfig.setString("taskmanager.memory.managed.size",
                        config.taskManagerMemoryManagedSize());
            }
            // Tracker 14 box 906 (2026-08-12): export RocksDB native-memory
            // gauges via the per-property boolean keys (verified against
            // RocksDBProperty in the pinned flink-statebackend-rocksdb-2.2.1
            // jar: block-cache-usage / cur-size-all-mem-tables /
            // estimate-table-readers-mem are valid enum kebab names). The
            // gauges register on the keyed-state operator metric group and
            // land on the TM reporter output as
            // flink_taskmanager_job_task_operator_<state.backend.rocksdb.<prop>>
            // series. RocksDB-only by construction — the hashmap branch sets
            // none of these keys.
            flinkConfig.setString("state.backend.rocksdb.metrics.block-cache-usage", "true");
            flinkConfig.setString(
                    "state.backend.rocksdb.metrics.cur-size-all-mem-tables", "true");
            flinkConfig.setString(
                    "state.backend.rocksdb.metrics.estimate-table-readers-mem", "true");
        } else {
            flinkConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        }
        // Streaming-3000 T3 G3: network max 256m for p=8 (Fluss 16 buckets →
        // 8 slots, hash(token) rebalance). Local MiniCluster defaults to
        // 64 MB (2048 × 32 KB buffers) — at 8 subtasks the connected
        // forming-bar branch (connect+keyBy) needs 256m (observed 2026-08-17:
        // p16 required 17 buffers, only 13 available; p8 needs headroom for
        // 16→8 rebalance + checkpoint barrier). Default 256m when not
        // overridden; min pinned below max so the pair is sane. Backend-
        // agnostic: network buffers are for shuffles/credit, not state.
        String netMax = config.taskManagerNetworkMemoryMax() != null
                ? config.taskManagerNetworkMemoryMax() : "256m";
        flinkConfig.setString("taskmanager.memory.network.max", netMax);
        flinkConfig.setString("taskmanager.memory.network.min", netMax);
        if (config.savepointDir() != null) {
            flinkConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, config.savepointDir());
        }
        // Native OpenTelemetry metric reporter (CHG-023 item 1, 2026-08-17):
        // every Signal-job counter/gauge (compute.invalid.byReason.schema-
        // version, compute.dedup.*, compute.candles.late.dropped, source
        // throughput/watermark, container memory, ...) is now a Flink
        // MetricGroup metric exported by flink-metrics-otel — the hand-rolled
        // ComputeOtlpEmitter metric mirror is deleted. The reporter is wired
        // from OTEL_COLLECTOR_HOST (default otel-collector:4318 — the HTTP
        // OTLP port; protocol=http matches the emitter's old HTTP path).
        // Keys verified against OpenTelemetryReporterOptions + MetricOptions
        // in the pinned 2.2.1 dist: exporter.endpoint (required),
        // exporter.protocol (gRPC default → http), service.name/version, and
        // the standard metrics.reporter.<name>.interval (default 10 s — the
        // old emitter's flush cadence). ServiceLoader discovers the factory
        // from the job classpath (embedded/dev/E2E) or /opt/flink/plugins/
        // (distributed dist — see CHG-023 deployment note).
        flinkConfig.setString("metrics.reporter.otel.factory.class",
                "org.apache.flink.metrics.otel.OpenTelemetryMetricReporterFactory");
        flinkConfig.setString("metrics.reporter.otel.exporter.endpoint",
                // The /v1/metrics path is REQUIRED in the endpoint (2026-08-17
                // verification): flink-metrics-otel 2.2.1 passes the configured
                // value verbatim to the OTLP HTTP sender — no signal-path
                // append. The shaded SDK's OWN default is
                // http://localhost:4318/v1/metrics (path included); a bare
                // host:port endpoint makes the reporter POST to the ROOT, which
                // the otelcol OTLP receiver answers 404 (observed: every 10 s
                // flush failed, O2 got nothing until this fix).
                "http://" + config.otelCollectorHost() + "/v1/metrics");
        flinkConfig.setString("metrics.reporter.otel.exporter.protocol", "http");
        flinkConfig.setString("metrics.reporter.otel.service.name", "compute");
        flinkConfig.setString("metrics.reporter.otel.service.version",
                config.configurationVersion());
        // Pin the 10 s cadence explicitly — the DELTA alert semantics (fires
        // on NEW rejections/episodes per poll, never on replay) depend on it.
        flinkConfig.setString("metrics.reporter.otel.interval", "10s");
        // Tracker 14 P4.2 — object-store (S3/R2) checkpoint access. The
        // endpoint/credentials/region go into the Flink Configuration ONLY
        // when a checkpoint/savepoint URI is an object-store URI (config
        // validation in SignalJobConfig.s3Endpoint already failed closed
        // otherwise). Credentials come from secret injection via env — never
        // committed files — and the effective-backend log below prints URI
        // schemes only, never the endpoint path or keys.
        if (config.s3Endpoint() != null) {
            flinkConfig.setString("fs.s3a.endpoint", config.s3Endpoint());
            flinkConfig.setString("fs.s3a.access.key", config.s3AccessKey());
            flinkConfig.setString("fs.s3a.secret.key", config.s3SecretKey());
            flinkConfig.setString("fs.s3a.endpoint.region", config.s3Region());
            flinkConfig.setString("fs.s3a.path.style.access", String.valueOf(config.s3PathStyle()));
            flinkConfig.setString("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            // G4 Durability T9: enforce encrypted S3 at-rest for checkpoints/savepoints.
            // R2 encrypts at rest by default (AES256, verified via get_bucket_encryption);
            // S3 uses SSE-S3. The bucket policy already enforces AES256;
            // this Flink property makes the client request SSE explicitly so an
            // unencrypted write is never attempted even on a non-default bucket.
            flinkConfig.setString("fs.s3a.server-side-encryption-algorithm", "AES256");
        }
        // Effective-backend log WITHOUT secrets: the checkpoint URI is printed
        // as its scheme only, never the full path (credentials may be embedded
        // in S3 URIs — tracker 14 P4.2 "never committed files").
        // T9-errata 2026-08-22: blank env injection (SAVEPOINT_DIR=${VAR:-} -> "")
        // is not null — guard against both so the scheme log cannot blow up
        // with StringIndexOutOfBoundsException(0, -1) on every submit.
        String cpDir = config.checkpointDir();
        String spDir = config.savepointDir();
        String cpScheme = (cpDir == null || cpDir.isEmpty()) ? "none"
                : cpDir.substring(0, cpDir.indexOf(':'));
        String spScheme = (spDir == null || spDir.isEmpty()) ? "none"
                : spDir.substring(0, spDir.indexOf(':'));
        LOG.info("signal-job: effective state backend = {} (dev={}, incremental={}), "
                + "checkpoint URI class = {}, savepoint URI class = {}, parallelism = {}",
                config.stateBackend(), config.deploymentEnv(),
                "rocksdb".equals(config.stateBackend())
                        && flinkConfig.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS),
                cpScheme, spScheme, config.parallelism());
    }

    /**
     * Read-only metadata preflight (tracker 14 P1 — CANDLE-SCHEMA-002;
     * tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001): opens a short Fluss
     * connection and checks the three deployed tables the write paths rely
     * on — the candle KV (PK exactly [instrument_token, window_start],
     * instrument_token routing, 16 buckets, exact 15-column v2 schema), the
     * signal LOG (no PK, instrument_token routing, 16 buckets, exact
     * 22-column v3 schema), and the signal current-state KV (PK exactly
     * [instrument_token], instrument_token routing, 16 buckets, the same
     * 22-column schema) — then closes. Any
     * violation — or an unreachable cluster — fails startup before the graph
     * is built: the job never writes to a table that contradicts the contract
     * it was compiled against.
     */
    static void preflightTableContracts(SignalJobConfig config) {
        org.apache.fluss.config.Configuration clientConf = new org.apache.fluss.config.Configuration();
        clientConf.setString("bootstrap.servers", config.bootstrapServers());
        try (org.apache.fluss.client.Connection conn =
                org.apache.fluss.client.ConnectionFactory.createConnection(clientConf)) {
            org.apache.fluss.metadata.TableInfo candleKv = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(config.database(), config.candleTable()))
                    .getTableInfo();
            TableContractValidator.validateCandleKvTable(candleKv);
            org.apache.fluss.metadata.TableInfo signalLog = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(
                            config.database(), config.signalCandidatesTable()))
                    .getTableInfo();
            TableContractValidator.validateSignalLogTable(signalLog);
            org.apache.fluss.metadata.TableInfo signalCurrent = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(
                            config.database(), config.signalCurrentTable()))
                    .getTableInfo();
            TableContractValidator.validateSignalCurrentKvTable(signalCurrent);
            // Forming-bar durable home (persistence phase, 2026-08-16): the
            // forming_bar KV is Fluss-authoritative durable state (DEC-038
            // matrix) — a hard startup dependency; fail closed on drift,
            // never write to a table that contradicts the 11-column v1
            // current-state contract.
            org.apache.fluss.metadata.TableInfo formingBar = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(
                            config.database(), config.formingBarTable()))
                    .getTableInfo();
            TableContractValidator.validateFormingBarKvTable(formingBar);
            // Position_State handshake (Option B, 2026-08-18): Signal job reads
            // the changelog of this KV table (PK instrument_token, 7 cols, 16
            // buckets) and clears its per-instrument ACTIVE block only on
            // CLOSED — fail closed on drift, never degrade to TTL.
            org.apache.fluss.metadata.TableInfo positionState = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(
                            config.database(), config.positionStateTable()))
                    .getTableInfo();
            TableContractValidator.validatePositionStateKvTable(positionState);
            if (config.multiTfEnabled()) {
                org.apache.fluss.metadata.TableInfo candleLive = conn
                        .getTable(org.apache.fluss.metadata.TablePath.of(
                                config.database(), config.candleLiveTable()))
                        .getTableInfo();
                TableContractValidator.validateCandleLiveTable(candleLive);
                org.apache.fluss.metadata.TableInfo candleClosed = conn
                        .getTable(org.apache.fluss.metadata.TablePath.of(
                                config.database(), config.candleClosedTable()))
                        .getTableInfo();
                TableContractValidator.validateCandleClosedTable(candleClosed);
                LOG.info("signal-job: candle_live contract OK ({})", config.candleLiveTable());
                LOG.info("signal-job: {}",
                        TableContractValidator.schemaReport(
                                candleLive, CandleLiveColumns.COLUMN_NULLABLE_IN_DDL));
                LOG.info("signal-job: candle_closed contract OK ({})", config.candleClosedTable());
                LOG.info("signal-job: {}",
                        TableContractValidator.schemaReport(
                                candleClosed, CandleClosedColumns.COLUMN_NULLABLE_IN_DDL));
            }
            if (config.executionIntentEnabled()) {
                org.apache.fluss.metadata.TableInfo executionIntent = conn
                        .getTable(org.apache.fluss.metadata.TablePath.of(
                                config.database(), config.executionIntentTable()))
                        .getTableInfo();
                TableContractValidator.validateExecutionIntentLogTable(executionIntent);
                LOG.info("signal-job: execution-intent LOG contract OK ({})",
                        config.executionIntentTable());
                LOG.info("signal-job: {}", TableContractValidator.schemaReport(
                        executionIntent, ExecutionIntentTableColumns.COLUMN_NULLABLE_IN_DDL));
            }
            // SCH-19 (machinery): when the decision dual-sink is enabled, the
            // Trade_Decisions LOG + trade_instruction_state KV index must
            // match the contracts the write paths rely on before the graph is
            // built — fail closed on drift, never write degraded. Disabled by
            // default: the ranking feed (Slice 3) does not exist yet.
            if (config.tradeDecisionsEnabled()) {
                org.apache.fluss.metadata.TableInfo tradeLog = conn
                        .getTable(org.apache.fluss.metadata.TablePath.of(
                                config.database(), config.tradeDecisionsTable()))
                        .getTableInfo();
                TableContractValidator.validateTradeDecisionsLogTable(tradeLog);
                org.apache.fluss.metadata.TableInfo tradeIndex = conn
                        .getTable(org.apache.fluss.metadata.TablePath.of(
                                config.database(), config.tradeInstructionStateTable()))
                        .getTableInfo();
                TableContractValidator.validateTradeInstructionStateKvTable(tradeIndex);
                LOG.info("signal-job: trade-decisions LOG contract OK ({})",
                        config.tradeDecisionsTable());
                LOG.info("signal-job: {}", TableContractValidator.schemaReport(
                        tradeLog, TradeDecisionsTableColumns.COLUMN_NULLABLE_IN_DDL));
                LOG.info("signal-job: trade-instruction-state KV contract OK ({})",
                        config.tradeInstructionStateTable());
            }
            // Log the validated schema reports — exact live columns/types
            // (and the DDL-vs-live nullability divergence where Fluss does
            // not carry NOT NULL) as startup evidence.
            LOG.info("signal-job: candle table contract OK ({} KV)", config.candleTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            candleKv, CandleTableSchema.COLUMN_NULLABLE_IN_DDL));
            LOG.info("signal-job: signal LOG contract OK ({})", config.signalCandidatesTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            signalLog, SignalCandidatesTableColumns.COLUMN_NULLABLE_IN_DDL));
            LOG.info("signal-job: signal current-state KV contract OK ({})",
                    config.signalCurrentTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            signalCurrent, SignalCandidatesTableColumns.COLUMN_NULLABLE_IN_DDL));
            LOG.info("signal-job: forming-bar KV contract OK ({})",
                    config.formingBarTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            formingBar, FormingBarTableColumns.COLUMN_NULLABLE_IN_DDL));
            LOG.info("signal-job: position-state KV contract OK ({})",
                    config.positionStateTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            positionState, PositionStateTableColumns.COLUMN_NULLABLE_IN_DDL));
        } catch (TableContractValidator.ContractViolation e) {
            throw e; // contract drift: fail closed, do not build a degraded graph
        } catch (Exception e) {
            throw new IllegalStateException(
                    "signal-job: table preflight failed — is the dev Fluss cluster reachable at "
                            + config.bootstrapServers() + "? (" + e.getMessage() + ")", e);
        }
    }

    /**
     * Best-effort host name for the {@code host.name} resource attribute
     * (tracker 14 P8.0/831): the container/OS hostname, never a secret.
     * Fallback chain: {@code HOSTNAME} env (containers/shells) →
     * {@code InetAddress} → {@code "unknown"} — a resolve failure must not
     * fail the job (telemetry is off the critical path).
     */
    static String hostName() {
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) {
            return env;
        }
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
            return "unknown";
        }
    }
}
