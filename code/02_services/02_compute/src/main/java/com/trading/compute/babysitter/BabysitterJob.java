package com.trading.compute.babysitter;

import com.trading.common.schema.position.PositionSnapshot;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Babysitter Flink job — read-only {@code Positions} observer (Task 7 of
 * docs/08_implementation/19-nautilus-execution-service-implementation-plan.md,
 * DEC-017).
 *
 * <p>Replaces the MVP marker source with a versioned {@code Positions} KV
 * changelog source: full snapshot+changelog offsets
 * ({@code OffsetsInitializer.full()}), current-value {@code RowKind} filtering
 * (INSERT/UPDATE_AFTER), a keyed observation operator that retains the latest
 * validated version/freshness metadata per {@code position_id} in checkpointed
 * {@code ValueState}, and a deliberate no-op terminal sink. The job
 * checkpoints observation state, so a restored run continues from the last
 * accepted version instead of losing it.
 *
 * <p>Emits zero {@code Position_Actions} records for every input in MVP.
 * {@code POSITION_ACTIONS_ENABLED} must be unset or {@code false}; any attempt
 * to enable it fails startup (with the same R-286 trimming that guards the
 * legacy shell). Missing/malformed source config fails closed at startup.
 *
 * <p>Safety rule: the Babysitter never calls the Arrow REST API or any broker
 * endpoint directly, and it never writes lifecycle/position/execution tables —
 * <p>See docs/08_implementation/05-execution-core.md (Babysitter — position
 * observation) for the full implementation contract.
 *
 * <p>Implementation note for gate: POSITION_ACTIONS_ENABLED = false hard-coded
 * in MVP (validated via BabysitterConfig.parseActionEnabled); startup fails
 * if POSITION_ACTIONS_ENABLED must be false in MVP is violated.
 */
public final class BabysitterJob {

    private static final Logger LOG = LoggerFactory.getLogger(BabysitterJob.class);

    /** Pinned source UID; replaced the MVP {@code babysitter-mvp-source}. */
    static final String SOURCE_UID = "positions-changelog";

    private BabysitterJob() {
        // utility class
    }

    /** Production entry point: build and submit the Positions observer. */
    public static void main(String[] args) throws Exception {
        // Fail closed on the action flag + required configuration.
        BabysitterConfig config = BabysitterConfig.fromEnv();
        LOG.info(
            "babysitter: starting Positions observer "
                    + "(POSITION_ACTIONS_ENABLED={}, database={}, table={}, checkpointDir={}, "
                    + "checkpointIntervalMs={}, freshnessThresholdMs={})",
            config.actionEnabled(), config.database(), config.table(), config.checkpointDir(),
            config.checkpointIntervalMs(), config.freshnessThresholdMs());
        buildTopology(config).execute("Babysitter Positions observer");
    }

    /**
     * Fail closed if {@code envValue} is anything but unset or {@code false}
     * (case-insensitive, trimmed). Package-private so BAB-UNIT-002 can drive
     * every variant without a Flink cluster.
     */
    static void validateActionFlag(String envValue) {
        BabysitterConfig.parseActionEnabled(envValue);
    }

    /**
     * Builds the full production topology — real {@code FlussSource} over
     * {@code Positions} (snapshot+changelog), checkpointing, then the shared
     * observation pipeline. Submitting this connects to the Fluss cluster
     * (fail-fast if unreachable or the table is missing).
     */
    static StreamExecutionEnvironment buildTopology(BabysitterConfig config) {
        // Checkpoint storage is a deployment property; point local/dev runs at
        // a durable directory (mirrors the proven SignalJob CHECKPOINT_DIR
        // pattern) so a restart can restore observation state.
        Configuration flinkConfig = new Configuration();
        if (config.checkpointDir() != null) {
            flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointDir());
        }
        // Optional restore: BABYSITTER_STATE_RECOVERY_PATH (a completed checkpoint
        // URI) makes a restarted Babysitter continue from the last accepted
        // observation versions instead of a full replay. Same path key SignalJob
        // uses for state recovery (StateRecoveryOptions.SAVEPOINT_PATH), read at
        // StreamGraph generation time.
        if (config.stateRecoveryPath() != null) {
            flinkConfig.set(StateRecoveryOptions.SAVEPOINT_PATH, config.stateRecoveryPath());
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(config.checkpointIntervalMs());
        // A deliberate cancel/restart must retain the completed observation-state
        // checkpoint (DEC-017: a restored run continues from the last accepted
        // version). Mirror the proven SignalJob idiom — without this, cancelling
        // the Babysitter deletes its checkpoint and a restart falls back to a
        // full-replay, silently losing the version gate.
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        FlussSource<RowData> source = FlussSource.<RowData>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setDatabase(config.database())
                .setTable(config.table())
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();

        DataStream<RowData> positions = env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), SOURCE_UID)
                .uid(SOURCE_UID);

        attachObservationPipeline(positions, config);
        return env;
    }

    /**
     * Cluster-free observation pipeline: RowKind filter → validated snapshot
     * deserializer → keyed observation operator (checkpointed ValueState) →
     * deliberate no-op discard. Separated so a unit test can attach the exact
     * production operators to an in-memory {@code Positions} stand-in and
     * inspect the {@code StreamGraph} without a cluster.
     */
    static void attachObservationPipeline(DataStream<RowData> positions,
            BabysitterConfig config) {
        positions
                .filter(BabysitterJob::isCurrentValueRow)
                .name("babysitter-rowkind-filter")
                .uid("babysitter-rowkind-filter")
                .flatMap(new PositionsRowDeserializer())
                .name("babysitter-position-deserialize")
                .uid("babysitter-position-deserialize")
                .keyBy(PositionSnapshot::positionId)
                .process(new PositionsObservationOperator(config.freshnessThresholdMs()))
                .name("babysitter-position-observation")
                .uid("babysitter-position-observation")
                .addSink(new BabysitterDiscardSink())
                .name("babysitter-discard")
                .uid("babysitter-discard");
    }

    /** INSERT and UPDATE_AFTER carry the current full row; BEFORE/DELETE are skipped. */
    static boolean isCurrentValueRow(RowData row) {
        RowKind kind = row.getRowKind();
        return kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER;
    }

    /**
     * Deliberate no-op terminal boundary: receives nothing (the observation
     * operator never emits) and would only discard anyway. Guarantees the
     * graph has a sink node with no Action Capture / Arrow / lifecycle /
     * position / execution output behind it.
     */
    @SuppressWarnings("deprecation") // SinkFunction (legacy package) is the standard 2.2.1 no-op sink
    public static final class BabysitterDiscardSink implements SinkFunction<Void> {

        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(Void value, Context context) {
            // Intentionally empty: no Position_Actions, no persistence, no
            // broker call. The Babysitter is a read-only observer.
        }
    }
}
