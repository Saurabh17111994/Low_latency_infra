package com.trading.compute.safetyhalt;

import com.trading.common.safety.SafetyHaltRequestParser;
import com.trading.common.safety.SafetyStateTracker;
import com.trading.common.safety.SlotAssignment;
import com.trading.common.safety.SlotAssignmentResolver;
import com.trading.common.safety.SlotSafetyRequest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Safety-halt consumer — the Signal Job's slot-scoped safety propagation
 * feature (plan.md &sect; "Slot-scoped safety propagation").
 *
 * <p>Consumes the {@code Safety_Halt_Requests} KV changelog (Fluss
 * {@code FlussSource}, exactly-once snapshot+changelog via
 * {@code OffsetsInitializer.full()}), bridges each current-value row
 * (INSERT/UPDATE_AFTER) into {@link SlotSafetyRequest}, and applies it to a
 * {@link SafetyStateTracker}. In this shell there is no decision pipeline
 * yet, so the observable contract is: every changelog row is parsed, applied,
 * and the transition is logged and counted. When the compute pipeline lands,
 * the tracker snapshot moves to broadcast state so decision operators
 * consult {@code isTokenSuppressed} / {@code SuppressionGate}
 * (docs/08_implementation/04-signal-job.md).
 *
 * <p>RowKind semantics: the KV changelog carries INSERT (new key),
 * UPDATE_AFTER (upsert), UPDATE_BEFORE, and DELETE. Only INSERT/UPDATE_AFTER
 * carry the current full row; BEFORE/DELETE are skipped. Re-delivered rows
 * (Flink replay, server-side upsert no-ops) are deduped by the tracker's
 * strict-epoch rules.
 *
 * <p>Fail-fast startup: {@code FlussSource.build()} performs a live
 * {@code Admin.getTableInfo} — the job refuses to start without a reachable
 * cluster and an existing table. The manifest-derived slot assignment is
 * loaded from {@code SAFETY_MANIFEST_TOKENS} (comma-separated instrument
 * tokens); the production job replaces this with the manifest loader.
 */
public final class SafetyHaltJob {

    // P2-219: no outer logger — logging lives in SafetyHaltApplyFunction.LOG.
    private SafetyHaltJob() {
        // utility class
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = envOr("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123");
        String database = envOr("FLUSS_DATABASE", "default");
        String table = envOr("FLUSS_TABLE", "Safety_Halt_Requests");
        long checkpointIntervalMs = parseLongEnv("SAFETY_CHECKPOINT_INTERVAL_MS", "60000");
        if (checkpointIntervalMs <= 0) {
            throw new IllegalStateException(
                    "SAFETY_CHECKPOINT_INTERVAL_MS must be positive, got '" + checkpointIntervalMs
                            + "'");
        }
        SlotAssignment assignment = loadAssignment();

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointIntervalMs);

        FlussSource<RowData> source = FlussSource.<RowData>builder()
                .setBootstrapServers(bootstrap)
                .setDatabase(database)
                .setTable(table)
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();

        DataStream<RowData> haltRequests = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "safety-halt-requests");

        // P2-220: single RowKind path — the flatMap guard counts every drop
        // in rows.skipped; the upstream filter dropped rows uncounted.
        haltRequests
                .flatMap(new SafetyHaltApplyFunction(assignment))
                .name("safety-halt-tracker");

        env.execute("Safety-halt consumer");
    }

    /** INSERT and UPDATE_AFTER carry the current full row; BEFORE/DELETE are skipped. */
    static boolean isCurrentValueRow(RowData row) {
        RowKind kind = row.getRowKind();
        return kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER;
    }

    /**
     * Manifest-derived slot assignment for the tracker's trust gates.
     * {@code SAFETY_MANIFEST_TOKENS} is a comma-separated token list;
     * {@code SAFETY_SLOTS} (default 1) and {@code SAFETY_CONNECTION_LIMIT}
     * (default 1024) shape the Go-parity chunking.
     */
    static SlotAssignment loadAssignment() {
        String tokensValue = envOr("SAFETY_MANIFEST_TOKENS", null);
        if (tokensValue == null) {
            throw new IllegalStateException(
                "SAFETY_MANIFEST_TOKENS is required (comma-separated instrument tokens); "
                + "the production job loads the manifest file instead");
        }
        // P2-116: split(-1) keeps trailing empties; blank tokens and bare
        // NumberFormatException become startup errors naming var + value.
        List<Long> tokens = new ArrayList<>();
        for (String part : tokensValue.split(",", -1)) {
            String t = part.trim();
            if (t.isEmpty()) {
                throw new IllegalStateException(
                        "SAFETY_MANIFEST_TOKENS contains blank token: '" + tokensValue + "'");
            }
            try {
                tokens.add(Long.parseLong(t));
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                        "SAFETY_MANIFEST_TOKENS invalid token '" + t + "': '" + tokensValue + "'",
                        e);
            }
        }
        int slots = parseIntEnv("SAFETY_SLOTS", "1");
        int connectionLimit = parseIntEnv("SAFETY_CONNECTION_LIMIT", "1024");
        int checkpointInterval = parseIntEnv("SAFETY_CHECKPOINT_INTERVAL_MS", "60000");
        if (checkpointInterval <= 0) {
            throw new IllegalStateException(
                    "SAFETY_CHECKPOINT_INTERVAL_MS must be positive, got '" + checkpointInterval
                            + "'");
        }
        return SlotAssignmentResolver.of(tokens, slots, connectionLimit);
    }

    /** P2-116: numeric env parses fail with var name + offending value. */
    static int parseIntEnv(String name, String def) {
        String value = envOr(name, def);
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    name + " must be an integer, got '" + value + "'", e);
        }
    }

    /** P2-116: same for longs (checkpoint interval). */
    static long parseLongEnv(String name, String def) {
        String value = envOr(name, def);
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    name + " must be an integer, got '" + value + "'", e);
        }
    }

    static String envOr(String name, String def) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? def : value;
    }

    /**
     * Applies parsed safety rows to a per-task {@link SafetyStateTracker}.
     * Malformed rows are counted and skipped — never fatal. Transitions are
     * logged for the operator; counters feed the observability contract.
     */
    static final class SafetyHaltApplyFunction
            extends RichFlatMapFunction<RowData, SlotSafetyRequest> {

        private static final Logger LOG =
            LoggerFactory.getLogger(SafetyHaltApplyFunction.class);

        private final SlotAssignment assignment;
        private transient SafetyStateTracker tracker;
        private transient Counter applied;
        private transient Counter malformed;
        private transient Counter skipped;

        SafetyHaltApplyFunction(SlotAssignment assignment) {
            this.assignment = assignment;
        }

        @Override
        public void open(OpenContext ctx) {
            tracker = new SafetyStateTracker(assignment);
            var group = getRuntimeContext().getMetricGroup().addGroup("safety");
            applied = group.counter("transitions.applied");
            malformed = group.counter("rows.malformed");
            skipped = group.counter("rows.skipped");
        }

        @Override
        public void flatMap(RowData row, Collector<SlotSafetyRequest> out) {
            if (!isCurrentValueRow(row)) {
                skipped.inc();
                return;
            }
            final SlotSafetyRequest request;
            try {
                request = SafetyHaltRowDataBridge.toRequest(row);
            } catch (SafetyHaltRequestParser.ParseException e) {
                malformed.inc();
                LOG.warn("safety: malformed Safety_Halt_Requests row skipped: {}", e.getMessage());
                return;
            } catch (RuntimeException e) {
                // P2-010 backstop: bridge NPE/arity/cast failures must be a
                // counted skip, never a task-killing poison pill on full()
                // replay. Bridge null-safety (P2-014/015) is the cure.
                malformed.inc();
                LOG.warn("safety: unexpected Safety_Halt_Requests row skipped: {}: {}",
                        e.getClass().getSimpleName(), e.getMessage());
                return;
            }
            SafetyStateTracker.ApplyResult result = tracker.apply(request);
            // P2-011+P2-117: only real transitions move forward and count as
            // applied; IGNORED_* (replays, trust-gate rejections) count as
            // skipped and stay at DEBUG so full() replays don't flood INFO or
            // masquerade as new halts — and distrusted rows never reach the
            // decision operators when the broadcast pipeline lands.
            switch (result) {
                case NEW_UNSAFE, RECOVERED -> {
                    applied.inc();
                    LOG.info("safety: slot {} {} -> {} (epoch {}, reason '{}')",
                            request.slotId(), request.status(), result,
                            request.connectionEpoch(), request.reasonCode());
                    out.collect(request);
                }
                default -> {
                    skipped.inc();
                    LOG.debug("safety: slot {} {} ignored: {} (epoch {})",
                            request.slotId(), request.status(), result,
                            request.connectionEpoch());
                }
            }
        }
    }
}
