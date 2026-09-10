package com.trading.compute.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.trading.compute.signaljob.SignalJob;
import com.trading.compute.signaljob.SignalJobConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline JobGraph / operator-ID dump for checkpoint-restore compatibility
 * evidence (CANDLE-KV-REPLAY-001 P6).
 *
 * <p>Builds the exact SignalJob topology via {@link SignalJob#buildTopology} —
 * the same code path a running job uses — <b>without executing it</b> (no
 * task deployment, no checkpoints, no table writes; the Fluss source/sink
 * builders perform read-only metadata lookups, so the target cluster must be
 * reachable, exactly like a real startup). Writes three artifacts into the
 * output directory:
 *
 * <ul>
 *   <li>{@code stream-nodes.txt} — every stream operator:
 *       {@code id | transformation UID | operator name}. Fails when an operator
 *       lacks an explicit UID (restore keys on UIDs — P2-072).</li>
  *   <li>{@code job-vertices.txt} — every job vertex (post-chaining):
 *       {@code name | vertex id | parallelism | operators=user UIDs}, sorted by
 *       {@code (name, id)}. The restore-compatibility contract is the operator
 *       UID set (with unchanged operator state shapes) — {@code JobVertexID}
 *       is randomly generated per {@code JobGraph} instantiation, so diff the
 *       names/UIDs, never the vertex IDs (P2-073).</li>
  *   <li>{@code jobgraph.json} — the full JSON plan (JsonPlanGenerator).</li>
 * </ul>
 *
 * <p>Usage: {@code java -cp <compute classes>:<flink dist libs>}
 * {@code com.trading.compute.tools.JobGraphDump [out-dir]}. The config is read
 * from the environment exactly like the job ({@code SignalJobConfig.fromEnv});
 * the startup mode ({@code RESTORE | FULL_REPLAY | LATEST} per
 * {@code SignalJobConfig.validateStartupMode}) selects the source offsets, so
 * the mode is logged and written into the output for comparable evidence —
 * dumps taken under different modes (different {@code rawSourceOffsets}) must
 * never be compared as restore evidence.
 */
public final class JobGraphDump {

    private static final Logger LOG = LoggerFactory.getLogger(JobGraphDump.class);

    private JobGraphDump() {}

    public static void main(String[] args) throws Exception {
        Path outDir = args.length > 0 ? Path.of(args[0]) : Path.of("logs/candle-kv-replay-001/jobgraph");
        Files.createDirectories(outDir);

        SignalJobConfig config = SignalJobConfig.fromEnv();
        // P2-194: the mode selects the source offsets, so it rides the log
        // and a sidecar file — dumps on different modes are not comparable.
        // P2-071: never log the whole config record — record toString() prints
        // s3AccessKey/s3SecretKey. Field-selective: mode/parallelism/backend/
        // tables only, never credentials or full paths.
        LOG.info("jobgraph-dump: startupMode={} parallelism={} backend={} liveTable={} "
                        + "closedTable={} signalTables={}/{}",
                config.startupMode(), config.parallelism(), config.stateBackend(),
                config.candleLiveTable(), config.candleClosedTable(),
                config.signalCandidatesTable(), config.signalCurrentTable());

        StreamExecutionEnvironment env = SignalJob.buildTopology(config);
        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();

        writeStreamNodes(outDir, streamGraph);
        writeJobVertices(outDir, jobGraph);
        writeJsonPlan(outDir, jobGraph);
        Files.writeString(outDir.resolve("startup-mode.txt"),
                config.startupMode().name() + "\n", StandardCharsets.UTF_8);

        LOG.info("jobgraph-dump: wrote stream-nodes.txt, job-vertices.txt, jobgraph.json to {}", outDir);
    }

    // Package-visible for JobGraphDumpTest (offline topology, no cluster).
    static void writeStreamNodes(Path outDir, StreamGraph graph) throws Exception {
        String content = graph.getStreamNodes().stream()
                .sorted(Comparator.comparingInt(StreamNode::getId))
                .map(JobGraphDump::formatStreamNode)
                .collect(Collectors.joining("\n")) + "\n";
        Files.writeString(outDir.resolve("stream-nodes.txt"), content, StandardCharsets.UTF_8);
    }

    // Package-visible for JobGraphDumpTest.
    static String formatStreamNode(StreamNode n) {
        // P2-072: the dump must prove what restore keys on — the transformation
        // UID (StreamNode#getId is allocation order, getOperatorName is
        // display-only). Fail like the UID test when an operator lacks an
        // explicit .uid(...).
        String uid = n.getTransformationUID();
        if (uid == null) {
            throw new IllegalStateException(
                    "stream node without transformation UID (restore contract "
                            + "requires explicit .uid()): id=" + n.getId()
                            + " name=" + n.getOperatorName());
        }
        return n.getId() + " | " + uid + " | " + n.getOperatorName();
    }

    // Package-visible for JobGraphDumpTest (offline topology, no cluster).
    static void writeJobVertices(Path outDir, JobGraph graph) throws Exception {
        java.util.List<JobVertex> vertices = new java.util.ArrayList<>();
        graph.getVertices().forEach(vertices::add);
        String content = vertices.stream()
                // P2-073: (name, id) sort — names duplicate after chaining
                // changes, and getVertices() order is undefined. The operators=
                // column carries the restore-relevant user UIDs; the vertex ID
                // itself is per-run random and proves nothing across runs.
                .sorted(Comparator.comparing(JobVertex::getName)
                        .thenComparing(v -> v.getID().toString()))
                .map(v -> v.getName() + " | " + v.getID() + " | parallelism=" + v.getParallelism()
                        + " | operators=" + operatorUids(v))
                .collect(Collectors.joining("\n")) + "\n";
        Files.writeString(outDir.resolve("job-vertices.txt"), content, StandardCharsets.UTF_8);
    }

    /** User-defined operator UIDs in a vertex ("&lt;generated&gt;" when Flink hashed one). */
    private static String operatorUids(JobVertex v) {
        return v.getOperatorIDs().stream()
                .map(p -> p.getUserDefinedOperatorUid() == null
                        ? "<generated>:" + p.getGeneratedOperatorID()
                        : p.getUserDefinedOperatorUid())
                .collect(Collectors.joining(","));
    }

    // Package-visible for JobGraphDumpTest (offline topology, no cluster).
    static void writeJsonPlan(Path outDir, JobGraph graph) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // P2-074: JsonPlanGenerator already returns serialized JSON (String) —
        // re-serializing double-encodes it into a quoted string. Pass through;
        // only serialize when a future Flink returns a Jackson tree instead.
        // (JsonPlanGenerator is @Internal — if it disappears, fall back to
        // env.getStreamGraph().getStreamingPlanAsJSON().)
        Object plan = JsonPlanGenerator.generatePlan(graph);
        String json = plan instanceof String s ? s : mapper.writeValueAsString(plan);
        Files.writeString(outDir.resolve("jobgraph.json"), json, StandardCharsets.UTF_8);
    }
}
