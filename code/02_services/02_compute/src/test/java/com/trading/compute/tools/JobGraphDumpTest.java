package com.trading.compute.tools;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Offline {@link JobGraphDump} artifact checks (P2-072/073/074): the writers run
 * against a locally built topology — no cluster, no execution — and prove the
 * dump carries the restore-relevant identities in a deterministic, valid form.
 */
@DisplayName("JobGraphDump offline artifacts (P2-072/073/074)")
class JobGraphDumpTest {

    @TempDir
    Path tmp;

    /** Minimal source → map topology with explicit UIDs. */
    private static StreamGraph topology() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Long> src =
                env.fromElements(1L, 2L, 3L).name("dump-src");
        SingleOutputStreamOperator<Long> mapped = src.map(x -> x).name("dump-map");
        src.uid("dump-src-uid");
        mapped.uid("dump-map-uid");
        return env.getStreamGraph();
    }

    @Test
    @DisplayName("stream nodes carry transformation UIDs (P2-072)")
    void streamNodesListTransformationUids() throws Exception {
        JobGraphDump.writeStreamNodes(tmp, topology());
        String content = Files.readString(tmp.resolve("stream-nodes.txt"), StandardCharsets.UTF_8);
        assertTrue(content.contains("dump-src-uid"), "source UID dumped:\n" + content);
        assertTrue(content.contains("dump-map-uid"), "map UID dumped:\n" + content);
        for (String line : content.strip().split("\n")) {
            assertTrue(line.matches("\\d+ \\| \\S+ \\| .+"),
                    "id | uid | name per line, got: " + line);
        }
    }

    @Test
    @DisplayName("stream node without UID fails loudly (P2-072)")
    void streamNodeWithoutUidFails() {
        // Directly constructed nodes carry no UID (normally Flink itself refuses
        // to build such a graph when auto-generated UIDs are off — the dump's
        // fail-loud check is defense-in-depth for that invariant).
        StreamNode uidLess = new StreamNode(1, "default", null,
                (org.apache.flink.streaming.api.operators.StreamOperatorFactory<?>) null, "uid-less-op",
                org.apache.flink.streaming.runtime.tasks.OneInputStreamTask.class);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> JobGraphDump.formatStreamNode(uidLess));
        assertTrue(e.getMessage().contains("without transformation UID"), e.getMessage());
    }

    @Test
    @DisplayName("vertices sorted by name with operator UIDs, not random IDs (P2-073)")
    void verticesSortedWithOperatorUids() throws Exception {
        JobGraphDump.writeJobVertices(tmp, topology().getJobGraph());
        String content = Files.readString(tmp.resolve("job-vertices.txt"), StandardCharsets.UTF_8);
        assertTrue(content.contains("operators="), "operators column present:\n" + content);
        assertTrue(content.contains("dump-src-uid"), "source UID in operators:\n" + content);
        assertTrue(content.contains("dump-map-uid"), "map UID in operators:\n" + content);
        assertTrue(content.contains("parallelism=1"), "parallelism kept:\n" + content);
        List<String> names = content.strip().lines()
                .map(l -> l.split(" \\| ")[0]).toList();
        for (int i = 1; i < names.size(); i++) {
            assertTrue(names.get(i - 1).compareTo(names.get(i)) <= 0,
                    "vertices sorted by name: " + names);
        }
    }

    @Test
    @DisplayName("plan is a JSON object, not a double-encoded string (P2-074)")
    void jsonPlanIsAnObject() throws Exception {
        JobGraphDump.writeJsonPlan(tmp, topology().getJobGraph());
        String content = Files.readString(tmp.resolve("jobgraph.json"), StandardCharsets.UTF_8);
        assertTrue(content.stripLeading().startsWith("{"),
                "plan must be an object, got: " + content.substring(0, Math.min(60, content.length())));
        assertTrue(content.contains("\"nodes\""), "plan carries nodes");
    }
}
