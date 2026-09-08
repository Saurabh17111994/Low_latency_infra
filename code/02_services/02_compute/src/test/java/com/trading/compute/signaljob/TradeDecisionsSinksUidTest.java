package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.data.RowData;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * SCH-19 dual-sink UID pin (CHECKPOINT-RESTORE-001 habit): the two sinks
 * {@link TradeDecisionsSinks#attach} wires — the {@code Trade_Decisions} LOG
 * and the {@code trade_instruction_state} KV index — plus the index mapper,
 * must carry pinned UIDs so their checkpoint-restore anchors survive topology
 * changes when the dual-sink is wired into the live graph with the ranking
 * feed.
 *
 * <p>Graph construction only — the graph is never executed. FlussSink.build()
 * opens the writer client's admin connection to look up the table (verified:
 * building a sink with an unreachable bootstrap fails here), so the test is
 * gated on the P6 evidence runner exactly like
 * {@link SignalJobOperatorUidTest} — plain {@code mvn test} skips it.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("SCH-19: TradeDecisionsSinks dual-sink UID pin")
class TradeDecisionsSinksUidTest {

    private static String bootstrap;

    @BeforeAll
    static void connect() {
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            try (Connection connection = ConnectionFactory.createConnection(conf)) {
                connection.getAdmin().listDatabases().get(); // reachability probe
            }
        } catch (Exception e) {
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @Test
    @DisplayName("attach wires exactly the three pinned UIDs onto the decision stream")
    void attachPinsDualSinkUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> decisions = env.fromCollection(
                List.<RowData>of(), TradeDecisionsTableColumns.ROW_TYPE_INFO)
                .name("decisions-source")
                .uid("decisions-source");
        TradeDecisionsSinks.attach(decisions, SignalJobConfig.from(env()));

        StreamGraph graph = env.getStreamGraph();
        Map<String, String> uidToName = new HashMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String uid = node.getTransformationUID();
            assertNotNull(uid,
                    "operator '" + node.getOperatorName() + "' has NO explicit UID — "
                            + "its checkpoint-restore anchor would drift (CHECKPOINT-RESTORE-001)");
            String previous = uidToName.put(uid, node.getOperatorName());
            assertTrue(previous == null,
                    "duplicate UID '" + uid + "' on '" + previous + "' and '"
                            + node.getOperatorName() + "'");
        }

        assertEquals(5, uidToName.size(),
                "attach must wire exactly the source + LOG sink + FWW filter + index mapper + KV index sink");
        assertTrue(uidToName.get("trade-decisions-sink").startsWith("trade-decisions-sink"),
                "LOG sink UID must anchor the immutable instruction sink");
        assertTrue(uidToName.get("trade-instruction-state-first-write-wins")
                        .startsWith("trade-instruction-state-first-write-wins"),
                "FWW filter UID must anchor the instruction-state replay guard");
        assertTrue(uidToName.get("trade-instruction-index-map").startsWith("trade-instruction-index-map"),
                "index mapper UID must anchor the hash-recompute operator");
        assertTrue(uidToName.get("trade-instruction-state-sink").startsWith("trade-instruction-state-sink"),
                "KV index sink UID must anchor the instruction-state upsert");
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }
}
