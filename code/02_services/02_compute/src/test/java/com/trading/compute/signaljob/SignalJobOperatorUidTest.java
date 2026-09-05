package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Operator-identity pin for checkpoint-restore (CHECKPOINT-RESTORE-001,
 * DEC-035): every operator in {@link SignalJob#buildTopology} carries an
 * explicit transformation UID, so Flink derives the restore anchor from the
 * UID instead of the transitive topology hash.
 *
 * <p>Why this test exists: the 2026-08-13 rescope JobGraphDump proved that
 * hash-derived operator IDs drift whenever a chained operator is added or
 * removed (baseline {@code candle-15s -> canonical-candle-filter} chaining,
 * then {@code forming-bar-detection -> canonical-signal-filter} chaining) — and
 * the drift propagates downstream transitively. Only explicit UIDs make the
 * stateful anchors durable across such topology changes. Removing a
 * {@code .uid(...)} from the graph — or renaming one — must fail here.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("SignalJob operator UIDs (CHECKPOINT-RESTORE-001, DEC-035)")
class SignalJobOperatorUidTest {

    /**
     * operator uid -> expected vertex-name prefix. Covers the whole graph:
     * the source (offset state), every stateful processor (dedup TTL state,
     * window state, signal keyed state), the filter, and all sinks.
     */
    private static final Map<String, String> EXPECTED_OPERATORS = new LinkedHashMap<>();
    static {
        EXPECTED_OPERATORS.put("raw-table-1", "Source: raw-table-1");
        EXPECTED_OPERATORS.put("raw-validation", "raw-validation");
        EXPECTED_OPERATORS.put("fingerprint-dedup-v2", "fingerprint-dedup");
        // Per-tick ingest->monitor age observability (2026-09-03, 2aef756):
        // non-keyed identity map on the deduped stream, no state. Unconditional.
        EXPECTED_OPERATORS.put("ingest-latency-monitor", "ingest-latency-monitor");
        // NOTE (cutover 2026-09-05, batch 3): the 15 s candle path, the
        // forming-bar stack, the position-state gate, and the old signal
        // sinks are retired — no uids for them exist. Signals flow
        // raw ticks -> dedup -> multi-TF candles -> strategy host.
    }

    /**
     * Multi-TF branch UIDs (Phase 4, 2026-09-05) — present only when
     * MULTITF_ENABLED=true. Pinned so a rename fails closed; absent when
     * disabled so the old path stays byte-identical.
     */
    private static final Map<String, String> EXPECTED_MULTITF_OPERATORS = new LinkedHashMap<>();
    static {
        EXPECTED_MULTITF_OPERATORS.put("multi-tf-aggregator-v1", "multi-tf-aggregator");
        EXPECTED_MULTITF_OPERATORS.put("candle-live-sink", "candle-live-sink");
        EXPECTED_MULTITF_OPERATORS.put("candle-closed-first-write-wins", "candle-closed-first-write-wins");
        EXPECTED_MULTITF_OPERATORS.put("candle-closed-sink", "candle-closed-sink");
        // N7 retired (2026-09-05 cutover, batch 2): n7-signal-v1, the
        // multitf-*-sinks, and canonical-signal-filter-multitf are gone —
        // N7 runs ONLY as a host strategy with identical candidate ids.
    }

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            ScratchTables.dropCreated(admin, TIMEOUT);
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("every operator carries its pinned UID; UID set equals the contract exactly (multitf disabled)")
    void everyOperatorCarriesPinnedUid() throws Exception {
        assertTopology(false);
    }

    @Test
    @DisplayName("multitf branch adds its pinned UIDs when MULTITF_ENABLED=true (Phase 4)")
    void multitfBranchAddsPinnedUidsWhenEnabled() throws Exception {
        assertTopology(true, false);
    }

    /**
     * Strategy-host branch UIDs (2026-09-05) — present only when
     * MULTITF_ENABLED=true (the host reads the multi-TF streams) AND
     * STRATEGY_HOST_ENABLED=true. Pinned so a rename fails closed; absent
     * otherwise so the graph stays byte-identical.
     */
    private static final Map<String, String> EXPECTED_STRATEGY_HOST_OPERATORS =
            new LinkedHashMap<>();
    static {
        EXPECTED_STRATEGY_HOST_OPERATORS.put("strategy-host-v1", "strategy-host");
        EXPECTED_STRATEGY_HOST_OPERATORS.put(
                "strategy-host-candidates-sink", "strategy-host-candidates-sink");
        EXPECTED_STRATEGY_HOST_OPERATORS.put(
                "strategy-host-candidates-current-sink", "strategy-host-candidates-current-sink");
        // Note: canonical-signal-filter-strategy-host (the filter before the
        // KV sink) follows the multitf precedent — asserted present via
        // containsKey, not pinned in the required set.
    }

    @Test
    @DisplayName("strategy-host branch adds its pinned UIDs when STRATEGY_HOST_ENABLED=true")
    void strategyHostBranchAddsPinnedUidsWhenEnabled() throws Exception {
        assertTopology(true, true);
    }

    private void assertTopology(boolean multiTfEnabled) throws Exception {
        assertTopology(multiTfEnabled, false);
    }

    private void assertTopology(boolean multiTfEnabled, boolean hostEnabled) throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        String candleName = "p6_uid_" + suffix + "_candle";
        String signalName = "p6_uid_" + suffix + "_sig";
        String currentName = "p6_uid_" + suffix + "_cur";
        String quarName = "p6_uid_" + suffix + "_quar";
        ScratchTables.create(connection, admin, candleName, ScratchTables.candleSchema(),
                List.of("instrument_token", "window_start"), 16, "candle KV", TIMEOUT);
        ScratchTables.create(connection, admin, signalName, ScratchTables.signalLogSchema(), null,
                16, "signal LOG", TIMEOUT);
        ScratchTables.create(connection, admin, currentName,
                ScratchTables.signalCurrentSchema(), List.of("instrument_token"), 16,
                "signal current KV", TIMEOUT);
        ScratchTables.create(connection, admin, quarName,
                ScratchTables.ingestionQuarantineSchema(), null, 16,
                "quarantine LOG", TIMEOUT);
        // When multi-TF is enabled, also create its tables so preflight succeeds
        String liveName = null;
        String closedName = null;
        if (multiTfEnabled) {
            liveName = "p6_uid_" + suffix + "_live";
            closedName = "p6_uid_" + suffix + "_closed";
            ScratchTables.create(connection, admin, liveName, scratchCandleLiveSchema(),
                    List.of("instrument_token", "tf", "window_start"), 16, "candle_live KV", TIMEOUT);
            ScratchTables.create(connection, admin, closedName, scratchCandleClosedSchema(),
                    List.of("instrument_token", "tf", "window_start"), 16, "candle_closed KV", TIMEOUT);
        }
        // buildTopology preflights the table contracts against live metadata.
        // The scratch tables carry the DEC-035 contracts that the dev cluster's
        // legacy tables only gain in Stage 6 (live DDL application), so the
        // UID assertions never depend on Stage 6 having landed.
        Map<String, String> cfg = env();
        cfg.put("CANDLE_TABLE", candleName);
        cfg.put("SIGNAL_CANDIDATES_TABLE", signalName);
        cfg.put("SIGNAL_CURRENT_TABLE", currentName);
        cfg.put("QUARANTINE_TABLE", quarName);
        if (multiTfEnabled) {
            cfg.put("MULTITF_ENABLED", "true");
            cfg.put("CANDLE_LIVE_TABLE", liveName);
            cfg.put("CANDLE_CLOSED_TABLE", closedName);
        }
        if (hostEnabled) {
            cfg.put("STRATEGY_HOST_ENABLED", "true");
            cfg.put("STRATEGIES", N7RangeBreakoutStrategy.RULE_ID);
        }
        StreamExecutionEnvironment senv = SignalJob.buildTopology(SignalJobConfig.from(cfg));
        StreamGraph graph = senv.getStreamGraph();

        Map<String, String> uidToName = new HashMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String uid = node.getTransformationUID();
            assertNotNull(uid,
                    "operator '" + node.getOperatorName() + "' has NO explicit UID — "
                            + "its checkpoint-restore anchor would drift with topology changes "
                            + "(CHECKPOINT-RESTORE-001)");
            String previous = uidToName.put(uid, node.getOperatorName());
            assertTrue(previous == null,
                    "duplicate UID '" + uid + "' on '" + previous + "' and '"
                            + node.getOperatorName() + "'");
        }

        for (Map.Entry<String, String> expected : EXPECTED_OPERATORS.entrySet()) {
            String actualName = uidToName.get(expected.getKey());
            assertNotNull(actualName,
                    "UID '" + expected.getKey() + "' is missing from the graph");
            assertTrue(actualName.startsWith(expected.getValue()),
                    "UID '" + expected.getKey() + "' is on unexpected operator '"
                            + actualName + "'");
        }
        if (!multiTfEnabled) {
            for (String mtUid : EXPECTED_MULTITF_OPERATORS.keySet()) {
                assertTrue(!uidToName.containsKey(mtUid),
                        "MULTITF_ENABLED=false: multi-TF UID '" + mtUid + "' must be absent — "
                                + "topology must be byte-identical to baseline (Phase 4)");
            }
            assertEquals(EXPECTED_OPERATORS.size(), uidToName.size(),
                    "MULTITF_ENABLED=false: graph carries operators outside the pinned UID set");
            for (String hostUid : EXPECTED_STRATEGY_HOST_OPERATORS.keySet()) {
                assertTrue(!uidToName.containsKey(hostUid),
                        "MULTITF_ENABLED=false: host UID '" + hostUid + "' must be absent");
            }
        } else {
            for (Map.Entry<String, String> expected : EXPECTED_MULTITF_OPERATORS.entrySet()) {
                String actualName = uidToName.get(expected.getKey());
                assertNotNull(actualName,
                        "MULTITF_ENABLED=true: multi-TF UID '" + expected.getKey() + "' is missing");
                assertTrue(actualName.startsWith(expected.getValue()),
                        "MULTITF_ENABLED=true: UID '" + expected.getKey() + "' on unexpected operator '"
                                + actualName + "'");
            }
            assertTrue(!uidToName.containsKey("n7-signal-v1"),
                    "n7-signal-v1 is retired — N7 runs only on the host");
            assertTrue(!uidToName.containsKey("multitf-signal-candidates-sink"),
                    "multitf-*-sinks are retired with n7-signal-v1");
            assertTrue(!uidToName.containsKey("canonical-signal-filter-multitf"),
                    "canonical-signal-filter-multitf is retired with n7-signal-v1");
            assertTrue(uidToName.containsKey("candle-closed-first-write-wins"),
                    "MULTITF_ENABLED=true: closed first-write-wins must be present");
            int expectedTotal = EXPECTED_OPERATORS.size() + EXPECTED_MULTITF_OPERATORS.size();
            if (hostEnabled) {
                for (Map.Entry<String, String> expected : EXPECTED_STRATEGY_HOST_OPERATORS.entrySet()) {
                    String actualName = uidToName.get(expected.getKey());
                    assertNotNull(actualName,
                            "STRATEGY_HOST_ENABLED=true: host UID '" + expected.getKey() + "' is missing");
                    assertTrue(actualName.startsWith(expected.getValue()),
                            "STRATEGY_HOST_ENABLED=true: UID '" + expected.getKey() + "' on unexpected operator '"
                                    + actualName + "'");
                }
                assertTrue(uidToName.containsKey("canonical-signal-filter-strategy-host"),
                        "STRATEGY_HOST_ENABLED=true: expected host KV filter UID "
                                + "'canonical-signal-filter-strategy-host'");
                expectedTotal += EXPECTED_STRATEGY_HOST_OPERATORS.size() + 1; // +1 for the host KV filter
            } else {
                for (String hostUid : EXPECTED_STRATEGY_HOST_OPERATORS.keySet()) {
                    assertTrue(!uidToName.containsKey(hostUid),
                            "STRATEGY_HOST_ENABLED=false: host UID '" + hostUid + "' must be absent — "
                                    + "topology must be byte-identical to baseline");
                }
                assertTrue(!uidToName.containsKey("canonical-signal-filter-strategy-host"),
                        "STRATEGY_HOST_ENABLED=false: host KV filter must be absent");
            }
            assertEquals(expectedTotal, uidToName.size(),
                    "MULTITF_ENABLED=true: graph UID set must be exactly baseline + multi-TF branch");
        }
        assertTrue(!uidToName.containsKey("execution-intent-producer"),
                "EXECUTION_INTENT_ENABLED defaults false; the executable-intent branch must be absent");
        assertTrue(!uidToName.containsKey("execution-intent-sink"),
                "EXECUTION_INTENT_ENABLED defaults false; the executable-intent sink must be absent");
    }

    private static org.apache.fluss.metadata.Schema scratchCandleLiveSchema() {
        return org.apache.fluss.metadata.Schema.newBuilder()
                .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
                .column("exchange", org.apache.fluss.types.DataTypes.STRING())
                .column("symbol", org.apache.fluss.types.DataTypes.STRING())
                .column("tf", org.apache.fluss.types.DataTypes.STRING())
                .column("window_start", org.apache.fluss.types.DataTypes.BIGINT())
                .column("window_end", org.apache.fluss.types.DataTypes.BIGINT())
                .column("open_paise", org.apache.fluss.types.DataTypes.BIGINT())
                .column("high_paise", org.apache.fluss.types.DataTypes.BIGINT())
                .column("low_paise", org.apache.fluss.types.DataTypes.BIGINT())
                .column("close_paise", org.apache.fluss.types.DataTypes.BIGINT())
                .column("volume", org.apache.fluss.types.DataTypes.BIGINT())
                .column("tick_count", org.apache.fluss.types.DataTypes.INT())
                .column("last_event_time", org.apache.fluss.types.DataTypes.BIGINT())
                .column("last_event_fingerprint", org.apache.fluss.types.DataTypes.STRING())
                .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
                .primaryKey("instrument_token", "tf", "window_start")
                .build();
    }

    private static org.apache.fluss.metadata.Schema scratchCandleClosedSchema() {
        return scratchCandleLiveSchema();
    }

    private static Map<String, String> env() {
        // Gated runner (P6 recipe, inside 01_docker_trading-net) overrides the
        // cluster + scratch tables via the process env; plain `mvn test` skips
        // this class entirely (@EnabledIfEnvironmentVariable).
        Map<String, String> env = new HashMap<>();
        env.put("FLUSS_BOOTSTRAP_SERVERS",
                System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123"));
        env.put("RAW_TABLE", System.getenv().getOrDefault("RAW_TABLE", "raw_table_1"));
        env.put("CANDLE_TABLE",
                System.getenv().getOrDefault("CANDLE_TABLE", "feature_candles_15s"));
        env.put("SIGNAL_CANDIDATES_TABLE",
                System.getenv().getOrDefault("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates"));
        env.put("SIGNAL_CURRENT_TABLE",
                System.getenv().getOrDefault("SIGNAL_CURRENT_TABLE", "Signal_Candidates_current"));
        env.put("DEDUP_WINDOW_ENTRIES", "2000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }
}
