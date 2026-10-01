package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        // Wave C W-C5a (DEC-059 end state): the legacy candle sinks
        // (candle-live-sink, candle-closed-first-write-wins, candle-closed-sink)
        // are retired — the host is the only candle writer; its merged sink is
        // pinned in the host set below.
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
        // Wave C W-C5a (DEC-059): the merged candle+feature sink — the host is
        // the only candle writer.
        EXPECTED_STRATEGY_HOST_OPERATORS.put(
                "candle-features-sink-v1", "candle-features-sink");
        // Note: canonical-signal-filter-strategy-host (the filter before the
        // KV sink) follows the multitf precedent — asserted present via
        // containsKey, not pinned in the required set.
    }

    @Test
    @DisplayName("strategy-host branch adds its pinned UIDs when STRATEGY_HOST_ENABLED=true")
    void strategyHostBranchAddsPinnedUidsWhenEnabled() throws Exception {
        assertTopology(true, true);
    }

    @Test
    @DisplayName("CHG-505: the market-tick flag adds no operator — the UID set is unchanged")
    void marketTickFlagAddsNoOperator() throws Exception {
        // The market-only row rides the existing fast side output; with the
        // flag on the graph must still satisfy the exact same UID contract
        // (this helper asserts every node has an explicit UID and the set
        // equals baseline + multi-TF + host).
        assertTopology(true, true, true);
    }

    @Test
    @DisplayName("legacy candle sink UIDs are retired; the merged sink carries the write (W-C5a)")
    void legacyCandleSinkUidsAreRetired() throws Exception {
        // Wave C W-C5a (DEC-059 end state): the guarded legacy sinks and their
        // guard filters are gone for good, and the end-state graph builds
        // without the retired candle tables existing anywhere (candle_live/
        // candle_closed were dropped on dev in W-C7 — this leg proves no code
        // path needs them; candle_features is the single live candle table).
        String suffix = String.valueOf(System.nanoTime());
        String candleName = "p6_uid_" + suffix + "_candle";
        String signalName = "p6_uid_" + suffix + "_sig";
        String currentName = "p6_uid_" + suffix + "_cur";
        String quarName = "p6_uid_" + suffix + "_quar";
        String mergedName = "p6_uid_" + suffix + "_merged";
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
        ScratchTables.create(connection, admin, mergedName, scratchMergedCandleSchema(),
                List.of("instrument_token", "tf", "window_start"), 16, "candle_features KV",
                TIMEOUT);

        Map<String, String> cfg = env();
        cfg.put("CANDLE_TABLE", candleName);
        cfg.put("SIGNAL_CANDIDATES_TABLE", signalName);
        cfg.put("SIGNAL_CURRENT_TABLE", currentName);
        cfg.put("QUARANTINE_TABLE", quarName);
        cfg.put("MULTITF_ENABLED", "true");
        cfg.put("STRATEGY_HOST_ENABLED", "true");
        cfg.put("STRATEGIES", N7RangeBreakoutStrategy.RULE_ID);
        cfg.put("MERGED_CANDLE_TABLE", mergedName);
        cfg.put("CANDLE_CONTEXT_TABLE", mergedName);

        StreamExecutionEnvironment senv = SignalJob.buildTopology(SignalJobConfig.from(cfg));
        StreamGraph graph = senv.getStreamGraph();
        Map<String, String> uidToName = new HashMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String uid = node.getTransformationUID();
            assertNotNull(uid, "operator '" + node.getOperatorName() + "' has NO explicit UID");
            uidToName.put(uid, node.getOperatorName());
        }

        for (String retired : List.of("candle-live-sink", "candle-closed-sink",
                "candle-closed-first-write-wins", "candle-live-legacy-guard",
                "candle-closed-legacy-guard")) {
            assertFalse(uidToName.containsKey(retired),
                    "retired legacy candle UID still present: " + retired);
        }
        assertTrue(uidToName.containsKey("candle-features-sink-v1"),
                "the merged sink carries the candle write (DEC-059 end state)");
    }

    @Test
    @DisplayName("merged table contract violation fails the build (W-C3 preflight)")
    void mergedTableContractViolationFailsBuild() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        String candleName = "p6_uid_" + suffix + "_candle";
        String signalName = "p6_uid_" + suffix + "_sig";
        String currentName = "p6_uid_" + suffix + "_cur";
        String quarName = "p6_uid_" + suffix + "_quar";
        String badMergedName = "p6_uid_" + suffix + "_merged_bad";
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
        // 15 columns — deliberately NOT the DDL 35 merged contract.
        ScratchTables.create(connection, admin, badMergedName, scratchCandleClosedSchema(),
                List.of("instrument_token", "tf", "window_start"), 16, "candle_features KV",
                TIMEOUT);

        Map<String, String> cfg = env();
        cfg.put("CANDLE_TABLE", candleName);
        cfg.put("SIGNAL_CANDIDATES_TABLE", signalName);
        cfg.put("SIGNAL_CURRENT_TABLE", currentName);
        cfg.put("QUARANTINE_TABLE", quarName);
        cfg.put("MULTITF_ENABLED", "true");
        cfg.put("MERGED_CANDLE_TABLE", badMergedName);

        assertThrows(TableContractValidator.ContractViolation.class,
                () -> SignalJob.buildTopology(SignalJobConfig.from(cfg)),
                "the 15-column table must be refused by the merged preflight");
    }

    private void assertTopology(boolean multiTfEnabled) throws Exception {
        assertTopology(multiTfEnabled, false, false);
    }

    private void assertTopology(boolean multiTfEnabled, boolean hostEnabled) throws Exception {
        assertTopology(multiTfEnabled, hostEnabled, false);
    }

    private void assertTopology(boolean multiTfEnabled, boolean hostEnabled,
            boolean marketTickEnabled) throws Exception {
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
        // Wave C W-C5a (DEC-059 end state): no legacy candle scratch tables —
        // the multi-TF branch preflights only the merged candle_features table
        // (the real dev table; read-only metadata check).
        Map<String, String> cfg = env();
        cfg.put("CANDLE_TABLE", candleName);
        cfg.put("SIGNAL_CANDIDATES_TABLE", signalName);
        cfg.put("SIGNAL_CURRENT_TABLE", currentName);
        cfg.put("QUARANTINE_TABLE", quarName);
        if (multiTfEnabled) {
            cfg.put("MULTITF_ENABLED", "true");
        }
        if (hostEnabled) {
            cfg.put("STRATEGY_HOST_ENABLED", "true");
            cfg.put("STRATEGIES", N7RangeBreakoutStrategy.RULE_ID);
        }
        if (marketTickEnabled) {
            cfg.put("STRATEGY_MARKET_TICK_ENABLED", "true");
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

    /** DDL 35 shape (the only candle table, W-C5a): 15 candle columns + features + sealed. */
    private static org.apache.fluss.metadata.Schema scratchMergedCandleSchema() {
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
                .column("features", org.apache.fluss.types.DataTypes.MAP(
                        org.apache.fluss.types.DataTypes.INT(),
                        org.apache.fluss.types.DataTypes.DOUBLE()))
                .column("sealed", org.apache.fluss.types.DataTypes.BOOLEAN())
                .primaryKey("instrument_token", "tf", "window_start")
                .build();
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
