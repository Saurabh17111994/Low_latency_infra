package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** B4.2 signal-to-intent live E2E, HALTED-path (non-market half).
 *
 *  Producer side of the B4.2 chain against a live Fluss cluster (same
 *  env-gate convention as SignalChainLiveE2ETest: set FLUSS_BOOTSTRAP). No
 *  broker, no market hours: raw ticks are written directly to raw_table_1 with
 *  event-time timestamps that force the pinned 15 s event-time candle windows
 *  to close (the ingestion leg itself is SIGNAL-CHAIN-E2E's domain), and a
 *  rising price series deterministically fires the rule-v1 breakout signal
 *  detector (SIGNAL_LOOKBACK_CANDLES pinned to 2 so the run stays short). The
 *  real SignalJob topology then produces Signal_Candidates and -- only with
 *  EXECUTION_INTENT_ENABLED=true -- immutable Execution_Intent LOG rows.
 *
 *  Tests:
 *   1. enabled: candidates flow AND Execution_Intent grows with canonical
 *      immutable rows (request_hash non-blank, schema_version 1, side BUY,
 *      quantity 1, unique instruction_ids).
 *   2. disabled (default, fail-closed): the SAME signal flow writes zero
 *      intents -- the intent branch is absent at runtime, B4.1's unit
 *      guarantee proven live.
 */
@Tag("integration")
class B4SignalIntentE2ETest {
    private static final Logger LOG = LoggerFactory.getLogger(B4SignalIntentE2ETest.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    @DisplayName("B4-SIGNAL-INTENT-001: enabled -> Signal_Candidates + immutable Execution_Intent")
    void enabledSignalFlowProducesImmutableIntents() {
        run(true);
    }

    @Test
    @DisplayName("B4-SIGNAL-INTENT-002: disabled-by-default -> signals flow, zero intents")
    void disabledBranchWritesNoIntents() {
        run(false);
    }

    private void run(boolean intentsEnabled) {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live B4.2 signal-to-intent evidence");
        // Clock-derived token: unique per second per branch, so rows left by
        // earlier experiments on the same cluster can never collide with this
        // run's rows (the intent LOG is immutable and read from offset 0).
        long token = 1_000_000_000L + ((System.currentTimeMillis() / 1_000L) % 1_000_000_000L)
                + (intentsEnabled ? 0 : 1);
        String symbol = intentsEnabled ? "B4UP-EQ" : "B4OFF-EQ";
        long runStart = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            requirePlatformTables(conn);          // preflight parity: what the job will validate
            writeRisingRawSeries(conn, token, symbol);

            Map<String, String> env = jobEnv(bootstrap);
            if (intentsEnabled) {
                env.put("EXECUTION_INTENT_ENABLED", "true");
                env.put("CONFIGURATION_VERSION", "1.0.0");
                env.put("ACCOUNT_SCOPE_ID", "dev-scope");
                env.put("EXECUTION_PARTITION_ID", "dev-partition");
                env.put("EXECUTION_PRODUCT_TYPE", "CNC");
                env.put("EXECUTION_TIME_IN_FORCE", "DAY");
            }
            SignalJobConfig config = SignalJobConfig.from(env);
            assertEquals(intentsEnabled, config.executionIntentEnabled(),
                    "config must mirror the flag (fail-closed default false)");

            StreamExecutionEnvironment senv = SignalJob.buildTopology(config);
            JobClient job = senv.executeAsync("b4-signal-intent-" + (intentsEnabled ? "on" : "off"));
            try {
                awaitTrue(() -> {
                            try {
                                return statusIs(job, JobStatus.RUNNING);
                            } catch (Exception e) {
                                return false;
                            }
                        },
                        "SignalJob reaches RUNNING", 90);
                int candidates = 0;
                int intents = 0;
                List<GenericRow> intentRows = new ArrayList<>();
                for (int i = 0; i < 60; i++) {
                    Thread.sleep(3000);
                    candidates = countLogRows(conn, token, "Signal_Candidates",
                            SignalCandidatesTableColumns.INSTRUMENT_TOKEN,
                            SignalCandidatesTableColumns.DETECTION_TS, runStart);
                    intents = countLogRows(conn, token, "Execution_Intent",
                            ExecutionIntentTableColumns.INSTRUMENT_TOKEN,
                            ExecutionIntentTableColumns.CREATED_TS, runStart);
                    LOG.info("b4 sample {}: candidates={} intents={}", i, candidates, intents);
                    if (intentsEnabled && intents > 0) {
                        intentRows = readIntentRows(conn, token, runStart);
                    }
                    if (candidates > 0 && (!intentsEnabled || intents > 0)) {
                        break;
                    }
                }

                assertTrue(candidates > 0,
                        "Signal_Candidates must grow for the crafted breakout series"
                                + " (candidates=" + candidates + ")");

                if (intentsEnabled) {
                    assertTrue(intents > 0,
                            "Execution_Intent LOG must grow when EXECUTION_INTENT_ENABLED=true"
                                    + " (intents=" + intents + ", candidates=" + candidates + ")");
                    assertCanonicalIntents(intentRows, symbol);
                    // Immutability: a later sample must never shrink the LOG.
                    int later = countLogRows(conn, token, "Execution_Intent",
                            ExecutionIntentTableColumns.INSTRUMENT_TOKEN,
                            ExecutionIntentTableColumns.CREATED_TS, runStart);
                    assertTrue(later >= intents,
                            "immutable LOG must not shrink: " + intents + " -> " + later);
                } else {
                    assertEquals(0, intents,
                            "the intent branch is absent by default -- signals must NOT"
                                    + " produce Execution_Intent rows (fail-closed)");
                }
            } finally {
                try {
                    job.cancel();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        } catch (Exception e) {
            fail("b4 signal-to-intent E2E failed: " + e, e);
        }
    }

    // ---- crafted raw stream: 7 rising 15 s windows + a flush tick ----
    private static void writeRisingRawSeries(Connection conn, long token, String symbol)
            throws Exception {
        Table raw = conn.getTable(TablePath.of("default", "raw_table_1"));
        long base = (System.currentTimeMillis() / 15_000L) * 15_000L - 90_000L;
        int price = 1_000;
        AppendWriter w = raw.newAppend().createWriter();
        for (int win = 0; win < 7; win++) {
            for (int t = 0; t < 3; t++) {
                long eventTime = base + win * 15_000L + 4_000L + t * 2_000L;
                price += 10 + t;
                w.append(rawRow(token, symbol, eventTime, price))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
        // Flush tick 10 s into window 7: the watermark (maxTs-5000) then
        // passes the end of window 6, closing the candles the detector
        // consumes. The flush tick's own window 7 candle also forms.
        w.append(rawRow(token, symbol, base + 7 * 15_000L + 10_000L, price + 1))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        w.flush();
    }

    private static GenericRow rawRow(long token, String symbol, long eventTime, int price) {
        String fingerprint = "fp-" + token + "-" + eventTime;
        return GenericRow.of(
                bs(fingerprint), bs("1"), bs("e2e"), 1L, token, bs("NSE"), bs(symbol),
                eventTime, eventTime, eventTime, bs("T"), (long) price, 1L,
                new byte[] {1, 2}, bs("h-" + fingerprint), bs("1"), bs("v1"),
                bs("VALID"), bs("FRESH"), bs("2"));
    }

    // ---- fluss reads ----
    private static int countLogRows(Connection conn, long token, String tableName, int tokenCol,
                                    int tsCol, long minTs)
            throws Exception {
        int[] n = {0};
        scanLog(conn, tableName, row -> {
            if (!row.isNullAt(tokenCol) && row.getLong(tokenCol) == token
                    && !row.isNullAt(tsCol) && row.getLong(tsCol) >= minTs) {
                n[0]++;
            }
        });
        return n[0];
    }

    private static List<GenericRow> readIntentRows(Connection conn, long token, long minTs)
            throws Exception {
        List<GenericRow> out = new ArrayList<>();
        scanLog(conn, "Execution_Intent", row -> {
            if (!row.isNullAt(ExecutionIntentTableColumns.INSTRUMENT_TOKEN)
                    && row.getLong(ExecutionIntentTableColumns.INSTRUMENT_TOKEN) == token
                    && !row.isNullAt(ExecutionIntentTableColumns.CREATED_TS)
                    && row.getLong(ExecutionIntentTableColumns.CREATED_TS) >= minTs) {
                out.add((GenericRow) row);
            }
        });
        return out;
    }

    private static void scanLog(Connection conn, String tableName,
                                java.util.function.Consumer<InternalRow> fn) throws Exception {
        Table table = conn.getTable(TablePath.of("default", tableName));
        TableInfo info = table.getTableInfo();
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
                scanner.subscribe(bucket, 0L);
            }
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofMillis(500));
                if (records == null || records.isEmpty()) {
                    break;
                }
                for (var r : records) {
                    fn.accept(r.getRow());
                }
            }
        }
    }

    private static void requirePlatformTables(Connection conn) throws Exception {
        String[] names = {"raw_table_1", "feature_candles_15s", "forming_bar",
                "Signal_Candidates", "Signal_Candidates_current", "Position_State",
                "Trade_Decisions", "Execution_Intent"};
        for (String n : names) {
            conn.getTable(TablePath.of("default", n));
        }
    }

    // ---- assertions ----
    private static void assertCanonicalIntents(List<GenericRow> rows, String symbol) {
        assertFalse(rows.isEmpty(), "intent rows must be readable back from the LOG");
        Set<String> ids = new HashSet<>();
        for (InternalRow r : rows) {
            String instructionId = r.getString(ExecutionIntentTableColumns.INSTRUCTION_ID).toString();
            String requestHash = r.getString(ExecutionIntentTableColumns.REQUEST_HASH).toString();
            String schemaVersion = r.getString(ExecutionIntentTableColumns.SCHEMA_VERSION).toString();
            String side = r.getString(ExecutionIntentTableColumns.SIDE).toString();
            String sym = r.getString(ExecutionIntentTableColumns.SYMBOL).toString();
            assertTrue(ids.add(instructionId), "instruction_id must be unique: " + instructionId);
            assertFalse(requestHash.isBlank(), "request_hash must be non-blank");
            assertEquals("1", schemaVersion, "canonical schema_version 1");
            assertEquals(symbol, sym, "symbol carried through");
            assertEquals("BUY", side, "crafted breakout side BUY");
            assertEquals(1L, r.getLong(ExecutionIntentTableColumns.QUANTITY),
                    "quantity 1 (safe-instrument style)");
            assertEquals("dev-scope",
                    r.getString(ExecutionIntentTableColumns.ACCOUNT_SCOPE_ID).toString(),
                    "account scope from config");
            assertEquals("dev-partition",
                    r.getString(ExecutionIntentTableColumns.EXECUTION_PARTITION_ID).toString(),
                    "partition from config");
        }
    }

    // ---- job env (SIGNAL_CHAIN_E2E pattern, intent branch added) ----
    private static Map<String, String> jobEnv(String bootstrap) {
        Map<String, String> e = new HashMap<>();
        e.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        e.put("FLUSS_DATABASE", "default");
        e.put("RAW_TABLE", "raw_table_1");
        e.put("CANDLE_TABLE", "feature_candles_15s");
        e.put("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates");
        e.put("SIGNAL_CURRENT_TABLE", "Signal_Candidates_current");
        e.put("DEDUP_TTL_MS", "60000");
        e.put("CANDLE_WINDOW_MS", "15000");
        e.put("CHECKPOINT_INTERVAL_MS", "10000");
        e.put("CHECKPOINT_TIMEOUT_MS", "30000");
        e.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        e.put("ALLOW_FULL_REPLAY", "true");
        e.put("CHECKPOINT_DIR",
                "file:///tmp/b4-signal-intent-e2e-checkpoints-" + System.nanoTime());
        e.put("SIGNAL_LOOKBACK_CANDLES", "2");
        for (String k : new String[] {"FORMING_RULE_ID", "FORMING_BAR_TABLE",
                "TRADE_DECISIONS_TABLE", "SIGNAL_STRATEGY_ID", "SIGNAL_STRATEGY_VERSION",
                "SIGNAL_RULE_ID", "FORMING_BAR_WRITE_BATCH_MS", "STATE_BACKEND",
                "STATE_BACKEND_LOCAL_DIRS", "TASK_MANAGER_MEMORY_MANAGED_SIZE",
                "TASK_MANAGER_NETWORK_MEMORY_MAX", "PARALLELISM", "OTEL_COLLECTOR_HOST"}) {
            String v = System.getenv().get(k);
            if (v != null && !v.isBlank()) {
                e.put(k, v);
            }
        }
        return e;
    }

    private static BinaryString bs(String s) {
        return s == null ? null : BinaryString.fromString(s);
    }

    private static boolean statusIs(JobClient job, JobStatus want) throws Exception {
        return job.getJobStatus().get(10, TimeUnit.SECONDS) == want;
    }

    private static void awaitTrue(java.util.function.BooleanSupplier check, String what, int secs)
            throws Exception {
        long deadline = System.currentTimeMillis() + secs * 1000L;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (check.getAsBoolean()) {
                    return;
                }
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(1000);
        }
        fail("timeout waiting for " + what + (last == null ? "" : ": " + last));
    }
}
