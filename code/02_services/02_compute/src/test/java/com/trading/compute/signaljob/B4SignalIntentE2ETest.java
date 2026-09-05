package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
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
 *  crafted narrowing-range series deterministically fires the N7
 *  range-breakout rule on the strategy host (n7-range-breakout-v1, the sole
 *  signal producer since the 15 s chain was retired 2026-09-05). The real
 *  SignalJob topology then produces Signal_Candidates and -- only with
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
    // IST day bucket: matches the ingestion contract (event_day from
    // event_time, Asia/Kolkata) and the auto-partition naming rule.
    private static final DateTimeFormatter EVENT_DAY_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("Asia/Kolkata"));

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
                // Write AFTER the job runs: default startup is LATEST, so the
                // source snapshots the log end at startup and only rows
                // appended after that flow through. (ALLOW_FULL_REPLAY would
                // force a full-log drain first — tens of millions of rows on
                // a lived-in cluster — and the tail never arrives in budget.
                // Replay semantics belong to RawSourceOffsetSelectionTest.)
                // Two-burst write (N7 only evaluates against a WARM 7-candle
                // ring: one-shot bursts never arm). Burst 1 closes 7
                // narrowing 15 s candles; burst 2 breaks the setup high.
                int[] price = {10_000};
                // One base for both bursts: a 15 s boundary between them
                // must not shift burst 2 into unintended windows.
                long base = (System.currentTimeMillis() / 15_000L) * 15_000L - 90_000L;
                writeBurst1(conn, token, symbol, price, base);
                awaitCandles(conn, token, 7);
                writeBurst2(conn, token, symbol, price, base);
                int candidates = 0;
                int intents = 0;
                List<GenericRow> intentRows = new ArrayList<>();
                // Phase 1 — smoke (90 s): the crafted series must yield at
                // least one candidate fast. A failure here names the dead
                // leg instead of burning the full soak budget: candles == 0
                // means the rows never reached the candle leg (partition,
                // offsets, validation); candles > 0 with no candidates
                // means the detector/canonical leg dropped them.
                int candles = 0;
                boolean smoked = false;
                for (int i = 0; i < 30; i++) {
                    Thread.sleep(3000);
                    candidates = countLogRows(conn, token, "Signal_Candidates",
                            SignalCandidatesTableColumns.INSTRUMENT_TOKEN,
                            SignalCandidatesTableColumns.DETECTION_TS, runStart);
                    if (i % 5 == 4) {
                        candles = countCandles(conn, token);
                    }
                    LOG.info("b4 smoke {}: candidates={} candles={}", i, candidates, candles);
                    if (candidates > 0) {
                        smoked = true;
                        break;
                    }
                }
                assertTrue(smoked,
                        "smoke: no Signal_Candidates row for the crafted series in 90 s"
                                + " (candles=" + candles + " for token=" + token + ")");
                // Phase 2 — soak (90 s): intents + immutability on the rest
                // of the budget. Only reached when the smoke passed.
                for (int i = 0; i < 30; i++) {
                    Thread.sleep(3000);
                    candidates = countLogRows(conn, token, "Signal_Candidates",
                            SignalCandidatesTableColumns.INSTRUMENT_TOKEN,
                            SignalCandidatesTableColumns.DETECTION_TS, runStart);
                    intents = countLogRows(conn, token, "Execution_Intent",
                            ExecutionIntentTableColumns.INSTRUMENT_TOKEN,
                            ExecutionIntentTableColumns.CREATED_TS, runStart);
                    LOG.info("b4 soak {}: candidates={} intents={}", i, candidates, intents);
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

    // ---- crafted raw stream (two bursts): 7 narrowing 15 s windows + a breakout ----
    // Burst 1: windows 0..6 in full plus the first two ticks of window 7.
    // Window i has range (7 - i) paise on a rising base, so the N7 ring is
    // strictly narrowing and window 6 arms the setup. The window-7 ticks
    // push the watermark (maxTs-5000) past the end of window 6, so all
    // seven closed candles form and the host strategy's rings fill.
    private static void writeBurst1(Connection conn, long token, String symbol, int[] price, long base)
            throws Exception {
        Table raw = conn.getTable(TablePath.of("default", "raw_table_1"));
        AppendWriter w = raw.newAppend().createWriter();
        for (int win = 0; win < 7; win++) {
            int low = price[0] + win * 10;
            int high = low + (7 - win);
            long eventTime = base + win * 15_000L + 4_000L;
            w.append(rawRow(token, symbol, eventTime, low))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            w.append(rawRow(token, symbol, eventTime + 2_000L, high))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            w.append(rawRow(token, symbol, eventTime + 4_000L, low + 1))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        price[0] += 70;
        for (int t = 0; t < 2; t++) {
            long eventTime = base + 7 * 15_000L + 4_000L + t * 2_000L;
            w.append(rawRow(token, symbol, eventTime, price[0] + t))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        w.flush();
    }

    // Block until the token owns `want` closed FIFTEEN_S candles (proves the
    // N7 ring can only be warm: every closed candle whose window ended before
    // the current forming window is admitted). Fails fast instead of letting
    // the smoke loop burn its budget on a burst the strategy cannot evaluate.
    private static void awaitCandles(Connection conn, long token, int want) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        int n = 0;
        while (System.currentTimeMillis() < deadline) {
            n = countCandles(conn, token);
            if (n >= want) {
                LOG.info("b4 burst1: candles={} (want {})", n, want);
                return;
            }
            Thread.sleep(3000);
        }
        fail("burst 1 never closed its candles (candles=" + n + " want=" + want
                + " token=" + token + "): rows are not reaching the candle leg");
    }

    // Burst 2: window 7 closes with a high above the window-6 setup high,
    // plus a flush tick 10 s into window 8. The N7 ring is warm by
    // construction now, so the window-7 close fires deterministically
    // (BUY: closed high breaks the armed setup high).
    private static void writeBurst2(Connection conn, long token, String symbol, int[] price, long base)
            throws Exception {
        Table raw = conn.getTable(TablePath.of("default", "raw_table_1"));
        AppendWriter w = raw.newAppend().createWriter();
        // Setup high is the window-6 high (price + 60 + 1 after burst 1):
        // break it by a clear margin.
        int breakout = price[0] + 60 + 1 + 10;
        long eventTime = base + 7 * 15_000L + 4_000L + 2 * 2_000L;
        w.append(rawRow(token, symbol, eventTime, breakout))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        // Flush tick 10 s into window 8: closes window 7 behind it.
        w.append(rawRow(token, symbol, base + 8 * 15_000L + 10_000L, breakout))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        w.flush();
    }

    private static GenericRow rawRow(long token, String symbol, long eventTime, int price) {
        String fingerprint = "fp-" + token + "-" + eventTime;
        return GenericRow.of(
                // 21 columns, DDL order (RawTableColumns indices):
                // event_day, event_fingerprint, fingerprint_version, connection_id,
                // connection_epoch, instrument_token, exchange, symbol, event_time,
                // ingest_ts, ack_ts, tick_type, last_price_paise, last_qty,
                // raw_payload, payload_hash, decoder_version, protocol_version,
                // validity_state, validity_reason, schema_version.
                // event_day MUST be the IST yyyyMMdd of event_time: raw_table_1
                // is auto-partitioned by event_day and a running source only
                // tails partitions it discovered at startup. A stale literal
                // (was "20260901") lands rows in an old partition the job
                // never reads, so no candle ever forms (observed 2026-09-05:
                // B4 symbols absent from the entire job output).
                bs(EVENT_DAY_FMT.format(Instant.ofEpochMilli(eventTime))),
                bs(fingerprint), bs("v2"), bs("e2e"), 1L, token,
                bs("NSE"), bs(symbol), eventTime, eventTime, eventTime, bs("TRADE"),
                (long) price, 1L, new byte[] {1, 2}, bs("h-" + fingerprint),
                bs("1"), bs("v1"), bs("VALID"), bs("FRESH"), bs("3"));
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

    // Smoke milestone: closed FIFTEEN_S candles for the token (KV prefix
    // lookup — candle_closed PK starts with instrument_token). One RPC, no
    // full-table scan, so the smoke loop can afford it every 5th sample.
    private static int countCandles(Connection conn, long token) throws Exception {
        Table candles = conn.getTable(TablePath.of("default", "candle_closed"));
        Lookuper lookuper =
                candles.newLookup().lookupBy("instrument_token").createLookuper();
        LookupResult res = lookuper.lookup(GenericRow.of(token))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (res == null || res.getRowList() == null) {
            return 0;
        }
        int n = 0;
        for (Object row : res.getRowList()) {
            if (row instanceof InternalRow r && !r.isNullAt(CandleClosedColumns.TF)
                    && "FIFTEEN_S".equals(r.getString(CandleClosedColumns.TF).toString())) {
                n++;
            }
        }
        return n;
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
        String[] names = {"raw_table_1", "candle_live", "candle_closed",
                "Signal_Candidates", "Signal_Candidates_current",
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
        e.put("MULTITF_ENABLED", "true");
        e.put("MULTITF_SESSION_BYPASS", "true");
        e.put("STRATEGY_HOST_ENABLED", "true");
        e.put("STRATEGIES", "n7-range-breakout-v1");
        e.put("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates");
        e.put("SIGNAL_CURRENT_TABLE", "Signal_Candidates_current");
        e.put("DEDUP_TTL_MS", "60000");
        e.put("CANDLE_WINDOW_MS", "15000");
        e.put("CHECKPOINT_INTERVAL_MS", "10000");
        e.put("CHECKPOINT_TIMEOUT_MS", "30000");
        e.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        // No ALLOW_FULL_REPLAY: default LATEST startup skips the live LOG
        // backlog (see run: the series is written after the job is RUNNING).
        e.put("CHECKPOINT_DIR",
                "file:///tmp/b4-signal-intent-e2e-checkpoints-" + System.nanoTime());
        for (String k : new String[] {
                "TRADE_DECISIONS_TABLE", "SIGNAL_STRATEGY_ID", "SIGNAL_STRATEGY_VERSION",
                "STATE_BACKEND",
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
