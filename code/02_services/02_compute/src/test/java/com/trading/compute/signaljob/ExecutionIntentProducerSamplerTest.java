package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Intent-leg latency sampler: cadence math plus never-drop pass-through. */
class ExecutionIntentProducerSamplerTest {

    private OneInputStreamOperatorTestHarness<RowData, RowData> harness;

    @BeforeEach
    void openHarness() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("EXECUTION_INTENT_ENABLED", "true");
        env.put("CONFIGURATION_VERSION", "1.0.0");
        env.put("ACCOUNT_SCOPE_ID", "dev-scope");
        env.put("EXECUTION_PARTITION_ID", "dev-partition");
        env.put("EXECUTION_PRODUCT_TYPE", "CNC");
        env.put("EXECUTION_TIME_IN_FORCE", "DAY");
        ExecutionIntentProducerFunction fn =
                new ExecutionIntentProducerFunction(SignalJobConfig.from(env));
        harness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(fn));
        harness.open();
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void samplesFirstAndEveryThousandthRecord() {
        assertTrue(ExecutionIntentProducerFunction.isSampled(0));
        assertFalse(ExecutionIntentProducerFunction.isSampled(1));
        assertFalse(ExecutionIntentProducerFunction.isSampled(999));
        assertTrue(ExecutionIntentProducerFunction.isSampled(1000));
        assertTrue(ExecutionIntentProducerFunction.isSampled(2000));
    }

    @Test
    void latencyMathRejectsNonPositiveAndFutureTimestamps() {
        assertEquals(50, ExecutionIntentProducerFunction.latencyOrNegative(1000, 950));
        assertEquals(-1, ExecutionIntentProducerFunction.latencyOrNegative(1000, 0));
        assertEquals(-1, ExecutionIntentProducerFunction.latencyOrNegative(1000, -5));
        assertEquals(-1, ExecutionIntentProducerFunction.latencyOrNegative(1000, 1001));
    }

    @Test
    void latencyMathCapsInsaneValues() {
        // P2-136: s-vs-ms mixup (~1.7T ms) and ancient ts drop, never pollute.
        assertEquals(-1, ExecutionIntentProducerFunction.latencyOrNegative(
                1_752_000_000_000L, 1_000_000_000L));
        assertEquals(-1, ExecutionIntentProducerFunction.latencyOrNegative(
                1_752_000_000_000L, 1_752_000_000_000L - 25L * 60 * 60 * 1_000));
        assertEquals(60_000, ExecutionIntentProducerFunction.latencyOrNegative(
                1_752_000_000_000L, 1_752_000_000_000L - 60_000));
    }

    @Test
    void poisonRowsRejectWithoutFailing() throws Exception {
        // P2-034: NPE/ClassCast/arity poison becomes a counted drop, not a
        // task failover — output stays empty, harness stays alive.
        GenericRowData nullToken = candidate(System.currentTimeMillis() - 50,
                System.currentTimeMillis() - 10);
        nullToken.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, null);
        harness.processElement(nullToken, 0);
        harness.processElement(new GenericRowData(3), 0);
        assertEquals(0, harness.getOutput().size());
        // …and the operator still serves valid rows afterwards.
        harness.processElement(
                candidate(System.currentTimeMillis() - 50, System.currentTimeMillis() - 10), 0);
        assertEquals(1, harness.getOutput().size());
    }

    @Test
    void samplerNeverDropsValidCandidates() throws Exception {
        long now = System.currentTimeMillis();
        // 1001 rows cross two sample points (0 and 1000); the null-eval row
        // exercises the null-tolerant skip without failing.
        for (int i = 0; i < 1000; i++) {
            harness.processElement(candidate(now - 50, now - 10), 0);
        }
        harness.processElement(candidate(now - 50, null), 0);

        assertEquals(1001, harness.getOutput().size());
    }

    @Test
    void invalidCandidatesStayRejected() throws Exception {
        GenericRowData bad = candidate(System.currentTimeMillis() - 50,
                System.currentTimeMillis() - 10);
        bad.setField(SignalCandidatesTableColumns.VALIDITY_REASON, StringData.fromString("STALE"));

        harness.processElement(bad, 0);

        assertEquals(0, harness.getOutput().size());
    }

    private static GenericRowData candidate(long detectionTs, Long evaluationTs) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString("candidate-1"));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, 123L);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString("ABC"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString("strategy-1"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString("1.0.0"));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString("rule-1"));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, detectionTs);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, evaluationTs);
        row.setField(SignalCandidatesTableColumns.ACTION,
                StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE,
                StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, 10L);
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE,
                StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, null);
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF, null);
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }
}
