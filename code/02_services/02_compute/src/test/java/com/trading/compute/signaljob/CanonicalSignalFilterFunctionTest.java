package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Canonical-signal filter (DEC-035, tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001),
 * driven through the Flink 2.2.1 operator harness (OneInputStreamOperatorTestHarness
 * + StreamFilter — no cluster) so the RichFilterFunction's {@code open()} runs
 * and the {@code compute.signal.kv.filtered.noncanonical} MetricGroup counter is
 * live (CHG-023 item 1: the client-side ComputeOtlpEmitter mirror is gone; the
 * native flink-metrics-otel reporter exports the MetricGroup counter).
 */
@DisplayName("CanonicalSignalFilterFunction")
class CanonicalSignalFilterFunctionTest {

    private final CanonicalSignalFilterFunction filter = new CanonicalSignalFilterFunction();

    private OneInputStreamOperatorTestHarness<RowData, RowData> harness;

    @BeforeEach
    void openHarness() throws Exception {
        harness = new OneInputStreamOperatorTestHarness<>(
                new StreamFilter<>(filter),
                SignalCandidatesTableColumns.ROW_TYPE_INFO.createSerializer(new SerializerConfigImpl()));
        harness.open();
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    @Test
    @DisplayName("a row carrying the pinned canonical identity passes through untouched")
    void canonicalRowPasses() throws Exception {
        process(canonicalRow());
        assertEquals(1, harness.getOutput().size(), "canonical row must pass through");
        assertEquals(0L, filter.filteredCountForTest(), "pass-through rows are never counted");
    }

    @Test
    @DisplayName("a row with a drifted strategy identity is dropped and counted exactly once")
    void driftedRowIsDroppedAndCounted() throws Exception {
        GenericRowData row = canonicalRow();
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID,
                StringData.fromString("other-strategy"));

        process(row);
        process(row);
        process(canonicalRow());

        assertEquals(1, harness.getOutput().size(), "only the canonical row passes");
        assertEquals(2L, filter.filteredCountForTest(),
                "one MetricGroup-counter increment per dropped row");
    }

    @Test
    @DisplayName("a null identity column never passes (strict exact-match)")
    void nullIdentityColumnIsDropped() throws Exception {
        GenericRowData row = canonicalRow();
        row.setField(SignalCandidatesTableColumns.RULE_ID, null);

        process(row);
        assertTrue(harness.getOutput().isEmpty(), "null identity must be dropped");
        assertEquals(1L, filter.filteredCountForTest());
    }

    @Test
    @DisplayName("the forming-bar canonical rule id passes (Slice 2.2 dual-sink admission)")
    void formingBarRuleIdPasses() throws Exception {
        GenericRowData row = canonicalRow();
        row.setField(SignalCandidatesTableColumns.RULE_ID,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_FORMING_RULE_ID));
        process(row);
        assertEquals(1, harness.getOutput().size(), "forming-bar rule id must pass");
        assertEquals(0L, filter.filteredCountForTest());
    }

    @Test
    @DisplayName("the N7 canonical rule id passes (2026-09-05 dual-sink admission)")
    void n7RuleIdPasses() throws Exception {
        GenericRowData row = canonicalRow();
        row.setField(SignalCandidatesTableColumns.RULE_ID,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID));
        process(row);
        assertEquals(1, harness.getOutput().size(), "N7 rule id must pass");
        assertEquals(0L, filter.filteredCountForTest());
    }

    private void process(GenericRowData row) throws Exception {
        harness.processElement(new StreamRecord<>(row, 1_700_000_000_000L));
    }

    private static GenericRowData canonicalRow() {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID,
                StringData.fromString("candidate-1"));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, 1000L);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString("SYM"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION));
        row.setField(SignalCandidatesTableColumns.RULE_ID,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_RULE_ID));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, 1_700_000_000_000L);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, 1_700_000_000_000L);
        row.setField(SignalCandidatesTableColumns.ACTION,
                StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE,
                StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, 1L);
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
