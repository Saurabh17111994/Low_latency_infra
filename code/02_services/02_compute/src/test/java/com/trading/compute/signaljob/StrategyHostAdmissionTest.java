package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Admission tests for strategy-host rows (2026-09-05): a config-registered
 * rule id passes the KV filter only through the host branch's extra-id set;
 * the default filter still drops it; the base identity must match either way.
 */
class StrategyHostAdmissionTest {

    private static final String CUSTOM_RULE = "future-strategy-v1";

    private OneInputStreamOperatorTestHarness<RowData, RowData> harness;

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static GenericRowData rowWithRule(String ruleId) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID,
                StringData.fromString("candidate-1"));
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, 1000L);
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION));
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        row.setField(SignalCandidatesTableColumns.RULE_ID,
                ruleId == null ? null : StringData.fromString(ruleId));
        return row;
    }

    private void openWith(CanonicalSignalFilterFunction filter) throws Exception {
        harness = new OneInputStreamOperatorTestHarness<>(
                new StreamFilter<>(filter),
                SignalCandidatesTableColumns.ROW_TYPE_INFO.createSerializer(new SerializerConfigImpl()));
        harness.open();
    }

    @Test
    @DisplayName("policy admits a registered id with matching base identity")
    void policyAdmitsRegisteredId() {
        assertTrue(CanonicalSignalPolicy.isCanonicalIn(
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                CUSTOM_RULE,
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                Set.of(CUSTOM_RULE)));
    }

    @Test
    @DisplayName("policy refuses on base mismatch or unlisted id or null set")
    void policyRefusesMismatch() {
        assertFalse(CanonicalSignalPolicy.isCanonicalIn(
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                "other-strategy",
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                CUSTOM_RULE,
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                Set.of(CUSTOM_RULE)));
        assertFalse(CanonicalSignalPolicy.isCanonicalIn(
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                "unlisted-v1",
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                Set.of(CUSTOM_RULE)));
        assertFalse(CanonicalSignalPolicy.isCanonicalIn(
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                CUSTOM_RULE,
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                null));
    }

    @Test
    @DisplayName("host-branch filter passes the registered id")
    void hostBranchFilterPassesRegisteredId() throws Exception {
        CanonicalSignalFilterFunction filter =
                new CanonicalSignalFilterFunction(Set.of(CUSTOM_RULE));
        openWith(filter);
        harness.processElement(new StreamRecord<>(rowWithRule(CUSTOM_RULE), 1L));
        assertEquals(1, harness.getOutput().size());
        assertEquals(0L, filter.filteredCountForTest());
    }

    @Test
    @DisplayName("default filter still drops the unlisted id (host rows stay LOG-only)")
    void defaultFilterDropsUnlistedId() throws Exception {
        CanonicalSignalFilterFunction filter = new CanonicalSignalFilterFunction();
        openWith(filter);
        harness.processElement(new StreamRecord<>(rowWithRule(CUSTOM_RULE), 1L));
        assertTrue(harness.getOutput().isEmpty());
        assertEquals(1L, filter.filteredCountForTest());
    }

    @Test
    @DisplayName("stub smoke id never passes even with extras (LOG-only proof)")
    void stubIdNeverAdmitted() throws Exception {
        CanonicalSignalFilterFunction filter = new CanonicalSignalFilterFunction(
                Set.of(StubSmokeStrategy.RULE_ID, CUSTOM_RULE));
        openWith(filter);
        harness.processElement(
                new StreamRecord<>(rowWithRule(StubSmokeStrategy.RULE_ID), 1L));
        assertTrue(harness.getOutput().isEmpty(),
                "stub rows must never reach the KV current-state");
    }
}
