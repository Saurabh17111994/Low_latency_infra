package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

class ExecutionIntentBuilderTest {

    private static final long CREATED = 1_752_000_000_000L;

    static ExecutionIntent sample() {
        return new ExecutionIntent("ignored-derived-id", "candidate-1", "context-1", "acct-1",
                "partition-1", 123L, "NSE", "ABC", ExecutionIntentTableColumns.SIDE_BUY, 10L,
                ExecutionIntentTableColumns.ORDER_TYPE_MARKET, null, "CNC", "DAY", "strategy-1",
                "1.0.0", "config-1", CREATED, CREATED + 60_000L, null);
    }

    @Test
    void buildUsesDeterministicIdentityAndRequestHash() {
        ExecutionIntent intent = sample();
        RowData first = ExecutionIntentBuilder.build(intent);
        RowData second = ExecutionIntentBuilder.build(intent);
        assertEquals(ExecutionIntentBuilder.instructionId(intent),
                first.getString(ExecutionIntentTableColumns.INSTRUCTION_ID).toString());
        assertEquals(ExecutionIntentBuilder.requestHash(intent),
                first.getString(ExecutionIntentTableColumns.REQUEST_HASH).toString());
        assertEquals(first, second);
    }

    @Test
    void everyExecutableFieldChangesRequestHash() {
        ExecutionIntent base = sample();
        assertNotEquals(ExecutionIntentBuilder.requestHash(base),
                ExecutionIntentBuilder.requestHash(new ExecutionIntent(
                        "ignored", "candidate-2", "context-1", "acct-1", "partition-1", 123L,
                        "NSE", "ABC", "BUY", 10L, "MARKET", null, "CNC", "DAY", "strategy-1",
                        "1.0.0", "config-1", CREATED, CREATED + 60_000L, null)));
        assertNotEquals(ExecutionIntentBuilder.requestHash(base),
                ExecutionIntentBuilder.requestHash(new ExecutionIntent(
                        "ignored", "candidate-1", "context-1", "acct-2", "partition-1", 123L,
                        "NSE", "ABC", "BUY", 10L, "MARKET", null, "CNC", "DAY", "strategy-1",
                        "1.0.0", "config-1", CREATED, CREATED + 60_000L, null)));
        assertNotEquals(ExecutionIntentBuilder.requestHash(base),
                ExecutionIntentBuilder.requestHash(new ExecutionIntent(
                        "ignored", "candidate-1", "context-1", "acct-1", "partition-1", 123L,
                        "NSE", "ABC", "BUY", 11L, "MARKET", null, "CNC", "DAY", "strategy-1",
                        "1.0.0", "config-1", CREATED, CREATED + 60_000L, null)));
        assertNotEquals(ExecutionIntentBuilder.requestHash(base),
                ExecutionIntentBuilder.requestHash(new ExecutionIntent(
                        "ignored", "candidate-1", "context-1", "acct-1", "partition-2", 123L,
                        "NSE", "ABC", "BUY", 10L, "MARKET", null, "CNC", "DAY", "strategy-1",
                        "1.0.0", "config-1", CREATED, CREATED + 60_000L, "old-intent")));
    }

    @Test
    void invalidIntentFailsBeforeRowCreation() {
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                "ignored", "candidate", "context", "acct", "partition", 1L, "NSE", "ABC",
                "BUY", 1L, "MARKET", 100L, "CNC", "DAY", "strategy", "1", "config", CREATED,
                null, null));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                "ignored", "candidate", "context", "acct", "partition", 1L, "NSE", "ABC",
                "BUY", 1L, "LIMIT", null, "CNC", "DAY", "strategy", "1", "config",
                CREATED, null, null));
    }

    @Test
    void recordRejectsInvalidAtConstruction() {
        // P2-029: direct construction enforces the contract — no build() needed.
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                null, null, "context", "acct", "partition", 1L, "NSE", "ABC", "BUY", 1L,
                "MARKET", null, "CNC", "DAY", "strategy", "1", "config", CREATED, null,
                null));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                null, "candidate", "context", "acct", "partition", 0L, "NSE", "ABC", "BUY",
                1L, "MARKET", null, "CNC", "DAY", "strategy", "1", "config", CREATED, null,
                null));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                null, "candidate", "context", "acct", "partition", 1L, "NSE", "ABC", "BUY",
                1L, "MARKET", null, "CNC", "DAY", "strategy", "1", "config", CREATED,
                CREATED, null));
    }

    @Test
    void unknownSideAndOrderTypeFailClosed() {
        // P2-030/134 (Option A, no enums): HOLD/STOP never reach the log.
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                null, "candidate", "context", "acct", "partition", 1L, "NSE", "ABC",
                "HOLD", 1L, "MARKET", null, "CNC", "DAY", "strategy", "1", "config",
                CREATED, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                null, "candidate", "context", "acct", "partition", 1L, "NSE", "ABC",
                "BUY", 1L, "STOP", null, "CNC", "DAY", "strategy", "1", "config",
                CREATED, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionIntent(
                null, "candidate", "context", "acct", "partition", 1L, "NSE", "ABC",
                "BUY", 1L, "STOP", 100L, "CNC", "DAY", "strategy", "1", "config",
                CREATED, null, null));
    }

    @Test
    void candidateMapperRequiresUpstreamTradeContext() {
        GenericRowData candidate = candidate(null);
        assertThrows(IllegalArgumentException.class, () -> ExecutionIntentBuilder.fromCandidate(
                candidate, "acct", "partition", "CNC", "DAY", "config"));
    }

    @Test
    void candidateMapperProducesIntentWithoutBrokerFields() {
        GenericRowData candidate = candidate("context-1");
        ExecutionIntent intent = ExecutionIntentBuilder.fromCandidate(
                candidate, "acct", "partition", "CNC", "DAY", "config");
        assertEquals("candidate-1", intent.candidateId());
        assertEquals("context-1", intent.tradeContextId());
        assertEquals(123L, intent.instrumentToken());
        assertEquals("BUY", intent.side());
        assertEquals("MARKET", intent.orderType());
        assertEquals(null, intent.limitPricePaise());
        assertEquals(null, intent.supersedesInstructionId());
    }

    @Test
    void nullLongColumnsRejectInsteadOfNpe() {
        // P2-031: null DETECTION_TS/TOKEN/QUANTITY must be IAE (counted
        // reject), never NPE out of fromCandidate.
        GenericRowData noTs = candidate("context-1");
        noTs.setField(SignalCandidatesTableColumns.DETECTION_TS, null);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExecutionIntentBuilder.fromCandidate(
                        noTs, "acct", "partition", "CNC", "DAY", "config", "context-1"));
        GenericRowData noToken = candidate("context-1");
        noToken.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, null);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExecutionIntentBuilder.fromCandidate(
                        noToken, "acct", "partition", "CNC", "DAY", "config", "context-1"));
        GenericRowData noQty = candidate("context-1");
        noQty.setField(SignalCandidatesTableColumns.QUANTITY, null);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExecutionIntentBuilder.fromCandidate(
                        noQty, "acct", "partition", "CNC", "DAY", "config", "context-1"));
    }

    @Test
    void venueIsPartOfIdentity() {
        // P2-032: same token/symbol on two venues must mint distinct ids,
        // or the protocol misreads a venue difference as a VIOLATION.
        ExecutionIntent nse = sample();
        ExecutionIntent bse = new ExecutionIntent(null, nse.candidateId(), nse.tradeContextId(),
                nse.accountScopeId(), nse.executionPartitionId(), nse.instrumentToken(), "BSE",
                nse.symbol(), nse.side(), nse.quantity(), nse.orderType(), nse.limitPricePaise(),
                nse.productType(), nse.timeInForce(), nse.strategyId(), nse.strategyVersion(),
                nse.configurationVersion(), nse.createdTs(), nse.expiryTs(),
                nse.supersedesInstructionId());
        org.junit.jupiter.api.Assertions.assertNotEquals(
                ExecutionIntentBuilder.instructionId(nse),
                ExecutionIntentBuilder.instructionId(bse));
        org.junit.jupiter.api.Assertions.assertTrue(
                ExecutionIntentBuilder.instructionId(nse).startsWith("ei-v1-"));
        org.junit.jupiter.api.Assertions.assertTrue(
                ExecutionIntentBuilder.identityContent(nse).contains("ei-id-v2"));
    }

    private static GenericRowData candidate(String tradeContextId) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString("candidate-1"));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID,
                tradeContextId == null ? null : StringData.fromString(tradeContextId));
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, 123L);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString("ABC"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString("strategy-1"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString("1.0.0"));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString("rule-1"));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, CREATED);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, null);
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
