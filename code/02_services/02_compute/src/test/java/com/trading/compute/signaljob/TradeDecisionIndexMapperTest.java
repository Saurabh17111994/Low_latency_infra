package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SCH-19 pure-JVM unit tests for {@link TradeDecisionIndexMapper}: the
 * dual-sink twin must map one immutable {@code Trade_Decisions} LOG row to its
 * {@code trade_instruction_state} KV index row with the canonical hash
 * recomputed from the row itself — so the index always stores exactly the hash
 * the instruction-feed protocol compares (REQ-FLS-015).
 */
@DisplayName("SCH-19: TradeDecisionIndexMapper (LOG row → instruction-state index row)")
class TradeDecisionIndexMapperTest {

    private final TradeDecisionIndexMapper mapper = new TradeDecisionIndexMapper();

    @Test
    @DisplayName("mapped index row carries instruction_id, recomputed canonical hash, first_written_ts, v1")
    void mapsLogRowToIndexRow() throws Exception {
        TradeDecision d = TradeDecisionBuilderTest.sampleDecision();
        RowData log = TradeDecisionBuilder.build(d);

        RowData index = mapper.map(log);

        assertEquals(TradeInstructionStateColumns.FIELD_COUNT, index.getArity());
        String instructionId = index.getString(TradeInstructionStateColumns.INSTRUCTION_ID).toString();
        assertEquals(TradeDecisionBuilder.instructionId(d), instructionId,
                "the routing identity must be copied through unchanged");
        assertTrue(instructionId.startsWith(TradeDecisionBuilder.INSTRUCTION_ID_PREFIX));
        assertEquals(TradeDecisionBuilder.canonicalHash(d),
                index.getString(TradeInstructionStateColumns.CANONICAL_HASH).toString(),
                "the index must store exactly the canonical hash the protocol compares");
        assertEquals(d.createdTs(), index.getLong(TradeInstructionStateColumns.FIRST_WRITTEN_TS),
                "first_written_ts = the decision's created_ts");
        assertEquals(TradeInstructionStateColumns.SCHEMA_VERSION_V1,
                index.getString(TradeInstructionStateColumns.SCHEMA_VERSION).toString());
    }

    @Test
    @DisplayName("a content-only mutation produces the same instruction_id but a different hash")
    void mutatedRowYieldsDifferentHash() throws Exception {
        TradeDecision d = TradeDecisionBuilderTest.sampleDecision();
        RowData log = TradeDecisionBuilder.build(d);

        RowData indexA = mapper.map(log);

        // Mutate a PROVENANCE field (evaluation_id — not part of the REQ-SS-004
        // identity) and re-map: same instruction_id with a different content hash
        // is exactly the VIOLATION shape the protocol must catch — the index must
        // not mask a content change behind an unchanged identity.
        TradeDecision changed = new TradeDecision(d.candidateId(), d.tradeContextId(),
                d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                d.accountScopeId(), d.strategyId(), d.strategyVersion(), d.configurationVersion(),
                "eval-mutated", d.compositeScore(), d.reservationId(), d.reservationVersion(),
                d.createdTs(), d.expiryTs(), d.supersedesInstructionId(),
                d.supersededByInstructionId());
        RowData indexB = mapper.map(TradeDecisionBuilder.build(changed));

        assertEquals(indexA.getString(TradeInstructionStateColumns.INSTRUCTION_ID),
                indexB.getString(TradeInstructionStateColumns.INSTRUCTION_ID),
                "provenance never changes the executable identity (REQ-SS-004)");
        assertTrue(!indexA.getString(TradeInstructionStateColumns.CANONICAL_HASH)
                        .equals(indexB.getString(TradeInstructionStateColumns.CANONICAL_HASH)),
                "changed content must change the stored hash — a mismatch is a bug, "
                        + "not drift between the two sinks");
    }

    @Test
    @DisplayName("round trip through toDecision preserves the executable request")
    void roundTripPreservesExecutableRequest() {
        TradeDecision d = TradeDecisionBuilderTest.sampleDecision();
        RowData log = TradeDecisionBuilder.build(d);
        TradeDecision back = TradeDecisionIndexMapper.toDecision(log);
        assertNotNull(back);
        assertEquals(d.instrumentToken(), back.instrumentToken());
        assertEquals(d.symbol(), back.symbol());
        assertEquals(d.side(), back.side());
        assertEquals(d.quantity(), back.quantity());
        assertEquals(d.orderType(), back.orderType());
        assertEquals(d.productType(), back.productType());
        assertEquals(d.strategyVersion(), back.strategyVersion());
        assertEquals(d.tradeContextId(), back.tradeContextId());
        assertEquals(d.candidateId(), back.candidateId());
        assertEquals(d.createdTs(), back.createdTs());
    }

    @Test
    @DisplayName("null in a NOT NULL column fails with the column name (P2-064)")
    void nullNotNullColumnNamesItself() {
        TradeDecision d = TradeDecisionBuilderTest.sampleDecision();
        org.apache.flink.table.data.GenericRowData log =
                (org.apache.flink.table.data.GenericRowData) TradeDecisionBuilder.build(d);
        log.setField(TradeDecisionsTableColumns.SYMBOL, null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> mapper.map(log));
        assertTrue(e.getMessage().contains("symbol"),
                "must name the corrupt column, got: " + e.getMessage());
    }

    @Test
    @DisplayName("wrong arity and tampered instruction_id fail loud (P2-184)")
    void arityAndIdDriftFailLoud() {
        TradeDecision d = TradeDecisionBuilderTest.sampleDecision();
        org.apache.flink.table.data.GenericRowData log =
                (org.apache.flink.table.data.GenericRowData) TradeDecisionBuilder.build(d);
        org.apache.flink.table.data.GenericRowData shortRow =
                new org.apache.flink.table.data.GenericRowData(3);
        shortRow.setField(0, log.getString(0));
        shortRow.setField(1, log.getString(1));
        shortRow.setField(2, log.getString(2));
        assertThrows(IllegalArgumentException.class, () -> mapper.map(shortRow));
        org.apache.flink.table.data.GenericRowData tampered =
                (org.apache.flink.table.data.GenericRowData) TradeDecisionBuilder.build(d);
        tampered.setField(TradeDecisionsTableColumns.INSTRUCTION_ID,
                org.apache.flink.table.data.StringData.fromString("ins-v1-tampered"));
        assertThrows(IllegalStateException.class, () -> mapper.map(tampered));
    }
}
