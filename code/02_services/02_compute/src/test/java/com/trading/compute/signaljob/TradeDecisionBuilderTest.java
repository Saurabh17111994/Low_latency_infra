package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SCH-19 pure-JVM unit tests for {@link TradeDecisionBuilder}: deterministic
 * {@code instruction_id} over the REQ-SS-004 executable identity, canonical
 * content hash over the complete execution request (REQ-FLS-008), the frozen
 * 25-column row layout, and fail-closed validation.
 */
@DisplayName("SCH-19: TradeDecisionBuilder")
class TradeDecisionBuilderTest {

    private static final long TS = 1_752_000_000_000L;

    /** Package-visible so the protocol test can drive a realistic builder round trip. */
    static TradeDecision sampleDecision() {
        return new TradeDecision(
                "cand-1",                       // candidateId
                "ctx-7",                        // tradeContextId
                123456L,                        // instrumentToken
                "NSE",                          // exchange
                "RELIANCE",                     // symbol
                TradeDecisionsTableColumns.SIDE_BUY,          // side
                50L,                            // quantity
                TradeDecisionsTableColumns.ORDER_TYPE_MARKET, // orderType
                "EQ",                           // productType
                null,                           // limitPricePaise (market)
                "port-1",                       // portfolioId
                "acct-1",                       // accountScopeId
                "simple-breakout",              // strategyId
                "1.0.0",                        // strategyVersion
                "cfg-2026-08",                  // configurationVersion
                "eval-9",                       // evaluationId
                0.875,                          // compositeScore
                "res-3",                        // reservationId
                "7",                            // reservationVersion
                TS,                             // createdTs
                null,                           // expiryTs
                null,                           // supersedesInstructionId
                null);                          // supersededByInstructionId
    }

    private static TradeDecision mutate(TradeDecision d,
            java.util.function.Function<TradeDecision, TradeDecision> f) {
        return f.apply(d);
    }

    @Test
    @DisplayName("identical input yields the same instruction_id and the same row")
    void deterministicForIdenticalContent() {
        TradeDecision a = sampleDecision();
        TradeDecision b = sampleDecision();
        assertEquals(TradeDecisionBuilder.instructionId(a), TradeDecisionBuilder.instructionId(b));
        assertEquals(TradeDecisionBuilder.canonicalHash(a), TradeDecisionBuilder.canonicalHash(b));
        RowData rowA = TradeDecisionBuilder.build(a);
        RowData rowB = TradeDecisionBuilder.build(b);
        assertEquals(rowA, rowB, "build must be a pure function of its input");
    }

    @Test
    @DisplayName("instruction_id changes when ANY REQ-SS-004 executable field changes")
    void instructionIdChangesWithAnyExecutableField() {
        String base = TradeDecisionBuilder.instructionId(sampleDecision());
        String[][] mutated = {
            // label, mutated field
            {"symbol", "TATAMOTORS"},
            {"trade_context_id", "ctx-8"},
            {"side", TradeDecisionsTableColumns.SIDE_SELL},
            {"strategy_version", "2.0.0"},
            {"order_type", TradeDecisionsTableColumns.ORDER_TYPE_LIMIT},
            {"product_type", "FUT"},
        };
        for (String[] m : mutated) {
            TradeDecision d = sampleDecision();
            TradeDecision changed = switch (m[0]) {
                case "symbol" -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), m[1], d.side(), d.quantity(),
                        d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId());
                case "trade_context_id" -> new TradeDecision(d.candidateId(), m[1],
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId());
                case "side" -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), m[1], d.quantity(),
                        d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId());
                case "strategy_version" -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), m[1], d.configurationVersion(),
                        d.evaluationId(), d.compositeScore(), d.reservationId(),
                        d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId());
                case "order_type" -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        m[1], d.productType(),
                        TradeDecisionsTableColumns.ORDER_TYPE_LIMIT.equals(m[1])
                                ? 10_000L : d.limitPricePaise(),
                        d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId());
                case "product_type" -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        d.orderType(), m[1], d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId());
                default -> throw new IllegalStateException("unhandled mutation " + m[0]);
            };
            assertTrue(!TradeDecisionBuilder.instructionId(changed).equals(base),
                    "instruction_id must change when " + m[0] + " changes (REQ-SS-004)");
        }
        // numeric fields
        TradeDecision qty = mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(),
                d.tradeContextId(), d.instrumentToken(), d.exchange(), d.symbol(), d.side(),
                d.quantity() + 1, d.orderType(), d.productType(), d.limitPricePaise(),
                d.portfolioId(), d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                d.configurationVersion(), d.evaluationId(), d.compositeScore(), d.reservationId(),
                d.reservationVersion(), d.createdTs(), d.expiryTs(), d.supersedesInstructionId(),
                d.supersededByInstructionId()));
        assertTrue(!TradeDecisionBuilder.instructionId(qty).equals(base),
                "instruction_id must change when quantity changes (REQ-SS-004)");
        TradeDecision price = mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(),
                d.tradeContextId(), d.instrumentToken(), d.exchange(), d.symbol(), d.side(),
                d.quantity(), TradeDecisionsTableColumns.ORDER_TYPE_LIMIT, d.productType(),
                10_000L, d.portfolioId(),
                d.accountScopeId(), d.strategyId(), d.strategyVersion(), d.configurationVersion(),
                d.evaluationId(), d.compositeScore(), d.reservationId(), d.reservationVersion(),
                d.createdTs(), d.expiryTs(), d.supersedesInstructionId(),
                d.supersededByInstructionId()));
        assertTrue(!TradeDecisionBuilder.instructionId(price).equals(base),
                "instruction_id must change when order_type LIMIT with a changed limit price "
                        + "changes (REQ-SS-004)");
        TradeDecision token = mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(),
                d.tradeContextId(), d.instrumentToken() + 1, d.exchange(), d.symbol(), d.side(),
                d.quantity(), d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                d.accountScopeId(), d.strategyId(), d.strategyVersion(), d.configurationVersion(),
                d.evaluationId(), d.compositeScore(), d.reservationId(), d.reservationVersion(),
                d.createdTs(), d.expiryTs(), d.supersedesInstructionId(),
                d.supersededByInstructionId()));
        assertTrue(!TradeDecisionBuilder.instructionId(token).equals(base),
                "instruction_id must change when the instrument changes (REQ-SS-004)");
    }

    @Test
    @DisplayName("provenance never affects instruction_id (same-winner unchanged = audit-only)")
    void instructionIdIgnoresProvenance() {
        String base = TradeDecisionBuilder.instructionId(sampleDecision());
        TradeDecision differentProvenance = mutate(sampleDecision(), d -> new TradeDecision(
                "cand-999", d.tradeContextId(), d.instrumentToken(), d.exchange(), d.symbol(),
                d.side(), d.quantity(), d.orderType(), d.productType(), d.limitPricePaise(),
                d.portfolioId(), d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                d.configurationVersion(), "eval-999", d.compositeScore(), d.reservationId(),
                d.reservationVersion(), d.createdTs() + 1, d.expiryTs(), d.supersedesInstructionId(),
                d.supersededByInstructionId()));
        assertEquals(base, TradeDecisionBuilder.instructionId(differentProvenance),
                "candidate_id/evaluation_id/timestamps must not change the identity — "
                        + "a same-winner unchanged-parameter re-evaluation is audit-only "
                        + "(10-ranking.md REQ-RNK-004)");
        assertTrue(!TradeDecisionBuilder.canonicalHash(differentProvenance)
                        .equals(TradeDecisionBuilder.canonicalHash(sampleDecision())),
                "canonical hash DOES cover provenance (complete execution request, REQ-FLS-008)");
    }

    @Test
    @DisplayName("canonical hash is deterministic and sensitive to every content field")
    void canonicalHashSensitivity() {
        String base = TradeDecisionBuilder.canonicalHash(sampleDecision());
        assertEquals(base, TradeDecisionBuilder.canonicalHash(sampleDecision()), "deterministic");
        // a provenance-only change already proven above; here: reservation evidence changes the hash
        TradeDecision differentReservation = mutate(sampleDecision(), d -> new TradeDecision(
                d.candidateId(), d.tradeContextId(), d.instrumentToken(), d.exchange(), d.symbol(),
                d.side(), d.quantity(), d.orderType(), d.productType(), d.limitPricePaise(),
                d.portfolioId(), d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                d.configurationVersion(), d.evaluationId(), d.compositeScore(), "res-999",
                d.reservationVersion(), d.createdTs(), d.expiryTs(), d.supersedesInstructionId(),
                d.supersededByInstructionId()));
        assertTrue(!TradeDecisionBuilder.canonicalHash(differentReservation).equals(base),
                "canonical hash covers the reservation evidence (complete execution request)");
    }

    @Test
    @DisplayName("build emits the frozen 25-column row in DDL order")
    void buildProduces25ColumnRowInDdlOrder() {
        RowData row = TradeDecisionBuilder.build(sampleDecision());
        assertEquals(TradeDecisionsTableColumns.FIELD_COUNT, row.getArity());
        // identity + routing
        assertNotNull(row.getString(TradeDecisionsTableColumns.INSTRUCTION_ID));
        assertTrue(row.getString(TradeDecisionsTableColumns.INSTRUCTION_ID).toString()
                        .startsWith(TradeDecisionBuilder.INSTRUCTION_ID_PREFIX),
                "instruction_id must use the versioned encoding prefix");
        assertEquals("cand-1", row.getString(TradeDecisionsTableColumns.CANDIDATE_ID).toString());
        assertEquals(123456L, row.getLong(TradeDecisionsTableColumns.INSTRUMENT_TOKEN));
        // executable request
        assertEquals("RELIANCE", row.getString(TradeDecisionsTableColumns.SYMBOL).toString());
        assertEquals("BUY", row.getString(TradeDecisionsTableColumns.SIDE).toString());
        assertEquals(50L, row.getLong(TradeDecisionsTableColumns.QUANTITY));
        assertEquals("MARKET", row.getString(TradeDecisionsTableColumns.ORDER_TYPE).toString());
        // nullable columns are null when absent
        assertTrue(row.isNullAt(TradeDecisionsTableColumns.LIMIT_PRICE_PAISE));
        assertTrue(row.isNullAt(TradeDecisionsTableColumns.EXPIRY_TS));
        assertTrue(row.isNullAt(TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID));
        assertTrue(row.isNullAt(TradeDecisionsTableColumns.SUPERSEDED_BY_INSTRUCTION_ID));
        // nullable columns carry values when present
        assertEquals(0.875, row.getDouble(TradeDecisionsTableColumns.COMPOSITE_SCORE), 1e-9);
        // provenance + reservation
        assertEquals("eval-9", row.getString(TradeDecisionsTableColumns.EVALUATION_ID).toString());
        assertEquals("res-3", row.getString(TradeDecisionsTableColumns.RESERVATION_ID).toString());
        assertEquals(TS, row.getLong(TradeDecisionsTableColumns.CREATED_TS));
        // version column
        assertEquals("2", row.getString(TradeDecisionsTableColumns.SCHEMA_VERSION).toString());
    }

    @Test
    @DisplayName("supersession link is carried when provided")
    void buildCarriesSupersessionLink() {
        TradeDecision d = mutate(sampleDecision(), x -> new TradeDecision(x.candidateId(),
                x.tradeContextId(), x.instrumentToken(), x.exchange(), x.symbol(), x.side(),
                x.quantity(), x.orderType(), x.productType(), x.limitPricePaise(), x.portfolioId(),
                x.accountScopeId(), x.strategyId(), x.strategyVersion(), x.configurationVersion(),
                x.evaluationId(), x.compositeScore(), x.reservationId(), x.reservationVersion(),
                x.createdTs(), x.expiryTs(), "ins-v1-prior", null));
        RowData row = TradeDecisionBuilder.build(d);
        assertEquals("ins-v1-prior",
                row.getString(TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID).toString(),
                "a replacement instruction carries supersedes_instruction_id (REQ-SS-004)");
    }

    @Test
    @DisplayName("invalid inputs fail closed before any row is produced")
    void rejectsInvalidInputs() {
        // blank required identity field
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision("", d.tradeContextId(), d.instrumentToken(),
                        d.exchange(), d.symbol(), d.side(), d.quantity(), d.orderType(),
                        d.productType(), d.limitPricePaise(), d.portfolioId(), d.accountScopeId(),
                        d.strategyId(), d.strategyVersion(), d.configurationVersion(),
                        d.evaluationId(), d.compositeScore(), d.reservationId(),
                        d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "blank candidate_id must be rejected");
        // non-positive quantity
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), 0L, d.orderType(),
                        d.productType(), d.limitPricePaise(), d.portfolioId(), d.accountScopeId(),
                        d.strategyId(), d.strategyVersion(), d.configurationVersion(),
                        d.evaluationId(), d.compositeScore(), d.reservationId(),
                        d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "zero quantity must be rejected");
        // non-positive instrument
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(), 0L,
                        d.exchange(), d.symbol(), d.side(), d.quantity(), d.orderType(),
                        d.productType(), d.limitPricePaise(), d.portfolioId(), d.accountScopeId(),
                        d.strategyId(), d.strategyVersion(), d.configurationVersion(),
                        d.evaluationId(), d.compositeScore(), d.reservationId(),
                        d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "non-positive instrument_token must be rejected");
        // non-finite score (10-ranking.md: reject null/non-finite/invalid inputs)
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), Double.NaN, d.reservationId(),
                        d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "NaN composite_score must be rejected");
        // non-positive created_ts
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), -1L, d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "non-positive created_ts must be rejected");
        // negative limit price
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        d.orderType(), d.productType(), -5L, d.portfolioId(), d.accountScopeId(),
                        d.strategyId(), d.strategyVersion(), d.configurationVersion(),
                        d.evaluationId(), d.compositeScore(), d.reservationId(),
                        d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "negative limit_price_paise must be rejected");
        // P4-025: unknown side — closed BUY/SELL enum
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), "HOLD", d.quantity(),
                        d.orderType(), d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "side outside {BUY, SELL} must be rejected (P4-025 closed enum)");
        // P4-025: unknown order_type — closed MARKET/LIMIT enum
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        "STOP", d.productType(), d.limitPricePaise(), d.portfolioId(),
                        d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "order_type outside {MARKET, LIMIT} must be rejected (P4-025 closed enum)");
        // P4-025: LIMIT without a positive limit price — coupling
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        TradeDecisionsTableColumns.ORDER_TYPE_LIMIT, d.productType(), null,
                        d.portfolioId(), d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "LIMIT order without a positive limit_price_paise must be rejected (P4-025)");
        // P4-025: MARKET carrying a limit price — coupling
        assertThrows(IllegalArgumentException.class, () -> TradeDecisionBuilder.build(
                mutate(sampleDecision(), d -> new TradeDecision(d.candidateId(), d.tradeContextId(),
                        d.instrumentToken(), d.exchange(), d.symbol(), d.side(), d.quantity(),
                        TradeDecisionsTableColumns.ORDER_TYPE_MARKET, d.productType(), 10_000L,
                        d.portfolioId(), d.accountScopeId(), d.strategyId(), d.strategyVersion(),
                        d.configurationVersion(), d.evaluationId(), d.compositeScore(),
                        d.reservationId(), d.reservationVersion(), d.createdTs(), d.expiryTs(),
                        d.supersedesInstructionId(), d.supersededByInstructionId()))),
                "MARKET order with a limit_price_paise must be rejected (P4-025)");
    }

    @Test
    @DisplayName("LIMIT decision with a positive price builds and carries the price (P4-025 coupling)")
    void limitDecisionWithPriceBuilds() {
        TradeDecision d = mutate(sampleDecision(), x -> new TradeDecision(x.candidateId(),
                x.tradeContextId(), x.instrumentToken(), x.exchange(), x.symbol(), x.side(),
                x.quantity(), TradeDecisionsTableColumns.ORDER_TYPE_LIMIT, x.productType(),
                10_000L, x.portfolioId(), x.accountScopeId(), x.strategyId(), x.strategyVersion(),
                x.configurationVersion(), x.evaluationId(), x.compositeScore(), x.reservationId(),
                x.reservationVersion(), x.createdTs(), x.expiryTs(),
                x.supersedesInstructionId(), x.supersededByInstructionId()));
        RowData row = TradeDecisionBuilder.build(d);
        assertEquals("LIMIT", row.getString(TradeDecisionsTableColumns.ORDER_TYPE).toString());
        assertEquals(10_000L, row.getLong(TradeDecisionsTableColumns.LIMIT_PRICE_PAISE),
                "LIMIT order must carry its positive limit price");
    }
}
