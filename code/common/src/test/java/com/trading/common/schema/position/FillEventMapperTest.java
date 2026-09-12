package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.junit.jupiter.api.Test;

/**
 * SCH-20 pin for {@link FillEventMapper}: a Fills LOG row (08_fills.sql v2,
 * 23 columns by {@link FillsColumns} index) maps to the projector's
 * {@link FillEvent} with the caller-resolved context; non-fill rows are
 * filtered out; {@code sourceVersion} pins to {@code receive_time} and
 * {@code eventTimeMs} falls back to it when broker event time is absent.
 */
class FillEventMapperTest {

    private static final String POSITION_ID = "pos-acc-1-123-BUY-1";

    private static BinaryString bs(String s) {
        return s == null ? BinaryString.EMPTY_UTF8 : BinaryString.fromString(s);
    }

    /** A 23-column Fills LOG row in DDL order (08_fills.sql v2). */
    private static GenericRow fillsRow(String postbackEventId, String accountScopeId,
            String tradeContextId, Long fillQty, Long fillPricePaise, Long brokerEventTime,
            long receiveTime) {
        return GenericRow.of(
                bs(postbackEventId), bs("fp-1"), bs("1"), bs(accountScopeId), bs("bro-1"),
                bs("ins-1"), bs("att-1"), tradeContextId == null ? null : bs(tradeContextId),
                bs("FILLED"), 1L, 0L, fillQty, fillPricePaise, bs("f-1"), brokerEventTime,
                receiveTime, receiveTime, new byte[] {1}, bs("h-1"), bs("CORRELATED"),
                bs(""), bs("1"), bs("2"));
    }

    private static FillContext ctx() {
        return new FillContext(FillEvent.SIDE_BUY, 123L, "NSE", "RELIANCE");
    }

    @Test
    void mapsFillRowWithCallerResolvedContext() {
        Optional<FillEvent> fill = FillEventMapper.mapIfFill(
                fillsRow("pb-42", "acc-1", "tc-9", 100L, 10050L, 1_700_000_000_100L,
                        1_700_000_000_150L),
                POSITION_ID, ctx());
        assertThat(fill).isPresent();
        FillEvent e = fill.get();
        assertThat(e.positionId()).isEqualTo(POSITION_ID);
        assertThat(e.side()).isEqualTo(FillEvent.SIDE_BUY);
        assertThat(e.instrumentToken()).isEqualTo(123L);
        assertThat(e.exchange()).isEqualTo("NSE");
        assertThat(e.symbol()).isEqualTo("RELIANCE");
        assertThat(e.tradeContextId()).isEqualTo("tc-9");
        assertThat(e.accountScopeId()).isEqualTo("acc-1");
        assertThat(e.fillQty()).isEqualTo(100L);
        assertThat(e.fillPricePaise()).isEqualTo(10050L);
        // sourceEventId = postback_event_id; sourceVersion = receive_time
        assertThat(e.sourceEventId()).isEqualTo("pb-42");
        assertThat(e.sourceVersion()).isEqualTo(1_700_000_000_150L);
        // eventTimeMs prefers broker event time
        assertThat(e.eventTimeMs()).isEqualTo(1_700_000_000_100L);
    }

    @Test
    void eventTimeFallsBackToReceiveTimeWhenBrokerEventTimeAbsent() {
        Optional<FillEvent> fill = FillEventMapper.mapIfFill(
                fillsRow("pb-42", "acc-1", "tc-9", 100L, 10050L, null, 1_700_000_000_150L),
                POSITION_ID, ctx());
        assertThat(fill).isPresent();
        assertThat(fill.get().eventTimeMs()).isEqualTo(1_700_000_000_150L);
    }

    @Test
    void skipsRowWithZeroOrMissingFillQty() {
        assertThat(FillEventMapper.mapIfFill(
                fillsRow("pb-1", "acc-1", "tc-9", 0L, 10050L, 1L, 2L), POSITION_ID, ctx()))
                .isEmpty();
        assertThat(FillEventMapper.mapIfFill(
                fillsRow("pb-1", "acc-1", "tc-9", null, 10050L, 1L, 2L), POSITION_ID, ctx()))
                .isEmpty();
    }

    @Test
    void skipsRowWithMissingOrNegativePrice() {
        assertThat(FillEventMapper.mapIfFill(
                fillsRow("pb-1", "acc-1", "tc-9", 100L, null, 1L, 2L), POSITION_ID, ctx()))
                .isEmpty();
        assertThat(FillEventMapper.mapIfFill(
                fillsRow("pb-1", "acc-1", "tc-9", 100L, -5L, 1L, 2L), POSITION_ID, ctx()))
                .isEmpty();
    }

    @Test
    void nullableTradeContextPassesThroughAsNull() {
        Optional<FillEvent> fill = FillEventMapper.mapIfFill(
                fillsRow("pb-1", "acc-1", null, 100L, 10050L, 1L, 2L), POSITION_ID, ctx());
        assertThat(fill).isPresent();
        assertThat(fill.get().tradeContextId()).isNull();
    }

    @Test
    void rejectsInvalidContext() {
        assertThatThrownBy(() -> new FillContext("HOLD", 123L, "NSE", "RELIANCE"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FillContext(FillEvent.SIDE_BUY, 0L, "NSE", "RELIANCE"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FillContext(FillEvent.SIDE_BUY, 123L, " ", "RELIANCE"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FillContext(FillEvent.SIDE_BUY, 123L, "NSE", " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FillEventMapper.mapIfFill(
                fillsRow("pb-1", "acc-1", "tc-9", 100L, 10050L, 1L, 2L), " ", ctx()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- P3-160/161/162: required columns fail fast with the column identity, not a raw NPE ---

    /** A 23-column fills row with exactly one column explicitly null. */
    private static GenericRow fillsRowWithNullAt(int nullIndex) {
        Object[] cols = {
                bs("pb-1"), bs("fp-1"), bs("1"), bs("acc-1"), bs("bro-1"), bs("ins-1"),
                bs("att-1"), bs("tc-1"), bs("FILLED"), 1L, 0L, 100L, 10050L, bs("f-1"),
                1_700_000_000_100L, 1_700_000_000_150L, 1_700_000_000_150L, new byte[] {1},
                bs("h-1"), bs("CORRELATED"), bs(""), bs("1"), bs("2")
        };
        cols[nullIndex] = null;
        return GenericRow.of(cols);
    }

    @Test
    void rejectsNullIdentityColumnsWithTheColumnName() {
        // account_scope_id feeds the position key; postback_event_id IS the fill identity; both
        // were dereferenced unguarded, so a null threw a raw NPE past the quarantine path.
        assertThatThrownBy(() -> FillEventMapper.mapIfFill(
                fillsRowWithNullAt(FillsColumns.ACCOUNT_SCOPE_ID), POSITION_ID, ctx()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("account_scope_id");
        assertThatThrownBy(() -> FillEventMapper.mapIfFill(
                fillsRowWithNullAt(FillsColumns.POSTBACK_EVENT_ID), POSITION_ID, ctx()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("postback_event_id");
        // receive_time seeds BOTH sourceVersion and the event-time fallback, so a silent 0 would
        // corrupt STALE/DUPLICATE gating rather than merely failing.
        assertThatThrownBy(() -> FillEventMapper.mapIfFill(
                fillsRowWithNullAt(FillsColumns.RECEIVE_TIME), POSITION_ID, ctx()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("receive_time");
    }
}
