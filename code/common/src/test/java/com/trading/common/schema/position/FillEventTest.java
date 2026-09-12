package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * P3-159 pin for the {@link FillEvent} identity invariant: {@code sourceEventId}
 * is the fill identity used by the duplicate/conflict content check, and it is
 * dereferenced by {@link PositionProjector} and by
 * {@code FlussPositionsStateStore.upsert}, so a null/blank one is rejected at
 * construction rather than surfacing as an NPE mid-projection.
 *
 * <p>P3-389 adds the caller-resolved identity fields: the direct-feed path
 * ({@code FillEvent} → {@code PositionProjectorDriver.feed}) bypasses
 * {@link FillContext}, so {@code accountScopeId}/{@code instrumentToken}/{@code exchange}/
 * {@code symbol} are checked here too — while {@code tradeContextId} stays nullable by design.
 */
class FillEventTest {

    private static FillEvent fill(String sourceEventId, long sourceVersion) {
        return new FillEvent("pos-acc-1-123-BUY-1", "tc-1", "acc-1", 123L, "NSE", "RELIANCE",
                FillEvent.SIDE_BUY, 10L, 10050L, sourceEventId, sourceVersion, 0L);
    }

    @Test
    void rejectsNullSourceEventId() {
        assertThatThrownBy(() -> fill(null, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source_event_id");
    }

    @Test
    void rejectsBlankSourceEventId() {
        assertThatThrownBy(() -> fill("   ", 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source_event_id");
    }

    @Test
    void negativeSourceVersionIsDeliberatelyNotRejectedHere() {
        // Negativity must flow to PositionProjector / KvStateUpdateProtocol, which maps it to
        // UNKNOWN -> VIOLATION (PositionProjectorTest.negativeVersionIsUnknownViolation).
        // Rejecting it here would change that pinned outcome into a constructor failure.
        assertThatCode(() -> fill("pb-1", -1L)).doesNotThrowAnyException();
    }

    @Test
    void rejectsZeroFillPrice() {
        // P3-390: only `< 0` was rejected, so a zero-price fill diluted the weighted average.
        assertThatThrownBy(() -> new FillEvent("pos-acc-1-123-BUY-1", "tc-1", "acc-1", 123L,
                "NSE", "RELIANCE", FillEvent.SIDE_BUY, 10L, 0L, "pb-1", 1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fill_price_paise must be positive");
    }

    @Test
    void withPositionIdRebindsOnlyThePositionId() {
        FillEvent rebound = fill("pb-1", 7L).withPositionId("pos-acc-1-123-BUY-2");

        assertThat(rebound.positionId()).isEqualTo("pos-acc-1-123-BUY-2");
        assertThat(rebound.sourceEventId()).isEqualTo("pb-1");
        assertThat(rebound.sourceVersion()).isEqualTo(7L);
        assertThat(rebound.tradeContextId()).isEqualTo("tc-1");
        assertThat(rebound.accountScopeId()).isEqualTo("acc-1");
        assertThat(rebound.fillQty()).isEqualTo(10L);
        assertThat(rebound.fillPricePaise()).isEqualTo(10050L);
        assertThat(rebound.side()).isEqualTo(FillEvent.SIDE_BUY);
    }

    @Test
    void rejectsNullOrBlankAccountScopeId() {
        // P3-389: the direct-feed path bypassed FillContext, so a null account reached
        // FlussPositionsStateStore.upsert and NPE'd in BinaryString.fromString.
        assertThatThrownBy(() -> new FillEvent("pos-1", "tc-1", null, 123L, "NSE", "RELIANCE",
                FillEvent.SIDE_BUY, 10L, 10050L, "pb-1", 1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("account_scope_id");
        assertThatThrownBy(() -> new FillEvent("pos-1", "tc-1", "   ", 123L, "NSE", "RELIANCE",
                FillEvent.SIDE_BUY, 10L, 10050L, "pb-1", 1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("account_scope_id");
    }

    @Test
    void rejectsNonPositiveInstrumentToken() {
        // P3-389: the token is part of the position key, so 0/negative corrupted correlation.
        assertThatThrownBy(() -> new FillEvent("pos-1", "tc-1", "acc-1", 0L, "NSE", "RELIANCE",
                FillEvent.SIDE_BUY, 10L, 10050L, "pb-1", 1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instrument_token must be positive");
    }

    @Test
    void rejectsNullOrBlankExchangeOrSymbol() {
        // P3-389: same bypass — blank exchange/symbol reaches the store and mis-keys the position.
        assertThatThrownBy(() -> new FillEvent("pos-1", "tc-1", "acc-1", 123L, null, "RELIANCE",
                FillEvent.SIDE_BUY, 10L, 10050L, "pb-1", 1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exchange is required");
        assertThatThrownBy(() -> new FillEvent("pos-1", "tc-1", "acc-1", 123L, "NSE", " ",
                FillEvent.SIDE_BUY, 10L, 10050L, "pb-1", 1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("symbol is required");
    }

    @Test
    void nullTradeContextIdStaysAcceptedByDesign() {
        // P3-389 fix block: FillEventMapper passes null when TRADE_CONTEXT_ID is null and the
        // store's bs() is null-tolerant, so this field must stay unchecked and documented as such.
        assertThatCode(() -> new FillEvent("pos-1", null, "acc-1", 123L, "NSE", "RELIANCE",
                FillEvent.SIDE_BUY, 10L, 10050L, "pb-1", 1L, 0L)).doesNotThrowAnyException();
    }
}
