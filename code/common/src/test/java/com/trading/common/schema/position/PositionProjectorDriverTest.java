package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.PositionState;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.junit.jupiter.api.Test;

/**
 * SCH-20 operator-wiring core: {@link PositionProjectorDriver} mints
 * deterministic position ids per account/instrument/side, projects fills
 * through {@link PositionProjector} with version gating (stale rejected,
 * duplicate no-op, oversell violation), re-opens with a NEW position_id after
 * a full close, and reports NOT_A_FILL for status-only rows.
 */
class PositionProjectorDriverTest {

    private static final String ACCOUNT = "acc-1";
    private static final long INSTRUMENT = 123L;
    private static final String EXCHANGE = "NSE";
    private static final String SYMBOL = "RELIANCE";
    private static final long NOW = 1_700_000_000_000L;

    private final PositionProjectorDriver driver = new PositionProjectorDriver();

    private static BinaryString bs(String s) {
        return BinaryString.fromString(s);
    }

    private static FillContext ctx(String side) {
        return new FillContext(side, INSTRUMENT, EXCHANGE, SYMBOL);
    }

    private static GenericRow row(String postbackEventId, long qty, long price, long version) {
        return GenericRow.of(
                bs(postbackEventId), bs("fp-1"), bs("1"), bs(ACCOUNT), bs("bro-1"),
                bs("ins-1"), bs("att-1"), bs("tc-1"), bs("FILLED"), 1L, 0L, qty, price,
                bs("f-1"), version, version, version, new byte[] {1}, bs("h-1"),
                bs("CORRELATED"), bs(""), bs("1"), bs("2"));
    }

    private static FillEvent fill(String positionId, String side, long qty, long price,
            long version, String eventId) {
        return new FillEvent(positionId, "tc-1", ACCOUNT, INSTRUMENT, EXCHANGE, SYMBOL,
                side, qty, price, eventId, version, NOW);
    }

    private static PositionProjectorDriver.PositionKey key(String side) {
        return new PositionProjectorDriver.PositionKey(ACCOUNT, INSTRUMENT, side);
    }

    @Test
    void mintsDeterministicPositionIdOnFirstBuy() {
        PositionProjectorDriver.FeedResult r = driver.feed(row("pb-1", 100L, 10050L, 1L),
                ctx(FillEvent.SIDE_BUY), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.APPLIED);
        assertThat(r.positionId()).isEqualTo("pos-acc-1-123-BUY-1");
        assertThat(driver.positionIdFor(key(FillEvent.SIDE_BUY))).isEqualTo("pos-acc-1-123-BUY-1");
        PositionSnapshot s = r.snapshot();
        assertThat(s.state()).isEqualTo(PositionState.OPEN);
        assertThat(s.openQuantity()).isEqualTo(100L);
        assertThat(s.averageEntryPaise()).isEqualTo(10050L);
        assertThat(s.sourceVersion()).isEqualTo(1L);
        assertThat(s.schemaVersion()).isEqualTo("2");
    }

    @Test
    void scaleInReduceAndClose() {
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        String id = driver.positionIdFor(key(FillEvent.SIDE_BUY));

        // scale-in: SELL 40 of 100
        PositionProjectorDriver.FeedResult reduce = driver.feed(
                fill(id, FillEvent.SIDE_SELL, 40L, 11000L, 2L, "pb-2"), NOW);
        assertThat(reduce.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.APPLIED);
        assertThat(reduce.snapshot().state()).isEqualTo(PositionState.REDUCING);
        assertThat(reduce.snapshot().closedQuantity()).isEqualTo(40L);
        assertThat(reduce.snapshot().averageExitPaise()).isEqualTo(11000L);
        assertThat(reduce.snapshot().currentQuantity()).isEqualTo(60L);

        // close: SELL remaining 60
        PositionProjectorDriver.FeedResult close = driver.feed(
                fill(id, FillEvent.SIDE_SELL, 60L, 12000L, 3L, "pb-3"), NOW);
        assertThat(close.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.APPLIED);
        assertThat(close.snapshot().state()).isEqualTo(PositionState.CLOSED);
        assertThat(close.snapshot().openQuantity()).isEqualTo(100L);
        assertThat(close.snapshot().closedQuantity()).isEqualTo(100L);
    }

    @Test
    void reEntryAfterCloseMintsNewPositionId() {
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        String firstId = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        driver.feed(fill(firstId, FillEvent.SIDE_SELL, 100L, 12000L, 2L, "pb-2"), NOW);
        assertThat(driver.snapshot(firstId).state()).isEqualTo(PositionState.CLOSED);

        // fresh BUY after a full close -> NEW position_id, new cycle
        PositionProjectorDriver.FeedResult re = driver.feed(
                row("pb-3", 50L, 10100L, 3L), ctx(FillEvent.SIDE_BUY), NOW);
        assertThat(re.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.APPLIED);
        assertThat(re.positionId()).isEqualTo("pos-acc-1-123-BUY-2");
        assertThat(re.snapshot().state()).isEqualTo(PositionState.OPEN);
        // the closed snapshot is retained (history), the driver tracks 2 positions
        assertThat(driver.snapshot(firstId).state()).isEqualTo(PositionState.CLOSED);
        assertThat(driver.size()).isEqualTo(2);
    }

    @Test
    void sellOnClosedPositionIsOversellViolation() {
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        String id = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        driver.feed(fill(id, FillEvent.SIDE_SELL, 100L, 12000L, 2L, "pb-2"), NOW);

        PositionProjectorDriver.FeedResult r = driver.feed(
                row("pb-3", 10L, 13000L, 3L), ctx(FillEvent.SIDE_SELL), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.VIOLATION);
        // stays on the closed id — no new position is minted for a non-BUY
        assertThat(r.positionId()).isEqualTo("pos-acc-1-123-SELL-1");
        assertThat(driver.snapshot(r.positionId())).isNull();
    }

    @Test
    void staleFillRejected() {
        driver.feed(row("pb-1", 100L, 10050L, 5L), ctx(FillEvent.SIDE_BUY), NOW);
        String id = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        // same source event redelivered at an OLDER version (out-of-order
        // delivery) -> STALE (content matches); a different event at an older
        // version would be REGRESSION -> VIOLATION
        PositionProjectorDriver.FeedResult r = driver.feed(
                fill(id, FillEvent.SIDE_BUY, 10L, 9000L, 3L, "pb-1"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.STALE);
        // no mutation
        assertThat(driver.snapshot(id).openQuantity()).isEqualTo(100L);
        assertThat(driver.snapshot(id).sourceVersion()).isEqualTo(5L);
    }

    @Test
    void duplicateFillIsNoOp() {
        driver.feed(row("pb-1", 100L, 10050L, 5L), ctx(FillEvent.SIDE_BUY), NOW);
        String id = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        PositionProjectorDriver.FeedResult r = driver.feed(
                fill(id, FillEvent.SIDE_BUY, 10L, 9000L, 5L, "pb-1"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.DUPLICATE);
        assertThat(driver.snapshot(id).openQuantity()).isEqualTo(100L);
        assertThat(driver.snapshot(id).averageEntryPaise()).isEqualTo(10050L);
    }

    @Test
    void oversellViolation() {
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        String id = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        PositionProjectorDriver.FeedResult r = driver.feed(
                fill(id, FillEvent.SIDE_SELL, 150L, 11000L, 2L, "pb-2"), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.VIOLATION);
        assertThat(r.reason()).contains("overshoots");
        assertThat(driver.snapshot(id).state()).isEqualTo(PositionState.OPEN);
        assertThat(driver.snapshot(id).closedQuantity()).isZero();
    }

    @Test
    void instrumentSeparationMintsDistinctPositions() {
        // positions are keyed by account/instrument/side — two instruments
        // (or two accounts) never share a position id
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        PositionProjectorDriver.FeedResult second = driver.feed(
                row("pb-2", 50L, 20000L, 1L),
                new FillContext(FillEvent.SIDE_BUY, 456L, "NSE", "HDFC"), NOW);
        assertThat(second.positionId()).isEqualTo("pos-acc-1-456-BUY-1");
        assertThat(driver.positionIdFor(key(FillEvent.SIDE_BUY)))
                .isEqualTo("pos-acc-1-123-BUY-1");
        assertThat(driver.size()).isEqualTo(2);
        assertThat(driver.snapshot("pos-acc-1-123-BUY-1").averageEntryPaise()).isEqualTo(10050L);
        assertThat(driver.snapshot("pos-acc-1-456-BUY-1").averageEntryPaise()).isEqualTo(20000L);
    }

    @Test
    void weightedAverageEntryAcrossBuys() {
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        String id = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        driver.feed(fill(id, FillEvent.SIDE_BUY, 100L, 10150L, 2L, "pb-2"), NOW);
        // (100*10050 + 100*10150) / 200 = 10100
        assertThat(driver.snapshot(id).averageEntryPaise()).isEqualTo(10100L);
        assertThat(driver.snapshot(id).openQuantity()).isEqualTo(200L);
    }

    @Test
    void nonFillRowIsNotAFill() {
        PositionProjectorDriver.FeedResult r = driver.feed(
                row("pb-1", 0L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.NOT_A_FILL);
        assertThat(driver.size()).isZero();
    }

    @Test
    void rowPathMintsAndProjectsEndToEnd() {
        PositionProjectorDriver.FeedResult r = driver.feed(
                row("pb-1", 100L, 10050L, 7L), ctx(FillEvent.SIDE_BUY), NOW);
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.APPLIED);
        assertThat(r.positionId()).isEqualTo("pos-acc-1-123-BUY-1");
        PositionSnapshot s = driver.snapshot(r.positionId());
        assertThat(s.sourceEventId()).isEqualTo("pb-1");
        assertThat(s.sourceVersion()).isEqualTo(7L);
        assertThat(s.createdTs()).isEqualTo(NOW);
        assertThat(s.lastUpdateTs()).isEqualTo(NOW);
    }

    // --- P3-166: re-entry must not be minted from the prior state alone ---

    @Test
    void redeliveredOpeningFillAfterCloseIsNotAppliedAsAPhantomReopen() {
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        String firstId = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        driver.feed(fill(firstId, FillEvent.SIDE_SELL, 100L, 12000L, 2L, "pb-2"), NOW);
        assertThat(driver.snapshot(firstId).state()).isEqualTo(PositionState.CLOSED);

        // A redelivered/out-of-order copy of the ORIGINAL opening fill (same event "pb-1",
        // older version 1) arrives on the row path. Minting a fresh cycle id here gave that id
        // no snapshot, so the version gate compared against version 0 and APPLIED it — a phantom
        // OPEN position on pos-...-BUY-2 that bypassed the STALE/DUPLICATE guarantee entirely.
        PositionProjectorDriver.FeedResult r = driver.feed(
                row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);

        // It is now rejected against the closed snapshot (older version, different event ->
        // REGRESSION) instead of being applied as fresh exposure.
        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.VIOLATION);
        assertThat(r.positionId()).isEqualTo(firstId);
        // No new cycle, no phantom position, no fabricated exposure.
        assertThat(driver.size()).isEqualTo(1);
        assertThat(driver.positionIdFor(key(FillEvent.SIDE_BUY))).isEqualTo(firstId);
        assertThat(driver.snapshot("pos-acc-1-123-BUY-2")).isNull();
        assertThat(driver.snapshot(firstId).state()).isEqualTo(PositionState.CLOSED);
        assertThat(driver.snapshot(firstId).openQuantity()).isEqualTo(100L);
        assertThat(driver.snapshot(firstId).closedQuantity()).isEqualTo(100L);
    }

    // --- P3-395: a status-only row must not mint an id or displace `active` ---

    @Test
    void statusOnlyRowDoesNotMintOrDisplaceTheActiveId() {
        driver.feed(row("pb-1", 100L, 10050L, 1L), ctx(FillEvent.SIDE_BUY), NOW);
        String closedId = driver.positionIdFor(key(FillEvent.SIDE_BUY));
        driver.feed(fill(closedId, FillEvent.SIDE_SELL, 100L, 12000L, 2L, "pb-2"), NOW);
        assertThat(driver.snapshot(closedId).state()).isEqualTo(PositionState.CLOSED);

        // A status-only postback on the CLOSED key. Resolving the id before checking fill-ness
        // minted pos-...-BUY-2 and pointed `active` at it — an id holding no snapshot, reachable
        // by positionIdFor(key), and permanently displacing the closed position.
        PositionProjectorDriver.FeedResult r = driver.feed(
                row("pb-3", 0L, 10050L, 3L), ctx(FillEvent.SIDE_BUY), NOW);

        assertThat(r.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.NOT_A_FILL);
        assertThat(r.positionId()).isNull();
        assertThat(driver.positionIdFor(key(FillEvent.SIDE_BUY))).isEqualTo(closedId);
        assertThat(driver.snapshot("pos-acc-1-123-BUY-2")).isNull();
        assertThat(driver.size()).isEqualTo(1);
    }
}
