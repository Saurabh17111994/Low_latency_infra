package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

/**
 * REQ-FC-002 OHLCV semantics: OHLC from {@code last_price_paise} of every
 * accepted row (trades AND quotes), volume/{@code tick_count} only from
 * TRADE rows with positive quantity, open/close from the deterministic
 * {@code (event_time, fingerprint)} order key — not arrival order.
 */
class CandleAggregateFunctionTest {

    private final CandleAggregateFunction agg = new CandleAggregateFunction();

    private static final long T0 = 1_750_000_000_000L; // window start

    @Test
    void mixedTradesAndQuotesProduceExpectedOhlcv() {
        CandleAccumulator acc = agg.createAccumulator();
        // arrival order deliberately differs from event order for one row
        agg.add(trade(T0 + 9000, "fp-d", 108, 3), acc);   // later event, arrives first
        agg.add(trade(T0 + 1000, "fp-b", 102, 5), acc);   // earliest event (open)
        agg.add(quote(T0 + 5000, "fp-c", 105, 0), acc);   // quote: OHLC only
        agg.add(trade(T0 + 5000, "fp-e", 107, 0), acc);   // zero qty trade: no volume
        agg.add(trade(T0 + 14000, "fp-f", 106, 7), acc);  // latest event (close)

        assertEquals(102, acc.openPaise);    // earliest order key, not arrival
        assertEquals(108, acc.highPaise);
        assertEquals(102, acc.lowPaise);
        assertEquals(106, acc.closePaise);   // latest order key
        assertEquals(3 + 5 + 7, acc.volume); // quote + zero-qty trade excluded
        assertEquals(3, acc.tickCount);
        assertEquals("NSE", acc.exchange);
        assertEquals("TEST", acc.symbol);
    }

    @Test
    void sameEventTimeTieBreaksOnFingerprint() {
        CandleAccumulator acc = agg.createAccumulator();
        agg.add(trade(T0 + 3000, "fp-z", 104, 1), acc); // same time, larger fp
        agg.add(trade(T0 + 3000, "fp-a", 101, 1), acc); // same time, smaller fp -> open
        agg.add(trade(T0 + 8000, "fp-m", 103, 1), acc);

        assertEquals(101, acc.openPaise);  // lexicographically smaller fingerprint
        assertEquals(103, acc.closePaise); // later event wins the close
        assertEquals(104, acc.highPaise);
        assertEquals(101, acc.lowPaise);
    }

    @Test
    void quoteOnlyWindowHasOhlcButZeroVolumeAndTickCount() {
        CandleAccumulator acc = agg.createAccumulator();
        agg.add(quote(T0 + 1000, "fp-1", 100, 0), acc);
        agg.add(quote(T0 + 5000, "fp-2", 102, 0), acc);

        assertEquals(100, acc.openPaise);
        assertEquals(102, acc.highPaise);
        assertEquals(100, acc.lowPaise);
        assertEquals(102, acc.closePaise);
        assertEquals(0, acc.volume);
        assertEquals(0, acc.tickCount);
    }

    @Test
    void singleRowWindow() {
        CandleAccumulator acc = agg.createAccumulator();
        agg.add(trade(T0 + 2000, "fp-1", 99, 2), acc);

        assertEquals(99, acc.openPaise);
        assertEquals(99, acc.highPaise);
        assertEquals(99, acc.lowPaise);
        assertEquals(99, acc.closePaise);
        assertEquals(2, acc.volume);
        assertEquals(1, acc.tickCount);
    }

    @Test
    void mergeCombinesBothPartialsByOrderKey() {
        CandleAccumulator a = agg.createAccumulator();
        agg.add(trade(T0 + 1000, "fp-a", 100, 1), a);
        agg.add(trade(T0 + 5000, "fp-b", 105, 2), a);

        CandleAccumulator b = agg.createAccumulator();
        agg.add(trade(T0 + 3000, "fp-c", 103, 3), b);
        agg.add(trade(T0 + 9000, "fp-d", 101, 4), b);

        CandleAccumulator merged = agg.merge(a, b);
        assertEquals(100, merged.openPaise);   // earliest order key across partials
        assertEquals(101, merged.closePaise);  // latest order key across partials
        assertEquals(105, merged.highPaise);
        assertEquals(100, merged.lowPaise);
        assertEquals(10, merged.volume);
        assertEquals(4, merged.tickCount);
    }

    @Test
    void lastIngestTsTracksTheCloseSettingTick() {
        CandleAccumulator acc = agg.createAccumulator();
        // ingest_ts rides the row; close is set by the LATEST event-time row.
        agg.add(TestRawRows.withIngestTs(trade(T0 + 1000, "fp-a", 100, 1), 1_000_000L), acc);
        agg.add(TestRawRows.withIngestTs(trade(T0 + 5000, "fp-b", 105, 2), 5_000_000L), acc);
        agg.add(TestRawRows.withIngestTs(trade(T0 + 9000, "fp-c", 107, 3), 9_000_000L), acc);

        assertEquals(9_000_000L, acc.lastIngestTs); // the row that set closePaise
    }

    @Test
    void lastIngestTsSurvivesMergeFromTheNewerPartial() {
        CandleAccumulator a = agg.createAccumulator();
        agg.add(TestRawRows.withIngestTs(trade(T0 + 1000, "fp-a", 100, 1), 1_000_000L), a);
        CandleAccumulator b = agg.createAccumulator();
        agg.add(TestRawRows.withIngestTs(trade(T0 + 9000, "fp-b", 107, 3), 9_000_000L), b);

        CandleAccumulator merged = agg.merge(a, b);
        assertEquals(9_000_000L, merged.lastIngestTs); // newer partial wins
    }

    private static RowData trade(long eventTime, String fp, long price, long qty) {
        return TestRawRows.row(2885L, eventTime, fp, "TRADE", price, qty);
    }

    private static RowData quote(long eventTime, String fp, long price, long qty) {
        return TestRawRows.row(2885L, eventTime, fp, "QUOTE", price, qty);
    }
}
