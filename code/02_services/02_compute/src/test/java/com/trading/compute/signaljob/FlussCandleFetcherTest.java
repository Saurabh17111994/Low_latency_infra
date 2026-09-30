package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FlussCandleFetcher} — the native Fluss lookup seam
 * behind {@link ContextProvider} (C1, docs/plans/2026-09-30-strategy-context-live-fetch.md).
 *
 * <p>Pins the two halves that must match the deployed {@code candle_closed}
 * contract (DDL 33): the lookup key row is exactly the primary key
 * {@code (instrument_token, tf, window_start)} in order, and the decoded
 * candle reads the DDL column indexes. The lookuper is a fake — no cluster.
 */
class FlussCandleFetcherTest {

    /** Captures the lookup key row; the result future is test-controlled. */
    private static final class FakeLookuper implements Lookuper {
        private InternalRow seen;
        private final CompletableFuture<LookupResult> result = new CompletableFuture<>();

        @Override
        public CompletableFuture<LookupResult> lookup(InternalRow row) {
            this.seen = row;
            return result;
        }
    }

    private static GenericRow candleRow(long token, String tf, long windowStart) {
        // DDL 33 column order (15 columns).
        GenericRow row = new GenericRow(15);
        row.setField(0, token);
        row.setField(1, BinaryString.fromString("NSE"));
        row.setField(2, BinaryString.fromString("TEST"));
        row.setField(3, BinaryString.fromString(tf));
        row.setField(4, windowStart);
        row.setField(5, windowStart + 60_000L);
        row.setField(6, 1_000L); // open
        row.setField(7, 1_050L); // high
        row.setField(8, 990L); // low
        row.setField(9, 1_020L); // close
        row.setField(10, 7_777L); // volume
        row.setField(11, 21); // tick_count
        row.setField(12, windowStart + 59_000L); // last_event_time
        row.setField(13, BinaryString.fromString("fp")); // last_event_fingerprint
        row.setField(14, BinaryString.fromString("1")); // schema_version
        return row;
    }

    @Test
    @DisplayName("fetch sends the exact (token, tf, window_start) key and decodes the row")
    void fetchBuildsKeyRowAndDecodes() {
        FakeLookuper lookuper = new FakeLookuper();
        FlussCandleFetcher fetcher = new FlussCandleFetcher(lookuper, null);
        ContextKey key = new ContextKey(42L, Timeframe.ONE_M, 111_000L);

        CompletableFuture<ContextCandle> future = fetcher.fetch(key);

        assertNotNull(lookuper.seen, "lookuper must be called synchronously");
        assertEquals(3, lookuper.seen.getFieldCount());
        assertEquals(42L, lookuper.seen.getLong(0));
        assertEquals("ONE_M", lookuper.seen.getString(1).toString());
        assertEquals(111_000L, lookuper.seen.getLong(2));

        lookuper.result.complete(new LookupResult(candleRow(42L, "ONE_M", 111_000L)));
        ContextCandle candle = future.join();

        assertNotNull(candle);
        assertEquals(42L, candle.token());
        assertEquals(Timeframe.ONE_M, candle.tf());
        assertEquals(111_000L, candle.windowStart());
        assertEquals(171_000L, candle.windowEnd());
        assertEquals(1_000L, candle.openPaise());
        assertEquals(1_050L, candle.highPaise());
        assertEquals(990L, candle.lowPaise());
        assertEquals(1_020L, candle.closePaise());
        assertEquals(7_777L, candle.volume());
        assertEquals(21, candle.tickCount());
        assertEquals(170_000L, candle.lastEventTime());
    }

    @Test
    @DisplayName("an empty lookup result means absent, not an error")
    void emptyResultIsAbsent() {
        FakeLookuper lookuper = new FakeLookuper();
        FlussCandleFetcher fetcher = new FlussCandleFetcher(lookuper, null);

        CompletableFuture<ContextCandle> future =
                fetcher.fetch(new ContextKey(42L, Timeframe.ONE_M, 111_000L));
        lookuper.result.complete(new LookupResult(List.of()));

        assertNull(future.join());
    }

    @Test
    @DisplayName("a row that does not match the requested key is rejected, never served")
    void mismatchedRowIsRejected() {
        FakeLookuper lookuper = new FakeLookuper();
        FlussCandleFetcher fetcher = new FlussCandleFetcher(lookuper, null);

        CompletableFuture<ContextCandle> future =
                fetcher.fetch(new ContextKey(42L, Timeframe.ONE_M, 111_000L));
        // Row for a different token — must never be cached under this key.
        lookuper.result.complete(new LookupResult(candleRow(99L, "ONE_M", 111_000L)));

        assertNull(future.join());
    }
}
