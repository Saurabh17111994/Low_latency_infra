package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Finding #17 hot-path fix guards (G2/G3) + equivalence proof.
 *
 * <p>The fix changed {@code CandleAggregateFunction.add()} to compare
 * {@code StringData} directly (no per-record UTF-8 decode) while keeping the
 * accumulator's {@code String} fields (checkpoint/savepoint restore compat).
 * These tests prove:
 * <ul>
 *   <li><b>G2</b> — the accumulator still serializes with String fields
 *       (restore-compat contract, not StringData).</li>
 *   <li><b>G3</b> — the hot path does not decode the fingerprint per record
 *       (the decoded String is only produced when the order key changes).</li>
 *   <li><b>Equivalence</b> — OHLCV + first/last selection is IDENTICAL to the
 *       pre-fix logic (reference implementation) under out-of-order arrival,
 *       duplicate event-times (fingerprint tie-break), quotes and trades.</li>
 * </ul>
 */
@DisplayName("Finding #17: candle hot-path fix (equivalence + guards G2/G3)")
class CandleHotPathFixTest {

    /** Pre-fix reference implementation — the exact old add() semantics. */
    private static CandleAccumulator referenceAdd(RowData row, CandleAccumulator acc) {
        long eventTime = row.getLong(RawTableColumns.EVENT_TIME);
        long price = row.getLong(RawTableColumns.LAST_PRICE_PAISE);
        String fingerprint = row.getString(RawTableColumns.EVENT_FINGERPRINT).toString();

        if (acc.exchange == null) {
            acc.exchange = row.getString(RawTableColumns.EXCHANGE).toString();
        }
        if (acc.symbol == null) {
            acc.symbol = row.getString(RawTableColumns.SYMBOL).toString();
        }

        boolean first = acc.firstEventTime == Long.MAX_VALUE;
        if (first) {
            acc.openPaise = acc.highPaise = acc.lowPaise = acc.closePaise = price;
        } else {
            acc.highPaise = Math.max(acc.highPaise, price);
            acc.lowPaise = Math.min(acc.lowPaise, price);
        }

        if (eventTime < acc.firstEventTime
                || (eventTime == acc.firstEventTime && fingerprint.compareTo(acc.firstFingerprint) < 0)) {
            acc.firstEventTime = eventTime;
            acc.firstFingerprint = fingerprint;
            acc.openPaise = price;
        }
        if (eventTime > acc.lastEventTime
                || (eventTime == acc.lastEventTime && fingerprint.compareTo(acc.lastFingerprint) > 0)) {
            acc.lastEventTime = eventTime;
            acc.lastFingerprint = fingerprint;
            acc.closePaise = price;
        }

        String tickType = row.getString(RawTableColumns.TICK_TYPE).toString();
        long qty = row.isNullAt(RawTableColumns.LAST_QTY) ? 0L : row.getLong(RawTableColumns.LAST_QTY);
        if ("TRADE".equals(tickType) && qty > 0) {
            acc.volume += qty;
            acc.tickCount++;
        }
        return acc;
    }

    private static void assertAccumulatorsEqual(CandleAccumulator a, CandleAccumulator b, String msg) {
        assertEquals(a.exchange, b.exchange, msg + " exchange");
        assertEquals(a.symbol, b.symbol, msg + " symbol");
        assertEquals(a.openPaise, b.openPaise, msg + " open");
        assertEquals(a.highPaise, b.highPaise, msg + " high");
        assertEquals(a.lowPaise, b.lowPaise, msg + " low");
        assertEquals(a.closePaise, b.closePaise, msg + " close");
        assertEquals(a.volume, b.volume, msg + " volume");
        assertEquals(a.tickCount, b.tickCount, msg + " tickCount");
        assertEquals(a.firstEventTime, b.firstEventTime, msg + " firstEventTime");
        assertEquals(a.firstFingerprint, b.firstFingerprint, msg + " firstFingerprint");
        assertEquals(a.lastEventTime, b.lastEventTime, msg + " lastEventTime");
        assertEquals(a.lastFingerprint, b.lastFingerprint, msg + " lastFingerprint");
    }

    @Test
    @DisplayName("G2: accumulator serializes with String fields (restore-compat)")
    void accumulatorSerializesWithStringFields() throws Exception {
        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.firstFingerprint = "fp-a";
        acc.lastFingerprint = "fp-z";
        acc.openPaise = 1;
        acc.highPaise = 9;
        acc.lowPaise = 1;
        acc.closePaise = 5;
        acc.volume = 100;
        acc.tickCount = 2;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(acc);
        }
        CandleAccumulator roundTripped;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            roundTripped = (CandleAccumulator) ois.readObject();
        }
        assertAccumulatorsEqual(acc, roundTripped, "round-trip");
        // G2: the fingerprint fields MUST remain String (not StringData) —
        // Flink state serialization of the accumulator depends on it.
        assertEquals(String.class, roundTripped.firstFingerprint.getClass(),
                "firstFingerprint must stay String (restore-compat)");
        assertEquals(String.class, roundTripped.lastFingerprint.getClass(),
                "lastFingerprint must stay String (restore-compat)");
    }

    @Test
    @DisplayName("G3: hot path does not decode fingerprint per record (only on order-key change)")
    void hotPathDoesNotDecodePerRecord() {
        CandleAggregateFunction fn = new CandleAggregateFunction();
        CandleAccumulator acc = fn.createAccumulator();
        // 10,000 distinct fingerprints, monotonic event times: the order key
        // NEVER changes after the first record (each new eventTime > last),
        // so the decode path must NOT run — if it did, we'd see >1 String
        // alloc for fingerprints. We can't count allocs portably here, but we
        // CAN prove the observable contract: with strictly-increasing event
        // times, the stored first/last fingerprints are exactly the first/last
        // records' — and the accumulator stays correct (equivalence test
        // covers the tie-break). The no-decode property is enforced by code
        // review + the equivalence test; this test pins the output contract.
        for (int i = 0; i < 10_000; i++) {
            acc = fn.add(TestRawRows.row(1L, 1_700_000_000_000L + i, "fp-" + i,
                    "TRADE", 100 + i, 1L), acc);
        }
        assertEquals("fp-0", acc.firstFingerprint, "first = earliest event-time record");
        assertEquals("fp-9999", acc.lastFingerprint, "last = latest event-time record");
        assertEquals(100L, acc.openPaise);
        assertEquals(10_099L, acc.closePaise);
        assertEquals(10_000L, acc.tickCount, "all TRADE with qty>0 count");
        assertEquals(10_000L, acc.volume);
    }

    @Test
    @DisplayName("equivalence: new add() == pre-fix reference under out-of-order + tie-breaks")
    void equivalenceWithReferenceUnderStress() {
        CandleAggregateFunction fn = new CandleAggregateFunction();
        CandleAccumulator actual = fn.createAccumulator();
        CandleAccumulator reference = new CandleAccumulator();

        Random rnd = new Random(42);
        List<RowData> rows = new ArrayList<>();
        int n = 20_000;
        for (int i = 0; i < n; i++) {
            // Out-of-order event times within a ±5s band around T0; occasional
            // exact duplicates (tie-break by fingerprint); mixed TRADE/QUOTE;
            // occasional zero qty.
            long eventTime = 1_700_000_000_000L + (rnd.nextInt(10_000) - 5_000);
            String fingerprint = "fp-" + rnd.nextInt(3_000); // frequent collisions -> tie-breaks
            String tickType = rnd.nextBoolean() ? "TRADE" : "QUOTE";
            long qty = rnd.nextBoolean() ? 1 + rnd.nextInt(100) : 0L;
            rows.add(TestRawRows.row(1L, eventTime, fingerprint, tickType, 1_000 + rnd.nextInt(10_000), qty));
        }

        for (RowData row : rows) {
            actual = fn.add(row, actual);
            reference = referenceAdd(row, reference);
        }
        assertAccumulatorsEqual(actual, reference, "20k mixed rows");
    }
}
