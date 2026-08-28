package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * OHLCV aggregation over one 15-second event-time window (REQ-FC-002).
 *
 * <p>Every accepted row contributes to OHLC through its
 * {@code last_price_paise} — trades AND quotes, since the schema-v2 raw rows
 * carry no bid/ask depth (R-054/R-231); the last price is the only price a
 * quote carries. Volume and {@code tick_count} accumulate ONLY on
 * {@code tick_type = 'TRADE'} rows with {@code last_qty > 0}; quote rows and
 * zero-quantity trades contribute neither.
 *
 * <p>Open and close are taken from the row with the smallest / largest
 * {@code (event_time, event_fingerprint)} order key, not from arrival order —
 * this makes the candle deterministic under bounded out-of-order arrival and
 * exactly reproducible from replay. Fingerprints are content hashes, so the
 * tie-break is a stable total order.
 */
public class CandleAggregateFunction implements AggregateFunction<RowData, CandleAccumulator, CandleAccumulator> {

    private static final long serialVersionUID = 1L;

    /** Cached StringData for the TRADE tick-type check (hot path — no per-record alloc). */
    private static final StringData TRADE = StringData.fromString("TRADE");

    @Override
    public CandleAccumulator createAccumulator() {
        return new CandleAccumulator();
    }

    @Override
    public CandleAccumulator add(RowData row, CandleAccumulator acc) {
        long eventTime = row.getLong(RawTableColumns.EVENT_TIME);
        long price = row.getLong(RawTableColumns.LAST_PRICE_PAISE);
        // Hot-path (Finding #17): keep the fingerprint as byte-backed StringData
        // (NO UTF-8 decode per record). The accumulator stores plain String for
        // checkpoint/savepoint restore compatibility — we decode to String ONLY
        // when the order key actually changes (rare: ~0.5% of records, when the
        // event-time/fingerprint order key beats the current first/last).
        StringData fingerprint = row.getString(RawTableColumns.EVENT_FINGERPRINT);

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
                || (eventTime == acc.firstEventTime && fingerprint.compareTo(
                        StringData.fromString(acc.firstFingerprint)) < 0)) {
            acc.firstEventTime = eventTime;
            // Only here (order-key beat) do we decode — the decode cost is paid
            // on the rare store path, not on every record.
            acc.firstFingerprint = fingerprint.toString();
            acc.openPaise = price;
        }
        if (eventTime > acc.lastEventTime
                || (eventTime == acc.lastEventTime && fingerprint.compareTo(
                        StringData.fromString(acc.lastFingerprint)) > 0)) {
            acc.lastEventTime = eventTime;
            acc.lastFingerprint = fingerprint.toString();
            acc.closePaise = price;
        }

        // tickType is only compared, never stored — compare StringData directly,
        // zero decode (byte compare == string compare for UTF-8).
        StringData tickType = row.getString(RawTableColumns.TICK_TYPE);
        long qty = row.isNullAt(RawTableColumns.LAST_QTY) ? 0L : row.getLong(RawTableColumns.LAST_QTY);
        if (tickType.equals(TRADE) && qty > 0) {
            acc.volume += qty;
            acc.tickCount++;
        }
        return acc;
    }

    @Override
    public CandleAccumulator getResult(CandleAccumulator acc) {
        return acc;
    }

    @Override
    public CandleAccumulator merge(CandleAccumulator a, CandleAccumulator b) {
        // Tumbling windows never merge, but implement correctly for safety:
        // earlier order key wins the open, later wins the close.
        if (b.firstEventTime < a.firstEventTime
                || (b.firstEventTime == a.firstEventTime
                        && b.firstFingerprint != null
                        && b.firstFingerprint.compareTo(a.firstFingerprint) < 0)) {
            a.firstEventTime = b.firstEventTime;
            a.firstFingerprint = b.firstFingerprint;
            a.openPaise = b.openPaise;
        }
        if (b.lastEventTime > a.lastEventTime
                || (b.lastEventTime == a.lastEventTime
                        && b.lastFingerprint != null
                        && b.lastFingerprint.compareTo(a.lastFingerprint) > 0)) {
            a.lastEventTime = b.lastEventTime;
            a.lastFingerprint = b.lastFingerprint;
            a.closePaise = b.closePaise;
        }
        a.highPaise = Math.max(a.highPaise, b.highPaise);
        a.lowPaise = Math.min(a.lowPaise, b.lowPaise);
        a.volume += b.volume;
        a.tickCount += b.tickCount;
        if (a.exchange == null) {
            a.exchange = b.exchange;
        }
        if (a.symbol == null) {
            a.symbol = b.symbol;
        }
        return a;
    }
}
