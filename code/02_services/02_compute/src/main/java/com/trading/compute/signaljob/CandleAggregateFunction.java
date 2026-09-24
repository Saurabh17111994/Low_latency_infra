package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * OHLCV aggregation over one 15-second event-time window (REQ-FC-002).
 *
 * <p>Every accepted row contributes to OHLC through its
 * {@code last_price_paise} — trades AND quotes; the last price is the only price
 * a quote carries. (v4 stores bid/ask depth, so a future version may prefer a
 * quote midpoint; v2/v3 rows have 0 ladders and R-054/R-231 removed the columns,
 * which is why the last price is used here.)
 *
 * <p>Volume and {@code tick_count} accumulate ONLY on {@code tick_type = 'TRADE'}
 * rows with a positive {@code volume_delta} — the quantity traded since the
 * previous tick for that token. It is deliberately NOT {@code last_qty}: that is
 * a single trade's size, so it under-counts when several trades land between two
 * snapshots, and on a snapshot with no trade at all it repeats the previous value
 * and made every periodic snapshot look like a trade (measured on the standard
 * stream: 5 of 6 repeated tokens had a zero volume change). A NULL
 * {@code volume_delta} means the baseline is unknown (first tick of a connection)
 * and is never treated as 0 volume.
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
        // P2-018: standalone AggregateFunction has no non-null upstream
        // contract — a null fingerprint/exchange/symbol is a counted-skip
        // class upstream; here silent skip beats a subtask-killing NPE.
        StringData fingerprint = row.getString(RawTableColumns.EVENT_FINGERPRINT);
        if (fingerprint == null || row.isNullAt(RawTableColumns.EXCHANGE)
                || row.isNullAt(RawTableColumns.SYMBOL)) {
            return acc;
        }

        if (acc.exchange == null) {
            acc.exchange = row.getString(RawTableColumns.EXCHANGE).toString();
        }
        if (acc.symbol == null) {
            acc.symbol = row.getString(RawTableColumns.SYMBOL).toString();
        }

        boolean first = acc.isEmpty();
        if (first) {
            acc.openPaise = acc.highPaise = acc.lowPaise = acc.closePaise = price;
        } else {
            acc.highPaise = Math.max(acc.highPaise, price);
            acc.lowPaise = Math.min(acc.lowPaise, price);
        }

        // P2-120: allocation-free open tie-break on the stored String — no
        // per-tie fromString re-encode, no fromString(null) throw ("" init
        // from P2-017 means the stored side is never null, but guard anyway).
        if (eventTime < acc.firstEventTime
                || (eventTime == acc.firstEventTime && acc.firstFingerprint != null
                        && fingerprint.toString().compareTo(acc.firstFingerprint) < 0)) {
            acc.firstEventTime = eventTime;
            // Only here (order-key beat) do we decode — the decode cost is paid
            // on the rare store path, not on every record.
            acc.firstFingerprint = fingerprint.toString();
            acc.openPaise = price;
        }
        // P2-121: same for the close order key.
        if (eventTime > acc.lastEventTime
                || (eventTime == acc.lastEventTime && acc.lastFingerprint != null
                        && fingerprint.toString().compareTo(acc.lastFingerprint) > 0)) {
            acc.lastEventTime = eventTime;
            acc.lastFingerprint = fingerprint.toString();
            acc.closePaise = price;
            // Latency probe: track the ingest time of the close-setting tick
            // (same host clock as any downstream monitor — no skew; never
            // written to an output row; observability only). Null-safe: a row
            // without ingest_ts leaves the previous value untouched.
            if (!row.isNullAt(RawTableColumns.INGEST_TS)) {
                acc.lastIngestTs = row.getLong(RawTableColumns.INGEST_TS);
            }
        }

        // tickType is only compared, never stored — compare StringData directly,
        // zero decode (byte compare == string compare for UTF-8).
        // P2-019: constant-first equals — a null tick_type is non-TRADE, not NPE.
        StringData tickType = row.getString(RawTableColumns.TICK_TYPE);
        // v4: the traded quantity since the previous tick, not a single trade's size.
        // NULL (unknown baseline) stays 0 here on purpose: we cannot know how much
        // traded, and inventing the cumulative volume would be worse than skipping.
        long qty = row.isNullAt(RawTableColumns.VOLUME_DELTA) ? 0L : row.getLong(RawTableColumns.VOLUME_DELTA);
        if (TRADE.equals(tickType) && qty > 0) {
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
        // P2-122/123/126: single empty-aware rewrite — short-circuit empties
        // first (no 0-drag, no null-compare), then guarded compares. Volume,
        // tickCount and identity still accumulate on every short-circuit.
        boolean aEmpty = a.isEmpty();
        boolean bEmpty = b.isEmpty();
        if (bEmpty && aEmpty) {
            return a;
        }
        if (bEmpty) {
            return a;
        }
        if (aEmpty) {
            a.firstEventTime = b.firstEventTime;
            a.firstFingerprint = b.firstFingerprint;
            a.lastEventTime = b.lastEventTime;
            a.lastFingerprint = b.lastFingerprint;
            a.openPaise = b.openPaise;
            a.highPaise = b.highPaise;
            a.lowPaise = b.lowPaise;
            a.closePaise = b.closePaise;
            a.lastIngestTs = b.lastIngestTs;
        } else {
            if (b.firstEventTime < a.firstEventTime
                    || (b.firstEventTime == a.firstEventTime
                            && b.firstFingerprint != null && a.firstFingerprint != null
                            && b.firstFingerprint.compareTo(a.firstFingerprint) < 0)) {
                a.firstEventTime = b.firstEventTime;
                a.firstFingerprint = b.firstFingerprint;
                a.openPaise = b.openPaise;
            }
            if (b.lastEventTime > a.lastEventTime
                    || (b.lastEventTime == a.lastEventTime
                            && b.lastFingerprint != null && a.lastFingerprint != null
                            && b.lastFingerprint.compareTo(a.lastFingerprint) > 0)) {
                a.lastEventTime = b.lastEventTime;
                a.lastFingerprint = b.lastFingerprint;
                a.closePaise = b.closePaise;
                a.lastIngestTs = b.lastIngestTs;
            }
            // P2-126: min/max only when both partials are non-empty.
            a.highPaise = Math.max(a.highPaise, b.highPaise);
            a.lowPaise = Math.min(a.lowPaise, b.lowPaise);
        }
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
