package com.trading.compute.signaljob;

import java.io.Serializable;

/**
 * One composite state entry per instrument (design §C.1).
 *
 * <p>Heap-managed, checkpointed as one {@code ValueState<MultiTimeframeState>}
 * per instrument_token. Holds six forming {@link CandleAccumulator}s, six
 * {@link MultiTimeframeClosedRing}s (15 each), quote snapshot, discontinuity
 * marker, and monotonic gate fields. Plain {@link Serializable} with D1 public
 * fields for Flink POJO extraction — same rule as {@link CandleAccumulator}.
 *
 * <p>Six named public fields per dimension (not Map/array) is the POJO-safe
 * choice: maps/arrays as POJO fields confuse Flink's type extractor in some
 * versions. Accessors {@link #forming(Timeframe)} and {@link #closed(Timeframe)}
 * switch on {@link Timeframe} so callers can loop over
 * {@link Timeframe#values()} without reflection.
 *
 * <p>Memory per instrument ~7.3 KiB heap (≈1 KiB serialised — §C.2): 6×~300 B
 * forming + 6×15×~60 B closed + quote/marker overhead. O(instruments×TF×15),
 * not O(ticks).
 */
public class MultiTimeframeState implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── forming accumulators — one per TF ────────────────────────────────
    public CandleAccumulator formingFifteenS;
    public CandleAccumulator formingThirtyS;
    public CandleAccumulator formingOneM;
    public CandleAccumulator formingThreeM;
    public CandleAccumulator formingFiveM;
    public CandleAccumulator formingFifteenM;

    // ── closed rings — one per TF, 15 deep each ──────────────────────────
    public MultiTimeframeClosedRing closedFifteenS;
    public MultiTimeframeClosedRing closedThirtyS;
    public MultiTimeframeClosedRing closedOneM;
    public MultiTimeframeClosedRing closedThreeM;
    public MultiTimeframeClosedRing closedFiveM;
    public MultiTimeframeClosedRing closedFifteenM;

    // ── quote snapshot (QUOTE-only ticks) ────────────────────────────────
    public long lastBidPaise;
    public long lastAskPaise;
    public long lastBidSize;
    public long lastAskSize;
    public long lastQuoteEventTime;

    // ── discontinuity marker ─────────────────────────────────────────────
    public boolean discontinuityPending;
    public long lastDiscontinuityEventTime;

    // ── monotonic gate per instrument ────────────────────────────────────
    public long lastEventTime;
    public String lastFingerprint;

    /** Public no-arg constructor for Flink POJO extraction. Initializes all slots. */
    public MultiTimeframeState() {
        formingFifteenS = new CandleAccumulator();
        formingThirtyS = new CandleAccumulator();
        formingOneM = new CandleAccumulator();
        formingThreeM = new CandleAccumulator();
        formingFiveM = new CandleAccumulator();
        formingFifteenM = new CandleAccumulator();

        closedFifteenS = new MultiTimeframeClosedRing();
        closedThirtyS = new MultiTimeframeClosedRing();
        closedOneM = new MultiTimeframeClosedRing();
        closedThreeM = new MultiTimeframeClosedRing();
        closedFiveM = new MultiTimeframeClosedRing();
        closedFifteenM = new MultiTimeframeClosedRing();

        lastBidPaise = 0L;
        lastAskPaise = 0L;
        lastBidSize = 0L;
        lastAskSize = 0L;
        lastQuoteEventTime = 0L;

        discontinuityPending = false;
        lastDiscontinuityEventTime = 0L;

        lastEventTime = Long.MIN_VALUE;
        lastFingerprint = null;
    }

    /**
     * Forming accumulator for the given timeframe.
     *
     * @param tf timeframe
     * @return live accumulator (never null when state is constructed)
     */
    public CandleAccumulator forming(Timeframe tf) {
        switch (tf) {
            case FIFTEEN_S:
                return formingFifteenS;
            case THIRTY_S:
                return formingThirtyS;
            case ONE_M:
                return formingOneM;
            case THREE_M:
                return formingThreeM;
            case FIVE_M:
                return formingFiveM;
            case FIFTEEN_M:
                return formingFifteenM;
            default:
                throw new IllegalArgumentException("unknown timeframe: " + tf);
        }
    }

    /**
     * Closed ring for the given timeframe.
     *
     * @param tf timeframe
     * @return ring (never null when state is constructed)
     */
    public MultiTimeframeClosedRing closed(Timeframe tf) {
        switch (tf) {
            case FIFTEEN_S:
                return closedFifteenS;
            case THIRTY_S:
                return closedThirtyS;
            case ONE_M:
                return closedOneM;
            case THREE_M:
                return closedThreeM;
            case FIVE_M:
                return closedFiveM;
            case FIFTEEN_M:
                return closedFifteenM;
            default:
                throw new IllegalArgumentException("unknown timeframe: " + tf);
        }
    }

    /**
     * Reset all forming accumulators to fresh (empty) state, keeping closed
     * rings and other snapshot fields intact. Useful after a gap drop where
     * the stale forming candle is discarded but history must survive.
     */
    public void resetForming() {
        formingFifteenS = new CandleAccumulator();
        formingThirtyS = new CandleAccumulator();
        formingOneM = new CandleAccumulator();
        formingThreeM = new CandleAccumulator();
        formingFiveM = new CandleAccumulator();
        formingFifteenM = new CandleAccumulator();
    }

    /**
     * Convenience: is the per-TF closed ring warm (≥15)?
     *
     * @param tf timeframe
     * @return true when 15 closed candles are buffered
     */
    public boolean isWarm(Timeframe tf) {
        return closed(tf).size() >= MultiTimeframeClosedRing.CAPACITY;
    }
}
