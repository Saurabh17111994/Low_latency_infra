package com.trading.compute.signaljob;

import java.io.Serializable;

/**
 * One composite state entry per instrument (design §C.1).
 *
 * <p>Heap-managed ONLY — held in the operator's plain {@code HashMap<Long,Slot>},
 * never as Flink managed {@code ValueState} (intentional amnesia: a restore
 * restarts empty and rebuilds from live ticks). Holds six forming
 * {@link CandleAccumulator}s, six {@link MultiTimeframeClosedRing}s (15 each),
 * a nested {@link MarketSnapshot} (42 values + two change clocks), a
 * discontinuity marker, and monotonic gate fields. Plain
 * {@link Serializable} with D1 public fields for Flink POJO extraction — same
 * rule as {@link CandleAccumulator}.
 *
 * <p>P2-047: the six ring fields intentionally resolve to GenericTypeInfo/Kryo
 * (ring is a plain Serializable ArrayDeque wrapper, not a POJO) — zero cost
 * while state stays heap-only, but do NOT convert this to ValueState without
 * POJO-ifying the ring first (plus JDK17 final-field risk). Pinned by
 * {@code MultiTimeframeStatePojoTest}.
 *
 * <p>Six named public fields per dimension (not Map/array) is the POJO-safe
 * choice: maps/arrays as POJO fields confuse Flink's type extractor in some
 * versions. Accessors {@link #forming(Timeframe)} and {@link #closed(Timeframe)}
 * switch on {@link Timeframe} so callers can loop over
 * {@link Timeframe#values()} without reflection.
 *
 * <p>Memory per instrument ~7.3 KiB heap (≈1 KiB serialised — §C.2): 6×~300 B
 * forming + 6×15×~60 B closed + quote/market overhead (~0.4 KiB for the
 * nested market snapshot). O(instruments×TF×15), not O(ticks).
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

    // ── market snapshot (2026-10-01 native design) ──────────────────────
    // Latest non-null raw market value per field (0 = not yet seen), updated
    // by every accepted tick (trade or quote), never cleared by resetForming —
    // the same survival contract the original quote snapshot had (P2-152).
    // One nested POJO holds the 12 stats, the 30-column depth ladder, and the
    // two change clocks; the type implements MarketView for strategies.
    public MarketSnapshot market;

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

        market = new MarketSnapshot();

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
     * rings and the market snapshot intact.
     *
     * <p>P2-151: in-place {@code clear()} — preserves {@code forming(tf)}
     * reference identity, so previously returned live refs stay valid. No
     * new objects per reset (less GC on gap/overnight path).
     *
     * <p>P2-152: gate + marker advance atomically in the same call — the
     * caller cannot forget the second half. Overnight/session-boundary
     * reset passes {@code isDiscontinuity=false} (clears a stale pending);
     * gap-drop passes {@code true} (sets it with the gap time).
     *
     * @param gapEventTime event_time to advance the monotonic gate to
     * @param isDiscontinuity true for same-session gap-drop, false for overnight fresh start
     */
    public void resetForming(long gapEventTime, boolean isDiscontinuity) {
        for (Timeframe tf : Timeframe.values()) {
            forming(tf).clear();
        }
        lastEventTime = gapEventTime;
        if (isDiscontinuity) {
            discontinuityPending = true;
            lastDiscontinuityEventTime = gapEventTime;
        } else {
            discontinuityPending = false;
        }
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
