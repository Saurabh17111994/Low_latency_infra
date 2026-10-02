package com.trading.compute.feature;

import com.trading.compute.signaljob.MarketView;
import com.trading.compute.signaljob.Timeframe;
import java.util.Arrays;
import java.util.Map;

/**
 * One instrument's feature state (DEC-056): latest values indexed by feature
 * id, plus one preallocated {@link FeatureComputer} per TICK feature, per
 * MARKET feature (fed from the shared market snapshot, CHG-512) and per
 * (CLOSE feature, timeframe) pair.
 *
 * <p><b>Timeframe separation.</b> A CLOSE feature has one independent value
 * per declared timeframe (an SMA(20) on 1 m is not the SMA(20) on 3 m); the
 * per-close update for one timeframe must never leak into another's stored
 * row. TICK features are timeframe-independent by construction: a single
 * computer per instrument, and the same value appears in every declared
 * timeframe's snapshot.
 *
 * <p><b>Forming view (2026-10-01).</b> The host feeds the evolving candle of
 * every timeframe ({@link #onFormingCandle}) once per accepted tick, and
 * {@link #latestLive(int, Timeframe)} previews a CLOSE feature as if that
 * candle closed now. The preview is pure arithmetic over the closed state —
 * nothing is stored and the closed values never change because of it.
 *
 * <p><b>Live precompute (2026-10-02).</b> With live precompute on (default),
 * every forming event materializes each CLOSE feature declared for that
 * timeframe into {@link #closeLive} once; a strategy read is then a plain
 * array read, all strategies of the instrument share the same value, and the
 * per-tick cost is independent of the number of readers. The slab is
 * heap-only and never reaches storage: {@link #snapshot(Timeframe, Map)}
 * reads the closed values only. With the kill switch
 * ({@code FEATURE_LIVE_PRECOMPUTE=false}) the live value is computed on read
 * as before — same numbers, no slab.
 *
 * <p>Everything is allocated here, once, at strategy-host slot creation. The
 * update methods are O(active features) with no allocation, boxing, or
 * collection growth — they run on the tick path (48k ticks/s at the HFT
 * universe) and must stay flat.
 */
public final class PerInstrumentFeatures implements FeatureView {

    private final boolean precomputeLive;
    private final boolean[] isTick;
    private final boolean[] isMarket;
    private final double[] tickLatest;
    private final double[] marketLatest;
    private final double[][] closeLatest;
    /**
     * Materialized live previews (2026-10-02): one value per (CLOSE feature,
     * timeframe), computed once per forming event and read by every strategy.
     * Null when {@link #precomputeLive} is off. Heap-only, never stored.
     */
    private final double[][] closeLive;
    private final FeatureComputer[] tickComputers;
    private final FeatureComputer[] marketComputers;
    private final FeatureComputer[][] closeComputers;
    /** Evolving candle per timeframe (2026-10-01): heap-only, never stored. */
    private final long[] formingWindowStart;
    private final long[] formingOpen;
    private final long[] formingHigh;
    private final long[] formingLow;
    private final long[] formingClose;
    private final long[] formingVolume;
    private final long[] formingTicks;
    /** Newest closed window per timeframe; a forming row previews only above it. */
    private final long[] lastClosedWindowStart;

    /** Production default: live precompute on (see the boolean constructor). */
    public PerInstrumentFeatures() {
        this(true);
    }

    /**
     * @param precomputeLive true (default, {@code FEATURE_LIVE_PRECOMPUTE}
     *     on): every forming event materializes each declared CLOSE feature's
     *     live value once, so a strategy read is a plain slab read; false
     *     (kill switch): the live value is computed on read as before — same
     *     numbers, no slab.
     */
    public PerInstrumentFeatures(boolean precomputeLive) {
        this.precomputeLive = precomputeLive;
        int size = FeatureRegistry.SIZE;
        int tfCount = Timeframe.values().length;
        this.isTick = new boolean[size];
        this.isMarket = new boolean[size];
        this.tickLatest = new double[size];
        this.marketLatest = new double[size];
        this.closeLatest = new double[size][];
        this.closeLive = precomputeLive ? new double[size][] : null;
        this.tickComputers = new FeatureComputer[size];
        this.marketComputers = new FeatureComputer[size];
        this.closeComputers = new FeatureComputer[size][];
        this.formingWindowStart = new long[tfCount];
        this.formingOpen = new long[tfCount];
        this.formingHigh = new long[tfCount];
        this.formingLow = new long[tfCount];
        this.formingClose = new long[tfCount];
        this.formingVolume = new long[tfCount];
        this.formingTicks = new long[tfCount];
        this.lastClosedWindowStart = new long[tfCount];
        Arrays.fill(tickLatest, Double.NaN);
        Arrays.fill(marketLatest, Double.NaN);
        Arrays.fill(formingWindowStart, Long.MIN_VALUE);
        Arrays.fill(lastClosedWindowStart, Long.MIN_VALUE);
        for (FeatureDef def : FeatureRegistry.all()) {
            if (def.status() != FeatureStatus.ACTIVE) {
                continue; // retired: no computer, never updated, never stored
            }
            if (def.cadence() == FeatureCadence.TICK) {
                isTick[def.id()] = true;
                tickComputers[def.id()] = def.computerFactory().get();
            } else if (def.cadence() == FeatureCadence.MARKET) {
                isMarket[def.id()] = true;
                marketComputers[def.id()] = def.computerFactory().get();
            } else {
                double[] latestPerTimeframe = new double[tfCount];
                Arrays.fill(latestPerTimeframe, Double.NaN);
                closeLatest[def.id()] = latestPerTimeframe;
                if (precomputeLive) {
                    double[] livePerTimeframe = new double[tfCount];
                    Arrays.fill(livePerTimeframe, Double.NaN);
                    closeLive[def.id()] = livePerTimeframe;
                }
                FeatureComputer[] computersPerTimeframe = new FeatureComputer[tfCount];
                for (Timeframe tf : def.timeframes()) {
                    computersPerTimeframe[tf.ordinal()] = def.computerFactory().get();
                }
                closeComputers[def.id()] = computersPerTimeframe;
            }
        }
    }

    /**
     * Latest value of {@code featureId} for {@code tf}, or NaN when not ready.
     * TICK and MARKET features are timeframe-independent — {@code tf} is
     * ignored for them.
     */
    @Override
    public double latest(int featureId, Timeframe tf) {
        FeatureDef def = FeatureRegistry.byId(featureId); // bounds check
        if (def.status() != FeatureStatus.ACTIVE) {
            return Double.NaN; // retired features are never computed
        }
        if (isTick[featureId]) {
            return tickLatest[featureId];
        }
        if (isMarket[featureId]) {
            return marketLatest[featureId];
        }
        return closeLatest[featureId][tf.ordinal()];
    }

    /** Feed one accepted trade tick to every TICK feature. Allocation-free. */
    public void onTick(
            long eventTimeMs, long pricePaise, long formingVolume, long formingTicks) {
        for (int id : FeatureRegistry.TICK_IDS) {
            FeatureComputer computer = tickComputers[id];
            computer.onTick(eventTimeMs, pricePaise, formingVolume, formingTicks);
            tickLatest[id] = computer.value();
        }
    }

    /**
     * Feed the per-instrument market snapshot to every MARKET feature
     * (2026-10-02, CHG-512) — called on every accepted tick, before the
     * strategy fan-out and before a closing window's sealed row is built, so
     * the stored values are the latest-known book/day stats. Allocation-free.
     */
    public void onMarket(MarketView view) {
        for (int id : FeatureRegistry.MARKET_IDS) {
            FeatureComputer computer = marketComputers[id];
            computer.onMarket(view);
            marketLatest[id] = computer.value();
        }
    }

    /**
     * Record one timeframe's evolving candle (2026-10-01): called once per
     * forming row the host receives — every timeframe, every accepted tick on
     * the fast feed. Heap-only and allocation-free; these values feed
     * {@link #latestLive(int, Timeframe)} and are never stored.
     */
    public void onFormingCandle(
            Timeframe tf,
            long windowStart,
            long openPaise,
            long highPaise,
            long lowPaise,
            long closePaise,
            long volume,
            long tickCount) {
        int ord = tf.ordinal();
        formingWindowStart[ord] = windowStart;
        formingOpen[ord] = openPaise;
        formingHigh[ord] = highPaise;
        formingLow[ord] = lowPaise;
        formingClose[ord] = closePaise;
        formingVolume[ord] = volume;
        formingTicks[ord] = tickCount;
        // 2026-10-02: materialize the live value of every CLOSE feature this
        // timeframe declares, once per forming event, before the strategy
        // fan-out. O(declared close features), allocation-free; every
        // strategy of the instrument reads the same slab. A throwing preview
        // is counted and dropped by the host caller (same isolation contract
        // as onTick/onClose), never thrown into a strategy.
        if (precomputeLive) {
            for (int id : FeatureRegistry.closeIdsFor(tf)) {
                closeLive[id][ord] = closeComputers[id][ord].previewOnForming(
                        openPaise, highPaise, lowPaise, closePaise, volume, tickCount);
            }
        }
    }

    /**
     * Evolving value of {@code featureId} for {@code tf} (2026-10-01): TICK
     * features are already live; a CLOSE feature previews as if the current
     * forming candle closed now, falling back to the closed value when no
     * forming candle is newer than the last closed window (or the feature is
     * not declared for the timeframe). With live precompute on (default) this
     * is a plain read of the value materialized on the latest forming event
     * for that timeframe; with it off the preview is computed on read. Pure
     * read — never mutates closed state.
     */
    @Override
    public double latestLive(int featureId, Timeframe tf) {
        FeatureDef def = FeatureRegistry.byId(featureId); // bounds check
        if (def.status() != FeatureStatus.ACTIVE) {
            return Double.NaN; // retired features are never computed
        }
        if (isTick[featureId]) {
            return tickLatest[featureId];
        }
        if (isMarket[featureId]) {
            return marketLatest[featureId];
        }
        int ord = tf.ordinal();
        FeatureComputer computer = closeComputers[featureId][ord];
        if (computer == null) {
            return Double.NaN; // the feature is not declared for this timeframe
        }
        if (formingWindowStart[ord] <= lastClosedWindowStart[ord]) {
            return closeLatest[featureId][ord]; // no current forming candle
        }
        if (precomputeLive) {
            return closeLive[featureId][ord]; // materialized on the latest forming event
        }
        return computer.previewOnForming(
                formingOpen[ord],
                formingHigh[ord],
                formingLow[ord],
                formingClose[ord],
                formingVolume[ord],
                formingTicks[ord]);
    }

    /** Feed a closed candle to the CLOSE features declared for that timeframe. */
    public void onClosedCandle(
            Timeframe tf,
            long windowStart,
            long windowEnd,
            long openPaise,
            long highPaise,
            long lowPaise,
            long closePaise,
            long volume,
            long tickCount) {
        int ord = tf.ordinal();
        for (int id : FeatureRegistry.closeIdsFor(tf)) {
            FeatureComputer computer = closeComputers[id][ord];
            computer.onClose(
                    windowStart, windowEnd, openPaise, highPaise, lowPaise,
                    closePaise, volume, tickCount);
            closeLatest[id][ord] = computer.value();
            if (precomputeLive) {
                closeLive[id][ord] = closeLatest[id][ord]; // window closed: live == closed
            }
        }
        // A forming row previews only above the newest closed window, so a
        // forming candle that just closed is never counted twice.
        if (windowStart > lastClosedWindowStart[ord]) {
            lastClosedWindowStart[ord] = windowStart;
        }
    }

    /**
     * Fill the stored row's feature map for a closing timeframe: every id the
     * timeframe declares whose value is ready. Not-ready (NaN) features are
     * skipped — storage never carries a placeholder. Reads the closed values
     * only: the live slab never reaches storage.
     */
    public void snapshot(Timeframe tf, Map<Integer, Double> out) {
        for (int id : FeatureRegistry.idsFor(tf)) {
            double value;
            if (isTick[id]) {
                value = tickLatest[id];
            } else if (isMarket[id]) {
                value = marketLatest[id];
            } else {
                value = closeLatest[id][tf.ordinal()];
            }
            if (!Double.isNaN(value)) {
                out.put(id, value);
            }
        }
    }

    /**
     * Test seam: the materialized live slab value of a CLOSE feature (NaN
     * when precompute is off). Production reads go through
     * {@link #latestLive(int, Timeframe)}.
     */
    double liveValueForTest(int featureId, Timeframe tf) {
        return closeLive == null ? Double.NaN : closeLive[featureId][tf.ordinal()];
    }
}
