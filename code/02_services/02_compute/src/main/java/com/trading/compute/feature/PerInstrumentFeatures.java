package com.trading.compute.feature;

import com.trading.compute.signaljob.Timeframe;
import java.util.Arrays;
import java.util.Map;

/**
 * One instrument's feature state (DEC-056): latest values indexed by feature
 * id, plus one preallocated {@link FeatureComputer} per TICK feature and per
 * (CLOSE feature, timeframe) pair.
 *
 * <p><b>Timeframe separation.</b> A CLOSE feature has one independent value
 * per declared timeframe (an SMA(20) on 1 m is not the SMA(20) on 3 m); the
 * per-close update for one timeframe must never leak into another's stored
 * row. TICK features are timeframe-independent by construction: a single
 * computer per instrument, and the same value appears in every declared
 * timeframe's snapshot.
 *
 * <p>Everything is allocated here, once, at strategy-host slot creation. The
 * update methods are O(active features) with no allocation, boxing, or
 * collection growth — they run on the tick path (48k ticks/s at the HFT
 * universe) and must stay flat.
 */
public final class PerInstrumentFeatures {

    private final boolean[] isTick;
    private final double[] tickLatest;
    private final double[][] closeLatest;
    private final FeatureComputer[] tickComputers;
    private final FeatureComputer[][] closeComputers;

    public PerInstrumentFeatures() {
        int size = FeatureRegistry.SIZE;
        int tfCount = Timeframe.values().length;
        this.isTick = new boolean[size];
        this.tickLatest = new double[size];
        this.closeLatest = new double[size][];
        this.tickComputers = new FeatureComputer[size];
        this.closeComputers = new FeatureComputer[size][];
        Arrays.fill(tickLatest, Double.NaN);
        for (FeatureDef def : FeatureRegistry.all()) {
            if (def.status() != FeatureStatus.ACTIVE) {
                continue; // retired: no computer, never updated, never stored
            }
            if (def.cadence() == FeatureCadence.TICK) {
                isTick[def.id()] = true;
                tickComputers[def.id()] = def.computerFactory().get();
            } else {
                double[] latestPerTimeframe = new double[tfCount];
                Arrays.fill(latestPerTimeframe, Double.NaN);
                closeLatest[def.id()] = latestPerTimeframe;
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
     * TICK features are timeframe-independent — {@code tf} is ignored for them.
     */
    public double latest(int featureId, Timeframe tf) {
        FeatureDef def = FeatureRegistry.byId(featureId); // bounds check
        if (def.status() != FeatureStatus.ACTIVE) {
            return Double.NaN; // retired features are never computed
        }
        return isTick[featureId] ? tickLatest[featureId] : closeLatest[featureId][tf.ordinal()];
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
        for (int id : FeatureRegistry.closeIdsFor(tf)) {
            FeatureComputer computer = closeComputers[id][tf.ordinal()];
            computer.onClose(
                    windowStart, windowEnd, openPaise, highPaise, lowPaise,
                    closePaise, volume, tickCount);
            closeLatest[id][tf.ordinal()] = computer.value();
        }
    }

    /**
     * Fill the stored row's feature map for a closing timeframe: every id the
     * timeframe declares whose value is ready. Not-ready (NaN) features are
     * skipped — storage never carries a placeholder.
     */
    public void snapshot(Timeframe tf, Map<Integer, Double> out) {
        for (int id : FeatureRegistry.idsFor(tf)) {
            double value =
                    isTick[id] ? tickLatest[id] : closeLatest[id][tf.ordinal()];
            if (!Double.isNaN(value)) {
                out.put(id, value);
            }
        }
    }
}
