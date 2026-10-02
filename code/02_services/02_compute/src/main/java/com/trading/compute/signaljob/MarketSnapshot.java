package com.trading.compute.signaljob;

import java.io.Serializable;

/**
 * One instrument's latest market snapshot — the mutable, POJO-safe storage
 * behind {@link MarketView} (2026-10-01 native design).
 *
 * <p><b>Two roles, one type.</b> The aggregator holds one instance inside
 * {@link MultiTimeframeState} and updates it from every accepted tick; the
 * strategy host holds one instance per slot and refreshes it from the
 * canonical forming row. Both paths write plain public fields, so the type is
 * zero-allocation and Flink-POJO-extractable (D1: public class, public
 * fields, public no-arg constructor, {@link Serializable}).
 *
 * <p><b>0 = never seen.</b> A field is 0 until a tick that carries it arrives;
 * the row renders 0 as NULL and a later tick that does not carry the field
 * never erases the last known value. The two change clocks are set only when
 * a value actually differs from the stored one (see
 * {@code MultiTimeframeAggregateFunction#updateMarketSnapshot}). The seven
 * extra raw values (CHG-516) additionally carry a per-field seen flag, so a
 * provided 0 is stored as 0 while a never-provided field stays NULL.
 */
public class MarketSnapshot implements MarketView, Serializable {

    private static final long serialVersionUID = 1L;

    // ── stats (12) ───────────────────────────────────────────────────────
    public long totalBuyQty;
    public long totalSellQty;
    public long dayOpenPaise;
    public long dayHighPaise;
    public long dayLowPaise;
    public long prevClosePaise;
    public long vwapPaise;
    public long openInterest;
    public long oiDayHigh;
    public long oiDayLow;
    public long lowerLimitPaise;
    public long upperLimitPaise;

    // ── extra raw market values (7; 2026-10-02, CHG-516) ─────────────────
    // Feed-dependent optional fields. The seen flag records that the feed
    // provided the field at least once, so a provided 0 stores as 0 while a
    // never-provided field stays NULL/absent (never a fabricated zero).
    public long dayVolume;
    public long changeFlag;
    public long imbalanceQty;
    public long indicativeClosePaise;
    public long refPricePaise;
    public long atv;
    public long btv;
    public boolean dayVolumeSeen;
    public boolean changeFlagSeen;
    public boolean imbalanceQtySeen;
    public boolean indicativeCloseSeen;
    public boolean refPriceSeen;
    public boolean atvSeen;
    public boolean btvSeen;

    // ── depth ladder (30) — bid side, level 1 = best ─────────────────────
    public long bidPx1;
    public long bidPx2;
    public long bidPx3;
    public long bidPx4;
    public long bidPx5;
    public long bidQty1;
    public long bidQty2;
    public long bidQty3;
    public long bidQty4;
    public long bidQty5;
    public long bidOrd1;
    public long bidOrd2;
    public long bidOrd3;
    public long bidOrd4;
    public long bidOrd5;

    // ── depth ladder (30) — ask side, level 1 = best ─────────────────────
    public long askPx1;
    public long askPx2;
    public long askPx3;
    public long askPx4;
    public long askPx5;
    public long askQty1;
    public long askQty2;
    public long askQty3;
    public long askQty4;
    public long askQty5;
    public long askOrd1;
    public long askOrd2;
    public long askOrd3;
    public long askOrd4;
    public long askOrd5;

    // ── change clocks ────────────────────────────────────────────────────
    /** Event time of the last tick that changed a stats value; 0 = never. */
    public long statsChangedAt;
    /** Event time of the last tick that changed a depth value; 0 = never. */
    public long depthChangedAt;

    /** Public no-arg constructor for Flink POJO extraction. */
    public MarketSnapshot() {}

    @Override
    public boolean hasStats() {
        return statsChangedAt > 0L;
    }

    @Override
    public boolean hasDepth() {
        return depthChangedAt > 0L;
    }

    @Override
    public long statsChangedAt() {
        return statsChangedAt;
    }

    @Override
    public long depthChangedAt() {
        return depthChangedAt;
    }

    @Override
    public long totalBuyQty() {
        return totalBuyQty;
    }

    @Override
    public long totalSellQty() {
        return totalSellQty;
    }

    @Override
    public long dayOpenPaise() {
        return dayOpenPaise;
    }

    @Override
    public long dayHighPaise() {
        return dayHighPaise;
    }

    @Override
    public long dayLowPaise() {
        return dayLowPaise;
    }

    @Override
    public long prevClosePaise() {
        return prevClosePaise;
    }

    @Override
    public long vwapPaise() {
        return vwapPaise;
    }

    @Override
    public long openInterest() {
        return openInterest;
    }

    @Override
    public long oiDayHigh() {
        return oiDayHigh;
    }

    @Override
    public long oiDayLow() {
        return oiDayLow;
    }

    @Override
    public long lowerLimitPaise() {
        return lowerLimitPaise;
    }

    @Override
    public long upperLimitPaise() {
        return upperLimitPaise;
    }

    // ── extra raw market values (2026-10-02, CHG-516) ────────────────────

    @Override
    public long dayVolume() {
        return dayVolume;
    }

    @Override
    public long changeFlag() {
        return changeFlag;
    }

    @Override
    public long imbalanceQty() {
        return imbalanceQty;
    }

    @Override
    public long indicativeClosePaise() {
        return indicativeClosePaise;
    }

    @Override
    public long refPricePaise() {
        return refPricePaise;
    }

    @Override
    public long atv() {
        return atv;
    }

    @Override
    public long btv() {
        return btv;
    }

    @Override
    public boolean hasDayVolume() {
        return dayVolumeSeen;
    }

    @Override
    public boolean hasChangeFlag() {
        return changeFlagSeen;
    }

    @Override
    public boolean hasImbalanceQty() {
        return imbalanceQtySeen;
    }

    @Override
    public boolean hasIndicativeClose() {
        return indicativeCloseSeen;
    }

    @Override
    public boolean hasRefPrice() {
        return refPriceSeen;
    }

    @Override
    public boolean hasAtv() {
        return atvSeen;
    }

    @Override
    public boolean hasBtv() {
        return btvSeen;
    }

    @Override
    public long bidPxPaise(int level) {
        switch (level) {
            case 1: return bidPx1;
            case 2: return bidPx2;
            case 3: return bidPx3;
            case 4: return bidPx4;
            case 5: return bidPx5;
            default: throw new IllegalArgumentException("depth level out of range [1, 5]: " + level);
        }
    }

    @Override
    public long bidQty(int level) {
        switch (level) {
            case 1: return bidQty1;
            case 2: return bidQty2;
            case 3: return bidQty3;
            case 4: return bidQty4;
            case 5: return bidQty5;
            default: throw new IllegalArgumentException("depth level out of range [1, 5]: " + level);
        }
    }

    @Override
    public long bidOrd(int level) {
        switch (level) {
            case 1: return bidOrd1;
            case 2: return bidOrd2;
            case 3: return bidOrd3;
            case 4: return bidOrd4;
            case 5: return bidOrd5;
            default: throw new IllegalArgumentException("depth level out of range [1, 5]: " + level);
        }
    }

    @Override
    public long askPxPaise(int level) {
        switch (level) {
            case 1: return askPx1;
            case 2: return askPx2;
            case 3: return askPx3;
            case 4: return askPx4;
            case 5: return askPx5;
            default: throw new IllegalArgumentException("depth level out of range [1, 5]: " + level);
        }
    }

    @Override
    public long askQty(int level) {
        switch (level) {
            case 1: return askQty1;
            case 2: return askQty2;
            case 3: return askQty3;
            case 4: return askQty4;
            case 5: return askQty5;
            default: throw new IllegalArgumentException("depth level out of range [1, 5]: " + level);
        }
    }

    @Override
    public long askOrd(int level) {
        switch (level) {
            case 1: return askOrd1;
            case 2: return askOrd2;
            case 3: return askOrd3;
            case 4: return askOrd4;
            case 5: return askOrd5;
            default: throw new IllegalArgumentException("depth level out of range [1, 5]: " + level);
        }
    }
}
