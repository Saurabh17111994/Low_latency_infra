package com.trading.compute.feature;

import com.trading.compute.signaljob.Timeframe;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * One registry line (DEC-056): everything the platform needs to know about a
 * feature — its stable integer id, its name, its lifecycle status, when it is
 * fed, which timeframe rows carry it, and how to build its computer.
 *
 * <p><b>Id contract (DEC-057).</b> Ids are append-only: never renumbered,
 * never reused, never deleted. To remove a feature, change its status to
 * {@link FeatureStatus#RETIRED} — the line stays, computation stops, stored
 * rows keep their meaning.
 *
 * @param id stable integer feature id ({@code 0..N-1} in declaration order)
 * @param name stable human name (unique; the pin-ledger and registry-dump key)
 * @param status {@link FeatureStatus#ACTIVE} or {@link FeatureStatus#RETIRED}
 * @param cadence {@link FeatureCadence#TICK} or {@link FeatureCadence#CLOSE}
 * @param timeframes timeframe rows that carry this feature (non-empty)
 * @param computerFactory builds one fresh computer per (instrument, tf) slot
 */
public record FeatureDef(
        int id,
        String name,
        FeatureStatus status,
        FeatureCadence cadence,
        Set<Timeframe> timeframes,
        Supplier<FeatureComputer> computerFactory) {

    public FeatureDef {
        if (id < 0) {
            throw new IllegalArgumentException("feature id must be >= 0, got " + id);
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("feature name must be non-blank");
        }
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(cadence, "cadence");
        if (timeframes == null || timeframes.isEmpty()) {
            throw new IllegalArgumentException("feature " + name + ": at least one timeframe");
        }
        // EnumSet keeps ordinal order — the snapshot order and test pins rely on it.
        timeframes = Collections.unmodifiableSet(EnumSet.copyOf(timeframes));
        Objects.requireNonNull(computerFactory, "computerFactory");
    }

    /** True when timeframe {@code tf}'s rows carry this feature. */
    public boolean serves(Timeframe tf) {
        return timeframes.contains(tf);
    }
}
