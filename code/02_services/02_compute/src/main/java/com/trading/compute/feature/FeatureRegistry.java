package com.trading.compute.feature;

import com.trading.compute.signaljob.Timeframe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The one shared feature registry (DEC-056): a static table where each feature
 * is a single {@link FeatureDef} line, assigned a stable integer id.
 *
 * <h2>How to add or remove a feature — the only procedure</h2>
 *
 * <ol>
 *   <li><b>Add:</b> append a {@code FeatureDef} at the end of {@link #FEATURES}
 *       with the next id ({@code == SIZE}) and status {@code ACTIVE}, and append
 *       its id/name line to the pin ledger
 *       ({@code src/test/resources/feature-registry-pins.tsv}). Nothing else:
 *       no DDL, no sink, no strategy, no rollout flag changes.</li>
 *   <li><b>Remove:</b> change that line's status to {@code RETIRED}. Do not
 *       delete the line and do not change its id or name.</li>
 *   <li><b>Never</b> renumber, rename, reorder or reuse an id. Old stored rows
 *       are keyed by id and stay readable only under this contract (DEC-057).</li>
 * </ol>
 *
 * <p>The rules are enforced, not trusted: {@link #validate(List)} fails fast at
 * class load on a gap/duplicate, the routing tables exclude retired entries,
 * and {@code FeatureRegistryPinTest} fails on any rename/renumber/delete with
 * the fix written in the failure message.
 */
public final class FeatureRegistry {

    private static final Timeframe[] TFS = Timeframe.values();

    private static final Set<Timeframe> ALL_TFS =
            Collections.unmodifiableSet(EnumSet.allOf(Timeframe.class));

    /** ONE LINE PER FEATURE — append at the end with the next id (DEC-057). */
    private static final List<FeatureDef> FEATURES =
            List.of(
                    new FeatureDef(
                            0,
                            "last_price",
                            FeatureStatus.ACTIVE,
                            FeatureCadence.TICK,
                            ALL_TFS,
                            LastPriceComputer::new),
                    new FeatureDef(
                            1,
                            "sma_close_20",
                            FeatureStatus.ACTIVE,
                            FeatureCadence.CLOSE,
                            EnumSet.of(
                                    Timeframe.ONE_M,
                                    Timeframe.THREE_M,
                                    Timeframe.FIVE_M,
                                    Timeframe.FIFTEEN_M),
                            () -> new SmaComputer(20)),
                    new FeatureDef(
                            2,
                            "rsi_close_14",
                            FeatureStatus.ACTIVE,
                            FeatureCadence.CLOSE,
                            EnumSet.of(Timeframe.ONE_M, Timeframe.FIVE_M, Timeframe.FIFTEEN_M),
                            () -> new RsiComputer(14)));

    /** Number of registered features, retired included (== registry dump size). */
    public static final int SIZE = FEATURES.size();

    /** ACTIVE feature ids fed on every tick (one computer per instrument). */
    static final int[] TICK_IDS;

    /** ACTIVE feature ids carried by a timeframe's stored row, ascending. */
    private static final int[][] IDS_BY_TF;

    /** ACTIVE CLOSE feature ids updated when a timeframe closes, ascending. */
    private static final int[][] CLOSE_IDS_BY_TF;

    private static final FeatureDef[] BY_ID;
    private static final Map<String, FeatureDef> BY_NAME;

    static {
        validate(FEATURES);
        Routing routing = layout(FEATURES);
        TICK_IDS = routing.tickIds();
        IDS_BY_TF = routing.idsByTf();
        CLOSE_IDS_BY_TF = routing.closeIdsByTf();
        BY_ID = new FeatureDef[SIZE];
        Map<String, FeatureDef> byName = new HashMap<>();
        for (FeatureDef def : FEATURES) {
            BY_ID[def.id()] = def;
            byName.put(def.name(), def);
        }
        BY_NAME = Map.copyOf(byName);
    }

    /** Routing tables derived from a definition list (package-visible for guard tests). */
    record Routing(int[] tickIds, int[][] idsByTf, int[][] closeIdsByTf) {}

    /** Builds the routing tables; RETIRED entries are excluded everywhere. */
    static Routing layout(List<FeatureDef> defs) {
        int[][] idsByTf = new int[TFS.length][];
        int[][] closeIdsByTf = new int[TFS.length][];
        for (int t = 0; t < TFS.length; t++) {
            List<Integer> serving = new ArrayList<>();
            List<Integer> closing = new ArrayList<>();
            for (FeatureDef def : defs) {
                if (def.status() != FeatureStatus.ACTIVE || !def.serves(TFS[t])) {
                    continue;
                }
                serving.add(def.id());
                if (def.cadence() == FeatureCadence.CLOSE) {
                    closing.add(def.id());
                }
            }
            idsByTf[t] = toIntArray(serving);
            closeIdsByTf[t] = toIntArray(closing);
        }
        List<Integer> tick = new ArrayList<>();
        for (FeatureDef def : defs) {
            if (def.status() == FeatureStatus.ACTIVE && def.cadence() == FeatureCadence.TICK) {
                tick.add(def.id());
            }
        }
        return new Routing(toIntArray(tick), idsByTf, closeIdsByTf);
    }

    private FeatureRegistry() {}

    /** All registered features in id order, retired included. */
    public static List<FeatureDef> all() {
        return FEATURES;
    }

    /** The feature with {@code id}; ids are validated against the registry size. */
    public static FeatureDef byId(int id) {
        if (id < 0 || id >= SIZE) {
            throw new IllegalArgumentException("no feature with id " + id + " (registry size " + SIZE + ")");
        }
        return BY_ID[id];
    }

    /** The feature named {@code name} (registry-dump lookup for external readers). */
    public static Optional<FeatureDef> byName(String name) {
        return Optional.ofNullable(BY_NAME.get(name));
    }

    /** ACTIVE feature ids a timeframe's stored row carries, ascending. */
    static int[] idsFor(Timeframe tf) {
        return IDS_BY_TF[tf.ordinal()];
    }

    /** ACTIVE CLOSE feature ids updated when {@code tf} closes, ascending. */
    static int[] closeIdsFor(Timeframe tf) {
        return CLOSE_IDS_BY_TF[tf.ordinal()];
    }

    /**
     * Enforces the append-only id contract: ids must be {@code 0..N-1} in
     * declaration order (a gap means a renumber or a delete — both forbidden)
     * and names must be unique. Package-visible so tests can feed bad lists.
     */
    static void validate(List<FeatureDef> defs) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < defs.size(); i++) {
            FeatureDef def = defs.get(i);
            if (def.id() != i) {
                throw new IllegalStateException(
                        "feature '" + def.name() + "' has id " + def.id() + " at index " + i
                                + " — ids must be 0..N-1 in declaration order and append-only (DEC-057)");
            }
            if (!names.add(def.name())) {
                throw new IllegalStateException("duplicate feature name: " + def.name());
            }
        }
    }

    private static int[] toIntArray(List<Integer> values) {
        int[] out = new int[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = values.get(i);
        }
        return out;
    }
}
