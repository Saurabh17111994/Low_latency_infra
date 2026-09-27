package com.trading.compute.feature;

/**
 * Lifecycle of a registry entry (DEC-057).
 *
 * <p>Ids and names are permanent. A feature that is no longer wanted is
 * <b>retired</b> — its line stays in the registry so the id stays reserved and
 * every stored row keyed by that id keeps its meaning forever. Retired
 * features are excluded from computation and from new stored rows.
 */
public enum FeatureStatus {

    /** Computed and carried by the declared timeframe rows. */
    ACTIVE,

    /** Not computed, never written to new rows; id and name remain reserved. */
    RETIRED
}
