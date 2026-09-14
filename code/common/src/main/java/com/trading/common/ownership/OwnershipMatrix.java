package com.trading.common.ownership;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Ownership matrix (docs/08_implementation/01-foundation.md &rarr; "Ownership matrix", orig L615).
 *
 * <p>Encodes, per data/behavior: the sole writer owner, the allowed readers, and the
 * prohibited owners. Enforcement helpers let a component ask "may I write/read this?".
 *
 * <p>The 12 rows mirror the foundation doc table (orig L921-934) exactly, with these
 * documented interpretations:
 * <ul>
 *   <li>"Business Logic/ranking" (candle reader) runs inside the Signal job per Fixed
 *       scope ("one Signal Flink job") &rarr; SIGNAL_JOB.</li>
 *   <li>"audit/offload" and "audit" readers &rarr; STORAGE (lake/audit layer).</li>
 *   <li>"operations" readers &rarr; PLATFORM_HEALTH (ops/health role).</li>
 *   <li>"reconciliation" (portfolio-reservations reader) &rarr; EXECUTOR (executor-side
 *       reconciliation).</li>
 *   <li>"raw ingestion" &rarr; INGESTION.</li>
 *   <li>"Authorized components" (safety-halt owner) &rarr; {@code soleOwner == null}:
 *       the row's owner set is deliberately open, so this class cannot decide it.
 *       {@link #canWrite} denies for such a row and the caller checks
 *       {@link #isOpenOwnerRow} and applies its own authorization.</li>
 *   <li>Gate/attempt row prohibited column "Signal, Executor" in the doc is
 *       self-contradictory (the sole owner may not also be prohibited); interpreted as
 *       Signal + Babysitter, mirroring the order-lifecycle row. The foundation doc row
 *       carries the same reconciliation.</li>
 * </ul>
 */
public final class OwnershipMatrix {

    private OwnershipMatrix() {}

    public enum Component {
        INGESTION, SIGNAL_JOB, ACTION_CAPTURE, EXECUTOR, BABYSITTER, STORAGE, PLATFORM_HEALTH,
        STRATEGY, POSITION_PROJECTOR, RECOVERY_SCANNER, BROKER
    }

    public static final class Rule {
        public final String target;
        /** Sole writer owner; {@code null} = the doc's open "Authorized components" row. */
        public final Component soleOwner;
        public final Set<Component> readers;
        public final Set<Component> prohibitedOwners;

        public Rule(String target, Component soleOwner, Set<Component> readers,
                    Set<Component> prohibitedOwners) {
            if (target == null || target.isBlank()) {
                throw new IllegalArgumentException("ownership rule target must not be null or blank");
            }
            // Defensive immutable copies: the caller's set used to be stored by
            // reference, so whoever built a rule kept a handle on global
            // enforcement (the "every other component" rows are mutable EnumSets).
            this.target = target;
            this.soleOwner = soleOwner;
            this.readers = Set.copyOf(Objects.requireNonNull(readers, "readers"));
            this.prohibitedOwners =
                    Set.copyOf(Objects.requireNonNull(prohibitedOwners, "prohibitedOwners"));
        }
    }

    /** The 12-row ownership matrix (foundation doc, orig L921-934). */
    public static final List<Rule> RULES = List.of(
        new Rule("raw packet/decode", Component.INGESTION,
                Set.of(Component.SIGNAL_JOB, Component.STORAGE), // Signal, audit/offload
                Set.of(Component.STRATEGY, Component.EXECUTOR)), // Strategy, Executor
        new Rule("candle/forming-bar state", Component.SIGNAL_JOB,
                Set.of(Component.SIGNAL_JOB, Component.STORAGE), // Business Logic/ranking, audit/offload
                Set.of(Component.INGESTION, Component.EXECUTOR)), // Ingestion, Executor
        new Rule("candidates/ranking/decisions", Component.SIGNAL_JOB,
                Set.of(Component.EXECUTOR, Component.STORAGE), // Executor, audit
                Set.of(Component.ACTION_CAPTURE)), // Action Capture
        new Rule("order lifecycle", Component.ACTION_CAPTURE,
                Set.of(Component.EXECUTOR, Component.PLATFORM_HEALTH), // Executor, operations
                Set.of(Component.SIGNAL_JOB, Component.BABYSITTER)), // Signal, Babysitter
        new Rule("position aggregate", Component.POSITION_PROJECTOR,
                Set.of(Component.BABYSITTER, Component.EXECUTOR), // Babysitter, Executor
                Set.of(Component.STRATEGY, Component.INGESTION)), // Strategy, raw ingestion
        new Rule("order gate/attempt/mapping/audit", Component.EXECUTOR,
                Set.of(Component.ACTION_CAPTURE, Component.PLATFORM_HEALTH), // Action Capture/operations
                Set.of(Component.SIGNAL_JOB, Component.BABYSITTER)), // doc: "Signal, Executor" — see class javadoc
        new Rule("postback audit/lifecycle", Component.ACTION_CAPTURE,
                Set.of(Component.EXECUTOR, Component.PLATFORM_HEALTH), // Executor, operations
                Set.of(Component.SIGNAL_JOB, Component.BABYSITTER)), // Signal, Babysitter
        new Rule("portfolio reservations", Component.SIGNAL_JOB,
                Set.of(Component.EXECUTOR), // Executor, reconciliation
                Set.of()), // N/A
        new Rule("projection ledger", Component.ACTION_CAPTURE,
                Set.of(Component.RECOVERY_SCANNER), // Recovery scanner
                Set.of()), // N/A
        new Rule("safety halt requests", null, // Authorized components (open set)
                Set.of(Component.EXECUTOR), // Executor
                Set.of()), // N/A
        new Rule("broker REST call", Component.EXECUTOR,
                Set.of(Component.BROKER), // Broker
                EnumSet.complementOf(EnumSet.of(Component.EXECUTOR, Component.BROKER))), // Every other component
        new Rule("position actions", Component.BABYSITTER,
                Set.of(Component.EXECUTOR), // Executor
                EnumSet.complementOf(EnumSet.of(Component.BABYSITTER))) // Arrow REST / direct callers
    );

    /**
     * True only if {@code actor} is the sole owner of {@code rule} and not prohibited.
     *
     * <p>Fails closed. A {@code null} rule or actor denies: an enforcement path must
     * answer "no" rather than throw, and must never authorize what it cannot
     * identify. An open-owner row (see {@link #isOpenOwnerRow}) also denies, because
     * this class records ownership and cannot perform the authorization the doc
     * defers to the caller.
     */
    public static boolean canWrite(Rule rule, Component actor) {
        if (rule == null || actor == null || rule.soleOwner == null) {
            return false;
        }
        return rule.soleOwner == actor && !rule.prohibitedOwners.contains(actor);
    }

    /** True only if {@code actor} is an allowed reader and not prohibited; fails closed on null. */
    public static boolean canRead(Rule rule, Component actor) {
        if (rule == null || actor == null) {
            return false;
        }
        return rule.readers.contains(actor) && !rule.prohibitedOwners.contains(actor);
    }

    /**
     * The doc's open-owner rows ("Authorized components", today only
     * {@code safety halt requests}): the writer set is deliberately not enumerated
     * here, so {@link #canWrite} denies and the caller must authorize against its
     * own halt-writer rules before writing.
     */
    public static boolean isOpenOwnerRow(Rule rule) {
        return rule != null && rule.soleOwner == null;
    }

    /**
     * A log/telemetry table: {@code owner} writes, operations
     * ({@link Component#PLATFORM_HEALTH}) and the lake/audit layer
     * ({@link Component#STORAGE}) read, every other component is a prohibited
     * writer. The previous default handed read to all components and prohibited
     * nobody, i.e. an open-read/open-write rule.
     */
    public static Rule logRule(String table, Component owner) {
        if (owner == null) {
            throw new IllegalArgumentException("log rule owner must not be null");
        }
        Set<Component> readers = Set.of(Component.PLATFORM_HEALTH, Component.STORAGE);
        return new Rule(table, owner, readers,
                EnumSet.complementOf(EnumSet.of(owner, Component.PLATFORM_HEALTH, Component.STORAGE)));
    }
}
