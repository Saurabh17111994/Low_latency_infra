package com.trading.compute.signaljob;

import java.util.Collection;

/**
 * Canonical-signal policy (DEC-035, tracker 14 re-scoped P2 —
 * SIGNAL-SCHEMA-001; extended Slice 2.2 forming-bar handoff, Phase C).
 *
 * <p>A signal row is <b>canonical</b> iff all four identity columns equal the
 * expected values exactly: {@code schema_version == expectedSchemaVersion}
 * <em>and</em> {@code strategy_id == expectedStrategyId} <em>and</em>
 * {@code strategy_version == expectedStrategyVersion} <em>and</em>
 * {@code rule_id} equals {@code expectedRuleId} <em>or</em> (Phase C) the
 * alternate forming-bar rule id {@code alternateRuleId}. Any other
 * combination — a strategy iteration, an unversioned row, a blank value — is
 * non-canonical and is excluded from the canonical current-state projection
 * ({@code Signal_Candidates_current}). The LOG twin
 * ({@code Signal_Candidates}) keeps every emitted signal regardless, so no
 * audit history is lost by the filter.
 *
 * <p>The expected identity is {@link SignalCandidatesTableColumns}' pinned
 * canonical constants (the config defaults ARE the canonical identity); an
 * overridden {@code SIGNAL_STRATEGY_*} config therefore feeds rows that this
 * policy filters from the KV projection and counts via
 * {@code compute.signal.kv.filtered.noncanonical}. Phase C adds the pinned
 * forming-bar rule id ({@code CANONICAL_FORMING_RULE_ID}) as the second
 * accepted rule — same canonical strategy/schema/version, distinct rule — so
 * forming-bar placeholder candidates reach the KV current-state like candle
 * candidates (REQ-SS-003 + DEC-035 dual-sink contract).
 *
 * <p>Strict exact-match semantics: {@code null} and blank values never match,
 * and whitespace is not trimmed (a padded value is a different identity
 * string, not an equivalent one).
 */
public final class CanonicalSignalPolicy {

    private CanonicalSignalPolicy() {}

    /**
     * Single-rule form (pre-Phase-C callers/tests): canonical iff all four
     * row identity columns exactly equal the expected values.
     */
    public static boolean isCanonical(
            String schemaVersion,
            String strategyId,
            String strategyVersion,
            String ruleId,
            String expectedSchemaVersion,
            String expectedStrategyId,
            String expectedStrategyVersion,
            String expectedRuleId) {
        return isCanonical(schemaVersion, strategyId, strategyVersion, ruleId,
                expectedSchemaVersion, expectedStrategyId, expectedStrategyVersion,
                expectedRuleId, null);
    }

    /**
     * Two-rule form (Phase C): canonical iff all four identity columns equal
     * the expected values with {@code ruleId} matching {@code expectedRuleId}
     * OR (when {@code alternateRuleId} is non-null) {@code alternateRuleId}.
     * The schema/strategy/version must match the pinned expected identity in
     * both cases — the alternate is a second pinned rule of the same
     * canonical strategy, not a relaxation of the other columns.
     */
    public static boolean isCanonical(
            String schemaVersion,
            String strategyId,
            String strategyVersion,
            String ruleId,
            String expectedSchemaVersion,
            String expectedStrategyId,
            String expectedStrategyVersion,
            String expectedRuleId,
            String alternateRuleId) {
        return isCanonical(schemaVersion, strategyId, strategyVersion, ruleId,
                expectedSchemaVersion, expectedStrategyId, expectedStrategyVersion,
                expectedRuleId, alternateRuleId, null);
    }

    /**
     * Three-rule form (N7, 2026-09-05): like the two-rule form but also
     * admits a second alternate rule id. The N7 canonical rule
     * ({@link SignalCandidatesTableColumns#CANONICAL_N7_RULE_ID}) joins the
     * forming-bar rule as a second alternate of the same canonical strategy.
     */
    public static boolean isCanonical(
            String schemaVersion,
            String strategyId,
            String strategyVersion,
            String ruleId,
            String expectedSchemaVersion,
            String expectedStrategyId,
            String expectedStrategyVersion,
            String expectedRuleId,
            String alternateRuleId,
            String alternateRuleId2) {
        boolean identityMatch = expectedSchemaVersion != null
                && expectedSchemaVersion.equals(schemaVersion)
                && expectedStrategyId != null
                && expectedStrategyId.equals(strategyId)
                && expectedStrategyVersion != null
                && expectedStrategyVersion.equals(strategyVersion);
        if (!identityMatch) {
            return false;
        }
        return (expectedRuleId != null && expectedRuleId.equals(ruleId))
                || (alternateRuleId != null && alternateRuleId.equals(ruleId))
                || (alternateRuleId2 != null && alternateRuleId2.equals(ruleId));
    }

    /**
     * Registry form (strategy-host design, 2026-09-05): canonical iff the
     * schema/strategy/version equal the pinned expected identity AND the
     * rule id is a member of {@code admittedRuleIds}. Lets the filter admit
     * config-registered strategy ids without a code change per strategy —
     * the same strictness as the pinned forms, with the rule set supplied
     * by configuration instead of constants.
     */
    public static boolean isCanonicalIn(
            String schemaVersion,
            String strategyId,
            String strategyVersion,
            String ruleId,
            String expectedSchemaVersion,
            String expectedStrategyId,
            String expectedStrategyVersion,
            Collection<String> admittedRuleIds) {
        boolean identityMatch = expectedSchemaVersion != null
                && expectedSchemaVersion.equals(schemaVersion)
                && expectedStrategyId != null
                && expectedStrategyId.equals(strategyId)
                && expectedStrategyVersion != null
                && expectedStrategyVersion.equals(strategyVersion);
        return identityMatch && admittedRuleIds != null && admittedRuleIds.contains(ruleId);
    }
}
