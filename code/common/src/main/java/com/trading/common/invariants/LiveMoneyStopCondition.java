package com.trading.common.invariants;

/**
 * The ten documented live-money stop conditions (01-foundation.md,
 * "Live-money stop conditions"). Live-money order placement must remain
 * DISABLED while any of these is true.
 *
 * The guard is intentionally dependency-free: it only knows the condition
 * vocabulary and an evaluator. The surrounding runtime (executor / coordinator
 * / CI) supplies the actual facts.
 */
public enum LiveMoneyStopCondition {
    CRITICAL_RISK_OPEN("critical-risk-open", "A critical risk is open"),
    BROKER_IDENTITY_UNVERIFIED("broker-identity-unverified",
            "A broker/protocol identity or response behavior is unverified"),
    FLUSS_FLINK_CAPABILITY_UNVERIFIED("fluss-flink-capability-unverified",
            "A Fluss/Flink capability is assumed but not version-tested"),
    DDL_REQUIREMENTS_DISAGREE("ddl-requirements-disagree", "DDL and requirements disagree"),
    EXECUTOR_STATE_INVALID("executor-state-invalid",
            "Executor state is missing, corrupt, unfenced, or not auditable"),
    ATTEMPT_OUTCOME_UNRESOLVED("attempt-outcome-unresolved", "An attempt has an unresolved outcome"),
    CHANGELOG_CHECKPOINT_UNKNOWN("changelog-checkpoint-unknown",
            "Changelog continuity or checkpoint health is unknown"),
    SAFE_HALT_RESUME_UNPROVEN("safe-halt-resume-unproven", "Safe-halt or single-operator (Saurabh, DEC-044) resume is unproven"),
    OBSERVABILITY_UNAVAILABLE("observability-unavailable", "Required observability is unavailable"),
    EOD_AUDIT_RETENTION_UNVERIFIED("eod-audit-retention-unverified",
            "EOD data or audit retention is unverified");

    // P3-114: stable id, independent of the Java name — never rename once persisted.
    private final String stableId;
    private final String description;

    LiveMoneyStopCondition(String stableId, String description) {
        this.stableId = stableId;
        this.description = description;
    }

    /** Human-readable description used in halt records and alerts. */
    public String getDescription() {
        return description;
    }

    /** Stable id used in audit / halt records — independent of the Java name, never rename once persisted. */
    public String conditionId() {
        return stableId;
    }
}
