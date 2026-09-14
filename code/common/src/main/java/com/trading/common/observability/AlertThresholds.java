package com.trading.common.observability;

/**
 * Alert thresholds (docs/08_implementation/01-foundation.md &rarr; "Observability invariant", orig L727).
 *
 * <p>{@link Alert#condition} is the human-readable condition published as the
 * {@code alert.condition} attribute. The two percentage rows are written from
 * {@link #CONTAINER_MEMORY_ALERT_PERCENT} and {@link #PENDING_APPEND_WARNING_PERCENT},
 * and the enforcing components read the same constants, so the published text and
 * the enforced setpoint cannot drift apart.
 *
 * <p>This class holds the shared {@link #CONSECUTIVE_BREACH_SECONDS} bound; it does
 * not debounce anything. The window is applied where the alert is evaluated: the
 * JVM-side {@code JvmHeapReadinessGate} reads the constant, and the infrastructure
 * rules provisioned by {@code o2-provision.py} carry their own {@code for 60s}.
 *
 * <p>The safety halt is idempotent and never auto-resumes (handled by
 * {@link SafetyHaltRequest}).
 *
 * <p>Rows whose doc wording is open-ended, and the value each is implemented
 * against (docs/08_implementation/10-observability.md &rarr; "Alert thresholds",
 * docs/02_requirements/03-non-functional.md, docs/06_operations/01-runbooks.md):
 * <ul>
 *   <li>{@code CONTAINER_MEMORY} — {@link #CONTAINER_MEMORY_ALERT_PERCENT} (85).</li>
 *   <li>{@code PENDING_APPEND_RECORDS}/{@code PENDING_APPEND_BYTES} —
 *       {@link #PENDING_APPEND_WARNING_PERCENT} (80): readiness is false from 80%,
 *       and the producer stops at 100%.</li>
 *   <li>{@code CHECKPOINT_DURATION} — the pinned checkpoint timeout
 *       ({@code PlatformConfig.CHECKPOINT_TIMEOUT_MS}, 30 s); the runbook alert
 *       {@code SIGNAL-error-checkpoint-slow} fires at 80% of it (24 s). The row
 *       states the relation, not a second number.
 *       <br>Open conflict: the 10-observability threshold table gives this row as
 *       "p99 &gt; 5 s" — 5 s, 24 s and the timeout are three different boundaries,
 *       so the row needs one owner before an evaluator is built.</li>
 *   <li>{@code CHECKPOINT_FAILURE_RATE} — the documented trigger is a single failed
 *       checkpoint ("Any checkpoint fails", critical, safety-halt request), not a rate.
 *       The enum name is kept as published in {@code alert.name}.</li>
 *   <li>{@code CHANGELOG_GAP} — a detected gap makes readiness false
 *       ({@code BAB-FAIL-001}) and is a gate {@code HALTED} trigger.</li>
 *   <li>{@code MISSING_FILL} — postback fill correlation; missing fill correlation is a
 *       gate {@code HALTED} trigger, and the SLA number is not pinned in the docs yet.</li>
 *   <li>{@code STALE_SIGNAL} — evidence-gated (DEC-028): the numeric freshness
 *       threshold is deferred, so nothing fires on this row yet.</li>
 * </ul>
 */
public final class AlertThresholds {

    private AlertThresholds() {}

    /** Consecutive-breach window shared by the alert rows (60 s). */
    public static final int CONSECUTIVE_BREACH_SECONDS = 60;

    /** Total container memory at/above which {@link Alert#CONTAINER_MEMORY} fires (percent). */
    public static final int CONTAINER_MEMORY_ALERT_PERCENT = 85;

    /** Pending records/bytes at which the warning fires and readiness goes false (percent). */
    public static final int PENDING_APPEND_WARNING_PERCENT = 80;

    public enum Alert {
        CONTAINER_MEMORY("container_memory_pct >= " + CONTAINER_MEMORY_ALERT_PERCENT),
        PENDING_APPEND_RECORDS(
                "pending_append_records >= " + PENDING_APPEND_WARNING_PERCENT + "% of limit"),
        PENDING_APPEND_BYTES(
                "pending_append_bytes >= " + PENDING_APPEND_WARNING_PERCENT + "% of limit"),
        CHECKPOINT_DURATION("checkpoint duration > timeout"),
        CHECKPOINT_FAILURE_RATE("any checkpoint failure"),
        CHANGELOG_GAP("detected changelog gap"),
        MISSING_FILL("postback fill missing beyond SLA"),
        STALE_SIGNAL("signal staleness beyond bound");

        public final String condition;

        Alert(String condition) {
            this.condition = condition;
        }
    }
}
