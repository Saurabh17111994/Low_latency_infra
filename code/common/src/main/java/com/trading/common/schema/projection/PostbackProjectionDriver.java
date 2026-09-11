package com.trading.common.schema.projection;

import com.trading.common.schema.position.PositionSnapshot;
import com.trading.common.schema.position.PositionsStateStore;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * T6 projection orchestrator (CHG-045): normalizes a
 * {@link NormalizedPostback} through correlation &rarr; audit &rarr; lifecycle
 * &rarr; position &rarr; ledger COMPLETE, and routes every unknown/malformed/
 * ambiguous disposition to Postback_Quarantine + audit + halt of the affected
 * scope. A partial write never becomes COMPLETE until every required write is
 * acknowledged, so a crash/restart resumes from the recorded ledger state and
 * never issues a broker command (projection is read-only).
 *
 * <p>This engine is the pure-JVM offline authority: each durable boundary is
 * an interface whose in-memory twins make the whole flow test-closed. The
 * production Fluss-backed stores and the Rust normalized emitter are the
 * remaining live wiring (T6 final step).
 */
public final class PostbackProjectionDriver {

    private final CorrelationIndex correlationIndex;
    private final LifecycleStore lifecycleStore;
    private final PositionsStateStore positionStore;
    private final ProjectionLedgerStore ledgerStore;
    private final ProjectionAuditStore auditStore;
    private final PostbackQuarantineStore quarantineStore;
    private final NautilusPositionAuthority positionAuthority;
    private final String actorId;
    private final long gateEpoch;
    /**
     * T6 projection orchestrator (CHG-045). Single-threaded by contract —
     * retained fields ({@code haltedScopeIds}, see below) are unsynchronized;
     * concurrent consumers must use one driver per thread or external locking.
     * Halts are sticky: once a scope halts it stays halted for the process
     * lifetime (no eviction — a halted scope must never silently un-halt).
     */
    // P3-408: LinkedHashSet is not thread-safe and Set.copyOf iterates — under
    // concurrent project() calls this races; confinement (above) is the rule.
    private final Set<String> haltedScopeIds =
            java.util.Collections.synchronizedSet(new LinkedHashSet<>());
    private String lastAppliedPositionId;
    // P3-171: lastAppliedPositionId is only ever assigned on APPLIED — but as a
    // retained field it leaks across project() calls: a non-fill postback (or a
    // DUPLICATE fill) would report a previous, unrelated postback's id, and
    // repeated calls are no longer deterministic. Reset per call; the APPLIED
    // branch sets it. Also a data race under concurrent consumers (see above).

    public PostbackProjectionDriver(CorrelationIndex correlationIndex,
            LifecycleStore lifecycleStore, PositionsStateStore positionStore,
            ProjectionLedgerStore ledgerStore, ProjectionAuditStore auditStore,
            PostbackQuarantineStore quarantineStore,
            NautilusPositionAuthority positionAuthority, String actorId, long gateEpoch) {
        this.correlationIndex = Objects.requireNonNull(correlationIndex, "correlationIndex");
        this.lifecycleStore = Objects.requireNonNull(lifecycleStore, "lifecycleStore");
        this.positionStore = Objects.requireNonNull(positionStore, "positionStore");
        this.ledgerStore = Objects.requireNonNull(ledgerStore, "ledgerStore");
        this.auditStore = Objects.requireNonNull(auditStore, "auditStore");
        this.quarantineStore = Objects.requireNonNull(quarantineStore, "quarantineStore");
        this.positionAuthority = Objects.requireNonNull(positionAuthority, "positionAuthority");
        this.actorId = actorId;
        this.gateEpoch = gateEpoch;
    }

    public enum Outcome { APPLIED, DUPLICATE, STALE, QUARANTINED, REJECTED }

    public record ProjectionResult(Outcome outcome, String postbackEventId,
            String positionId, QuarantineReason quarantineReason, String detail) {
    }

    public Set<String> haltedScopeIds() {
        return Set.copyOf(haltedScopeIds);
    }

    /**
     * Project one normalized postback. Deterministic ({@code nowMs} is a
     * parameter); side effects are confined to the injected stores.
     */
    public ProjectionResult project(NormalizedPostback postback, long nowMs) {
        Objects.requireNonNull(postback, "postback");
        // P3-171: per-call position id — never the previous postback's.
        lastAppliedPositionId = null;

        // --- Fingerprint integrity ------------------------------------------
        if (!PostbackFingerprint.matches(postback)) {
            return quarantine(postback, nowMs, QuarantineReason.FINGERPRINT_MISMATCH,
                    "fingerprint does not match canonical content", null);
        }

        // --- Idempotent ledger resume / duplicate ----------------------------
        Optional<ProjectionLedgerEntry> existing = lookupLedger(postback.postbackEventId());
        if (existing.isPresent()) {
            PostbackProjectionLedger.State s = existing.get().projectionState();
            if (s == PostbackProjectionLedger.State.COMPLETE
                    || s == PostbackProjectionLedger.State.QUARANTINED
                    || s == PostbackProjectionLedger.State.FAILED) {
                return new ProjectionResult(Outcome.DUPLICATE, postback.postbackEventId(),
                        null, null, "already " + s);
            }
        }

        transition(postback.postbackEventId(), nowMs,
                PostbackProjectionLedger.State.RECEIVED,
                PostbackProjectionLedger.State.CORRELATED, null);

        // --- Correlation precedence -----------------------------------------
        PostbackCorrelator.CorrelationResult cr =
                PostbackCorrelator.correlate(postback, correlationIndex);
        if (cr.outcome() == PostbackCorrelator.Outcome.QUARANTINED) {
            PostbackCorrelator.Quarantined q = (PostbackCorrelator.Quarantined) cr;
            return quarantine(postback, nowMs, q.reason(), q.detail(), q.ref());
        }
        AttemptRef ref = cr.ref();

        // --- Audit (immutable evidence) -------------------------------------
        appendAudit(postback, nowMs, "POSTBACK_RECEIVED",
                "correlated via " + (postback.hasBrokerOrderId() ? "broker_order_id"
                        : "echoed_client_order_ref"));
        transition(postback.postbackEventId(), nowMs,
                PostbackProjectionLedger.State.CORRELATED,
                PostbackProjectionLedger.State.AUDIT_WRITTEN, null);

        // --- Lifecycle monotonicity -----------------------------------------
        Optional<OrderLifecycleSnapshot> lifecycle = lookupLifecycle(ref, postback);
        OrderLifecycleProjector.LifecycleResult lr =
                OrderLifecycleProjector.apply(lifecycle.orElse(null), postback, ref, nowMs);
        switch (lr.outcome()) {
            case APPLIED -> upsertLifecycle(lr.snapshot());
            case DUPLICATE -> { /* no-op, continue to position (already-applied order) */ }
            case STALE -> {
                appendAudit(postback, nowMs, "POSTBACK_STALE",
                        "dropped stale lifecycle evidence (older source version)");
                return finishStale(postback, nowMs);
            }
            case CONFLICT, REGRESSION, UNKNOWN -> {
                return quarantine(postback, nowMs, lr.reason(), lr.detail(), ref);
            }
        }
        transition(postback.postbackEventId(), nowMs,
                PostbackProjectionLedger.State.AUDIT_WRITTEN,
                PostbackProjectionLedger.State.LIFECYCLE_APPLIED, null);

        // --- Position (serialize Nautilus-computed result, no arithmetic) ---
        if (postback.isFill()) {
            // P3-399: an authority throwing IAE (impossible Nautilus event —
            // ctor invariants) must quarantine + halt like any other
            // POSITION_VIOLATION, not escape as an unchecked exception past
            // the quarantine path the NautilusPositionEvent docs describe.
            final Optional<NautilusPositionEvent> event;
            try {
                event = positionAuthority.apply(postback);
            } catch (IllegalArgumentException impossible) {
                return quarantine(postback, nowMs, QuarantineReason.POSITION_VIOLATION,
                        "Nautilus event violates quantity/state invariants: " + impossible.getMessage(), ref);
            }
            if (event.isEmpty()) {
                return quarantine(postback, nowMs, QuarantineReason.UNKNOWN_POSTBACK_TYPE,
                        "fill produced no Nautilus position event", ref);
            }
            NautilusPositionEvent ne = event.get();
            Optional<PositionSnapshot> current = lookupPosition(ne.positionId());
            PositionProjectionWriter.PositionWriteResult pw =
                    PositionProjectionWriter.apply(current.orElse(null), ne, nowMs);
            switch (pw.outcome()) {
                case APPLIED -> {
                    upsertPosition(pw.snapshot());
                    lastAppliedPositionId = pw.snapshot().positionId();
                }
                case DUPLICATE -> { /* already reflected */ }
                case STALE -> {
                    appendAudit(postback, nowMs, "POSITION_STALE",
                            "dropped stale position update " + ne.positionId());
                    return finishStale(postback, nowMs);
                }
                case VIOLATION -> {
                    return quarantine(postback, nowMs, pw.reason(), pw.detail(), ref);
                }
            }
        }
        transition(postback.postbackEventId(), nowMs,
                PostbackProjectionLedger.State.LIFECYCLE_APPLIED,
                PostbackProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED, null);

        transition(postback.postbackEventId(), nowMs,
                PostbackProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED,
                PostbackProjectionLedger.State.COMPLETE,
                Long.valueOf(nowMs));

        String positionId = lastAppliedPositionId;
        return new ProjectionResult(Outcome.APPLIED, postback.postbackEventId(),
                positionId, null, null);
    }

    // --- helpers -------------------------------------------------------------

    private ProjectionResult finishStale(NormalizedPostback p, long nowMs) {
        // P3-514: record the real completion time (not null) and the true prior
        // state at each call site — POSITION_APPLIED_OR_NOT_REQUIRED here is
        // only valid for the position-STALE branch, not lifecycle-STALE.
        PostbackProjectionLedger.State prior = ledgerPrior(p.postbackEventId(), nowMs);
        transition(p.postbackEventId(), nowMs, prior,
                PostbackProjectionLedger.State.COMPLETE, Long.valueOf(nowMs));
        return new ProjectionResult(Outcome.STALE, p.postbackEventId(), null, null,
                "stale evidence rejected");
    }

    private ProjectionResult quarantine(NormalizedPostback p, long nowMs,
            QuarantineReason reason, String detail, AttemptRef ref) {
        QuarantinedPostback q = new QuarantinedPostback(
                "q-" + p.postbackEventId(),
                p.postbackEventId(),
                reason,
                new byte[0],
                p.originalPayloadHash(),
                p.brokerOrderId(),
                ref == null ? null : ref.instructionId(),
                ref == null ? null : ref.executionAttemptId(),
                "OPEN",
                detail,
                nowMs,
                null,
                "2");
        try {
            quarantineStore.append(q);
        } catch (Exception e) {
            throw new RuntimeException("quarantine append failed", e);
        }
        appendAudit(p, nowMs, "POSTBACK_QUARANTINED", reason + ": " + detail);
        haltedScopeIds.add(p.accountScopeId());
        transition(p.postbackEventId(), nowMs,
                PostbackProjectionLedger.State.CORRELATED,
                PostbackProjectionLedger.State.QUARANTINED, null);
        return new ProjectionResult(Outcome.QUARANTINED, p.postbackEventId(),
                null, reason, detail);
    }

    private void transition(String id, long nowMs, PostbackProjectionLedger.State from,
            PostbackProjectionLedger.State to, Long completedTs) {
        try {
            ProjectionLedgerEntry prior = ledgerStore.lookup(id).orElse(new ProjectionLedgerEntry(
                    id, from, null, 0, null, null, nowMs, null, "2"));
            PostbackProjectionLedger.State resolvedFrom = prior.projectionState();
            PostbackProjectionLedger.State advanced;
            try {
                advanced = PostbackProjectionLedger.next(resolvedFrom, to);
            } catch (IllegalStateException e) {
                // Already progressed past `to` (idempotent re-run after restart).
                advanced = resolvedFrom;
            }
            ledgerStore.put(new ProjectionLedgerEntry(
                    id, advanced, from.name(), prior.retryCount(), null,
                    null, nowMs, completedTs, "2"));
        } catch (Exception e) {
            throw new RuntimeException("ledger write failed", e);
        }
    }

    private void appendAudit(NormalizedPostback p, long nowMs, String eventType, String summary) {
        try {
            auditStore.append(new ProjectionAuditRecord(
                    "aud-" + p.postbackEventId() + "-" + eventType,
                    eventType, null, null,
                    "partition-" + p.accountScopeId(),
                    p.accountScopeId(), gateEpoch, actorId,
                    p.originalPayloadHash(), summary, nowMs, "2"));
        } catch (Exception e) {
            throw new RuntimeException("audit append failed", e);
        }
    }

    private Optional<ProjectionLedgerEntry> lookupLedger(String id) {
        try {
            return ledgerStore.lookup(id);
        } catch (Exception e) {
            throw new RuntimeException("ledger read failed", e);
        }
    }

    private PostbackProjectionLedger.State ledgerPrior(String id, long nowMs) {
        return lookupLedger(id).map(e -> e.projectionState())
                .orElse(PostbackProjectionLedger.State.RECEIVED);
    }

    private Optional<OrderLifecycleSnapshot> lookupLifecycle(AttemptRef ref, NormalizedPostback p) {
        try {
            return lifecycleStore.lookup(ref.accountScopeId(), p.brokerOrderId());
        } catch (Exception e) {
            throw new RuntimeException("lifecycle read failed", e);
        }
    }

    private void upsertLifecycle(OrderLifecycleSnapshot s) {
        try {
            lifecycleStore.upsert(s);
        } catch (Exception e) {
            throw new RuntimeException("lifecycle upsert failed", e);
        }
    }

    private Optional<PositionSnapshot> lookupPosition(String positionId) {
        try {
            return Optional.ofNullable(positionStore.lookup(positionId));
        } catch (Exception e) {
            throw new RuntimeException("position read failed", e);
        }
    }

    private void upsertPosition(PositionSnapshot s) {
        try {
            positionStore.upsert(s);
        } catch (Exception e) {
            throw new RuntimeException("position upsert failed", e);
        }
    }

}
