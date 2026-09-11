package com.trading.common.schema.execution;

import java.util.List;

/**
 * Durable boundary for the Execution_Gate table (v3, CHG-044; CHG-056 single-operator). A gateway-backed
 * implementation persists rows to Fluss; the offline engine composes against
 * this interface and an in-memory implementation so the protocol (order of
 * persists, fence concurrency, approvals, halting) is proven before the Fluss
 * writer is wired. The store owns fence acquisition (concurrent-owner
 * rejection), single-operator (Saurabh) approval registration, and the epoch-incrementing
 * halt — the writes that must never be inferred from a raw KV upsert.
 */
public interface GateStateStore {

    /** Outcome of an approval registration. */
    enum ApprovalOutcome {
        /** An authorized approval was recorded. */
        APPLIED,
        /** Approvals are already complete for this epoch/evidence. */
        ALREADY_APPLIED,
        /** The same principal tried to approve twice — single-operator gate already approved. */
        SAME_PRINCIPAL,
        /** The principal is not authorized to approve this scope. */
        UNAUTHORIZED,
        /** The approval is for a gate epoch that no longer matches the current row epoch. */
        EPOCH_MISMATCH,
        /** The approval's evidence hash does not match the row's bound evidence package (P3-151). */
        EVIDENCE_MISMATCH,
        /** No gate row exists for the partition. */
        NOT_FOUND
    }

    /** P3-376: {@code row} is null only when no gate row exists for the partition (NOT_FOUND). */
    record ApprovalResult(ApprovalOutcome outcome, /* @Nullable */ GateRow row, String reason) {
        // P3-150: never a null reason — callers (GatewayHttpServer) must not
        // paper over a missing audit detail with a fallback string.
        public static ApprovalResult applied(GateRow row) {
            return new ApprovalResult(ApprovalOutcome.APPLIED, row, "approval recorded");
        }
        public static ApprovalResult of(ApprovalOutcome o, GateRow row, String reason) {
            return new ApprovalResult(o, row, reason == null ? o.name() : reason);
        }
    }

    /** Fence acquisition result. A conflict always carries the reason (P3-375) —
     * "live lease held by X" vs "token mismatch" vs "expired" is what tells a
     * split-brain from a stale lease, so it must never be dropped.
     * P3-376: {@code row} is null only when no gate row exists for the partition. */
    record FenceResult(/* @Nullable */ GateRow row, String owner, long token, boolean conflict,
            String reason) {
        public static FenceResult acquired(GateRow row, String owner, long token) {
            return new FenceResult(row, owner, token, false, null);
        }
        public static FenceResult conflict(GateRow row, String reason) {
            return new FenceResult(row, null, 0L, true, reason);
        }
    }

    /** Immutable audit/evidence event appended on every meaningful write (Execution_Audit shape). */
    record AuditRecord(
            String partitionId,
            String eventType,
            long ts,
            long gateEpoch,
            long fenceToken,
            String detail,
            String evidenceHash) {}

    /**
     * Current durable snapshot, or {@code null} if no row exists for the partition.
     * A null key returns null (P3-375); lookup failures throw — never null-as-absent.
     * P3-376: implementations return an immutable snapshot, never a live view.
     */
    /* @Nullable */ GateRow read(String partitionId);

    /**
     * Create a gate row if absent, else return the existing row untouched.
     * Never clobbers a fenced gate (P3-151): "recreate" here means
     * return-existing, not reset. Null boot throws NullPointerException.
     */
    GateRow init(GateRow haltedBootRow);

    /**
     * Acquire (or refresh) the partition lease for {@code owner}. Fails closed
     * on a live lease held by a different owner (concurrent-owner rejection),
     * mirroring the monotonic fencing sequence: the returned token is always
     * strictly greater than any prior fence token for the partition.
     *
     * <p>P3-149: implementations MUST execute the live-lease check and the token
     * bump + durable persist atomically (linearizable per partition) — concurrent
     * acquirers must not both succeed. This operation is check-then-act, so a
     * caller-visible interleaving between the check and the persist would violate
     * the strictly-greater-token contract; where the backing store cannot express
     * that natively, the implementation must exclude it (the in-memory engine
     * synchronizes; the Fluss writer additionally re-reads and fails closed if the
     * durable row no longer carries its owner+token).
     */
    FenceResult acquire(String partitionId, String ownerInstanceId, long leaseMs, long nowTs);

    /**
     * Renew the lease for the current holder without changing fenceToken.
     * Extends leaseExpiresTs to nowTs+leaseMs only if holder and token match
     * and lease is still live. Do NOT increment fenceToken.
     * Returns conflict on mismatch, stale token, expired lease, or no live lease.
     */
    FenceResult renew(String partitionId, String ownerInstanceId, long fenceToken, long leaseMs, long nowTs);

    /** P3-150: what a revoke actually did. It must never be inferred from the returned row. */
    enum RevokeOutcome {
        /** The fence was cleared by this call. */
        REVOKED,
        /** The fence was already clear (no owner) — idempotent, no mutation. */
        ALREADY_CLEAR,
        /** A different owner holds the fence — refused, no mutation (offline fencing constraint). */
        NOT_OWNER,
        /** No row exists for the partition at all (P3-376). */
        NOT_FOUND
    }

    /**
     * P3-150: the outcome of a {@link #revoke}, with the row as it stands after the call.
     *
     * <p>{@code row} is null only for {@link RevokeOutcome#NOT_FOUND}. Every other outcome carries
     * a row, so "a row came back" says nothing about whether the revoke was applied — that is what
     * {@link #revoked()} is for. {@link RevokeOutcome#REVOKED} is the only outcome that mutated
     * durable state; {@code ALREADY_CLEAR} and {@code NOT_OWNER} both return an unchanged row and
     * were previously indistinguishable from each other and from success.
     */
    record RevokeResult(RevokeOutcome outcome, GateRow row) {
        /** Whether THIS call cleared the fence. */
        public boolean revoked() {
            return outcome == RevokeOutcome.REVOKED;
        }

        /** Whether the fence is now clear for this partition (revoked now, or already clear). */
        public boolean fenceIsClear() {
            return outcome == RevokeOutcome.REVOKED || outcome == RevokeOutcome.ALREADY_CLEAR;
        }
    }

    /**
     * Revoke (release) the fence, clearing owner and lease fields and recording fenceLostTs.
     * Only the current holder may revoke; others are refused with no mutation.
     * HALT path uses {@link #halt} which clears unconditionally.
     *
     * <p>P3-374: the {@code fenceToken} is <b>retained</b> rather than reset to 0. It is the
     * durable write-ordering version (VERSIONED merge engine), so a 0 would make the revoke
     * version-droppable, and "no fence" is already expressed by {@code ownerInstanceId == null}.
     *
     * <p>P3-377: every caller supplies an explicit {@code nowTs} — the wall-clock
     * convenience overloads and the {@code release} alias were removed, so no
     * hidden {@code System.currentTimeMillis()} can enter fenceLostTs/lease
     * horizons (crash-window replay and TTL tests stay deterministic).
     *
     * <p>P3-150: returns an explicit outcome; never a bare row (see {@link RevokeResult}).
     */
    RevokeResult revoke(String partitionId, String ownerInstanceId, long nowTs);

    /**
     * Register the single required approval for the exact
     * {@code (epoch, evidenceHash)} by the authorized operator (Saurabh). A
     * changed epoch invalidates pending approvals. (DEC-044 single-operator).
     *
     * <p>P3-151 evidence binding: {@code evidenceHash} must be non-null and must
     * equal the row's bound {@code evidenceHash} — otherwise the approval would
     * record stale/unauthorized evidence as APPLIED. A mismatch returns
     * {@link ApprovalOutcome#EVIDENCE_MISMATCH} with no mutation. A null hash is a
     * caller defect and is rejected fail-fast (never stored as a never-complete
     * approval). A row with no evidence bound yet (boot/HALTED) is the one
     * exception: the first approval defines the binding.
     */
    ApprovalResult approve(String partitionId, String principal, long epoch, String evidenceHash,
                           long nowTs);

    /**
     * P3-075: atomic approve-then-enable. Registers the approval exactly like
     * {@link #approve} and, when it completes the gate, promotes the SAME row
     * to ENABLED in one state transition — callers must not do read-approve-then-separate-write
     * (two synchronized calls let a concurrent halt() land between them and be
     * overwritten, and a non-InMemory impl has no way to receive the ENABLED write).
     * Default delegates to approve() and returns without ENABLED promotion
     * (promotion only where the impl overrides) — the caller replies with the
     * returned row's actual state, never an assumed "ENABLED".
     */
    default ApprovalResult approveAndEnableIfComplete(String partitionId, String principal,
            long epoch, String evidenceHash, long nowTs) {
        return approve(partitionId, principal, epoch, evidenceHash, nowTs);
    }

    /**
     * Raise a safety halt: move the gate to HALTED and increment the epoch by
     * exactly one, from {@code expected}. A null expected halts unconditionally
     * (P3-376) — the safety paths that cannot name a generation (unauthorized /
     * epoch-mismatch halts) rely on this. Idempotent (already HALTED) halts
     * record evidence without a second epoch increment. HALTED default is
     * fenced-off: halt clears the fence (owner null, fenceToken 0, lease null).
     * Returns {@code null} when no row exists for the partition (P3-376).
     *
     * <p>P3-152 CAS semantics: {@code expected} is a compare-and-set witness, not
     * advisory. When {@code expected} is non-null and its epoch differs from the
     * current row's, the halt is <b>not</b> applied and the current (unchanged) row
     * is returned — no mutation, no epoch bump — so a stale caller cannot
     * unconditionally halt/increment a newer generation. Callers that must
     * distinguish "halted" from "stale witness rejected" re-read the row or
     * compare the returned row's epoch; the safety path therefore passes the
     * freshest row it has (or null when it cannot name a generation).
     *
     * <p>P3-364/P3-369: {@code detectedTs} records when the condition being acted on was
     * <b>detected</b>. For a safety halt that is genuinely earlier than {@code nowTs} — the
     * evidence is detected on the safety path and only replayed into the gate afterwards — so it
     * is persisted separately in the nullable Execution_Gate {@code detection_time} column. Null
     * means no detection was recorded for this transition.
     */
    /* @Nullable */ GateRow halt(String partitionId, GateRow expected, String reason,
            String evidenceHash, long nowTs, Long detectedTs);

    /** As {@link #halt(String, GateRow, String, String, long, Long)} with no detection recorded. */
    default GateRow halt(String partitionId, GateRow expected, String reason,
            String evidenceHash, long nowTs) {
        return halt(partitionId, expected, reason, evidenceHash, nowTs, null);
    }

    /** Append an immutable audit/evidence event. */
    void audit(AuditRecord record);

    /**
     * The immutable audit log, in append order (crash-window reconstruction).
     * Implementations MUST return an unmodifiable snapshot copy (P3-376) — a
     * live list could be reordered or mutated after the read, defeating the
     * crash-window reconstruction the log exists for. In-memory by design and
     * unbounded: this is the evidence a post-crash reconcile reads — evicting it
     * would silently drop what reconciliation needs. Durable implementations
     * page at the backing store.
     */
    List<AuditRecord> auditLog();
}
