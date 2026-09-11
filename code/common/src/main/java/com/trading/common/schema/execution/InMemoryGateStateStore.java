package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link GateStateStore} — the offline durable boundary the
 * {ExecutionCommandGate} protocol engine and its crash-window/zero-duplicate
 * tests run against (T5, CHG-044; CHG-056 single-operator). It is deliberately NOT a production cache:
 * it can only be a lease/fence authority when the real writer is a gateway-backed
 * Fluss store using the deployment leadership/fencing mechanism (ASM-EXE-005 /
 * REQ-EXE-012). A Fluss-backed implementation satisfies the same interface.
 *
 * <p>It enforces the writes that a raw KV upsert must never be trusted to
 * provide: monotonic fence tokens with concurrent-owner rejection, single-operator
 * (Saurabh) approval tied to an exact (epoch, evidence hash), and
 * an exactly-once epoch increment on a safety halt.
 */
public final class InMemoryGateStateStore implements GateStateStore {

    private final Map<String, GateRow> rows = new LinkedHashMap<>();
    private final List<AuditRecord> audit = new ArrayList<>();
    private final AtomicLong fenceSequence = new AtomicLong();
    /**
     * Authorized approvers. An empty set means "any principal" and is reachable only through the
     * explicitly-named {@link #anyApprover()} seam.
     */
    private final Set<String> authorizedApprovers;

    /**
     * Test/live seam: no approver allow-list, so ANY principal may approve.
     *
     * <p>P3-142/P3-143: this behaviour is opt-in <i>by name</i>. The set-taking constructor
     * rejects an empty set, so a missing or misconfigured allow-list can no longer silently turn
     * the approver check off — it fails at construction instead.
     */
    public static InMemoryGateStateStore anyApprover() {
        return new InMemoryGateStateStore(Set.of(), true);
    }

    public InMemoryGateStateStore() {
        this(Set.of(), true);
    }

    public InMemoryGateStateStore(Set<String> authorizedApprovers) {
        this(authorizedApprovers, false);
    }

    /** Internal seam: {@code anyPrincipalAllowed} is only ever set by {@link #anyApprover()}. */
    InMemoryGateStateStore(Set<String> authorizedApprovers, boolean anyPrincipalAllowed) {
        Objects.requireNonNull(authorizedApprovers, "authorizedApprovers");
        if (authorizedApprovers.isEmpty() && !anyPrincipalAllowed) {
            throw new IllegalArgumentException(
                    "authorizedApprovers must not be empty: an empty allow-list accepts ANY "
                            + "principal as an approver (P3-142/P3-143). If any-principal behaviour "
                            + "is genuinely intended, request it by name with "
                            + "InMemoryGateStateStore.anyApprover().");
        }
        this.authorizedApprovers = Set.copyOf(authorizedApprovers);
    }

    @Override
    public synchronized GateRow read(String partitionId) {
        return rows.get(partitionId);
    }

    @Override
    public synchronized GateRow init(GateRow boot) {
        Objects.requireNonNull(boot, "boot");
        GateRow existing = rows.get(boot.partitionId());
        if (existing == null) {
            fenceSequence.accumulateAndGet(boot.fenceToken(), Math::max);
            rows.put(boot.partitionId(), boot);
            return boot;
        }
        return existing; // already initialized — never clobber a fenced gate
    }

    @Override
    public synchronized FenceResult acquire(String partitionId, String owner, long leaseMs,
                                            long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return FenceResult.conflict(null, "no gate row for partition " + partitionId);
        }
        // Concurrent-owner rejection: a live (unexpired) lease held by a
        // DIFFERENT instance blocks acquisition. Same owner refreshes.
        if (row.ownerInstanceId() != null && !row.fenceExpiredAt(nowTs)
                && !row.ownerInstanceId().equals(owner)) {
            return FenceResult.conflict(row,
                    "live lease held by " + row.ownerInstanceId());
        }
        long token = fenceSequence.incrementAndGet();
        GateRow next = row.withFence(owner, token, nowTs, leaseMs);
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "FENCE_ACQUIRE", nowTs, next.epoch(), token,
                "owner=" + owner, null));
        return FenceResult.acquired(next, owner, token);
    }

    @Override
    public synchronized FenceResult renew(String partitionId, String ownerInstanceId, long fenceToken,
                                          long leaseMs, long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return FenceResult.conflict(null, "no gate row for partition " + partitionId);
        }
        if (row.ownerInstanceId() == null) {
            return FenceResult.conflict(row, "no live lease to renew");
        }
        if (!row.ownerInstanceId().equals(ownerInstanceId)) {
            return FenceResult.conflict(row,
                    "renew rejected: holder is " + row.ownerInstanceId() + " not " + ownerInstanceId);
        }
        // P3-148: a recorded fence loss never renews — the holder must
        // re-acquire. (withFenceLost clears the lease, so the expiry check
        // below would pass on a lost fence without this guard.)
        if (row.fenceLostTs() != null) {
            return FenceResult.conflict(row,
                    "renew rejected: fence lost at " + row.fenceLostTs() + ", re-acquire required");
        }
        if (row.fenceToken() != fenceToken) {
            return FenceResult.conflict(row,
                    "renew rejected: token mismatch expected " + row.fenceToken() + " got " + fenceToken);
        }
        if (row.fenceExpiredAt(nowTs)) {
            return FenceResult.conflict(row,
                    "renew rejected: lease expired at " + row.leaseExpiresTs() + " now " + nowTs);
        }
        // Success: extend lease without changing fenceToken (offline lease semantics)
        GateRow next = row.withRenewedLease(nowTs, leaseMs);
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "FENCE_RENEW", nowTs, next.epoch(), next.fenceToken(),
                "owner=" + ownerInstanceId + " leaseMs=" + leaseMs, null));
        return FenceResult.acquired(next, ownerInstanceId, fenceToken);
    }

    @Override
    public synchronized GateRow revoke(String partitionId, String ownerInstanceId, long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return null;
        }
        if (row.ownerInstanceId() == null) {
            // already cleared — idempotent
            return row;
        }
        if (!row.ownerInstanceId().equals(ownerInstanceId)) {
            // not holder — fail closed, no mutation (offline fencing constraint)
            return row;
        }
        GateRow cleared = row.withFenceCleared(nowTs);
        rows.put(partitionId, cleared);
        audit(new AuditRecord(partitionId, "FENCE_REVOKE", nowTs, cleared.epoch(), cleared.fenceToken(),
                "owner=" + ownerInstanceId, null));
        return cleared;
    }

    @Override
    public synchronized ApprovalResult approve(String partitionId, String principal,
                                               long epoch, String evidenceHash, long nowTs) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(evidenceHash, "evidenceHash");
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return ApprovalResult.of(ApprovalOutcome.NOT_FOUND, null, "no gate row");
        }
        if (row.epoch() != epoch) {
            return ApprovalResult.of(ApprovalOutcome.EPOCH_MISMATCH, row,
                    "approval epoch " + epoch + " != current gate epoch " + row.epoch());
        }
        if (!authorizedApprovers.isEmpty() && !authorizedApprovers.contains(principal)) {
            return ApprovalResult.of(ApprovalOutcome.UNAUTHORIZED, row,
                    "principal " + principal + " not authorized");
        }
        // P3-015: an approval covers the exact (epoch, evidence hash) the gate
        // holds — approving any other package is a mismatch, not an approval.
        // Checked after auth so a wrong principal still reports UNAUTHORIZED
        // (GatewayHttpServer halts on either; auth identity comes first).
        // A null row evidence means no package bound yet (boot/HALTED with no
        // evidence) — the first approval defines the binding, so only enforce
        // equality when the gate already holds a package.
        if (row.evidenceHash() != null && !Objects.equals(row.evidenceHash(), evidenceHash)) {
            // P3-151: a distinct outcome, not EPOCH_MISMATCH — the epoch is right,
            // the evidence package is not, and the two need different operator action.
            return ApprovalResult.of(ApprovalOutcome.EVIDENCE_MISMATCH, row,
                    "approval evidence mismatch: gate holds a different evidence package");
        }
        if (row.approvalsComplete()) {
            return ApprovalResult.of(ApprovalOutcome.ALREADY_APPLIED, row, "approvals complete");
        }
        // P3-383: under single-operator semantics one approval completes the
        // gate, so reaching here with approval1 set means a partial edge-state
        // row (approval recorded without evidence via install/hydrate) — the
        // SAME_PRINCIPAL arm stays as its guard, never a second slot.
        if (row.approval1() != null && row.approval1().equals(principal)) {
            return ApprovalResult.of(ApprovalOutcome.SAME_PRINCIPAL, row,
                    "approver " + principal + " already approved once");
        }
        // P3-383: under single-operator semantics one approval completes the
        // gate, so reaching here with approval1 set means a prior approve stored
        // a partial row (null evidence) — treat as already applied, never a
        // second slot. The SAME_PRINCIPAL arm above stays as the edge-state guard.
        if (row.approval1() != null) {
            return ApprovalResult.of(ApprovalOutcome.ALREADY_APPLIED, row, "approvals complete");
        }
        GateRow next = new GateRow(row.partitionId(), row.accountScopeId(), row.state(), row.epoch(),
                row.reason(), row.evidenceHash(), principal, null, evidenceHash,
                row.ownerInstanceId(), row.fenceToken(), row.fenceAcquiredTs(),
                row.leaseExpiresTs(), row.fenceLostTs());
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "APPROVE", nowTs, next.epoch(), next.fenceToken(),
                "principal=" + principal, evidenceHash));
        return promoteIfCompleteLocked(partitionId, ApprovalResult.applied(next), nowTs);
    }

    /**
     * P3-075: single synchronized transition — approve and, when the approval
     * completes the gate, promote the SAME row to ENABLED. A caller doing
     * approve() then a separate install() lets a concurrent halt() land between
     * the two calls and be overwritten (ENABLED resurrected over HALTED).
     */
    @Override
    public synchronized ApprovalResult approveAndEnableIfComplete(String partitionId, String principal,
            long epoch, String evidenceHash, long nowTs) {
        ApprovalResult res = approve(partitionId, principal, epoch, evidenceHash, nowTs);
        if (res.outcome() != ApprovalOutcome.APPLIED) return res;
        return promoteIfCompleteLocked(partitionId, res, nowTs);
    }

    private ApprovalResult promoteIfCompleteLocked(String partitionId, ApprovalResult res, long nowTs) {
        GateRow approved = res.row();
        if (approved.state() == GateState.APPROVAL_PENDING && approved.approvalsComplete()) {
            GateRow enabled = new GateRow(approved.partitionId(), approved.accountScopeId(),
                    GateState.ENABLED, approved.epoch(), "approved " + approved.evidenceHash(),
                    approved.evidenceHash(), approved.approval1(), approved.approval2(),
                    approved.approvedEvidenceHash(), approved.ownerInstanceId(),
                    approved.fenceToken(), approved.fenceAcquiredTs(), approved.leaseExpiresTs(),
                    approved.fenceLostTs());
            rows.put(partitionId, enabled);
            audit(new AuditRecord(partitionId, "ENABLE", nowTs, enabled.epoch(), enabled.fenceToken(),
                    "approved " + enabled.evidenceHash(), enabled.evidenceHash()));
            return ApprovalResult.applied(enabled);
        }
        return res;
    }

    @Override
    public synchronized GateRow halt(String partitionId, GateRow expected, String reason,
                                     String evidenceHash, long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return null;
        }
        // P3-157: expected is the CAS witness — a stale caller (epoch behind the
        // current row) fails closed with no mutation, never bumps the epoch.
        // A null expected means "halt unconditionally" (safety paths that cannot
        // name a generation — GatewayHttpServer unauthorized/epoch-mismatch halts).
        if (expected != null && row.epoch() != expected.epoch()) {
            return row;
        }
        // P3-374/P3-010: the halt write carries a STRICTLY GREATER fence token. The durable
        // gate table orders writes by fence_token (VERSIONED merge engine), so a halt that
        // kept the old token — or reset it to 0 — could be dropped by the tablet and the
        // safety halt silently lost. Minting forward guarantees the halt always wins, and
        // retires any stale holder's token in the same write.
        long haltToken = fenceSequence.incrementAndGet();
        GateRow cleared;
        if (row.state() == GateState.HALTED) {
            // Idempotent safe halt while already HALTED: no second epoch increment.
            // HALTED default is fenced-off: ensure fence is cleared even on idempotent halt.
            // P3-384: carry the latest halt evidence on the same epoch (a new
            // reason/evidence package must not be dropped while the audit logs it),
            // and record a fresh fenceLostTs even when the fence is already cleared.
            GateRow c = row.withFenceCleared(nowTs);
            cleared = new GateRow(c.partitionId(), c.accountScopeId(), c.state(),
                    c.epoch(), reason != null ? reason : c.reason(),
                    evidenceHash != null ? evidenceHash : c.evidenceHash(),
                    c.approval1(), c.approval2(), c.approvedEvidenceHash(),
                    c.ownerInstanceId(), c.fenceToken(), c.fenceAcquiredTs(),
                    c.leaseExpiresTs(), nowTs);
        } else {
            cleared = row.withState(GateState.HALTED, reason, evidenceHash == null
                    ? row.evidenceHash() : evidenceHash).withFenceCleared(nowTs);
        }
        GateRow next = new GateRow(cleared.partitionId(), cleared.accountScopeId(), cleared.state(),
                cleared.epoch(), cleared.reason(), cleared.evidenceHash(), cleared.approval1(),
                cleared.approval2(), cleared.approvedEvidenceHash(), null, haltToken, null, null, nowTs);
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "HALT", nowTs, next.epoch(), next.fenceToken(),
                reason, evidenceHash));
        return next;
    }

    @Override
    public synchronized void audit(AuditRecord record) {
        audit.add(record);
    }

    @Override
    public synchronized List<AuditRecord> auditLog() {
        return List.copyOf(audit);
    }

    /** Test/migration helper: directly install a gate row (e.g. an already-ENABLED fenced gate). */
    public synchronized void install(GateRow row) {
        Objects.requireNonNull(row, "row");
        fenceSequence.accumulateAndGet(row.fenceToken(), Math::max);
        rows.put(row.partitionId(), row);
    }

    /**
     * Restart-refresh: install a durable gate row recovered from Fluss and advance the local
     * fence sequence to at least its token, so a subsequent {@link #acquire} in this process is
     * still strictly greater than any token already issued durably (monotonic across a restart).
     */
    public synchronized void hydrate(GateRow row) {
        Objects.requireNonNull(row, "row");
        fenceSequence.accumulateAndGet(row.fenceToken(), Math::max);
        rows.put(row.partitionId(), row);
    }
}
