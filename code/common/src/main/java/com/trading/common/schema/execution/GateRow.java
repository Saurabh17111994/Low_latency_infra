package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import java.util.Objects;

/**
 * Durable snapshot of one Execution_Gate row (v3, CHG-044) — the authorization
 * surface a money-moving command is checked against. It carries the gate
 * lifecycle ({@code state}/{@code epoch}), the single-operator (DEC-044) approval
 * evidence ({@code approval_1}/{@code approval_2}/{@code approvedEvidenceHash} —
 * a second approval is not required and not checked), and the
 * fenced lease ({@code ownerInstanceId}/{@code fenceToken}/{@code leaseExpiresTs}
 * plus acquisition/loss evidence). {@code epoch} is the gate-generation value;
 * {@code fenceToken} is the per-partition owner sequence that must still be
 * valid immediately before every authorized bridge command.
 *
 * <p>Validation helpers here are pure and shared by the offline engine and its
 * tests, so "reject stale lease/fence/epoch immediately before the bridge
 * command" has a single implementation.
 */
public record GateRow(
        String partitionId,
        String accountScopeId,
        GateState state,
        long epoch,
        String reason,
        String evidenceHash,
        String approval1,
        String approval2,
        String approvedEvidenceHash,
        String ownerInstanceId,
        long fenceToken,
        Long fenceAcquiredTs,
        Long leaseExpiresTs,
        Long fenceLostTs) {

    public GateRow {
        Objects.requireNonNull(partitionId, "partitionId");
        Objects.requireNonNull(accountScopeId, "accountScopeId");
        Objects.requireNonNull(state, "state");
    }

    /** Single-operator approval (Saurabh) is present and covers an evidence hash (DEC-044). */
    public boolean approvalsComplete() {
        return approval1 != null && approvedEvidenceHash != null;
    }

    /** Whether the approvals covered the given evidence hash exactly. */
    public boolean approvalsCover(String hash) {
        return approvedEvidenceHash != null && approvedEvidenceHash.equals(hash);
    }

    /** Whether the fence is live: this owner holds it, the token is current, the lease is unexpired. */
    public boolean fenceValidFor(String owner, long token, long nowTs) {
        if (ownerInstanceId == null) {
            return false; // no lease acquired — fencing not established
        }
        if (!ownerInstanceId.equals(owner)) {
            return false; // a different (stale) instance claims to own the partition
        }
        // P3-014: an explicitly recorded fence loss fails closed — a lost fence
        // never authorizes again until re-acquired via withFence.
        if (fenceLostTs != null) {
            return false;
        }
        // P3-146: the token must exactly match the issued fenceToken — a future
        // token was never granted and a past token is a stale owner.
        if (token != fenceToken) {
            return false;
        }
        // P3-147: expiry is inclusive and a held lease without an expiry is
        // corrupt — both fail closed (a null lease with an owner must never
        // read as an infinite lease).
        if (leaseExpiresTs == null || nowTs >= leaseExpiresTs) {
            return false;
        }
        return true;
    }

    /** Whether the current lease has expired at {@code nowTs} (used to detect lease loss). */
    public boolean fenceExpiredAt(long nowTs) {
        return leaseExpiresTs != null && nowTs > leaseExpiresTs;
    }

    /** Copy with a new gate state and a freshly incremented epoch. */
    public GateRow withState(GateState newState, String newReason, String hash) {
        return new GateRow(partitionId, accountScopeId, newState, epoch + 1, newReason, hash,
                approval1, approval2, approvedEvidenceHash, ownerInstanceId, fenceToken,
                fenceAcquiredTs, leaseExpiresTs, fenceLostTs);
    }

    /** Copy that records a fence acquisition (owner, monotonic token, lease horizon, acquired ts). */
    public GateRow withFence(String owner, long newToken, long acquiredTs, long leaseMs) {
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, owner, newToken, acquiredTs,
                acquiredTs + leaseMs, null);
    }

    /**
     * Copy that records fence loss (lease lost) and fails closed: the lease is
     * cleared so {@link #fenceValidFor} can never pass on a lost fence, while
     * owner/token are kept as loss evidence (P3-014).
     */
    public GateRow withFenceLost(long lostTs) {
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, ownerInstanceId, fenceToken,
                fenceAcquiredTs, null, lostTs);
    }

    /**
     * Copy that extends the lease for the current holder without changing fenceToken.
     *
     * <p>P3-148: renewing a fence that is not live fails closed instead of
     * resurrecting it. Without these guards the copy unconditionally cleared
     * {@code fenceLostTs} and pushed {@code leaseExpiresTs} forward, so a caller
     * that renewed after loss/expiry re-armed a dead fence without going through
     * {@link #withFence} — bypassing the loss enforcement that
     * {@link #fenceValidFor} relies on. A lost or expired fence must re-acquire.
     */
    public GateRow withRenewedLease(long nowTs, long leaseMs) {
        if (ownerInstanceId == null) {
            throw new IllegalStateException("cannot renew a fence that was never acquired; acquire via withFence");
        }
        if (fenceLostTs != null) {
            throw new IllegalStateException("cannot renew a lost fence (lost at " + fenceLostTs
                    + "); re-acquire via withFence");
        }
        if (leaseExpiresTs == null || nowTs >= leaseExpiresTs) {
            throw new IllegalStateException("cannot renew an expired/absent lease (expires "
                    + leaseExpiresTs + ", now " + nowTs + "); re-acquire via withFence");
        }
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, ownerInstanceId, fenceToken,
                fenceAcquiredTs, nowTs + leaseMs, null);
    }

    /**
     * Copy that clears the fence OWNER and lease and records revocation/loss at clearedTs,
     * while RETAINING {@code fenceToken} as the write-ordering version (P3-374).
     *
     * <p>The token is deliberately NOT reset to 0. The durable gate table orders writes by
     * {@code fence_token} (VERSIONED merge engine, P3-010/P3-012/P3-013), so a write that
     * reset the version to 0 would be dropped by the tablet instead of clearing the fence.
     * The "no fence" marker is {@code ownerInstanceId == null}, which {@link #fenceValidFor}
     * already requires as its first check.
     *
     * <p>Retention is also what keeps the version strictly non-decreasing across a restart:
     * the store seeds its sequence from the durable token via {@code hydrate}, so a durable 0
     * would let the next acquire mint token 1 again — REUSING a token that was already issued
     * and is still held as evidence by a stale holder.
     */
    public GateRow withFenceCleared(long clearedTs) {
        if (ownerInstanceId == null && fenceAcquiredTs == null && leaseExpiresTs == null
                && Objects.equals(fenceLostTs, clearedTs)) {
            return this;
        }
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, null, fenceToken, null, null, clearedTs);
    }
}
