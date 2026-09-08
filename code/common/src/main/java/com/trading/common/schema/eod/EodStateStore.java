package com.trading.common.schema.eod;

import java.util.List;

/**
 * Durable store for EOD offload state (SCH-23): the controller's state must
 * be durable with restart/resume (docs/04_contracts/02-storage.md "EOD
 * controller"), and every transition goes through the record's validated
 * state machine — so a crashed controller re-runs a day idempotently instead
 * of losing or regressing it.
 *
 * <p>Two implementations: {@link FlussEodStateStore} (the raw client against
 * the {@code eod_offload_state} KV table — the authoritative home) and the
 * in-memory store used by the unit tests. The interface is deliberately tiny:
 * read-all (current state, folded by record id), upsert (last-write-wins —
 * the KV convergence semantic that makes re-runs idempotent), and the
 * single-writer lease.
 */
public interface EodStateStore {

    /** All offload records currently on file (the lease row is excluded). */
    List<EodOffloadRecord> readAll() throws Exception;

    /**
     * Upsert one offload record (last-write-wins — replay/re-run converges
     * only for monotonic writes, P4-131). Callers must only pass records
     * produced by {@code EodOffloadRecord.transition()} from the latest
     * {@link #readAll} state; a stale reader overwriting a newer
     * VERIFIED/COMMITTED with an older PENDING/WRITING regresses the machine
     * and can permit premature source expiry. Implementations must reject
     * stale/regressive writes if possible (e.g. compare
     * {@code updatedAtMs}). Rejects null with
     * {@code NullPointerException}/{@code IllegalArgumentException}.
     */
    void upsert(EodOffloadRecord record) throws Exception;

    /**
     * Single-writer fencing (ADVISORY best-effort, P4-132 — the raw client
     * has no atomic compare-and-set, so the lease is read-then-write with a
     * token + expiry; concurrent acquirers racing an expired lease may both
     * succeed, and a crashed holder blocks until expiry). Returns the lease
     * NOW in effect: either freshly acquired (this {@code token}, expiry
     * {@code now + ttl}) or the unexpired lease of another holder — the
     * caller refuses to run when the returned token is not its own.
     * Requires {@code token} non-blank and {@code leaseTtlMs > 0}; long runs
     * must re-acquire/renew before expiry, and callers must re-check
     * {@link Lease#isHeldBy} before committing VERIFIED (overlapping runs
     * are possible). Future: atomic acquire + fencing epoch required on
     * {@link #upsert} + renew/release.
     */
    Lease acquireLease(String token, long nowMs, long leaseTtlMs) throws Exception;
}
