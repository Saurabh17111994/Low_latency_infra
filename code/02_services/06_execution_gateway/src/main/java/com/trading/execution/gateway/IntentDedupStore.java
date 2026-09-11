package com.trading.execution.gateway;

/** Durable store for source-event reprocessing dedup (Execution_Intent_Processed). */
public interface IntentDedupStore extends AutoCloseable {
    /**
     * Reconcile every durably-processed source event: instructionId -> requestHash.
     *
     * <p>P3-084: full-table snapshot — callers must not assume a retention or
     * size bound; implementations accumulate the whole processed history in
     * one Map (OOM/slow-restart as history grows). Prefer a paged/streaming
     * variant (e.g. hydrate(BiConsumer)) plus a retention/TTL contract before
     * production volume. Never null, never contains null keys/values
     * (P3-307) — DurableIntentDispatcher feeds entries straight to commit.
     */
    java.util.Map<String, String> hydrate() throws Exception;
    /**
     * Durably record that {@code instructionId} (hash {@code requestHash}) was handed off.
     *
     * <p>P3-085 idempotency: re-record with the SAME hash is a no-op;
     * re-record with a DIFFERING hash must not overwrite — local
     * IntentDeduplicator.commit is first-wins (putIfAbsent) and a last-wins
     * durable side would diverge after restart (HASH_VIOLATION masked).
     * Durable on normal return (P3-308): a throw means unproven — the caller
     * must fail closed (violation audit), never treat the intent as committed;
     * transient (timeout/network, retryable) vs fatal are not distinguished by
     * type today.
     *
     * @param instructionId non-null, non-empty (P3-307 — null would NPE in BinaryString.fromString)
     * @param requestHash non-null (P3-307 — IntentDeduplicator cannot
     *        distinguish a stored-null hash from an absent key)
     * @param logOffset nullable source offset, may be null if unknown
     */
    void record(String instructionId, String requestHash, Long logOffset) throws Exception;
    // DEV P3-308: keep broad throws Exception + document transient-vs-fatal
    // instead of a new DurableStoreException type — no caller branches on
    // exception type today, so a new public type has zero consumers.
    // close() stays throws Exception (not narrowed to IOException): the Fluss
    // Table/Connection closes it wraps throw Exception, so narrowing would
    // only add wrap-noise at every impl.
    @Override void close() throws Exception;
}
