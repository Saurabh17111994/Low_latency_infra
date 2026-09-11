package com.trading.execution.gateway;

import java.util.List;

/** Durable store for the cross-table projection workflow. */
public interface ProjectionLedgerStore extends AutoCloseable {
    record Entry(String eventId, ProjectionLedger.State state, String expectedPriorState,
                 int retryCount, String lastError, String disposition, long stepTs, Long completedTs) {
        // P3-325: no null eventId/state (put does e.state().name()), no blank
        // id, no negative retry — fail at write time, never poison later reads.
        public Entry {
            java.util.Objects.requireNonNull(eventId, "eventId");
            java.util.Objects.requireNonNull(state, "state");
            if (eventId.isBlank()) throw new IllegalArgumentException("eventId blank");
            if (retryCount < 0) throw new IllegalArgumentException("retryCount");
        }
    }
    /**
     * Current entry for {@code eventId}, or null when absent.
     *
     * <p>DEV P3-101: nullable (not Optional) is intentional — every caller
     * already null-checks and the Fluss/InMemory impls share this shape;
     * Optional would ripple the applier, both impls and tests for zero
     * runtime gain. Callers must treat null as not-found, never dereference.
     */
    Entry lookup(String eventId) throws Exception;
    /**
     * Unconditional last-write-wins upsert — {@code expectedPriorState} is
     * persisted <b>as an audit field, not as a concurrency guard</b> (it records
     * the prior state for traceability; {@code ProjectionApplier}'s error path
     * reads it to preserve the real prior, P3-098). Nothing enforces it.
     *
     * <p>DEV P3-326 — decision: last-write-wins is retained. The ORIGINAL rationale
     * ("a conditional write is not expressible on Fluss 0.9.1") was <b>wrong</b> and is
     * corrected here. Fluss 0.9.1 does have a durable conditional write: the VERSIONED
     * merge engine orders a table's writes by a declared version column and the tablet
     * DROPS a lower-version write server-side. That conditionality comes from the
     * <b>table's</b> merge engine, not from a client API — {@code Table} still exposes
     * only {@code newLookup()}/{@code newUpsert()} and {@code UpsertWriter} only
     * {@code upsert}/{@code delete}. Proven live 2026-09-11 for Execution_Gate
     * (CHG-122).
     *
     * <p>So {@code putIfPrior} <b>is</b> expressible in principle, but not for this
     * table as it currently stands: VERSIONED requires a monotonic per-key version
     * column, and the ledger has none. {@code eventId} is the key, and {@code stepTs} is
     * a step timestamp whose ordering is not guaranteed monotonic across the
     * RECEIVED&rarr;&hellip;&rarr;COMPLETE staging or across an applier clock step — using
     * it as the version column would silently <b>drop legitimate transitions</b>, and
     * because equal versions are accepted it would not fence a concurrent applier
     * either. Adopting it needs a real version-column design plus its own DDL change and
     * change record: <b>deferred, not impossible</b>.
     *
     * <p>What actually makes LWW safe here is <b>not</b> this store: it is the
     * single-writer-per-{@code eventId} deployment contract plus the staged
     * advance ({@code ProjectionApplier} checkpoints
     * RECEIVED&rarr;&hellip;&rarr;COMPLETE) and replay idempotency. Concurrent
     * appliers sharing one store <b>can</b> lost-update, and nothing in this
     * interface detects or prevents that — unlike the gate's single-owner fence,
     * there is no lease or token to make a second writer fail closed. If applier
     * concurrency is ever introduced, this store must be paired with an external
     * exclusion mechanism; LWW alone will silently drop the losing update.
     */
    void put(Entry entry) throws Exception;
    /**
     * Every recoverable entry. Unbounded full-ledger scan by contract.
     *
     * <p>DEV P3-102: no pagination/streaming here — the Fluss impl does
     * limit(MAX) per bucket into one ArrayList (O(ledger) restart cost).
     * Prefer a paged variant (e.g. incomplete(limit, afterEventId)) plus a
     * retention contract before production volume.
     */
    java.util.List<Entry> incomplete() throws Exception;
    @Override void close() throws Exception;
}
