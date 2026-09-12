package com.trading.execution.gateway;

import java.util.List;

/**
 * Durable store for the cross-table projection workflow.
 *
 * <p><b>Deployment contract: one writer process per {@code eventId}.</b> {@link #put} is an
 * unconditional last-write-wins upsert, so two processes writing the same {@code eventId} can
 * lost-update each other and nothing in this interface detects it. Inside one process the
 * {@code ProjectionApplier} stripe serialises a single stream's steps; across processes nothing
 * does. The deployed shape is what makes the contract hold: the stack declares
 * {@code execution-gateway} with {@code deploy.replicas: 1}
 * ({@code 01_docker/docker-stack.yml}). Scaling the gateway horizontally therefore needs the
 * versioned conditional write that {@link #put} documents and defers (P3-326).
 */
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
     * A bounded page of recoverable work, plus whether more remains.
     *
     * <p>Truncation is <b>reported</b> rather than left to be inferred. This read drives recovery,
     * so a caller that mistook a full page for the entire set would silently skip recoverable
     * work — the same class of bug as the unbounded scan it replaces, just quieter.
     */
    record IncompletePage(List<Entry> entries, boolean truncated) {
        public IncompletePage {
            entries = List.copyOf(entries);
        }
    }

    /**
     * At most {@code limit} recoverable entries, with truncation made explicit — bounded by
     * contract (P3-102).
     *
     * <p>Replaces an unbounded {@code incomplete()} that materialized every recoverable entry of
     * the ledger into one list. What this bounds is <b>memory</b>. It does <b>not</b> bound time,
     * and that is deliberate rather than overlooked: no sound cursor exists for this table.
     * {@code postback_event_id} is the KV primary key and {@code bucket.key} is
     * {@code postback_event_id}, so rows are ordered <i>within</i> a bucket only, and Fluss 0.9.1's
     * {@code BatchScanner} cannot seek to a key. An {@code afterEventId} parameter would therefore
     * re-scan and skip client-side — the same scan cost plus more code and a false promise of
     * pagination. It should not be added until a caller needs it and the cost is understood.
     *
     * <p>Retention is already contracted: {@code table.log.ttl = '2d'} on this table bounds how far
     * back recovery can be asked to look.
     *
     * <p>{@code truncated} is computed exactly, not guessed: the scan continues past a full page
     * only as far as one further recoverable entry, so it is true iff more work exists — not merely
     * when the page happens to be full. A false positive would cost one wasted page; a false
     * negative would skip recovery.
     *
     * @throws IllegalArgumentException if {@code limit} is not positive
     */
    IncompletePage incomplete(int limit) throws Exception;
    @Override void close() throws Exception;
}
