package com.trading.execution.gateway;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** Resumable coordinator for independent Fluss projection writes. */
public final class ProjectionApplier {
    private final ProjectionWriter writer;
    private final ProjectionLedgerStore ledger;
    private final Clock clock;
    // P3-319: per-eventId striped locks — method-level synchronized serialized
    // independent eventIds while still not fencing cross-instance races. Stripes
    // bound the lock count; the Fluss last-write-wins put has no CAS, so the
    // single-writer-per-eventId assumption is documented on apply().
    private final Map<String, Lock> stripes = new ConcurrentHashMap<>();

    public ProjectionApplier(ProjectionWriter writer, ProjectionLedgerStore ledger) {
        this(writer, ledger, Clock.systemUTC());
    }
    ProjectionApplier(ProjectionWriter writer, ProjectionLedgerStore ledger, Clock clock) {
        this.writer = writer; this.ledger = ledger; this.clock = clock;
    }

    /**
     * Apply one event through the staged workflow. Single-writer per eventId:
     * concurrent appliers sharing one store can still lost-update (Fluss put is
     * last-write-wins with no CAS on expectedPriorState).
     */
    public boolean apply(NormalizedExecutionEvent event) throws Exception {
        Lock stripe = stripes.computeIfAbsent(event.postbackEventId(), k -> new ReentrantLock());
        stripe.lock();
        try {
            return applyLocked(event);
        } finally {
            stripe.unlock();
        }
    }

    private boolean applyLocked(NormalizedExecutionEvent event) throws Exception {
        ProjectionLedgerStore.Entry current = ledger.lookup(event.postbackEventId());
        // P3-320: only COMPLETE is a silent duplicate — QUARANTINED/FAILED are
        // terminal dispositions needing retry/intervention, never silent drops.
        if (current != null && ProjectionLedger.terminal(current.state())) {
            if (current.state() != ProjectionLedger.State.COMPLETE)
                throw new IllegalStateException("ledger in failed terminal " + current.state());
            return false;
        }
        ProjectionLedger.State state = current == null ? ProjectionLedger.State.RECEIVED : current.state();
        // P3-321: carry the retry count forward — literal 0 makes every resume
        // indistinguishable from a first attempt (no retry limits/backoff/alerts).
        int baseRetry = current == null ? 0 : current.retryCount();
        if (current == null) ledger.put(entry(event, state, null, null, baseRetry, null));
        try {
            if (state == ProjectionLedger.State.RECEIVED) {
                writer.writeAudit(event);
                state = ProjectionLedger.advance(state, ProjectionLedger.State.AUDIT_WRITTEN);
                ledger.put(entry(event, state, ProjectionLedger.State.RECEIVED.name(), null, baseRetry, null));
            }
            if (state == ProjectionLedger.State.AUDIT_WRITTEN) {
                writer.writeLifecycle(event);
                state = ProjectionLedger.advance(state, ProjectionLedger.State.LIFECYCLE_APPLIED);
                ledger.put(entry(event, state, ProjectionLedger.State.AUDIT_WRITTEN.name(), null, baseRetry, null));
            }
            if (state == ProjectionLedger.State.LIFECYCLE_APPLIED) {
                writer.writePosition(event);
                state = ProjectionLedger.advance(state, ProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED);
                ledger.put(entry(event, state, ProjectionLedger.State.LIFECYCLE_APPLIED.name(), null, baseRetry, null));
            }
            // P3-097: never mark COMPLETE from a state that skipped the chain —
            // today the ifs converge, but a failed step or future state would be
            // silently completed. Enum prior, not a hardcoded literal.
            if (state != ProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED) {
                throw new IllegalStateException("cannot complete from " + state);
            }
            state = ProjectionLedger.advance(state, ProjectionLedger.State.COMPLETE);
            ledger.put(entry(event, state, ProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED.name(),
                    clock.millis(), baseRetry, null));
            return true;
        } catch (Exception failure) {
            // P3-098: preserve the real prior (not a self-loop of the current
            // state name) and never let the audit put mask the root failure.
            try {
                String prior = current == null ? null : current.expectedPriorState();
                ledger.put(entry(event, state, prior, null, baseRetry + 1, failure.getMessage()));
            } catch (Exception ledgerFailure) {
                failure.addSuppressed(ledgerFailure);
            }
            throw failure;
        }
    }

    private ProjectionLedgerStore.Entry entry(NormalizedExecutionEvent e, ProjectionLedger.State state,
            String prior, Long completed, int retryCount, String error) {
        return new ProjectionLedgerStore.Entry(e.postbackEventId(), state, prior, retryCount,
                error, state == ProjectionLedger.State.COMPLETE ? "COMPLETE" : "OPEN",
                clock.millis(), completed);
    }
}
