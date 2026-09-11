package com.trading.execution.gateway;

/** Durable source-event dedup: process-local classification reconciled from a
 *  durable store so a replayed Execution_Intent is not handed off twice after a
 *  gateway restart (T2 durable attempt/index lookup). */
final class DurableIntentDispatcher {
    enum Verdict { FIRST, DUPLICATE, HASH_VIOLATION }
    private final IntentDedupStore store;
    private final IntentDeduplicator dedup = new IntentDeduplicator();

    DurableIntentDispatcher(IntentDedupStore store) throws Exception {
        this.store = store;
        store.hydrate().forEach(dedup::commit);
    }

    Verdict classify(String instructionId, String requestHash) {
        return switch (dedup.classify(instructionId, requestHash)) {
            case FIRST -> Verdict.FIRST;
            case DUPLICATE -> Verdict.DUPLICATE;
            case HASH_VIOLATION -> Verdict.HASH_VIOLATION;
        };
    }

    /** Called after a durably-successful handoff: update local + durable state. */
    void committed(String instructionId, String requestHash, Long logOffset) throws Exception {
        // P3-059/P3-089/P3-092: durable-first — commit() is pure in-memory while
        // record() can throw (network/timeout). Local-first diverges: the guard
        // would report DUPLICATE for the rest of this run with nothing durable,
        // and restart replay would re-forward (duplicate side effect). A crash
        // between record and commit errs to at-most-one-extra replay, which the
        // hydrated guard plus idempotent forward absorbs; the reverse errs to a
        // silent drop. Callers must still treat a throw as unproven (fail closed).
        store.record(instructionId, requestHash, logOffset);
        dedup.commit(instructionId, requestHash);
    }
}
