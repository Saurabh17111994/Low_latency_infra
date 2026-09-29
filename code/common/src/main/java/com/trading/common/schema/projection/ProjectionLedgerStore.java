package com.trading.common.schema.projection;

import java.util.Optional;

/**
 * Port for the Postback_Projection_Ledger view keyed by {@code postback_event_id}.
 *
 * <p>The only implementation in this module is the in-memory oracle
 * ({@link InMemoryProjectionLedgerStore}) — offline tests and drills. The
 * durable path is the execution gateway's own store (single writer over
 * {@code Postback_Projection_Ledger}; H1-1/M1-6 keep the single-writer
 * invariant). A class here named after Fluss was deleted in L5-4: it opened a
 * Connection/Table and then touched only an in-memory map — a name promising
 * durability it did not provide (P4-010).
 */
public interface ProjectionLedgerStore {

    Optional<ProjectionLedgerEntry> lookup(String postbackEventId) throws Exception;

    void put(ProjectionLedgerEntry entry) throws Exception;
}
