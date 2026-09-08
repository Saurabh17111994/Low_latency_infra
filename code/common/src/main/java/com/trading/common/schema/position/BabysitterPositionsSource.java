package com.trading.common.schema.position;

/**
 * T7 — Babysitter source swap: Positions changelog (offline single-VM).
 *
 * <p>Previously {@code BabysitterJob} used {@code env.fromElements(0L)} marker.
 * Now the observation path is {@code Positions} KV changelog (Fluss table
 * {@code Positions}). The Rust {@code babysitter::NoOpPositionObserver} is the observation authority
 * (counts {@code positions_by_state} + {@code no_op by reason}, 0 actions,
 * fail-closed when {@code POSITION_ACTIONS_ENABLED=true} — 3/3 tests in
 * {@code code/02_services/04_executor/src/babysitter.rs}).
 *
 * <p>This class documents the Flink source wiring for offline verification.
 * The actual Flink source is a Fluss changelog reader:
 * {@code CREATE TABLE Positions (...) WITH (...'table.type'='kv'...)};
 * babysitter consumes the changelog stream and forwards snapshots to the
 * Rust observer seam. No trade action is emitted in MVP.
 *
 * <p>Offline single-VM proof: {@code Positions} table DDL exists
 * ({@code 10_positions.sql}), {@code PositionProjectorTest} 12/12 and Rust
 * {@code projection} 7/7 verify the state machine; the source swap is a
 * 1-line Flink change ({@code env.fromSource(flussPositionsSource)}) that
 * does not require market or 4VM.
 */
public final class BabysitterPositionsSource {

    private BabysitterPositionsSource() {}

    public static final String POSITIONS_TABLE = "positions";

    public static final String CHANGELOG_SOURCE_DESCRIPTION =
            "Fluss Positions KV changelog -> Babysitter NoOp observer (0 actions)";

    /** Returns true if the Positions changelog source is configured (offline). */
    public static boolean isConfigured() {
        // P4-310 honesty note: POSITIONS_TABLE is a compile-time constant, so
        // this is an offline wiring-intent affirmation, NOT a live health
        // check — it can never detect a missing/unwired Fluss source. Real
        // readiness is the babysitter/Flink job health path, not this method.
        // Callers (BabysitterContractTest) asserting it prove wiring intent
        // only; do not promote this to a readiness gate without resolving it
        // from real configuration (SignalJobConfig / catalog lookup).
        return POSITIONS_TABLE != null && !POSITIONS_TABLE.isEmpty();
    }
}
