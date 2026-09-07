package com.trading.ingestion.safety;

/**
 * Minimal safety-halt evidence surface consumed by {@code IngestionService}.
 *
 * <p>Implemented by the Fluss-backed {@link SafetyHaltWriter} in production
 * and by no-op substitutes in tests (ING-DQ-010) so the service can be
 * constructed and driven without a reachable Fluss. The state and reason-code
 * vocabularies stay on {@link SafetyHaltWriter} — the shared types documented
 * across the dossiers — so this interface references the concrete class for
 * the vocabulary only, never for behavior.
 */
public interface SafetySink extends AutoCloseable {

    /**
     * Write one safety request row.
     *
     * <p>P1-093: fail-loud contract — a synchronous failure to accept the row
     * throws (the returned ID must never be treated as proof of persistence;
     * async Fluss failures are only logged by the production writer). Callers
     * MUST evict their dedup entry on failure so the next tick retries.
     *
     * <p>P1-094: parameter contract (enforced by the production writer via
     * {@code SafetyHaltWriter.requireWritableArgs}; doubles that accept less
     * weaken the gate, so keep them honest).
     *
     * @param slotId non-null, non-blank slot id (e.g. hft-0)
     * @param connectionEpoch slot epoch at the transition
     * @param state non-null UNSAFE/RECOVERED; UNSAFE requires a non-null
     *        reasonCode, RECOVERED requires a null reasonCode (empty reason in
     *        the ID tuple — the service passes null for RECOVERED)
     * @param reasonCode exact reason code for UNSAFE, null for RECOVERED
     * @param assignedTokenHash nullable hex of the slot's sorted token set,
     *        null if unknown; evidenceReference nullable free text
     * @param detectedTsMs wall-clock millis of detection
     * @return the computed halt_request_id (for caller-side dedup), never null.
     *         Deterministic: hex(sha256(manifest_fp|slot|epoch|state|reason)).
     *         NOTE: the return is informational (logging) only — dedup is
     *         keyed by the caller on the ID it computed BEFORE calling, so a
     *         double returning a constant does not break dedup, only the logs.
     * @throws RuntimeException if the row cannot be accepted for writing
     */
    String write(String slotId, long connectionEpoch, SafetyHaltWriter.SafetyState state,
                 SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                 String evidenceReference, long detectedTsMs);

    @Override
    void close();
}
