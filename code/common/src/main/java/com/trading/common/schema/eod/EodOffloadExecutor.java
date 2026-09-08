package com.trading.common.schema.eod;

/**
 * The lake-offload half of the EOD controller (SCH-23): one SPI the state
 * machine drives — {@link #offload} copies a trading day's source data to the
 * lake target (the encrypted export pipeline plugs in here), and
 * {@link #verify} reconciles the copy (count/hash) before the day may reach VERIFIED
 *
 * <p>Fail-closed by construction: the shipped default is
 * {@link NotConfiguredEodOffloadExecutor} — with no target configured, every
 * offload fails and the day stays {@code FAILED_RETRYABLE}, so an unverified
 * day extends retention and is NEVER silently marked verified.
 * {@link MockEodOffloadExecutor} is the drill/dev capability
 * ({@code --offload mock}).
 */
public interface EodOffloadExecutor {

    /**
     * Offload one trading day's data for a table. A failed offload returns
     * {@link OffloadResult#failure(String)} — the controller transitions the
     * day to {@code FAILED_RETRYABLE} with backoff (it never fabricates a
     * successful copy). Must never return null; must be idempotent (WRITING
     * days are re-driven after a crash). If an exception escapes, callers
     * must treat it exactly like a returned failure: FAILED_RETRYABLE with
     * backoff, never VERIFIED.
     */
    OffloadResult offload(EodOffloadRecord record) throws Exception;

    /**
     * Verify an offloaded copy (row counts, hashes). True only when the copy
     * reconciles — {@code VERIFIED} is the gate that releases source expiry.
     * Any exception or mismatch must be treated as {@code false} (failed
     * verification → FAILED_RETRYABLE), never VERIFIED. Accepts records in
     * COMMITTED or VERIFYING state (crash-resume re-drive); must be idempotent.
     */
    boolean verify(EodOffloadRecord committedOrVerifying) throws Exception;
}
