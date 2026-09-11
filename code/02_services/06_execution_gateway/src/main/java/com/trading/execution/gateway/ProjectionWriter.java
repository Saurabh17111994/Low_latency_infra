package com.trading.execution.gateway;

/**
 * Durable projection boundary.
 *
 * <p>P3-103: implementations must synchronously observe every Fluss future
 * before returning (block on get/flush), must be safe to retry after failure
 * (upsert-idempotent or deduped), and must drain pending writes on close().
 * Callers must not assume atomicity across audit/lifecycle/position.
 */
public interface ProjectionWriter extends AutoCloseable {
    void writeAudit(NormalizedExecutionEvent event) throws Exception;
    void writeLifecycle(NormalizedExecutionEvent event) throws Exception;
    void writePosition(NormalizedExecutionEvent event) throws Exception;
    /**
     * Non-atomic convenience fan-out only; not crash-safe on partial failure.
     *
     * <p>P3-104: production path must use staged writeAudit/writeLifecycle/
     * writePosition with ProjectionLedger checkpoints (see ProjectionApplier);
     * do not use this for durable projection unless all three writes are made
     * idempotent — retry duplicates the non-idempotent audit/fills appends.
     */
    default void write(NormalizedExecutionEvent event) throws Exception {
        writeAudit(event);
        writeLifecycle(event);
        writePosition(event);
    }
    @Override void close() throws Exception;
}
