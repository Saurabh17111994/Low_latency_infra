package com.trading.execution.gateway;

import java.util.List;
import java.util.function.Consumer;
import org.apache.fluss.row.InternalRow;

/** Point-read/replay boundary for control state; it never mutates OMS state. */
public interface ControlStateStore extends AutoCloseable {
    enum Status { FOUND, NOT_FOUND, UNAVAILABLE }
    record Lookup(Status status, InternalRow row, String detail) {
        // P3-271: FOUND with a null row NPEs callers (gate.row().getString)
        // instead of fail-closed DEFERRED — enforce at construction so no
        // future caller needs a defensive null-check after a FOUND check.
        public Lookup {
            java.util.Objects.requireNonNull(status, "status");
            if (status == Status.FOUND && row == null) {
                throw new IllegalArgumentException("FOUND lookup must carry a non-null row");
            }
        }
    }
    Lookup lookup(String tableName, List<Object> keyFields);
    /**
     * Replays all safety-halt rows (P3-272 contract): {@code consumer} must
     * not be null. If {@code consumer} throws, replay aborts and the
     * exception propagates unwrapped; partial application may have occurred
     * (consumers must be idempotent — SafetyHaltTailProcessor dedups by id).
     * Implementations throw {@link IllegalStateException} if the underlying
     * scan fails. The void return carries no completeness signal: a normal
     * return means the scan drained, not that every row applied.
     */
    void replaySafetyHalts(Consumer<InternalRow> consumer);
    @Override void close() throws Exception;

    default Lookup lookup(String tableName, Object... keyFields) {
        // P3-273: List.of(null-array/element) throws a bare NPE with no param
        // name, bypassing the caller's fail-closed != FOUND → DEFERRED path.
        if (keyFields == null) {
            throw new IllegalArgumentException("keyFields must not be null");
        }
        for (Object key : keyFields) {
            if (key == null) {
                throw new IllegalArgumentException("key field must not be null");
            }
        }
        return lookup(tableName, List.of(keyFields));
    }
}
