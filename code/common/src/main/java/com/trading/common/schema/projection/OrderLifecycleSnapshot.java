package com.trading.common.schema.projection;

import com.trading.common.model.OrderLifecycleState;

/**
 * Projection snapshot mirroring the Order_Lifecycle KV layout
 * (09_order_lifecycle.sql v2, pinned by OrderLifecycleColumns). Written by the
 * T6 lifecycle projection under the {@code (account_scope_id, broker_order_id)}
 * composite key; source-version evidence is carried so replay/rebuild stays
 * deterministic and stale/conflict updates are rejected before upsert.
 */
public record OrderLifecycleSnapshot(
        String accountScopeId,
        String brokerOrderId,
        String instructionId,
        String executionAttemptId,
        String tradeContextId,
        OrderLifecycleState normalizedState,
        long cumulativeQty,
        long pendingQty,
        long averageFillPricePaise,
        String sourceEventId,
        long sourceVersion,
        long sourceEventTime,
        long lastReceiveTime,
        String correlationState,
        String schemaVersion) {

    public OrderLifecycleSnapshot {
        if (accountScopeId == null || accountScopeId.isBlank()) {
            throw new IllegalArgumentException("accountScopeId is required");
        }
        if (brokerOrderId == null || brokerOrderId.isBlank()) {
            throw new IllegalArgumentException("brokerOrderId is required");
        }
        if (normalizedState == null) {
            throw new IllegalArgumentException("normalizedState is required");
        }
        if (cumulativeQty < 0 || pendingQty < 0) {
            throw new IllegalArgumentException("quantities must be >= 0");
        }
        if (cumulativeQty < pendingQty) {
            throw new IllegalArgumentException("pending_qty cannot exceed cumulative_qty");
        }
        // P3-506: a negative average fill price is meaningless for a price
        // field and would persist into Order_Lifecycle via rebuild/replay.
        if (averageFillPricePaise < 0) {
            throw new IllegalArgumentException("averageFillPricePaise must be >= 0");
        }
        // P3-507: DDL pins these NOT NULL — fail fast here instead of at the
        // storage layer when a snapshot is rebuilt from a replayed row.
        // (instructionId/executionAttemptId/tradeContextId stay nullable:
        // correlated-via-broker rows legitimately lack attempt identity.)
        if (sourceEventId == null || sourceEventId.isBlank()) {
            throw new IllegalArgumentException("sourceEventId is required");
        }
        if (schemaVersion == null || schemaVersion.isBlank()) {
            throw new IllegalArgumentException("schemaVersion is required");
        }
        // P3-508: version/time evidence must stay monotone-parseable — a
        // negative version or timestamp would corrupt the replay evidence.
        if (sourceVersion < 0 || sourceEventTime < 0 || lastReceiveTime < 0) {
            throw new IllegalArgumentException("sourceVersion/sourceEventTime/lastReceiveTime must be >= 0");
        }
    }
}
