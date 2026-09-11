package com.trading.common.schema.projection;

import java.util.ArrayList;
import java.util.List;

/** Pure-JVM immutable quarantine LOG (offline tests/drills). */
public final class InMemoryPostbackQuarantineStore implements PostbackQuarantineStore {

    private final List<QuarantinedPostback> rows = new ArrayList<>();

    @Override
    public void append(QuarantinedPostback row) {
        // P3-397: copy on write — storing by reference lets the caller mutate
        // the "immutable LOG" through the shared byte[] after append.
        rows.add(copy(row));
    }

    @Override
    public List<QuarantinedPostback> scan(int limit) {
        // P3-410: bounded, like the durable implementation — an unbounded read of an
        // append-only LOG is a heap-growth bug, and the offline store should not
        // advertise a shape the durable one refuses to offer.
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive, got " + limit);
        // P3-398: List.copyOf is shallow — deep-copy payload bytes so readers
        // cannot corrupt stored evidence through the shared array.
        int n = Math.min(limit, rows.size());
        List<QuarantinedPostback> snapshot = new ArrayList<>(n);
        for (QuarantinedPostback r : rows.subList(0, n)) snapshot.add(copy(r));
        return List.copyOf(snapshot);
    }

    private static QuarantinedPostback copy(QuarantinedPostback r) {
        java.util.Objects.requireNonNull(r, "row");
        byte[] payload = r.originalPayload() == null ? null : r.originalPayload().clone();
        return new QuarantinedPostback(r.quarantineId(), r.postbackEventId(), r.reason(),
                payload, r.payloadHash(), r.brokerOrderId(), r.instructionId(),
                r.correlationAttempt(), r.disposition(), r.dispositionReason(),
                r.quarantinedTs(), r.dispositionTs(), r.schemaVersion());
    }

    public int size() {
        return rows.size();
    }
}
