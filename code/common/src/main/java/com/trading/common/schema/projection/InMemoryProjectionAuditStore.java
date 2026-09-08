package com.trading.common.schema.projection;

import java.util.ArrayList;
import java.util.List;

/** Pure-JVM immutable audit log (offline tests/drills). */
public final class InMemoryProjectionAuditStore implements ProjectionAuditStore {

    private final List<ProjectionAuditRecord> records = new ArrayList<>();

    @Override
    public void append(ProjectionAuditRecord record) {
        records.add(java.util.Objects.requireNonNull(record, "record"));
    }

    @Override
    public List<ProjectionAuditRecord> all() {
        return List.copyOf(records);
    }

    public int size() {
        return records.size();
    }
}
