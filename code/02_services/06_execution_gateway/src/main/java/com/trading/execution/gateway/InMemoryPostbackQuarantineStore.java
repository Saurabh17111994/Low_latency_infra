package com.trading.execution.gateway;

import java.util.ArrayList;
import java.util.List;

/**
 * Tier 0 #6 in-memory stub for {@link PostbackQuarantineStore}.
 * Offline-test twin of the Fluss append path; keeps rows in JVM memory.
 */
public final class InMemoryPostbackQuarantineStore implements PostbackQuarantineStore {

    public record QuarantineRecord(
            String postbackEventId,
            String reason,
            String evidenceSummary,
            byte[] rawPayload) {
        // P3-306: clone-on-write was defeated on read — the accessor exposed
        // the live array, so all().get(i).rawPayload()[j]=... mutated stored
        // audit evidence. Copy in and out.
        public QuarantineRecord {
            rawPayload = rawPayload == null ? new byte[0] : rawPayload.clone();
        }
        @Override public byte[] rawPayload() { return rawPayload.clone(); }
    }

    // P3-083: ArrayList is hit from concurrent HTTP request threads
    // (GatewayHttpServer pool + flood soak) — guard every path on the monitor.
    private final List<QuarantineRecord> rows =
            java.util.Collections.synchronizedList(new ArrayList<>());

    @Override
    public void quarantine(String postbackEventId, String reason, String evidenceSummary, byte[] rawPayload) {
        synchronized (rows) {
            rows.add(new QuarantineRecord(postbackEventId, reason, evidenceSummary,
                    rawPayload == null ? new byte[0] : rawPayload.clone()));
        }
    }

    public List<QuarantineRecord> all() {
        synchronized (rows) { return List.copyOf(rows); }
    }

    public int size() {
        synchronized (rows) { return rows.size(); }
    }

    public void clear() {
        synchronized (rows) { rows.clear(); }
    }
}
