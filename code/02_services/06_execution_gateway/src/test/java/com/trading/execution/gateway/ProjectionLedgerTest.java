package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProjectionLedgerTest {
    @Test void onlyForwardTransitionsAreAllowed() {
        assertThat(ProjectionLedger.advance(ProjectionLedger.State.RECEIVED,
                ProjectionLedger.State.AUDIT_WRITTEN)).isEqualTo(ProjectionLedger.State.AUDIT_WRITTEN);
        assertThatThrownBy(() -> ProjectionLedger.advance(ProjectionLedger.State.RECEIVED,
                ProjectionLedger.State.COMPLETE)).isInstanceOf(IllegalStateException.class);
    }

    @Test void failedPositionStepIsResumableWithoutRepeatingAudit() throws Exception {
        FakeWriter writer = new FakeWriter();
        FakeLedger ledger = new FakeLedger();
        ProjectionApplier applier = new ProjectionApplier(writer, ledger);
        NormalizedExecutionEvent event = event();
        writer.failPosition = true;
        assertThatThrownBy(() -> applier.apply(event)).isInstanceOf(RuntimeException.class);
        assertThat(ledger.entry.state()).isEqualTo(ProjectionLedger.State.LIFECYCLE_APPLIED);
        writer.failPosition = false;
        assertThat(applier.apply(event)).isTrue();
        assertThat(ledger.entry.state()).isEqualTo(ProjectionLedger.State.COMPLETE);
        assertThat(writer.auditWrites).isEqualTo(1);
        assertThat(writer.lifecycleWrites).isEqualTo(1);
        assertThat(writer.positionWrites).isEqualTo(1);
        assertThat(applier.apply(event)).isFalse();
    }

    private static NormalizedExecutionEvent event() {
        return new NormalizedExecutionEvent("postback-1", "acct", "part", 1, "nautilus", "FILLED", 1,
                new NormalizedExecutionEvent.Audit("audit-1", "hash", "evidence"), null, null, null, null);
    }
    private static final class FakeWriter implements ProjectionWriter {
        int auditWrites, lifecycleWrites, positionWrites; boolean failPosition;
        public void writeAudit(NormalizedExecutionEvent e) { auditWrites++; }
        public void writeLifecycle(NormalizedExecutionEvent e) { lifecycleWrites++; }
        public void writePosition(NormalizedExecutionEvent e) { if (failPosition) throw new RuntimeException("down"); positionWrites++; }
        public void close() {}
    }
    private static final class FakeLedger implements ProjectionLedgerStore {
        ProjectionLedgerStore.Entry entry;
        public Entry lookup(String id) { return entry; }
        public void put(Entry e) { entry = e; }
        public IncompletePage incomplete(int limit) {
            // Same contract as the durable impl — a fake that accepts what the real store
            // rejects would let a bad call site pass here and fail only in production.
            if (limit <= 0) throw new IllegalArgumentException("limit must be positive, got " + limit);
            return entry == null
                    ? new IncompletePage(List.of(), false)
                    : new IncompletePage(List.of(entry), false);
        }
        public void close() {}
    }
}
