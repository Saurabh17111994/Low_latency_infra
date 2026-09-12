package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.position.InMemoryPositionsStateStore;
import com.trading.common.schema.position.PositionSnapshot;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class PostbackProjectionDriverTest {

    private static final long NOW = 1000L;

    private record Harness(PostbackProjectionDriver driver,
                           CorrelationIndex correlation,
                           InMemoryLifecycleStore lifecycle,
                           InMemoryPositionsStateStore positions,
                           InMemoryProjectionLedgerStore ledger,
                           InMemoryProjectionAuditStore audit,
                           InMemoryPostbackQuarantineStore quarantine,
                           InMemoryCorrelationIndex index,
                           ReferencePositionAuthority authority) {}

    private static Harness harness() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex()
                .byBrokerOrderId("b-1", new AttemptRef("acc-1", "instr-1", "att-1", "tc-1"))
                .byBrokerOrderId("b-2", new AttemptRef("acc-1", "instr-2", "att-2", "tc-2"));
        InMemoryLifecycleStore lifecycle = new InMemoryLifecycleStore();
        InMemoryPositionsStateStore positions = new InMemoryPositionsStateStore();
        InMemoryProjectionLedgerStore ledger = new InMemoryProjectionLedgerStore();
        InMemoryProjectionAuditStore audit = new InMemoryProjectionAuditStore();
        InMemoryPostbackQuarantineStore quarantine = new InMemoryPostbackQuarantineStore();
        ReferencePositionAuthority authority = new ReferencePositionAuthority();
        PostbackProjectionDriver driver = new PostbackProjectionDriver(
                index, lifecycle, positions, ledger, audit, quarantine, authority,
                "nautilus-projection", 1L);
        return new Harness(driver, index, lifecycle, positions, ledger, audit, quarantine,
                index, authority);
    }

    @Test
    void happyPathProjectsFillToComplete() throws Exception {
        Harness h = harness();
        NormalizedPostback fill = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        PostbackProjectionDriver.ProjectionResult r = h.driver().project(fill, NOW);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);
        assertThat(r.positionId()).isNotBlank();
        assertThat(h.quarantine().size()).isZero();
        assertThat(h.driver().haltedScopeIds()).isEmpty();

        // Lifecycle row written.
        assertThat(h.lifecycle().size()).isEqualTo(1);
        // Position row written via the Nautilus-serialized event.
        assertThat(h.positions().size()).isEqualTo(1);
        PositionSnapshot pos = h.positions().lookup(r.positionId());
        assertThat(pos.openQuantity()).isEqualTo(10);
        assertThat(pos.sourceVersion()).isEqualTo(1L);
        // Ledger reached COMPLETE (all table writes acknowledged).
        ProjectionLedgerEntry entry = h.ledger().lookup(fill.postbackEventId()).get();
        assertThat(entry.projectionState()).isEqualTo(PostbackProjectionLedger.State.COMPLETE);
        assertThat(entry.completedTs()).isNotNull();
        // Audit evidence written.
        assertThat(h.audit().size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void duplicatePostbackIsNoOpNotReApplied() throws Exception {
        Harness h = harness();
        NormalizedPostback fill = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        h.driver().project(fill, NOW);
        PostbackProjectionDriver.ProjectionResult r2 = h.driver().project(fill, NOW + 5);
        assertThat(r2.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.DUPLICATE);
        // No second quarantine, no duplicate position.
        assertThat(h.quarantine().size()).isZero();
        assertThat(h.positions().size()).isEqualTo(1);
    }

    @Test
    void fingerprintMismatchQuarantinesAndHalts() throws Exception {
        Harness h = harness();
        NormalizedPostback tampered = TestPostbacks.withFingerprintMismatch(
                TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW));
        PostbackProjectionDriver.ProjectionResult r = h.driver().project(tampered, NOW);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.QUARANTINED);
        assertThat(r.quarantineReason()).isEqualTo(QuarantineReason.FINGERPRINT_MISMATCH);
        assertThat(h.quarantine().size()).isEqualTo(1);
        assertThat(h.driver().haltedScopeIds()).contains("acc-1");
        assertThat(h.lifecycle().size()).isZero();
    }

    @Test
    void unresolvableCorrelationQuarantinesAndHaltsWithoutWrite() throws Exception {
        Harness h = harness();
        NormalizedPostback p = TestPostbacks.fill(1L, "b-99", "BUY", 10, 0, 10, 1000L, NOW);
        PostbackProjectionDriver.ProjectionResult r = h.driver().project(p, NOW);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.QUARANTINED);
        assertThat(r.quarantineReason()).isEqualTo(QuarantineReason.NO_MATCHING_INSTRUCTION);
        assertThat(h.lifecycle().size()).isZero();
        assertThat(h.positions().size()).isZero();
        assertThat(h.driver().haltedScopeIds()).contains("acc-1");
    }

    @Test
    void lifecycleConflictQuarantinesAndHalts() throws Exception {
        Harness h = harness();
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        h.driver().project(p, NOW);
        // Same source sequence (1), different content (different source event id)
        // and a distinct postback id with a VALID fingerprint => lifecycle
        // conflict, not fingerprint failure.
        String peid = "pb-CONFLICT";
        String sess = "evt-CONFLICT";
        String fp = PostbackFingerprint.compute("1", PostbackFingerprint.canonicalFrom(
                "1", peid, sess, 1L, "b-1", "ref-b-1", "acc-1", "BUY", "PARTIAL",
                10, 0, 10, 1000L, NOW, NOW));
        NormalizedPostback conflict = new NormalizedPostback(
                peid, sess, 1L, fp, "1", "b-1", "ref-b-1",
                "acc-1", 1001L, "CME", "wti", "BUY", "PARTIAL", 10, 0, 10, 1000L,
                NOW, NOW, "1", "hash-x", "tc-1");
        PostbackProjectionDriver.ProjectionResult r = h.driver().project(conflict, NOW + 1);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.QUARANTINED);
        assertThat(r.quarantineReason()).isEqualTo(QuarantineReason.LIFECYCLE_CONFLICT);
        assertThat(h.driver().haltedScopeIds()).contains("acc-1");
    }

    @Test
    void staleLifecycleIsRejectedNotQuarantined() throws Exception {
        Harness h = harness();
        NormalizedPostback p2 = TestPostbacks.fill(2L, "b-1", "BUY", 20, 0, 10, 1000L, NOW);
        h.driver().project(p2, NOW);
        NormalizedPostback p1 = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        PostbackProjectionDriver.ProjectionResult r = h.driver().project(p1, NOW + 5);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.STALE);
        // P3-500 sibling: the stale detail names the rejected postback.
        assertThat(r.detail()).contains(r.postbackEventId());
        assertThat(h.quarantine().size()).isZero();
        assertThat(h.driver().haltedScopeIds()).isEmpty();
        // Lifecycle still reflects the newer version.
        assertThat(h.lifecycle().lookup("acc-1", "b-1").get().sourceVersion()).isEqualTo(2L);
    }

    @Test
    void oversellFillQuarantinesAndHalts() throws Exception {
        Harness h = harness();
        NormalizedPostback buy = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        h.driver().project(buy, NOW);
        // Sell more than the open position: the reference authority rejects it.
        NormalizedPostback oversell = TestPostbacks.fill(2L, "b-1", "SELL", 10, 0, 20, 900L, NOW + 1);
        PostbackProjectionDriver.ProjectionResult r = h.driver().project(oversell, NOW + 1);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.QUARANTINED);
        assertThat(h.driver().haltedScopeIds()).contains("acc-1");
        // No positive write past the offending fill.
        assertThat(h.positions().size()).isEqualTo(1);
    }

    @Test
    void statusOnlyPostbackCompletesWithNoPositionRow() throws Exception {
        Harness h = harness();
        NormalizedPostback s = TestPostbacks.status(1L, "b-1", "FILLED", 10, 0, NOW);
        PostbackProjectionDriver.ProjectionResult r = h.driver().project(s, NOW);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);
        assertThat(h.lifecycle().size()).isEqualTo(1);
        assertThat(h.positions().size()).isZero();
        assertThat(h.ledger().lookup(s.postbackEventId()).get().projectionState())
                .isEqualTo(PostbackProjectionLedger.State.COMPLETE);
    }

    @Test
    void projectionNeverIssuesBrokerCommands() {
        // The projection engine exposes only projection stores and audit/
        // quarantine sinks — no broker place/cancel/order surface. This pins
        // that the projected path cannot authorize a broker command even if a
        // future caller adds a sink (projection is read-only by contract).
        boolean hasBrokerSurface = Arrays.stream(PostbackProjectionDriver.class.getDeclaredMethods())
                .anyMatch(m -> m.getName().toLowerCase().contains("place")
                        || m.getName().toLowerCase().contains("cancel"));
        assertThat(hasBrokerSurface).isFalse();
    }
}
