package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.AttemptPhase;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * P3 attempts &amp; retries / broker-calls batch: pins the constructor and store
 * invariants added by the fixes so the guards are load-bearing, not decorative.
 * Offline (no Fluss): the durable legs are covered by the env-gated
 * {@code FlussGateAttemptStoresIntegrationTest}.
 */
class AttemptStoreContractTest {

    private static final String ACCOUNT = "acc-1";
    private static final String PARTITION = "p-1";

    private static AttemptStore.PrepareRequest req(String attemptId, String instructionId,
                                                   String requestHash) {
        return new AttemptStore.PrepareRequest(attemptId, ACCOUNT, instructionId, null,
                PARTITION, requestHash, "E" + attemptId, 42L, 7L, 0L);
    }

    // ── P3-006: BridgeOutcome invariants ──────────────────────────────────

    @Test
    void bridgeOutcomeEnforcesKindAndIdInvariants() {
        assertThatThrownBy(() -> new BridgeCaller.BridgeOutcome(null, null, "d"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new BridgeCaller.BridgeOutcome(
                BridgeCaller.OutcomeKind.ACCEPTED, null, "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BridgeCaller.BridgeOutcome(
                BridgeCaller.OutcomeKind.ACCEPTED, " ", "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BridgeCaller.BridgeOutcome(
                BridgeCaller.OutcomeKind.REJECTED, "B-1", "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BridgeCaller.BridgeOutcome(
                BridgeCaller.OutcomeKind.UNKNOWN, "B-1", "d"))
                .isInstanceOf(IllegalArgumentException.class);

        // The two legal shapes.
        assertThat(new BridgeCaller.BridgeOutcome(
                BridgeCaller.OutcomeKind.ACCEPTED, "B-1", "d").ambiguous()).isFalse();
        assertThat(new BridgeCaller.BridgeOutcome(
                BridgeCaller.OutcomeKind.UNKNOWN, null, "d").ambiguous()).isTrue();
        assertThat(new BridgeCaller.BridgeOutcome(
                BridgeCaller.OutcomeKind.REJECTED, null, "d").ambiguous()).isFalse();
    }

    // ── P3-128/P3-129: withPhase value + terminal guards ──────────────────

    @Test
    void withPhaseRejectsUnknownPhaseAndTerminalResurrection() {
        AttemptRecord rec = AttemptRecord.prepared("a-1", ACCOUNT, "ins-1", "buy",
                PARTITION, "h-1", "Ea-1", 42L, 7L, 0L);
        assertThatThrownBy(() -> rec.withPhase("BOGUS"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown phase");
        assertThatThrownBy(() -> rec.withPhase("prepared"))
                .isInstanceOf(IllegalArgumentException.class);

        AttemptRecord accepted = rec.withPhase("SUBMITTING").withPhase("ACCEPTED");
        assertThatThrownBy(() -> accepted.withPhase("SUBMITTING"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("terminal");
    }

    // ── P3-356: prepared() identity validation ────────────────────────────

    @Test
    void preparedRejectsNullAndBlankIdentities() {
        assertThatThrownBy(() -> AttemptRecord.prepared(null, ACCOUNT, "ins-1", "buy",
                PARTITION, "h-1", "Ea-1", 42L, 7L, 0L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AttemptRecord.prepared("a-1", ACCOUNT, " ", "buy",
                PARTITION, "h-1", "Ea-1", 42L, 7L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── P3-358/P3-359/P3-360: request/result construction invariants ──────

    @Test
    void prepareRequestRejectsNullAndBlankIdentities() {
        assertThatThrownBy(() -> new AttemptStore.PrepareRequest(null, ACCOUNT, "ins-1", null,
                PARTITION, "h-1", "Ea-1", 42L, 7L, 0L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AttemptStore.PrepareRequest("a-1", ACCOUNT, "ins-1", null,
                PARTITION, " ", "Ea-1", 42L, 7L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        // actionId is the only optional identity.
        assertThat(new AttemptStore.PrepareRequest("a-1", ACCOUNT, "ins-1", null,
                PARTITION, "h-1", "Ea-1", 42L, 7L, 0L).actionId()).isNull();
    }

    @Test
    void resultRecordsRequireRecordWhereDocumented() {
        assertThatThrownBy(() -> new AttemptStore.PrepareResult(AttemptStore.Status.CREATED,
                null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AttemptStore.PrepareResult(null, null, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AttemptStore.TransitionResult(
                AttemptStore.TransitionOutcome.APPLIED, null, null))
                .isInstanceOf(IllegalArgumentException.class);

        // NOT_FOUND / blank-phase rejections legally carry no record (P3-360).
        assertThat(new AttemptStore.TransitionResult(
                AttemptStore.TransitionOutcome.NOT_FOUND, null, "x").record()).isNull();
        assertThat(new AttemptStore.PrepareResult(
                AttemptStore.Status.CONTRACT_VIOLATION, null, "x").record()).isNull();
    }

    // ── P3-154: execution_attempt_id uniqueness ───────────────────────────

    @Test
    void prepareRejectsReusedAttemptIdUnderDifferentInstruction() {
        AtomicInteger halts = new AtomicInteger();
        InMemoryAttemptStore store = new InMemoryAttemptStore(halts::incrementAndGet);
        assertThat(store.prepare(req("a-1", "ins-1", "h-1")).status())
                .isEqualTo(AttemptStore.Status.CREATED);

        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-2", "h-2"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.CONTRACT_VIOLATION);
        assertThat(r.reason()).contains("already bound");
        assertThat(halts.get()).isEqualTo(1);
        assertThat(store.size()).isEqualTo(1);
        // the original attempt is untouched
        assertThat(store.attemptById("a-1").instructionId()).isEqualTo("ins-1");
        assertThat(store.attemptById("a-1").requestHash()).isEqualTo("h-1");
    }

    // ── P3-381: hydrate overwrites all three indexes together ─────────────

    @Test
    void hydrateKeepsAllIndexesConsistent() {
        InMemoryAttemptStore store = new InMemoryAttemptStore(() -> { });
        AttemptRecord v1 = AttemptRecord.prepared("t1", ACCOUNT, "ins-1", "buy",
                PARTITION, "h1", "c1", 1L, 0L, 0L);
        AttemptRecord v2 = AttemptRecord.prepared("t1", ACCOUNT, "ins-1", "buy",
                PARTITION, "h2", "c2", 1L, 0L, 0L);
        store.hydrate(v1);
        store.hydrate(v2);

        // The corrected identity's replay key must resolve to a DUPLICATE, not a
        // contract violation caused by a stale first-wins secondary index.
        AttemptStore.PrepareResult r = store.prepare(req("t1", "ins-1", "h2"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.DUPLICATE);
        assertThat(r.record().requestHash()).isEqualTo("h2");
    }

    // ── P3-128: phase strings are bounded by the canonical matrix ─────────

    @Test
    void unknownPhaseStringCannotEnterAnAttemptRecord() {
        // The canonical constructor is the boundary: an arbitrary phase string can
        // no longer be minted into a row and carried to the durable store (where
        // fromRow/persist map by index and would not validate it).
        assertThatThrownBy(() -> new AttemptRecord("a-1", ACCOUNT, "ins-1", "buy", PARTITION,
                "h-1", "Ea-1", 42L, null, 7L, "BOGUS", 0L, null, null, 0L,
                null, null, null, 0, "3"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown phase");
        // a lowercase canonical name is not canonical
        assertThatThrownBy(() -> new AttemptRecord("a-1", ACCOUNT, "ins-1", "buy", PARTITION,
                "h-1", "Ea-1", 42L, null, 7L, "accepted", 0L, null, null, 0L,
                null, null, null, 0, "3"))
                .isInstanceOf(IllegalArgumentException.class);
        // the terminal set still derives from the canonical matrix
        assertThat(AttemptRecord.TERMINAL_PHASES).containsExactlyInAnyOrder(
                AttemptPhase.ACCEPTED.name(), AttemptPhase.REJECTED.name(),
                AttemptPhase.CANCELLED.name());
    }

    @Test
    void unknownPhaseTargetIsStillRejectedAtTheStoreBoundary() {
        // transition()/resolveUnknown() take a raw string, so the store keeps its
        // own guard even though a record can no longer carry an unknown phase.
        InMemoryAttemptStore store = new InMemoryAttemptStore(() -> { });
        store.seedForTest(AttemptRecord.prepared("a-1", ACCOUNT, "ins-1", "buy", PARTITION,
                "h-1", "Ea-1", 42L, 7L, 0L));
        assertThat(store.transition("a-1", 0L, "BOGUS").outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        assertThat(store.transition("a-1", 0L, AttemptRecord.PHASE_SUBMITTING).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
    }
}
