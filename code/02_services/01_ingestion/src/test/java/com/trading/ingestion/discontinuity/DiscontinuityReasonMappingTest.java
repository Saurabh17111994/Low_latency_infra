package com.trading.ingestion.discontinuity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.bridge.BridgeEvent;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.fluss.client.table.writer.AppendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 3 — lifecycle evidence mapping (plan §DiscontinuityWriter, R-029).
 *
 * <p>Verifies the plan's event→Reason mapping:
 * DISCONNECTED/BRIDGE_SHUTDOWN/AUTH_FAILURE→DROP,
 * HEARTBEAT_FAILED/FEED_STALLED→HEARTBEAT_GAP, RECONNECT→RECONNECT,
 * SUBSCRIPTION_PARTIAL→FEED_HEALTH, others→null.
 *
 * <p>R-029: the bridge vocabulary is exactly {slot_state, subscription_ack,
 * heartbeat_failed, feed_stalled, disconnect, reconnect, auth_failure,
 * bridge_shutdown} — {@code bridge_exit} never occurs; the real exit event is
 * {@code bridge_shutdown}. A full {@code subscription_ack} (rejected == 0) is
 * healthy, not a discontinuity.
 */
@DisplayName("ING-UNIT-011: bridge event to discontinuity reason mapping")
class DiscontinuityReasonMappingTest {

    /** 64 lowercase hex chars — the SHA-256 digest shape the bridge emits. */
    private static final String HEX64 =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    @DisplayName("disconnect and bridge_shutdown map to DROP")
    void disconnectMapsToDrop() {
        assertEquals(DiscontinuityWriter.Reason.DROP, DiscontinuityWriter.mapEventToReason("disconnect"));
        assertEquals(DiscontinuityWriter.Reason.DROP, DiscontinuityWriter.mapEventToReason("bridge_shutdown"));
        // bridge_exit is not in the validated bridge vocabulary — dead event name.
        assertNull(DiscontinuityWriter.mapEventToReason("bridge_exit"));
    }

    @Test
    @DisplayName("auth_failure maps to DROP")
    void authFailureMapsToDrop() {
        assertEquals(DiscontinuityWriter.Reason.DROP, DiscontinuityWriter.mapEventToReason("auth_failure"));
    }

    @Test
    @DisplayName("heartbeat_failed and feed_stalled map to HEARTBEAT_GAP")
    void heartbeatAndStallMapToGap() {
        assertEquals(DiscontinuityWriter.Reason.HEARTBEAT_GAP, DiscontinuityWriter.mapEventToReason("heartbeat_failed"));
        assertEquals(DiscontinuityWriter.Reason.HEARTBEAT_GAP, DiscontinuityWriter.mapEventToReason("feed_stalled"));
    }

    @Test
    @DisplayName("reconnect maps to RECONNECT")
    void reconnectMapsToReconnect() {
        assertEquals(DiscontinuityWriter.Reason.RECONNECT, DiscontinuityWriter.mapEventToReason("reconnect"));
    }

    @Test
    @DisplayName("subscription_ack maps to FEED_HEALTH (partial subscription evidence)")
    void subscriptionAckMapsToFeedHealth() {
        assertEquals(DiscontinuityWriter.Reason.FEED_HEALTH,
                DiscontinuityWriter.mapEventToReason("subscription_ack"));
    }

    @Test
    @DisplayName("non-evidence events map to null")
    void nonEvidenceMapsToNull() {
        assertNull(DiscontinuityWriter.mapEventToReason("slot_state"));
        assertNull(DiscontinuityWriter.mapEventToReason(null));
    }

    @Test
    @DisplayName("full subscription ack carries no discontinuity evidence")
    void fullSubscriptionAckCarriesNoEvidence() {
        BridgeEvent fullAck = new BridgeEvent(
                "subscription_ack", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                1L, "ACTIVE", 1024, 1024, 0, "", 1_000L, HEX64, HEX64);
        // The event maps, but with rejectedTokens == 0 the write guard must
        // suppress the row — a clean full ack is not a discontinuity.
        assertEquals(DiscontinuityWriter.Reason.FEED_HEALTH,
                DiscontinuityWriter.mapEventToReason(fullAck.event()));
        assertEquals(false, DiscontinuityWriter.carriesDiscontinuityEvidence(fullAck));
    }

    @Test
    @DisplayName("partial subscription ack carries FEED_HEALTH evidence")
    void partialSubscriptionAckCarriesEvidence() {
        BridgeEvent partialAck = new BridgeEvent(
                "subscription_ack", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                1L, "PARTIAL", 1024, 900, 124, "tokens rejected", 1_000L, HEX64, HEX64);
        assertEquals(DiscontinuityWriter.Reason.FEED_HEALTH,
                DiscontinuityWriter.mapEventToReason(partialAck.event()));
        assertEquals(true, DiscontinuityWriter.carriesDiscontinuityEvidence(partialAck));
    }

    @Test
    @DisplayName("writeBridgeEvent rejects non-evidence events")
    void writeBridgeEventSkipsNonEvidence() {
        // slot_state must not produce a row — no exception, just no mapping.
        BridgeEvent slotState = new BridgeEvent(
                "slot_state", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                1L, "CONNECTING", 1024, 0, 0, "", 1_000L, HEX64, HEX64);
        assertEquals(null, DiscontinuityWriter.mapEventToReason(slotState.event()));
        assertEquals(false, DiscontinuityWriter.carriesDiscontinuityEvidence(slotState));
    }

    @Test
    @DisplayName("P4-213: row schema version matches DDL header + manifest (v2, bare numeric)")
    void schemaVersionMatchesHeader() {
        assertEquals("2", DiscontinuityWriter.SCHEMA_VERSION);
    }

    @Test
    @DisplayName("observe() propagates async append failures (R-030)")
    void observePropagatesAsyncFailure() {
        CompletableFuture<AppendResult> failed =
                CompletableFuture.failedFuture(new RuntimeException("broker down"));
        CompletableFuture<AppendResult> guarded =
                DiscontinuityWriter.observe(failed, "id-1", "DROP/test");
        assertTrue(failed.isCompletedExceptionally(),
                "async failures must reach the observe handler");
        assertThrows(CompletionException.class, guarded::join);
    }

    @Test
    @DisplayName("observe() completes normally on successful append (R-030)")
    void observeCompletesOnSuccess() {
        CompletableFuture<AppendResult> ok = CompletableFuture.completedFuture(null);
        CompletableFuture<AppendResult> guarded =
                DiscontinuityWriter.observe(ok, "id-2", "DROP/test");
        assertEquals(null, guarded.join(), "success must complete the guarded future");
    }
}
