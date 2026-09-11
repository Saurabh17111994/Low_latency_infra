package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3-010/P3-012/P3-013: Execution_Gate is a VERSIONED merge table ordered by
 * {@code fence_token}, so the tablet DROPS a write carrying a LOWER token than the stored
 * row's — and still reports SUCCESS, because Fluss 0.9.1 carries no "ignored" result on
 * {@code UpsertResult}. These pin the re-read decision that turns a dropped write into a
 * loud failure instead of a silent one.
 *
 * <p>The drop itself happens tablet-side, so end-to-end proof requires a live VERSIONED
 * table (the gate write-order live drill). This covers the comparison made on the re-read.
 */
class FlussGateStateStoreWriteOrderTest {

    private static GateRow row(long token, long epoch, GateState state) {
        return new GateRow("p1", "acct1", state, epoch, "r", "h",
                null, null, null, "owner-" + token, token, 1000L, 9000L, null);
    }

    @Test
    void staleTokenWriteIsRejectedNotSilentlyAccepted() {
        // Durable advanced to 8 (a newer owner took the fence); our write carries 7 and would
        // have been version-dropped — the caller must NOT see success.
        GateRow durable = row(8L, 5, GateState.ENABLED);
        GateRow written = row(7L, 5, GateState.ENABLED);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> FlussGateStateStore.checkPersisted("p1", durable, written, false));
        assertTrue(e.getMessage().contains("version-dropped"), e.getMessage());
    }

    @Test
    void supersededRevokeIsMootNotLost() {
        // A fence-CLEARING write that lost to a strictly newer token is legitimately moot:
        // a newer owner took the fence.
        GateRow durable = row(8L, 5, GateState.ENABLED);
        GateRow written = row(7L, 5, GateState.ENABLED);
        assertDoesNotThrow(() -> FlussGateStateStore.checkPersisted("p1", durable, written, true));
    }

    @Test
    void matchingWriteIsAccepted() {
        GateRow durable = row(7L, 5, GateState.ENABLED);
        GateRow written = row(7L, 5, GateState.ENABLED);
        assertDoesNotThrow(() -> FlussGateStateStore.checkPersisted("p1", durable, written, false));
    }

    @Test
    void clobberWithWrongStateOrEpochIsRejected() {
        // Same token but a different generation/state durable: the write did not land as issued.
        GateRow written = row(7L, 5, GateState.ENABLED);
        assertThrows(IllegalStateException.class, () -> FlussGateStateStore.checkPersisted(
                "p1", row(7L, 6, GateState.ENABLED), written, false));
        assertThrows(IllegalStateException.class, () -> FlussGateStateStore.checkPersisted(
                "p1", row(7L, 5, GateState.HALTED), written, false));
    }

    @Test
    void missingDurableRowAfterWriteIsRejected() {
        assertThrows(IllegalStateException.class, () -> FlussGateStateStore.checkPersisted(
                "p1", null, row(7L, 5, GateState.ENABLED), false));
    }

    @Test
    void haltMintsAheadOfTheStoredTokenSoItIsNeverDropped() {
        // The safety property behind the halt mint: halt takes the fence forward, so its write
        // can never be the lower-version one the tablet drops, and it retires the old holder.
        InMemoryGateStateStore store = new InMemoryGateStateStore(Set.of("saurabh"));
        store.init(new GateRow("p1", "acct1", GateState.HALTED, 0, "boot", "h0",
                null, null, null, null, 0L, null, null, null));
        long acquired = store.acquire("p1", "owner1", 5000L, 1000L).token();

        GateRow halted = store.halt("p1", store.read("p1"), "safety", "h1", 2000L);
        assertTrue(halted.fenceToken() > acquired,
                "halt must outrank the stored token so it cannot be version-dropped");
        assertNull(halted.ownerInstanceId(), "halt must leave the row unfenced");
        assertFalse(halted.fenceValidFor("owner1", acquired, 2000L),
                "the retired holder's token must no longer validate");
    }
}
