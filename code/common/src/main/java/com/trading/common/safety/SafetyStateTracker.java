package com.trading.common.safety;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Slot-scoped safety state machine (plan.md &sect; "Slot-scoped safety
 * propagation", rules 1-3). Consumes {@code Safety_Halt_Requests} rows from
 * the ingestion writer and answers {@code isTokenSuppressed(token)}.
 *
 * <p>Transition semantics:
 * <ul>
 *   <li>{@code UNSAFE} with no prior state (or after a recovery) opens the
 *       suppression window at that connection epoch.</li>
 *   <li>While already {@code UNSAFE}, only a strictly greater connection
 *       epoch advances the state (a new unsafe instance — the epoch is the
 *       connection-instance boundary, so an equal-epoch re-delivery is a
 *       duplicate regardless of reason); a smaller epoch is stale.</li>
 *   <li>{@code RECOVERED} is the only safe-clearing path and is accepted only
 *       with a strictly greater connection epoch than the {@code UNSAFE} it
 *       clears. A {@code RECOVERED} with no prior {@code UNSAFE} is a no-op.</li>
 *   <li>Rows are gated on source component {@code INGESTION}, contract
 *       version 2, the manifest fingerprint, and the slot's assigned
 *       token-set hash — a request that fails any gate is ignored and never
 *       trusted.</li>
 * </ul>
 *
 * <p>Thread-safe (concurrent map); in the Flink job the tracker is rebuilt
 * from broadcast state on each slot.
 */
public final class SafetyStateTracker {

    /** Outcome of applying one request row. */
    public enum ApplyResult {
        NEW_UNSAFE,
        RECOVERED,
        /** Same slot, equal-epoch transition (re-delivery / replay). */
        IGNORED_DUPLICATE,
        /** Epoch older than the current state's transition epoch. */
        IGNORED_STALE_EPOCH,
        /** RECOVERED without a prior UNSAFE to clear. */
        IGNORED_NO_PRIOR_UNSAFE,
        /** Slot id not present in the manifest-derived assignment. */
        IGNORED_UNKNOWN_SLOT,
        /** Row's manifest fingerprint does not match this assignment's. */
        IGNORED_MANIFEST_MISMATCH,
        /** Row's assigned_token_set_hash disagrees with the assignment. */
        IGNORED_HASH_MISMATCH,
        /** sourceComponent is not INGESTION. */
        IGNORED_SOURCE_COMPONENT,
        /** contractVersion is not 2. */
        IGNORED_CONTRACT_VERSION
    }

    private final SlotAssignment assignment;
    private final Map<String, SlotSafetyState> states = new ConcurrentHashMap<>();

    public SafetyStateTracker(SlotAssignment assignment) {
        if (assignment == null) {
            throw new IllegalArgumentException("assignment must not be null");
        }
        this.assignment = assignment;
    }

    /** The manifest-derived assignment this tracker trusts. */
    public SlotAssignment slotAssignment() {
        return assignment;
    }

    /** Current state for {@code slotId}, or {@code null} if never seen (P3-347: null in → null out, never NPE). */
    public SlotSafetyState stateOf(String slotId) {
        if (slotId == null) {
            return null;
        }
        return states.get(slotId);
    }

    /** True while the slot is in the UNSAFE (suppressed) window (P3-348: null → false, unknown slots never suppressed). */
    public boolean isUnsafe(String slotId) {
        if (slotId == null) {
            return false;
        }
        SlotSafetyState s = states.get(slotId);
        return s != null && s.status() == SlotSafetyStatus.UNSAFE;
    }

    /**
     * True if {@code token}'s owning slot is currently unsafe. Tokens not
     * part of the assignment are never suppressed.
     */
    public boolean isTokenSuppressed(long token) {
        String slotId = assignment.slotIdOf(token);
        return slotId != null && isUnsafe(slotId);
    }

    /** Immutable copy of the per-slot state (for snapshot / broadcast). */
    public Map<String, SlotSafetyState> snapshot() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(states));
    }

    /**
     * Applies one request row. Returns the transition outcome; the tracker
     * state changes only for {@link ApplyResult#NEW_UNSAFE} and
     * {@link ApplyResult#RECOVERED}.
     */
    public ApplyResult apply(SlotSafetyRequest row) {
        // P3-349: explicit fail-fast — the Flink path only catches
        // ParseException, so a raw NPE here would be fatal, not counted/skipped.
        if (row == null) {
            throw new IllegalArgumentException("row must not be null");
        }
        if (!SlotSafetyRequest.SOURCE_COMPONENT_INGESTION.equals(row.sourceComponent())) {
            return ApplyResult.IGNORED_SOURCE_COMPONENT;
        }
        if (row.contractVersion() != SlotSafetyRequest.CONTRACT_VERSION) {
            return ApplyResult.IGNORED_CONTRACT_VERSION;
        }
        if (!assignment.manifestFingerprint().equals(row.manifestFingerprint())) {
            return ApplyResult.IGNORED_MANIFEST_MISMATCH;
        }
        String expectedHash = assignment.tokenSetHashOf(row.slotId());
        if (expectedHash == null) {
            return ApplyResult.IGNORED_UNKNOWN_SLOT;
        }
        if (!expectedHash.equals(row.assignedTokenSetHash())) {
            return ApplyResult.IGNORED_HASH_MISMATCH;
        }

        // P3-122: epoch check + state replacement must be atomic per key —
        // a get/put pair lets a stale interleaving overwrite a newer epoch.
        AtomicReference<ApplyResult> result = new AtomicReference<>();
        states.compute(row.slotId(), (slot, current) ->
                row.status() == SlotSafetyStatus.UNSAFE
                        ? computeUnsafe(row, current, result)
                        : computeRecovered(row, current, result));
        return result.get();
    }

    private SlotSafetyState computeUnsafe(SlotSafetyRequest row, SlotSafetyState current,
                                          AtomicReference<ApplyResult> result) {
        if (current == null) {
            result.set(ApplyResult.NEW_UNSAFE);
            return unsafeState(row);
        }
        // P3-123: a stale UNSAFE after RECOVERED must not regress the epoch —
        // only a strictly newer connection generation re-opens the window.
        if (current.status() == SlotSafetyStatus.RECOVERED) {
            if (row.connectionEpoch() <= current.connectionEpoch()) {
                result.set(row.connectionEpoch() == current.connectionEpoch()
                        ? ApplyResult.IGNORED_DUPLICATE
                        : ApplyResult.IGNORED_STALE_EPOCH);
                return current;
            }
            result.set(ApplyResult.NEW_UNSAFE);
            return unsafeState(row);
        }
        if (row.connectionEpoch() < current.connectionEpoch()) {
            result.set(ApplyResult.IGNORED_STALE_EPOCH);
            return current;
        }
        if (row.connectionEpoch() == current.connectionEpoch()) {
            result.set(ApplyResult.IGNORED_DUPLICATE);
            return current;
        }
        result.set(ApplyResult.NEW_UNSAFE);
        return unsafeState(row);
    }

    private SlotSafetyState computeRecovered(SlotSafetyRequest row, SlotSafetyState current,
                                             AtomicReference<ApplyResult> result) {
        if (current == null || current.status() == SlotSafetyStatus.RECOVERED) {
            result.set(ApplyResult.IGNORED_NO_PRIOR_UNSAFE);
            return current;
        }
        // Recovery must be strictly newer than the unsafe it clears.
        if (row.connectionEpoch() <= current.connectionEpoch()) {
            result.set(ApplyResult.IGNORED_STALE_EPOCH);
            return current;
        }
        result.set(ApplyResult.RECOVERED);
        return new SlotSafetyState(row.slotId(), row.connectionEpoch(), SlotSafetyStatus.RECOVERED,
                "", row.detectionTimeMs(), current.tokenSetHash());
    }

    private SlotSafetyState unsafeState(SlotSafetyRequest row) {
        return new SlotSafetyState(row.slotId(), row.connectionEpoch(), SlotSafetyStatus.UNSAFE,
                row.reasonCode(), row.detectionTimeMs(), row.assignedTokenSetHash());
    }
}
