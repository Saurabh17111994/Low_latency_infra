package com.trading.ingestion.safety;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * H2-4 — the per-slot token-set carve and digest verdicts.
 *
 * <p>The Go bridge shards the subscription plan exactly like
 * {@code BuildSubscriptionPlan} (go-bridge/subscription_plan.go): tokens sorted
 * ascending, carved into slices of {@code ARROW_HFT_MAX_TOKENS_PER_CONNECTION},
 * slot ids {@code hft-{i}}. Every bridge event reports the digest of THAT
 * slot's slice in {@code assigned_token_set_hash} (main.go
 * {@code SetSlotTokenHash}) and the full-plan digest in
 * {@code manifest_fingerprint}. Before this class the service compared the
 * per-slot value against the full-set digest, so any plan with more than one
 * non-empty slot raised TOKEN_HASH_MISMATCH on every acknowledgement — a false
 * alarm on a correct bridge.
 *
 * <p>This carve mirrors the Go one byte for byte (same sort, same slice size,
 * same ids), so the comparison is like for like. One deliberate difference:
 * Go stops emitting slots when tokens run out, while this class keeps a slot
 * inside {@code [0, slotCount)} with no slice as an {@link Verdict#EMPTY_SLICE}
 * entry — that is exactly the layout drift the cross-check must detect (the
 * bridge reporting a slot the Java plan cannot fill).
 *
 * <p>The digest primitive is {@link SafetyHaltWriter#computeAssignedTokenHash(List)}
 * — the same one the full-set field uses — and is byte-identical to the Go
 * {@code TokenSetHash}. The shared fixture
 * {@code code/testdata/slot-token-hashes.json} (generated from the Go carve)
 * pins the two implementations together in tests.
 */
public final class TokenSetHash {

    /** Outcome of comparing a bridge-reported slot digest against the carve. */
    public enum Verdict {
        /** The reported digest equals the carved slice digest. */
        MATCH,
        /** The reported digest differs from the carved slice digest (or is absent). */
        MISMATCH,
        /** The slot id is not part of the carved plan at all. */
        UNKNOWN_SLOT,
        /** The slot id is inside the configured slot count but the carve has no tokens for it. */
        EMPTY_SLICE
    }

    private final Map<String, String> expectedBySlot;
    private final Map<String, Integer> sizeBySlot;
    private final Set<String> emptySlots;

    private TokenSetHash(Map<String, String> expectedBySlot,
                         Map<String, Integer> sizeBySlot,
                         Set<String> emptySlots) {
        this.expectedBySlot = expectedBySlot;
        this.sizeBySlot = sizeBySlot;
        this.emptySlots = emptySlots;
    }

    /**
     * Carves {@code tokens} exactly like the Go {@code BuildSubscriptionPlan}.
     *
     * @param tokens          instrument tokens, any order (sorted here)
     * @param slotCount       configured HFT connection count (ARROW_HFT_CONNECTIONS)
     * @param connectionLimit tokens per slot (ARROW_HFT_MAX_TOKENS_PER_CONNECTION)
     */
    public static TokenSetHash carve(List<Long> tokens, int slotCount, int connectionLimit) {
        if (slotCount <= 0) {
            throw new IllegalArgumentException("slotCount must be positive, got " + slotCount);
        }
        if (connectionLimit <= 0) {
            throw new IllegalArgumentException("connectionLimit must be positive, got " + connectionLimit);
        }
        List<Long> ordered = new ArrayList<>(tokens);
        Collections.sort(ordered);
        Map<String, String> expected = new LinkedHashMap<>();
        Map<String, Integer> sizes = new LinkedHashMap<>();
        Set<String> empty = new LinkedHashSet<>();
        for (int i = 0; i < slotCount; i++) {
            String slotId = "hft-" + i;
            int start = i * connectionLimit;
            if (start >= ordered.size()) {
                empty.add(slotId);
                continue;
            }
            List<Long> slice = ordered.subList(start, Math.min(ordered.size(), start + connectionLimit));
            expected.put(slotId, SafetyHaltWriter.computeAssignedTokenHash(slice));
            sizes.put(slotId, slice.size());
        }
        return new TokenSetHash(expected, sizes, empty);
    }

    /** Compares a bridge-reported digest for one slot. Never null. */
    public Verdict verdict(String slotId, String reportedHash) {
        if (slotId == null || !expectedBySlot.containsKey(slotId)) {
            return emptySlots.contains(slotId) ? Verdict.EMPTY_SLICE : Verdict.UNKNOWN_SLOT;
        }
        return expectedBySlot.get(slotId).equals(reportedHash) ? Verdict.MATCH : Verdict.MISMATCH;
    }

    /** The carved slice digest for {@code slotId}, or null when there is none. */
    public String expectedHash(String slotId) {
        return expectedBySlot.get(slotId);
    }

    /** The carved slice size for {@code slotId}, or null when there is none. */
    public Integer expectedSize(String slotId) {
        return sizeBySlot.get(slotId);
    }
}
