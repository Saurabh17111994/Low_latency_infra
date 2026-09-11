package com.trading.common.safety;

import java.io.Serializable;
import java.util.List;

/**
 * Deterministic manifest-derived slot assignment for safety suppression
 * (plan.md &sect; "Slot-scoped safety propagation", rule 4).
 *
 * <p>The tracker consults this to (a) resolve which slot owns a token and
 * (b) verify that an incoming safety request's {@code assigned_token_set_hash}
 * matches the hash of the tokens deterministically assigned to that slot.
 * A request whose hash disagrees with the manifest-derived assignment is not
 * trusted (its slot could have been claimed by a different manifest).
 *
 * <p>Serializable: the assignment is a value object carried as a field of the
 * safety-halt consumer's Flink operator, so it crosses the job-graph
 * serialization boundary (Item E live run, 2026-08-18).
 */
public interface SlotAssignment extends Serializable {

    /** All slot ids, e.g. {@code ["hft-0", "hft-1"]} in slot order (P3-350: unmodifiable, non-null — shared Flink-operator state). */
    List<String> slotIds();

    /**
     * Slot that owns {@code token}, or {@code null} if the token is not part
     * of this assignment (not subscribed under the manifest).
     * P3-351: null means unassigned — callers must null-check before map use.
     */
    /* @Nullable */ String slotIdOf(long token);

    /**
     * {@code TokenSetHash} of the tokens assigned to {@code slotId}, or
     * {@code null} for an unknown slot (including null input — never throws, P3-124).
     * Callers must null-check (safety-trust path).
     */
    /* @Nullable */ String tokenSetHashOf(/* @Nullable */ String slotId);

    /** {@code TokenSetHash} of the full manifest token set (P3-352: never null). */
    /* @NonNull */ String manifestFingerprint();
}
