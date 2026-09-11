package com.trading.common.safety;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Deterministic manifest-derived slot assignment, byte-parity with the Go
 * {@code BuildSubscriptionPlan} (go-bridge/subscription_plan.go):
 * tokens sorted ascending, chunked into up to {@code slots} contiguous
 * connections of at most {@code connectionLimit} tokens each; slot
 * {@code "hft-N"} owns sorted[ N*limit .. (N+1)*limit ).
 *
 * <p>Slot-scoped safety suppression needs only the slot&rarr;token-set mapping
 * (not the per-request chunking), so this resolver reproduces the Go slot
 * boundaries exactly while omitting the request-partition fingerprint.
 *
 * @param tokens           manifest instrument tokens (positive, unique)
 * @param slots            connection count, 1..{@value #MAX_SLOTS}
 * @param connectionLimit  tokens per connection, 1..{@value #MAX_TOKENS_PER_CONNECTION}
 */
public final class SlotAssignmentResolver implements SlotAssignment {

    /** Matches go-bridge MaxHFTConnections. */
    public static final int MAX_SLOTS = 3;
    /** Matches go-bridge MaxHFTTokensPerConnection. */
    public static final int MAX_TOKENS_PER_CONNECTION = 1024;

    private final List<SlotEntry> slots;
    private final long[] sortedTokens;
    private final int connectionLimit;
    private final String manifestFingerprint;

    private SlotAssignmentResolver(List<SlotEntry> slots, long[] sortedTokens, int connectionLimit) {
        this.slots = slots;
        this.sortedTokens = sortedTokens;
        this.connectionLimit = connectionLimit;
        this.manifestFingerprint = TokenSetHash.of(toList(sortedTokens));
    }

    public static SlotAssignmentResolver of(List<Long> tokens, int slots, int connectionLimit) {
        if (tokens == null || tokens.isEmpty()) {
            throw new IllegalArgumentException("instrument token input must not be empty");
        }
        // P3-125: fail fast with a named error — Long::longValue unboxing
        // would otherwise throw a bare NPE callers can't catch as IAE.
        for (Long t : tokens) {
            if (t == null) {
                throw new IllegalArgumentException("instrument token input must not contain null");
            }
        }
        if (slots <= 0 || slots > MAX_SLOTS) {
            throw new IllegalArgumentException("slot count must be between 1 and " + MAX_SLOTS);
        }
        if (connectionLimit <= 0 || connectionLimit > MAX_TOKENS_PER_CONNECTION) {
            throw new IllegalArgumentException("connection limit must be between 1 and "
                    + MAX_TOKENS_PER_CONNECTION);
        }
        if (tokens.size() > (long) slots * connectionLimit) {
            throw new IllegalArgumentException("token count " + tokens.size()
                    + " exceeds capacity " + ((long) slots * connectionLimit));
        }

        long[] sorted = tokens.stream().mapToLong(Long::longValue).sorted().toArray();
        // R-188 parity: duplicate or non-positive tokens fail plan construction.
        Set<Long> seen = new HashSet<>();
        for (long t : sorted) {
            // P3-353: Go takes []int32 — anything above int32 hashes
            // divergently (full 64-bit putLong vs Go sign-extension), so
            // reject it here instead of producing a hash Go can never match.
            if (t <= 0 || t > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("token " + t + " is invalid (must be positive int32)");
            }
            if (!seen.add(t)) {
                throw new IllegalArgumentException("duplicate token " + t);
            }
        }

        List<SlotEntry> entries = new ArrayList<>();
        for (int i = 0; i < slots; i++) {
            int start = i * connectionLimit;
            if (start >= sorted.length) {
                break;
            }
            int end = Math.min(start + connectionLimit, sorted.length);
            List<Long> slotTokens = new ArrayList<>(end - start);
            for (int k = start; k < end; k++) {
                slotTokens.add(sorted[k]);
            }
            String slotId = "hft-" + i;
            entries.add(new SlotEntry(slotId, slotTokens, TokenSetHash.of(slotTokens)));
        }
        return new SlotAssignmentResolver(entries, sorted, connectionLimit);
    }

    /** Varargs convenience overload (unsorted input is sorted internally). */
    public static SlotAssignmentResolver of(int slots, int connectionLimit, long... tokens) {
        // P3-496: explicit null array would NPE on tokens.length — same IAE
        // contract as the List overload's null/empty check.
        if (tokens == null) {
            throw new IllegalArgumentException("instrument token input must not be null");
        }
        List<Long> list = new ArrayList<>(tokens.length);
        for (long t : tokens) {
            list.add(t);
        }
        return of(list, slots, connectionLimit);
    }

    @Override
    public List<String> slotIds() {
        List<String> ids = new ArrayList<>(slots.size());
        for (SlotEntry e : slots) {
            ids.add(e.slotId());
        }
        return Collections.unmodifiableList(ids);
    }

    @Override
    public String slotIdOf(long token) {
        int pos = java.util.Arrays.binarySearch(sortedTokens, token);
        if (pos < 0) {
            return null;
        }
        return "hft-" + (pos / connectionLimit);
    }

    @Override
    public String tokenSetHashOf(String slotId) {
        for (SlotEntry e : slots) {
            if (e.slotId().equals(slotId)) {
                return e.tokenSetHash();
            }
        }
        return null;
    }

    @Override
    public String manifestFingerprint() {
        return manifestFingerprint;
    }

    private static List<Long> toList(long[] values) {
        List<Long> list = new ArrayList<>(values.length);
        for (long v : values) {
            list.add(v);
        }
        return list;
    }

    record SlotEntry(String slotId, List<Long> tokens, String tokenSetHash)
            implements java.io.Serializable {}
}
