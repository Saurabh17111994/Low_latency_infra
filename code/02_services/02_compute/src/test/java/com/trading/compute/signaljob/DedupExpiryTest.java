package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.signaljob.DedupExpiry.CleanupCandidate;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * DEC-038 pure unit tests for {@link DedupExpiry}: the writer-enforced logical
 * TTL (Fluss 0.9.1 has no per-key TTL), the read-path expiry semantics (a
 * stale row is never a false "seen"), and the bounded, re-entrant cleanup
 * selection — the exact rules the live cleanup pass drives
 * (docs/08_implementation/04-signal-job.md §fingerprint_dedup).
 */
@DisplayName("DEC-038: DedupExpiry (logical TTL + bounded cleanup selection)")
class DedupExpiryTest {

    private static final long TTL_MS = 60_000L;

    @Test
    @DisplayName("expiry_ms is exactly first_seen_ms + ttl_ms — entries are never expired early")
    void expiryIsFirstSeenPlusTtl() {
        assertEquals(700_000L + TTL_MS, DedupExpiry.expiryMs(700_000L, TTL_MS));
        assertEquals(TTL_MS, DedupExpiry.expiryMs(0L, TTL_MS));
    }

    @Test
    @DisplayName("non-positive TTL is rejected (a zero/negative horizon would wedge the window)")
    void rejectsNonPositiveTtl() {
        assertThrows(IllegalArgumentException.class, () -> DedupExpiry.expiryMs(0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> DedupExpiry.expiryMs(0L, -1L));
    }

    @Test
    @DisplayName("isExpired is strict — equality is still live (a row expiring now is still a seen)")
    void expiryIsStrict() {
        assertTrue(DedupExpiry.isExpired(100L, 101L), "expiry before now is expired");
        assertFalse(DedupExpiry.isExpired(100L, 100L), "expiry == now must still be live");
        assertFalse(DedupExpiry.isExpired(100L, 99L), "expiry after now is live");
    }

    @Test
    @DisplayName("selectBatch keeps only expired rows, earliest expiry first, capped at the batch size")
    void selectBatchFiltersSortsAndCaps() {
        long now = 1_000L;
        List<CleanupCandidate> rows = List.of(
                new CleanupCandidate("late", 100L, 2_000L),    // not expired
                new CleanupCandidate("mid", 100L, 900L),       // expired, expiry 900
                new CleanupCandidate("early", 100L, 500L));    // expired, expiry 500
        List<CleanupCandidate> batch = DedupExpiry.selectBatch(rows, now, 10);
        assertEquals(List.of("early", "mid"),
                batch.stream().map(CleanupCandidate::key).toList(),
                "expired rows only, ordered by earliest expiry");
    }

    @Test
    @DisplayName("selectBatch caps the batch and is re-entrant — no row is ever lost across passes")
    void selectBatchCapsAndResumes() {
        long now = 1_000L;
        List<CleanupCandidate> rows = new java.util.ArrayList<>(List.of(
                new CleanupCandidate("k1", 100L, 400L),
                new CleanupCandidate("k2", 100L, 500L),
                new CleanupCandidate("k3", 100L, 600L),
                new CleanupCandidate("k4", 100L, 700L)));
        // Pass 1: capped batch deletes the earliest-expiring keys.
        List<CleanupCandidate> first = DedupExpiry.selectBatch(rows, now, 2);
        assertEquals(List.of("k1", "k2"), first.stream().map(CleanupCandidate::key).toList());
        // The caller deletes the returned batch; the next pass feeds back the
        // remainder and continues with the next-earliest keys — nothing is lost
        // when a pass is interrupted mid-way.
        rows.removeAll(first);
        List<CleanupCandidate> second = DedupExpiry.selectBatch(rows, now, 2);
        assertEquals(List.of("k3", "k4"), second.stream().map(CleanupCandidate::key).toList());
        // Deterministic: the same input always yields the same batch.
        List<CleanupCandidate> original = List.of(
                new CleanupCandidate("k1", 100L, 400L),
                new CleanupCandidate("k2", 100L, 500L),
                new CleanupCandidate("k3", 100L, 600L),
                new CleanupCandidate("k4", 100L, 700L));
        assertEquals(List.of("k1", "k2"),
                DedupExpiry.selectBatch(original, now, 2).stream()
                        .map(CleanupCandidate::key).toList());
    }

    @Test
    @DisplayName("ties break by key for determinism (same expiry, stable order across passes)")
    void tiesBreakByKey() {
        long now = 1_000L;
        List<CleanupCandidate> rows = List.of(
                new CleanupCandidate("b", 100L, 500L),
                new CleanupCandidate("a", 100L, 500L));
        List<CleanupCandidate> batch = DedupExpiry.selectBatch(rows, now, 10);
        assertEquals(List.of("a", "b"), batch.stream().map(CleanupCandidate::key).toList());
    }

    @Test
    @DisplayName("null and empty candidate lists yield an empty batch")
    void nullAndEmptyAreNoOps() {
        assertEquals(List.of(), DedupExpiry.selectBatch(null, 1_000L, 10));
        assertEquals(List.of(), DedupExpiry.selectBatch(List.of(), 1_000L, 10));
    }

    @Test
    @DisplayName("non-positive batch size is rejected")
    void rejectsNonPositiveBatchSize() {
        assertThrows(IllegalArgumentException.class,
                () -> DedupExpiry.selectBatch(List.of(), 1_000L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> DedupExpiry.selectBatch(List.of(), 1_000L, -1));
    }

    @Test
    @DisplayName("a corrupt row (expiry before first-seen) fails closed instead of being deleted")
    void corruptRowFailsClosed() {
        List<CleanupCandidate> rows = List.of(
                new CleanupCandidate("ok", 100L, 500L),
                new CleanupCandidate("corrupt", 900L, 100L));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DedupExpiry.selectBatch(rows, 1_000L, 10));
        assertTrue(e.getMessage().contains("corrupt"), e.getMessage());
        assertTrue(e.getMessage().contains("corrupt"),
                "the error must surface the corrupt row, got: " + e.getMessage());
    }
}
