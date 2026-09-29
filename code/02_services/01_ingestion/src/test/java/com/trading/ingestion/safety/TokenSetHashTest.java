package com.trading.ingestion.safety;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * H2-4 — the per-slot carve/digest must equal the Go bridge's reference
 * implementation. The shared fixture {@code code/testdata/slot-token-hashes.json}
 * is GENERATED from the Go carve (BuildSubscriptionPlan + TokenSetHash); this
 * test fails if either side drifts. The service-level wiring (processBridgeEvent
 * using the verdicts instead of the old full-set comparison) is pinned by
 * {@code SlotTokenHashCrossCheckTest}.
 */
class TokenSetHashTest {

    /** Maven runs tests from the module dir; code/testdata is two levels up. */
    private static final Path FIXTURE = Path.of("..", "..", "testdata", "slot-token-hashes.json");

    private static List<Long> tokens(JsonNode c) {
        List<Long> out = new ArrayList<>();
        c.get("tokens").forEach(t -> out.add(t.asLong()));
        return out;
    }

    private static List<Long> sequence(int count) {
        List<Long> out = new ArrayList<>(count);
        for (long token = 1; token <= count; token++) {
            out.add(token);
        }
        return out;
    }

    @Test
    @DisplayName("fixture: Java carve equals the Go-generated slot ids/sizes/hashes")
    void fixtureMatchesTheGoCarve() throws Exception {
        JsonNode fixture = new ObjectMapper().readTree(Files.readString(FIXTURE));
        int limit = fixture.get("connectionLimit").asInt();
        assertEquals(1024, limit, "fixture connectionLimit");
        JsonNode cases = fixture.get("cases");
        assertTrue(cases.size() > 0, "fixture has cases");
        for (JsonNode c : cases) {
            String name = c.get("name").asText();
            TokenSetHash carved = TokenSetHash.carve(tokens(c), c.get("slotCount").asInt(), limit);
            JsonNode slots = c.get("slots");
            int listed = 0;
            for (int i = 0; i < c.get("slotCount").asInt(); i++) {
                String slotId = "hft-" + i;
                String hash = carved.expectedHash(slotId);
                if (hash == null) {
                    continue; // empty slice: the Go carve does not emit it
                }
                assertTrue(listed < slots.size(), name + ": carve has more slots than the fixture lists");
                JsonNode want = slots.get(listed++);
                assertEquals(slotId, want.get("slotId").asText(), name + ": slot id order");
                assertEquals(want.get("size").asInt(), carved.expectedSize(slotId), name + "/" + slotId + ": size");
                assertEquals(want.get("hash").asText(), hash, name + "/" + slotId + ": digest");
            }
            assertEquals(slots.size(), listed, name + ": fixture lists slots the carve does not have");
            assertEquals(c.get("manifestFingerprint").asText(),
                    SafetyHaltWriter.computeAssignedTokenHash(tokens(c)),
                    name + ": the full-set digest is the manifest_fingerprint the bridge emits");
        }
    }

    @Test
    @DisplayName("verdicts: MATCH/MISMATCH/UNKNOWN_SLOT/EMPTY_SLICE")
    void verdicts() {
        // 3 tokens, 2 slots, limit 2 → hft-0=[1,2], hft-1=[3].
        TokenSetHash carved = TokenSetHash.carve(List.of(3L, 1L, 2L), 2, 2);
        assertEquals(2, carved.expectedSize("hft-0"));
        assertEquals(1, carved.expectedSize("hft-1"));
        assertEquals(SafetyHaltWriter.computeAssignedTokenHash(List.of(1L, 2L)), carved.expectedHash("hft-0"));

        assertEquals(TokenSetHash.Verdict.MATCH, carved.verdict("hft-0", carved.expectedHash("hft-0")));
        assertEquals(TokenSetHash.Verdict.MISMATCH, carved.verdict("hft-0", carved.expectedHash("hft-1")));
        assertEquals(TokenSetHash.Verdict.MISMATCH, carved.verdict("hft-0", "0".repeat(64)));
        assertEquals(TokenSetHash.Verdict.MISMATCH, carved.verdict("hft-0", null));

        assertEquals(TokenSetHash.Verdict.UNKNOWN_SLOT, carved.verdict("hft-9", carved.expectedHash("hft-0")));
        assertEquals(TokenSetHash.Verdict.UNKNOWN_SLOT, carved.verdict("core-0", carved.expectedHash("hft-0")));
        assertEquals(TokenSetHash.Verdict.UNKNOWN_SLOT, carved.verdict(null, carved.expectedHash("hft-0")));

        // 2 tokens, 2 slots → hft-1 is inside the configured count but the carve
        // cannot fill it (Go emits one slot; an event for hft-1 is layout drift).
        TokenSetHash shortPlan = TokenSetHash.carve(List.of(1L, 2L), 2, 2);
        assertEquals(TokenSetHash.Verdict.EMPTY_SLICE, shortPlan.verdict("hft-1", "0".repeat(64)));
        assertNull(shortPlan.expectedHash("hft-1"));
        assertNull(shortPlan.expectedSize("hft-1"));

        // P6-486 stays true: the full-set digest is NOT a slot digest here —
        // that is why the old comparison had to false-positive on a correct ack.
        assertNotEquals(SafetyHaltWriter.computeAssignedTokenHash(List.of(1L, 2L, 3L)),
                carved.expectedHash("hft-0"));

        // Order must not matter (the plan sorts; so does the bridge).
        assertEquals(carved.expectedHash("hft-0"),
                TokenSetHash.carve(List.of(2L, 3L, 1L), 2, 2).expectedHash("hft-0"));

        assertThrows(IllegalArgumentException.class, () -> TokenSetHash.carve(List.of(1L), 0, 2));
        assertThrows(IllegalArgumentException.class, () -> TokenSetHash.carve(List.of(1L), 2, 0));
    }

    @Test
    @DisplayName("the real datastream (2433 NSE instruments) carves into 3 sockets: 1024+1024+385")
    void datastreamUniverseCarvesIntoThreeSockets() {
        // The daily VM runs UNIVERSE=full: 2433 NSE cash instruments over 3
        // sockets (day_run.py). This is the shape H2-4 unblocked — every one
        // of these acks false-positived TOKEN_HASH_MISMATCH before the fix.
        TokenSetHash carved = TokenSetHash.carve(sequence(2433), 3, 1024);
        assertEquals(1024, carved.expectedSize("hft-0"));
        assertEquals(1024, carved.expectedSize("hft-1"));
        assertEquals(385, carved.expectedSize("hft-2"));
        assertNull(carved.expectedHash("hft-3"));
        assertEquals(TokenSetHash.Verdict.UNKNOWN_SLOT, carved.verdict("hft-3", "0".repeat(64)));
        // The tail socket's digest differs from the full-set digest — the
        // exact comparison the old code got wrong.
        assertNotEquals(SafetyHaltWriter.computeAssignedTokenHash(sequence(2433)),
                carved.expectedHash("hft-2"));
    }
}
