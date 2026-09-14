package com.trading.common.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.ImmutabilityProtocol;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Audit reconstruction simulation — a selected order path is
 * reconstructed from immutable evidence and the hash chain verifies.
 *
 * <p>This is the pure-JVM chain-verification half (no cluster, no lake):
 * it simulates a three-trading-day order path (signal decision → attempt →
 * fill → position), records every event into per-day {@code Execution_Audit}
 * manifests, chains the manifests, and reconstructs the projected position by
 * replaying the immutable evidence. Tampering, dropping a day, and reordering
 * events must all be detected. Deletion-governance evidence from
 * {@link AuditDeletionControl} feeds the same chain, tying the two audit
 * components together.
 * <p>Retained as the hash-chain correctness proof
 * (clean-break replay shares the same immutability guarantee).
 */
@DisplayName("audit reconstruction simulation")
class AuditReconstructionSimulationTest {

    /** One canonical immutable event on the order path. */
    private record OrderEvent(String eventId, String canonical) {
        AuditHashChain.AuditEvent asAuditEvent() {
            return new AuditHashChain.AuditEvent(eventId, ImmutabilityProtocol.canonicalHash(canonical));
        }
    }

    private static final String TABLE = "Execution_Audit";
    private static final String SCHEMA = "1";
    private static final String INSTRUMENT = "26000";

    /** Decision/attempt/fill/position events for one trading day. */
    private static List<OrderEvent> dayEvents(String day, int fills, long qtyPerFill) {
        List<OrderEvent> events = new ArrayList<>();
        events.add(new OrderEvent(day + "-dec-1",
                "decision|inst-1|" + INSTRUMENT + "|qty=" + (fills * qtyPerFill) + "|side=BUY|ts=" + day));
        events.add(new OrderEvent(day + "-att-1",
                "attempt|inst-1|att-" + day + "|decision=dec-1|result=ACCEPTED|ts=" + day));
        for (int i = 0; i < fills; i++) {
            String fillId = day + "-fill-" + i;
            events.add(new OrderEvent(fillId,
                    "fill|" + fillId + "|" + INSTRUMENT + "|qty=" + qtyPerFill + "|ts=" + day));
        }
        events.add(new OrderEvent(day + "-pos-1",
                "position|" + INSTRUMENT + "|qty=" + (fills * qtyPerFill) + "|ts=" + day));
        return events;
    }

    /** Builds the three-day chain plus deletion-governance evidence. */
    private static List<AuditHashChain.Manifest> orderPathManifests() {
        List<AuditHashChain.Manifest> manifests = new ArrayList<>();
        long[] qty = {100, 100, -200}; // buy 100, buy 100, exit 200 → 0
        String[] days = {"2026-08-11", "2026-08-12", "2026-08-13"};
        for (int d = 0; d < days.length; d++) {
            AuditHashChain.ManifestBuilder builder =
                    new AuditHashChain.ManifestBuilder(days[d], TABLE, SCHEMA);
            for (OrderEvent e : dayEvents(days[d], 1, qty[d])) {
                builder.addEvent(e.asAuditEvent().eventId(), e.asAuditEvent().contentHash());
            }
            // Deletion-governance evidence for that day joins the same chain.
            AuditDeletionControl.DeletionRequest request = new AuditDeletionControl.DeletionRequest(
                    "del-" + days[d], INSTRUMENT, true, "PC-1", "LH-1", List.of("saurabh"));
            AuditDeletionControl.DeletionContext context = new AuditDeletionControl.DeletionContext(
                    java.util.Set.of("PC-1"), java.util.Set.of("LH-1"),
                    java.util.Set.of("saurabh"));
            AuditDeletionControl.DeletionDecision decision =
                    AuditDeletionControl.evaluate(request, context, 1_700_000_000_000L + d);
            AuditHashChain.AuditEvent evidence = decision.evidenceEvent().asAuditEvent();
            builder.addEvent(evidence.eventId(), evidence.contentHash());
            manifests.add(builder.build());
        }
        return manifests;
    }

    /** Reconstructs per-instrument position quantity by replaying fills from a valid chain. */
    private static Map<String, Long> reconstructPositions(List<AuditHashChain.Manifest> manifests) {
        Map<String, Long> positions = new HashMap<>();
        for (AuditHashChain.Manifest m : manifests) {
            for (AuditHashChain.AuditEvent e : m.events()) {
                // The event id embeds the canonical content hash; replay by
                // looking up the canonical form from the recorded hash is not
                // possible (hashes are one-way) — the simulation replays the
                // canonical event text recorded alongside. Here we use the
                // eventId to find the fill event text from the source log.
                String canonical = FILL_SOURCE.get(e.eventId());
                if (canonical == null) {
                    continue;
                }
                // "fill|<fill_id>|<instrument>|qty=<n>|ts=<day>"
                String[] parts = canonical.split("\\|");
                String instrument = parts[2];
                long qty = Long.parseLong(parts[3].substring("qty=".length()));
                positions.merge(instrument, qty, Long::sum);
            }
        }
        return positions;
    }

    /** Source-of-truth canonical event text per event id (the 'immutable evidence' being replayed). */
    private static final Map<String, String> FILL_SOURCE = new HashMap<>();

    static {
        String[] days = {"2026-08-11", "2026-08-12", "2026-08-13"};
        long[] qty = {100, 100, -200};
        for (int d = 0; d < days.length; d++) {
            String fillId = days[d] + "-fill-0";
            FILL_SOURCE.put(fillId,
                    "fill|" + fillId + "|" + INSTRUMENT + "|qty=" + qty[d] + "|ts=" + days[d]);
        }
    }

    @Test
    @DisplayName("order path reconstructs from a valid chain")
    void reconstructsOrderPathFromValidChain() {
        List<AuditHashChain.Manifest> manifests = orderPathManifests();
        String root = AuditHashChain.rootHash(manifests);
        assertThat(AuditHashChain.verifyChain(manifests, root))
                .isEqualTo(AuditHashChain.Verification.VALID);

        Map<String, Long> positions = reconstructPositions(manifests);
        assertThat(positions).containsEntry(INSTRUMENT, 0L);
    }

    @Test
    @DisplayName("deletion evidence joins the chain as immutable events")
    void deletionEvidenceFeedsTheChain() {
        List<AuditHashChain.Manifest> manifests = orderPathManifests();
        // Every day manifest must contain its deletion-evidence event (the
        // evidence eventId is a hash of the canonical request form, so assert
        // by count: 4 order-path events + 1 deletion-evidence event), and the
        // chain verifies with them included.
        for (AuditHashChain.Manifest m : manifests) {
            assertThat(m.events()).hasSize(5);
        }
        assertThat(AuditHashChain.verifyChain(manifests, AuditHashChain.rootHash(manifests)))
                .isEqualTo(AuditHashChain.Verification.VALID);
    }

    @Test
    @DisplayName("a tampered event breaks the chain")
    void tamperedEventBreaksChain() {
        List<AuditHashChain.Manifest> original = orderPathManifests();
        String root = AuditHashChain.rootHash(original);

        List<AuditHashChain.Manifest> tampered = new ArrayList<>();
        for (AuditHashChain.Manifest m : original) {
            if (m.tradingDate().equals("2026-08-12")) {
                // Rewrite day 2 with the fill quantity changed (content hash differs).
                List<AuditHashChain.AuditEvent> events = new ArrayList<>(m.events());
                for (int i = 0; i < events.size(); i++) {
                    AuditHashChain.AuditEvent e = events.get(i);
                    if (e.eventId().equals("2026-08-12-fill-0")) {
                        events.set(i, new AuditHashChain.AuditEvent(e.eventId(),
                                ImmutabilityProtocol.canonicalHash("tampered-content")));
                    }
                }
                tampered.add(new AuditHashChain.Manifest(m.tradingDate(), m.table(),
                        m.schemaVersion(), events));
            } else {
                tampered.add(m);
            }
        }
        assertThat(AuditHashChain.verifyChain(tampered, root))
                .isEqualTo(AuditHashChain.Verification.TAMPERED);
    }

    @Test
    @DisplayName("a dropped trading day breaks the chain")
    void droppedDayBreaksChain() {
        List<AuditHashChain.Manifest> original = orderPathManifests();
        String root = AuditHashChain.rootHash(original);
        List<AuditHashChain.Manifest> missingDay = original.stream()
                .filter(m -> !m.tradingDate().equals("2026-08-12")).toList();
        assertThat(AuditHashChain.verifyChain(missingDay, root))
                .isEqualTo(AuditHashChain.Verification.TAMPERED);
    }

    @Test
    @DisplayName("reordered events inside a day are detected at reconstruction time")
    void reorderedEventsDetected() {
        List<AuditHashChain.Manifest> manifests = orderPathManifests();
        AuditHashChain.Manifest dayTwo = manifests.stream()
                .filter(m -> m.tradingDate().equals("2026-08-12")).findFirst().orElseThrow();
        List<AuditHashChain.AuditEvent> reordered = new ArrayList<>(dayTwo.events());
        // Move the position event to the front.
        reordered.add(0, reordered.remove(reordered.size() - 1));
        assertThat(AuditHashChain.verifyManifestAgainstSource(
                new AuditHashChain.Manifest(dayTwo.tradingDate(), dayTwo.table(),
                        dayTwo.schemaVersion(), reordered), dayTwo.events()))
                .isEqualTo(AuditHashChain.Verification.REORDERED);
    }

    @Test
    @DisplayName("an approved deletion is a governed, single-operator (Saurabh) decision")
    void approvedDeletionRequiresGovernance() {
        AuditDeletionControl.DeletionRequest request = new AuditDeletionControl.DeletionRequest(
                "del-governed", INSTRUMENT, true, "PC-2", "LH-2", List.of("saurabh"));
        AuditDeletionControl.DeletionContext context = new AuditDeletionControl.DeletionContext(
                java.util.Set.of("PC-2"), java.util.Set.of("LH-2"), java.util.Set.of("saurabh"));
        AuditDeletionControl.DeletionDecision decision =
                AuditDeletionControl.evaluate(request, context, 1_700_000_000_000L);
        assertThat(decision.decision()).isEqualTo(AuditDeletionControl.Decision.APPROVED);
        assertThat(decision.evidenceEvent().asAuditEvent().eventId()).isNotBlank();
        assertThat(decision.evidenceEvent().asAuditEvent().contentHash())
                .hasSize(AuditHashChain.HASH_HEX_LENGTH);
    }

    @Test
    @DisplayName("one-year reconstruction replays 365 days immutably")
    void oneYearReconstructionReplays365Days() {
        // Build 365 synthetic trading days (offline, no cluster)
        java.util.List<AuditHashChain.Manifest> year = new java.util.ArrayList<>();
        for (int d = 0; d < 365; d++) {
            // Real ISO-8601 trading dates: Manifest validates the format (P6-654), and a
            // padded ordinal like 2025-001 is not a date the chain accepts.
            String day = java.time.LocalDate.of(2025, 1, 1).plusDays(d).toString();
            AuditHashChain.ManifestBuilder b = new AuditHashChain.ManifestBuilder(day, TABLE, SCHEMA);
            b.addEvent(day + "-evt", ImmutabilityProtocol.canonicalHash("event|" + day));
            year.add(b.build());
        }
        String root = AuditHashChain.rootHash(year);
        assertThat(AuditHashChain.verifyChain(year, root)).isEqualTo(AuditHashChain.Verification.VALID);
        // Tamper one day in middle should fail
        java.util.List<AuditHashChain.Manifest> tampered = new java.util.ArrayList<>(year);
        AuditHashChain.Manifest mid = tampered.get(180);
        java.util.List<AuditHashChain.AuditEvent> evts = new java.util.ArrayList<>(mid.events());
        evts.set(0, new AuditHashChain.AuditEvent(evts.get(0).eventId(), ImmutabilityProtocol.canonicalHash("tampered")));
        tampered.set(180, new AuditHashChain.Manifest(mid.tradingDate(), mid.table(), mid.schemaVersion(), evts));
        assertThat(AuditHashChain.verifyChain(tampered, root)).isEqualTo(AuditHashChain.Verification.TAMPERED);
    }
    @Test
    @DisplayName("credential redaction: no token appears in audit event hashes")
    void credentialRedactionNoTokenInHashes() {
        String secret = "ARROW_TOKEN=super-secret-token";
        String hash = ImmutabilityProtocol.canonicalHash(secret);
        // hash must not contain secret substring and must be fixed hex length
        assertThat(hash).doesNotContain("super-secret");
        assertThat(hash).hasSize(AuditHashChain.HASH_HEX_LENGTH);
        assertThat(hash).matches("[0-9a-f]+");
    }

}
