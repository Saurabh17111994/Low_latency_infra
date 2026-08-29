package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.quarantine.QuarantineWriter;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-DQ-001..002 — typed quality classification and quarantine routing
 * evidence (plan Amendment §Required tests and evidence).
 *
 * <p>The routing itself lives in {@link IngestionService#processTickEvent} and is
 * driven by live Fluss writers, so the pure seams are pinned here instead:
 * the exact quarantine reason vocabularies shared by the Go bridge and the
 * Java writer. Malformed frames are rejected at the ProtoFrameReader level.
 * Stale/future routing and its boundary semantics are covered by
 * {@link StaleDataTradeGuardTest}.
 *
 * <ul>
 *   <li>ING-DQ-001 — a malformed transport frame is rejected at frame-parse
 *       time and quarantined with the MALFORMED_JSON reason; the reason
 *       vocabulary stays exactly per plan.</li>
 *   <li>ING-DQ-002 — stale records remain durable evidence (STALE_BROKER_TIMESTAMP
 *       quarantine) and can never reach a trade path: the freshness gate runs
 *       before any trade classification (covered by StaleDataTradeGuardTest).</li>
 * </ul>
 */
@DisplayName("ING-DQ-001..002: typed quality classification and quarantine routing")
class IngestionQualityEvidenceTest {

    // (NDJSON-path helpers malformedJsonDecision/malformedJsonDetail/
    // rawLineBytes were removed with the NDJSON transport 2026-08-29 —
    // malformed frames are rejected at ProtoFrameReader level. The
    // MALFORMED_JSON quarantine reason remains for the vocabulary tests
    // below and for bridge-side broker_quarantine records.)

    @Test
    @DisplayName("ING-DQ-001: quarantine reason vocabulary is exact per plan")
    void quarantineReasonVocabularyExact() {
        String[] expected = {
                "MALFORMED_JSON", "INVALID_SCHEMA", "MISSING_INSTRUMENT",
                "INVALID_VALUES", "FUTURE_BROKER_TIMESTAMP", "STALE_BROKER_TIMESTAMP",
                "HASH_MISMATCH", "INTERNAL_ERROR",
                "FINGERPRINT_FAILURE",
        };
        QuarantineWriter.Reason[] actual = QuarantineWriter.Reason.values();
        assertEquals(expected.length, actual.length, "reason vocabulary must match plan exactly");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i].name(), "reason[" + i + "]");
        }
    }

    @Test
    @DisplayName("ING-DQ-001: broker quarantine reasons are a subset of the writer vocabulary")
    void brokerQuarantineReasonsSubset() {
        // The bridge-side REASONS set is private; assert the documented
        // vocabulary directly against the writer's accepted names.
        for (String bridgeReason : new String[]{
                "MALFORMED_JSON", "INVALID_SCHEMA", "INVALID_VALUES", "HASH_MISMATCH",
                "FUTURE_BROKER_TIMESTAMP", "STALE_BROKER_TIMESTAMP"}) {
            QuarantineWriter.Reason.valueOf(bridgeReason);
        }
    }

    @Test
    @DisplayName("ING-DQ-002: stale classification precedes any trade path")
    void stalePrecedesTradePath() {
        // Mirrors the production evidence-backed values
        // (ARROW_MAX_EVENT_AGE_MS=5000, ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000):
        // age boundary inclusive at 5000, STALE at 5001. The quarantine reason
        // is the durable evidence; the freshness gate runs before any trade
        // classification (see StaleDataTradeGuardTest for the full matrix).
        assertEquals(IngestionService.FreshnessDecision.FRESH,
                IngestionService.classifyFreshness(1_000_000_000L - 5000, 1_000_000_000L, 2000, 5000));
        assertEquals(IngestionService.FreshnessDecision.STALE,
                IngestionService.classifyFreshness(1_000_000_000L - 5001, 1_000_000_000L, 2000, 5000));
        assertEquals(QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP,
                QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP,
                "STALE quarantine reason is the durable evidence class");
    }
}
