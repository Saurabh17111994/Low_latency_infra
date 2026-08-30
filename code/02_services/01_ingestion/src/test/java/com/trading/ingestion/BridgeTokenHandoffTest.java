package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.trading.ingestion.model.Instrument;

/**
 * G3 single source of truth (2026-08-31): startBridge hands the Go child
 * EXACTLY the manifest token set Java loaded, via the bridge's
 * ARROW_INSTRUMENT_TOKENS env path (which takes precedence over its own CSV
 * read). Before this change, the parent env carried a 1,024-token slice
 * while Java loaded the full 2,431-row CSV — every bridge event tripped the
 * fingerprint cross-check with a false mismatch. joinTokensCsv is the
 * serialization half of that handoff; the child-env overwrite itself is
 * covered by the live harness guard (zero mismatch lines in java.out).
 */
@DisplayName("G3: bridge token handoff serialization")
class BridgeTokenHandoffTest {

    @Test
    @DisplayName("joinTokensCsv: comma-separated, no trailing comma, preserves order")
    void joinsInOrder() {
        assertEquals("4,128,9000", IngestionService.joinTokensCsv(List.of(4L, 128L, 9000L)));
    }

    @Test
    @DisplayName("joinTokensCsv: single token has no comma")
    void singleToken() {
        assertEquals("4", IngestionService.joinTokensCsv(List.of(4L)));
    }

    @Test
    @DisplayName("joinTokensCsv: empty set serializes to empty string")
    void emptySet() {
        assertEquals("", IngestionService.joinTokensCsv(List.of()));
    }

    @Test
    @DisplayName("format matches the bridge's parseTokensEnv contract (int32 tokens round-trip)")
    void matchesBridgeParserContract() {
        // parseTokensEnv: comma-separated, per-token trim, int32 range 1..2147483647.
        // Instrument tokens are int32 by contract, so the joined string must
        // be parseable verbatim by the bridge with every token surviving.
        List<Long> tokens = List.of(1L, 2147483647L, 100000L, 104900L);
        String joined = IngestionService.joinTokensCsv(tokens);
        String[] parts = joined.split(",", -1);
        assertEquals(tokens.size(), parts.length, "token count must survive the split");
        for (int i = 0; i < parts.length; i++) {
            assertEquals(tokens.get(i).longValue(), Long.parseLong(parts[i].trim()),
                    "token " + i + " must round-trip");
        }
    }

    @Test
    @DisplayName("hash of handed-over set == Java's manifest fingerprint input")
    void handoffMatchesFingerprintInput() {
        // The whole point of G3: whatever set Java hashes for the manifest
        // fingerprint must be byte-identical to the set the bridge receives.
        // computeFingerprint hashes the SORTED token list; the bridge's
        // tokenSetHash also sorts before hashing — so the handoff set and
        // the fingerprint input are the same set by construction. This test
        // pins that the join does not drop or reorder a token.
        List<Long> manifestTokens = List.of(9000L, 4L, 128L, 7L);
        java.util.function.Function<Long, Instrument> mk = t ->
                new Instrument.Builder().instrumentToken(t).tradingSymbol("S" + t)
                        .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build();
        String joined = IngestionService.joinTokensCsv(manifestTokens);
        List<Long> parsed = java.util.Arrays.stream(joined.split(","))
                .map(String::trim).map(Long::parseLong).toList();
        assertEquals(manifestTokens, parsed, "handoff must preserve the exact set");
        assertEquals(InstrumentManifestLoader.computeFingerprint(
                        manifestTokens.stream().map(mk).toList()),
                InstrumentManifestLoader.computeFingerprint(
                        parsed.stream().map(mk).toList()),
                "fingerprint over the handed-over set must equal Java's");
    }
}
