package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.ingestion.model.Instrument;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ING-INT-001: Manifest load from weekly CSV, subscription completeness.
 *
 * <p>Loads the real Arrow broker instrument CSV from
 * {@code Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv} and
 * verifies all ~2400 instruments are parsed with required fields.
 *
 * <p>Set {@code INGESTION_INT_TEST_MANIFEST=true} to run.
 */
@DisplayName("ING-INT-001: Manifest Load")
class ManifestLoadTest {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestLoadTest.class);

    @Test
    @DisplayName("Load NSE CSV manifest — all instruments parsed, non-null fields")
    void loadProductionManifest() {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("INGESTION_INT_TEST_MANIFEST", "false")),
                "Skipping — set INGESTION_INT_TEST_MANIFEST=true");

        InstrumentManifestLoader.ManifestResult result = InstrumentManifestLoader.loadFromPath(
                "/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/"
                        + "Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv");

        assertTrue(result.approved(), "manifest should be approved");
        assertEquals(1, result.version());
        assertTrue(result.instrumentCount() > 2000,
                "expected >2000 instruments, got " + result.instrumentCount());

        List<Instrument> instruments = result.instruments();
        LOG.info("manifest: loaded {} instruments", instruments.size());

        for (Instrument inst : instruments) {
            assertTrue(inst.instrumentToken() > 0, "token must be positive");
            assertNotNull(inst.tradingSymbol(), "symbol must not be null");
            assertTrue(!inst.tradingSymbol().isBlank(), "symbol must not be blank");
            assertEquals("NSE", inst.exchange());
            assertTrue(inst.lotSize() > 0, "lot size must be positive");
        }

        // Known symbol should be present
        boolean hasReliance = instruments.stream()
                .anyMatch(i -> "RELIANCE-EQ".equals(i.tradingSymbol()));
        assertTrue(hasReliance, "RELIANCE-EQ should be in manifest");

        // Fingerprint is deterministic SHA-256
        String fp = InstrumentManifestLoader.computeFingerprint(instruments);
        assertEquals(64, fp.length(), "SHA-256 hex is 64 chars");
        LOG.info("manifest: fingerprint={}", fp.substring(0, 12));
    }

    @Test
    @DisplayName("Quoted header CSV is parsed with correct columns")
    void parsesQuotedHeader() {
        List<String> cols = InstrumentManifestLoader.parseCsvRecord(
                "\"Exchange\",\"Segment\",\"ExchSeg\",\"Token\",\"TradingSymbol\"");
        assertEquals(5, cols.size());
        assertEquals("Exchange", cols.get(0));
        assertEquals("Token", cols.get(3));
        assertEquals("TradingSymbol", cols.get(4));
    }

    @Test
    @DisplayName("Synthetic manifest tokens match MockArrowServer formula (R-027)")
    void syntheticSetMatchesMockArrowServer() {
        // R-027 regression: the synthetic set used 100_000 + i*100 + (i%10),
        // which shares only 5 tokens with MockArrowServer's 100_000 + i*100 —
        // so fake-broker ticks were mostly quarantined as MISSING_INSTRUMENT
        // and the subscription-completeness check never passed.
        List<Instrument> set = InstrumentManifestLoader.syntheticSet();
        assertEquals(50, set.size(), "must be 50 synthetic instruments");

        for (int i = 0; i < 50; i++) {
            long expected = 100_000L + i * 100L;
            assertTrue(set.stream().anyMatch(inst -> inst.instrumentToken() == expected),
                    "token " + expected + " (MockArrowServer formula) missing from set");
        }
        assertEquals(50, set.stream().map(Instrument::instrumentToken).distinct().count(),
                "all 50 tokens must be unique");

        // The exact 5-token overlap that the old formula produced must be gone:
        // 100_000 + i*100 + (i%10) only coincided at i%10 == 0 (5 values).
        assertEquals(50, set.stream()
                .filter(inst -> inst.instrumentToken() >= 100_000L && inst.instrumentToken() <= 104_900L)
                .count(), "all tokens must be within the MockArrowServer range");
    }

    @Test
    @DisplayName("Quoted field containing a comma is preserved as one field")
    void quotedCommaPreserved() {
        List<String> cols = InstrumentManifestLoader.parseCsvRecord(
                "1,\"Final Dividend - Rs. - 0.6500;23 Jul 2026\",2");
        assertEquals(3, cols.size());
        assertEquals("Final Dividend - Rs. - 0.6500;23 Jul 2026", cols.get(1));
    }

    @Test
    @DisplayName("Escaped quotes are unescaped correctly")
    void escapedQuoteUnescaped() {
        List<String> cols = InstrumentManifestLoader.parseCsvRecord(
                "\"a\"\"b\"\"c\"");
        assertEquals(1, cols.size());
        assertEquals("a\"b\"c", cols.get(0));
    }

    @Test
    @DisplayName("Unterminated quoted field is rejected")
    void unterminatedQuoteRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestLoader.parseCsvRecord("\"unterminated"));
    }

    @Test
    @DisplayName("Real quoted 1024-manifest file loads 1024 instruments")
    void loadsRealQuotedManifest() {
        String path = "/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/"
                + "Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv";
        if (!java.nio.file.Files.exists(java.nio.file.Path.of(path))) {
            assumeTrue(false, "Real quoted manifest not present on this machine");
        }
        InstrumentManifestLoader.ManifestResult result =
                InstrumentManifestLoader.loadFromPath(path);
        assertTrue(result.approved(), "manifest should be approved");
        assertEquals(1024, result.instrumentCount(),
                "expected 1024 instruments, got " + result.instrumentCount());
        assertEquals(1024, result.instruments().size());
    }

    // ---- P1-068 header matching: case-insensitive + FullName alias ----

    private static String writeTempCsv(String name, String content) throws Exception {
        java.nio.file.Path p = java.nio.file.Files.createTempFile(name, ".csv");
        java.nio.file.Files.writeString(p, content);
        return p.toString();
    }

    @Test
    @DisplayName("P1-068: documented FullName header loads symbols (was: every row blank)")
    void fullNameHeaderLoadsSymbols() throws Exception {
        String path = writeTempCsv("fullname",
                "Exchange,Segment,ExchSeg,Token,FullName,LotSize\n"
                        + "NSE,CM,NSE_CM,3045,RELIANCE-EQ,1\n"
                        + "NSE,CM,NSE_CM,1333,HDFCBANK-EQ,1\n");
        InstrumentManifestLoader.ManifestResult result =
                InstrumentManifestLoader.loadFromPath(path, 1);
        assertTrue(result.approved(), "FullName CSV must load");
        assertEquals(2, result.instrumentCount());
        assertTrue(result.instruments().stream()
                .anyMatch(i -> "RELIANCE-EQ".equals(i.tradingSymbol())));
    }

    @Test
    @DisplayName("P1-068: lowercase headers load (was: exact-case miss aborts file)")
    void lowercaseHeadersLoad() throws Exception {
        String path = writeTempCsv("lower",
                "exchange,segment,exchseg,token,fullname,lotsize\n"
                        + "NSE,CM,NSE_CM,3045,RELIANCE-EQ,1\n");
        InstrumentManifestLoader.ManifestResult result =
                InstrumentManifestLoader.loadFromPath(path, 1);
        assertTrue(result.approved(), "lowercase headers must load");
        assertEquals("RELIANCE-EQ", result.instruments().get(0).tradingSymbol());
    }

    @Test
    @DisplayName("P1-068: missing symbol column fails fast at the header")
    void missingSymbolColumnFailsFast() throws Exception {
        String path = writeTempCsv("nosym",
                "Exchange,Segment,Token,LotSize\nNSE,CM,3045,1\n");
        InstrumentManifestLoader.ManifestResult result =
                InstrumentManifestLoader.loadFromPath(path, 1);
        assertEquals(0, result.instrumentCount(), "nothing loadable without a symbol column");
    }

    // ---- P1-070 fingerprint: every load-bearing field counts ----

    private static Instrument inst(long token, String symbol, String exch, int lot) {
        return new Instrument.Builder().instrumentToken(token).tradingSymbol(symbol)
                .exchange(exch).segment("CM").lotSize(lot).manifestVersion(1).build();
    }

    @Test
    @DisplayName("P1-070: same tokens different symbols fingerprint differently")
    void fingerprintCoversTradingSymbol() {
        String a = InstrumentManifestLoader.computeFingerprint(
                List.of(inst(3045, "RELIANCE-EQ", "NSE", 1)));
        String b = InstrumentManifestLoader.computeFingerprint(
                List.of(inst(3045, "RELIANCE-BL", "NSE", 1)));
        assertTrue(!a.equals(b), "symbol change must change the fingerprint");
    }

    @Test
    @DisplayName("P1-070: same tokens different lotSize/exchange fingerprint differently")
    void fingerprintCoversLotSizeAndExchange() {
        String a = InstrumentManifestLoader.computeFingerprint(
                List.of(inst(3045, "RELIANCE-EQ", "NSE", 1)));
        String lot = InstrumentManifestLoader.computeFingerprint(
                List.of(inst(3045, "RELIANCE-EQ", "NSE", 2)));
        String exch = InstrumentManifestLoader.computeFingerprint(
                List.of(inst(3045, "RELIANCE-EQ", "BSE", 1)));
        assertTrue(!a.equals(lot), "lotSize change must change the fingerprint");
        assertTrue(!a.equals(exch), "exchange change must change the fingerprint");
    }

    @Test
    @DisplayName("P1-070: identical manifests fingerprint identically (determinism pin)")
    void fingerprintDeterministic() {
        String a = InstrumentManifestLoader.computeFingerprint(
                List.of(inst(3045, "RELIANCE-EQ", "NSE", 1), inst(1333, "HDFCBANK-EQ", "NSE", 1)));
        String b = InstrumentManifestLoader.computeFingerprint(
                List.of(inst(1333, "HDFCBANK-EQ", "NSE", 1), inst(3045, "RELIANCE-EQ", "NSE", 1)));
        assertEquals(a, b, "order-independent deterministic fingerprint");
    }

    @Test
    @DisplayName("P1-086: omitted lotSize fails fast (no silent default 1)")
    void omittedLotSizeFailsFast() {
        assertThrows(IllegalArgumentException.class,
                () -> new Instrument.Builder().instrumentToken(3045)
                        .tradingSymbol("RELIANCE-EQ").exchange("NSE")
                        .segment("CM").manifestVersion(1).build(),
                "an omitted lotSize must fail the R-116 positive check");
    }

    @Test
    @DisplayName("P1-086: explicit lotSize 1 still legal (cash-equity truth)")
    void explicitLotSizeOneIsLegal() {
        Instrument in = inst(3045, "RELIANCE-EQ", "NSE", 1);
        assertEquals(1, in.lotSize());
    }

    @Test
    @DisplayName("P1-086: CSV without LotSize column refuses the load")
    void missingLotSizeColumnRefusesLoad(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("no-lot.csv");
        Files.writeString(csv, "Token,TradingSymbol,Exchange\n3045,RELIANCE-EQ,NSE\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertFalse(r.approved(), "missing LotSize column must refuse the load, not default to 1");
    }

    @Test
    @DisplayName("P1-086: blank lotSize cell aborts the load (fail-closed)")
    void blankLotSizeCellAbortsLoad(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("blank-lot.csv");
        Files.writeString(csv,
                "Token,TradingSymbol,Exchange,LotSize\n3045,RELIANCE-EQ,NSE,\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertFalse(r.approved(), "blank lotSize must abort the load, not mask as 1");
    }

    @Test
    @DisplayName("P1-086: zero lotSize cell aborts the load (fail-closed)")
    void zeroLotSizeCellAbortsLoad(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("zero-lot.csv");
        Files.writeString(csv,
                "Token,TradingSymbol,Exchange,LotSize\n3045,RELIANCE-EQ,NSE,0\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertFalse(r.approved(), "zero lotSize must abort the load, not mask as 1");
    }

    // ---- P1-227: manifest version from env, not hardcoded 1 ---- 

    @Test
    @DisplayName("P1-227: INSTRUMENT_MANIFEST_VERSION=42 is honored on loadFromPath")
    void manifestVersionFromEnv(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("v42.csv");
        Files.writeString(csv, "Token,TradingSymbol,Exchange,LotSize\n3045,RELIANCE-EQ,NSE,1\n");
        InstrumentManifestLoader.ManifestResult lbd =
                InstrumentManifestLoader.loadFromPath(csv.toString(), "42");
        assertEquals(42, lbd.version(), "env version must replace hardcoded 1");
        assertTrue(lbd.approved(), "valid CSV at version 42 must load");

        InstrumentManifestLoader.ManifestResult explicit =
                InstrumentManifestLoader.loadFromPath(csv.toString(), "7");
        assertEquals(7, explicit.version(), "explicit version string must win");

        // and the pure no-env entry point still defaults to 1
        InstrumentManifestLoader.ManifestResult plain =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertEquals(1, plain.version(), "blank env keeps today's version-1 behavior");
    }

    @Test
    @DisplayName("P1-227: garbage INSTRUMENT_MANIFEST_VERSION refuses the load")
    void garbageManifestVersionRefusesLoad(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("garbage-version.csv");
        Files.writeString(csv, "Token,TradingSymbol,Exchange,LotSize\n3045,RELIANCE-EQ,NSE,1\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString(), "not-a-number");
        assertFalse(r.approved(), "garbage version must refuse the load, not sail as 1");
    }

    @Test
    @DisplayName("P1-227: parseManifestVersion defaults 1, rejects blank/zero/negative/garbage")
    void parseManifestVersionEdges() {
        assertEquals(1, InstrumentManifestLoader.parseManifestVersion(null));
        assertEquals(1, InstrumentManifestLoader.parseManifestVersion("  "));
        assertEquals(3, InstrumentManifestLoader.parseManifestVersion("  3 "));
        assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestLoader.parseManifestVersion("0"));
        assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestLoader.parseManifestVersion("-2"));
        assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestLoader.parseManifestVersion("two"));
    }

    @Test
    @DisplayName("P1-229: Segment column is honored; absent column falls back to CM")
    void segmentColumnHonored(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("seg.csv");
        Files.writeString(csv,
                "Exchange,Segment,ExchSeg,Token,TradingSymbol,LotSize\n"
                        + "NSE,FO,NSE_FO,1001,RELIANCE-FUT,75\n"
                        + "NSE,CM,NSE_CM,3045,RELIANCE-EQ,1\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertTrue(r.approved(), "Segment column CSV must load");
        assertEquals(2, r.instrumentCount());
        assertTrue(r.instruments().stream()
                .anyMatch(i -> i.instrumentToken() == 1001 && "FO".equals(i.segment())),
                "FO row must carry Segment=FO, not hardcoded CM");
        assertTrue(r.instruments().stream()
                .anyMatch(i -> i.instrumentToken() == 3045 && "CM".equals(i.segment())),
                "CM row must carry Segment=CM");
    }

    @Test
    @DisplayName("P1-229: missing Segment column still defaults to CM (behavior preserved)")
    void segmentMissingDefaultsToCM(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("no-seg.csv");
        Files.writeString(csv, "Token,TradingSymbol,Exchange,LotSize\n3045,RELIANCE-EQ,NSE,1\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertTrue(r.approved(), "segment-less CSV must still load");
        assertEquals("CM", r.instruments().get(0).segment());
    }

    // ---- P1-226: non-positive tokens refuse the load (fail-closed) ----

    @Test
    @DisplayName("P1-226: negative token refuses the load (poisoned queue index)")
    void negativeTokenRefusesLoad(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("neg-token.csv");
        Files.writeString(csv,
                "Token,TradingSymbol,Exchange,LotSize\n3045,RELIANCE-EQ,NSE,1\n-7,POISON-EQ,NSE,1\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertFalse(r.approved(), "negative token must refuse the load, not poison the map");
    }

    @Test
    @DisplayName("P1-226: zero token refuses the load (no default identity)")
    void zeroTokenRefusesLoad(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("zero-token.csv");
        Files.writeString(csv,
                "Token,TradingSymbol,Exchange,LotSize\n0,ZERO-EQ,NSE,1\n");
        InstrumentManifestLoader.ManifestResult r =
                InstrumentManifestLoader.loadFromPath(csv.toString());
        assertFalse(r.approved(), "zero token must refuse the load");
    }

}
