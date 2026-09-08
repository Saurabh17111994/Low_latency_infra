package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.InstrumentManifestWriter.ManifestEntry;
import java.util.List;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-SCHEMA-002 / instrument-loader unit tests: the pure parts of
 * {@link InstrumentManifestWriter} (entry validation, DDL-order row mapping,
 * fail-closed manifest validation). No Fluss cluster required.
 */
@DisplayName("InstrumentManifestWriter — row mapping + fail-closed validation")
class InstrumentManifestWriterTest {

    private static ManifestEntry entry(long token, int version) {
        return new ManifestEntry(token, "NIFTY", "NSE", "CM", "EQUITY",
                75, 500L, null, null, null, version, true, 1_700_000_000_000L, "3");
    }

    // ── ManifestEntry validation (R-115/R-116/R-193) ────────────────────────

    @Test
    @DisplayName("entry validation rejects non-positive identity and blank routing fields")
    void entryValidationRejectsInvalidFields() {
        assertThrows(IllegalArgumentException.class,
                () -> entry(0, 1), "token must be positive");
        assertThrows(IllegalArgumentException.class,
                () -> entry(-5, 1), "negative token must fail");
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "  ", "NSE", "CM", "EQUITY",
                        75, 500L, null, null, null, 1, true, 1L, "3"),
                "blank tradingSymbol must fail (R-193)");
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "", "CM", "EQUITY",
                        75, 500L, null, null, null, 1, true, 1L, "3"),
                "blank exchange must fail (R-193)");
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", null, "EQUITY",
                        75, 500L, null, null, null, 1, true, 1L, "3"),
                "null segment must fail (DDL NOT NULL)");
        assertThrows(IllegalArgumentException.class,
                () -> entry(1, 0), "manifestVersion must be positive");
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", "CM", "EQUITY",
                        0, 500L, null, null, null, 1, true, 1L, "3"),
                "non-positive lotSize must fail (R-116)");
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", "CM", "EQUITY",
                        75, 500L, null, null, null, 1, true, 1L, ""),
                "blank schemaVersion must fail");
    }

    @Test
    @DisplayName("P4-066: tick/expiry/option coherence fails fast at the loader")
    void entryValidationRejectsIncoherentOptionFields() {
        // Non-positive tick size propagates to sizing — refuse.
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", "CM", "EQUITY",
                        75, 0L, null, null, null, 1, true, 1L, "3"),
                "non-positive tickSizePaise must fail");
        // Bad option code.
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", "OPT", "OPT",
                        75, 500L, null, 1_700_000_000_000L, "XX", 1, true, 1L, "3"),
                "optionType outside CE/PE must fail");
        // OPT without CE/PE.
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", "OPT", "OPT",
                        75, 500L, null, 1_700_000_000_000L, null, 1, true, 1L, "3"),
                "OPT without optionType must fail");
        // optionType on non-OPT.
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", "CM", "EQUITY",
                        75, 500L, null, null, "CE", 1, true, 1L, "3"),
                "optionType on EQUITY must fail");
        // EQUITY with expiry.
        assertThrows(IllegalArgumentException.class,
                () -> new ManifestEntry(1, "NIFTY", "NSE", "CM", "EQUITY",
                        75, 500L, null, 1_700_000_000_000L, null, 1, true, 1L, "3"),
                "expiry on EQUITY must fail");
        // Coherent OPT passes.
        ManifestEntry opt = new ManifestEntry(1, "NIFTY", "NSE", "OPT", "OPT",
                75, 500L, null, 1_700_000_000_000L, "CE", 1, true, 1L, "3");
        assertTrue(opt.optionType().equals("CE"));
    }

    // ── DDL-order row mapping ───────────────────────────────────────────────

    @Test
    @DisplayName("toRow builds the 14-column DDL-order row with correct values and types")
    void toRowMatchesDdlColumnOrder() {
        ManifestEntry e = new ManifestEntry(123, "NIFTY", "NSE", "FUT", "FUTIDX",
                75, 500L, null, 1_700_000_000_000L, null, 7, true,
                1_700_000_000_111L, "3");
        InternalRow row = InstrumentManifestWriter.toRow(e);

        // 14 columns in DDL order: token, symbol, exchange, segment, type,
        // lot, tick, strike, expiry, option, version, active, loaded_ts, schema.
        assertEquals(14, row.getFieldCount(), "row must carry all 14 DDL columns");
        assertEquals(123L, row.getLong(0), "instrument_token BIGINT");
        assertEquals("NIFTY", row.getString(1).toString(), "trading_symbol STRING");
        assertEquals("NSE", row.getString(2).toString(), "exchange STRING");
        assertEquals("FUT", row.getString(3).toString(), "segment STRING");
        assertEquals("FUTIDX", row.getString(4).toString(), "instrument_type STRING");
        assertEquals(75, row.getInt(5), "lot_size INT");
        assertEquals(500L, row.getLong(6), "tick_size_paise BIGINT");
        assertTrue(row.isNullAt(7), "strike_paise null when absent");
        assertEquals(1_700_000_000_000L, row.getLong(8), "expiry BIGINT");
        assertTrue(row.isNullAt(9), "option_type null when absent");
        assertEquals(7, row.getInt(10), "manifest_version INT");
        assertTrue(row.getBoolean(11), "is_active BOOLEAN");
        assertEquals(1_700_000_000_111L, row.getLong(12), "loaded_ts BIGINT");
        assertEquals("3", row.getString(13).toString(), "schema_version STRING");
    }

    @Test
    @DisplayName("toRow keeps nullable option fields null (not blanked)")
    void toRowKeepsNullablesNull() {
        // instrumentType/tickSize/strike/expiry/optionType all absent.
        ManifestEntry e = new ManifestEntry(1, "NIFTY", "NSE", "CM", null,
                75, null, null, null, null, 1, true, 1L, "3");
        InternalRow row = InstrumentManifestWriter.toRow(e);
        assertTrue(row.isNullAt(4), "instrument_type null");
        assertTrue(row.isNullAt(6), "tick_size_paise null");
        assertTrue(row.isNullAt(7), "strike_paise null");
        assertTrue(row.isNullAt(9), "option_type null");
    }

    // ── Manifest-level fail-closed validation ───────────────────────────────

    @Test
    @DisplayName("validate refuses an empty manifest and duplicate composite keys")
    void validateRejectsEmptyAndDuplicates() {
        assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestWriter.validate(List.of()),
                "an empty manifest must be refused (operator never half-loads)");

        List<ManifestEntry> dup = List.of(entry(1, 1), entry(2, 1), entry(1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestWriter.validate(dup),
                "duplicate (instrument_token, manifest_version) in one manifest must be refused");

        // Distinct versions of the same token are legal — R-090 version retention.
        List<ManifestEntry> ok = List.of(entry(1, 1), entry(1, 2), entry(2, 1));
        InstrumentManifestWriter.validate(ok);
    }

    @Test
    @DisplayName("P1-232: null element fails closed with a clear IAE, not a bare NPE")
    void validateRejectsNullElement() {
        List<ManifestEntry> withNull = new java.util.ArrayList<>();
        withNull.add(entry(1, 1));
        withNull.add(null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestWriter.validate(withNull),
                "null element must be refused with a context-carrying IAE");
        assertTrue(ex.getMessage().contains("null manifest entry"),
                "message must name the null entry, got: " + ex.getMessage());
    }

    // ── P1-231 kv.format-version gate ───────────────────────────────────────

    @Test
    @DisplayName("preflight accepts format-version 2")
    void kvFormatVersionTwoPasses() {
        TablePath path = TablePath.of("default", "instruments");
        InstrumentManifestWriter.requireKvFormatVersion(path, "2");
    }

    @Test
    @DisplayName("preflight refuses version 1 and a missing key (pre-key-era tables are v1)")
    void kvFormatVersionOneOrMissingFailsFast() {
        TablePath path = TablePath.of("default", "instruments");
        IllegalStateException v1 = assertThrows(IllegalStateException.class,
                () -> InstrumentManifestWriter.requireKvFormatVersion(path, "1"),
                "a v1 table passes PK+bucket checks but dies later raw — refuse here");
        assertTrue(v1.getMessage().contains("table.kv.format-version=1"));
        IllegalStateException missing = assertThrows(IllegalStateException.class,
                () -> InstrumentManifestWriter.requireKvFormatVersion(path, null),
                "a missing key means a pre-key-era (v1) table — must fail, not sail through");
        assertTrue(missing.getMessage().contains("must be 2"));
    }
}
