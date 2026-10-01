package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.RawTableSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.fluss.metadata.TableDescriptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * DDL-bootstrap guards (review R-003 / R-006 / R-007 / R-008, R-154).
 *
 * <ul>
 *   <li><b>No-drop invariant (R-003):</b> {@link DdlBootstrap} must never
 *       drop or recreate an existing table — schema reconciliation belongs to
 *       the offline DDL gate. Guarded by scanning the class source for
 *       {@code dropTable} usage outside javadoc.</li>
 *   <li><b>Owned-table scope (R-006):</b> {@code verifyTables} compares exact
 *       column counts only for the tables ingestion writes; every owned table
 *       must be registered and its in-code schema must agree with the DDL.</li>
 *   <li><b>Registry completeness (R-007):</b> every DDL file in
 *       {@code 01_platform/02_sql/ddl/} must have a registry entry, including
 *       {@code ingestion_quarantine}.</li>
 *   <li><b>Schema freshness (R-008):</b> owned-table schemas must not drift
 *       from the authoritative DDL column counts.</li>
 * </ul>
 */
@DisplayName("DdlBootstrap guards: no-drop, owned-scope, registry completeness")
class DdlBootstrapSchemaAgreementTest {

    /** CWD is the ingestion module dir; DDLs live under code/01_platform/02_sql/ddl. */
    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();

    /** Source of the class under test, for the no-drop source guard. */
    private static final Path CLASS_SOURCE = Path.of(
            "src/main/java/com/trading/ingestion/DdlBootstrap.java").toAbsolutePath();

    @Test
    @DisplayName("ensureTables never drops or recreates an existing table (R-003)")
    void noDropPathExists() throws IOException {
        String src = Files.readString(CLASS_SOURCE, StandardCharsets.UTF_8);
        // dropTable must not appear in any method body (only in javadoc,
        // which is stripped from the executable class).
        assertFalse(src.contains("dropTable"),
                "DdlBootstrap must never call dropTable — schema reconciliation is the "
                        + "offline DDL gate's job, never a runtime bootstrap's");
    }

    @Test
    @DisplayName("verifyTables checks exact columns only for owned tables (R-006)")
    void ownedTablesAreScopedAndRegistered() throws IOException {
        List<String> owned = DdlBootstrap.ownedTables();
        Map<String, TableDescriptor> registry = DdlBootstrap.tableRegistry();
        assertFalse(owned.isEmpty(), "must own at least one table");
        for (String name : owned) {
            assertTrue(registry.containsKey(name),
                    "owned table " + name + " must be registered in ALL_TABLES");
            // Owned tables must have a DDL file on disk.
            String ddlFile = ddlFileFor(name);
            assertTrue(Files.exists(DDL_DIR.resolve(ddlFile)),
                    "missing DDL for owned table " + name + ": " + ddlFile);
        }
    }

    @Test
    @DisplayName("every owned table schema agrees with its DDL column count (R-006/R-008)")
    void ownedSchemasMatchDdlColumnCounts() throws IOException {
        Map<String, TableDescriptor> registry = DdlBootstrap.tableRegistry();
        for (String name : DdlBootstrap.ownedTables()) {
            TableDescriptor td = registry.get(name);
            assertNotNull(td, "registry missing " + name);
            int expectedCols = td.getSchema().getColumns().size();
            List<String> ddlCols = parseColumns(readDdl(ddlFileFor(name)));
            assertEquals(expectedCols, ddlCols.size(),
                    "in-code schema for " + name + " has " + expectedCols
                            + " cols but its DDL declares " + ddlCols.size()
                            + " — in-code schema must match the authoritative DDL");
        }
    }

    @Test
    @DisplayName("every DDL file is registered (R-007: ingestion_quarantine must be present)")
    void everyDdlIsRegistered() throws IOException {
        Map<String, TableDescriptor> registry = DdlBootstrap.tableRegistry();
        List<String> ddlFiles;
        try (var stream = Files.list(DDL_DIR)) {
            ddlFiles = stream
                    .map(p -> p.getFileName().toString())
                    .filter(f -> f.matches("\\d{2}_.*\\.sql"))
                    .sorted()
                    .toList();
        }
        assertFalse(ddlFiles.isEmpty(), "no DDL files found under " + DDL_DIR);
        for (String file : ddlFiles) {
            String table = tableNameFromDdl(file);
            assertTrue(registry.containsKey(table),
                    "registry missing table " + table + " (from " + file + ")");
        }
        assertTrue(registry.containsKey("ingestion_quarantine"),
                "ingestion_quarantine must be in the registry (QuarantineWriter writes it)");
    }

    @Test
    @DisplayName("ensureTables creates only owned tables — never the compute/registry-only tables (A4.4)")
    void ensureTablesScopeIsOwnedTables() throws IOException {
        String src = Files.readString(CLASS_SOURCE, StandardCharsets.UTF_8);
        int start = src.indexOf("public static boolean ensureTables");
        int end = src.indexOf("private static void ensureDatabase");
        assertTrue(start >= 0 && end > start, "could not locate the ensureTables method body");
        String body = src.substring(start, end);
        assertTrue(body.contains("for (String name : OWNED_TABLES)"),
                "ensureTables must iterate OWNED_TABLES only (A4.4) — it must never "
                        + "bootstrap-create registry-only tables like Signal_Candidates");
        assertFalse(body.contains("ALL_TABLES.entrySet()"),
                "ensureTables must not iterate ALL_TABLES (A4.4) — compute tables are "
                        + "provisioned by the offline DDL gate, not by the ingestion bootstrap");
    }

    @Test
    @DisplayName("compute tables are never owned by the ingestion bootstrap (A4.4)")
    void computeTablesAreNotOwned() {
        for (String computeTable : List.of(
                "candle_features",
                "Signal_Candidates", "Signal_Candidates_current",
                "Ranking_Results", "Trade_Decisions",
                "Portfolio_Reservations")) {
            assertFalse(DdlBootstrap.ownedTables().contains(computeTable),
                    computeTable + " is a compute-owned table — DdlBootstrap must not own it "
                            + "(A4.4, CANDLE-KV-REPLAY-001 P4)");
        }
    }

    @Test
    @DisplayName("candle_features is registry-only and matches DDL 35 (Wave B/DEC-059)")
    void candleFeaturesRegistryMatchesDdl() throws IOException {
        TableDescriptor candleFeatures = DdlBootstrap.tableRegistry().get("candle_features");
        assertNotNull(candleFeatures,
                "registry must carry the compute-owned candle_features entry (DDL 35)");
        assertEquals(17, candleFeatures.getSchema().getColumns().size(),
                "candle_features schema must carry candle_closed's 15 columns + features + sealed");
        assertEquals(List.of("instrument_token", "tf", "window_start"),
                candleFeatures.getSchema().getPrimaryKeyColumnNames(),
                "candle_features PK must be exactly (instrument_token, tf, window_start)");
        assertEquals(List.of("instrument_token"), candleFeatures.getBucketKeys(),
                "candle_features must be distributed by instrument_token");
        assertEquals(17, parseColumns(readDdl(ddlFileFor("candle_features"))).size(),
                "in-code schema must match DDL 35's column count");
        assertFalse(DdlBootstrap.ownedTables().contains("candle_features"),
                "candle_features is compute-owned — ensureTables must never create it (A4.4)");
    }

    @Test
    @DisplayName("retired candle/feature entries are gone; candle_features carries the real schema")
    void candleRegistryEntriesUseRealSchemas() {
        for (String retired : List.of("feature_candles_15s", "feature_candles_15s_preview",
                "feature_candles_15s_current", "forming_bar",
                "candle_live", "candle_closed", "feature_values")) {
            assertFalse(DdlBootstrap.tableRegistry().containsKey(retired),
                    "registry must NOT contain " + retired + " — retired by the multi-timeframe "
                            + "cutover (2026-09-05) or the Wave C W-C5a decommission "
                            + "(2026-09-30, DEC-059); candle_features is the live candle table");
        }
        TableDescriptor merged = DdlBootstrap.tableRegistry().get("candle_features");
        assertNotNull(merged, "registry missing candle_features");
        assertEquals(List.of("instrument_token", "tf", "window_start"),
                merged.getSchema().getPrimaryKeyColumnNames(),
                "candle_features PK must be exactly (instrument_token, tf, window_start)");
        assertEquals(List.of("instrument_token"),
                merged.getBucketKeys(),
                "candle_features must be distributed by instrument_token (per-ticker colocation)");
    }

    @Test
    @DisplayName("raw_table_1 bootstrap pins the DDL storage options (arrow+zstd, 3-day retention)")
    void rawTableStorageOptionsMatchTheDdl() {
        TableDescriptor raw = DdlBootstrap.tableRegistry().get("raw_table_1");
        assertNotNull(raw, "registry missing raw_table_1");
        Map<String, String> props = raw.getProperties();
        assertEquals("zstd", props.get("table.log.arrow.compression.type"),
                "bootstrap must pin arrow+zstd — DropRawTable ensure recreates the table "
                        + "from this descriptor (CHG-487)");
        assertEquals(RawTableSchema.LOG_TTL, props.get("table.log.ttl"),
                "bootstrap table.log.ttl must equal RawTableSchema.LOG_TTL (3d)");
        assertEquals(
                RawTableSchema.PARTITION_RETENTION,
                props.get("table.auto-partition.num-retention"),
                "bootstrap num-retention must equal RawTableSchema.PARTITION_RETENTION (3)");
    }

    // ---- helpers ----

    /** Map a table name to its DDL file name (e.g. raw_table_1 → 02_raw_table_1.sql). */
    private static String ddlFileFor(String table) {
        try (var stream = Files.list(DDL_DIR)) {
            return stream
                    .map(p -> p.getFileName().toString())
                    .filter(f -> f.matches("\\d{2}_.*\\.sql"))
                    .filter(f -> {
                        try {
                            return tableNameFromDdl(f).equals(table);
                        } catch (IOException e) {
                            return false;
                        }
                    })
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no DDL file for table " + table));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** The actual CREATE TABLE name declared inside a DDL file (authoritative). */
    private static String tableNameFromDdl(String file) throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(file), StandardCharsets.UTF_8);
        Matcher m = Pattern.compile(
                "(?i)\\bCREATE\\s+TABLE\\s+([A-Za-z_][A-Za-z0-9_]*)").matcher(ddl);
        if (!m.find()) {
            throw new AssertionError("no CREATE TABLE in " + file);
        }
        return m.group(1);
    }

    private static String tableNameFromDdlFile(String file) {
        return file.replaceFirst("^\\d{2}_", "").replaceFirst("\\.sql$", "");
    }

    private static String readDdl(String name) throws IOException {
        Path p = DDL_DIR.resolve(name);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    /** Extract the top-level CREATE TABLE column names in declared order. */
    private static List<String> parseColumns(String ddl) {
        List<String> out = new ArrayList<>();
        // CHG-358: MAP joins the type set (DDL 34 feature_values) and \b keeps
        // a type token from matching a longer word (INT vs INTERVAL).
        Pattern col = Pattern.compile(
                "^\\s*([a-z_][a-z0-9_]*)\\s+"
                        + "(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN|MAP"
                        + "|TIMESTAMP_LTZ|TIMESTAMP|DATE|DECIMAL|VARCHAR|CHAR|TINYINT|SMALLINT|TIME)\\b",
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher m = col.matcher(ddl);
        while (m.find()) {
            out.add(m.group(1).toLowerCase(java.util.Locale.ROOT));
        }
        return out;
    }
}
