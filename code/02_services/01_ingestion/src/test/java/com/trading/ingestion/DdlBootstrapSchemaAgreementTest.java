package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                "candle_live", "candle_closed",
                "Signal_Candidates", "Signal_Candidates_current",
                "Ranking_Results", "Trade_Decisions",
                "Portfolio_Reservations")) {
            assertFalse(DdlBootstrap.ownedTables().contains(computeTable),
                    computeTable + " is a compute-owned table — DdlBootstrap must not own it "
                            + "(A4.4, CANDLE-KV-REPLAY-001 P4)");
        }
    }

    @Test
    @DisplayName("retired candle/forming-bar entries are gone; candle_live carries the real schema")
    void candleRegistryEntriesUseRealSchemas() {
        for (String retired : List.of("feature_candles_15s", "feature_candles_15s_preview",
                "feature_candles_15s_current", "forming_bar")) {
            assertFalse(DdlBootstrap.tableRegistry().containsKey(retired),
                    "registry must NOT contain " + retired + " — retired by the multi-timeframe "
                            + "cutover (2026-09-05); candle_live/candle_closed are the live tables");
        }
        TableDescriptor live = DdlBootstrap.tableRegistry().get("candle_live");
        assertNotNull(live, "registry missing candle_live");
        assertEquals(List.of("instrument_token", "tf", "window_start"),
                live.getSchema().getPrimaryKeyColumnNames(),
                "candle_live PK must be exactly (instrument_token, tf, window_start)");
        assertEquals(List.of("instrument_token"),
                live.getBucketKeys(),
                "candle_live must be distributed by instrument_token (per-ticker colocation)");
        assertNotNull(DdlBootstrap.tableRegistry().get("candle_closed"),
                "registry missing candle_closed");
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
        Pattern col = Pattern.compile(
                "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)",
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher m = col.matcher(ddl);
        while (m.find()) {
            out.add(m.group(1).toLowerCase(java.util.Locale.ROOT));
        }
        return out;
    }
}
