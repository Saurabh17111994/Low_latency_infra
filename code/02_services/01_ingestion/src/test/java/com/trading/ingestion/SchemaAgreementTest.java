package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-SCHEMA-001: managed writers append exactly the columns declared in their
 * offline DDL files (plan §Database: raw_table_1, suspected_discontinuities,
 * ingestion_quarantine must match source DDL column order and count).
 *
 * <p>This parses the DDL SQL files under code/01_platform/02_sql/ddl/ and
 * verifies the column name sequences match what the runtime schema descriptors
 * (DdlBootstrap) expect — the writers append positionally against those.
 */
@DisplayName("ING-SCHEMA-001: writer columns agree with source DDL")
class SchemaAgreementTest {

    /** CWD is the ingestion module dir; DDLs live one level up under code/01_platform. */
    private static final Path DDL_DIR = Path.of(
            "../../01_platform/02_sql/ddl"
    ).toAbsolutePath();

    /** DDL file name → expected first column (bucket key) per the plan. */
    private static final String[][] MANAGED_TABLES = {
            {"19_suspected_discontinuities.sql", "discontinuity_id"},
            {"21_ingestion_quarantine.sql", "quarantine_id"},
    };

    @Test
    @DisplayName("discontinuity DDL declares the 11-column schema in plan order")
    void discontinuityDdlMatchesPlan() throws IOException {
        List<String> cols = parseColumns(readDdl("19_suspected_discontinuities.sql"));
        assertEquals(11, cols.size(), "suspected_discontinuities must have 11 columns");
        String[] want = {
                "discontinuity_id", "source", "reason", "connection_epoch",
                "last_tick_ts", "last_tick_fingerprint", "last_tick_token",
                "last_tick_exchange", "last_tick_symbol", "detected_ts",
                "schema_version"};
        for (int i = 0; i < want.length; i++) {
            assertEquals(want[i], cols.get(i), "column " + i);
        }
    }

    @Test
    @DisplayName("ingestion_quarantine DDL declares the 10-column schema in plan order")
    void quarantineDdlMatchesPlan() throws IOException {
        List<String> cols = parseColumns(readDdl("21_ingestion_quarantine.sql"));
        assertEquals(10, cols.size(), "ingestion_quarantine must have 10 columns");
        String[] want = {
                "quarantine_id", "reason", "instrument_token", "exchange",
                "symbol", "raw_payload", "payload_hash", "detected_ts",
                "detail", "schema_version"};
        for (int i = 0; i < want.length; i++) {
            assertEquals(want[i], cols.get(i), "column " + i);
        }
    }

    @Test
    @DisplayName("every managed DDL exists and declares its bucket key first")
    void managedDdlsExist() throws IOException {
        for (String[] table : MANAGED_TABLES) {
            Path p = DDL_DIR.resolve(table[0]);
            assertTrue(Files.exists(p), "missing DDL: " + p);
            List<String> cols = parseColumns(Files.readString(p, StandardCharsets.UTF_8));
            assertNotNull(cols);
            assertTrue(!cols.isEmpty(), "no columns parsed from " + table[0]);
            assertEquals(table[1], cols.get(0), "bucket key must be first column in " + table[0]);
        }
    }

    @Test
    @DisplayName("raw_table_1 DDL declares ack_ts as nullable (R-010)")
    void rawTableAckTsIsNullable() throws IOException {
        String ddl = readDdl("02_raw_table_1.sql");
        // R-010: ack_ts is not known at row-build time on an immutable LOG
        // table — it must be nullable with 0 meaning 'unknown', never NOT NULL.
        String ackLine = ddl.lines()
                .filter(l -> l.trim().startsWith("ack_ts"))
                .findFirst().orElse("<missing ack_ts column>");
        assertTrue(ackLine.contains("NULL"),
                "ack_ts must be declared nullable: " + ackLine);
        assertTrue(!ackLine.contains("NOT NULL"),
                "ack_ts must not be NOT NULL: " + ackLine);
    }

    // ---- Phase 6 guards (G5): DDLs tell the truth ----------------

    @Test
    @DisplayName("raw_table_1 v3 declares exactly the 21 written columns (R-054/R-231; v3 event_day first, CHG-117)")
    void rawTableV3ColumnCount() throws IOException {
        List<String> cols = parseColumns(readDdl("02_raw_table_1.sql"));
        String[] want = {
                "event_day",
                "event_fingerprint", "fingerprint_version", "connection_id",
                "connection_epoch", "instrument_token", "exchange", "symbol",
                "event_time", "ingest_ts", "ack_ts", "tick_type",
                "last_price_paise", "last_qty", "raw_payload", "payload_hash",
                "decoder_version", "protocol_version", "validity_state",
                "validity_reason", "schema_version"};
        assertEquals(want.length, cols.size(),
                "raw_table_1 must have " + want.length + " columns (R-054/R-231 removed the never-populated quote and option fields; v3 added event_day first — CHG-117); got: " + cols);
        for (int i = 0; i < want.length; i++) {
            assertEquals(want[i], cols.get(i), "column " + i);
        }
    }

    @Test
    @DisplayName("no DDL uses the non-Fluss table.retention.days option (R-011/R-055/R-087/R-088)")
    void noRetentionDaysOption() throws IOException {
        for (Path p : ddlFiles()) {
            String ddl = Files.readString(p, StandardCharsets.UTF_8);
            // Check only the WITH-clause region — comments may legitimately
            // reference the old option while documenting the fix.
            int with = ddl.indexOf(") WITH");
            String clause = with >= 0 ? ddl.substring(with) : "";
            assertTrue(!clause.contains("table.retention.days"),
                    p.getFileName() + " WITH clause still uses table.retention.days; use table.log.ttl");
        }
    }

    @Test
    @DisplayName("every Lake-claiming DDL carries datalake options (R-168/R-146/R-011)")
    void lakeHeadersCarryDatalakeOptions() throws IOException {
        for (Path p : ddlFiles()) {
            String ddl = Files.readString(p, StandardCharsets.UTF_8);
            if (ddl.contains("Lake: none")) {
                continue;
            }
            if (ddl.contains("Lake:")) {
                assertTrue(ddl.contains("table.datalake.enabled"),
                        p.getFileName() + " header claims lake storage but the WITH clause drops datalake options");
                assertTrue(ddl.contains("table.datalake.format"),
                        p.getFileName() + " missing table.datalake.format");
            }
        }
    }

    @Test
    @DisplayName("no typo'd column identifiers survive (R-235)")
    void noTypoColumns() throws IOException {
        for (Path p : ddlFiles()) {
            String ddl = Files.readString(p, StandardCharsets.UTF_8);
            // Check only column-definition lines (name TYPE ...), so a comment
            // documenting a rename can mention the old spelling.
            for (String line : ddl.split("\n")) {
                if (line.matches("(?s)^\\s*[a-z_][a-z0-9_]*\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|BOOLEAN)\\b.*")) {
                    assertTrue(!line.contains("completeted_ts"),
                            p.getFileName() + " still contains the completeted_ts typo: " + line.trim());
                }
            }
        }
    }

    @Test
    @DisplayName("Order_Lifecycle is account-scoped with a composite PK (R-013)")
    void orderLifecycleAccountScoped() throws IOException {
        String ddl = readDdl("09_order_lifecycle.sql");
        assertTrue(ddl.contains("account_scope_id"),
                "Order_Lifecycle must materialize account_scope_id (R-013)");
        assertTrue(ddl.contains("PRIMARY KEY (account_scope_id, broker_order_id)"),
                "Order_Lifecycle PK must be (account_scope_id, broker_order_id)");
        assertTrue(ddl.contains("'bucket.key' = 'account_scope_id'"),
                "Order_Lifecycle bucket key must be the raw-client-compatible account subset");
        assertTrue(ddl.contains("'table.kv.format-version' = '2'"),
                "Order_Lifecycle must use the raw-client-compatible KV encoding");
    }

    @Test
    @DisplayName("Signal_Candidates LOG v3 has no PK; Signal_Candidates_current KV keys on instrument_token (DEC-035)")
    void signalTablesFollowReScope() throws IOException {
        String candidates = readDdl("05_signal_candidates.sql");
        assertTrue(!candidates.contains("PRIMARY KEY"),
                "Signal_Candidates is append-only LOG v3 (DEC-035) — it must not declare a primary key");
        assertTrue(candidates.contains("'bucket.key' = 'instrument_token'"),
                "Signal_Candidates LOG must route by instrument_token (colocates with the KV current-state twin)");
        String current = readDdl("23_signal_candidates_current.sql");
        assertTrue(current.contains("PRIMARY KEY (instrument_token)"),
                "Signal_Candidates_current must be KV keyed on instrument_token so supersessions upsert the same key (DEC-035)");
        String halts = readDdl("18_safety_halt_requests.sql");
        assertTrue(halts.contains("PRIMARY KEY (halt_request_id)"),
                "Safety_Halt_Requests must be KV keyed on halt_request_id so duplicate deliveries are no-ops (R-089)");
    }

    @Test
    @DisplayName("Fills materializes account_scope_id and covers the Order_Lifecycle window (R-085/R-144)")
    void fillsAccountScopedAndCoversRebuild() throws IOException {
        String fills = readDdl("08_fills.sql");
        assertTrue(fills.contains("account_scope_id"),
                "Fills must materialize account_scope_id (R-085)");
        // R-144: Fills is the rebuild source for Order_Lifecycle (7d) — its
        // ttl must cover that window, not a narrower 3d.
        assertTrue(fills.contains("'table.log.ttl' = '7d'"),
                "Fills ttl must cover the Order_Lifecycle rebuild window (R-144); got: "
                        + withClause(fills));
    }

    private static String withClause(String ddl) {
        int i = ddl.indexOf(") WITH");
        return i >= 0 ? ddl.substring(i) : "<no WITH clause>";
    }

    // ---- helpers ----

    private static List<Path> ddlFiles() throws IOException {
        List<Path> out = new ArrayList<>();
        try (var stream = Files.list(DDL_DIR)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".sql"))
                    .sorted()
                    .forEach(out::add);
        }
        return out;
    }

    private static String readDdl(String name) throws IOException {
        Path p = DDL_DIR.resolve(name);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    /** Extract the top-level CREATE TABLE column names in declared order. */
    private static List<String> parseColumns(String ddl) {
        List<String> out = new ArrayList<>();
        // Columns are the lines between "( " and ") WITH" that start with a name.
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
