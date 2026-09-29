package com.trading.common.schema;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Signal DDL contract guards (DEC-035, tracker 14 re-scoped P2 —
 * SIGNAL-SCHEMA-001): the immutable signal LOG
 * {@code 05_signal_candidates.sql} and its KV current-state companion
 * {@code 23_signal_candidates_current.sql} must agree on the frozen
 * 22-column layout and stay routing-compatible.
 *
 * <ul>
 *   <li>The LOG keeps its append-only nature: it MUST NOT gain a primary key
 *       (a PK would flip it to KV and destroy the audit trail).</li>
 *   <li>The KV table MUST declare exactly {@code PRIMARY KEY
 *       (instrument_token) NOT ENFORCED} and keep
 *       {@code bucket.key=instrument_token} — a subset of the PK, so every
 *       signal of a ticker lands in the same bucket as its LOG twin
 *       (colocation contract, DEC-035).</li>
 *   <li>Both tables share the identical 22-column layout and types, 16
 *       buckets, and 7-day LOG retention.</li>
 * </ul>
 *
 * <p>The 22-column list is hardcoded here on purpose: the compute module's
 * {@code SignalCandidatesTableColumns} is not visible from the common module,
 * and a DDL-vs-DDL pin test must not depend on the consumer module it guards.
 * Ingestion's {@code SchemaAgreementTest} pins the same DDLs against the Java
 * row layouts from the other direction.
 */
@DisplayName("Signal DDL contract: LOG stays LOG, KV carries the exact PK, columns match")
class SignalCurrentDdlContractTest {

    /** CWD is the common module dir; DDLs live under code/01_platform/02_sql/ddl. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();

    /** Frozen 22-column layout shared by DDL 05 (v3) and DDL 23, in DDL order. */
    private static final List<String> COLUMNS = List.of(
            "candidate_id", "instruction_id", "trade_context_id", "instrument_token",
            "exchange", "symbol", "strategy_id", "strategy_version", "rule_id",
            "detection_ts", "evaluation_ts", "action", "side", "quantity",
            "order_type", "limit_price_paise", "score_inputs",
            "formation_snapshot_ref", "validity_reason", "supersedes_candidate_id",
            "superseded_by_candidate_id", "schema_version");

    private static final List<String> KEY_COLUMNS = List.of("instrument_token");

    private static final Pattern CREATE_TABLE = Pattern.compile(
            "(?i)\\bCREATE\\s+TABLE\\s+([A-Za-z_][A-Za-z0-9_]*)");
    private static final Pattern PRIMARY_KEY = Pattern.compile(
            "PRIMARY\\s+KEY\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern BUCKET_KEY = Pattern.compile(
            "'bucket\\.key'\\s*=\\s*'([^']*)'");
    private static final Pattern BUCKET_NUM = Pattern.compile(
            "'bucket\\.num'\\s*=\\s*'([^']*)'");
    private static final Pattern LOG_TTL = Pattern.compile(
            "'table\\.log\\.ttl'\\s*=\\s*'([^']*)'");
    /** Top-level column lines: name + type is always the line's first two tokens. */
    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)\\b",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    /** M5-1: the row contract version in the DDL header. */
    private static final Pattern SCHEMA_VERSION_HEADER = Pattern.compile(
            "^--\\s*Schema version:\\s*(\\d+)", Pattern.MULTILINE);
    /** M5-1: the domain line naming what the writer sets. */
    private static final Pattern SCHEMA_VERSION_WRITER = Pattern.compile(
            "schema_version writer-set '(\\d+)'");

    @Test
    @DisplayName("M5-1: both signal DDLs pin row schema_version 2 (header + writer-set line)")
    void bothDdlsPinRowSchemaVersionTwo() throws IOException {
        String log = readDdl("05_signal_candidates.sql");
        String kv = readDdl("23_signal_candidates_current.sql");
        assertEquals("2", schemaVersionHeader(log),
                "the LOG header must carry the row contract version the writer sets "
                        + "(the v2/v3 notes below it are table-kind history)");
        assertEquals("2", schemaVersionHeader(kv),
                "the KV header must carry the same row contract version as the LOG twin");
        assertEquals("2", schemaVersionWriter(log),
                "the LOG domain line must say schema_version writer-set '2'");
        assertEquals("2", schemaVersionWriter(kv),
                "the KV domain line must say schema_version writer-set '2'");
    }

    @Test
    @DisplayName("LOG DDL stays LOG: no primary key, instrument_token bucket, 22 shared columns")
    void logDdlRemainsImmutableLog() throws IOException {
        String ddl = readDdl("05_signal_candidates.sql");
        assertEquals("Signal_Candidates", tableName(ddl));
        assertFalse(hasPrimaryKey(ddl),
                "Signal_Candidates is the immutable audit trail — it must never gain a PRIMARY KEY "
                        + "(a PK flips the table to KV and breaks append-only evidence)");
        assertSharedContract(ddl, "Signal_Candidates");
    }

    @Test
    @DisplayName("KV DDL declares the exact (instrument_token) PK and keeps the shared column layout")
    void kvDdlCarriesExactPrimaryKey() throws IOException {
        String ddl = readDdl("23_signal_candidates_current.sql");
        assertEquals("Signal_Candidates_current", tableName(ddl));

        assertTrue(hasPrimaryKey(ddl),
                "Signal_Candidates_current must be a KV table (PRIMARY KEY present)");
        List<String> pk = primaryKeyColumns(ddl);
        assertEquals(KEY_COLUMNS, pk,
                "KV primary key must be exactly " + KEY_COLUMNS + " (per-instrument current state)");

        assertSharedContract(ddl, "Signal_Candidates_current");
    }

    @Test
    @DisplayName("both signal DDLs agree column-for-column: 22 columns, identical types, routing, retention")
    void bothDdlsAgreeWithSharedLayout() throws IOException {
        String log = readDdl("05_signal_candidates.sql");
        String kv = readDdl("23_signal_candidates_current.sql");

        assertEquals(COLUMNS, parseColumns(log),
                "LOG DDL column order must match the frozen 22-column contract");
        assertEquals(COLUMNS, parseColumns(kv),
                "KV DDL column order must match the LOG twin column-for-column");

        assertEquals(parseTypes(log), parseTypes(kv),
                "LOG and KV DDLs must declare identical column types");

        assertEquals("16", bucketNum(log), "LOG bucket.num must stay 16");
        assertEquals("16", bucketNum(kv), "KV bucket.num must stay 16");
        assertEquals("7d", logTtl(log), "LOG retention must stay 7d");
        assertEquals("7d", logTtl(kv), "KV retention must stay 7d");
    }

    // ---- shared column-layout assertions ----

    private static void assertSharedContract(String ddl, String table) {
        List<String> cols = parseColumns(ddl);
        assertEquals(COLUMNS.size(), cols.size(),
                table + " DDL must declare " + COLUMNS.size() + " columns");
        assertEquals(COLUMNS, cols,
                table + " DDL column order must match the frozen 22-column contract");
        String bucket = bucketKey(ddl);
        assertNotNull(bucket, table + " must declare bucket.key");
        assertEquals("instrument_token", bucket,
                table + " must route by instrument_token (colocation contract)");
    }

    // ---- DDL parsing helpers ----

    private static String readDdl(String name) throws IOException {
        Path p = DDL_DIR.resolve(name);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private static String tableName(String ddl) {
        Matcher m = CREATE_TABLE.matcher(ddl);
        assertTrue(m.find(), "no CREATE TABLE in DDL");
        return m.group(1);
    }

    private static boolean hasPrimaryKey(String ddl) {
        return PRIMARY_KEY.matcher(ddl).find();
    }

    private static List<String> primaryKeyColumns(String ddl) {
        Matcher m = PRIMARY_KEY.matcher(ddl);
        assertTrue(m.find(), "no PRIMARY KEY in DDL");
        List<String> out = new ArrayList<>();
        for (String col : m.group(1).split(",")) {
            out.add(col.trim().toLowerCase(java.util.Locale.ROOT));
        }
        return out;
    }

    private static String bucketKey(String ddl) {
        Matcher m = BUCKET_KEY.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    private static String bucketNum(String ddl) {
        Matcher m = BUCKET_NUM.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    private static String logTtl(String ddl) {
        Matcher m = LOG_TTL.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    private static String schemaVersionHeader(String ddl) {
        Matcher m = SCHEMA_VERSION_HEADER.matcher(ddl);
        assertTrue(m.find(), "DDL must declare '-- Schema version: <n>'");
        return m.group(1);
    }

    private static String schemaVersionWriter(String ddl) {
        Matcher m = SCHEMA_VERSION_WRITER.matcher(ddl);
        assertTrue(m.find(), "DDL must declare schema_version writer-set '<n>'");
        return m.group(1);
    }

    /** Top-level column names in declared order. */
    private static List<String> parseColumns(String ddl) {
        List<String> names = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            names.add(m.group(1).toLowerCase(java.util.Locale.ROOT));
        }
        return names;
    }

    private static List<String> parseTypes(String ddl) {
        List<String> types = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            types.add(m.group(2).toUpperCase(java.util.Locale.ROOT));
        }
        return types;
    }
}
