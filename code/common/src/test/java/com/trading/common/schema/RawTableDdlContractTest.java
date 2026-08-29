package com.trading.common.schema;

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
 * Raw-table DDL contract guard (configuration-driven plan SC1, 2026-08-29):
 * {@link RawTableSchema} — the single source of truth for the raw tick row —
 * must match {@code 02_raw_table_1.sql} exactly: column names in DDL order,
 * types, bucket routing, and the 20-column count. Any drift fails here
 * before a deploy can ship a mismatched writer.
 */
@DisplayName("Raw table DDL contract: RawTableSchema matches 02_raw_table_1.sql exactly")
class RawTableDdlContractTest {

    /** CWD is the common module dir; DDLs live under code/01_platform/02_sql/ddl. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();

    private static final Pattern CREATE_TABLE = Pattern.compile(
            "(?i)\\bCREATE\\s+TABLE\\s+([A-Za-z_][A-Za-z0-9_]*)");
    private static final Pattern BUCKET_KEY = Pattern.compile(
            "'bucket\\.key'\\s*=\\s*'([^']*)'");
    private static final Pattern BUCKET_NUM = Pattern.compile(
            "'bucket\\.num'\\s*=\\s*'([^']*)'");
    /** Top-level column lines: name + type is always the line's first two tokens. */
    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)\\b",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    @Test
    @DisplayName("RawTableSchema columns match the DDL column-for-column in order")
    void schemaMatchesDdlColumns() throws IOException {
        String ddl = readDdl("02_raw_table_1.sql");
        List<String> cols = parseColumns(ddl);
        assertEquals(RawTableSchema.COLUMNS.size(), cols.size(),
                "DDL must declare " + RawTableSchema.COLUMNS.size() + " columns");
        assertEquals(RawTableSchema.COLUMNS, cols,
                "DDL column order must match RawTableSchema exactly");
    }

    @Test
    @DisplayName("RawTableSchema types match the DDL type-for-type in order")
    void schemaMatchesDdlTypes() throws IOException {
        String ddl = readDdl("02_raw_table_1.sql");
        List<String> types = parseTypes(ddl);
        assertEquals(RawTableSchema.COLUMN_TYPE_ROOTS.size(), types.size(),
                "DDL must declare " + RawTableSchema.COLUMN_TYPE_ROOTS.size() + " types");
        assertEquals(RawTableSchema.COLUMN_TYPE_ROOTS, types,
                "DDL types must match RawTableSchema exactly");
    }

    @Test
    @DisplayName("raw_table_1 routes by instrument_token with 16 buckets (DDL contract)")
    void schemaMatchesRouting() throws IOException {
        String ddl = readDdl("02_raw_table_1.sql");
        assertEquals("raw_table_1", tableName(ddl));
        assertEquals("instrument_token", bucketKey(ddl),
                "raw_table_1 must route by instrument_token");
        assertEquals("16", bucketNum(ddl), "raw_table_1 must use 16 buckets");
        assertNotNull(RawTableSchema.COLUMNS);
    }

    @Test
    @DisplayName("RawTableSchema FIELD_COUNT equals COLUMNS size")
    void fieldCountMatchesColumns() {
        assertEquals(RawTableSchema.COLUMNS.size(), RawTableSchema.FIELD_COUNT);
        assertEquals(RawTableSchema.COLUMN_TYPE_ROOTS.size(), RawTableSchema.FIELD_COUNT);
    }

    // ---- DDL parsing helpers (same approach as SignalCurrentDdlContractTest) ----

    private static String readDdl(String name) throws IOException {
        Path p = DDL_DIR.resolve(name);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private static String tableName(String ddl) {
        Matcher m = CREATE_TABLE.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    private static List<String> parseColumns(String ddl) {
        List<String> cols = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            cols.add(m.group(1));
        }
        return cols;
    }

    private static List<String> parseTypes(String ddl) {
        List<String> types = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            types.add(m.group(2).toUpperCase());
        }
        return types;
    }

    private static String bucketKey(String ddl) {
        Matcher m = BUCKET_KEY.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    private static String bucketNum(String ddl) {
        Matcher m = BUCKET_NUM.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }
}
