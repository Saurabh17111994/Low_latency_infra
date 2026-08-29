package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.RawTableSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Schema parity guard (configuration-driven plan SC3, 2026-08-29): the three
 * representations of the raw-table layout must agree —
 * {@link RawTableSchema} (single source), the DDL
 * ({@code 02_raw_table_1.sql}), and the {@link DdlBootstrap} table registry
 * (what the runtime bootstrap actually creates). Drift fails here before a
 * deploy ships a mismatched writer.
 */
@DisplayName("SC3: RawTableSchema ↔ DDL ↔ DdlBootstrap schema parity")
class RawTableSchemaParityTest {

    /** CWD is the ingestion module dir; DDLs live under code/01_platform/02_sql/ddl. */
    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();

    /** Top-level column lines: name + type is always the line's first two tokens. */
    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)\\b",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    @Test
    @DisplayName("RawTableSchema columns == DDL 02_raw_table_1.sql columns (name + order)")
    void schemaMatchesDdlColumns() throws IOException {
        String ddl = readDdl("02_raw_table_1.sql");
        List<String> cols = parseColumns(ddl);
        assertEquals(RawTableSchema.COLUMNS.size(), cols.size(),
                "DDL must declare " + RawTableSchema.COLUMNS.size() + " columns");
        assertEquals(RawTableSchema.COLUMNS, cols,
                "DDL column order must match RawTableSchema exactly");
    }

    @Test
    @DisplayName("RawTableSchema types == DDL 02_raw_table_1.sql types (type + order)")
    void schemaMatchesDdlTypes() throws IOException {
        String ddl = readDdl("02_raw_table_1.sql");
        List<String> types = parseTypes(ddl);
        assertEquals(RawTableSchema.COLUMN_TYPE_ROOTS.size(), types.size(),
                "DDL must declare " + RawTableSchema.COLUMN_TYPE_ROOTS.size() + " types");
        assertEquals(RawTableSchema.COLUMN_TYPE_ROOTS, types,
                "DDL types must match RawTableSchema exactly");
    }

    @Test
    @DisplayName("DdlBootstrap created schema == RawTableSchema (name + order + count)")
    void bootstrapSchemaMatchesSingleSource() {
        TableDescriptor td = DdlBootstrap.tableRegistry().get(RawTableSchema.TABLE);
        assertTrue(td != null, "DdlBootstrap must register " + RawTableSchema.TABLE);

        List<Schema.Column> columns = td.getSchema().getColumns();
        assertEquals(RawTableSchema.FIELD_COUNT, columns.size(),
                "DdlBootstrap schema must have " + RawTableSchema.FIELD_COUNT + " columns");

        for (int i = 0; i < columns.size(); i++) {
            assertEquals(RawTableSchema.COLUMNS.get(i), columns.get(i).getName(),
                    "DdlBootstrap column #" + i + " name must match RawTableSchema");
        }
    }

    // ---- DDL parsing helpers ----

    private static String readDdl(String name) throws IOException {
        Path p = DDL_DIR.resolve(name);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
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
}
