package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * SCH-20 cross-boundary pin: {@link PositionsColumns} must stay a faithful
 * mirror of {@code code/01_platform/02_sql/ddl/10_positions.sql} v2 — 17
 * columns, names in DDL order, type roots, nullability — plus the KV key
 * contract the projector relies on: PK exactly {@code [position_id]}, routing
 * on {@code position_id}, and NO derived {@code current_quantity} column
 * (v2 removed it so no write path can corrupt one side of the quantity pair).
 */
class PositionsColumnsAgreementTest {

    /** CWD is the common module dir (code/common); DDLs live one level up. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "10_positions.sql";

    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)(.*)$",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private record Column(String name, String type, boolean nullableInDdl) {}

    private static List<Column> parseColumns() throws IOException {
        Path p = DDL_DIR.resolve(DDL_FILE);
        assertThat(p).as("missing DDL file %s", p).exists();
        String ddl = Files.readString(p, StandardCharsets.UTF_8);
        List<Column> out = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            boolean notNull = m.group(3).toUpperCase(Locale.ROOT).contains("NOT NULL");
            String type = m.group(2).toUpperCase(Locale.ROOT);
            out.add(new Column(m.group(1).toLowerCase(Locale.ROOT),
                    type.equals("INT") ? "INTEGER" : type,
                    !notNull));
        }
        return out;
    }

    @Test
    void ddlDeclares17ColumnsInPinnedOrder() throws IOException {
        List<Column> cols = parseColumns();
        assertThat(cols).hasSize(PositionsColumns.FIELD_COUNT);
        assertThat(cols.stream().map(Column::name).toList())
                .containsExactlyElementsOf(PositionsColumns.NAMES);
    }

    @Test
    void ddlTypesMatchTypeRootsPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertThat(cols.get(i).type())
                    .as("column %d (%s) type root", i, cols.get(i).name())
                    .isEqualTo(PositionsColumns.TYPE_ROOTS.get(i));
        }
    }

    @Test
    void ddlNullabilityMatchesPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertThat(cols.get(i).nullableInDdl())
                    .as("column %d (%s) nullability", i, cols.get(i).name())
                    .isEqualTo(PositionsColumns.COLUMN_NULLABLE_IN_DDL.get(i));
        }
    }

    @Test
    void kvContractSingleFieldPkOnPositionId() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        assertThat(ddl).containsIgnoringCase("PRIMARY KEY (position_id) NOT ENFORCED");
        assertThat(ddl).containsIgnoringCase("'bucket.key' = 'position_id'");
        List<Column> cols = parseColumns();
        assertThat(cols.stream().map(Column::name))
                .as("no derived current_quantity column in v2")
                .doesNotContain("current_quantity");
    }

    @Test
    void schemaVersionHeaderIsV2() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        assertThat(ddl).containsIgnoringCase("Schema version: 2");
        assertThat(PositionsColumns.SCHEMA_VERSION_V2).isEqualTo("2");
    }
}
