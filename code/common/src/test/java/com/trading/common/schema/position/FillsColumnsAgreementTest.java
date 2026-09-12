package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * SCH-20 cross-boundary pin: {@link FillsColumns} must stay a faithful mirror
 * of {@code code/01_platform/02_sql/ddl/08_fills.sql} v2 — 23 columns, names
 * in DDL order, per-column type roots, per-column nullability — so a DDL
 * rename/reorder/retype or a one-sided code edit fails the suite.
 */
class FillsColumnsAgreementTest {

    /** CWD is the common module dir (code/common); DDLs live one level up. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "08_fills.sql";

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
    void ddlDeclares23ColumnsInPinnedOrder() throws IOException {
        List<Column> cols = parseColumns();
        assertThat(cols).hasSize(FillsColumns.FIELD_COUNT);
        assertThat(cols.stream().map(Column::name).toList())
                .containsExactlyElementsOf(FillsColumns.NAMES);
    }

    @Test
    void namesIsImmutableAndConsistentWithFieldCount() {
        // P3-163: NAMES was a mutable String[] — `final` bound only the reference, so any caller
        // could write through it (`NAMES[0] = "x"`, `Arrays.sort(NAMES)`) and corrupt this shared
        // agreement pin JVM-wide. It is now an immutable List.
        assertThatThrownBy(() -> FillsColumns.NAMES.set(0, "corrupted"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> FillsColumns.NAMES.add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);
        // The class-load guard (P3-391) asserts this too; this pins the invariant from the test
        // side. The guard itself cannot be unit-tested without provoking drift at load time.
        assertThat(FillsColumns.NAMES).hasSize(FillsColumns.FIELD_COUNT);
        assertThat(FillsColumns.NAMES.get(FillsColumns.FILL_QTY)).isEqualTo("fill_qty");
    }

    @Test
    void ddlTypesMatchTypeRootsPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertThat(cols.get(i).type())
                    .as("column %d (%s) type root", i, cols.get(i).name())
                    .isEqualTo(FillsColumns.TYPE_ROOTS.get(i));
        }
    }

    @Test
    void ddlNullabilityMatchesPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertThat(cols.get(i).nullableInDdl())
                    .as("column %d (%s) nullability", i, cols.get(i).name())
                    .isEqualTo(FillsColumns.COLUMN_NULLABLE_IN_DDL.get(i));
        }
    }

    @Test
    void schemaVersionHeaderIsV2() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        assertThat(ddl).containsIgnoringCase("Schema version: 2");
        assertThat(FillsColumns.SCHEMA_VERSION_V2).isEqualTo("2");
    }
}
