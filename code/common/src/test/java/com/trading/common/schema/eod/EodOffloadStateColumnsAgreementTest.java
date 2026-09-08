package com.trading.common.schema.eod;

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
 * SCH-23 cross-boundary pin (DDL → Java column layout, the repo's
 * cross-boundary pin habit): {@link EodOffloadStateColumns} must stay a
 * faithful mirror of {@code code/01_platform/02_sql/ddl/26_eod_offload_state.sql}
 * v1 — 17 columns, names in DDL order, per-column type roots, and per-column
 * nullability — so a DDL rename/reorder/retype or a one-sided code edit fails
 * the suite instead of silently mis-serializing controller state.
 *
 * <p>Also pins the KV contract the raw-client store relies on (SCH-23,
 * COMPAT-FLUSS-005): primary key exactly {@code [record_id]} and
 * {@code record_id} routing — the single-field PK that keeps the plain-JVM
 * controller raw-client writable.
 */
class EodOffloadStateColumnsAgreementTest {

    /** CWD is the common module dir (code/common); DDLs live one level up under code/01_platform. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "26_eod_offload_state.sql";

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
                    type.equals("INT") ? "INTEGER" : type, // DDL INT == Fluss root INTEGER
                    !notNull));
        }
        return out;
    }

    private static String readDdl() throws IOException {
        return Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
    }

    @Test
    void ddlDeclares17ColumnsInPinnedOrder() throws IOException {
        List<Column> cols = parseColumns();
        assertThat(cols).hasSize(EodOffloadStateColumns.FIELD_COUNT);
        assertThat(cols.stream().map(Column::name).toList())
                .containsExactlyElementsOf(EodOffloadStateColumns.NAMES);
    }

    @Test
    void ddlTypesMatchTypeRootsPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertThat(cols.get(i).type())
                    .as("column %d (%s) type root", i, cols.get(i).name())
                    .isEqualTo(EodOffloadStateColumns.TYPE_ROOTS.get(i));
        }
    }

    @Test
    void ddlNullabilityMatchesAllNotNull() throws IOException {
        List<Column> cols = parseColumns();
        assertThat(cols.stream().map(Column::nullableInDdl).toList())
                .containsExactlyElementsOf(EodOffloadStateColumns.COLUMN_NULLABLE_IN_DDL);
        assertThat(cols).noneMatch(Column::nullableInDdl);
    }

    @Test
    void ddlIsSingleFieldPkKvWithRecordIdRouting() throws IOException {
        String ddl = readDdl();
        assertThat(ddl).contains("PRIMARY KEY (record_id)")
                .as("the offload-state table must be KV with PK exactly [record_id] "
                        + "(raw-client writable — COMPAT-FLUSS-005)");
        assertThat(ddl).contains("'bucket.key' = 'record_id'");
        assertThat(ddl).contains("'bucket.num' = '16'");
    }

    @Test
    void recordIdIsTradingDatePipeTableName() {
        assertThat(EodOffloadStateColumns.recordId("2026-08-14", "feature_candles_15s"))
                .isEqualTo("2026-08-14|feature_candles_15s");
        assertThat(EodOffloadStateColumns.LEASE_RECORD_ID).isEqualTo("lease|controller");
    }

    @Test
    void recordIdRejectsPipeCollisionAndLeaseMasquerade() {
        try {
            EodOffloadStateColumns.recordId("2026-08-14", "a|b");
            throw new AssertionError("expected IllegalArgumentException for embedded pipe");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains("'|'");
        }
        try {
            EodOffloadStateColumns.recordId("lease", "controller");
            throw new AssertionError("expected IllegalStateException-free rejection of lease collision");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains("lease");
        }
    }
}
