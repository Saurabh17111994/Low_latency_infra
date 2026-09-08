package com.trading.common.schema.ownership;

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
 * SCH-15 cross-boundary pin: {@link ExecutionAttemptsColumns} must stay a
 * faithful mirror of {@code code/01_platform/02_sql/ddl/12_execution_attempts.sql}
 * v3 — 20 columns, names in DDL order, per-column type roots, per-column
 * nullability — plus the KV contract the ownership matrix relies on: PK
 * exactly {@code [execution_attempt_id]}, routing on the same key, and the
 * v2 monotonic {@code phase_epoch} (R-234 stale-phase protection) and the v3
 * durable {@code gate_fence_token} authorization column (CHG-044, T5).
 */
class ExecutionAttemptsColumnsAgreementTest {

    /** CWD is the common module dir (code/common); DDLs live one level up. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "12_execution_attempts.sql";

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
    void ddlDeclares20ColumnsInPinnedOrder() throws IOException {
        List<Column> cols = parseColumns();
        assertThat(cols).hasSize(ExecutionAttemptsColumns.FIELD_COUNT);
        assertThat(cols.stream().map(Column::name).toList())
                .containsExactlyElementsOf(ExecutionAttemptsColumns.NAMES);
    }

    @Test
    void ddlTypesMatchTypeRootsPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertThat(cols.get(i).type())
                    .as("column %d (%s) type root", i, cols.get(i).name())
                    .isEqualTo(ExecutionAttemptsColumns.TYPE_ROOTS.get(i));
        }
    }

    @Test
    void ddlNullabilityMatchesPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertThat(cols.get(i).nullableInDdl())
                    .as("column %d (%s) nullability", i, cols.get(i).name())
                    .isEqualTo(ExecutionAttemptsColumns.COLUMN_NULLABLE_IN_DDL.get(i));
        }
    }

    @Test
    void kvContractSingleFieldPkOnExecutionAttemptId() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        assertThat(ddl)
                .containsIgnoringCase("PRIMARY KEY (execution_attempt_id) NOT ENFORCED");
        assertThat(ddl).containsIgnoringCase("'bucket.key' = 'execution_attempt_id'");
        assertThat(ddl).containsIgnoringCase("'table.log.ttl' = '30d'");
    }

    @Test
    void schemaVersionHeaderIsV3() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        assertThat(ddl).containsIgnoringCase("Schema version: 3");
        assertThat(ExecutionAttemptsColumns.SCHEMA_VERSION_V3).isEqualTo("3");
    }

    @Test
    void durableFenceAuthorizationColumnPersistsFenceToken() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        // The gate_epoch authorization identity must be paired with the exact
        // fence token that authorized the attempt (dossier "gate epoch + fence").
        assertThat(ddl).containsIgnoringCase("gate_fence_token")
                .containsIgnoringCase("BIGINT")
                .containsIgnoringCase("NOT NULL");
        // schema_version is the trailing column (repo convention); the fence
        // authorization identity sits immediately before it.
        assertThat(ExecutionAttemptsColumns.SCHEMA_VERSION)
                .isEqualTo(ExecutionAttemptsColumns.FIELD_COUNT - 1);
        assertThat(ExecutionAttemptsColumns.GATE_FENCE_TOKEN)
                .isEqualTo(ExecutionAttemptsColumns.SCHEMA_VERSION - 1);
    }
}
