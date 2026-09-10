package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Cross-boundary pin between the Execution_Intent DDL and Java row layout. */
@DisplayName("Execution_Intent DDL and Java layout agree")
class ExecutionIntentTableColumnsAgreementTest {

    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)(.*)$",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private record Column(String name, String type, boolean nullable) {}

    private static String ddl() throws IOException {
        return Files.readString(DDL_DIR.resolve("27_execution_intent.sql"),
                StandardCharsets.UTF_8);
    }

    private static List<Column> columns(String ddl) {
        List<Column> result = new ArrayList<>();
        Matcher matcher = COLUMN.matcher(ddl);
        while (matcher.find()) {
            String suffix = matcher.group(3).toUpperCase(Locale.ROOT);
            result.add(new Column(matcher.group(1).toLowerCase(Locale.ROOT),
                    matcher.group(2).toUpperCase(Locale.ROOT), !suffix.contains("NOT NULL")));
        }
        return result;
    }

    @Test
    void ddlNamesAndTypesMatchJavaLayout() throws IOException {
        List<Column> columns = columns(ddl());
        assertEquals(ExecutionIntentTableColumns.FIELD_COUNT, columns.size());
        assertArrayEquals(ExecutionIntentTableColumns.namesCopy(),
                columns.stream().map(Column::name).toArray(String[]::new));
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(ExecutionIntentTableColumns.TYPE_ROOTS.get(i), columns.get(i).type(),
                    "type at column " + i);
            assertEquals(ExecutionIntentTableColumns.COLUMN_NULLABLE_IN_DDL.get(i),
                    columns.get(i).nullable(), "nullability at column " + i);
        }
    }

    @Test
    void ddlIsAppendOnlyAndRoutedByInstructionId() throws IOException {
        String ddl = ddl();
        assertFalse(ddl.contains("PRIMARY KEY"));
        assertTrue(ddl.contains("'bucket.key' = 'instruction_id'"));
        assertTrue(ddl.contains("'bucket.num' = '8'"));
    }

    @Test
    void rowTypeInfoMatchesLayout() {
        assertNotNull(ExecutionIntentTableColumns.ROW_TYPE_INFO);
        var info = (org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                ExecutionIntentTableColumns.ROW_TYPE_INFO;
        assertEquals(ExecutionIntentTableColumns.FIELD_COUNT, info.toRowSize());
        assertArrayEquals(ExecutionIntentTableColumns.namesCopy(), info.toRowFieldNames());
    }

    @Test
    void layoutContainsNoMutableExecutionFields() {
        String joined = String.join(",", ExecutionIntentTableColumns.namesCopy());
        assertFalse(joined.contains("client_order_ref"));
        assertFalse(joined.contains("broker_order_id"));
        assertFalse(joined.contains("execution_status"));
        assertFalse(joined.contains("reservation_id"));
    }
}
