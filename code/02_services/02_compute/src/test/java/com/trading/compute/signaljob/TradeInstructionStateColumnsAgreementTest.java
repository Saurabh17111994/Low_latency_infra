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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SCH-19 cross-boundary pin (DDL → Java row layout, the repo's cross-boundary
 * pin habit): {@link TradeInstructionStateColumns} must stay a faithful mirror
 * of {@code code/01_platform/02_sql/ddl/25_trade_instruction_state.sql} v1 —
 * 4 columns, names in DDL order, per-column type roots, and per-column
 * nullability — so a DDL rename/reorder/retype or a one-sided code edit fails
 * the suite instead of silently mis-serializing index rows at the KV sink.
 *
 * <p>Also pins the KV contract the instruction-feed protocol relies on
 * (SCH-19, REQ-FLS-015): primary key exactly {@code [instruction_id]},
 * {@code instruction_id} routing, 8 buckets — a single-field PK, so the table
 * is raw-client writable per the COMPAT-FLUSS-005 matrix.
 */
@DisplayName("SCH-19: TradeInstructionStateColumns mirrors 25_trade_instruction_state.sql")
class TradeInstructionStateColumnsAgreementTest {

    /** CWD is the compute module dir; DDLs live two levels up under code/01_platform. */
    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "25_trade_instruction_state.sql";

    /** Column name + type + rest-of-line (NOT NULL clause) per DDL column line. */
    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)(.*)$",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private static String readDdl() throws IOException {
        Path p = DDL_DIR.resolve(DDL_FILE);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private record Column(String name, String type, boolean nullableInDdl) {}

    private static List<Column> parseColumns(String ddl) {
        List<Column> out = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            boolean notNull = m.group(3).toUpperCase(Locale.ROOT).contains("NOT NULL");
            out.add(new Column(m.group(1).toLowerCase(Locale.ROOT),
                    m.group(2).toUpperCase(Locale.ROOT), !notNull));
        }
        return out;
    }

    @Test
    @DisplayName("DDL declares the frozen 4-column v1 layout in pinned order")
    void ddlDeclares4ColumnsInPinnedOrder() throws IOException {
        String ddl = readDdl();
        List<Column> cols = parseColumns(ddl);
        assertEquals(TradeInstructionStateColumns.FIELD_COUNT, cols.size(),
                "25_trade_instruction_state.sql must declare exactly 4 columns (v1); got "
                        + cols.size() + " — if the DDL changed deliberately, update "
                        + "TradeInstructionStateColumns in the same change (cross-boundary pin habit)");
        assertArrayEquals(TradeInstructionStateColumns.namesCopy(),
                cols.stream().map(Column::name).toArray(String[]::new),
                "column names/order must match the code layout in DDL order");
    }

    @Test
    @DisplayName("DDL types match the pinned TYPE_ROOTS per column")
    void ddlTypesMatchTypeRoots() throws IOException {
        List<Column> cols = parseColumns(readDdl());
        assertEquals(TradeInstructionStateColumns.TYPE_ROOTS.size(), cols.size());
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(TradeInstructionStateColumns.TYPE_ROOTS.get(i), cols.get(i).type(),
                    "column " + i + " (" + cols.get(i).name() + ") type root");
        }
    }

    @Test
    @DisplayName("DDL nullability matches COLUMN_NULLABLE_IN_DDL per column (all NOT NULL)")
    void ddlNullabilityMatches() throws IOException {
        List<Column> cols = parseColumns(readDdl());
        assertEquals(TradeInstructionStateColumns.COLUMN_NULLABLE_IN_DDL.size(), cols.size());
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(TradeInstructionStateColumns.COLUMN_NULLABLE_IN_DDL.get(i),
                    cols.get(i).nullableInDdl(),
                    "column " + i + " (" + cols.get(i).name() + ") nullability");
        }
        assertTrue(cols.stream().noneMatch(Column::nullableInDdl),
                "the instruction index must be fully NOT NULL — a partial index row is unusable");
    }

    @Test
    @DisplayName("DDL is the KV contract: PK exactly [instruction_id], instruction_id routing, 8 buckets")
    void ddlIsKvWithInstructionIdRouting() throws IOException {
        String ddl = readDdl();
        assertTrue(ddl.contains("PRIMARY KEY (instruction_id)"),
                "trade_instruction_state must declare primary key (instruction_id) — "
                        + "the durable instruction → hash index (REQ-FLS-015)");
        assertTrue(ddl.contains("'bucket.key' = 'instruction_id'"),
                "trade_instruction_state must route by instruction_id");
        assertTrue(ddl.contains("'bucket.num' = '8'"),
                "trade_instruction_state must declare bucket.num = 8");
        // single-field PK → raw-client writable (COMPAT-FLUSS-005 matrix)
        List<Column> cols = parseColumns(ddl);
        assertFalse(cols.get(0).nullableInDdl(), "instruction_id must be NOT NULL");
        assertEquals("instruction_id", cols.get(0).name());
    }

    @Test
    @DisplayName("index constants are pairwise distinct, in range, and point at the pinned names")
    void indexConstantsMatchNames() {
        String[] names = TradeInstructionStateColumns.namesCopy();
        int[] idx = {
            TradeInstructionStateColumns.INSTRUCTION_ID,
            TradeInstructionStateColumns.CANONICAL_HASH,
            TradeInstructionStateColumns.FIRST_WRITTEN_TS,
            TradeInstructionStateColumns.SCHEMA_VERSION
        };
        assertEquals(TradeInstructionStateColumns.FIELD_COUNT, idx.length);
        assertEquals(TradeInstructionStateColumns.FIELD_COUNT,
                Arrays.stream(idx).distinct().count(), "index constants must be pairwise distinct");
        for (int i = 0; i < idx.length; i++) {
            assertTrue(idx[i] >= 0 && idx[i] < TradeInstructionStateColumns.FIELD_COUNT,
                    "index " + i + " out of range: " + idx[i]);
            assertEquals(names[i], names[idx[i]],
                    "index constant " + idx[i] + " must point at the " + i + "-th DDL column");
        }
    }

    @Test
    @DisplayName("ROW_TYPE_INFO has one field per DDL column")
    void rowTypeInfoMatchesFieldCount() {
        assertNotNull(TradeInstructionStateColumns.ROW_TYPE_INFO);
        org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData> info =
                (org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                        TradeInstructionStateColumns.ROW_TYPE_INFO;
        assertEquals(TradeInstructionStateColumns.FIELD_COUNT, info.toRowSize(),
                "ROW_TYPE_INFO must declare one field per DDL column");
        assertArrayEquals(TradeInstructionStateColumns.namesCopy(), info.toRowFieldNames(),
                "ROW_TYPE_INFO field names must follow the pinned DDL order");
    }

    @Test
    @DisplayName("stream types are non-nullable, mirroring the all-NOT-NULL DDL (P2-190)")
    void rowTypeInfoIsNonNullable() {
        org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData> info =
                (org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                        TradeInstructionStateColumns.ROW_TYPE_INFO;
        for (int i = 0; i < TradeInstructionStateColumns.FIELD_COUNT; i++) {
            assertFalse(info.toRowFieldTypes()[i].isNullable(),
                    "column " + i + " must be non-nullable in the stream type");
        }
    }
}
