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
 * pin habit): {@code TradeDecisionsTableColumns} must stay a faithful mirror
 * of {@code code/01_platform/02_sql/ddl/07_trade_decisions.sql} v2 — 25
 * columns, names in DDL order, per-column type roots, and per-column
 * nullability — so a DDL rename/reorder/retype or a one-sided code edit fails
 * the suite instead of silently mis-serializing rows at the LOG sink.
 *
 * <p>Also pins the LOG contract the writer relies on (REQ-FLS-008): no
 * primary key, {@code instruction_id} routing, 8 buckets, and NO
 * Executor-assigned fields anywhere in the layout.
 */
@DisplayName("SCH-19: TradeDecisionsTableColumns mirrors 07_trade_decisions.sql")
class TradeDecisionsTableColumnsAgreementTest {

    /** CWD is the compute module dir; DDLs live two levels up under code/01_platform. */
    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "07_trade_decisions.sql";

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
    @DisplayName("DDL declares the frozen 25-column v2 layout in pinned order")
    void ddlDeclares25ColumnsInPinnedOrder() throws IOException {
        String ddl = readDdl();
        List<Column> cols = parseColumns(ddl);
        assertEquals(TradeDecisionsTableColumns.FIELD_COUNT, cols.size(),
                "07_trade_decisions.sql must declare exactly 25 columns (v2); got " + cols.size()
                        + " — if the DDL changed deliberately, update TradeDecisionsTableColumns "
                        + "in the same change (cross-boundary pin habit)");
        assertArrayEquals(TradeDecisionsTableColumns.namesCopy(),
                cols.stream().map(Column::name).toArray(String[]::new),
                "column names/order must match the code layout in DDL order");
    }

    @Test
    @DisplayName("DDL types match the pinned TYPE_ROOTS per column")
    void ddlTypesMatchTypeRoots() throws IOException {
        List<Column> cols = parseColumns(readDdl());
        assertEquals(TradeDecisionsTableColumns.TYPE_ROOTS.size(), cols.size());
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(TradeDecisionsTableColumns.TYPE_ROOTS.get(i), cols.get(i).type(),
                    "column " + i + " (" + cols.get(i).name() + ") type root");
        }
    }

    @Test
    @DisplayName("DDL nullability matches COLUMN_NULLABLE_IN_DDL per column")
    void ddlNullabilityMatches() throws IOException {
        List<Column> cols = parseColumns(readDdl());
        assertEquals(TradeDecisionsTableColumns.COLUMN_NULLABLE_IN_DDL.size(), cols.size());
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(TradeDecisionsTableColumns.COLUMN_NULLABLE_IN_DDL.get(i),
                    cols.get(i).nullableInDdl(),
                    "column " + i + " (" + cols.get(i).name() + ") nullability");
        }
    }

    @Test
    @DisplayName("DDL is the LOG contract the writer relies on: no PK, instruction_id routing, 8 buckets")
    void ddlIsLogWithInstructionIdRouting() throws IOException {
        String ddl = readDdl();
        assertFalse(ddl.contains("PRIMARY KEY"),
                "Trade_Decisions is an immutable LOG feed (REQ-FLS-008) — it must not declare a primary key");
        assertTrue(ddl.contains("'bucket.key' = 'instruction_id'"),
                "Trade_Decisions must route by instruction_id (SCH-07 non-null routing identity)");
        assertTrue(ddl.contains("'bucket.num' = '8'"),
                "Trade_Decisions must declare bucket.num = 8");
        // The routing identity is NOT NULL in the DDL — a LOG write without an
        // instruction_id must be impossible by construction.
        Column instructionId = parseColumns(ddl).get(0);
        assertEquals("instruction_id", instructionId.name());
        assertFalse(instructionId.nullableInDdl(), "instruction_id must be NOT NULL");
    }

    @Test
    @DisplayName("index constants are pairwise distinct, in range, and point at the pinned names")
    void indexConstantsMatchNames() {
        String[] names = TradeDecisionsTableColumns.namesCopy();
        int[] idx = {
            TradeDecisionsTableColumns.INSTRUCTION_ID, TradeDecisionsTableColumns.CANDIDATE_ID,
            TradeDecisionsTableColumns.TRADE_CONTEXT_ID, TradeDecisionsTableColumns.INSTRUMENT_TOKEN,
            TradeDecisionsTableColumns.EXCHANGE, TradeDecisionsTableColumns.SYMBOL,
            TradeDecisionsTableColumns.SIDE, TradeDecisionsTableColumns.QUANTITY,
            TradeDecisionsTableColumns.ORDER_TYPE, TradeDecisionsTableColumns.PRODUCT_TYPE,
            TradeDecisionsTableColumns.LIMIT_PRICE_PAISE, TradeDecisionsTableColumns.PORTFOLIO_ID,
            TradeDecisionsTableColumns.ACCOUNT_SCOPE_ID, TradeDecisionsTableColumns.STRATEGY_ID,
            TradeDecisionsTableColumns.STRATEGY_VERSION, TradeDecisionsTableColumns.CONFIGURATION_VERSION,
            TradeDecisionsTableColumns.EVALUATION_ID, TradeDecisionsTableColumns.COMPOSITE_SCORE,
            TradeDecisionsTableColumns.RESERVATION_ID, TradeDecisionsTableColumns.RESERVATION_VERSION,
            TradeDecisionsTableColumns.CREATED_TS, TradeDecisionsTableColumns.EXPIRY_TS,
            TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID,
            TradeDecisionsTableColumns.SUPERSEDED_BY_INSTRUCTION_ID,
            TradeDecisionsTableColumns.SCHEMA_VERSION
        };
        assertEquals(TradeDecisionsTableColumns.FIELD_COUNT, idx.length);
        assertEquals(TradeDecisionsTableColumns.FIELD_COUNT,
                Arrays.stream(idx).distinct().count(), "index constants must be pairwise distinct");
        for (int i = 0; i < idx.length; i++) {
            assertTrue(idx[i] >= 0 && idx[i] < TradeDecisionsTableColumns.FIELD_COUNT,
                    "index " + i + " out of range: " + idx[i]);
            assertEquals(names[i], names[idx[i]],
                    "index constant " + idx[i] + " must point at the " + i + "-th DDL column");
        }
    }

    @Test
    @DisplayName("ROW_TYPE_INFO has one field per DDL column")
    void rowTypeInfoMatchesFieldCount() {
        assertNotNull(TradeDecisionsTableColumns.ROW_TYPE_INFO);
        // InternalTypeInfo extends plain TypeInformation (getArity() == 1 by
        // default) — toRowSize()/toRowFieldNames() carry the row layout.
        org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData> info =
                (org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                        TradeDecisionsTableColumns.ROW_TYPE_INFO;
        assertEquals(TradeDecisionsTableColumns.FIELD_COUNT, info.toRowSize(),
                "ROW_TYPE_INFO must declare one field per DDL column");
        assertArrayEquals(TradeDecisionsTableColumns.namesCopy(), info.toRowFieldNames(),
                "ROW_TYPE_INFO field names must follow the pinned DDL order");
    }

    @Test
    @DisplayName("layout carries no Executor-assigned fields (REQ-FLS-008)")
    void noExecutorAssignedFields() {
        String joined =
                String.join(",", TradeDecisionsTableColumns.namesCopy()).toLowerCase(Locale.ROOT);
        assertFalse(joined.contains("client_order_ref"),
                "Trade_Decisions SHALL NOT contain client_order_ref (REQ-FLS-008)");
        assertFalse(joined.contains("broker_order_id"),
                "Trade_Decisions SHALL NOT contain broker_order_id (REQ-FLS-008)");
        assertFalse(joined.contains("execution_status"),
                "Trade_Decisions SHALL NOT carry mutable execution status (REQ-FLS-008)");
    }
}
