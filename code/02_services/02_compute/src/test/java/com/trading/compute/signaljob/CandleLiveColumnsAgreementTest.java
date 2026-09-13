package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

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
 * Phase 0 multi-TF aggregator contract: {@link CandleLiveColumns} must mirror
 * {@code code/01_platform/02_sql/ddl/32_candle_live.sql} v1 (15 columns,
 * DDL order, types, nullability, PK instrument_token/tf/window_start)
 * — same pattern as {@link CandleClosedColumnsAgreementTest}.
 */
class CandleLiveColumnsAgreementTest {

    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "32_candle_live.sql";

    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)(.*)$",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private record Column(String name, String type, boolean nullableInDdl) {}

    private static List<Column> parseColumns() throws IOException {
        Path p = DDL_DIR.resolve(DDL_FILE);
        assert Files.exists(p) : "missing DDL file " + p;
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
    void ddlDeclares15ColumnsInPinnedOrder() throws IOException {
        List<Column> cols = parseColumns();
        assertEquals(CandleLiveColumns.FIELD_COUNT, cols.size());
        assertEquals(CandleLiveColumns.COLUMN_NAMES,
                cols.stream().map(Column::name).toList());
    }

    @Test
    void ddlTypesMatchTypeRootsPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(CandleLiveColumns.TYPE_ROOTS.get(i), cols.get(i).type(),
                    "column " + i + " (" + cols.get(i).name() + ") type root");
        }
    }

    @Test
    void ddlNullabilityMatchesPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(CandleLiveColumns.COLUMN_NULLABLE_IN_DDL.get(i),
                    cols.get(i).nullableInDdl(),
                    "column " + i + " (" + cols.get(i).name() + ") nullability");
        }
    }

    @Test
    void liveLayoutHardeningMatchesClosedConvention() {
        assertEquals(CandleLiveColumns.FIELD_COUNT, CandleLiveColumns.COLUMN_NAMES.size());
        assertEquals(CandleLiveColumns.FIELD_COUNT, CandleLiveColumns.TYPE_ROOTS.size());
        assertEquals(CandleLiveColumns.FIELD_COUNT, CandleLiveColumns.COLUMN_NULLABLE_IN_DDL.size());
        assertEquals(CandleLiveColumns.FIELD_COUNT, CandleLiveColumns.ROW_TYPE_INFO.toRowSize());
        String[] a = CandleLiveColumns.namesCopy();
        String[] b = CandleLiveColumns.namesCopy();
        assertNotSame(a, b);
        assertEquals(java.util.Arrays.asList(a), java.util.Arrays.asList(b));
        assertEquals(CandleLiveColumns.COLUMN_NAMES, java.util.Arrays.asList(a));
    }

    @Test
    void kvContractCompositePkOnInstrumentTokenTfWindowStart() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        assertEquals(true, ddl.contains("PRIMARY KEY (instrument_token, tf, window_start) NOT ENFORCED"));
        assertEquals(true, ddl.contains("'bucket.key' = 'instrument_token'"));
        assertEquals(true, ddl.contains("'bucket.num' = '16'"));
        assertEquals(true, ddl.contains("'table.log.ttl' = '60s'"));
        assertEquals(true, ddl.contains("'table.datalake.enabled' = 'false'"));
    }
}
