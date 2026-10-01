package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
 * Phase 0 multi-TF aggregator contract: the first
 * {@link CandleLiveColumns#DDL_FIELD_COUNT} columns of {@link CandleLiveColumns}
 * must mirror the candle prefix of
 * {@code code/01_platform/02_sql/ddl/35_candle_features.sql} (DDL order, types,
 * nullability, PK instrument_token/tf/window_start) — the merged table's candle
 * prefix. Wave C W-C5a retired DDL 32/33 with their tables, so the pin moved to
 * the surviving DDL — same pattern as {@link CandleClosedColumnsAgreementTest}.
 *
 * <p>The trailing {@link CandleLiveColumns#INGEST_TS} probe is deliberately NOT
 * part of the DDL prefix: it is an in-memory observability column on the live
 * stream only (never persisted), and the last test below proves the DDL does not
 * declare it.
 */
class CandleLiveColumnsAgreementTest {

    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "35_candle_features.sql";

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
        assertTrue(cols.size() >= CandleLiveColumns.DDL_FIELD_COUNT,
                "merged DDL carries the candle contract plus features + sealed");
        assertEquals(CandleLiveColumns.COLUMN_NAMES.subList(0, CandleLiveColumns.DDL_FIELD_COUNT),
                cols.subList(0, CandleLiveColumns.DDL_FIELD_COUNT).stream().map(Column::name).toList());
    }

    @Test
    void ddlTypesMatchTypeRootsPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < CandleLiveColumns.DDL_FIELD_COUNT; i++) {
            assertEquals(CandleLiveColumns.TYPE_ROOTS.get(i), cols.get(i).type(),
                    "column " + i + " (" + cols.get(i).name() + ") type root");
        }
    }

    @Test
    void ddlNullabilityMatchesPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < CandleLiveColumns.DDL_FIELD_COUNT; i++) {
            assertEquals(CandleLiveColumns.COLUMN_NULLABLE_IN_DDL.get(i),
                    cols.get(i).nullableInDdl(),
                    "column " + i + " (" + cols.get(i).name() + ") nullability");
        }
    }

    @Test
    void trailingIngestProbeIsPinnedAndAbsentFromTheDdl() throws IOException {
        // The probe is the first column AFTER the DDL prefix, BIGINT, NOT NULL.
        assertEquals(CandleLiveColumns.DDL_FIELD_COUNT, CandleLiveColumns.INGEST_TS,
                "the ingest probe must sit immediately after the pinned prefix");
        assertEquals("ingest_ts", CandleLiveColumns.COLUMN_NAMES.get(CandleLiveColumns.INGEST_TS));
        assertEquals("BIGINT", CandleLiveColumns.TYPE_ROOTS.get(CandleLiveColumns.INGEST_TS));
        assertEquals(Boolean.FALSE,
                CandleLiveColumns.COLUMN_NULLABLE_IN_DDL.get(CandleLiveColumns.INGEST_TS));
        // …and the merged DDL does NOT declare it: index 15 there is a feature.
        List<Column> cols = parseColumns();
        assertNotEquals("ingest_ts", cols.get(CandleLiveColumns.DDL_FIELD_COUNT).name(),
                "ingest_ts must never leak into the stored merged-table schema");
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
        assertEquals(true, ddl.contains("'table.log.ttl' = '3d'"));
        assertEquals(true, ddl.contains("'table.datalake.enabled' = 'false'"));
    }
}
