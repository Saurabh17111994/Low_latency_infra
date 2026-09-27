package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * DDL agreement pin for {@link FeatureValuesColumns}: the column contract the
 * writer emits must mirror {@code 34_feature_values.sql} v1 (names, order,
 * types, primary key, routing). Same pattern as
 * {@link CandleClosedColumnsAgreementTest}.
 */
class FeatureValuesColumnsAgreementTest {

    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "34_feature_values.sql";

    private static String ddl() throws Exception {
        Path path = DDL_DIR.resolve(DDL_FILE);
        assertTrue(Files.exists(path), "missing DDL file " + path);
        return Files.readString(path, StandardCharsets.UTF_8);
    }

    @Test
    void columnsAppearInDdlOrderWithTheDeclaredTypes() throws Exception {
        String ddl = ddl();
        int previous = -1;
        for (int i = 0; i < FeatureValuesColumns.COLUMN_NAMES.size(); i++) {
            String name = FeatureValuesColumns.COLUMN_NAMES.get(i);
            int at = ddl.indexOf(name);
            assertTrue(at > previous, name + " missing or out of DDL order");
            previous = at;
        }
        assertTrue(ddl.contains("instrument_token  BIGINT"), "instrument_token BIGINT");
        assertTrue(ddl.contains("tf                STRING"), "tf STRING");
        assertTrue(ddl.contains("window_start      BIGINT"), "window_start BIGINT");
        assertTrue(ddl.contains("features          MAP<INT, DOUBLE>"), "features MAP<INT, DOUBLE>");
    }

    @Test
    void primaryKeyAndRoutingMatchTheContract() throws Exception {
        String ddl = ddl();
        assertTrue(
                ddl.contains("PRIMARY KEY (instrument_token, tf, window_start)"),
                "composite primary key");
        assertTrue(ddl.contains("'bucket.num' = '16'"), "bucket count");
        assertTrue(ddl.contains("'bucket.key' = 'instrument_token'"), "bucket key");
        assertTrue(ddl.contains("'table.kv.format-version' = '2'"), "KV format version");
    }

    @Test
    void fieldListsAgreeWithTheColumnCount() {
        assertEquals(FeatureValuesColumns.FIELD_COUNT, FeatureValuesColumns.COLUMN_NAMES.size());
        assertEquals(FeatureValuesColumns.FIELD_COUNT, FeatureValuesColumns.TYPE_ROOTS.size());
        assertEquals(FeatureValuesColumns.FIELD_COUNT, FeatureValuesColumns.COLUMN_NULLABLE_IN_DDL.size());
        List<String> roots = FeatureValuesColumns.TYPE_ROOTS;
        assertEquals(List.of("BIGINT", "STRING", "BIGINT", "MAP"), roots);
    }
}
