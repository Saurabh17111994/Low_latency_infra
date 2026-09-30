package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * DDL agreement pin for {@link MergedCandleFeaturesColumns}: the column
 * contract the writer emits must mirror {@code 35_candle_features.sql} v1
 * (names, order, types, primary key, routing, DEC-060 opt-in lake). Same
 * pattern as {@link FeatureValuesColumnsAgreementTest}.
 */
class MergedCandleFeaturesColumnsAgreementTest {

    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "35_candle_features.sql";

    private static String ddl() throws Exception {
        Path path = DDL_DIR.resolve(DDL_FILE);
        assertTrue(Files.exists(path), "missing DDL file " + path);
        return Files.readString(path, StandardCharsets.UTF_8);
    }

    @Test
    void columnsAppearInDdlOrderWithTheDeclaredTypes() throws Exception {
        String ddl = ddl();
        int create = ddl.indexOf("CREATE TABLE candle_features");
        String body = ddl
                .substring(ddl.indexOf("(", create), ddl.indexOf(") WITH ("))
                .replaceAll("\\s+", " ");
        int previous = -1;
        for (String name : MergedCandleFeaturesColumns.COLUMN_NAMES) {
            int at = body.indexOf(name);
            assertTrue(at > previous, name + " missing or out of DDL order");
            previous = at;
        }
        assertTrue(body.contains("instrument_token BIGINT NOT NULL"), "instrument_token BIGINT");
        assertTrue(body.contains("tf STRING NOT NULL"), "tf STRING");
        assertTrue(body.contains("tick_count INT NOT NULL"), "tick_count INT");
        assertTrue(body.contains("features MAP<INT, DOUBLE>"), "features MAP<INT, DOUBLE>");
        assertTrue(body.contains("sealed BOOLEAN NOT NULL"), "sealed BOOLEAN");
    }

    @Test
    void primaryKeyRoutingAndTheOptInLakeMatchTheContract() throws Exception {
        String ddl = ddl();
        assertTrue(ddl.contains("PRIMARY KEY (instrument_token, tf, window_start)"),
                "composite primary key");
        assertTrue(ddl.contains("'bucket.num' = '16'"), "bucket count");
        assertTrue(ddl.contains("'bucket.key' = 'instrument_token'"), "bucket key");
        assertTrue(ddl.contains("'table.kv.format-version' = '2'"), "KV format version");
        assertTrue(ddl.contains("'table.log.ttl' = '3d'"), "3d retention");
        assertTrue(ddl.contains("'table.datalake.enabled' = 'false'"),
                "DEC-060: archiving is opt-in per table");
    }

    @Test
    void fieldListsAgreeWithTheColumnCount() {
        assertEquals(MergedCandleFeaturesColumns.FIELD_COUNT,
                MergedCandleFeaturesColumns.COLUMN_NAMES.size());
        assertEquals(MergedCandleFeaturesColumns.FIELD_COUNT,
                MergedCandleFeaturesColumns.TYPE_ROOTS.size());
        assertEquals(MergedCandleFeaturesColumns.FIELD_COUNT,
                MergedCandleFeaturesColumns.COLUMN_NULLABLE_IN_DDL.size());
        assertEquals(
                List.of("BIGINT", "STRING", "STRING", "STRING", "BIGINT", "BIGINT", "BIGINT",
                        "BIGINT", "BIGINT", "BIGINT", "BIGINT", "INTEGER", "BIGINT", "STRING",
                        "STRING", "MAP", "BOOLEAN"),
                MergedCandleFeaturesColumns.TYPE_ROOTS);
    }
}
