package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * DEC-038 cross-boundary pin (DDL → Java row layout, the repo's cross-boundary
 * pin habit): {@link FingerprintDedupTableColumns} must stay a faithful mirror
 * of {@code code/01_platform/02_sql/ddl/24_fingerprint_dedup.sql} v1 — 6
 * columns, names in DDL order, per-column type roots, and per-column
 * nullability — so a DDL rename/reorder/retype or a one-sided code edit fails
 * the suite instead of silently mis-serializing dedup rows.
 *
 * <p>Also pins the KV contract (DEC-038 §fingerprint_dedup): primary key
 * exactly {@code [instrument_token, fingerprint_version, event_fingerprint]},
 * {@code instrument_token} routing (PK prefix → per-instrument colocation), 16
 * buckets, {@code kv.format-version = 2} (composite-PK raw-client path per
 * COMPAT-FLUSS-005), and the transient-state shape: key = identity only,
 * value = {@code (first_seen_ms, expiry_ms)} only — never raw event bytes
 * (SIG-UNIT-008).
 */
@DisplayName("DEC-038: FingerprintDedupTableColumns mirrors 24_fingerprint_dedup.sql")
class FingerprintDedupTableColumnsAgreementTest {

    /** CWD is the compute module dir; DDLs live two levels up under code/01_platform. */
    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl/retired").toAbsolutePath();
    private static final String DDL_FILE = "24_fingerprint_dedup.sql";

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
    @DisplayName("DDL declares the frozen 6-column v1 layout in pinned order")
    void ddlDeclares6ColumnsInPinnedOrder() throws IOException {
        String ddl = readDdl();
        List<Column> cols = parseColumns(ddl);
        assertEquals(FingerprintDedupTableColumns.FIELD_COUNT, cols.size(),
                "24_fingerprint_dedup.sql must declare exactly 6 columns (v1); got " + cols.size()
                        + " — if the DDL changed deliberately, update FingerprintDedupTableColumns "
                        + "in the same change (cross-boundary pin habit)");
        assertArrayEquals(FingerprintDedupTableColumns.names(),
                cols.stream().map(Column::name).toArray(String[]::new),
                "column names/order must match the code layout in DDL order");
    }

    @Test
    @DisplayName("DDL types match the pinned TYPE_ROOTS per column")
    void ddlTypesMatchTypeRoots() throws IOException {
        List<Column> cols = parseColumns(readDdl());
        assertEquals(FingerprintDedupTableColumns.TYPE_ROOTS.size(), cols.size());
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(FingerprintDedupTableColumns.TYPE_ROOTS.get(i), cols.get(i).type(),
                    "column " + i + " (" + cols.get(i).name() + ") type root");
        }
    }

    @Test
    @DisplayName("DDL nullability matches COLUMN_NULLABLE_IN_DDL per column (all NOT NULL)")
    void ddlNullabilityMatches() throws IOException {
        List<Column> cols = parseColumns(readDdl());
        assertEquals(FingerprintDedupTableColumns.COLUMN_NULLABLE_IN_DDL.size(), cols.size());
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(FingerprintDedupTableColumns.COLUMN_NULLABLE_IN_DDL.get(i),
                    cols.get(i).nullableInDdl(),
                    "column " + i + " (" + cols.get(i).name() + ") nullability");
        }
    }

    @Test
    @DisplayName("DDL is the KV contract: composite PK, instrument_token routing, 16 buckets, kv.format-version 2")
    void ddlIsKvWithCompositePkAndInstrumentRouting() throws IOException {
        String ddl = readDdl();
        assertTrue(ddl.contains("PRIMARY KEY (instrument_token, fingerprint_version, event_fingerprint)"),
                "fingerprint_dedup must declare the composite PK "
                        + "(instrument_token, fingerprint_version, event_fingerprint)");
        assertTrue(ddl.contains("'bucket.key' = 'instrument_token'"),
                "fingerprint_dedup must route by instrument_token (PK prefix — per-instrument colocation)");
        assertTrue(ddl.contains("'bucket.num' = '16'"),
                "fingerprint_dedup must declare bucket.num = 16");
        assertTrue(ddl.contains("'table.kv.format-version' = '2'"),
                "composite-PK KV tables need kv.format-version=2 for the raw client "
                        + "(COMPAT-FLUSS-005 matrix)");
    }

    @Test
    @DisplayName("index constants are pairwise distinct, in range, and point at the pinned names")
    void indexConstantsMatchNames() {
        String[] names = FingerprintDedupTableColumns.names();
        int[] idx = {
            FingerprintDedupTableColumns.INSTRUMENT_TOKEN,
            FingerprintDedupTableColumns.FINGERPRINT_VERSION,
            FingerprintDedupTableColumns.EVENT_FINGERPRINT,
            FingerprintDedupTableColumns.FIRST_SEEN_MS,
            FingerprintDedupTableColumns.EXPIRY_MS,
            FingerprintDedupTableColumns.SCHEMA_VERSION
        };
        assertEquals(FingerprintDedupTableColumns.FIELD_COUNT, idx.length);
        assertEquals(FingerprintDedupTableColumns.FIELD_COUNT,
                Arrays.stream(idx).distinct().count(), "index constants must be pairwise distinct");
        for (int i = 0; i < idx.length; i++) {
            assertTrue(idx[i] >= 0 && idx[i] < FingerprintDedupTableColumns.FIELD_COUNT,
                    "index " + i + " out of range: " + idx[i]);
            assertEquals(names[i], names[idx[i]],
                    "index constant " + idx[i] + " must point at the " + i + "-th DDL column");
        }
    }

    @Test
    @DisplayName("ROW_TYPE_INFO has one field per DDL column")
    void rowTypeInfoMatchesFieldCount() {
        assertNotNull(FingerprintDedupTableColumns.ROW_TYPE_INFO);
        org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData> info =
                (org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                        FingerprintDedupTableColumns.ROW_TYPE_INFO;
        assertEquals(FingerprintDedupTableColumns.FIELD_COUNT, info.toRowSize(),
                "ROW_TYPE_INFO must declare one field per DDL column");
        assertArrayEquals(FingerprintDedupTableColumns.names(), info.toRowFieldNames(),
                "ROW_TYPE_INFO field names must follow the pinned DDL order");
    }
}
