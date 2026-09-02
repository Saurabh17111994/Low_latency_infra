package com.trading.common.schema.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.schema.SchemaManifest;
import com.trading.common.schema.SchemaManifestEntry;
import com.trading.common.schema.ddl.DdlText;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * COMPAT-FLUSS-001 (docs/08_implementation/02-schema-storage.md SCH-12/SCH-13):
 * every approved DDL parses, applies, and is inspected on the pinned matrix —
 * effective schema/options equal the approved manifest.
 *
 * <p>Fluss 0.9.1 has no SQL client, so "parse" means building the admin-API
 * {@link TableDescriptor} from the DDL text (columns, primary key, bucket key,
 * bucket count, TTL, datalake options). Each of the 21 approved DDLs under
 * {@code code/01_platform/02_sql/ddl} is applied to a unique scratch table on
 * the live cluster, then inspected via {@link Admin#getTableInfo}; the
 * effective schema/options are asserted against both the DDL text and the
 * committed {@code schema_manifest.json} entry.
 *
 * <p>Deviation, deliberately matching the live dev cluster (see the lake-state
 * note in 02-schema-storage.md): {@code table.datalake.enabled} is forced to
 * {@code false} at apply time because Fluss 0.9.1 lake-enable is create-only
 * and collides with orphaned R2 lake objects. Production DDLs keep
 * {@code enabled=true} (the blueprint); this test proves the DDL-to-descriptor
 * contract on everything except the lake tier, which has its own evidence.
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP} (e.g. {@code localhost:9123}) — skipped
 * without it or when the cluster is unreachable. Scratch tables are named
 * {@code compat_ddl_*} and dropped after the run; platform tables are never
 * touched. Run: {@code FLUSS_BOOTSTRAP=localhost:9123 mvn -o test -pl common}
 */
@Tag("integration")
@DisplayName("COMPAT-FLUSS-001: every approved DDL applies and inspects with effective "
        + "schema/options equal to the manifest")
class CompatFlussDdlParityIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CompatFlussDdlParityIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String SCRATCH_PREFIX = "compat_ddl_";
    private static final List<String> CREATED = new ArrayList<>();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Path ddlDir;
    private static SchemaManifest manifest;
    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP to run the COMPAT-FLUSS-001 integration test");
        ddlDir = resolveDdlDir();
        manifest = MAPPER.readValue(
                ddlDir.resolve("schema_manifest.json").toFile(), SchemaManifest.class);
        assertNotNull(manifest.tables, "manifest must carry tables");
        assertEquals(27, manifest.tables.size(), "approved manifest must hold 27 tables");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("compat-fluss-001: connected to {} ({} manifest entries)", bootstrap,
                    manifest.tables.size());
        } catch (Exception e) {
            LOG.warn("compat-fluss-001: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            for (String name : CREATED) {
                try {
                    admin.dropTable(TablePath.of("default", name), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    LOG.info("compat-fluss-001: dropped {}", name);
                } catch (Exception e) {
                    LOG.warn("compat-fluss-001: drop {} failed: {}", name, e.getMessage());
                }
            }
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("every approved DDL applies; effective schema/options equal the manifest")
    void everyApprovedDdlAppliesWithManifestParity() throws Exception {
        assumeTrue(admin != null, "no live cluster");
        List<String> failures = new ArrayList<>();
        for (int i = 0; i < manifest.tables.size(); i++) {
            SchemaManifestEntry entry = manifest.tables.get(i);
            try {
                String scratch = applyAndVerify(i, entry);
                LOG.info("compat-fluss-001: {} applied + inspected OK", scratch);
            } catch (Throwable t) {
                String message = t.getMessage();
                failures.add(entry.tableName + ": "
                        + (message == null ? t.getClass().getSimpleName() : message));
            }
        }
        assertTrue(failures.isEmpty(),
                "every approved DDL must apply with manifest parity — failures:\n  "
                        + String.join("\n  ", failures));
    }

    /** Applies DDL {@code entry} to a scratch table and asserts effective parity. */
    private static String applyAndVerify(int index, SchemaManifestEntry entry) throws Exception {
        Path ddlPath = ddlDir.resolve(entry.ddlPath);
        String text = Files.readString(ddlPath, StandardCharsets.UTF_8);
        assertEquals(entry.ddlSha256, sha256Hex(Files.readAllBytes(ddlPath)),
                entry.tableName + ": committed manifest ddl_sha256 must equal the DDL file bytes");
        DdlText.ParsedDdl ddl = DdlText.parse(text, entry.ddlPath);

        // Manifest vs DDL consistency (the manifest is generated from the DDLs;
        // a mismatch means the generator and the committed manifest disagree).
        String expectedKind = ddl.primaryKey().isEmpty() ? "LOG" : "KV";
        assertEquals(expectedKind, entry.tableKind,
                entry.tableName + ": manifest table_kind must match DDL PK presence");
        if (expectedKind.equals("KV")) {
            assertEquals(String.join(", ", ddl.primaryKey()), entry.primaryKey,
                    entry.tableName + ": manifest primary_key must match DDL");
        } else {
            assertEquals(ddl.bucketKey(), entry.bucketKey,
                    entry.tableName + ": manifest bucket_key must match DDL");
        }

        String scratch = SCRATCH_PREFIX + index + "_" + ddl.tableName();
        TableDescriptor descriptor = DdlText.toDescriptor(ddl);
        admin.createTable(TablePath.of("default", scratch), descriptor, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED.add(scratch);

        TableInfo info = admin.getTableInfo(TablePath.of("default", scratch))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEffectiveParity(entry, ddl, info, scratch);
        return scratch;
    }

    private static void assertEffectiveParity(SchemaManifestEntry entry, DdlText.ParsedDdl ddl,
            TableInfo info, String scratch) {
        String label = scratch + " (" + entry.tableName + ")";

        assertEquals(ddl.bucketCount(), info.getNumBuckets(),
                label + ": effective bucket count must equal the DDL bucket.num");
        assertEquals(ddl.partitionKeys(), info.getPartitionKeys(),
                label + ": effective partition keys must match the DDL");

        Schema effective = info.getSchema();
        List<String> effectiveColumns = effective.getColumnNames();
        assertEquals(ddl.columns().size(), effectiveColumns.size(),
                label + ": effective column count must equal the DDL");
        for (int c = 0; c < ddl.columns().size(); c++) {
            DdlText.Column want = ddl.columns().get(c);
            assertEquals(want.name(), effectiveColumns.get(c),
                    label + ": column order/name must match the DDL");
            // Fluss marks primary-key columns NOT NULL automatically; compare
            // the base type with nullability stripped on both sides.
            assertEquals(want.type().copy(false),
                    effective.getColumn(want.name()).getDataType().copy(false),
                    label + ": column " + want.name() + " type must match the DDL");
        }

        if (ddl.primaryKey().isEmpty()) {
            assertFalse(info.hasPrimaryKey(), label + ": LOG table must have no primary key");
            assertEquals(List.of(ddl.bucketKey()), info.getBucketKeys(),
                    label + ": LOG routing bucket key must match the DDL");
        } else {
            assertTrue(info.hasPrimaryKey(), label + ": KV table must have a primary key");
            assertEquals(ddl.primaryKey(), info.getPrimaryKeys(),
                    label + ": effective primary key must match the DDL");
            List<String> expectedBucket = List.of(ddl.bucketKey().split(","));
            assertEquals(expectedBucket, info.getBucketKeys(),
                    label + ": KV bucket key must match the DDL (subset of PK)");
        }

        // Full WITH-option parity (verified 2026-08-15 against all 21 tables):
        // EVERY option the DDL declares — the manifest pins the DDL bytes by
        // checksum — must be honored by the effective table, with exactly two
        // carve-outs: bucket.num / bucket.key are distribution (distributedBy,
        // not properties), and table.datalake.enabled is the documented dev
        // deviation (forced false; lake-enable is create-only in 0.9.1). The
        // coordinator may stamp extras (replication.factor, cluster-inherited
        // datalake.format, kv.format-version) — those are not asserted.
        Map<String, String> effectiveOptions = info.getProperties().toMap();
        for (Map.Entry<String, String> declared : ddl.options().entrySet()) {
            String key = declared.getKey();
            if (key.equals("bucket.num") || key.equals("bucket.key")) {
                continue;
            }
            String want = declared.getValue();
            String have = effectiveOptions.get(key);
            if (key.equals("table.datalake.enabled")) {
                assertEquals("false", have,
                        label + ": table.datalake.enabled must be false "
                                + "(documented dev deviation)");
                continue;
            }
            assertEquals(want, have,
                    label + ": effective " + key + " must match the DDL");
        }
        // The coordinator stamps datalake.format but never the enabled flag:
        // a DDL that declares no lake options must not gain table.datalake.enabled.
        if (!ddl.options().containsKey("table.datalake.enabled")) {
            assertEquals(null, effectiveOptions.get("table.datalake.enabled"),
                    label + ": DDL without lake options must not gain "
                            + "table.datalake.enabled");
        }
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(bytes);
        StringBuilder sb = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /** Locate {@code code/01_platform/02_sql/ddl} by walking up from the working directory. */
    private static Path resolveDdlDir() throws IOException {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve("01_platform/02_sql/ddl/schema_manifest.json");
            if (Files.isRegularFile(candidate)) {
                return candidate.getParent();
            }
            current = current.getParent();
        }
        throw new IOException("cannot locate 01_platform/02_sql/ddl/schema_manifest.json "
                + "from working directory");
    }

}
