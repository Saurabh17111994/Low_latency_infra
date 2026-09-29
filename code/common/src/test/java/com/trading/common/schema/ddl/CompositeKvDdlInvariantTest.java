package com.trading.common.schema.ddl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * L6-2: every composite-KV DDL must ship in the raw-client writable cell.
 *
 * <p>Fluss 0.9.1-incubating's raw client encodes KV keys through
 * {@code KeyEncoder.ofPrimaryKeyEncoder}. With the (cluster-inherited) iceberg
 * datalake format, a composite primary key is writable ONLY when
 * {@code table.kv.format-version=2} AND the bucket key is a single-field subset
 * of the PK (COMPAT-FLUSS-005 matrix, {@link CompositeKeyMatrixVerifier}).
 * {@code 29_position_state.sql} declared the subset bucket key but not the
 * format version, so the apply's raw-client upsert would have failed — and
 * nothing offline compared the two. This test parses every DDL under
 * {@code 01_platform/02_sql/ddl} with the same {@link DdlText} parser the apply
 * engine uses and fails, naming the file, when a composite-PK table declares a
 * proper-subset bucket key without v2, or a bucket key outside the PK.
 */
@DisplayName("L6-2: composite-KV DDLs are raw-client writable (subset bucket key + kv v2)")
class CompositeKvDdlInvariantTest {

    private static final String KV_FORMAT_VERSION = "table.kv.format-version";

    @Test
    @DisplayName("a proper-subset bucket key on a composite PK requires kv.format-version=2")
    void subsetBucketKeyRequiresFormatVersionTwo() throws IOException {
        List<String> violations = new ArrayList<>();
        for (DdlText.ParsedDdl parsed : parseCorpus()) {
            if (parsed.primaryKey().size() <= 1) {
                continue;
            }
            Set<String> pk = new HashSet<>(parsed.primaryKey());
            Set<String> bucket = splitKeys(parsed.bucketKey());
            if (bucket.equals(pk)) {
                // The default bucket key (= PK) is the acknowledged limited
                // cell (isPredictedLimited + ackLimitations); not this check.
                continue;
            }
            if (!"2".equals(parsed.options().get(KV_FORMAT_VERSION))) {
                violations.add(parsed.sourcePath() + ": composite PK " + parsed.primaryKey()
                        + " with bucket.key '" + parsed.bucketKey() + "' needs '"
                        + KV_FORMAT_VERSION + "' = '2'");
            }
        }
        assertThat(violations)
                .as("a composite-PK table with a proper-subset bucket key is raw-client "
                        + "writable only with kv.format-version=2 (COMPAT-FLUSS-005); "
                        + "fix the DDL(s) above")
                .isEmpty();
    }

    @Test
    @DisplayName("every composite PK's bucket key is a subset of the primary key")
    void bucketKeyIsAlwaysASubsetOfThePrimaryKey() throws IOException {
        List<String> violations = new ArrayList<>();
        for (DdlText.ParsedDdl parsed : parseCorpus()) {
            if (parsed.primaryKey().size() <= 1) {
                continue;
            }
            Set<String> pk = new HashSet<>(parsed.primaryKey());
            for (String key : splitKeys(parsed.bucketKey())) {
                if (!pk.contains(key)) {
                    violations.add(parsed.sourcePath() + ": bucket key '" + key
                            + "' is not in the composite PK " + pk);
                }
            }
        }
        assertThat(violations)
                .as("the Fluss connector requires bucket.key ⊆ primary key; a bucket key "
                        + "outside the PK is rejected at CREATE")
                .isEmpty();
    }

    private static Set<String> splitKeys(String keys) {
        Set<String> out = new HashSet<>();
        for (String part : keys.split(",")) {
            out.add(part.trim());
        }
        return out;
    }

    private static List<DdlText.ParsedDdl> parseCorpus() throws IOException {
        List<DdlText.ParsedDdl> parsed = new ArrayList<>();
        try (Stream<Path> files = Files.list(locateDdlDir())) {
            for (Path ddl : files.filter(p -> p.getFileName().toString().endsWith(".sql"))
                    .sorted().toList()) {
                String text = Files.readString(ddl, StandardCharsets.UTF_8);
                if (!text.contains("CREATE TABLE")) {
                    continue; // catalog/database bootstrap, not a table
                }
                parsed.add(DdlText.parse(text, ddl.getFileName().toString()));
            }
        }
        assertThat(parsed).as("no DDLs parsed — the corpus location moved").isNotEmpty();
        return parsed;
    }

    /**
     * Locates {@code 01_platform/02_sql/ddl} by walking up from the working
     * directory (Maven runs tests with cwd = module dir, i.e. {@code code/common};
     * IDEs may run from the repo root). Fails with a descriptive message if the
     * corpus moves — never a silent skip.
     */
    private static Path locateDdlDir() {
        Path dir = Path.of("").toAbsolutePath();
        for (int depth = 0; depth < 6; depth++) {
            Path candidate = dir.resolve("01_platform/02_sql/ddl");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            candidate = dir.resolve("code/01_platform/02_sql/ddl");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
            if (dir == null) {
                break;
            }
        }
        throw new IllegalStateException(
                "cannot locate 01_platform/02_sql/ddl under " + Path.of("").toAbsolutePath()
                        + " — expected code/01_platform/02_sql/ddl");
    }
}
