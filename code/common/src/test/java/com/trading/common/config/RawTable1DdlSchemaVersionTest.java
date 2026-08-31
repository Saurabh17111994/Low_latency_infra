package com.trading.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Cross-boundary version pin (process rule, approved 2026-08-10): the
 * {@code schema_version} column of {@code raw_table_1} is written by the
 * ingestion service and validated by the compute service — both must derive
 * from {@link PlatformConfig#RAW_TABLE_1_SCHEMA_VERSION} (single source of
 * truth in {@code common}). The DDL header's {@code Schema version: N} is a
 * mirrored copy of that constant; this test fails the build if the two drift,
 * so a future v3 bump cannot silently desynchronize the producer/consumer
 * gate (the 2026-08-10 stall: ingestion builder defaulted to "1" while
 * compute validated "2").
 */
@DisplayName("raw_table_1 DDL header Schema version pin")
class RawTable1DdlSchemaVersionTest {

    /** Matches the header line {@code -- Schema version: N} (see 02_raw_table_1.sql). */
    private static final Pattern SCHEMA_VERSION_LINE =
            Pattern.compile("(?m)^--\\s*Schema version:\\s*(\\S+)\\s*$");

    @Test
    @DisplayName("DDL header equals PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION")
    void ddlHeaderMatchesPlatformConfig() throws IOException {
        Path ddl = locateRawTable1Ddl();
        String header = Files.readString(ddl, StandardCharsets.UTF_8);
        String ddlVersion = parseSchemaVersion(header);

        assertThat(ddlVersion)
                .as("02_raw_table_1.sql header 'Schema version' must match "
                        + "PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION — bump DDL and constant "
                        + "together in one change (raw_table_1 is written by ingestion, "
                        + "validated by compute)")
                .isEqualTo(PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION);
    }

    @Test
    @DisplayName("parser reads a future version header (mismatch would be caught, not silently passed)")
    void parserDetectsVersionBump() {
        String futureHeader = "-- raw_table_1\n-- Schema version: 4\n--\n";
        assertThat(parseSchemaVersion(futureHeader)).isEqualTo("4");
        assertThat(parseSchemaVersion(futureHeader))
                .isNotEqualTo(PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION);
    }

    @Test
    @DisplayName("missing or malformed version marker fails loudly, not with null")
    void parserRejectsMissingMarker() {
        assertThatThrownBy(() -> parseSchemaVersion("-- raw_table_1\n-- Type: LOG\n"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Schema version");
    }

    /**
     * Package-private for fixture tests: extracts the {@code Schema version: N}
     * value from a DDL header block. Throws on missing/malformed marker so a
     * header edit cannot silently neuter the pin.
     */
    static String parseSchemaVersion(String header) {
        Matcher m = SCHEMA_VERSION_LINE.matcher(header);
        if (!m.find()) {
            throw new IllegalStateException(
                    "02_raw_table_1.sql header has no '-- Schema version: N' line — "
                            + "the DDL↔code pin test cannot run");
        }
        return m.group(1);
    }

    /**
     * Locates {@code 01_platform/02_sql/ddl/02_raw_table_1.sql} by walking up
     * from the working directory (Maven runs tests with cwd = module dir, i.e.
     * {@code code/common}; IDEs may run from the repo root). Fails with a
     * descriptive message if the DDL moves — not with a silent skip.
     */
    static Path locateRawTable1Ddl() {
        Path dir = Path.of("").toAbsolutePath();
        for (int depth = 0; depth < 6; depth++) {
            Path candidate = dir.resolve("01_platform/02_sql/ddl/02_raw_table_1.sql");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            candidate = dir.resolve("code/01_platform/02_sql/ddl/02_raw_table_1.sql");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
            if (dir == null) {
                break;
            }
        }
        throw new IllegalStateException(
                "cannot locate 02_raw_table_1.sql under " + Path.of("").toAbsolutePath()
                        + " — expected code/01_platform/02_sql/ddl/02_raw_table_1.sql");
    }
}
