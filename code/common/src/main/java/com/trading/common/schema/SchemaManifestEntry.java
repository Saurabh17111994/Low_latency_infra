package com.trading.common.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * One table entry in the {@link SchemaManifest}.
 * Mirrors the manifest fields required by docs/08_implementation/01-foundation.md (orig L400).
 */
public class SchemaManifestEntry {

    @JsonProperty("table_name") public String tableName;
    @JsonProperty("schema_version") public String schemaVersion;
    @JsonProperty("ddl_path") public String ddlPath;
    @JsonProperty("ddl_sha256") public String ddlSha256;
    @JsonProperty("table_kind") public String tableKind;          // LOG | KV
    @JsonProperty("writer_owner") public String writerOwner;
    @JsonProperty("primary_key") public String primaryKey;
    @JsonProperty("bucket_key") public String bucketKey;          // non-null for LOG (routing identity)
    @JsonProperty("retention_policy") public String retentionPolicy;
    @JsonProperty("lake_policy") public String lakePolicy;
    @JsonProperty("compatibility_class") public String compatibilityClass;
    @JsonProperty("validated_matrix") public String validatedMatrix;  // version matrix id
    // R-267: typed — the same package already defines SchemaState
    // (PROPOSED/APPROVED/APPLYING/OBSERVED/REJECTED), whose values exactly
    // match the old inline comment; a raw String accepted any typo.
    // Missing/unknown/case-variant states default to PROPOSED so one bad
    // entry cannot fail the whole manifest load (P4-258).
    @JsonProperty("schema_state") public SchemaState schemaState = SchemaState.PROPOSED;

    /** Lenient setter: case-insensitive, unknown/blank values fall back to PROPOSED. */
    @JsonSetter("schema_state")
    public void setSchemaState(String raw) {
        if (raw == null || raw.isBlank()) {
            this.schemaState = SchemaState.PROPOSED;
            return;
        }
        try {
            this.schemaState = SchemaState.valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException unknown) {
            this.schemaState = SchemaState.PROPOSED;
        }
    }

    /** Fail-fast: every entry must name its table, DDL file, and checksum. */
    public void validate() {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("table_name is required");
        }
        if (ddlPath == null || ddlPath.isBlank()) {
            throw new IllegalArgumentException(tableName + ": ddl_path is required");
        }
        if (ddlSha256 == null || ddlSha256.isBlank()) {
            throw new IllegalArgumentException(tableName + ": ddl_sha256 is required");
        }
    }

    /**
     * Fail-fast routing check: kind must be LOG or KV (typos fail instead of
     * silently skipping), and LOG tables must carry a bucket key.
     * Kept as String (not enum) so one bad entry fails validation instead of
     * failing the whole manifest deserialization.
     */
    public void validateRouting() {
        if (tableKind == null || tableKind.isBlank()
                || (!"LOG".equalsIgnoreCase(tableKind.trim())
                    && !"KV".equalsIgnoreCase(tableKind.trim()))) {
            throw new IllegalArgumentException(
                    tableName + ": table_kind must be LOG or KV, got '" + tableKind + "'");
        }
        if ("LOG".equalsIgnoreCase(tableKind.trim())
                && (bucketKey == null || bucketKey.isBlank())) {
            throw new IllegalArgumentException(
                    tableName + ": LOG table missing non-null bucket.key");
        }
    }
}
