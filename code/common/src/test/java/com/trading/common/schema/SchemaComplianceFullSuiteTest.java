package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Full-suite compliance coverage at the unit level (the parts that need no live
 * Fluss cluster). Each assertion maps to a schema/storage test ID from
 * docs/08_implementation/11-testing-and-release.md:
 *
 * <ul>
 *   <li>SCHEMA-UNIT-002 — non-null routing identity (routing skew flagged)</li>
 *   <li>SCHEMA-UNIT-003 — unknown/placeholder schema version blocks executable authority</li>
 *   <li>COMPAT-FLUSS-004 — stale / conflict KV updates rejected (halt on conflict)</li>
 *   <li>immutable LOG — duplicate dropped, mutation violation</li>
 *   <li>foundation L848 — every committed DDL entry carries ddl_sha256 + compatibility_class</li>
 * </ul>
 */
class SchemaComplianceFullSuiteTest {

  @Test
  void immutableDuplicateIsDropped() {
    String h = ImmutabilityProtocol.canonicalHash("v1");
    assertThat(ImmutabilityProtocol.evaluate(h, h)).isEqualTo(ImmutabilityProtocol.Outcome.DUPLICATE);
  }

  @Test
  void immutableMutationIsViolation() {
    assertThat(ImmutabilityProtocol.evaluate("h1", "h2")).isEqualTo(ImmutabilityProtocol.Outcome.VIOLATION);
  }

  @Test
  void kvStaleAndConflictAreRejectedWithHalt() {
    // stale
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.evaluate(5, 3, false))).isTrue();
    // conflict (same version, different content)
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.evaluate(5, 5, false))).isTrue();
  }

  @Test
  void routingSkewIsFlagged() {
    SchemaManifestEntry e = new SchemaManifestEntry();
    e.tableName = "l";
    e.tableKind = "LOG";
    e.bucketKey = "";
    assertThat(RoutingKeyRule.check(List.of(e))).isNotEmpty();
  }

  @Test
  void placeholderVersionBlocksExecutableAuthority() {
    assertThat(SchemaState.PROPOSED.isExecutableAuthority()).isFalse();
    assertThat(SchemaState.REJECTED.isExecutableAuthority()).isFalse();
    assertThat(SchemaState.OBSERVED.isExecutableAuthority()).isFalse();
  }

  @Test
  void everyManifestTableHasRoutingIdentity() throws Exception {
    // Full-manifest half of "non-null routing and bucket-skew tests": the
    // RoutingKeyRule unit path checks LOG-only, so this asserts the complete
    // committed set — every LOG table carries a non-null bucket.key and every
    // KV table carries a primary key (its default routing identity); a KV
    // entry that declares a bucket key must not declare it blank.
    Path manifestPath = Path.of("..", "01_platform", "02_sql", "ddl", "schema_manifest.json");
    SchemaManifest manifest =
        new ObjectMapper().readValue(Files.readAllBytes(manifestPath), SchemaManifest.class);
    List<String> violations = new java.util.ArrayList<>();
    for (SchemaManifestEntry e : manifest.tables) {
      if ("LOG".equalsIgnoreCase(e.tableKind)
          && (e.bucketKey == null || e.bucketKey.isBlank())) {
        violations.add(e.tableName + " (LOG) missing non-null bucket.key");
      }
      if ("KV".equalsIgnoreCase(e.tableKind)
          && (e.primaryKey == null || e.primaryKey.isBlank())) {
        violations.add(e.tableName + " (KV) missing primary key (default routing identity)");
      }
      if ("KV".equalsIgnoreCase(e.tableKind)
          && e.bucketKey != null && e.bucketKey.isBlank()) {
        violations.add(e.tableName + " (KV) declares an empty bucket.key");
      }
    }
    assertThat(violations)
        .as("every committed table must carry a routing identity")
        .isEmpty();
  }

  @Test
  void committedManifestCarriesChecksumsAndCompatibilityClasses() throws Exception {
    // Foundation L848: every DDL entry must have a checksum AND a compatibility
    // class; candle/signal tables ride the Fluss-Flink connector boundary.
    Path manifestPath = Path.of("..", "01_platform", "02_sql", "ddl", "schema_manifest.json");
    assertThat(Files.exists(manifestPath))
        .as("committed schema_manifest.json must exist at %s", manifestPath.toAbsolutePath())
        .isTrue();
    SchemaManifest manifest =
        new ObjectMapper().readValue(Files.readAllBytes(manifestPath), SchemaManifest.class);
    assertThat(manifest.tables).hasSize(31);
    Map<String, String> boundaries = new HashMap<>();
    for (SchemaManifestEntry e : manifest.tables) {
      assertThat(e.ddlSha256).as(e.tableName + " ddl_sha256").isNotBlank();
      assertThat(e.compatibilityClass).as(e.tableName + " compatibility_class").isNotBlank();
      assertThat(e.validatedMatrix).as(e.tableName + " validated_matrix").isNotBlank();
      boundaries.put(e.tableName, e.validatedMatrix);
    }
    assertThat(boundaries.get("feature_candles_15s")).isEqualTo("VM-FLUSS-CONN-007");
    // Low-latency candles Phase 1 (2026-08-29): preview table rides the same
    // Fluss-Flink connector boundary as the final candle table.
    assertThat(boundaries.get("feature_candles_15s_preview")).isEqualTo("VM-FLUSS-CONN-007");
    assertThat(boundaries.get("Signal_Candidates_current")).isEqualTo("VM-FLUSS-CONN-007");
    assertThat(boundaries.get("raw_table_1")).isEqualTo("VM-FLUSS-SRV-005");
  }
}
