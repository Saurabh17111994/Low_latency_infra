package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the routing-identity rule (LOG tables route by `bucket.key`). */
class RoutingKeyRuleTest {

  private SchemaManifestEntry entry(String kind, String bucketKey, String pk) {
    SchemaManifestEntry e = new SchemaManifestEntry();
    e.tableName = "t";
    e.tableKind = kind;
    e.bucketKey = bucketKey;
    e.primaryKey = pk;
    return e;
  }

  @Test
  void logTableWithoutBucketKeyIsViolation() {
    var v = RoutingKeyRule.check(List.of(entry("LOG", "  ", null)));
    assertThat(v).hasSize(1);
    assertThat(v.get(0).tableName).isEqualTo("t");
    assertThat(v.get(0).reason).contains("bucket.key");
  }

  @Test
  void logTableWithBucketKeyIsClean() {
    assertThat(RoutingKeyRule.check(List.of(entry("LOG", "instrument_id", null)))).isEmpty();
  }

  @Test
  void kvTableNeedsNoBucketKey() {
    // KV (primary-key) tables route by primary key, not bucket.key.
    assertThat(RoutingKeyRule.check(List.of(entry("KV", null, "id")))).isEmpty();
  }

  @Test
  void nullInputAndNullElementsAreSafe() {
    assertThat(RoutingKeyRule.check(null)).isEmpty();
    var v = RoutingKeyRule.check(java.util.Arrays.asList(entry("LOG", "k", null), null));
    assertThat(v).hasSize(1);
    assertThat(v.get(0).reason).contains("null schema entry");
  }

  @Test
  void manifestEntryValidationFailsFast() {
    SchemaManifestEntry e = new SchemaManifestEntry();
    e.tableName = "t";
    assertThatThrownBy(e::validate).isInstanceOf(IllegalArgumentException.class);
    e.ddlPath = "x.sql";
    assertThatThrownBy(e::validate).isInstanceOf(IllegalArgumentException.class);
    e.ddlSha256 = "abc";
    e.validate();
    e.tableKind = "LOGG";
    assertThatThrownBy(e::validateRouting).isInstanceOf(IllegalArgumentException.class);
    e.tableKind = "LOG";
    e.bucketKey = "  ";
    assertThatThrownBy(e::validateRouting).isInstanceOf(IllegalArgumentException.class);
    e.bucketKey = "k";
    e.validateRouting();
  }

  @Test
  void schemaStateLenient() {
    SchemaManifestEntry e = new SchemaManifestEntry();
    assertThat(e.schemaState).isEqualTo(SchemaState.PROPOSED);
    e.setSchemaState("approved");
    assertThat(e.schemaState).isEqualTo(SchemaState.APPROVED);
    e.setSchemaState("bogus-state");
    assertThat(e.schemaState).isEqualTo(SchemaState.PROPOSED);
    e.setSchemaState(null);
    assertThat(e.schemaState).isEqualTo(SchemaState.PROPOSED);
  }
}
