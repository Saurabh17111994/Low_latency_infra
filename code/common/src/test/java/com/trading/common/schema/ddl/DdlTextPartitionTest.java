package com.trading.common.schema.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Regression tests for preserving SQL partitioning in the Fluss admin descriptor. */
@DisplayName("DdlText partition preservation")
class DdlTextPartitionTest {

    @Test
    @DisplayName("parses and applies PARTITIONED BY keys")
    void parsesAndAppliesPartitionKeys() {
        String ddl = """
                CREATE TABLE raw_table_1 (
                    event_day STRING NOT NULL,
                    event_id STRING NOT NULL
                ) PARTITIONED BY (event_day) WITH (
                    'bucket.num' = '4',
                    'bucket.key' = 'event_id',
                    'table.auto-partition.enabled' = 'true'
                )
                """;

        DdlText.ParsedDdl parsed = DdlText.parse(ddl, "raw_table_1.sql");
        assertEquals(List.of("event_day"), parsed.partitionKeys());

        TableDescriptor descriptor = DdlText.toDescriptor(parsed);
        assertTrue(descriptor.isPartitioned());
        assertEquals(List.of("event_day"), descriptor.getPartitionKeys());
    }

    @Test
    @DisplayName("synthetic seven-argument fixtures remain unpartitioned")
    void legacyFixtureConstructorRemainsUnpartitioned() {
        DdlText.ParsedDdl parsed = new DdlText.ParsedDdl(
                "t", List.of(new DdlText.Column("id", DataTypes.STRING())), List.of(), 1,
                "id", Map.of(), "fixture.sql");
        assertEquals(List.of(), parsed.partitionKeys());
        assertEquals(List.of(), DdlText.toDescriptor(parsed).getPartitionKeys());
    }
}
