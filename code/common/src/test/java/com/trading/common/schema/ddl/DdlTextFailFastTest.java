package com.trading.common.schema.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Fail-fast parser contract: P4-007/106/107/265/266 (S6a.1). */
@DisplayName("DdlText fail-fast contract")
class DdlTextFailFastTest {

    private static String ddl(String body) {
        return "CREATE TABLE t (\n" + body + "\n) WITH (\n'bucket.num' = '1',\n'bucket.key' = 'a'\n)";
    }

    @Test
    @DisplayName("lowercase keywords and types parse (P4-106)")
    void lowercaseParses() {
        DdlText.ParsedDdl parsed = DdlText.parse(
                ddl("a bigint not null").replace("CREATE TABLE", "create table"), "p.sql");
        assertEquals("t", parsed.tableName());
        assertEquals(List.of(new DdlText.Column("a", DataTypes.BIGINT())), parsed.columns());
    }

    @Test
    @DisplayName("parameterized types map or fail with type name (P4-007)")
    void parameterizedTypes() {
        DdlText.ParsedDdl parsed = DdlText.parse(ddl("a DECIMAL(10,2) NOT NULL"), "p.sql");
        assertEquals(DataTypes.DECIMAL(10, 2), parsed.columns().get(0).type());
        assertThrows(IllegalArgumentException.class,
                () -> DdlText.parse(ddl("a WEIRDTYPE NOT NULL"), "p.sql"));
    }

    @Test
    @DisplayName("IF NOT EXISTS and quoted names (P4-106)")
    void ifNotExists() {
        DdlText.ParsedDdl parsed = DdlText.parse(
                "CREATE TABLE IF NOT EXISTS `t` (\na STRING NOT NULL\n) WITH (\n'bucket.num' = '1',\n'bucket.key' = 'a'\n)",
                "p.sql");
        assertEquals("t", parsed.tableName());
    }

    @Test
    @DisplayName("whitespace/case variants of WITH delimit (P4-107)")
    void withVariants() {
        String body = "CREATE TABLE t (\na STRING NOT NULL\n)\nWITH (\n'bucket.num' = '1',\n'bucket.key' = 'a'\n)";
        assertEquals(1, DdlText.parse(body, "p.sql").columns().size());
        String part = "CREATE TABLE t (\na STRING NOT NULL\n) PARTITIONED BY (a) WITH (\n'bucket.num' = '1',\n'bucket.key' = 'a'\n)";
        DdlText.ParsedDdl parsed = DdlText.parse(part, "p.sql");
        assertEquals(List.of("a"), parsed.partitionKeys());
    }

    @Test
    @DisplayName("key typos and duplicates fail at parse (P4-265)")
    void keyValidation() {
        assertThrows(IllegalArgumentException.class, () -> DdlText.parse(
                "CREATE TABLE t (\na STRING NOT NULL,\nPRIMARY KEY (oder_id) NOT ENFORCED\n) WITH (\n'bucket.num' = '1',\n'bucket.key' = 'a'\n)",
                "p.sql"));
        assertThrows(IllegalArgumentException.class,
                () -> DdlText.parse(ddl("a STRING NOT NULL,\na BIGINT NOT NULL"), "p.sql"));
    }

    @Test
    @DisplayName("bucket count positive and keys trimmed columns (P4-266)")
    void bucketValidation() {
        assertThrows(IllegalArgumentException.class, () -> DdlText.parse(
                ddl("a STRING NOT NULL").replace("'bucket.num' = '1'", "'bucket.num' = '0'"),
                "p.sql"));
        DdlText.ParsedDdl parsed = DdlText.parse(
                "CREATE TABLE t (\na STRING NOT NULL,\nb STRING NOT NULL\n) WITH (\n'bucket.num' = '1',\n'bucket.key' = 'a, b '\n)",
                "p.sql");
        assertEquals("a,b", parsed.bucketKey());
    }

    @Test
    @DisplayName("datalake deviation is opt-out (P4-267)")
    void datalakeParam() {
        DdlText.ParsedDdl parsed = DdlText.parse(
                "CREATE TABLE t (\na STRING NOT NULL\n) WITH (\n'bucket.num' = '1',\n'bucket.key' = 'a',\n'table.datalake.enabled' = 'true'\n)",
                "p.sql");
        assertEquals("false",
                DdlText.toDescriptor(parsed).getProperties().get("table.datalake.enabled"));
        assertEquals("true", DdlText.toDescriptor(parsed, false).getProperties()
                .get("table.datalake.enabled"));
    }
}
