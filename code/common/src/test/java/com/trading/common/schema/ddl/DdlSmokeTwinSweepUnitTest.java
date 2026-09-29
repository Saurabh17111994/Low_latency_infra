package com.trading.common.schema.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * CHG-100 unit half: the fixture signature and the twin naming contract of
 * {@link DdlApplyTool}, with no cluster required.
 *
 * <p>The live half (twin smoke leaves no fixtures, sweep detects/repairs) is in
 * {@link DdlSmokeTwinSweepTest}, gated on {@code FLUSS_BOOTSTRAP}.
 */
@Tag("integration")
@DisplayName("CHG-100: smoke fixture signature + twin naming (unit)")
class DdlSmokeTwinSweepUnitTest {

    @Test
    @DisplayName("fixture signature matches only all-default rows")
    void fixtureSignatureMatchesOnlyAllDefaultRows() {
        RowType rt = RowType.of(
                DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.INT(),
                DataTypes.DOUBLE(), DataTypes.BOOLEAN(), DataTypes.BYTES());
        Object[] values = new Object[rt.getFieldCount()];
        for (int i = 0; i < rt.getFieldCount(); i++) {
            values[i] = DdlApplyTool.defaultValue(rt.getFields().get(i).getType(), i);
        }
        InternalRow fixture = GenericRow.of(values);
        assertTrue(DdlApplyTool.isSmokeFixtureRow(fixture, rt),
                "all-default row is the fixture signature");

        InternalRow realish = GenericRow.of(BinaryString.fromString("real-0"),
                45L, 7, 1.0, true, new byte[] {1, 2});
        assertFalse(DdlApplyTool.isSmokeFixtureRow(realish, rt),
                "a real row must NOT match the fixture signature");

        InternalRow oneOff = GenericRow.of(BinaryString.fromString("smoke-0"),
                2L, 1, 1.0, true, new byte[] {1, 2});
        assertFalse(DdlApplyTool.isSmokeFixtureRow(oneOff, rt),
                "a single different field rejects the signature");

        InternalRow nullField = GenericRow.of(BinaryString.fromString("smoke-0"),
                null, 1, 1.0, true, new byte[] {1, 2});
        assertFalse(DdlApplyTool.isSmokeFixtureRow(nullField, rt),
                "a null field rejects the signature");

        assertFalse(DdlApplyTool.isSmokeFixtureRow(
                GenericRow.of(BinaryString.fromString("smoke-0")), rt),
                "arity mismatch rejects the signature");
    }

    @Test
    @DisplayName("twin naming carries the optional prefix")
    void twinNameCarriesOptionalPrefix() {
        assertEquals("smoke_twin_Execution_Intent",
                DdlApplyTool.smokeTwinName(null, "Execution_Intent"));
        assertEquals("scratch_smoke_twin_Execution_Intent",
                DdlApplyTool.smokeTwinName("scratch_", "Execution_Intent"));
    }

    @Test
    @DisplayName("serving gate returns as soon as the buckets are served")
    void servingGateReturnsWhenProbeCompletes() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DdlApplyTool.awaitServing("twin_x", () -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(0L);
        }, Duration.ofMillis(500));
        assertEquals(1, calls.get(), "a writable table is probed exactly once");
    }

    @Test
    @DisplayName("serving gate re-issues the probe and succeeds on a later attempt")
    void servingGateRetriesUntilServed() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Supplier<CompletableFuture<?>> probe = () -> {
            if (calls.incrementAndGet() < 3) {
                CompletableFuture<Long> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("no leader yet"));
                return failed;
            }
            return CompletableFuture.completedFuture(0L);
        };
        DdlApplyTool.awaitServing("twin_x", probe, Duration.ofSeconds(10));
        assertEquals(3, calls.get(), "the gate re-issues a read probe until the table is served");
    }

    @Test
    @DisplayName("serving gate fails closed, naming the table, when no leader ever arrives")
    void servingGateFailsClosedNamingTheTable() {
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> DdlApplyTool.awaitServing("ddlsmoke123_raw_table_1 (16 bucket(s))", () -> {
                    calls.incrementAndGet();
                    // Never completes: the bucket has no leader, so the probe cannot settle.
                    return new CompletableFuture<>();
                }, Duration.ofMillis(300)));
        assertTrue(e.getMessage().contains("ddlsmoke123_raw_table_1"), e.getMessage());
        assertTrue(e.getMessage().contains("not writable"), e.getMessage());
        assertEquals(1, calls.get(), "one bounded probe exhausts a 300 ms budget");
    }

    /**
     * P6-xxx (2026-09-29 gate step 9): {@code feature_values} declared
     * {@code MAP<INT, DOUBLE>} on 2026-09-27, and the smoke's type switch had no
     * MAP arm — the twin sweep died with "no smoke default for MAP" only in the
     * LIVE gate (the unit fixture used the six scalar types). This walks every
     * committed DDL so a new column type cannot reach the live drill uncovered:
     * the failure it prevents is a red certifying gate, not a code path.
     */
    @Test
    @DisplayName("every DDL column type has a smoke default that matches the fixture signature")
    void everyDdlColumnTypeHasASmokeDefaultMatchingTheSignature() throws Exception {
        Path ddlDir = resolveDdlDir();
        List<Path> sqls;
        try (var stream = Files.list(ddlDir)) {
            sqls = stream.filter(p -> p.getFileName().toString().endsWith(".sql"))
                    .sorted().toList();
        }
        assertFalse(sqls.isEmpty(), "no DDL files found under " + ddlDir);
        for (Path sql : sqls) {
            DdlText.ParsedDdl ddl = DdlText.parse(Files.readString(sql), sql.toString());
            DataType[] types = new DataType[ddl.columns().size()];
            Object[] values = new Object[ddl.columns().size()];
            for (int c = 0; c < ddl.columns().size(); c++) {
                types[c] = ddl.columns().get(c).type();
                values[c] = DdlApplyTool.defaultValue(types[c], c);
            }
            assertTrue(DdlApplyTool.isSmokeFixtureRow(GenericRow.of(values), RowType.of(types)),
                    sql.getFileName() + ": the all-default row must match the fixture signature");
        }
    }

    @Test
    @DisplayName("a MAP smoke default is one entry of the declared key/value types")
    void mapSmokeDefaultMatchesTheFixtureSignature() {
        DataType map = DataTypes.MAP(DataTypes.INT(), DataTypes.DOUBLE());
        RowType rt = RowType.of(DataTypes.BIGINT(), map);
        // The map is column 1, so the fixture default is key 2 / value 2.0 —
        // defaultValue() is index-parameterized and isSmokeFixtureRow recomputes
        // it with the column's index.
        Object[] values = {1L, DdlApplyTool.defaultValue(map, 1)};
        assertTrue(values[1] instanceof InternalMap, "MAP default must be an InternalMap");
        InternalMap built = (InternalMap) values[1];
        assertEquals(1, built.size(), "one deterministic entry");
        assertEquals(2, built.keyArray().getInt(0), "key follows the declared key type");
        assertEquals(2.0d, built.valueArray().getDouble(0), "value follows the declared value type");
        assertTrue(DdlApplyTool.isSmokeFixtureRow(GenericRow.of(values), rt),
                "the default map is the fixture signature for a MAP column");
        assertFalse(DdlApplyTool.isSmokeFixtureRow(
                        GenericRow.of(1L, new GenericMap(java.util.Map.of(2, 2.5d))), rt),
                "a different map must NOT match the fixture signature");
    }

    private static Path resolveDdlDir() throws Exception {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve("01_platform/02_sql/ddl/schema_manifest.json");
            if (Files.isRegularFile(candidate)) {
                return candidate.getParent();
            }
            current = current.getParent();
        }
        throw new IllegalStateException("cannot locate 01_platform/02_sql/ddl");
    }
}
