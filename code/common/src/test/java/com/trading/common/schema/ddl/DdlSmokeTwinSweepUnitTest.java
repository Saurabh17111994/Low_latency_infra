package com.trading.common.schema.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
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
}
