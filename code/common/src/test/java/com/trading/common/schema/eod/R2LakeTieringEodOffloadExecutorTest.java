package com.trading.common.schema.eod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class R2LakeTieringEodOffloadExecutorTest {

    @Test
    void parsesDayAndManifestEvidence() {
        List<String> lines = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/instrument_token_bucket=0/x.parquet\t100",
                "lake/default/raw_table_1/data/event_day=20260831/instrument_token_bucket=1/y.parquet\t200",
                "lake/default/raw_table_1/data/event_day=20260830/z.parquet\t50",
                "lake/default/raw_table_1/metadata/event_day=20260831/snap-1.avro\t10");
        R2LakeTieringEodOffloadExecutor.DayEvidence e =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
        assertEquals(2, e.dataObjects());
        assertEquals(300, e.dataBytes());
        assertEquals(1, e.manifestFiles());
        assertTrue(e.keysHash().length() == 64);
    }

    @Test
    void staleManifestFromPriorDayDoesNotSatisfyNewDay() {
        List<String> lines = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/instrument_token_bucket=0/x.parquet\t100",
                "lake/default/raw_table_1/metadata/snap-1.avro\t10",
                "lake/default/raw_table_1/metadata/event_day=20260830/snap-0.avro\t10");
        R2LakeTieringEodOffloadExecutor.DayEvidence e =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
        assertEquals(1, e.dataObjects());
        assertEquals(0, e.manifestFiles());
    }

    @Test
    void malformedSizeLineFailsClosed() {
        List<String> lines = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/x.parquet\tnot-a-number");
        try {
            R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
            throw new AssertionError("expected IllegalStateException for malformed size");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("malformed r2-list.sh line"));
        }
    }

    @Test
    void negativeSizeLineFailsClosed() {
        List<String> lines = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/x.parquet\t-5");
        try {
            R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
            throw new AssertionError("expected IllegalStateException for negative size");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("malformed r2-list.sh line"));
        }
    }

    @Test
    void emptyDayIsZeroEvidence() {
        List<String> lines = List.of(
                "lake/default/raw_table_1/data/event_day=20260830/z.parquet\t50");
        R2LakeTieringEodOffloadExecutor.DayEvidence e =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
        assertEquals(0, e.dataObjects());
        assertEquals(0, e.manifestFiles());
    }

    @Test
    void hashIsOrderIndependentAndContentBound() {
        List<String> fwd = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/a.parquet\t100",
                "lake/default/raw_table_1/data/event_day=20260831/b.parquet\t200");
        List<String> rev = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/b.parquet\t200",
                "lake/default/raw_table_1/data/event_day=20260831/a.parquet\t100");
        List<String> resized = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/a.parquet\t100",
                "lake/default/raw_table_1/data/event_day=20260831/b.parquet\t201");
        String hFwd = R2LakeTieringEodOffloadExecutor
                .parseEvidence(fwd, "lake", "raw_table_1", "20260831").keysHash();
        String hRev = R2LakeTieringEodOffloadExecutor
                .parseEvidence(rev, "lake", "raw_table_1", "20260831").keysHash();
        String hResized = R2LakeTieringEodOffloadExecutor
                .parseEvidence(resized, "lake", "raw_table_1", "20260831").keysHash();
        assertEquals(hFwd, hRev);
        assertTrue(!hFwd.equals(hResized));
    }

    @Test
    void nonDefaultDatabaseIsScoped() {
        List<String> lines = List.of(
                "lake/prod/raw_table_1/data/event_day=20260831/x.parquet\t100");
        R2LakeTieringEodOffloadExecutor.DayEvidence e =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "prod", "raw_table_1", "20260831");
        assertEquals(1, e.dataObjects());
        R2LakeTieringEodOffloadExecutor.DayEvidence missed =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
        assertEquals(0, missed.dataObjects());
    }

    @Test
    void badCtorConfigFailsFast() {
        try {
            new R2LakeTieringEodOffloadExecutor(
                    java.nio.file.Path.of("relative/r2-list.sh"), "lake");
            throw new AssertionError("expected IllegalArgumentException for relative script");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("absolute"));
        }
        try {
            new R2LakeTieringEodOffloadExecutor(
                    java.nio.file.Path.of("/abs/r2-list.sh"), "lake/");
            // trailing slash is normalized, not rejected — must not throw
        } catch (IllegalArgumentException e) {
            throw new AssertionError("trailing-slash prefix must normalize, got: " + e.getMessage());
        }
    }
}
