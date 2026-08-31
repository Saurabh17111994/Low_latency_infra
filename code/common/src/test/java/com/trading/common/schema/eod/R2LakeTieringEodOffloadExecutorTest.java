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
                "lake/default/raw_table_1/metadata/snap-1.avro\t10");
        R2LakeTieringEodOffloadExecutor.DayEvidence e =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
        assertEquals(2, e.dataObjects());
        assertEquals(300, e.dataBytes());
        assertEquals(1, e.manifestFiles());
        assertTrue(e.keysHash().length() == 64);
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
}
