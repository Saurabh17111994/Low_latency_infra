package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Step-2 per-tick latency monitor: identity pass-through + histogram update. */
class IngestLatencyMonitorFunctionTest {

    private OneInputStreamOperatorTestHarness<RowData, RowData> harness;

    @BeforeEach
    void openHarness() throws Exception {
        IngestLatencyMonitorFunction fn = new IngestLatencyMonitorFunction();
        harness = new OneInputStreamOperatorTestHarness<>(
                new org.apache.flink.streaming.api.operators.StreamMap<>(fn));
        harness.open();
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void forwardsEveryRowUnchanged() throws Exception {
        GenericRowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 1);
        TestRawRows.withIngestTs(row, System.currentTimeMillis() - 50);

        harness.processElement(row, 0);

        assertEquals(1, harness.getOutput().size());
        RowData out = ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<RowData>)
                harness.getOutput().peek()).getValue();
        assertEquals("fp-1", out.getString(RawTableColumns.EVENT_FINGERPRINT).toString());
        assertEquals(100L, out.getLong(RawTableColumns.LAST_PRICE_PAISE));
        // Identity: the same row object must pass through untouched.
        assertEquals(row, out);
    }

    @Test
    void skipsNullIngestTsWithoutFailing() throws Exception {
        // TestRawRows defaults ingest_ts to null — a row without it must pass
        // through (monitor is observability-only, never a failure point).
        GenericRowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-2", "TRADE", 101, 1);
        assertNotNull(row);

        harness.processElement(row, 0);

        assertEquals(1, harness.getOutput().size());
    }
}
