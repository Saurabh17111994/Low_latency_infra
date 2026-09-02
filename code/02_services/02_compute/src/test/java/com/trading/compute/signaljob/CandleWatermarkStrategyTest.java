package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.trading.common.schema.RawTableSchema;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

class CandleWatermarkStrategyTest {

    @Test
    void emitsBoundedWatermarksWhenNewEventTimeArrives() {
        WatermarkGenerator<RowData> generator = CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L);
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(null, 10_000L, output);
        generator.onEvent(null, 9_000L, output);
        generator.onEvent(null, 10_002L, output);
        generator.onPeriodicEmit(output);

        assertEquals(List.of(4_999L, 5_001L), output.timestamps);

        // The Fluss v3 source row gained event_day at index 0. Keep the
        // compute-side indexes tied to the shared schema so a future DDL
        // migration cannot silently feed symbol/string data to getLong().
        assertEquals(RawTableSchema.COLUMNS, Arrays.asList(RawTableColumns.NAMES));
        assertEquals(RawTableSchema.FIELD_COUNT, RawTableColumns.FIELD_COUNT);
        GenericRowData v3 = new GenericRowData(RawTableColumns.FIELD_COUNT);
        v3.setField(RawTableColumns.EVENT_DAY, StringData.fromString("20260901"));
        v3.setField(RawTableColumns.EVENT_TIME, 10_000L);
        assertEquals(10_000L, v3.getLong(RawTableColumns.EVENT_TIME));
        assertThrows(IllegalStateException.class,
                () -> RawTableColumns.validateSchemaContract(20, List.of("event_time")));
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final List<Long> timestamps = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            timestamps.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
