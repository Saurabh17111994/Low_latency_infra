package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

/**
 * Pins the LIVE_TICK_TAG side-output serialization contract (2026-10-02): the
 * tag types as a generic {@link RowData} (Kryo), so the wire preserves the
 * full {@link CandleLiveColumns#FIELD_COUNT} arity — the market section
 * survives the edge. This is what makes a 16-column row on the wire
 * unambiguous: it was serialized by a pre-market-section job (an unaligned
 * checkpoint's in-flight record), never truncated by the serializer.
 */
class LiveSideOutputSerializationTest {

    @Test
    void sideOutputRoundTripKeepsArity() throws Exception {
        TypeInformation<RowData> ti = MultiTimeframeAggregateFunction.LIVE_TICK_TAG.getTypeInfo();
        System.out.println("LIVE_TICK_TAG typeInfo = " + ti.getClass().getName() + " :: " + ti);
        System.out.println("CandleLiveColumns.ROW_TYPE_INFO arity = " + CandleLiveColumns.ROW_TYPE_INFO.toRowSize());
        System.out.println("CandleClosedColumns.ROW_TYPE_INFO arity = " + CandleClosedColumns.ROW_TYPE_INFO.toRowSize());

        TypeSerializer<RowData> ser = ti.createSerializer(new SerializerConfigImpl());
        GenericRowData row = new GenericRowData(CandleLiveColumns.FIELD_COUNT);
        for (int i = 0; i < CandleLiveColumns.FIELD_COUNT; i++) {
            row.setField(i, null);
        }
        row.setField(CandleLiveColumns.INSTRUMENT_TOKEN, 42L);
        row.setField(CandleLiveColumns.TF, org.apache.flink.table.data.StringData.fromString("15S"));

        DataOutputSerializer out = new DataOutputSerializer(1024);
        ser.serialize(row, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RowData back = ser.deserialize(in);
        System.out.println("round-trip arity = " + back.getArity() + " class = " + back.getClass().getName());
        assertEquals(CandleLiveColumns.FIELD_COUNT, back.getArity(),
                "side output serializer must preserve the 60-field market section");
    }
}
