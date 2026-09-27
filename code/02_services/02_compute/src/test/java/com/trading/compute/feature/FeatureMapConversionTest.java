package com.trading.compute.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkRowToFlussRowConverter;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

/**
 * DEC-056 feature storage, connector half: the feature writer will carry feature rows as
 * Flink {@code RowData} through {@code FlussSink} + {@code RowDataSerializationSchema}, so the
 * candidate feature column types (MAP keyed by name or registry id, ARRAY + version) must
 * survive the Flink -&gt; Fluss conversion on the pinned Fluss 1.0.0 connector.
 *
 * <p>No cluster is involved: {@link FlinkRowToFlussRowConverter} is built from the Flink row
 * type alone, which is the same per-field conversion the sink serializer performs. A connector
 * upgrade that drops MAP/ARRAY support fails here instead of at the first feature write.
 *
 * <p>JSON needs no conversion coverage: the JSON candidate is a STRING column, already covered
 * by every existing STRING sink in the job.
 */
class FeatureMapConversionTest {

    private static final RowType MAP_STR_ROW = RowType.of(
            new LogicalType[] {
                new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(),
                new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DoubleType())
            },
            new String[] {"instrument_token", "tf", "window_start", "features"});

    private static final RowType MAP_INT_ROW = RowType.of(
            new LogicalType[] {
                new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(),
                new MapType(new IntType(), new DoubleType())
            },
            new String[] {"instrument_token", "tf", "window_start", "features"});

    private static final RowType ARRAY_ROW = RowType.of(
            new LogicalType[] {
                new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(),
                new ArrayType(new DoubleType()),
                new BigIntType()
            },
            new String[] {"instrument_token", "tf", "window_start", "features", "feature_version"});

    @Test
    void flinkMapTypeConvertsToFlussMapType() {
        assertEquals(
                org.apache.fluss.types.DataTypes.MAP(
                        org.apache.fluss.types.DataTypes.STRING(),
                        org.apache.fluss.types.DataTypes.DOUBLE()),
                FlinkConversions.toFlussType(org.apache.flink.table.api.DataTypes.MAP(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        org.apache.flink.table.api.DataTypes.DOUBLE())));
    }

    @Test
    void rowDataWithNameKeyedMapConvertsToFlussMap() throws Exception {
        Map<StringData, Double> features = new HashMap<>();
        features.put(StringData.fromString("feature_000"), 1.25);
        features.put(StringData.fromString("feature_001"), -2.5);
        GenericRowData row = GenericRowData.of(
                42L, StringData.fromString("FIFTEEN_S"), 1_790_000_000_000L,
                new GenericMapData(features));

        try (FlinkRowToFlussRowConverter converter = FlinkRowToFlussRowConverter.create(MAP_STR_ROW)) {
            InternalRow fluss = converter.toInternalRow(row);
            InternalMap map = fluss.getMap(3);
            assertNotNull(map);
            assertEquals(2, map.size());
            assertEquals(
                    Map.of("feature_000", 1.25, "feature_001", -2.5),
                    keysToMap(map));
        }
    }

    @Test
    void rowDataWithIdKeyedMapConvertsToFlussMap() throws Exception {
        Map<Integer, Double> features = new HashMap<>();
        features.put(0, 1.25);
        features.put(1, -2.5);
        GenericRowData row = GenericRowData.of(
                42L, StringData.fromString("ONE_M"), 1_790_000_000_000L, new GenericMapData(features));

        try (FlinkRowToFlussRowConverter converter = FlinkRowToFlussRowConverter.create(MAP_INT_ROW)) {
            InternalRow fluss = converter.toInternalRow(row);
            InternalMap map = fluss.getMap(3);
            assertNotNull(map);
            assertEquals(2, map.size());
            InternalArray keys = map.keyArray();
            InternalArray values = map.valueArray();
            assertEquals(0, keys.getInt(0));
            assertEquals(1.25, values.getDouble(0));
            assertEquals(1, keys.getInt(1));
            assertEquals(-2.5, values.getDouble(1));
        }
    }

    @Test
    void rowDataWithArrayConvertsToFlussArray() throws Exception {
        GenericRowData row = GenericRowData.of(
                42L, StringData.fromString("THREE_M"), 1_790_000_000_000L,
                new GenericArrayData(new double[] {1.25, -2.5, 0.0}), 1L);

        try (FlinkRowToFlussRowConverter converter = FlinkRowToFlussRowConverter.create(ARRAY_ROW)) {
            InternalRow fluss = converter.toInternalRow(row);
            InternalArray array = fluss.getArray(3);
            assertNotNull(array);
            assertEquals(3, array.size());
            assertEquals(1.25, array.getDouble(0));
            assertEquals(-2.5, array.getDouble(1));
            assertEquals(0.0, array.getDouble(2));
            assertEquals(1L, fluss.getLong(4));
        }
    }

    private static Map<String, Double> keysToMap(InternalMap map) {
        Map<String, Double> out = new HashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < map.size(); i++) {
            out.put(keys.getString(i).toString(), values.getDouble(i));
        }
        return out;
    }
}
