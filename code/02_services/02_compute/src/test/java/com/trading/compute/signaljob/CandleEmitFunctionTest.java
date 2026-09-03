package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Candle emission contract (CANDLE-KV-REPLAY-001 A2): every emitted candle row
 * must carry the pinned algorithm/configuration/schema versions (REQ-FC-001),
 * the window identity, and the business OHLCV fields — the values the
 * {@code CanonicalCandlePolicy} and the KV projection rely on.
 */
@DisplayName("CandleEmitFunction.buildRow carries pinned versions and window identity")
class CandleEmitFunctionTest {

    private static final long T0 = 1_750_000_000_000L;

    @Test
    @DisplayName("row carries config algorithm/configuration/schema versions and window identity")
    void rowCarriesPinnedVersionsAndWindowIdentity() {
        SignalJobConfig config = SignalJobConfig.from(env());

        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = 100;
        acc.highPaise = 110;
        acc.lowPaise = 99;
        acc.closePaise = 105;
        acc.volume = 25;
        acc.tickCount = 7;

        TimeWindow window = new TimeWindow(T0, T0 + 15_000L);
        GenericRowData row = CandleEmitFunction.buildRow(2885L, acc, window, 1_750_000_010_000L, config);

        assertEquals(15, row.getArity(), "row must have the shared 15-column layout");
        assertEquals(2885L, row.getLong(CandleTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", row.getString(CandleTableColumns.EXCHANGE).toString());
        assertEquals("TEST", row.getString(CandleTableColumns.SYMBOL).toString());
        assertEquals(T0, row.getLong(CandleTableColumns.WINDOW_START));
        assertEquals(T0 + 15_000L, row.getLong(CandleTableColumns.WINDOW_END));
        assertEquals(100L, row.getLong(CandleTableColumns.OPEN_PAISE));
        assertEquals(110L, row.getLong(CandleTableColumns.HIGH_PAISE));
        assertEquals(99L, row.getLong(CandleTableColumns.LOW_PAISE));
        assertEquals(105L, row.getLong(CandleTableColumns.CLOSE_PAISE));
        assertEquals(25L, row.getLong(CandleTableColumns.VOLUME));
        assertEquals(7, row.getInt(CandleTableColumns.TICK_COUNT));

        // The version-carrying contract (CanonicalCandlePolicy input):
        assertEquals(config.algorithmVersion(),
                row.getString(CandleTableColumns.ALGORITHM_VERSION).toString());
        assertEquals(config.configurationVersion(),
                row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString());
        assertEquals(config.candleSchemaVersion(),
                row.getString(CandleTableColumns.SCHEMA_VERSION).toString());
        assertEquals(1_750_000_010_000L, row.getLong(CandleTableColumns.OUTPUT_TS));
    }

    @Test
    @DisplayName("emitted version values are canonical per the policy")
    void emittedVersionsAreCanonical() {
        SignalJobConfig config = SignalJobConfig.from(env());
        assertTrue(CanonicalCandlePolicy.isCanonical(
                config.algorithmVersion(), config.configurationVersion(),
                config.algorithmVersion(), config.configurationVersion()),
                "the versions the job emits must be exactly the canonical expected values");
    }

    @Test
    @DisplayName("emit half: window state is only the Boolean emitted flag — no candle payload, no collection")
    void windowStateHoldsOnlyBooleanEmittedFlag() throws Exception {
        Field f = CandleEmitFunction.class.getDeclaredField("EMITTED_DESCRIPTOR");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        ValueStateDescriptor<Boolean> descriptor = (ValueStateDescriptor<Boolean>) f.get(null);

        assertEquals("candle-emitted", descriptor.getName(),
                "window state key must be the emitted flag, not a candle payload key");

        // Initialize the way the window operator does, then assert the state
        // type is a bare Boolean and nothing else.
        descriptor.initializeSerializerUnlessSet(new ExecutionConfig());
        TypeSerializer<Boolean> serializer = descriptor.getSerializer();
        assertTrue(serializer instanceof BooleanSerializer,
                "the only window state is a Boolean flag — a Boolean cannot carry candle rows, "
                        + "raw bytes, or event collections (SIG-UNIT-008/009 emit half); was "
                        + serializer.getClass().getName());
    }

    @Test
    @DisplayName("emit half: active candle state holds only OHLCV + identity + order keys — no tick list/collection/raw bytes")
    void accumulatorStateHoldsOnlyCompactScalarFields() {
        List<String> fieldNames = Arrays.stream(CandleAccumulator.class.getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .map(Field::getName)
                .sorted()
                .toList();
        assertEquals(List.of(
                        "closePaise", "exchange", "firstEventTime", "firstFingerprint",
                        "highPaise", "lastEventTime", "lastFingerprint", "lastIngestTs", "lowPaise",
                        "openPaise", "symbol", "tickCount", "volume"),
                fieldNames,
                "active candle state must be exactly the compact scalar field set — adding a tick "
                        + "list/collection breaks the no-tick-collection rule (SIG-UNIT-009); "
                        + "lastIngestTs is the deliberate step-2 latency probe (2026-09-03) — a "
                        + "scalar long, observability only, never an output column");

        for (Field f : CandleAccumulator.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            Class<?> type = f.getType();
            assertFalse(type.isArray(), f.getName() + " must not be an array");
            assertFalse(Collection.class.isAssignableFrom(type),
                    f.getName() + " must not be a collection (no tick list/raw event collection)");
            assertFalse(Map.class.isAssignableFrom(type), f.getName() + " must not be a map");
            assertTrue(type.isPrimitive() || type == String.class,
                    f.getName() + " must be a scalar primitive or String, was " + type.getName());
        }
    }

    /** Same 5-key baseline as SignalJobConfigTest; tuning keys take defaults. */
    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }
}
