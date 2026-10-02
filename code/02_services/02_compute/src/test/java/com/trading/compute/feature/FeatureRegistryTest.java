package com.trading.compute.feature;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.signaljob.Timeframe;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class FeatureRegistryTest {

    private static FeatureDef tickDef(int id, String name, FeatureStatus status) {
        return new FeatureDef(
                id, name, status, FeatureCadence.TICK, EnumSet.of(Timeframe.ONE_M),
                LastPriceComputer::new);
    }

    private static FeatureDef closeDef(int id, String name, FeatureStatus status) {
        return new FeatureDef(
                id, name, status, FeatureCadence.CLOSE, EnumSet.of(Timeframe.ONE_M),
                () -> new SmaComputer(2));
    }

    @Test
    void currentRegistryPassesValidation() {
        FeatureRegistry.validate(FeatureRegistry.all());
    }

    @Test
    void validatorRejectsIdGap() {
        assertThrows(IllegalStateException.class, () -> FeatureRegistry.validate(List.of(
                tickDef(0, "a", FeatureStatus.ACTIVE),
                tickDef(2, "b", FeatureStatus.ACTIVE))));
    }

    @Test
    void validatorRejectsDuplicateName() {
        assertThrows(IllegalStateException.class, () -> FeatureRegistry.validate(List.of(
                tickDef(0, "dup", FeatureStatus.ACTIVE),
                tickDef(1, "dup", FeatureStatus.ACTIVE))));
    }

    @Test
    void byNameResolvesRegisteredFeatures() {
        assertEquals(0, FeatureRegistry.byName("last_price").orElseThrow().id());
        assertTrue(FeatureRegistry.byName("nope").isEmpty());
    }

    @Test
    void declaredTimeframesRouteTickAndCloseUpdates() {
        assertTrue(FeatureRegistry.byId(0).serves(Timeframe.FIFTEEN_S));
        assertTrue(FeatureRegistry.byId(0).serves(Timeframe.FIFTEEN_M));
        assertFalse(FeatureRegistry.byId(2).serves(Timeframe.FIFTEEN_S));
        assertTrue(FeatureRegistry.byId(2).serves(Timeframe.ONE_M));
        // the ONE_M row carries last_price (tick) + sma + rsi (close) + the
        // 42 market features (MARKET, all timeframes; CHG-512)
        assertArrayEquals(
                IntStream.rangeClosed(0, 44).toArray(), FeatureRegistry.idsFor(Timeframe.ONE_M));
        // close-update routing carries only CLOSE features — market values are
        // fed from the snapshot, never from a close
        assertArrayEquals(new int[] {1, 2}, FeatureRegistry.closeIdsFor(Timeframe.ONE_M));
        assertArrayEquals(new int[] {1}, FeatureRegistry.closeIdsFor(Timeframe.THREE_M));
        assertArrayEquals(new int[] {}, FeatureRegistry.closeIdsFor(Timeframe.THIRTY_S));
    }

    @Test
    void layoutExcludesRetiredEntriesFromEveryRoute() {
        List<FeatureDef> defs = List.of(
                tickDef(0, "tick_a", FeatureStatus.ACTIVE),
                tickDef(1, "tick_retired", FeatureStatus.RETIRED),
                closeDef(2, "close_a", FeatureStatus.ACTIVE),
                closeDef(3, "close_retired", FeatureStatus.RETIRED));
        FeatureRegistry.validate(defs); // ids stay contiguous with a retired line in place
        FeatureRegistry.Routing routing = FeatureRegistry.layout(defs);
        assertArrayEquals(new int[] {0}, routing.tickIds(), "retired tick feature must not update");
        assertArrayEquals(
                new int[] {0, 2},
                routing.idsByTf()[Timeframe.ONE_M.ordinal()],
                "retired features must not be carried by new rows");
        assertArrayEquals(
                new int[] {2},
                routing.closeIdsByTf()[Timeframe.ONE_M.ordinal()],
                "retired close feature must not update");
    }
}
