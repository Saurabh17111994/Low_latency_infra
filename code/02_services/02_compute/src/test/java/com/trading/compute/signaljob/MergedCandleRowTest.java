package com.trading.compute.signaljob;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Wave B (DEC-059): the merged candle+feature row model and the
 * sealed-never-rewritten rule. Pure — no cluster.
 */
class MergedCandleRowTest {

    private static ClosedCandle candle() {
        return new ClosedCandle(
                1_000L, 16_000L, 100L, 120L, 90L, 110L, 42L, 7L, 15_500L, "fp-1");
    }

    @Test
    void sealedRowsAreTerminal() {
        assertThat(MergedCandleRow.acceptsUpdate(false)).isTrue();
        assertThat(MergedCandleRow.acceptsUpdate(true)).isFalse();
    }

    @Test
    void sealedFactoryCarriesTheCandleColumnsFeaturesAndFlag() {
        Map<Integer, Double> features = new LinkedHashMap<>();
        features.put(1, 1.5);
        features.put(2, Double.NaN);
        MergedCandleRow row =
                MergedCandleRow.sealed(candle(), 999L, "NSE", "ACME", "ONE_M", features);

        assertThat(row.instrumentToken()).isEqualTo(999L);
        assertThat(row.exchange()).isEqualTo("NSE");
        assertThat(row.symbol()).isEqualTo("ACME");
        assertThat(row.tf()).isEqualTo("ONE_M");
        assertThat(row.windowStart()).isEqualTo(1_000L);
        assertThat(row.windowEnd()).isEqualTo(16_000L);
        assertThat(row.openPaise()).isEqualTo(100L);
        assertThat(row.highPaise()).isEqualTo(120L);
        assertThat(row.lowPaise()).isEqualTo(90L);
        assertThat(row.closePaise()).isEqualTo(110L);
        assertThat(row.volume()).isEqualTo(42L);
        assertThat(row.tickCount()).isEqualTo(7);
        assertThat(row.lastEventTime()).isEqualTo(15_500L);
        assertThat(row.lastEventFingerprint()).isEqualTo("fp-1");
        assertThat(row.schemaVersion()).isEqualTo(CandleClosedColumns.SCHEMA_VERSION_V1);
        assertThat(row.sealed()).isTrue();
        assertThat(row.features()).containsEntry(1, 1.5).containsEntry(2, Double.NaN);
    }

    @Test
    void featuresMapIsCopiedSoCallersCannotMutateARow() {
        Map<Integer, Double> features = new LinkedHashMap<>();
        features.put(1, 1.0);
        MergedCandleRow row =
                MergedCandleRow.forming(candle(), 1L, null, null, "FIFTEEN_S", features);
        features.put(2, 2.0);

        assertThat(row.features()).containsOnlyKeys(1);
        assertThatThrownBy(() -> row.features().put(3, 3.0))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void formingFactorySealsFalse() {
        MergedCandleRow row =
                MergedCandleRow.forming(candle(), 1L, "NSE", "ACME", "FIFTEEN_S", Map.of());
        assertThat(row.sealed()).isFalse();
    }
}
