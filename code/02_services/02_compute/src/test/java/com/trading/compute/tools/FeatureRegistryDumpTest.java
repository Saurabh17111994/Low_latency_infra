package com.trading.compute.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.feature.FeatureDef;
import com.trading.compute.feature.FeatureRegistry;
import org.junit.jupiter.api.Test;

class FeatureRegistryDumpTest {

    @Test
    void pinLinesCoverEveryRegisteredFeature() {
        String pins = String.join("\n", FeatureRegistryDump.pinLines());
        for (FeatureDef def : FeatureRegistry.all()) {
            assertTrue(
                    pins.contains(def.id() + "\t" + def.name()),
                    "pin dump misses " + def.id() + "\t" + def.name());
        }
    }

    @Test
    void fullDumpCarriesStatusCadenceAndTimeframes() {
        String dump = FeatureRegistryDump.fullDump();
        for (FeatureDef def : FeatureRegistry.all()) {
            assertTrue(dump.contains(def.id() + "\t" + def.name()), def.name());
            assertTrue(dump.contains(def.status().name()), def.name());
            assertTrue(dump.contains(def.cadence().name()), def.name());
        }
    }
}
