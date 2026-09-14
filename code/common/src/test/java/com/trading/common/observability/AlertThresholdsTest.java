package com.trading.common.observability;

import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The percentage rows publish the numbers their consumers enforce, so the
 * condition text and the setpoint cannot drift apart (P6-666).
 */
@DisplayName("Alert threshold rows")
class AlertThresholdsTest {

    @Test
    void percentageConditionsAreDerivedFromTheSharedNumbers() {
        assertEquals(85, AlertThresholds.CONTAINER_MEMORY_ALERT_PERCENT);
        assertEquals(80, AlertThresholds.PENDING_APPEND_WARNING_PERCENT);
        assertThat(AlertThresholds.Alert.CONTAINER_MEMORY.condition)
                .isEqualTo("container_memory_pct >= "
                        + AlertThresholds.CONTAINER_MEMORY_ALERT_PERCENT);
        assertThat(AlertThresholds.Alert.PENDING_APPEND_RECORDS.condition)
                .isEqualTo("pending_append_records >= "
                        + AlertThresholds.PENDING_APPEND_WARNING_PERCENT + "% of limit");
        assertThat(AlertThresholds.Alert.PENDING_APPEND_BYTES.condition)
                .isEqualTo("pending_append_bytes >= "
                        + AlertThresholds.PENDING_APPEND_WARNING_PERCENT + "% of limit");
        assertEquals(60, AlertThresholds.CONSECUTIVE_BREACH_SECONDS);
    }

    @Test
    void everyRowHasAConditionAndADistinctName() {
        for (AlertThresholds.Alert alert : AlertThresholds.Alert.values()) {
            assertThat(alert.condition).isNotBlank();
        }
        long distinct = Arrays.stream(AlertThresholds.Alert.values()).map(Enum::name).distinct().count();
        assertEquals(AlertThresholds.Alert.values().length, distinct);
    }
}
