package com.trading.ingestion.telemetry;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P1-133 native follow-up (2026-09-07): the hand-rolled OTLP alert emitter is
 * deleted — heap-high / heap-recovered signals travel as ordinary LOG lines
 * via the javaagent (phase 1 pipeline) + the JSON file. This pins the
 * deletion: re-adding the class must be a conscious decision, not an accident
 * (revive this test's sibling OtlpAlertLogsTest with it).
 */
@DisplayName("Native alerts contract: no hand-rolled OTLP log path")
class NativeAlertsContractTest {

    @Test
    @DisplayName("OtlpAlertLogs stays deleted (agent + file own alert shipping)")
    void handRolledAlertEmitterStaysDeleted() {
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("com.trading.ingestion.telemetry.OtlpAlertLogs"),
                "OtlpAlertLogs was deleted in favour of agent log shipping; "
                        + "re-adding it must revive OtlpAlertLogsTest too");
    }
}
