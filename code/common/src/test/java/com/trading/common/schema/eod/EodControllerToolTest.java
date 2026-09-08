package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** EOD controller CLI unit tests (SCH-23): option parsing + pure helpers. */
class EodControllerToolTest {

    @Test
    void defaultScopeIsTheDocumentedTen2dTables() {
        assertThat(EodControllerTool.DEFAULT_TABLES).hasSize(10)
                .contains("raw_table_1", "feature_candles_15s", "Trade_Decisions");
    }

    @Test
    void parseDefaultsStateTableAndZone() {
        EodControllerTool.Options opts = EodControllerTool.Options.parse(
                new String[] {"status"});
        assertThat(opts.subcommand()).isEqualTo("status");
        assertThat(opts.stateTable()).isEqualTo("eod_offload_state");
        assertThat(opts.zone()).isEqualTo("Asia/Kolkata");
        assertThat(opts.tables()).containsExactlyElementsOf(EodControllerTool.DEFAULT_TABLES);
        assertThat(opts.safetyFloor()).isEqualTo(Duration.ofDays(7));
        assertThat(opts.extension()).isEqualTo(Duration.ofDays(30));
        assertThat(opts.offloadMode()).isEqualTo("none");
    }

    @Test
    void parseHonorsTablesAndDurations() {
        EodControllerTool.Options opts = EodControllerTool.Options.parse(new String[] {
                "run", "--tables", "raw_table_1,feature_candles_15s",
                "--safety-floor", "3d", "--extension", "45d",
                "--lease-ttl", "5m", "--run-date", "2026-08-14", "--offload", "mock"
        });
        assertThat(opts.tables()).containsExactly("raw_table_1", "feature_candles_15s");
        assertThat(opts.safetyFloor()).isEqualTo(Duration.ofDays(3));
        assertThat(opts.extension()).isEqualTo(Duration.ofDays(45));
        assertThat(opts.leaseTtl()).isEqualTo(Duration.ofMinutes(5));
        assertThat(opts.runDate()).isEqualTo("2026-08-14");
        assertThat(opts.offloadMode()).isEqualTo("mock");
    }

    @Test
    void parseRejectsUnknownOptionsAndBadValues() {
        assertThatThrownBy(() -> EodControllerTool.Options.parse(new String[] {"status", "--bogus"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown option");
        assertThatThrownBy(() -> EodControllerTool.Options.parse(
                new String[] {"run", "--offload", "real"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offload");
        assertThatThrownBy(() -> EodControllerTool.Options.parse(
                new String[] {"run", "--run-date", "14-08-2026"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("run-date");
        assertThatThrownBy(() -> EodControllerTool.Options.parse(
                new String[] {"run", "--safety-floor", "bogus"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttl");
    }

    @Test
    void ttlOptionRendersWholeUnits() {
        assertThat(EodControllerTool.ttlOption(Duration.ofDays(32))).isEqualTo("32d");
        assertThat(EodControllerTool.ttlOption(Duration.ofDays(2))).isEqualTo("2d");
        assertThat(EodControllerTool.ttlOption(Duration.ofHours(36))).isEqualTo("36h");
        assertThat(EodControllerTool.ttlOption(Duration.ofMinutes(90))).isEqualTo("90m");
        assertThat(EodControllerTool.ttlOption(Duration.ofSeconds(15))).isEqualTo("15000ms");
    }

    @Test
    void trailingFlagIsUsageErrorNotAioobe() {
        // P4-274/283: `run --bootstrap` with no value must name the flag
        // (caught as exit-4 usage), not ArrayIndexOutOfBoundsException.
        assertThatThrownBy(() -> EodControllerTool.Options.parse(
                new String[] {"run", "--bootstrap"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--bootstrap");
    }

    @Test
    void badParseSurfacesAsUsageExit4() throws Exception {
        // P4-276/280: run() maps parse failures to usage() (exit 4).
        assertThat(EodControllerTool.run(new String[] {"run", "--bogus"})).isEqualTo(4);
        assertThat(EodControllerTool.run(new String[] {})).isEqualTo(4);
    }
}
