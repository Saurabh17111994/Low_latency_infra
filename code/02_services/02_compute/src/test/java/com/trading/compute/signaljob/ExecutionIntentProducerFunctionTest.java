package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class ExecutionIntentProducerFunctionTest {

    @Test
    void validCandidateProducesImmutableIntentWithResolvedContext() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(enabledEnv());
        ExecutionIntentProducerFunction function = new ExecutionIntentProducerFunction(config);
        List<RowData> output = new ArrayList<>();

        function.flatMap(candidate("candidate-1", "VALID", "ENTRY"), collector(output));

        assertEquals(1, output.size());
        RowData intent = output.get(0);
        assertEquals("sandbox-account", intent.getString(
                ExecutionIntentTableColumns.ACCOUNT_SCOPE_ID).toString());
        assertEquals("partition-0", intent.getString(
                ExecutionIntentTableColumns.EXECUTION_PARTITION_ID).toString());
        assertNotNull(intent.getString(ExecutionIntentTableColumns.INSTRUCTION_ID));
        assertNotNull(intent.getString(ExecutionIntentTableColumns.REQUEST_HASH));
        assertNotNull(intent.getString(ExecutionIntentTableColumns.TRADE_CONTEXT_ID));
    }

    @Test
    void invalidCandidateProducesNoExecutableOutput() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(enabledEnv());
        ExecutionIntentProducerFunction function = new ExecutionIntentProducerFunction(config);
        List<RowData> output = new ArrayList<>();

        function.flatMap(candidate("candidate-1", "INVALID", "ENTRY"), collector(output));
        function.flatMap(candidate("candidate-2", "VALID", "EXIT"), collector(output));

        assertEquals(0, output.size());
    }

    private static Collector<RowData> collector(List<RowData> output) {
        return new Collector<>() {
            @Override
            public void collect(RowData row) {
                output.add(row);
            }

            @Override
            public void close() {}
        };
    }

    private static Map<String, String> enabledEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        env.put("EXECUTION_INTENT_ENABLED", "true");
        env.put("ACCOUNT_SCOPE_ID", "sandbox-account");
        env.put("EXECUTION_PARTITION_ID", "partition-0");
        env.put("EXECUTION_PRODUCT_TYPE", "CNC");
        env.put("EXECUTION_TIME_IN_FORCE", "DAY");
        env.put("CONFIGURATION_VERSION", "1.0.0");
        return env;
    }

    private static GenericRowData candidate(String id, String validity, String action) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(id));
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, 123L);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString("ABC"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString("strategy-1"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString("1.0.0"));
        row.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString(action));
        row.setField(SignalCandidatesTableColumns.SIDE,
                StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, 1L);
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE,
                StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, 1_752_000_000_000L);
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON, StringData.fromString(validity));
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }
}
