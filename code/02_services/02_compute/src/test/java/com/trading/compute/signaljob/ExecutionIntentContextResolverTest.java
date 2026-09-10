package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

class ExecutionIntentContextResolverTest {

    @Test
    void sameAccountInstrumentAndCandidateProduceSameContext() {
        GenericRowData candidate = candidate("candidate-1", 123L);
        assertEquals(
                ExecutionIntentContextResolver.resolveEntry(candidate, "sandbox-account"),
                ExecutionIntentContextResolver.resolveEntry(candidate, "sandbox-account"));
    }

    @Test
    void accountInstrumentAndCandidateAreIdentityInputs() {
        GenericRowData candidate = candidate("candidate-1", 123L);
        String base = ExecutionIntentContextResolver.resolveEntry(candidate, "account-a");
        assertNotEquals(base,
                ExecutionIntentContextResolver.resolveEntry(candidate, "account-b"));
        assertNotEquals(base,
                ExecutionIntentContextResolver.resolveEntry(candidate("candidate-2", 123L), "account-a"));
        assertNotEquals(base,
                ExecutionIntentContextResolver.resolveEntry(candidate("candidate-1", 124L), "account-a"));
    }

    @Test
    void onlyEntryCandidatesCanMintMvpContext() {
        GenericRowData candidate = candidate("candidate-1", 123L);
        candidate.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString("EXIT"));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutionIntentContextResolver.resolveEntry(candidate, "account-a"));
    }

    @Test
    void nullTokenRejectsInsteadOfNpe() {
        // P2-033: null INSTRUMENT_TOKEN must be IAE (counted reject), never
        // NPE out of resolveEntry.
        GenericRowData candidate = candidate("candidate-1", 123L);
        candidate.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, null);
        assertThrows(IllegalArgumentException.class,
                () -> ExecutionIntentContextResolver.resolveEntry(candidate, "account-a"));
    }

    private static GenericRowData candidate(String id, long instrumentToken) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(id));
        row.setField(SignalCandidatesTableColumns.ACTION,
                StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, instrumentToken);
        return row;
    }
}
