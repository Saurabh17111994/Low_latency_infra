package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Guards for P1-054 (deep schema compare), P1-055 (AlreadyExists), P1-056 (type map), P1-057 (LOG twins). */
@DisplayName("DdlBootstrap: schema compare, create race, type map, table kinds")
class DdlBootstrapTest {

    @Test
    @DisplayName("P1-056: INT and BOOLEAN map; unknown roots still fail fast")
    void typeMapCoversEvolvedRoots() {
        assertEquals(DataTypes.INT(), DdlBootstrap.toFlussType("INT"));
        assertEquals(DataTypes.BOOLEAN(), DdlBootstrap.toFlussType("BOOLEAN"));
        assertEquals(DataTypes.STRING(), DdlBootstrap.toFlussType("STRING"));
        assertEquals(DataTypes.BIGINT(), DdlBootstrap.toFlussType("BIGINT"));
        assertEquals(DataTypes.BYTES(), DdlBootstrap.toFlussType("BYTES"));
        assertThrows(IllegalStateException.class, () -> DdlBootstrap.toFlussType("TIMESTAMP_LTZ"));
    }

    @Test
    @DisplayName("P1-055: AlreadyExists anywhere in the cause chain counts as exists")
    void alreadyExistsWalksChain() {
        assertTrue(DdlBootstrap.isAlreadyExists(new ExecutionException(
                new RuntimeException("Table default.raw_table_1 already exists"))));
        assertTrue(DdlBootstrap.isAlreadyExists(
                new RuntimeException("ALREADY EXISTED concurrently")));
        assertFalse(DdlBootstrap.isAlreadyExists(
                new ExecutionException(new RuntimeException("connection refused"))));
        assertFalse(DdlBootstrap.isAlreadyExists(new RuntimeException()));
    }

    @Test
    @DisplayName("P1-216: typed Fluss AlreadyExist exceptions count as exists even without message")
    void alreadyExistsTypedExceptions() {
        assertTrue(DdlBootstrap.isAlreadyExists(
                new org.apache.fluss.exception.DatabaseAlreadyExistException("db")));
        assertTrue(DdlBootstrap.isAlreadyExists(new ExecutionException(
                new org.apache.fluss.exception.TableAlreadyExistException("t"))));
        // nested under a retryable-looking wrapper, like a concurrent create
        assertTrue(DdlBootstrap.isAlreadyExists(new ExecutionException(
                new RuntimeException("connection refused",
                        new org.apache.fluss.exception.DatabaseAlreadyExistException("db")))));
    }

    @Test
    @DisplayName("P1-216: unrelated typed exceptions (e.g. lake table) do NOT count")
    void alreadyExistsDoesNotMatchOtherTypes() {
        assertFalse(DdlBootstrap.isAlreadyExists(
                new org.apache.fluss.exception.LakeTableAlreadyExistException("lake")));
    }

    private static Schema schema(String name, org.apache.fluss.types.DataType type) {
        return Schema.newBuilder().column(name, type).build();
    }

    @Test
    @DisplayName("P1-054: identical schema matches; rename reorders the verdict, not the count")
    void schemaMismatchDetectsRename() {
        Schema expected = schema("event_day", DataTypes.STRING());
        // Same count, different name — the old count-only check passed this.
        org.apache.fluss.types.RowType renamed =
                Schema.newBuilder().column("event_date", DataTypes.STRING()).build().getRowType();
        Optional<String> mismatch = DdlBootstrap.describeSchemaMismatch(expected, renamed);
        assertTrue(mismatch.isPresent(), "rename with equal count must be reported");
        assertTrue(DdlBootstrap.describeSchemaMismatch(
                expected, expected.getRowType()).isEmpty());
    }

    @Test
    @DisplayName("P1-054: retype is reported with column name and both types")
    void schemaMismatchDetectsRetype() {
        Schema expected = schema("instrument_token", DataTypes.BIGINT());
        org.apache.fluss.types.RowType retyped =
                schema("instrument_token", DataTypes.STRING()).getRowType();
        Optional<String> mismatch = DdlBootstrap.describeSchemaMismatch(expected, retyped);
        assertTrue(mismatch.isPresent(), "retype with equal count must be reported");
        assertTrue(mismatch.get().contains("instrument_token"), mismatch.get());
    }

    @Test
    @DisplayName("P1-057: former kvTable entries are LOG (no PK) until deliberately changed")
    void formerKvEntriesHaveNoPrimaryKey() {
        for (String name : new String[] {"forming_bar", "Order_Lifecycle", "Positions",
                "Portfolio_Reservations", "fingerprint_dedup", "trade_instruction_state",
                "eod_offload_state"}) {
            assertTrue(DdlBootstrap.tableRegistry().containsKey(name), name);
            assertTrue(DdlBootstrap.tableRegistry().get(name).getSchema()
                    .getPrimaryKey().isEmpty(), name + " must stay LOG (no PK)");
        }
    }
}
