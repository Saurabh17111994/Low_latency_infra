package com.trading.compute.babysitter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.common.model.PositionState;
import com.trading.common.schema.position.PositionSnapshot;
import com.trading.common.schema.position.PositionsColumns;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * BAB-DESER-001 (Task 7): the Positions changelog deserializer parses a valid
 * full image exactly and rejects every malformed variant — unsupported schema
 * version, unknown state, missing required column, and quantity invariants —
 * without ever producing a {@code Position_Actions} record (the Babysitter
 * never computes position/PnL arithmetic; it observes authoritative rows).
 */
@DisplayName("BAB-DESER-001: Positions row deserialization + malformed rejection")
class PositionsRowDeserializerTest {

    @Test
    @DisplayName("a valid current-value row maps to the exact Positions full image")
    void validRowParsesExactly() {
        PositionSnapshot snap = PositionsRowDeserializer.toSnapshot(
                TestPositionsRows.row("POS-1", "ev-9", 9L, "OPEN", 100, 0));

        assertEquals("POS-1", snap.positionId());
        assertEquals("tc-1", snap.tradeContextId());
        assertEquals("acct-1", snap.accountScopeId());
        assertEquals(7L, snap.instrumentToken());
        assertEquals("NSE", snap.exchange());
        assertEquals("TEST", snap.symbol());
        assertEquals("BUY", snap.side());
        assertEquals(PositionState.OPEN, snap.state());
        assertEquals(100, snap.openQuantity());
        assertEquals(0, snap.closedQuantity());
        assertEquals(100, snap.currentQuantity());
        assertEquals("ev-9", snap.sourceEventId());
        assertEquals(9L, snap.sourceVersion());
        assertEquals(2_000L, snap.lastUpdateTs());
        assertEquals(PositionsColumns.SCHEMA_VERSION_V2, snap.schemaVersion());
    }

    @Test
    @DisplayName("unsupported schema_version is rejected as malformed")
    void unsupportedSchemaVersionRejected() {
        GenericRowData r = (GenericRowData) TestPositionsRows.row(
                "POS-1", "ev-1", 1L, "OPEN", 10, 0);
        r.setField(PositionsColumns.SCHEMA_VERSION, StringData.fromString("3"));
        assertThrows(IllegalArgumentException.class,
                () -> PositionsRowDeserializer.toSnapshot(r));
    }

    @Test
    @DisplayName("unknown position state is rejected as malformed")
    void unknownStateRejected() {
        assertThrows(IllegalArgumentException.class, () -> PositionsRowDeserializer.toSnapshot(
                TestPositionsRows.row("POS-1", "ev-1", 1L, "NOT_A_STATE", 10, 0)));
    }

    @Test
    @DisplayName("quantity invariant violations (negative / open < closed) are rejected")
    void quantityInvariantsRejected() {
        // PositionSnapshot constructor validates open >= closed >= 0.
        assertThrows(IllegalArgumentException.class, () -> PositionsRowDeserializer.toSnapshot(
                TestPositionsRows.row("POS-1", "ev-1", 1L, "OPEN", -1, 0)));
        assertThrows(IllegalArgumentException.class, () -> PositionsRowDeserializer.toSnapshot(
                TestPositionsRows.row("POS-1", "ev-1", 1L, "OPEN", 5, 10)));
    }

    @Test
    @DisplayName("missing required columns are rejected as malformed")
    void missingRequiredColumnRejected() {
        GenericRowData r = (GenericRowData) TestPositionsRows.row(
                "POS-1", "ev-1", 1L, "OPEN", 10, 0);
        r.setField(PositionsColumns.SOURCE_EVENT_ID, null);
        assertThrows(IllegalArgumentException.class,
                () -> PositionsRowDeserializer.toSnapshot(r));
    }

    @Test
    @DisplayName("P2-008: short-arity rows fail as malformed, not IndexOutOfBounds")
    void shortArityRowRejected() {
        GenericRowData r = new GenericRowData(3);
        r.setField(0, StringData.fromString("POS-1"));
        r.setField(1, StringData.fromString("tc-1"));
        r.setField(2, StringData.fromString("acct-1"));
        assertThrows(IllegalArgumentException.class,
                () -> PositionsRowDeserializer.toSnapshot(r));
    }

    @Test
    @DisplayName("P2-009: null non-nullable long is rejected, never silent 0")
    void nullQuantityRejected() {
        GenericRowData r = (GenericRowData) TestPositionsRows.row(
                "POS-1", "ev-1", 1L, "OPEN", 10, 0);
        r.setField(PositionsColumns.OPEN_QUANTITY, null);
        assertThrows(IllegalArgumentException.class,
                () -> PositionsRowDeserializer.toSnapshot(r));
    }
}
