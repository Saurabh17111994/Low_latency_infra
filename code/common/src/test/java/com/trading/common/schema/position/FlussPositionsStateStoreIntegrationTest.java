package com.trading.common.schema.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.common.model.PositionState;
import com.trading.common.schema.fluss.FlussPlacementAwait;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCH-20 live drill: {@link FlussPositionsStateStore} round-trips
 * {@link PositionSnapshot}s through a real Positions-shaped KV table
 * (10_positions.sql v2 layout, single-field PK {@code position_id} — the
 * raw-client-safe COMPAT-FLUSS-005 shape) on the dev cluster. Scratch tables
 * are created and dropped; platform tables are never touched.
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP} (e.g. {@code localhost:9123}) and tagged
 * {@code integration} like the other live-Fluss common tests.
 */
@Tag("integration")
class FlussPositionsStateStoreIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(FlussPositionsStateStoreIntegrationTest.class);

    private static final String PREFIX = "compat_test_pos_";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    /**
     * Fixture-readiness budget: bounded and logged, asserts nothing (CHG-212, CHG-213). The
     * write budget above is left alone - a fresh table is waited for, never given more time to
     * fail in.
     */
    private static final Duration READY_BUDGET = Duration.ofSeconds(240);

    private static String bootstrap;
    private static Connection connection;
    private static final List<String> CREATED_TABLES = new ArrayList<>();

    private static final Schema POSITIONS_SCHEMA = Schema.newBuilder()
            .column("position_id", DataTypes.STRING())
            .column("trade_context_id", DataTypes.STRING())
            .column("account_scope_id", DataTypes.STRING())
            .column("instrument_token", DataTypes.BIGINT())
            .column("exchange", DataTypes.STRING())
            .column("symbol", DataTypes.STRING())
            .column("side", DataTypes.STRING())
            .column("state", DataTypes.STRING())
            .column("open_quantity", DataTypes.BIGINT())
            .column("closed_quantity", DataTypes.BIGINT())
            .column("average_entry_paise", DataTypes.BIGINT())
            .column("average_exit_paise", DataTypes.BIGINT())
            .column("source_event_id", DataTypes.STRING())
            .column("source_version", DataTypes.BIGINT())
            .column("created_ts", DataTypes.BIGINT())
            .column("last_update_ts", DataTypes.BIGINT())
            .column("schema_version", DataTypes.STRING())
            .primaryKey("position_id")
            .build();

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP to run the SCH-20 Positions store drill");
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        connection = ConnectionFactory.createConnection(conf);
        LOG.info("sch-20 drill: connected to {}", bootstrap);
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (connection != null) {
            for (String table : CREATED_TABLES) {
                try {
                    connection.getAdmin().dropTable(TablePath.of("default", table), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    LOG.info("sch-20 drill: dropped {}", table);
                } catch (Exception e) {
                    LOG.warn("sch-20 drill: drop {} failed: {}", table, e.getMessage());
                }
            }
            connection.close();
        }
    }

    private static String createTable(String name) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(POSITIONS_SCHEMA)
                .distributedBy(1, "position_id")
                .build();
        TablePath path = TablePath.of("default", name);
        try {
            connection.getAdmin().createTable(path, td, false)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().toLowerCase().contains("already exist")) {
                throw e;
            }
        }
        FlussPlacementAwait.awaitServing(
                connection.getAdmin(), path, "positions table " + name, READY_BUDGET);
        CREATED_TABLES.add(name);
        return name;
    }

    private static PositionSnapshot snapshot(String id, String side, PositionState state,
            long open, long closed) {
        return new PositionSnapshot(id, "tc-1", "acc-1", 123L, "NSE", "RELIANCE", side,
                state, open, closed, 10050L, 0L, "pb-" + id, 1L, 1_700_000_000_000L,
                1_700_000_000_000L, "2");
    }

    @Test
    @DisplayName("SCH-20: Fluss Positions store upsert + lookup round-trip")
    void upsertAndLookupRoundTrip() throws Exception {
        String table = createTable(PREFIX + System.nanoTime());
        try (FlussPositionsStateStore store =
                FlussPositionsStateStore.open(bootstrap, "default", table, TIMEOUT)) {
            PositionSnapshot buy = snapshot("pos-1", "BUY", PositionState.OPEN, 100L, 0L);
            PositionSnapshot sell = snapshot("pos-2", "SELL", PositionState.OPEN, 50L, 0L);
            store.upsert(buy);
            store.upsert(sell);

            PositionSnapshot readBuy = store.lookup("pos-1");
            assertThat(readBuy).isNotNull();
            assertThat(readBuy.positionId()).isEqualTo("pos-1");
            assertThat(readBuy.state()).isEqualTo(PositionState.OPEN);
            assertThat(readBuy.openQuantity()).isEqualTo(100L);
            assertThat(readBuy.averageEntryPaise()).isEqualTo(10050L);
            assertThat(readBuy.sourceVersion()).isEqualTo(1L);
            assertThat(readBuy.schemaVersion()).isEqualTo("2");
            assertThat(readBuy.side()).isEqualTo("BUY");

            PositionSnapshot readSell = store.lookup("pos-2");
            assertThat(readSell).isNotNull();
            assertThat(readSell.side()).isEqualTo("SELL");
            assertThat(readSell.instrumentToken()).isEqualTo(123L);
            assertThat(readSell.exchange()).isEqualTo("NSE");
            assertThat(readSell.symbol()).isEqualTo("RELIANCE");

            assertThat(store.lookup("pos-missing")).isNull();
            LOG.info("sch-20 drill: upsert+lookup round-trip OK on {}", table);
        }
    }

    @Test
    @DisplayName("SCH-20: Fluss Positions store re-upsert is last-write-wins")
    void reUpsertIsLastWriteWins() throws Exception {
        String table = createTable(PREFIX + System.nanoTime());
        try (FlussPositionsStateStore store =
                FlussPositionsStateStore.open(bootstrap, "default", table, TIMEOUT)) {
            store.upsert(snapshot("pos-1", "BUY", PositionState.OPEN, 100L, 0L));
            PositionSnapshot closed = new PositionSnapshot("pos-1", "tc-1", "acc-1", 123L,
                    "NSE", "RELIANCE", "BUY", PositionState.CLOSED, 100L, 100L, 10050L,
                    11000L, "pb-2", 2L, 1_700_000_000_000L, 1_700_000_000_005L, "2");
            store.upsert(closed);

            PositionSnapshot read = store.lookup("pos-1");
            assertThat(read.state()).isEqualTo(PositionState.CLOSED);
            assertThat(read.closedQuantity()).isEqualTo(100L);
            assertThat(read.averageExitPaise()).isEqualTo(11000L);
            assertThat(read.sourceVersion()).isEqualTo(2L);
            assertThat(read.lastUpdateTs()).isEqualTo(1_700_000_000_005L);
            LOG.info("sch-20 drill: last-write-wins re-upsert OK on {}", table);
        }
    }
}
