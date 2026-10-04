package com.trading.compute.signaljob;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.common.schema.ddl.DdlText;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CHG-547 live proof: the versioned merge engine that
 * {@code 23_signal_candidates_current.sql} declares really does enforce write
 * ordering on a running tablet.
 *
 * <p>The unit checks ({@code SignalCurrentDdlContractTest},
 * {@code DdlBootstrapSchemaAgreementTest}, {@code TableContractValidatorTest})
 * prove the DDL, the bootstrap descriptor and the startup contract all AGREE on
 * {@code table.merge-engine=versioned}. Agreement is not behaviour — this test is
 * the behaviour: it builds a table from that same DDL descriptor (under a scratch
 * name, never the platform table), writes a newer signal and then a delayed older
 * one, and asserts the tablet kept the newer row.
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP} (e.g. {@code localhost:9123}) and tagged
 * {@code integration}: it needs a live cluster and is skipped otherwise. The scratch
 * table is dropped in {@link #cleanup()}.
 */
@Tag("integration")
class SignalCandidatesCurrentMergeEngineIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(SignalCandidatesCurrentMergeEngineIntegrationTest.class);

    private static final Path DDL = Path.of(
            "../../01_platform/02_sql/ddl/23_signal_candidates_current.sql").toAbsolutePath();
    private static final String PROBE_TABLE = "chg547_merge_probe";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final long READY_BUDGET_MS = 60_000L;

    /** Two distinct instruments so each assertion owns its key. */
    private static final long TOKEN_OUT_OF_ORDER = 9_100_001L;
    private static final long TOKEN_EQUAL_VERSION = 9_100_002L;

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void connect() {
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP to run Fluss integration tests");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            assumeTrue(false, "Fluss cluster not available at " + bootstrap + ": " + e.getMessage());
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            try {
                admin.dropTable(TablePath.of("default", PROBE_TABLE), false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("chg547: dropped scratch table {}", PROBE_TABLE);
            } catch (Exception e) {
                LOG.warn("chg547: drop {} failed: {}", PROBE_TABLE, e.getMessage());
            }
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("CHG-547: tablet keeps the greater evaluation_ts (older write is dropped)")
    void versionedEngineEnforcesWriteOrdering() throws Exception {
        TableDescriptor descriptor = descriptorFromDdl();
        assertEquals("versioned", descriptor.getProperties().get("table.merge-engine"),
                "the DDL must declare table.merge-engine=versioned (CHG-547)");
        assertEquals("evaluation_ts",
                descriptor.getProperties().get("table.merge-engine.versioned.ver-column"),
                "the DDL must version on evaluation_ts (CHG-547)");

        TablePath path = TablePath.of("default", PROBE_TABLE);
        try {
            admin.createTable(path, descriptor, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().toLowerCase().contains("already exist")) {
                throw e;
            }
            LOG.info("chg547: {} already exists — reusing it", PROBE_TABLE);
        }
        Table table = connection.getTable(path);
        Lookuper lookuper = table.newLookup().createLookuper();
        awaitLeader(lookuper, TOKEN_OUT_OF_ORDER, path);
        UpsertWriter writer = table.newUpsert().createWriter();

        // (1) A newer signal is written first; a delayed/retried OLDER signal follows.
        // Under last-writer-wins the older row would win — the clobber this change removes.
        write(writer, TOKEN_OUT_OF_ORDER, 2_000L, "SELL");
        write(writer, TOKEN_OUT_OF_ORDER, 1_000L, "BUY");
        InternalRow kept = lookup(lookuper, TOKEN_OUT_OF_ORDER);
        assertEquals(2_000L, kept.getLong(SignalCandidatesTableColumns.EVALUATION_TS),
                "an older evaluation_ts must not overwrite the newer row");
        assertEquals("SELL",
                kept.getString(SignalCandidatesTableColumns.SIDE).toString(),
                "the whole older row must be dropped, not just its version column");

        // (2) Equal version: the engine keeps last-writer-wins (newer write wins), i.e.
        // a re-emission at the same evaluation_ts is still applied.
        write(writer, TOKEN_EQUAL_VERSION, 2_000L, "SELL");
        write(writer, TOKEN_EQUAL_VERSION, 2_000L, "BUY");
        InternalRow sameVersion = lookup(lookuper, TOKEN_EQUAL_VERSION);
        assertEquals(2_000L, sameVersion.getLong(SignalCandidatesTableColumns.EVALUATION_TS));
        assertEquals("BUY", sameVersion.getString(SignalCandidatesTableColumns.SIDE).toString(),
                "equal evaluation_ts must keep last-writer-wins");
    }

    private static TableDescriptor descriptorFromDdl() throws Exception {
        String text = Files.readString(DDL, UTF_8);
        return DdlText.toDescriptor(DdlText.parse(text, DDL.toString()), false);
    }

    private static void write(UpsertWriter writer, long token, long evaluationTs, String side)
            throws Exception {
        writer.upsert(row(token, evaluationTs, side)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * A full row for the 22-column schema: every NOT NULL column carries a value, so
     * the only differences between two rows of one token are the version column and
     * the side.
     */
    private static InternalRow row(long token, long evaluationTs, String side) {
        String[] names = SignalCandidatesTableColumns.NAMES;
        Object[] values = new Object[names.length];
        for (int i = 0; i < names.length; i++) {
            values[i] = switch (names[i]) {
                case "instrument_token" -> token;
                case "detection_ts" -> evaluationTs - 1L;
                case "evaluation_ts" -> evaluationTs;
                case "quantity", "limit_price_paise" -> 1L;
                default -> BinaryString.fromString("chg547");
            };
        }
        values[SignalCandidatesTableColumns.SIDE] = BinaryString.fromString(side);
        return GenericRow.of(values);
    }

    private static InternalRow lookup(Lookuper lookuper, long token) throws Exception {
        LookupResult result = lookuper
                .lookup(GenericRow.of(token))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return result == null ? null : result.getSingletonRow();
    }

    /**
     * A table created milliseconds ago may not have its bucket leader placed yet; poll a
     * read-only lookup until it returns (the first write otherwise burns its whole budget
     * in the coordinator queue — see {@code CompatFlussIntegrationTest.awaitWritable}).
     */
    private static void awaitLeader(Lookuper lookuper, long token, TablePath path)
            throws Exception {
        long deadline = System.currentTimeMillis() + READY_BUDGET_MS;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                lookuper.lookup(GenericRow.of(token)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(500L);
            }
        }
        assertNotNull(last);
        throw new AssertionError("table " + path + " never became readable: " + last, last);
    }
}
