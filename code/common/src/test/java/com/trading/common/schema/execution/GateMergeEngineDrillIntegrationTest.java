package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.common.model.GateState;
import com.trading.common.schema.ddl.DdlText;
import com.trading.common.schema.ownership.ExecutionGateColumns;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
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
 * CHG-122 live drill: the Execution_Gate merge engine on a real cluster.
 *
 * <p>Offline tests can pin the re-read decision ({@code FlussGateStateStoreWriteOrderTest}),
 * but they cannot prove the DROP — it happens tablet-side. This drill proves it on hardware:
 *
 * <ol>
 *   <li>the table creates with the merge engine the DDL declares, and the options are
 *       effective (not just accepted);</li>
 *   <li>a write carrying a LOWER {@code fence_token} than the stored row is DROPPED — the
 *       durable row keeps the newer token and the stale writer's value is nowhere, which is
 *       the server-side conditional write the CRITICAL findings were blocked on;</li>
 *   <li>a halt still LANDS when a competing writer has already moved the token ahead, because
 *       {@code halt} hydrates from durable and mints strictly forward — the safety property
 *       that keeps a halt from being version-dropped.</li>
 * </ol>
 *
 * <p>The scratch descriptor is parsed from {@code 11_execution_gate.sql} itself, so the drill
 * cannot drift from the DDL.
 *
 * <p><b>Datalake caveat:</b> the dev cluster runs with {@code table.datalake.enabled=false}
 * (the documented deviation the apply-parity test asserts), so the merge behavior is drilled
 * with datalake disabled. The production combination — VERSIONED <i>with</i> datalake, which is
 * untested upstream — is covered by {@link #versionedWithDatalakeEnabledIsAccepted()} only on a
 * lake-wired cluster ({@code FLUSS_LAKE_WIRED=true}).
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP}; scratch tables are dropped in {@code @AfterAll}.
 */
@Tag("integration")
class GateMergeEngineDrillIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(GateMergeEngineDrillIntegrationTest.class);

    private static final String SCRATCH_PREFIX = "compat_gate_merge_";
    private static final String DDL_FILE = "11_execution_gate.sql";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private static String bootstrap;
    private static Connection connection;
    private static final List<String> CREATED = new ArrayList<>();

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP to run the CHG-122 gate merge-engine drill");
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        connection = ConnectionFactory.createConnection(conf);
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (connection != null) {
            for (String table : CREATED) {
                try {
                    connection.getAdmin().dropTable(TablePath.of("default", table), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    LOG.info("chg-122 drill: dropped {}", table);
                } catch (Exception e) {
                    LOG.warn("chg-122 drill: drop {} failed: {}", table, e.getMessage());
                }
            }
            connection.close();
        }
    }

    /** Locate {@code code/01_platform/02_sql/ddl} by walking up from the working directory. */
    private static Path resolveDdlDir() throws Exception {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve("01_platform/02_sql/ddl/schema_manifest.json");
            if (Files.isRegularFile(candidate)) {
                return candidate.getParent();
            }
            current = current.getParent();
        }
        throw new IllegalStateException(
                "cannot locate 01_platform/02_sql/ddl/schema_manifest.json from working directory");
    }

    private static DdlText.ParsedDdl parsedDdl() throws Exception {
        Path ddl = resolveDdlDir().resolve(DDL_FILE);
        assertThat(Files.exists(ddl)).as("DDL not found at %s", ddl).isTrue();
        return DdlText.parse(Files.readString(ddl, StandardCharsets.UTF_8), DDL_FILE);
    }

    private static String createScratch(boolean datalakeEnabled) throws Exception {
        String name = SCRATCH_PREFIX + System.nanoTime();
        TableDescriptor descriptor = DdlText.toDescriptor(parsedDdl(), !datalakeEnabled);
        connection.getAdmin().createTable(TablePath.of("default", name), descriptor, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED.add(name);
        return name;
    }

    private static GenericRow gateRow(String pid, GateState state, long epoch, String owner,
                                      long token, Long acquiredTs, Long leaseExpiresTs, Long lostTs) {
        Object[] v = new Object[ExecutionGateColumns.FIELD_COUNT];
        v[ExecutionGateColumns.EXECUTION_PARTITION_ID] = BinaryString.fromString(pid);
        v[ExecutionGateColumns.ACCOUNT_SCOPE_ID] = BinaryString.fromString("acct1");
        v[ExecutionGateColumns.STATE] = BinaryString.fromString(state.name());
        v[ExecutionGateColumns.EPOCH] = epoch;
        v[ExecutionGateColumns.REASON] = BinaryString.fromString("drill");
        v[ExecutionGateColumns.DETECTION_TIME] = null;
        v[ExecutionGateColumns.EVIDENCE_HASH] = BinaryString.fromString("h0");
        v[ExecutionGateColumns.APPROVAL_1] = null;
        v[ExecutionGateColumns.APPROVAL_2] = null;
        v[ExecutionGateColumns.TRANSITION_TS] = 0L;
        v[ExecutionGateColumns.OWNER_INSTANCE_ID] = owner == null ? null : BinaryString.fromString(owner);
        v[ExecutionGateColumns.FENCE_TOKEN] = token;
        v[ExecutionGateColumns.FENCE_ACQUIRED_TS] = acquiredTs;
        v[ExecutionGateColumns.LEASE_EXPIRES_TS] = leaseExpiresTs;
        v[ExecutionGateColumns.FENCE_LOST_TS] = lostTs;
        v[ExecutionGateColumns.APPROVED_EVIDENCE_HASH] = null;
        v[ExecutionGateColumns.SCHEMA_VERSION] =
                BinaryString.fromString(ExecutionGateColumns.SCHEMA_VERSION_V3);
        return GenericRow.of(v);
    }

    private static void upsert(Table table, GenericRow row) throws Exception {
        UpsertWriter w = table.newUpsert().createWriter();
        w.upsert(row).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static InternalRow lookup(Table table, String pid) throws Exception {
        Lookuper lookuper = table.newLookup().createLookuper();
        return lookuper.lookup(GenericRow.of(BinaryString.fromString(pid)))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
    }

    @Test
    @DisplayName("table creates with the DDL's merge engine and the options are effective")
    void mergeEngineOptionsAreEffective() throws Exception {
        String name = createScratch(false);
        try (Table table = connection.getTable(TablePath.of("default", name))) {
            Map<String, String> effective = table.getTableInfo().getProperties().toMap();
            assertThat(effective).containsEntry("table.merge-engine", "versioned");
            assertThat(effective).containsEntry("table.merge-engine.versioned.ver-column", "fence_token");
            assertThat(effective).containsEntry("table.delete.behavior", "ignore");
        }
    }

    @Test
    @DisplayName("a stale-token write is dropped by the merge engine, not applied")
    void staleTokenWriteIsDropped() throws Exception {
        String name = createScratch(false);
        try (Table table = connection.getTable(TablePath.of("default", name))) {
            upsert(table, gateRow("p1", GateState.ENABLED, 5, "owner-new", 100L, 1L, 9_999_999L, null));

            // The stale writer: an older token from a zombie executor. Under LWW this would
            // clobber owner-new. Under VERSIONED the tablet must drop it.
            upsert(table, gateRow("p1", GateState.ENABLED, 5, "owner-stale", 99L, 1L, 9_999_999L, null));

            InternalRow durable = lookup(table, "p1");
            assertThat(durable).isNotNull();
            assertThat(durable.getLong(ExecutionGateColumns.FENCE_TOKEN))
                    .as("stale token write must NOT be applied")
                    .isEqualTo(100L);
            assertThat(durable.getString(ExecutionGateColumns.OWNER_INSTANCE_ID).toString())
                    .as("stale writer must not become the owner")
                    .isEqualTo("owner-new");

            // And a strictly NEWER token does win, so the filter is ordering, not rejection.
            upsert(table, gateRow("p1", GateState.ENABLED, 5, "owner-newer", 101L, 1L, 9_999_999L, null));
            assertThat(lookup(table, "p1").getLong(ExecutionGateColumns.FENCE_TOKEN)).isEqualTo(101L);
        }
    }

    @Test
    @DisplayName("halt lands and takes the fence forward even after a competing writer advanced the token")
    void haltLandsAboveACompetingWriter() throws Exception {
        String name = createScratch(false);
        try (Table table = connection.getTable(TablePath.of("default", name))) {
            upsert(table, gateRow("p1", GateState.ENABLED, 5, "owner-1", 10L, 1L, 9_999_999L, null));

            // A competing writer races ahead of the local process.
            upsert(table, gateRow("p1", GateState.ENABLED, 5, "owner-2", 50L, 1L, 9_999_999L, null));

            FlussGateStateStore store = FlussGateStateStore.open(
                    bootstrap, "default", name, TIMEOUT, Set.of("saurabh"));
            try {
                GateRow boot = new GateRow("p1", "acct1", GateState.HALTED, 5, "boot", "h0",
                        null, null, null, null, 0L, null, null, null);
                GateRow current = store.init(boot);

                GateRow halted = store.halt("p1", current, "safety drill", "h1",
                        System.currentTimeMillis());
                // No exception: the halt was not version-dropped.
                assertThat(halted).isNotNull();
                assertThat(halted.state()).isEqualTo(GateState.HALTED);
                assertThat(halted.ownerInstanceId()).isNull();
                assertThat(halted.fenceToken())
                        .as("halt must outrank the competing writer's token")
                        .isGreaterThan(50L);

                // Durable agrees with what halt returned (verifyPersisted passed inside).
                InternalRow durable = lookup(table, "p1");
                assertThat(durable.getLong(ExecutionGateColumns.FENCE_TOKEN))
                        .isEqualTo(halted.fenceToken());
                assertThat(durable.getString(ExecutionGateColumns.STATE).toString())
                        .isEqualTo("HALTED");
            } finally {
                store.close();
            }
        }
    }

    @Test
    @DisplayName("VERSIONED combined with datalake.enabled is accepted (lake-wired cluster only)")
    void versionedWithDatalakeEnabledIsAccepted() throws Exception {
        // The unmatched risk in CHG-122: VERSIONED and table.datalake.* are both accepted by
        // the server's validation independently, but no upstream test exercises them TOGETHER.
        // The dev cluster forces datalake off, so this needs a lake-wired cluster.
        assumeTrue("true".equalsIgnoreCase(System.getenv("FLUSS_LAKE_WIRED")),
                "set FLUSS_LAKE_WIRED=true on a lake-wired cluster to prove VERSIONED + datalake");

        String name = createScratch(true);
        try (Table table = connection.getTable(TablePath.of("default", name))) {
            assertThat(table.getTableInfo().getProperties().toMap())
                    .containsEntry("table.merge-engine", "versioned")
                    .containsEntry("table.datalake.enabled", "true");
            upsert(table, gateRow("p1", GateState.ENABLED, 5, "owner-1", 10L, 1L, 9_999_999L, null));
            upsert(table, gateRow("p1", GateState.ENABLED, 5, "owner-0", 9L, 1L, 9_999_999L, null));
            assertThat(lookup(table, "p1").getLong(ExecutionGateColumns.FENCE_TOKEN)).isEqualTo(10L);
        }
    }

    @Test
    @DisplayName("lookup/write handles are created once, reused across calls, and released on close")
    void handlesAreCachedAndReleased() throws Exception {
        // P3-365/P3-367/P3-371: the handles are @NotThreadSafe, so reuse is only sound because
        // every method is synchronized (monitor confinement). This asserts the cache is real —
        // reverting to per-call creation makes the identity assertions fail.
        String name = createScratch(false);
        try (Table table = connection.getTable(TablePath.of("default", name))) {
            upsert(table, gateRow("p1", GateState.HALTED, 5, null, 0L, null, null, 1L));
        }
        FlussGateStateStore store = FlussGateStateStore.open(
                bootstrap, "default", name, TIMEOUT, Set.of("saurabh"));
        Object lookuper = store.cachedLookuperForTest();
        Object writer = store.cachedUpsertWriterForTest();
        assertThat(lookuper).as("lookuper created at open").isNotNull();
        assertThat(writer).as("writer created at open").isNotNull();

        long ts = 1_000L;
        for (int i = 0; i < 5; i++) {
            store.read("p1");
            // Same owner so each acquire refreshes and WRITES (exercising the cached writer)
            // rather than conflicting against its own live lease.
            store.acquire("p1", "owner-1", 5_000L, ts + i);
        }
        assertThat(store.cachedLookuperForTest())
                .as("the same Lookuper must serve every call").isSameAs(lookuper);
        assertThat(store.cachedUpsertWriterForTest())
                .as("the same UpsertWriter must serve every call").isSameAs(writer);

        store.close();
        assertThat(store.cachedLookuperForTest()).as("released on close").isNull();
        assertThat(store.cachedUpsertWriterForTest()).as("released on close").isNull();
        // The cached handle is what the store actually USES — identity alone would not prove
        // that (a field can be cached and ignored). With the cache released, a use after close
        // must fail immediately with NPE rather than attempting I/O on a dead table; a
        // per-call implementation creates a fresh handle here and fails differently.
        assertThrows(NullPointerException.class, () -> store.read("p1"),
                "a read after close must fail on the released cached handle");
    }

    @Test
    @DisplayName("the store refuses to report success for a lost write")
    void storeDoesNotReportSuccessForALostWrite() throws Exception {
        String name = createScratch(false);
        try (Table table = connection.getTable(TablePath.of("default", name))) {
            upsert(table, gateRow("p1", GateState.HALTED, 5, null, 0L, null, null, 1L));
            FlussGateStateStore store = FlussGateStateStore.open(
                    bootstrap, "default", name, TIMEOUT, Set.of("saurabh"));
            try {
                // Directly assert the comparison the store applies to its own re-read, using
                // the durable row as the witness.
                GateRow durable = store.read("p1");
                assertThat(durable).isNotNull();
                // The durable row is its own witness: writing exactly it is accepted.
                assertDoesNotThrow(() -> FlussGateStateStore.checkPersisted("p1", durable, durable, false));
                // A durable row AHEAD of the write is a dropped write and must be rejected.
                GateRow ahead = durable.withFence("other-owner", durable.fenceToken() + 1, 1L, 1000L);
                assertThrows(IllegalStateException.class,
                        () -> FlussGateStateStore.checkPersisted("p1", ahead, durable, false),
                        "a durable row ahead of the write must be rejected");
            } finally {
                store.close();
            }
        }
    }
}
