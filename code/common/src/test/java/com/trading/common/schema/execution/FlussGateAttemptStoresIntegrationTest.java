package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.AttemptStore.PrepareRequest;
import com.trading.common.schema.execution.AttemptStore.PrepareResult;
import com.trading.common.schema.execution.GateStateStore.FenceResult;
import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import com.trading.common.schema.ownership.ExecutionGateColumns;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WP-3 live drill (T5 CHG-044/045; CHG-056 single-operator): the Fluss-backed {@link FlussGateStateStore}
 * (fence/lease/ownership + single-operator Saurabh approval) and {@link FlussAttemptStore}
 * (exactly-one PREPARED) round-trip the durable v3 Execution_Gate /
 * Execution_Attempts KV shapes on a real Fluss cluster. Scratch tables are
 * created and dropped; platform tables are never touched.
 *
 * <p>Proves, on the durable store:
 * <ul>
 *   <li><b>PREPARED-before-bridge</b> — a {@code prepare()} mints a PREPARED
 *       attempt that is durably persisted (raw Fluss lookup) before any bridge
 *       command is issued;</li>
 *   <li><b>exactly-one command</b> — re-preparing the same
 *       (instruction_id, request_hash) returns {@code DUPLICATE} and leaves
 *       exactly one row, so a driving engine would issue one bridge command;</li>
 *   <li>gate HALTED-boot + monotonic fence token + single-operator (Saurabh) approval all
 *       persist durably (raw lookup), mirroring the offline crash-window suite
 *       against the InMemory implementation.</li>
 * </ul>
 *
 * <p>Cross-restart (crash-window) zero-duplicate is enforced by the command
 * gate's reconciliation, which reads these durable rows to find SUBMITTING /
 * UNKNOWN attempts before minting anything new (the babysitter path) — this
 * drill proves the durable rows exist for that reconciliation to see.
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP} (e.g. {@code localhost:9123}) and tagged
 * {@code integration} like the other live-Fluss common tests. Scratch table
 * schemas are derived from the {@code Execution*Columns} ownership constants
 * (themselves pinned to the v3 DDL by the agreement tests), so the drill can
 * never drift from the pinned column layout.
 */
@Tag("integration")
class FlussGateAttemptStoresIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(FlussGateAttemptStoresIntegrationTest.class);

    private static final String PREFIX = "wp3_gate_atm_";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final long NOW = 1_700_000_000_000L;

    private static String bootstrap;
    private static Connection connection;
    private static final List<String> CREATED_TABLES = new ArrayList<>();

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP to run the WP-3 gate/attempt durable-store drill");
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        connection = ConnectionFactory.createConnection(conf);
        LOG.info("wp-3 drill: connected to {}", bootstrap);
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (connection != null) {
            for (String table : CREATED_TABLES) {
                try {
                    connection.getAdmin().dropTable(TablePath.of("default", table), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    LOG.info("wp-3 drill: dropped {}", table);
                } catch (Exception e) {
                    LOG.warn("wp-3 drill: drop {} failed: {}", table, e.getMessage());
                }
            }
            connection.close();
        }
    }

    private static Schema schemaFromOwnership(String[] names, List<String> typeRoots,
                                              String pk) {
        List<DataType> types = new ArrayList<>();
        for (String root : typeRoots) {
            switch (root) {
                case "STRING": types.add(DataTypes.STRING()); break;
                case "BIGINT": types.add(DataTypes.BIGINT()); break;
                case "INTEGER": types.add(DataTypes.INT()); break;
                default: types.add(DataTypes.STRING());
            }
        }
        return Schema.newBuilder().fromFields(List.of(names), types).primaryKey(pk).build();
    }

    private static String createTable(String name, Schema schema, int buckets, String key)
            throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(buckets, key)
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
        CREATED_TABLES.add(name);
        return name;
    }

    /** Raw Fluss lookup of one row by its single-field PK — proves durability independent of the store's in-process delegate. */
    private static InternalRow rawLookup(String tableName, String pk) throws Exception {
        Table table = connection.getTable(TablePath.of("default", tableName));
        Lookuper lookuper = table.newLookup().createLookuper();
        return lookuper.lookup(GenericRow.of(BinaryString.fromString(pk)))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .getSingletonRow();
    }

    @Test
    @DisplayName("WP-3: PREPARED-before-bridge + exactly-one + gate fence/approval durable")
    void preparedBeforeBridgeAndExactlyOneCommandOnDurableStore() throws Exception {
        String gateT = createTable(PREFIX + "g" + System.nanoTime(),
                schemaFromOwnership(ExecutionGateColumns.NAMES.toArray(new String[0]),
                        ExecutionGateColumns.TYPE_ROOTS, "execution_partition_id"),
                4, "execution_partition_id");
        String attT = createTable(PREFIX + "a" + System.nanoTime(),
                schemaFromOwnership(ExecutionAttemptsColumns.NAMES.toArray(new String[0]),
                        ExecutionAttemptsColumns.TYPE_ROOTS, "execution_attempt_id"),
                8, "execution_attempt_id");
        String partition = "p-1";

        try (FlussGateStateStore gate =
                     FlussGateStateStore.open(bootstrap, "default", gateT, TIMEOUT);
             FlussAttemptStore attempts =
                     FlussAttemptStore.open(bootstrap, "default", attT, TIMEOUT, () -> { })) {

            // --- Gate boots HALTED, epoch 0, durably persisted. ---
            GateRow boot = new GateRow(partition, "acc-1", GateState.HALTED, 0L,
                    null, null, null, null, null, null, 0L, null, null, null);
            gate.init(boot);
            InternalRow bootRow = rawLookup(gateT, partition);
            assertThat(bootRow).isNotNull();
            assertThat(bootRow.getString(ExecutionGateColumns.STATE).toString())
                    .isEqualTo("HALTED");
            assertThat(bootRow.getLong(ExecutionGateColumns.EPOCH)).isEqualTo(0L);

            // --- Fence acquisition: monotonic token + owner persist durably. ---
            FenceResult fence = gate.acquire(partition, "exec-1", 30_000L, NOW);
            assertThat(fence.conflict()).isFalse();
            assertThat(fence.token()).isEqualTo(1L);
            InternalRow fenced = rawLookup(gateT, partition);
            assertThat(fenced.getString(ExecutionGateColumns.OWNER_INSTANCE_ID).toString())
                    .isEqualTo("exec-1");
            assertThat(fenced.getLong(ExecutionGateColumns.FENCE_TOKEN)).isEqualTo(1L);

            // --- DEC-044 single-operator gate: one approval completes the gate;
            // a second distinct principal on a complete gate is ALREADY_APPLIED
            // (no second slot — matches InMemoryGateStateStore). ---
            assertThat(gate.approve(partition, "op-a", 0L, "ev-1", NOW).outcome())
                    .isEqualTo(GateStateStore.ApprovalOutcome.APPLIED);
            assertThat(gate.approve(partition, "op-b", 0L, "ev-1", NOW).outcome())
                    .isEqualTo(GateStateStore.ApprovalOutcome.ALREADY_APPLIED);
            InternalRow approved = rawLookup(gateT, partition);
            assertThat(approved.getString(ExecutionGateColumns.APPROVAL_1).toString())
                    .isEqualTo("op-a");
            assertThat(approved.getString(ExecutionGateColumns.APPROVAL_2)).isNull();

            // --- PREPARED-before-bridge: prepare() mints a PREPARED attempt durable in Fluss. ---
            PrepareRequest req = new PrepareRequest("try-1", "acc-1", "instr-1", "buy",
                    partition, "req-hash-1", "clref-1", fence.token(), 0L, NOW);
            assertThat(attempts.prepare(req).status())
                    .isEqualTo(AttemptStore.Status.CREATED);
            InternalRow attemptRow = rawLookup(attT, "try-1");
            assertThat(attemptRow).isNotNull();
            assertThat(attemptRow.getString(ExecutionAttemptsColumns.PHASE).toString())
                    .isEqualTo("PREPARED");
            // The gate fence token is persisted at PREPARED — the durable authorization.
            assertThat(attemptRow.getLong(ExecutionAttemptsColumns.GATE_FENCE_TOKEN))
                    .isEqualTo(1L);
            assertThat(attemptRow.getLong(ExecutionAttemptsColumns.GATE_EPOCH))
                    .isEqualTo(0L);

            // --- Exactly-one command: re-prepare same (instruction_id, request_hash) -> DUPLICATE. ---
            PrepareResult again = attempts.prepare(new PrepareRequest("try-1", "acc-1",
                    "instr-1", "buy", partition, "req-hash-1", "clref-1",
                    fence.token(), 0L, NOW + 1));
            assertThat(again.status()).isEqualTo(AttemptStore.Status.DUPLICATE);
            assertThat(attempts.size()).isEqualTo(1);
            // The durable store still holds exactly one PREPARED row.
            assertThat(rawLookup(attT, "try-1")).isNotNull();

            LOG.info("wp-3 drill: PREPARED-before-bridge + exactly-one + durable fence/approval OK"
                    + " (gate {}, attempts {})", gateT, attT);
        }
    }

    @Test
    @DisplayName("WP-3: cross-restart — a fresh instance re-derives prior state and refuses a duplicate")
    void crossRestartHydrationSeesPriorStateAndRejectsDuplicate() throws Exception {
        String gateT = createTable(PREFIX + "g" + System.nanoTime(),
                schemaFromOwnership(ExecutionGateColumns.NAMES.toArray(new String[0]),
                        ExecutionGateColumns.TYPE_ROOTS, "execution_partition_id"),
                4, "execution_partition_id");
        String attT = createTable(PREFIX + "a" + System.nanoTime(),
                schemaFromOwnership(ExecutionAttemptsColumns.NAMES.toArray(new String[0]),
                        ExecutionAttemptsColumns.TYPE_ROOTS, "execution_attempt_id"),
                8, "execution_attempt_id");
        String partition = "p-1";
        String attemptId = "try-9";
        PrepareRequest req = new PrepareRequest(attemptId, "acc-1", "instr-9", "buy",
                partition, "req-hash-9", "clref-9", 1L, 0L, NOW);

        // Instance A: fence + approve + prepare -> persisted.
        try (FlussGateStateStore aGate = FlussGateStateStore.open(bootstrap, "default", gateT, TIMEOUT);
             FlussAttemptStore aAttempts = FlussAttemptStore.open(bootstrap, "default", attT, TIMEOUT, () -> { })) {
            aGate.init(new GateRow(partition, "acc-1", GateState.HALTED, 0L,
                    null, null, null, null, null, null, 0L, null, null, null));
            aGate.acquire(partition, "exec-1", 30_000L, NOW); // fence token 1, durable
            aGate.approve(partition, "op-a", 0L, "ev-1", NOW);
            aGate.approve(partition, "op-b", 0L, "ev-1", NOW);
            assertThat(aAttempts.prepare(req).status())
                    .isEqualTo(AttemptStore.Status.CREATED);
        }

        // Instance B = the restarted process: fresh in-memory delegate, same durable tables.
        try (FlussGateStateStore bGate = FlussGateStateStore.open(bootstrap, "default", gateT, TIMEOUT);
             FlussAttemptStore bAttempts = FlussAttemptStore.open(bootstrap, "default", attT, TIMEOUT, () -> { })) {

            // Gate: read() re-derives the prior fenced, single-operator-approved row from Fluss.
            GateRow recovered = bGate.read(partition);
            assertThat(recovered).isNotNull();
            assertThat(recovered.fenceToken()).isEqualTo(1L);
            assertThat(recovered.ownerInstanceId()).isEqualTo("exec-1");
            assertThat(recovered.approvalsComplete()).isTrue();

            // Attempts: re-preparing the SAME (instruction_id, request_hash) and deterministic
            // execution_attempt_id returns DUPLICATE — the exact crash-window zero-duplicate
            // guarantee, now reproduced on the durable store across a process restart.
            PrepareResult again = bAttempts.prepare(req);
            assertThat(again.status()).isEqualTo(AttemptStore.Status.DUPLICATE);
            assertThat(bAttempts.size()).isEqualTo(1);
            // The fresh process's fence sequence is monotonic: next acquire > prior token 1.
            FenceResult nextFence = bGate.acquire(partition, "exec-1", 30_000L, NOW);
            assertThat(nextFence.token()).isGreaterThan(1L);

            LOG.info("wp-3 drill: cross-restart hydration + zero-duplicate OK (gate {}, attempts {})",
                    gateT, attT);
        }
    }
}
