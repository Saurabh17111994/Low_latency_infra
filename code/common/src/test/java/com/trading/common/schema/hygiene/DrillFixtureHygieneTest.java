package com.trading.common.schema.hygiene;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drill fixture hygiene: the live drill classes must leave the dev cluster exactly
 * as they found it. Every drill fixture carries a documented family prefix (see
 * {@link #DB_FAMILIES} and {@link #TABLE_FAMILIES}); a leftover is a bug with a
 * measured cost — on 2026-09-25 one drill step spent 825s (80 waits) in
 * "placement backlog" because earlier runs' fixtures were still occupying the
 * coordinator's placement queue, and every leftover makes the next run slower.
 * Leftovers therefore compound instead of decaying.
 *
 * <p>Wired into the drill step AFTER the live drills ({@code make drill-live}).
 * Read-only by construction: it lists, never drops. When it fails, the owning
 * drill family named in the message either failed its own awaited teardown or
 * predates the fix; the owning class sweeps its family on its next run (CHG-312).
 *
 * <p>One deliberate exception is kept only until the next run of its owner:
 * {@code B4HaltedIntentConsumeDeferE2ETest} KEEPs its scratch DB when a write never
 * resolved — dropping it under a possibly-pending batch makes Fluss's Sender
 * busy-loop on metadata for the whole cluster (measured 2026-09-18). That test now
 * sweeps older {@code b4_halted_*} keeps at the end of each run (CHG-312), so at
 * most the newest keep survives; this guard failing on a {@code b4_halted_*}
 * database is the correct outcome and names the owner.
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP} like every live drill (self-skips offline).
 */
@Tag("integration")
class DrillFixtureHygieneTest {

    private static final Logger LOG = LoggerFactory.getLogger(DrillFixtureHygieneTest.class);

    /** Fixture databases per owning drill class. Prefix -> owner. */
    static final List<String> DB_FAMILIES = List.of(
            "prewarm_",        // GatewayStartupPrewarmTest
            "gateway_replay_", // GatewayFlussDurableReplayIntegrationTest
            "gateway_ledger_", // GatewayFlussDurableReplayIntegrationTest
            "gateway_page_",   // GatewayFlussDurableReplayIntegrationTest
            "proj_",           // FlussProjectionWriterIntegrationTest
            "b4_halted_",      // B4HaltedIntentConsumeDeferE2ETest
            "quar_");          // FlussPostbackQuarantineStoreIntegrationTest

    /** Fixture tables (any database) per owning component. Prefix -> owner. */
    static final List<String> TABLE_FAMILIES = List.of(
            "compat_",    // CompatFluss*, GateMergeEngineDrillIntegrationTest
            "chg100",     // DdlSmokeTwinSweepTest twins/sweep fixtures
            "wp3_gate_",  // FlussGateAttemptStoresIntegrationTest
            "rr_settle_", // tiering-remote-read-verify.sh leader-settle scratch (leak fixed 2026-09-25)
            "a2probe_");  // A2-lane probe tables: no in-repo creator, so any survivor is stray

    static boolean isFixtureDatabase(String name) {
        return DB_FAMILIES.stream().anyMatch(name::startsWith);
    }

    static boolean isFixtureTable(String name) {
        return TABLE_FAMILIES.stream().anyMatch(name::startsWith);
    }

    @Test
    @DisplayName("the drill step leaves zero fixture databases or tables behind")
    void noDrillFixturesRemain() throws Exception {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for the drill fixture hygiene guard");

        Configuration c = new Configuration();
        c.setString("bootstrap.servers", bootstrap);
        Connection conn = ConnectionFactory.createConnection(c);
        List<String> leftovers = new ArrayList<>();
        int databases = 0;
        int tables = 0;
        try {
            Admin admin = conn.getAdmin();
            List<String> dbs = admin.listDatabases().get(30, TimeUnit.SECONDS);
            List<String> inventory = new ArrayList<>();
            for (String db : dbs) {
                databases++;
                if (isFixtureDatabase(db)) {
                    leftovers.add("database " + db);
                }
                List<String> tablesInDb = admin.listTables(db).get(30, TimeUnit.SECONDS);
                List<String> names = new ArrayList<>(tablesInDb);
                names.sort(null);
                inventory.add(db + "=" + names);
                for (String table : tablesInDb) {
                    tables++;
                    if (isFixtureTable(table)) {
                        leftovers.add("table " + db + "." + table);
                    }
                }
            }
            System.out.println("drill-hygiene: inventory " + inventory);
            LOG.info("drill-hygiene: inventory {}", inventory);
            admin.close();
        } finally {
            conn.close();
        }

        System.out.printf("drill-hygiene: scanned %d databases, %d tables; leftovers=%d%n",
                databases, tables, leftovers.size());
        LOG.info("drill-hygiene: scanned {} databases, {} tables; leftovers={}",
                databases, tables, leftovers.size());

        assertThat(leftovers)
                .as("drill fixtures must not survive their drill step: a leftover occupies the "
                        + "coordinator's placement queue and slows every later run (measured "
                        + "2026-09-25: 825s of placement backlog in one drill step). The owning "
                        + "drill class sweeps its own family on its next run (CHG-312); this guard "
                        + "is the fail-closed backstop (CHG-313)")
                .isEmpty();
    }
}
