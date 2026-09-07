package com.trading.ingestion;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/**
 * Test-only utility: fully drop raw_table_1 (metadata + data segments) so the
 * next ingestion start recreates it fresh. Used by the loadtest clean-baseline
 * procedure (2026-08-29) — a plain data-dir delete left the catalog metadata
 * in place, so the ingestion "verified" the table and silently wrote appends
 * into a table with no data segments.
 *
 * <p>Usage:
 * <pre>
 *   drop:   java -cp &lt;ingestion classpath&gt; com.trading.ingestion.DropRawTable drop [bootstrap] [--force]
 *   ensure: java -cp &lt;ingestion classpath&gt; com.trading.ingestion.DropRawTable ensure [bootstrap] [--force]
 * </pre>
 *
 * <p>P1-058: non-local clusters require {@code --force} (third arg) — a bare
 * {@code drop <prod-bootstrap>} is refused before any connection is opened.
 */
public final class DropRawTable {

    private DropRawTable() {}

    // P1-219: bound every admin RPC — an unresponsive coordinator must fail
    // the clean-baseline procedure in 30s, not hang it forever.
    // Visible for testing: the timeout value is a pinned contract.
    static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(30);

    // P1-218: exit-code mapping for ensure — a FAILED verification must be
    // visible to calling scripts, not exit 0. Visible for testing.
    static int ensureExitCode(boolean ok) {
        return ok ? 0 : 1;
    }

    // Visible for testing: only the two documented modes pass. Anything
    // else (bare invocation, typo, --help) throws instead of falling
    // through to the destructive drop.
    static String requireMode(String[] args) {
        String mode = args.length > 0 ? args[0] : "";
        if (!"drop".equals(mode) && !"ensure".equals(mode)) {
            throw new IllegalArgumentException(
                    "Usage: DropRawTable <drop|ensure> [bootstrap] — got '" + mode + "'");
        }
        return mode;
    }

    // Visible for testing: remote clusters need explicit --force.
    static void requireLocalOrForce(String bootstrap, String[] args) {
        boolean local = bootstrap.startsWith("localhost")
                || bootstrap.startsWith("127.0.0.1")
                || bootstrap.startsWith("[::1]");
        boolean force = args.length > 2 && "--force".equals(args[2]);
        if (!local && !force) {
            throw new IllegalArgumentException("refusing to run '" + args[0]
                    + "' against non-local cluster '" + bootstrap
                    + "' without --force");
        }
    }

    public static void main(String[] args) throws Exception {
        String mode;
        try {
            mode = requireMode(args);
        } catch (IllegalArgumentException bad) {
            // P1-006 fail-fast: bare/typo invocations must never reach drop.
            System.err.println(bad.getMessage());
            System.exit(2);
            return;
        }
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        try {
            requireLocalOrForce(bootstrap, args);
        } catch (IllegalArgumentException remote) {
            // P1-058 fail-fast: refuse before opening any connection.
            System.err.println(remote.getMessage());
            System.exit(2);
            return;
        }
        // P1-217: ensure dispatches BEFORE opening a connection —
        // DdlBootstrap.ensureTables opens its own, so the outer one was
        // wasted resources + a spurious failure mode (ensure failing on
        // the unused outer connection). Drop needs the connection; ensure
        // must not pay for it.
        if ("ensure".equals(mode)) {
            boolean ok = com.trading.ingestion.DdlBootstrap.ensureTables(bootstrap);
            System.out.println("ensureTables: " + (ok ? "OK" : "FAILED"));
            System.exit(ensureExitCode(ok));
            return;
        }
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);

        TablePath path = TablePath.of("default", "raw_table_1");
        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {
            // P1-059: ignoreIfNotExists=true already handles the absent
            // table — no exists pre-check (avoids TOCTOU with a concurrent
            // bootstrap recreating the table between the two RPCs).
            admin.dropTable(path, true).get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            System.out.println("dropped default.raw_table_1 (metadata + data)");
        }
    }
}
