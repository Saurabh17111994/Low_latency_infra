package com.trading.ingestion;

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
 *   drop:   java -cp &lt;ingestion classpath&gt; com.trading.ingestion.DropRawTable drop [bootstrap]
 *   ensure: java -cp &lt;ingestion classpath&gt; com.trading.ingestion.DropRawTable ensure [bootstrap]
 * </pre>
 */
public final class DropRawTable {

    private DropRawTable() {}

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "drop";
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);

        TablePath path = TablePath.of("default", "raw_table_1");
        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {
            if ("ensure".equals(mode)) {
                boolean ok = com.trading.ingestion.DdlBootstrap.ensureTables(bootstrap);
                System.out.println("ensureTables: " + (ok ? "OK" : "FAILED"));
                return;
            }
            if (!admin.tableExists(path).get()) {
                System.out.println("raw_table_1 does not exist — nothing to drop");
                return;
            }
            admin.dropTable(path, true).get();
            System.out.println("dropped default.raw_table_1 (metadata + data)");
        }
    }
}
