package com.trading.ingestion;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/**
 * Quick check: how many columns does each table actually have in Fluss?
 * Run with FLUSS_BOOTSTRAP=localhost:9123
 */
public final class SchemaCheck {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);

        String[] tables = {
            "raw_table_1", "Postback_Quarantine", "suspected_discontinuities",
            "candle_live", "Signal_Candidates"
        };

        try (Connection c = ConnectionFactory.createConnection(conf)) {
            for (String name : tables) {
                try {
                    var info = c.getTable(TablePath.of("default", name)).getTableInfo();
                    int cols = info.getRowType().getFieldCount();
                    String status = switch (name) {
                        case "raw_table_1" -> cols == 20 ? "✅" : "❌ expected 20 (DDL v2, R-054/R-231: quote/option columns removed)";
                        case "Postback_Quarantine" -> cols == 18 ? "✅" : "❌ expected 18";
                        case "suspected_discontinuities" -> cols == 15 ? "✅" : "❌ expected 15";
                        default -> "";
                    };
                    System.out.printf("%-35s %d columns %s%n", name, cols, status);
                } catch (Exception e) {
                    System.out.printf("%-35s ERROR: %s%n", name, e.getMessage());
                }
            }
        }
        System.out.println("\n✅ = correct  ❌ = wrong column count (will be auto-fixed by DDL bootstrap)");
    }
}
