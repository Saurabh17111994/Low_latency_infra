package com.trading.common.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Routing-key rule (docs/08_implementation/01-foundation.md &rarr; "Routing-key rule", orig L444):
 * every LOG table must declare a non-null routing identity (bucket.key).
 */
public final class RoutingKeyRule {

    private RoutingKeyRule() {}

    public static final class Violation {
        public final String tableName;
        public final String reason;
        public Violation(String tableName, String reason) {
            this.tableName = tableName;
            this.reason = reason;
        }
    }

    public static List<Violation> check(List<SchemaManifestEntry> entries) {
        List<Violation> violations = new ArrayList<>();
        if (entries == null) {
            return violations;
        }
        for (SchemaManifestEntry e : entries) {
            if (e == null) {
                violations.add(new Violation(null, "null schema entry"));
                continue;
            }
            if ("LOG".equalsIgnoreCase(e.tableKind)) {
                if (e.bucketKey == null || e.bucketKey.isBlank()) {
                    violations.add(new Violation(e.tableName,
                        "LOG table missing non-null routing identity (bucket.key)"));
                }
            }
        }
        return violations;
    }
}
