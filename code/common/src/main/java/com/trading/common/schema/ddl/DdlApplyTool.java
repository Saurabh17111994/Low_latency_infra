package com.trading.common.schema.ddl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.trading.common.schema.SchemaManifest;
import com.trading.common.schema.SchemaManifestEntry;
import com.trading.common.schema.fluss.CompositeKeyMatrixVerifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

/**
 * The DDL application contract engine (docs/08_implementation/02-schema-storage.md
 * "DDL application contract", 9 steps), invoked by {@code ddl_apply.py} when
 * {@code make ddl APPLY=1} is run:
 *
 * <ol>
 *   <li>Exact Fluss/Flink versions — enforced by {@code ddl_apply.py} before this tool runs
 *       (also recorded in the evidence record).</li>
 *   <li>Manifest + DDL checksums — re-verified here (each DDL's bytes must hash to the
 *       committed {@code ddl_sha256}).</li>
 *   <li>Parse every DDL against the pinned dialect — {@link DdlText#parse} into admin-API
 *       descriptors (Fluss 0.9.1 has no SQL client).</li>
 *   <li>Apply to an empty acceptance catalog in deterministic order — REFUSES (exit 3) if any
 *       target table already exists; BEFORE creating anything, the COMPAT-FLUSS-005 raw-client
 *       composite-PK matrix is re-verified IN-BAND against this live cluster (scratch tables,
 *       dropped after) — a deviation refuses the apply (exit 1) so the matrix is never just
 *       referenced as capability evidence. Tables are created in DDL-file order.</li>
 *   <li>Inspect the effective schema/options from the runtime — {@code Admin.getTableInfo}.</li>
 *   <li>Schema parity against logical requirements — effective schema/options must equal the
 *       manifest + DDL (columns, PK, bucket key, bucket count, TTL, datalake deviation).</li>
 *   <li>Write/read round-trip per table — LOG append + scan, KV upsert + lookup. (The full
 *       changelog/restore battery is the COMPAT-FLUSS-* / COMPAT-FLINK-* capability evidence,
 *       which {@code --matrix-evidence} supplies.) A composite-PK KV table the raw client
 *       cannot upsert is a documented Fluss 0.9.1 limitation (COMPAT-FLUSS-005 matrix) —
 *       recorded per table, and NEVER absorbed into {@code PASS}: the apply is
 *       {@code PASS_WITH_LIMITATION}, which exits 0 only when {@code --ack-limitations}
 *       names exactly the limited tables (the documented Flink-connector-only design), and
 *       exits 1 otherwise (fail-closed).</li>
 *   <li>Record the applied manifest ID — sha256 of the manifest bytes, written with per-table
 *       evidence (status, limitations, acknowledged limitations) to {@code --evidence-out}.
 *       Exit codes: 0 = full PASS; 6 = PASS_WITH_LIMITATION (acknowledged partial apply —
 *       distinct from PASS so automation can branch); 1 = failure or refused limitation;
 *       2/3/4/5 = usage/classpath / empty-catalog / gate refusals. Every terminal outcome
 *       prints a machine-readable {@code ddl-apply: RESULT=<STATUS> EXIT=<code> TABLES=<n>
 *       MANIFEST=<id>} sentinel.</li>
 *   <li>Service readiness — the recorded applied-manifest-id is the token services compare
 *       against their required table/version (each service's existing preflight refuses
 *       readiness on mismatch).</li>
 * </ol>
 *
 * <p>Dev capability: {@code --table-prefix} prepends a name to every table, so the full
 * contract can be exercised against a non-empty dev cluster (e.g. scratch verification)
 * without touching platform tables; prefixed tables are dropped when the run completes.
 * Production invocations never pass a prefix — the empty-catalog rule applies to the real
 * table names.
 */
public final class DdlApplyTool {

    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** Bound on waiting for a partitioned table's auto-created partitions (normally well under 1s). */
    private static final Duration AUTO_PARTITION_WAIT = Duration.ofSeconds(30);

    private DdlApplyTool() {}

    public static void main(String[] args) {
        try {
            System.exit(run(args));
        } catch (Throwable t) {
            System.err.println("ddl-apply: FATAL — " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    static int run(String[] args) throws Exception {
        Options opts = Options.parse(args);

        // Sweep mode: detect/repair fixture rows left by the pre-CHG-100 smoke
        // path (smokeRoundTrip writing defaultValue fixtures into live tables).
        // Bypasses the apply contract entirely (no manifest/catalog preconditions).
        if (opts.sweepOnly) {
            Connection connection = null;
            Admin admin = null;
            try {
                Configuration conf = new Configuration();
                conf.setString("bootstrap.servers", opts.bootstrap);
                connection = ConnectionFactory.createConnection(conf);
                admin = connection.getAdmin();
                return sweep(connection, admin, opts);
            } finally {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }

        // Dev capability: drop every live table whose name starts with the
        // prefix (leftovers from interrupted prefixed runs). No manifest or
        // ddl-dir needed — list + drop by prefix only.
        if (opts.cleanupPrefix != null) {
            Connection connection = null;
            Admin admin = null;
            try {
                Configuration conf = new Configuration();
                conf.setString("bootstrap.servers", opts.bootstrap);
                connection = ConnectionFactory.createConnection(conf);
                admin = connection.getAdmin();
                List<String> live = admin.listTables("default")
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                int dropped = 0;
                for (String name : live) {
                    if (name.startsWith(opts.cleanupPrefix)) {
                        admin.dropTable(TablePath.of("default", name), false)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        System.out.println("ddl-apply: dropped leftover " + name);
                        dropped++;
                    }
                }
                System.out.println("ddl-apply: cleanup done — " + dropped + " table(s) dropped");
                return 0;
            } finally {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }

        Path ddlDir = opts.ddlDir.toAbsolutePath();
        Path manifestPath = ddlDir.resolve("schema_manifest.json");
        if (!Files.isRegularFile(manifestPath)) {
            System.err.println("ddl-apply: no schema_manifest.json at " + manifestPath);
            return 2;
        }
        SchemaManifest manifest = JSON.readValue(manifestPath.toFile(), SchemaManifest.class);
        if (manifest.tables == null || manifest.tables.isEmpty()) {
            System.err.println("ddl-apply: manifest carries no tables");
            return 2;
        }
        List<String> manifestErrors = new ArrayList<>();
        for (SchemaManifestEntry entry : manifest.tables) {
            if (entry == null) {
                manifestErrors.add("null manifest entry");
                continue;
            }
            try {
                entry.validate();
                entry.validateRouting();
            } catch (IllegalArgumentException bad) {
                manifestErrors.add(bad.getMessage());
            }
        }
        if (!manifestErrors.isEmpty()) {
            System.err.println("ddl-apply: MANIFEST VALIDATION FAILED:");
            manifestErrors.forEach(f -> System.err.println("  - " + f));
            return 2;
        }

        // Step 2 — manifest/DDL checksums.
        List<String> checksumFailures = new ArrayList<>();
        Map<String, DdlText.ParsedDdl> parsed = new HashMap<>();
        for (SchemaManifestEntry entry : manifest.tables) {
            Path ddl = ddlDir.resolve(entry.ddlPath);
            if (!Files.isRegularFile(ddl)) {
                checksumFailures.add(entry.ddlPath + " missing");
                continue;
            }
            byte[] bytes = Files.readAllBytes(ddl);
            String actual = sha256Hex(bytes);
            if (entry.ddlSha256 == null || !entry.ddlSha256.equalsIgnoreCase(actual)) {
                checksumFailures.add(entry.ddlPath + " checksum mismatch (manifest vs DDL)");
                continue;
            }
            parsed.put(entry.tableName,
                    DdlText.parse(new String(bytes, StandardCharsets.UTF_8), entry.ddlPath));
        }
        if (!checksumFailures.isEmpty()) {
            System.err.println("ddl-apply: CHECKSUM VERIFICATION FAILED:");
            checksumFailures.forEach(f -> System.err.println("  - " + f));
            return 1;
        }

        // Deterministic order: DDL-file order (the manifest is generated sorted).
        List<SchemaManifestEntry> ordered = new ArrayList<>(manifest.tables);
        ordered.sort((a, b) -> a.ddlPath.compareTo(b.ddlPath));

        List<String> targetNames = new ArrayList<>();
        for (SchemaManifestEntry e : ordered) {
            targetNames.add(opts.prefix == null ? e.tableName : opts.prefix + e.tableName);
        }

        // Auto-detect the raw-client-limited tables from the manifest (composite
        // PK + default bucket key — the COMPAT-FLUSS-005 failing cell) so the
        // operator confirms with --ack-limitations auto instead of guessing.
        List<String> expectedLimitations = new ArrayList<>();
        for (SchemaManifestEntry e : ordered) {
            DdlText.ParsedDdl ddl = parsed.get(e.tableName);
            if (ddl != null && isPredictedLimited(ddl)) {
                expectedLimitations.add(e.tableName);
            }
        }
        String ackMode = opts.ackLimitations.isEmpty() ? "none"
                : (opts.ackLimitations.size() == 1 && opts.ackLimitations.get(0)
                        .equalsIgnoreCase("auto") ? "auto" : "explicit");
        List<String> ackLimitations = resolveAck(opts.ackLimitations, expectedLimitations);

        Connection connection = null;
        Admin admin = null;
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", opts.bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();

            // Step 3a — ensure the target database exists. A lost catalog
            // (2026-09-05: ZK had no named volume) leaves NO 'default'
            // database; the apply contract's "empty catalog" precondition is
            // satisfied by a missing db, so create it (ignore if exists) to
            // make recovery self-sufficient. Never drops or alters an
            // existing database.
            ensureDatabase(admin, "default");

            // Step 4 — empty-catalog precondition.
            List<String> existing = new ArrayList<>();
            for (String name : targetNames) {
                try {
                    TableInfo info = admin.getTableInfo(TablePath.of("default", name))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    if (info != null) {
                        existing.add(name);
                    }
                } catch (Exception e) {
                    // TableInfo absent -> not exists; other failures surface at create.
                    if (e.getMessage() != null
                            && !e.getMessage().toLowerCase().contains("not exist")) {
                        throw e;
                    }
                }
            }
            if (!existing.isEmpty()) {
                System.err.println("ddl-apply: REFUSED — catalog is not empty; found existing "
                        + "tables: " + existing);
                System.err.println("  The contract applies to an EMPTY acceptance catalog. "
                        + "Drop or rename these tables, or use --table-prefix for scratch "
                        + "verification only.");
                return 3;
            }

            List<TableRecord> records = new ArrayList<>();
            List<String> failures = new ArrayList<>();

            // COMPAT-FLUSS-005 matrix — verified IN-BAND against this live
            // cluster (scratch tables named <prefix>matrix_cell1..4, dropped by
            // the verifier), never merely referenced as capability evidence.
            // A deviation is a hard apply failure and refuses the apply BEFORE
            // any table is created (in acceptance mode the catalog stays
            // empty); it feeds the evidence record's matrix object.
            CompositeKeyMatrixVerifier.Result matrix;
            try {
                String matrixBase = opts.prefix != null
                        ? opts.prefix + "matrix"
                        : "matrix_" + System.nanoTime();
                matrix = CompositeKeyMatrixVerifier.verify(
                        connection, admin, matrixBase, TIMEOUT);
            } catch (Exception e) {
                matrix = new CompositeKeyMatrixVerifier.Result(List.of(), false,
                        List.of("matrix verification failed: "
                                + (e.getMessage() == null
                                        ? e.getClass().getSimpleName()
                                        : e.getMessage())));
            }
            if (!matrix.passed()) {
                // Contract: refuse the apply BEFORE any table is created
                // (catalog stays empty) — record matrix evidence and return
                // before the Step 4 create loop.
                failures.add("COMPAT-FLUSS-005 matrix in-band failed");
                matrix.deviations().forEach(failures::add);
                writeMatrixRefusedEvidence(opts, manifestPath, matrix, failures);
                return 1;
            }

            // Step 4 — apply in deterministic order.
            List<String> created = new ArrayList<>();
            try {
                for (int i = 0; i < ordered.size(); i++) {
                    SchemaManifestEntry entry = ordered.get(i);
                    DdlText.ParsedDdl ddl = parsed.get(entry.tableName);
                    String target = targetNames.get(i);
                    admin.createTable(TablePath.of("default", target),
                            DdlText.toDescriptor(ddl), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    created.add(target);
                }

                // Steps 5+6 — inspect + parity.
                for (int i = 0; i < ordered.size(); i++) {
                    SchemaManifestEntry entry = ordered.get(i);
                    DdlText.ParsedDdl ddl = parsed.get(entry.tableName);
                    String target = targetNames.get(i);
                    TableInfo info = admin.getTableInfo(TablePath.of("default", target))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    List<String> parityFailures = parityFailures(entry, ddl, info, target);
                    records.add(new TableRecord(entry.tableName, target, info.getTableId(),
                            ddl.isKv() ? "KV" : "LOG", info.getNumBuckets(),
                            parityFailures.isEmpty()));
                    failures.addAll(parityFailures);
                }

                // Step 7 — write/read smoke per table. A raw-client write to a
                // composite-PK KV table whose bucket key equals the PK is a
                // DOCUMENTED Fluss 0.9.1 limitation (the iceberg datalake key
                // encoder needs exactly one key field; verified 2026-08-15 that
                // kv.format-version=2 + a single-field subset bucket key lifts
                // it — feature_candles_15s/instruments carry that config). The
                // Flink connector bypasses the limitation entirely (proven by
                // candle writes). Recorded per table — the status decision then
                // refuses to call the apply fully PASS unless the operator
                // acknowledges the exact limited tables (see decideStatus).
                if (!opts.skipSmoke) {
                    for (int i = 0; i < ordered.size(); i++) {
                        DdlText.ParsedDdl ddl = parsed.get(ordered.get(i).tableName);
                        String target = targetNames.get(i);
                        // CHG-100: smoke NEVER writes fixture rows to the real
                        // table (LOG fixture rows are undeletable and would
                        // permanently halt fail-closed consumers). The round
                        // trip runs against a scratch twin created from the
                        // same descriptor and dropped immediately after.
                        String smokeName = opts.allowLiveSmoke ? target
                                : smokeTwinName(opts.prefix, ordered.get(i).tableName);
                        try {
                            if (opts.allowLiveSmoke) {
                                System.out.println("ddl-apply: WARNING --allow-live-smoke: "
                                        + "fixture rows go to the REAL table " + target
                                        + " (LOG rows are undeletable; do NOT use on "
                                        + "consumer-bearing catalogs)");
                            } else {
                                dropIfExists(admin, smokeName);
                                admin.createTable(TablePath.of("default", smokeName),
                                        DdlText.toDescriptor(ddl), false)
                                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            }
                            String outcome = smokeRoundTrip(connection, admin, smokeName, ddl);
                            records.get(i).smoke(outcome);
                            if (outcome != null && !isKnownLimitation(outcome)) {
                                failures.add(target + " smoke failed: " + outcome);
                            }
                            System.out.println("ddl-apply: smoke " + target + " via twin "
                                    + smokeName + " -> " + (outcome == null ? "PASS"
                                            : isKnownLimitation(outcome) ? "LIMITATION" : "FAIL"));
                        } catch (Exception e) {
                            String outcome = e.getMessage() == null
                                    ? e.getClass().getSimpleName() : e.getMessage();
                            records.get(i).smoke(outcome);
                            if (!isKnownLimitation(outcome)) {
                                failures.add(target + " smoke failed: " + outcome);
                            }
                        } finally {
                            if (!opts.allowLiveSmoke) {
                                dropIfExists(admin, smokeName);
                            }
                        }
                    }
                }
            } finally {
                // Dev capability: prefixed scratch tables are dropped after the run.
                if (opts.prefix != null && admin != null) {
                    for (String name : created) {
                        try {
                            admin.dropTable(TablePath.of("default", name), false)
                                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            System.out.println("ddl-apply: dropped scratch table " + name);
                        } catch (Exception e) {
                            System.err.println("ddl-apply: drop " + name + " failed: "
                                    + e.getMessage());
                        }
                    }
                }
            }

            // Step 8 — evidence record. Status honors the composite-PK matrix:
            // an apply is PASS only when EVERY table smoke passes; composite-PK
            // raw-client limitations (the COMPAT-FLUSS-005 matrix's failing
            // cells) are never absorbed into PASS — they yield
            // PASS_WITH_LIMITATION, which exits 0 ONLY when --ack-limitations
            // names exactly the limited tables (the documented Flink-only
            // design), and exits 1 otherwise (fail-closed).
            String appliedManifestId = sha256Hex(Files.readAllBytes(manifestPath));
            StatusDecision decision = decideStatus(failures, records, ackLimitations);
            ObjectNode evidence = JSON.createObjectNode();
            evidence.put("record_id", "ddl-apply-" + Instant.now());
            evidence.put("applied_manifest_id", appliedManifestId);
            evidence.put("status", decision.status());
            evidence.put("ack_mode", ackMode);
            evidence.put("timestamp", Instant.now().toString());
            evidence.put("bootstrap", opts.bootstrap);
            evidence.put("table_prefix", opts.prefix);
            evidence.put("flink_version", opts.flinkVersion);
            evidence.put("fluss_version", opts.flussVersion);
            evidence.put("tables_applied", ordered.size());
            evidence.put("tables_existing_refused", 0);
            ArrayNode tables = evidence.putArray("tables");
            for (TableRecord r : records) {
                ObjectNode t = tables.addObject();
                t.put("logical_name", r.logicalName);
                t.put("physical_name", r.physicalName);
                t.put("table_id", r.tableId);
                t.put("kind", r.kind);
                t.put("bucket_count", r.bucketCount);
                t.put("parity", r.parityOk ? "PASS" : "FAIL");
                t.put("smoke", r.smokeStatus);
                if (r.smokeNote != null) {
                    t.put("smoke_note", r.smokeNote);
                }
            }
            ArrayNode fail = evidence.putArray("failures");
            failures.forEach(fail::add);
            ArrayNode lims = evidence.putArray("limitations");
            decision.limitationTables().forEach(lims::add);
            ArrayNode acked = evidence.putArray("acknowledged_limitations");
            decision.acknowledged().forEach(acked::add);
            ObjectNode matrixNode = evidence.putObject("matrix");
            matrixNode.put("status", matrix.passed() ? "PASS" : "FAIL");
            ArrayNode matrixCells = matrixNode.putArray("cells");
            for (CompositeKeyMatrixVerifier.CellResult c : matrix.cells()) {
                ObjectNode cell = matrixCells.addObject();
                cell.put("label", c.label());
                cell.put("bucket_key", String.join(",", c.bucketKeys()));
                cell.put("kv_format_version", c.kvFormatVersion() == null
                        ? "absent" : c.kvFormatVersion());
                cell.put("expected", c.expectedPass() ? "PASS" : "FAIL");
                cell.put("outcome", c.outcome());
                cell.put("matched", c.matched());
            }

            if (opts.evidenceOut != null) {
                Path parent = opts.evidenceOut.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                JSON.writeValue(opts.evidenceOut.toFile(), evidence);
            }

            // RESULT= sentinel — the machine-readable terminal line downstream
            // automation branches on: RESULT=PASS EXIT=0, RESULT=PASS_WITH_
            // LIMITATION EXIT=6 (acknowledged partial) or EXIT=1 (refused),
            // RESULT=FAIL EXIT=1.
            String sentinel = "ddl-apply: RESULT=" + decision.status()
                    + " EXIT=" + decision.exitCode()
                    + " TABLES=" + ordered.size()
                    + " MANIFEST=" + appliedManifestId;
            if (!failures.isEmpty()) {
                System.err.println("ddl-apply: PARITY/SMOKE/MATRIX FAILURES:");
                failures.forEach(f -> System.err.println("  - " + f));
                System.err.println(sentinel);
                return 1;
            }
            for (String m : decision.messages()) {
                if (decision.exitCode() == 0 || decision.exitCode() == 6) {
                    System.out.println(m);
                } else {
                    System.err.println(m);
                }
            }
            if (decision.exitCode() != 0 && decision.exitCode() != 6) {
                System.err.println("applied_manifest_id=" + appliedManifestId
                        + " (status " + decision.status() + " — NOT fully PASS)");
                System.err.println(sentinel);
                return decision.exitCode();
            }
            System.out.println("ddl-apply: APPLIED " + ordered.size() + " tables "
                    + "applied_manifest_id=" + appliedManifestId
                    + " (status " + decision.status() + ")");
            System.out.println(sentinel);
            return decision.exitCode();
        } finally {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    // ── limitation prediction (step 7/8) ──────────────────────────────────

    /**
     * Predict, from the DDL alone, whether the raw client will be unable to
     * upsert this table (the COMPAT-FLUSS-005 failing cell): a KV table with a
     * COMPOSITE primary key whose bucket key equals the PK (default bucket
     * key). With the (cluster-inherited) iceberg datalake format, such tables
     * use {@code IcebergKeyEncoder} for the PK, which requires exactly one
     * field; {@code kv.format-version=2} + a single-field subset bucket key
     * lifts it. Single-field-PK tables and composite-PK tables with a subset
     * bucket key are always raw-client writable.
     */
    static boolean isPredictedLimited(DdlText.ParsedDdl ddl) {
        if (ddl.primaryKey().size() <= 1) {
            return false;
        }
        java.util.Set<String> pk = new java.util.HashSet<>(ddl.primaryKey());
        java.util.Set<String> bucket = new java.util.HashSet<>();
        for (String part : ddl.bucketKey().split(",")) {
            bucket.add(part.trim());
        }
        return pk.equals(bucket);
    }

    /**
     * Resolve the acknowledgment list. {@code auto} (the confirm-only operator
     * flow) fills the exact limited tables predicted from the manifest — the
     * operator confirms, never guesses. Any other value passes through as an
     * explicit name list (the set-equality check in {@link #decideStatus}
     * still validates it).
     */
    static List<String> resolveAck(List<String> ackLimitations, List<String> expected) {
        if (ackLimitations.size() == 1 && ackLimitations.get(0).equalsIgnoreCase("auto")) {
            return List.copyOf(expected);
        }
        return ackLimitations;
    }

    // ── status decision (step 8) ──────────────────────────────────────────

    /** Outcome of the apply-status decision. Pure JVM — unit tested. */
    record StatusDecision(String status, int exitCode, List<String> limitationTables,
                          List<String> acknowledged, List<String> messages) {}

    /**
     * Decide the apply's status from the parity/smoke outcomes.
     *
     * <ul>
     *   <li>Any parity/smoke failure → {@code FAIL}, exit 1 (failures dominate).</li>
     *   <li>No failures, no limitations → {@code PASS}, exit 0.</li>
     *   <li>Limitations present: the apply is never silently {@code PASS}. The
     *       status is {@code PASS_WITH_LIMITATION}; dedicated exit code 6 ONLY
     *       when {@code ackLimitations} names exactly the limited tables (the
     *       documented Flink-connector-only design — currently
     *       {@code Order_Lifecycle}, {@code Order_Correlation}; pass
     *       {@code auto} to fill the set detected from the manifest instead of
     *       naming them), exit 1 otherwise (fail-closed refusal with the
     *       mismatch explained).</li>
     * </ul>
     */
    static StatusDecision decideStatus(List<String> failures, List<TableRecord> records,
            List<String> ackLimitations) {
        List<String> limitationTables = new ArrayList<>();
        for (TableRecord r : records) {
            if ("LIMITATION".equals(r.smokeStatus)) {
                limitationTables.add(r.logicalName);
            }
        }
        if (!failures.isEmpty()) {
            return new StatusDecision("FAIL", 1, List.copyOf(limitationTables),
                    List.copyOf(ackLimitations), List.of());
        }
        if (limitationTables.isEmpty()) {
            return new StatusDecision("PASS", 0, List.of(), List.of(), List.of());
        }
        java.util.Set<String> limited = new java.util.LinkedHashSet<>(limitationTables);
        java.util.Set<String> ack = new java.util.LinkedHashSet<>();
        for (String name : ackLimitations) {
            String trimmed = name.trim();
            if (!trimmed.isEmpty()) {
                ack.add(trimmed);
            }
        }
        List<String> messages = new ArrayList<>();
        if (limited.equals(ack)) {
            messages.add("ddl-apply: acknowledged the documented composite-PK raw-client "
                    + "limitation on " + String.join(", ", limitationTables)
                    + " (Flink-connector-only design) — status PASS_WITH_LIMITATION");
            // Dedicated exit code 6 (distinct from full-PASS 0 and from
            // failure 1) so downstream automation can branch on acknowledged
            // partial applies; the RESULT= sentinel carries the same signal.
            return new StatusDecision("PASS_WITH_LIMITATION", 6,
                    List.copyOf(limitationTables), List.copyOf(ackLimitations), messages);
        }
        java.util.Set<String> unacked = new java.util.LinkedHashSet<>(limited);
        unacked.removeAll(ack);
        java.util.Set<String> extra = new java.util.LinkedHashSet<>(ack);
        extra.removeAll(limited);
        messages.add("ddl-apply: REFUSED to mark the apply fully PASS — "
                + limitationTables.size() + " table(s) are not raw-client writable "
                + "(composite-PK iceberg key encoding, COMPAT-FLUSS-005 matrix): "
                + String.join(", ", limitationTables));
        if (!unacked.isEmpty()) {
            messages.add("  unacknowledged: " + String.join(", ", unacked));
        }
        if (!extra.isEmpty()) {
            messages.add("  acknowledged but not limited: " + String.join(", ", extra));
        }
        messages.add("  Pass --ack-limitations auto to confirm the composite-PK tables "
                + "auto-detected from the manifest (" + String.join(", ", limitationTables)
                + "), or --ack-limitations <exact names>; or fix the DDLs "
                + "(kv.format-version=2 + single-field subset bucket key).");
        return new StatusDecision("PASS_WITH_LIMITATION", 1,
                List.copyOf(limitationTables), List.copyOf(ackLimitations), messages);
    }

    // ── database ensure (apply step 3a) ──────────────────────────────────

    /** Creates {@code name} when absent (ignoreIfExists). Never drops. */
    private static void ensureDatabase(Admin admin, String name) throws Exception {
        try {
            admin.createDatabase(name, org.apache.fluss.metadata.DatabaseDescriptor.builder()
                    .build(), false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            System.out.println("ddl-apply: created database '" + name
                    + "' (catalog was absent)");
        } catch (Exception e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (!msg.contains("already exist")) {
                throw e;
            }
        }
    }

    // ── parity (step 6) ───────────────────────────────────────────────────

    private static List<String> parityFailures(SchemaManifestEntry entry,
            DdlText.ParsedDdl ddl, TableInfo info, String physical) {
        String label = physical + " (" + entry.tableName + ")";
        List<String> out = new ArrayList<>();

        if (info.getNumBuckets() != ddl.bucketCount()) {
            out.add(label + ": bucket count " + info.getNumBuckets() + " != DDL "
                    + ddl.bucketCount());
        }

        Schema effective = info.getSchema();
        List<String> effectiveColumns = effective.getColumnNames();
        if (effectiveColumns.size() != ddl.columns().size()) {
            out.add(label + ": column count " + effectiveColumns.size() + " != DDL "
                    + ddl.columns().size());
        } else {
            for (int c = 0; c < ddl.columns().size(); c++) {
                DdlText.Column want = ddl.columns().get(c);
                if (!want.name().equals(effectiveColumns.get(c))) {
                    out.add(label + ": column " + c + " name " + effectiveColumns.get(c)
                            + " != DDL " + want.name());
                    continue;
                }
                DataType have = effective.getColumn(want.name()).getDataType().copy(false);
                if (!have.equals(want.type().copy(false))) {
                    out.add(label + ": column " + want.name() + " type " + have + " != DDL "
                            + want.type());
                }
            }
        }

        if (ddl.primaryKey().isEmpty()) {
            if (info.hasPrimaryKey()) {
                out.add(label + ": LOG table must have no primary key");
            }
            if (!info.getBucketKeys().equals(List.of(ddl.bucketKey()))) {
                out.add(label + ": LOG bucket key " + info.getBucketKeys() + " != DDL "
                        + ddl.bucketKey());
            }
        } else {
            if (!info.hasPrimaryKey()) {
                out.add(label + ": KV table must have a primary key");
            }
            if (!info.getPrimaryKeys().equals(ddl.primaryKey())) {
                out.add(label + ": effective PK " + info.getPrimaryKeys() + " != DDL "
                        + ddl.primaryKey());
            }
            List<String> wantBucket = Arrays.stream(ddl.bucketKey().split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).toList();
            if (!info.getBucketKeys().equals(wantBucket)) {
                out.add(label + ": KV bucket key " + info.getBucketKeys() + " != DDL "
                        + wantBucket);
            }
        }

        // Full WITH-option parity (verified 2026-08-15 against all 21 tables):
        // EVERY option the DDL declares — the manifest pins the DDL bytes by
        // checksum, so the parsed WITH block is the approved option set — must
        // be honored by the effective table, with exactly two carve-outs:
        //   * bucket.num / bucket.key are distribution, expressed via
        //     distributedBy, never table properties (Fluss rejects them);
        //   * table.datalake.enabled is the documented dev deviation: lake-
        //     enable is create-only in 0.9.1, so applies force it to false.
        // The coordinator may STAMP extra properties on top (replication.factor,
        // cluster-inherited datalake.format, kv.format-version) — those are not
        // asserted (reverse parity is impossible by design).
        Map<String, String> effectiveOptions = info.getProperties().toMap();
        for (Map.Entry<String, String> declared : ddl.options().entrySet()) {
            String key = declared.getKey();
            if (key.equals("bucket.num") || key.equals("bucket.key")) {
                continue;
            }
            String want = declared.getValue();
            String have = effectiveOptions.get(key);
            if (key.equals("table.datalake.enabled")) {
                if (!"false".equals(have)) {
                    out.add(label + ": table.datalake.enabled must be false "
                            + "(documented dev deviation), got " + have);
                }
                continue;
            }
            if (!want.equals(have)) {
                out.add(label + ": effective " + key + " " + have + " != DDL " + want);
            }
        }
        // The coordinator stamps datalake.format but never the enabled flag:
        // a DDL that declares no lake options must not gain table.datalake.enabled.
        if (!ddl.options().containsKey("table.datalake.enabled")
                && effectiveOptions.get("table.datalake.enabled") != null) {
            out.add(label + ": DDL without lake options must not gain "
                    + "table.datalake.enabled");
        }
        return out;
    }

    // ── smoke (step 7) ────────────────────────────────────────────────────

    /** The known Fluss 0.9.1 composite-key limitation (recorded, not failed). */
    private static boolean isKnownLimitation(String message) {
        return message != null
                && message.contains("Key fields must have exactly one field");
    }

    /**
     * One write + read round trip: LOG append + scan, KV upsert + lookup.
     * Returns null on PASS, or the reason (a known limitation for composite-PK KV).
     */
    private static String smokeRoundTrip(Connection connection, Admin admin, String name,
            DdlText.ParsedDdl ddl) throws Exception {
        Table table = connection.getTable(TablePath.of("default", name));
        // A partitioned table routes on the partition values carried IN the row
        // (AbstractTableWriter's partitionFieldGetter), so the fixture must name
        // a partition the table actually has. defaultValue() would write the
        // generic "smoke-0" STRING, which is not a real partition of a
        // PARTITIONED BY (event_day) table: the append resolves to no live
        // bucket and dies with "Leader not found after retry 3 times ...". Read
        // the twin's own auto-created partition instead of deriving the day —
        // no date math, so this holds for any time-zone/retention/precreate
        // configuration the DDL declares.
        PartitionInfo partition = ddl.partitionKeys().isEmpty()
                ? null : awaitAutoPartition(admin, name, ddl);
        Object[] values = new Object[ddl.columns().size()];
        for (int c = 0; c < ddl.columns().size(); c++) {
            values[c] = defaultValue(ddl.columns().get(c).type(), c);
        }
        if (partition != null) {
            applyPartitionValues(ddl, partition, values);
        }
        if (ddl.primaryKey().isEmpty()) {
            AppendWriter writer = table.newAppend().createWriter();
            // No flush in a finally here (2026-09-11): flush() is unbounded in Fluss
            // 0.9.1 and in that slot masked the bounded get()'s timeout instead of
            // cleaning anything up — see flush_guard.sh.
            writer.append(GenericRow.of(values)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            long seen = scanCount(table, info(admin, name),
                    partition == null ? null : partition.getPartitionId());
            if (seen < 1) {
                return "LOG append not readable back (scan count " + seen + ")";
            }
            return null;
        }
        UpsertWriter writer = table.newUpsert().createWriter();
        // Same shape as the LOG branch above — one bounded await, no flush.
        writer.upsert(GenericRow.of(values)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Object[] key = new Object[ddl.primaryKey().size()];
        for (int k = 0; k < ddl.primaryKey().size(); k++) {
            int colIndex = columnIndex(ddl, ddl.primaryKey().get(k));
            key[k] = values[colIndex];
        }
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow found = lookuper.lookup(GenericRow.of(key))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
        if (found == null) {
            return "KV upsert not found by primary key lookup";
        }
        return null;
    }

    private static TableInfo info(Admin admin, String name) throws Exception {
        return admin.getTableInfo(TablePath.of("default", name))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Waits for a freshly created partitioned table's auto-created partitions and returns one.
     * Auto-partitioning is asynchronous (the coordinator's AutoPartitionManager creates the DAY
     * partitions after the create), so the smoke polls briefly rather than assuming they exist, and
     * fails closed with a clear message instead of falling through to the confusable "Leader not
     * found" that a write to a non-existent partition raises.
     */
    private static PartitionInfo awaitAutoPartition(Admin admin, String name, DdlText.ParsedDdl ddl)
            throws Exception {
        TablePath path = TablePath.of("default", name);
        long deadline = System.currentTimeMillis() + AUTO_PARTITION_WAIT.toMillis();
        while (true) {
            List<PartitionInfo> partitions = admin.listPartitionInfos(path)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!partitions.isEmpty()) {
                return partitions.get(0);
            }
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(ddl.sourcePath() + ": partitioned table " + name
                        + " has no partitions after " + AUTO_PARTITION_WAIT.toSeconds()
                        + "s — auto-partition created none, so the smoke cannot target one");
            }
            Thread.sleep(200);
        }
    }

    /** Overrides the fixture's partition columns with an existing partition's own values. */
    private static void applyPartitionValues(DdlText.ParsedDdl ddl, PartitionInfo partition,
            Object[] values) {
        ResolvedPartitionSpec spec = partition.getResolvedPartitionSpec();
        List<String> keys = spec.getPartitionKeys();
        List<String> partitionValues = spec.getPartitionValues();
        for (int k = 0; k < keys.size(); k++) {
            int idx = columnIndex(ddl, keys.get(k));
            values[idx] = partitionValue(ddl.columns().get(idx).type(), partitionValues.get(k),
                    ddl, keys.get(k));
        }
    }

    /**
     * Converts a partition value (Fluss carries partition values as strings) into the column's
     * runtime type. Only STRING partition columns exist in today's DDLs ({@code event_day}); any
     * other type fails closed rather than writing a wrongly-typed fixture.
     */
    private static Object partitionValue(DataType type, String raw, DdlText.ParsedDdl ddl,
            String key) {
        if (type.getTypeRoot() == DataTypeRoot.STRING) {
            return BinaryString.fromString(raw);
        }
        throw new IllegalStateException(ddl.sourcePath() + ": smoke cannot build a "
                + type.getTypeRoot() + " fixture for partition column '" + key + "'");
    }

    /**
     * Scans every bucket and returns the total row count (drain loop — slow appends included).
     * {@code partitionId} is null for a non-partitioned table; a partitioned table's buckets exist
     * only inside a partition, so the read-back must target the partition the fixture was written
     * to or it sees nothing.
     */
    private static long scanCount(Table table, TableInfo info, Long partitionId) throws Exception {
        long count = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), partitionId, b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb)) {
                while (true) {
                    try (CloseableIterator<InternalRow> it =
                            scanner.pollBatch(Duration.ofMillis(250))) {
                        boolean any = false;
                        while (it != null && it.hasNext()) {
                            it.next();
                            count++;
                            any = true;
                        }
                        if (!any) {
                            break;
                        }
                    }
                }
            }
        }
        return count;
    }

    private static int columnIndex(DdlText.ParsedDdl ddl, String column) {
        for (int c = 0; c < ddl.columns().size(); c++) {
            if (ddl.columns().get(c).name().equals(column)) {
                return c;
            }
        }
        throw new IllegalArgumentException(ddl.sourcePath() + ": PK column " + column
                + " not in columns");
    }

    static Object defaultValue(DataType type, int index) {
        DataTypeRoot root = type.getTypeRoot();
        return switch (root) {
            case STRING -> BinaryString.fromString("smoke-" + index);
            case BIGINT -> 1L + index;
            case INTEGER -> 1 + index;
            case BYTES -> new byte[] {(byte) (index + 1), 2};
            case DOUBLE -> 1.0d + index;
            case BOOLEAN -> true;
            // Forward-cover arms (no TIMESTAMP/DATE/DECIMAL/CHAR column exists
            // in today's 27 DDLs; DdlText.type() maps them for future DDLs):
            // values must match what isSmokeFixtureRow/copyFieldValue expect.
            case TIMESTAMP_WITHOUT_TIME_ZONE -> org.apache.fluss.row.TimestampNtz
                    .fromLocalDateTime(java.time.LocalDateTime.of(2026, 1, 1, 0, 0)
                            .plusSeconds(index));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> org.apache.fluss.row.TimestampLtz
                    .fromEpochMillis(1767225600000L + index * 1000L);
            case DATE -> (int) java.time.LocalDate.of(2026, 1, 1).plusDays(index).toEpochDay();
            case DECIMAL -> org.apache.fluss.row.Decimal.fromBigDecimal(
                    java.math.BigDecimal.valueOf(1L + index), 38, 18);
            case CHAR -> BinaryString.fromString("s" + index);
            case TINYINT -> (byte) (1 + index);
            case SMALLINT -> (short) (1 + index);
            case FLOAT -> 1.0f + index;
            case TIME_WITHOUT_TIME_ZONE -> (int) (index * 1000L);
            default -> throw new IllegalArgumentException("no smoke default for " + root);
        };
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(bytes);
        StringBuilder sb = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * Matrix-refused evidence: the apply stopped before creating any table,
     * so the record carries zero tables plus the matrix FAIL cells.
     */
    private static void writeMatrixRefusedEvidence(Options opts, Path manifestPath,
            CompositeKeyMatrixVerifier.Result matrix, List<String> failures) throws Exception {
        System.err.println("ddl-apply: COMPAT-FLUSS-005 MATRIX FAILED (in-band):");
        matrix.deviations().forEach(d -> System.err.println("  - " + d));
        ObjectNode evidence = JSON.createObjectNode();
        evidence.put("record_id", "ddl-apply-" + Instant.now());
        evidence.put("applied_manifest_id", sha256Hex(Files.readAllBytes(manifestPath)));
        evidence.put("status", "FAIL");
        evidence.put("timestamp", Instant.now().toString());
        evidence.put("bootstrap", opts.bootstrap);
        evidence.put("table_prefix", opts.prefix);
        evidence.put("tables_applied", 0);
        evidence.putArray("tables");
        ArrayNode fail = evidence.putArray("failures");
        failures.forEach(fail::add);
        ObjectNode matrixNode = evidence.putObject("matrix");
        matrixNode.put("status", "FAIL");
        ArrayNode matrixCells = matrixNode.putArray("cells");
        for (CompositeKeyMatrixVerifier.CellResult c : matrix.cells()) {
            ObjectNode cell = matrixCells.addObject();
            cell.put("label", c.label());
            cell.put("bucket_key", String.join(",", c.bucketKeys()));
            cell.put("kv_format_version", c.kvFormatVersion() == null
                    ? "absent" : c.kvFormatVersion());
            cell.put("expected", c.expectedPass() ? "PASS" : "FAIL");
            cell.put("outcome", c.outcome());
            cell.put("matched", c.matched());
        }
        writeEvidenceFile(opts, evidence);
        System.err.println("ddl-apply: RESULT=FAIL EXIT=1 TABLES=0 (matrix refused before create)");
    }

    /** Evidence file write with null-parent guard (bare `--evidence-out x.json`). */
    private static void writeEvidenceFile(Options opts, ObjectNode evidence) throws Exception {
        if (opts.evidenceOut != null) {
            Path parent = opts.evidenceOut.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            JSON.writeValue(opts.evidenceOut.toFile(), evidence);
        }
    }

    // ── options / record types ────────────────────────────────────────────
    // ── CHG-100: scratch-twin smoke + fixture sweep ────────────────────────

    /** Scratch twin name for step-7 smoke (the real table is never written). */
    static String smokeTwinName(String prefix, String logicalName) {
        return (prefix == null ? "" : prefix) + "smoke_twin_" + logicalName;
    }

    /** Drop a table, swallowing only not-exist (leftover hygiene). */
    private static void dropIfExists(Admin admin, String name) {
        try {
            admin.dropTable(TablePath.of("default", name), false)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            System.out.println("ddl-apply: dropped " + name);
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().toLowerCase().contains("not exist")) {
                System.err.println("ddl-apply: drop " + name + " failed: " + e.getMessage());
            }
        }
    }

    /**
     * Fixture signature: every field equals {@link #defaultValue(DataType, int)}
     * at its column index — exactly what the pre-CHG-100 smoke path wrote.
     */
    static boolean isSmokeFixtureRow(InternalRow row, RowType rowType) {
        if (row == null) {
            return false;
        }
        int n = rowType.getFieldCount();
        if (row.getFieldCount() != n) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (row.isNullAt(i)) {
                return false;
            }
            DataType type = rowType.getFields().get(i).getType();
            Object want = defaultValue(type, i);
            switch (type.getTypeRoot()) {
                case STRING -> {
                    if (!want.equals(row.getString(i))) {
                        return false;
                    }
                }
                case BIGINT -> {
                    if (row.getLong(i) != ((Number) want).longValue()) {
                        return false;
                    }
                }
                case INTEGER -> {
                    if (row.getInt(i) != ((Number) want).intValue()) {
                        return false;
                    }
                }
                case DOUBLE -> {
                    if (row.getDouble(i) != ((Number) want).doubleValue()) {
                        return false;
                    }
                }
                case BOOLEAN -> {
                    if (row.getBoolean(i) != (Boolean) want) {
                        return false;
                    }
                }
                case BYTES -> {
                    if (!Arrays.equals((byte[]) want, row.getBytes(i))) {
                        return false;
                    }
                }
                case TIMESTAMP_WITHOUT_TIME_ZONE -> {
                    if (!want.equals(row.getTimestampNtz(i, 6))) {
                        return false;
                    }
                }
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                    if (!want.equals(row.getTimestampLtz(i, 6))) {
                        return false;
                    }
                }
                case DATE -> {
                    if (row.getInt(i) != ((Number) want).intValue()) {
                        return false;
                    }
                }
                case DECIMAL -> {
                    if (!want.equals(row.getDecimal(i, 38, 18))) {
                        return false;
                    }
                }
                case CHAR -> {
                    if (!want.equals(row.getChar(i, 1))) {
                        return false;
                    }
                }
                case TINYINT -> {
                    if (row.getByte(i) != ((Number) want).byteValue()) {
                        return false;
                    }
                }
                case SMALLINT -> {
                    if (row.getShort(i) != ((Number) want).shortValue()) {
                        return false;
                    }
                }
                case FLOAT -> {
                    if (row.getFloat(i) != ((Number) want).floatValue()) {
                        return false;
                    }
                }
                case TIME_WITHOUT_TIME_ZONE -> {
                    if (row.getInt(i) != ((Number) want).intValue()) {
                        return false;
                    }
                }
                default -> {
                    return false;
                }
            }
        }
        return true;
    }

    private static int indexByName(RowType rowType, String name) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("column not found: " + name);
    }

    /** Full scan (drain loop — the CHG-099 contention lesson) collecting fixture PKs. */
    private static List<Object[]> scanFixtures(Table table, TableInfo info)
            throws Exception {
        List<String> pks = info.getPrimaryKeys();
        int[] idx = new int[pks.size()];
        for (int k = 0; k < pks.size(); k++) {
            idx[k] = indexByName(info.getRowType(), pks.get(k));
        }
        // PK field values copied per row — InternalRow objects may be reused
        // across polls/after scanner close, so never retain the row itself.
        List<Object[]> matches = new ArrayList<>();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                    .limit(Integer.MAX_VALUE)
                    .createBatchScanner(tb)) {
                while (true) {
                    try (CloseableIterator<InternalRow> it =
                            scanner.pollBatch(Duration.ofMillis(250))) {
                        boolean any = false;
                        while (it != null && it.hasNext()) {
                            InternalRow row = it.next();
                            any = true;
                            if (isSmokeFixtureRow(row, info.getRowType())) {
                                Object[] key = new Object[info.getRowType().getFieldCount()];
                                for (int k = 0; k < idx.length; k++) {
                                    key[idx[k]] = copyFieldValue(row, info.getRowType(), idx[k]);
                                }
                                matches.add(key);
                            }
                        }
                        if (!any) {
                            break;
                        }
                    }
                }
            }
        }
        return matches;
    }

    /** Copy one field value out of a row (BinaryString/byte[] are copied, never aliased). */
    private static Object copyFieldValue(InternalRow row, RowType rowType, int index) {
        DataType type = rowType.getFields().get(index).getType();
        DataTypeRoot root = type.getTypeRoot();
        return switch (root) {
            case STRING -> BinaryString.fromString(row.getString(index).toString());
            case BIGINT -> row.getLong(index);
            case INTEGER -> row.getInt(index);
            case DOUBLE -> row.getDouble(index);
            case BOOLEAN -> row.getBoolean(index);
            case BYTES -> row.getBytes(index).clone();
            case TIMESTAMP_WITHOUT_TIME_ZONE ->
                row.getTimestampNtz(index, 6);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                row.getTimestampLtz(index, 6);
            case DATE -> row.getInt(index);
            case DECIMAL -> row.getDecimal(index, 38, 18);
            case CHAR -> row.getChar(index, 1).copy();
            case TINYINT -> row.getByte(index);
            case SMALLINT -> row.getShort(index);
            case FLOAT -> row.getFloat(index);
            case TIME_WITHOUT_TIME_ZONE -> row.getInt(index);
            default -> throw new IllegalArgumentException("unsupported row type " + root);
        };
    }

    /** Delete KV fixture rows by primary key (LOG rows are undeletable). */
    private static int deleteKvFixtures(Table table, TableInfo info,
            List<Object[]> rows) throws Exception {
        int deleted = 0;
        UpsertWriter writer = table.newUpsert().createWriter();
        for (Object[] key : rows) {
            // The delete writer validates arity against the FULL schema;
            // only the PK fields are used to identify the row.
            writer.delete(GenericRow.of(key))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            deleted++;
        }
        return deleted;
    }

    /**
     * Detect (and with {@code --sweep-fix-kv} repair) fixture rows left by the
     * pre-CHG-100 smoke path. Exit 0 when no fixture rows remain (or all KV
     * fixtures were deleted); exit 3 when LOG fixtures remain (undeletable —
     * operator must re-create the table).
     */
    static int sweep(Connection connection, Admin admin, Options op) throws Exception {
        List<String> live = admin.listTables("default")
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        List<String> scoped = op.sweepTables.isEmpty() ? live : op.sweepTables;
        List<String> kvReports = new ArrayList<>();
        List<String> logReports = new ArrayList<>();
        int kvTotal = 0;
        int kvDeleted = 0;
        int scanned = 0;
        for (String name : scoped) {
            TableInfo info;
            try {
                info = info(admin, name);
            } catch (Exception e) {
                System.err.println("ddl-apply: sweep: " + name + " skipped: "
                        + e.getMessage());
                continue;
            }
            Table table = connection.getTable(TablePath.of("default", name));
            scanned++;
            List<Object[]> matches = scanFixtures(table, info);
            if (matches.isEmpty()) {
                continue;
            }
            if (info.hasPrimaryKey()) {
                kvTotal += matches.size();
                kvReports.add(name + ": " + matches.size() + " fixture row(s)");
                if (op.sweepFixKv) {
                    kvDeleted += deleteKvFixtures(table, info, matches);
                }
            } else {
                logReports.add(name + ": " + matches.size()
                        + " fixture row(s) — LOG rows are undeletable; re-create the table");
            }
        }
        boolean kvFixed = kvTotal == kvDeleted;
        boolean clean = logReports.isEmpty() && (kvTotal == 0 || (op.sweepFixKv && kvFixed));
        if (!clean) {
            System.err.println("ddl-apply: SWEEP POLLUTED TABLES:");
            kvReports.forEach(f -> System.err.println("  [KV] " + f));
            logReports.forEach(f -> System.err.println("  [LOG] " + f));
        }
        if (op.sweepFixKv && kvDeleted > 0) {
            System.out.println("ddl-apply: sweep deleted " + kvDeleted
                    + " KV fixture row(s)");
        }
        int exit = clean ? 0 : 3;
        System.out.println("ddl-apply: SWEEP RESULT=" + (clean ? "CLEAN" : "POLLUTED")
                + " EXIT=" + exit + " SCANNED=" + scanned);
        return exit;
    }

    record Options(Path ddlDir, String bootstrap, Path evidenceOut, String prefix,
                   String cleanupPrefix, String flinkVersion, String flussVersion,
                   boolean skipSmoke, boolean sweepOnly, boolean sweepFixKv,
                   boolean allowLiveSmoke, List<String> sweepTables,
                   List<String> ackLimitations) {

        static Options parse(String[] args) {
            String ddlDir = null;
            String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
            String evidenceOut = null;
            String prefix = null;
            String cleanupPrefix = null;
            String flinkVersion = System.getenv().getOrDefault("FLINK_VERSION", "unknown");
            String flussVersion = System.getenv().getOrDefault("FLUSS_VERSION", "unknown");
            boolean skipSmoke = false;
            boolean sweepOnly = false;
            boolean sweepFixKv = false;
            boolean allowLiveSmoke = false;
            List<String> sweepTables = new ArrayList<>();
            List<String> ackLimitations = new ArrayList<>();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--ddl-dir" -> { ddlDir = needValue(args, i, "--ddl-dir"); i++; }
                    case "--bootstrap" -> { bootstrap = needValue(args, i, "--bootstrap"); i++; }
                    case "--evidence-out" -> {
                        evidenceOut = needValue(args, i, "--evidence-out"); i++;
                    }
                    case "--table-prefix" -> { prefix = needValue(args, i, "--table-prefix"); i++; }
                    case "--cleanup-prefix" -> {
                        cleanupPrefix = needValue(args, i, "--cleanup-prefix"); i++;
                    }
                    case "--flink-version" -> {
                        flinkVersion = needValue(args, i, "--flink-version"); i++;
                    }
                    case "--fluss-version" -> {
                        flussVersion = needValue(args, i, "--fluss-version"); i++;
                    }
                    case "--skip-smoke" -> skipSmoke = true;
                    case "--sweep" -> sweepOnly = true;
                    case "--sweep-fix-kv" -> sweepFixKv = true;
                    case "--allow-live-smoke" -> allowLiveSmoke = true;
                    case "--sweep-table" -> {
                        sweepTables.add(needValue(args, i, "--sweep-table")); i++;
                    }
                    // "auto" = confirm-only: the tool fills the composite-PK
                    // tables detected from the manifest (no name guessing).
                    case "--ack-limitations" -> {
                        for (String t : needValue(args, i, "--ack-limitations").split(",")) {
                            String trimmed = t.trim();
                            if (!trimmed.isEmpty()) {
                                ackLimitations.add(trimmed);
                            }
                        }
                        i++;
                    }
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            if (ddlDir == null && !sweepOnly) {
                throw new IllegalArgumentException("--ddl-dir is required");
            }
            if (prefix != null && !prefix.matches("[a-zA-Z0-9_]+")) {
                throw new IllegalArgumentException("--table-prefix must be [a-zA-Z0-9_]+");
            }
            if (cleanupPrefix != null && !cleanupPrefix.matches("[a-zA-Z0-9_]+")) {
                throw new IllegalArgumentException("--cleanup-prefix must be [a-zA-Z0-9_]+");
            }
            return new Options(ddlDir == null ? null : Path.of(ddlDir), bootstrap,
                    evidenceOut == null ? null : Path.of(evidenceOut), prefix,
                    cleanupPrefix, flinkVersion, flussVersion, skipSmoke,
                    sweepOnly, sweepFixKv, allowLiveSmoke,
                    List.copyOf(sweepTables), List.copyOf(ackLimitations));
        }

        /** Value-taking option must have a following value (usage error, not AIOOBE). */
        private static String needValue(String[] args, int i, String flag) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException(flag + " requires a value");
            }
            return args[i + 1];
        }
    }

    static final class TableRecord {
        final String logicalName;
        final String physicalName;
        final long tableId;
        final String kind;
        final int bucketCount;
        final boolean parityOk;
        String smokeStatus;
        String smokeNote;

        TableRecord(String logicalName, String physicalName, long tableId, String kind,
                    int bucketCount, boolean parityOk) {
            this.logicalName = logicalName;
            this.physicalName = physicalName;
            this.tableId = tableId;
            this.kind = kind;
            this.bucketCount = bucketCount;
            this.parityOk = parityOk;
            this.smokeStatus = "PASS";
            this.smokeNote = null;
        }

        void smoke(String outcome) {
            if (outcome == null) {
                smokeStatus = "PASS";
            } else if (isKnownLimitation(outcome)) {
                smokeStatus = "LIMITATION";
                smokeNote = outcome;
            } else {
                smokeStatus = "FAIL";
                smokeNote = outcome;
            }
        }
    }
}
