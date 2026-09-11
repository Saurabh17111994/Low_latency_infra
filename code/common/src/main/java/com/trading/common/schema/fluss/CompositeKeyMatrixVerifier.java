package com.trading.common.schema.fluss;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

/**
 * COMPAT-FLUSS-005: the raw-client composite-PK upsert matrix, as executable
 * verification. Shared by the env-gated JUnit test
 * ({@code CompatFlussCompositeKeyIntegrationTest}) AND the DDL apply engine
 * ({@code com.trading.common.schema.ddl.DdlApplyTool}) so the matrix is verified
 * IN-BAND during an apply — not merely referenced as capability evidence.
 *
 * <p>Fluss 0.9.1-incubating's raw client encodes KV keys through
 * {@code KeyEncoder.ofPrimaryKeyEncoder}. When the (cluster-inherited) datalake
 * format is iceberg, a composite primary key is writable ONLY when
 * {@code table.kv.format-version=2} AND the bucket key is a single-field subset
 * of the PK ({@code CompactedKeyEncoder}); otherwise {@code IcebergKeyEncoder}
 * throws {@value #ICEBERG_ERROR}. The pinned matrix (evidence 2026-08-15,
 * {@code logs/schema-compat/composite-pk-raw-client-20260815.md}):
 *
 * <pre>
 *   PK         bucket key           kv.format-version   raw-client upsert
 *   composite  = PK (default)       (absent → 1)        ✗ IcebergKeyEncoder error
 *   composite  = PK (default)       2                    ✗ same error
 *   composite  single-field subset  2                    ✓ PASS
 *   composite  single-field subset  1                    ✗ same error
 * </pre>
 *
 * <p>{@link #verify} creates the four scratch tables ({@code <base>_cell1..4}),
 * upserts + looks up each, drops them in a {@code finally}, and returns per-cell
 * outcomes + deviations. Any deviation is a deliberate matrix change: the
 * caller (apply or test) must fail and update the matrix + docs.
 */
public final class CompositeKeyMatrixVerifier {

    public static final String ICEBERG_ERROR =
            "Key fields must have exactly one field for iceberg format";

    /** One pinned matrix cell: config + the documented outcome. */
    public record CellSpec(String label, List<String> bucketKeys, String kvFormatVersion,
                           boolean expectedPass) {
        /** True when the observed outcome matches the documented cell outcome. */
        boolean matches(String outcome) {
            return expectedPass
                    ? "PASS".equals(outcome)
                    : outcome != null && outcome.contains(ICEBERG_ERROR);
        }
    }

    /** Observed outcome of one cell. */
    public record CellResult(String label, List<String> bucketKeys, String kvFormatVersion,
                             boolean expectedPass, String outcome, boolean matched) {}

    /** Full matrix outcome. {@code passed()} is false iff any cell deviated. */
    public record Result(List<CellResult> cells, boolean passed, List<String> deviations) {}

    /** The documented 4-cell matrix, in pinned order. */
    public static final List<CellSpec> MATRIX = List.of(
            new CellSpec("v1 + default bucket key", List.of("k1", "k2"), null, false),
            new CellSpec("v2 + default bucket key", List.of("k1", "k2"), "2", false),
            new CellSpec("v2 + single-field subset bucket key", List.of("k1"), "2", true),
            new CellSpec("v1 + single-field subset bucket key", List.of("k1"), "1", false));

    private static final Schema COMPOSITE_SCHEMA = Schema.newBuilder()
            .column("k1", DataTypes.STRING())
            .column("k2", DataTypes.STRING())
            .column("v", DataTypes.BIGINT())
            .primaryKey("k1", "k2")
            .build();

    private CompositeKeyMatrixVerifier() {}

    /**
     * Run the 4-cell matrix against the live cluster. Scratch tables are named
     * {@code <base>_cell1..4} and dropped in a {@code finally} (also on
     * failure). Returns the outcome; never throws for a cell deviation (an
     * unexpected create/connection failure surfaces as an exception to the
     * caller, which must treat it as a failure).
     */
    public static Result verify(Connection connection, Admin admin, String base,
            Duration timeout) throws Exception {
        List<CellResult> cells = new ArrayList<>();
        List<String> deviations = new ArrayList<>();
        List<String> created = new ArrayList<>();
        try {
            for (int i = 0; i < MATRIX.size(); i++) {
                CellSpec spec = MATRIX.get(i);
                String name = base + "_cell" + (i + 1);
                Table table;
                try {
                    table = createCompositeTable(admin, connection, name, spec, timeout);
                } finally {
                    // createTable succeeded server-side even if getTable failed
                    created.add(name);
                }
                String outcome = runCell(table, timeout);
                boolean matched = spec.matches(outcome);
                cells.add(new CellResult(spec.label(), spec.bucketKeys(), spec.kvFormatVersion(),
                        spec.expectedPass(), outcome, matched));
                if (!matched) {
                    deviations.add("cell " + (i + 1) + " (" + spec.label() + "): expected "
                            + (spec.expectedPass() ? "PASS" : "IcebergKeyEncoder failure")
                            + " but got: " + outcome);
                }
            }
        } finally {
            for (String name : created) {
                try {
                    admin.dropTable(TablePath.of("default", name), false)
                            .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Best-effort drop — a leftover scratch table is a nuisance,
                    // not a matrix failure.
                }
            }
        }
        return new Result(cells, deviations.isEmpty(), deviations);
    }

    /** Create a composite-PK scratch KV table with the given bucket key + kv format version. */
    private static Table createCompositeTable(Admin admin, Connection connection, String name,
            CellSpec spec, Duration timeout) throws Exception {
        TableDescriptor.Builder tb = TableDescriptor.builder()
                .schema(COMPOSITE_SCHEMA)
                // Iceberg key encoding is triggered by the (cluster-inherited)
                // datalake format; the table also declares format=iceberg
                // explicitly so the matrix does not depend on cluster defaults.
                // enabled stays false (pinned evidence 2026-08-15 shows the
                // IcebergKeyEncoder failure fires with format alone) — enabling
                // tiering on scratch tables risks real lake writes on dev.
                .property("table.datalake.enabled", "false")
                .property("table.datalake.format", "iceberg")
                .distributedBy(4, spec.bucketKeys().toArray(new String[0]));
        if (spec.kvFormatVersion() != null) {
            tb.property("table.kv.format-version", spec.kvFormatVersion());
        }
        TablePath path = TablePath.of("default", name);
        admin.createTable(path, tb.build(), false)
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        return connection.getTable(path);
    }

    /**
     * One matrix cell: upsert + lookup round-trip. Returns {@code "PASS"} or the
     * failure message (never throws) — the caller asserts the expected outcome.
     */
    private static String runCell(Table table, Duration timeout) {
        // NOTE (P4-145): UpsertWriter/Lookuper are NOT Closeable in this Fluss
        // version (only Table is AutoCloseable), so there is nothing to release
        // and no close() to call — the bounded get() below IS the whole write.
        // The flush that used to sit in a finally here was removed 2026-09-11:
        // it was a no-op on success (the ack had already been awaited) and
        // unbounded on failure, so it masked the very timeout it read as
        // guarding — a catch() cannot rescue an awaited latch (see
        // flush_guard.sh). Table lifecycle stays with the caller (verify drops
        // scratch tables in finally).
        try {
            // createWriter() is INSIDE the guard on purpose: the bucket key's
            // IcebergKeyEncoder is constructed here, not at upsert time, so the
            // documented composite-bucket-key cells (cells 1-2) throw at this
            // line. Left outside the guard, that throw escaped runCell entirely
            // and aborted the whole matrix as "matrix verification failed"
            // instead of being recorded as the cell's expected outcome.
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(GenericRow.of(
                            BinaryString.fromString("a"), BinaryString.fromString("b"), 7L))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow found = lookuper.lookup(
                            GenericRow.of(BinaryString.fromString("a"), BinaryString.fromString("b")))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
            if (found == null) {
                return "KV upsert not found by composite-PK lookup";
            }
            return found.getLong(2) == 7L ? "PASS" : "unexpected value " + found.getLong(2);
        } catch (Exception e) {
            StringBuilder chained = new StringBuilder();
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t.getMessage() != null) {
                    chained.append(t.getMessage()).append(" | ");
                } else {
                    chained.append(t.getClass().getSimpleName()).append(" | ");
                }
            }
            return chained.toString().split("\n")[0];
        }
    }
}
