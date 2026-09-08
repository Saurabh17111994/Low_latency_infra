package com.trading.common.schema.ownership;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Writer/column ownership matrix (DEC-005;
 * docs/04_contracts/02-storage.md &rarr; "Required schemas").
 *
 * <p>A KV table using {@code partial_update} assigns every column group to one
 * declared writer; cross-writer, untested updates are rejected by stale-version
 * guards (docs/02_requirements/02-functional/02-storage.md). This class is the
 * machine-checkable form of that contract: which columns are identity
 * (written exactly once at row creation, never in a partial update) and which
 * mutable column group each writer owns.
 *
 * <p>Invariants enforced at construction (fail-closed — an invalid matrix
 * cannot be constructed):
 * <ol>
 *   <li>identity columns and every writer's columns are in range and unique;</li>
 *   <li>no two writers share a column (one declared writer per column group);</li>
 *   <li>identity columns are never claimed by a writer (creation-only);</li>
 *   <li>the union of identity columns and all writer column groups covers every
 *       column — no unowned column.</li>
 * </ol>
 *
 * <p>{@link #checkWrite} is the enforcement helper a {@code partial_update}
 * writer calls before touching a row: any column outside the writer's group
 * (or an identity column) throws, so ownership drift fails loudly instead of
 * clobbering another writer's columns. Storage-layer merge behaviour itself is
 * observed live by COMPAT-FLUSS-003 (changelog records carry merged FULL row
 * images); this core pins the writer half until Executor-era writers exist
 * (SCH-15).
 */
public final class ColumnOwnership {

    /** One partial-update writer and the columns (DDL-pin indexes) it owns. */
    public static final class Writer {
        private final String name;
        private final int[] columns;

        public Writer(String name, int... columns) {
            this.name = Objects.requireNonNull(name, "writer name");
            if (this.name.isBlank()) {
                throw new IllegalArgumentException("writer name must be non-blank");
            }
            if (columns == null || columns.length == 0) {
                throw new IllegalArgumentException("writer " + name + " declares no columns");
            }
            this.columns = columns.clone();
        }

        public String name() {
            return name;
        }

        public int[] columns() {
            return columns.clone();
        }
    }

    private final String tableName;
    private final String schemaVersion;
    private final String owner;
    private final String[] columnNames;
    private final int[] identityColumns;
    private final Writer[] writers;

    /** Column index &rarr; owning writer name (identity columns absent). */
    private final Map<Integer, String> writerOfColumn = new HashMap<>();

    public ColumnOwnership(String tableName, String schemaVersion, String owner,
                           String[] columnNames, int[] identityColumns, Writer... writers) {
        this.tableName = requireNonBlank(tableName, "tableName");
        this.schemaVersion = requireNonBlank(schemaVersion, "schemaVersion");
        this.owner = requireNonBlank(owner, "owner");
        this.columnNames = Objects.requireNonNull(columnNames, "columnNames").clone();
        if (columnNames.length == 0) {
            throw new IllegalArgumentException("columnNames must not be empty");
        }
        for (String n : columnNames) {
            if (n == null || n.isBlank()) {
                throw new IllegalArgumentException("columnNames contains a blank name");
            }
        }
        this.identityColumns = validateUniqueInRange(
                Objects.requireNonNull(identityColumns, "identityColumns"), columnNames.length,
                "identityColumns");
        if (identityColumns.length == 0) {
            throw new IllegalArgumentException("identityColumns must not be empty");
        }
        if (writers == null || writers.length == 0) {
            throw new IllegalArgumentException("at least one writer is required");
        }
        this.writers = writers.clone();

        java.util.Set<Integer> identitySet = new java.util.HashSet<>();
        for (int idx : identityColumns) {
            identitySet.add(idx);
        }
        java.util.Set<String> seenWriterNames = new java.util.HashSet<>();
        for (Writer w : writers) {
            if (w == null) {
                throw new IllegalArgumentException("writers contains a null entry");
            }
            if (!seenWriterNames.add(w.name())) {
                throw new IllegalArgumentException("duplicate writer name: " + w.name());
            }
            int[] cols = validateUniqueInRange(w.columns(), columnNames.length, "writer " + w.name());
            for (int idx : cols) {
                if (identitySet.contains(idx)) {
                    throw new IllegalArgumentException(
                            "column " + describe(idx) + " is identity (creation-only) and must "
                                    + "not be claimed by writer " + w.name());
                }
                String prior = writerOfColumn.putIfAbsent(idx, w.name());
                if (prior != null) {
                    throw new IllegalArgumentException(
                            "column " + describe(idx) + " is owned by both " + prior + " and "
                                    + w.name() + " — one declared writer per column group (DEC-005)");
                }
            }
        }
        for (int i = 0; i < columnNames.length; i++) {
            if (!identitySet.contains(i) && !writerOfColumn.containsKey(i)) {
                throw new IllegalArgumentException(
                        "column " + describe(i) + " is owned by no writer and is not identity");
            }
        }
    }

    private static String requireNonBlank(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " must be non-blank");
        }
        return value;
    }

    private static int[] validateUniqueInRange(int[] indexes, int fieldCount, String what) {
        int[] out = indexes.clone();
        boolean[] seen = new boolean[fieldCount];
        for (int idx : out) {
            if (idx < 0 || idx >= fieldCount) {
                throw new IllegalArgumentException(what + " index " + idx + " out of range [0, "
                        + fieldCount + ")");
            }
            if (seen[idx]) {
                throw new IllegalArgumentException(what + " contains duplicate column " + idx);
            }
            seen[idx] = true;
        }
        return out;
    }

    private String describe(int columnIndex) {
        return columnIndex + " (" + columnNames[columnIndex] + ")";
    }

    public String tableName() {
        return tableName;
    }

    public String schemaVersion() {
        return schemaVersion;
    }

    /** The service that mints rows — it writes identity columns at creation. */
    public String owner() {
        return owner;
    }

    public String[] columnNames() {
        return columnNames.clone();
    }

    public int[] identityColumns() {
        return identityColumns.clone();
    }

    public Writer[] writers() {
        Writer[] out = new Writer[writers.length];
        for (int i = 0; i < writers.length; i++) {
            out[i] = new Writer(writers[i].name(), writers[i].columns());
        }
        return out;
    }

    public boolean isIdentity(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnNames.length) {
            throw new IllegalArgumentException("column index " + columnIndex + " out of range");
        }
        for (int idx : identityColumns) {
            if (idx == columnIndex) {
                return true;
            }
        }
        return false;
    }

    /** Owning writer for a mutable column; {@code null} for identity columns. */
    public String writerFor(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnNames.length) {
            throw new IllegalArgumentException("column index " + columnIndex + " out of range");
        }
        return writerOfColumn.get(columnIndex);
    }

    /**
     * Fail-closed guard for a {@code partial_update} writer: every listed column
     * must be owned by {@code writerName} and must not be an identity column.
     * Throws naming the first violating column (and its real owner) so a writer
     * can never clobber another writer's columns or rewrite identity.
     */
    public void checkWrite(String writerName, int... columnIndexes) {
        if (writerName == null || writerName.isBlank()) {
            throw new IllegalArgumentException("writerName must be non-blank");
        }
        if (columnIndexes == null) {
            throw new IllegalArgumentException("columnIndexes must not be null");
        }
        for (int idx : columnIndexes) {
            if (isIdentity(idx)) {
                throw new IllegalArgumentException(
                        "writer " + writerName + " may not partial-update identity column "
                                + describe(idx) + " (creation-only)");
            }
            String actual = writerFor(idx);
            if (!writerName.equals(actual)) {
                throw new IllegalArgumentException(
                        "writer " + writerName + " does not own column " + describe(idx)
                                + (actual == null ? "" : " (owned by " + actual + ")"));
            }
        }
    }

    @Override
    public String toString() {
        return "ColumnOwnership{" + tableName + " v" + schemaVersion + ", owner=" + owner
                + ", identity=" + Arrays.toString(identityColumns) + ", writers="
                + Arrays.stream(writers).map(w -> w.name() + Arrays.toString(w.columns()))
                        .toList() + "}";
    }
}
