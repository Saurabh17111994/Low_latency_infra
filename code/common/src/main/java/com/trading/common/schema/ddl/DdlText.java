package com.trading.common.schema.ddl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

/**
 * Lightweight parser for the project's regular DDL corpus
 * ({@code code/01_platform/02_sql/ddl/*.sql}) plus the Fluss admin-API
 * {@link TableDescriptor} builder.
 *
 * <p>Fluss 0.9.1 has no SQL client, so "parse against the pinned dialect"
 * means deriving the admin-API descriptor from the DDL text — columns, primary
 * key, partition keys, bucket key, bucket count, and WITH options — and applying it through
 * {@code Admin.createTable}. Shared by {@link DdlApplyTool} (the DDL
 * application contract) and the COMPAT-FLUSS-001 parity test.
 *
 * <p>Dev deviation (deliberate, matches the live dev cluster): when a DDL
 * declares {@code table.datalake.enabled=true}, the applied descriptor forces
 * {@code false} — Fluss 0.9.1 lake-enable is create-only and collides with
 * orphaned R2 lake objects. Production DDLs keep {@code enabled=true} (the
 * blueprint); the dev cluster and this parser deviate, documented in
 * docs/08_implementation/02-schema-storage.md Phase C lake-state note.
 */
public final class DdlText {

    private DdlText() {}

    /** Parsed DDL model. */
    public record ParsedDdl(String tableName, List<Column> columns, List<String> primaryKey,
                            int bucketCount, String bucketKey, Map<String, String> options,
                            String sourcePath, List<String> partitionKeys) {

        /** Backward-compatible constructor for synthetic test fixtures. */
        public ParsedDdl(String tableName, List<Column> columns, List<String> primaryKey,
                         int bucketCount, String bucketKey, Map<String, String> options,
                         String sourcePath) {
            this(tableName, columns, primaryKey, bucketCount, bucketKey, options, sourcePath,
                    List.of());
        }

        public boolean isKv() {
            return !primaryKey.isEmpty();
        }
    }

    /** One column: name + Fluss type (nullability is not expressible in the client schema). */
    public record Column(String name, DataType type) {}

    private static final Pattern CREATE_TABLE = Pattern.compile(
            "CREATE TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?[`\"\\[]?([\\w.]+)[`\"\\]]?",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PRIMARY_KEY = Pattern.compile("PRIMARY KEY\\s*\\(([^)]+)\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PARTITIONED_BY =
            Pattern.compile("PARTITIONED\\s+BY\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMN_LINE = Pattern.compile(
            "^\\s*([a-zA-Z0-9_]+)\\s+([a-zA-Z]+(?:\\s*\\([^)]*\\))?)\\s*(?:NOT\\s+NULL|NULL)?\\s*,?\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern WITH_CLAUSE =
            Pattern.compile("\\)\\s*WITH\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern OPTION =
            Pattern.compile("'([a-zA-Z0-9_.-]+)'\\s*=\\s*'([^']*)'");

    /** Parse a DDL file's text into the model; throws on structural problems. */
    public static ParsedDdl parse(String text, String sourcePath) {
        Matcher create = CREATE_TABLE.matcher(text);
        if (!create.find()) {
            throw new IllegalArgumentException(sourcePath + ": no CREATE TABLE");
        }
        String tableName = create.group(1);
        int bodyStart = text.indexOf('(', create.end());
        Matcher with = WITH_CLAUSE.matcher(text);
        int withIdx = -1;
        int searchFrom = Math.max(bodyStart, 0);
        while (with.find(searchFrom)) {
            int candidate = with.start();
            String between = text.substring(bodyStart + 1, candidate);
            if (parenDepth(between) == 0) {
                withIdx = candidate;
                break;
            }
            searchFrom = with.end();
        }
        int bodyEnd = withIdx >= 0 ? withIdx : text.lastIndexOf(')');
        if (bodyStart < 0 || bodyEnd < bodyStart) {
            throw new IllegalArgumentException(sourcePath + ": cannot delimit column block");
        }
        String body = text.substring(bodyStart + 1, bodyEnd);
        Matcher partitionAnywhere = PARTITIONED_BY.matcher(text);
        if (partitionAnywhere.find()) {
            String keys = partitionAnywhere.group(1);
            int cut = body.toUpperCase(java.util.Locale.ROOT)
                    .indexOf("PARTITIONED BY (".concat(keys).toUpperCase(java.util.Locale.ROOT));
            if (cut < 0) {
                cut = body.toUpperCase(java.util.Locale.ROOT).indexOf("PARTITIONED BY");
            }
            if (cut >= 0) {
                body = body.substring(0, cut);
            }
        }
        String partitionClause = partitionAnywhere.reset().find()
                ? partitionAnywhere.group(0) : "";

        List<Column> columns = new ArrayList<>();
        for (String line : body.split("\\n")) {
            // Inline `--` comments (e.g. `ack_ts BIGINT NULL, -- 0 = unknown (R-010)`)
            // must be stripped BEFORE matching — otherwise the trailing comment
            // breaks the end-anchored COLUMN_LINE regex and the column is
            // silently dropped from the applied schema (DdlText parser bug,
            // fixed 2026-08-28: apply created raw_table_1 with 19 cols, missing
            // ack_ts, which failed ingestion DdlBootstrap's 20-col expectation).
            String bare = line.indexOf("--") >= 0
                    ? line.substring(0, line.indexOf("--")) : line;
            if (bare.trim().isEmpty() || bare.trim().equals("(") || bare.trim().equals(")")) {
                continue;
            }
            if (bare.trim().regionMatches(true, 0, "PRIMARY KEY", 0, "PRIMARY KEY".length())) {
                continue;
            }
            Matcher col = COLUMN_LINE.matcher(bare);
            if (col.matches()) {
                columns.add(new Column(col.group(1), type(col.group(2), sourcePath)));
            } else {
                throw new IllegalArgumentException(
                        sourcePath + ": unparsable column line: " + line.trim());
            }
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException(sourcePath + ": no columns parsed");
        }
        java.util.Set<String> columnNames = new java.util.HashSet<>();
        for (Column c : columns) {
            if (!columnNames.add(c.name().toLowerCase(java.util.Locale.ROOT))) {
                throw new IllegalArgumentException(sourcePath + ": duplicate column " + c.name());
            }
        }

        Matcher pk = PRIMARY_KEY.matcher(body);
        List<String> primaryKey = new ArrayList<>();
        if (pk.find()) {
            for (String part : pk.group(1).split(",")) {
                String key = part.trim();
                if (key.isEmpty()) {
                    throw new IllegalArgumentException(sourcePath + ": empty primary-key entry");
                }
                if (!columnNames.contains(key.toLowerCase(java.util.Locale.ROOT))) {
                    throw new IllegalArgumentException(
                            sourcePath + ": primary key not a column: " + key);
                }
                if (primaryKey.contains(key)) {
                    throw new IllegalArgumentException(
                            sourcePath + ": duplicate primary-key entry: " + key);
                }
                primaryKey.add(key);
            }
        }

        List<String> partitionKeys = new ArrayList<>();
        Matcher partition = PARTITIONED_BY.matcher(partitionClause);
        if (partition.find()) {
            for (String part : partition.group(1).split(",")) {
                String key = part.trim();
                if (key.isEmpty()) {
                    throw new IllegalArgumentException(sourcePath + ": empty partition key");
                }
                if (!columnNames.contains(key.toLowerCase(java.util.Locale.ROOT))) {
                    throw new IllegalArgumentException(
                            sourcePath + ": partition key not a column: " + key);
                }
                partitionKeys.add(key);
            }
        }

        Map<String, String> options = new HashMap<>();
        if (withIdx >= 0) {
            Matcher opt = OPTION.matcher(text.substring(withIdx));
            while (opt.find()) {
                options.put(opt.group(1), opt.group(2));
            }
        }
        String bucketKey = options.getOrDefault("bucket.key", "");
        if (bucketKey.isBlank()) {
            throw new IllegalArgumentException(sourcePath + ": no bucket.key option");
        }
        int bucketCount;
        try {
            bucketCount = Integer.parseInt(options.getOrDefault("bucket.num", "1"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(sourcePath + ": bad bucket.num", e);
        }
        if (bucketCount <= 0) {
            throw new IllegalArgumentException(
                    sourcePath + ": bucket.num must be positive, got " + bucketCount);
        }
        List<String> bucketColumns = new ArrayList<>();
        for (String part : bucketKey.split(",")) {
            String key = part.trim();
            if (key.isEmpty()) {
                throw new IllegalArgumentException(sourcePath + ": empty bucket-key entry");
            }
            if (!columnNames.contains(key.toLowerCase(java.util.Locale.ROOT))) {
                throw new IllegalArgumentException(
                        sourcePath + ": bucket key not a column: " + key);
            }
            bucketColumns.add(key);
        }
        return new ParsedDdl(tableName, List.copyOf(columns), List.copyOf(primaryKey),
                bucketCount, String.join(",", bucketColumns), Map.copyOf(options), sourcePath,
                List.copyOf(partitionKeys));
    }

    /** Build the admin-API descriptor that applies the parsed DDL to Fluss. */
    public static TableDescriptor toDescriptor(ParsedDdl ddl) {
        return toDescriptor(ddl, true);
    }

    /**
     * Build the descriptor, optionally forcing {@code table.datalake.enabled=false}
     * (the dev-cluster deviation; callers with a lake-wired cluster pass {@code false}
     * to honor the blueprint).
     */
    public static TableDescriptor toDescriptor(ParsedDdl ddl, boolean forceDatalakeDisabled) {
        Schema.Builder sb = Schema.newBuilder();
        for (Column c : ddl.columns()) {
            sb.column(c.name(), c.type());
        }
        if (!ddl.primaryKey().isEmpty()) {
            sb.primaryKey(ddl.primaryKey().toArray(new String[0]));
        }
        TableDescriptor.Builder tb = TableDescriptor.builder().schema(sb.build())
                // bucket.num and bucket.key are distribution concerns expressed
                // via distributedBy, NOT table properties — Fluss rejects them
                // as properties (InvalidConfigException).
                .distributedBy(ddl.bucketCount(), ddl.bucketKey().split(","));
        if (!ddl.partitionKeys().isEmpty()) {
            tb.partitionedBy(ddl.partitionKeys());
        }
        ddl.options().forEach((key, value) -> {
            if (key.equals("bucket.num") || key.equals("bucket.key")) {
                return;
            }
            if (key.equals("table.datalake.enabled")) {
                tb.property(key, forceDatalakeDisabled ? "false" : value); // dev deviation — see class javadoc
            } else {
                tb.property(key, value);
            }
        });
        return tb.build();
    }

    private static DataType type(String name, String sourcePath) {
        String base = name.replaceAll("\\s*\\(.*\\)$", "").toUpperCase(java.util.Locale.ROOT);
        return switch (base) {
            case "STRING" -> DataTypes.STRING();
            case "BIGINT" -> DataTypes.BIGINT();
            case "INT" -> DataTypes.INT();
            case "BYTES" -> DataTypes.BYTES();
            case "DOUBLE" -> DataTypes.DOUBLE();
            case "BOOLEAN" -> DataTypes.BOOLEAN();
            case "TIMESTAMP", "TIMESTAMP_LTZ" -> DataTypes.TIMESTAMP_LTZ();
            case "DATE" -> DataTypes.DATE();
            case "DECIMAL" -> {
                Matcher params = Pattern.compile("\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)")
                        .matcher(name);
                if (params.find()) {
                    yield DataTypes.DECIMAL(Integer.parseInt(params.group(1)),
                            Integer.parseInt(params.group(2)));
                }
                yield DataTypes.DECIMAL(38, 18);
            }
            case "VARCHAR", "CHAR" -> DataTypes.STRING();
            case "TINYINT" -> DataTypes.TINYINT();
            case "SMALLINT" -> DataTypes.SMALLINT();
            case "FLOAT" -> DataTypes.FLOAT();
            case "TIME" -> DataTypes.TIME();
            default -> throw new IllegalArgumentException(
                    sourcePath + ": unknown DDL type " + name);
        };
    }

    private static int parenDepth(String text) {
        int depth = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
        }
        return depth;
    }
}
