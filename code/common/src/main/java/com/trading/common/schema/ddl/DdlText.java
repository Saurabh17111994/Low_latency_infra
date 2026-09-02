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

    private static final Pattern CREATE_TABLE = Pattern.compile("CREATE TABLE\\s+(\\w+)");
    private static final Pattern PRIMARY_KEY = Pattern.compile("PRIMARY KEY\\s*\\(([^)]+)\\)");
    private static final Pattern PARTITIONED_BY =
            Pattern.compile("PARTITIONED\\s+BY\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMN_LINE =
            Pattern.compile("^\\s*([a-zA-Z0-9_]+)\\s+([A-Z]+)\\s*(?:NOT\\s+NULL|NULL)?\\s*,?\\s*$");
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
        int withIdx = text.indexOf(") WITH", bodyStart);
        int bodyEnd = withIdx >= 0 ? withIdx : text.lastIndexOf(')');
        if (bodyStart < 0 || bodyEnd < bodyStart) {
            throw new IllegalArgumentException(sourcePath + ": cannot delimit column block");
        }
        String body = text.substring(bodyStart + 1, bodyEnd);

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
            if (bare.trim().startsWith("PRIMARY KEY")) {
                continue;
            }
            Matcher col = COLUMN_LINE.matcher(bare);
            if (col.matches()) {
                columns.add(new Column(col.group(1), type(col.group(2))));
            }
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException(sourcePath + ": no columns parsed");
        }

        Matcher pk = PRIMARY_KEY.matcher(body);
        List<String> primaryKey = new ArrayList<>();
        if (pk.find()) {
            for (String part : pk.group(1).split(",")) {
                primaryKey.add(part.trim());
            }
        }

        List<String> partitionKeys = new ArrayList<>();
        Matcher partition = PARTITIONED_BY.matcher(text);
        if (partition.find()) {
            for (String part : partition.group(1).split(",")) {
                String key = part.trim();
                if (key.isEmpty()) {
                    throw new IllegalArgumentException(sourcePath + ": empty partition key");
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
        return new ParsedDdl(tableName, List.copyOf(columns), List.copyOf(primaryKey),
                bucketCount, bucketKey, Map.copyOf(options), sourcePath,
                List.copyOf(partitionKeys));
    }

    /** Build the admin-API descriptor that applies the parsed DDL to Fluss. */
    public static TableDescriptor toDescriptor(ParsedDdl ddl) {
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
                tb.property(key, "false"); // dev deviation — see class javadoc
            } else {
                tb.property(key, value);
            }
        });
        return tb.build();
    }

    private static DataType type(String name) {
        return switch (name) {
            case "STRING" -> DataTypes.STRING();
            case "BIGINT" -> DataTypes.BIGINT();
            case "INT" -> DataTypes.INT();
            case "BYTES" -> DataTypes.BYTES();
            case "DOUBLE" -> DataTypes.DOUBLE();
            case "BOOLEAN" -> DataTypes.BOOLEAN();
            default -> throw new IllegalArgumentException("unknown DDL type " + name);
        };
    }
}
