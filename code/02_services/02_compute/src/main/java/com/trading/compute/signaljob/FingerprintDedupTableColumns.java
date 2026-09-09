package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the authoritative dedup state table
 * {@code fingerprint_dedup} (DEC-038; the design is
 * {@code docs/08_implementation/04-signal-job.md} §Design — fingerprint_dedup
 * dedup state table).
 *
 * <p>Must mirror the frozen 6-column layout of
 * {@code code/01_platform/02_sql/ddl/24_fingerprint_dedup.sql} v1 (KV, PK
 * {@code (instrument_token, fingerprint_version, event_fingerprint)},
 * {@code bucket.key = instrument_token}): key holds only identity; value holds
 * only {@code (first_seen_ms, expiry_ms)} — no raw bytes, decoded fields,
 * candle/candidate values, or event objects (SIG-UNIT-008). Pinned by
 * {@code FingerprintDedupTableColumnsAgreementTest} against the DDL file
 * itself (cross-boundary pin habit).
 */
public final class FingerprintDedupTableColumns {

    private FingerprintDedupTableColumns() {}

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int FINGERPRINT_VERSION = 1;
    public static final int EVENT_FINGERPRINT = 2;
    public static final int FIRST_SEEN_MS = 3;
    public static final int EXPIRY_MS = 4;
    public static final int SCHEMA_VERSION = 5;

    public static final int FIELD_COUNT = 6;

    public static final String SCHEMA_VERSION_V1 = "1";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "BIGINT", "STRING", "STRING", "BIGINT", "BIGINT", "STRING");

    /** DDL nullability per column (24 DDL v1): all NOT NULL. */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). Do not expose mutably. */
    private static final String[] NAMES_INTERNAL = {
        "instrument_token", "fingerprint_version", "event_fingerprint",
        "first_seen_ms", "expiry_ms", "schema_version"
    };

    public static final List<String> NAMES = List.of(NAMES_INTERNAL);

    public static String[] names() {
        return NAMES_INTERNAL.clone();
    }

    /** Stream type info for emitted dedup rows, derived from the v1 DDL order. */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(),                        // instrument_token
                new VarCharType(VarCharType.MAX_LENGTH), // fingerprint_version
                new VarCharType(VarCharType.MAX_LENGTH), // event_fingerprint
                new BigIntType(),                        // first_seen_ms
                new BigIntType(),                        // expiry_ms
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES_INTERNAL);
}
