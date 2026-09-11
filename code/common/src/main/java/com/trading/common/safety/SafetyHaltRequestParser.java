package com.trading.common.safety;

import java.util.Map;

/**
 * Parses a {@code Safety_Halt_Requests} KV row (column-name &rarr; value map,
 * as bridged from the Flink source row) into a {@link SlotSafetyRequest}.
 *
 * <p>Column names and value domain mirror the DDL
 * (code/01_platform/02_sql/ddl/18_safety_halt_requests.sql, schema v3) and
 * the ingestion writer (SafetyHaltWriter, contract_version 2). Malformed
 * rows throw {@link ParseException} — the Flink job converts that into a
 * metric and skips the row; it never crashes the pipeline.
 */
public final class SafetyHaltRequestParser {

    /** DDL / writer column names. */
    public static final String COL_HALT_REQUEST_ID = "halt_request_id";
    public static final String COL_SOURCE_COMPONENT = "source_component";
    public static final String COL_SLOT_ID = "slot_id";
    public static final String COL_CONNECTION_EPOCH = "connection_epoch";
    public static final String COL_STATE = "state";
    public static final String COL_REASON_CODE = "reason_code";
    public static final String COL_MANIFEST_FINGERPRINT = "manifest_fingerprint";
    public static final String COL_ASSIGNED_TOKEN_SET_HASH = "assigned_token_set_hash";
    public static final String COL_DETECTION_TIME = "detection_time";
    public static final String COL_CONTRACT_VERSION = "contract_version";

    private SafetyHaltRequestParser() {}

    /**
     * @param row column-name &rarr; value; strings as {@code String}, numbers
     *            as {@code Integer}/{@code Long}/{@code BigDecimal}
     * @throws ParseException if a required field is missing, a value is
     *         malformed, the state is not UNSAFE/RECOVERED, contract_version
     *         is not 2, or an UNSAFE row carries no reason
     */
    public static SlotSafetyRequest parse(Map<String, Object> row) {
        if (row == null) {
            throw new ParseException("row must not be null");
        }
        int contractVersion = readInt(row, COL_CONTRACT_VERSION);
        if (contractVersion != SlotSafetyRequest.CONTRACT_VERSION) {
            throw new ParseException(COL_CONTRACT_VERSION + " must be "
                    + SlotSafetyRequest.CONTRACT_VERSION + ", got " + contractVersion);
        }
        String state = readString(row, COL_STATE);
        SlotSafetyStatus status;
        try {
            status = SlotSafetyStatus.valueOf(state);
        } catch (IllegalArgumentException e) {
            throw new ParseException(COL_STATE + " must be UNSAFE or RECOVERED, got '" + state + "'");
        }
        String reasonCode = readStringOrEmpty(row, COL_REASON_CODE);
        if (status == SlotSafetyStatus.UNSAFE && reasonCode.isBlank()) {
            throw new ParseException(COL_REASON_CODE + " is required for UNSAFE rows");
        }
        try {
            return new SlotSafetyRequest(
                    readString(row, COL_HALT_REQUEST_ID),
                    readString(row, COL_SOURCE_COMPONENT),
                    readString(row, COL_SLOT_ID),
                    readLong(row, COL_CONNECTION_EPOCH),
                    status,
                    reasonCode,
                    readString(row, COL_MANIFEST_FINGERPRINT),
                    readString(row, COL_ASSIGNED_TOKEN_SET_HASH),
                    readLong(row, COL_DETECTION_TIME),
                    contractVersion);
        } catch (ParseException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new ParseException("invalid safety row: " + e.getMessage());
        } catch (RuntimeException e) {
            // P3-345: never let a non-IAE escape as-is — the Flink path only
            // catches ParseException, anything else would kill the task.
            throw new ParseException("invalid safety row: " + e);
        }
    }

    /** A malformed safety row; carries a human-readable reason. */
    public static final class ParseException extends RuntimeException {
        public ParseException(String message) {
            super(message);
        }
    }

    private static String readString(Map<String, Object> row, String col) {
        Object value = row.get(col);
        if (!(value instanceof String s) || s.isBlank()) {
            throw new ParseException(col + " must be a non-blank string, got " + describe(value));
        }
        return s;
    }

    private static String readStringOrEmpty(Map<String, Object> row, String col) {
        Object value = row.get(col);
        // P3-346: missing/null coerces to "" (RECOVERED carries ""), but a
        // wrongly-typed value is a writer schema violation — fail, don't hide.
        if (value == null) {
            return "";
        }
        if (value instanceof String s) {
            return s;
        }
        throw new ParseException(col + " must be a string, got " + describe(value));
    }

    private static long readLong(Map<String, Object> row, String col) {
        Object value = row.get(col);
        // P3-121: strict integral parsing — longValue() truncation turned
        // contract_version 2.9 into 2 and mangled epochs on this safety path.
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof Short s) {
            return s;
        }
        if (value instanceof Byte b) {
            return b;
        }
        if (value instanceof java.math.BigInteger bi) {
            try {
                return bi.longValueExact();
            } catch (ArithmeticException e) {
                throw new ParseException(col + " must be an integral long, got " + describe(value));
            }
        }
        if (value instanceof java.math.BigDecimal bd) {
            try {
                return bd.longValueExact();
            } catch (ArithmeticException e) {
                throw new ParseException(col + " must be an integral long, got " + describe(value));
            }
        }
        if (value instanceof Double d) {
            if (!Double.isFinite(d) || d % 1 != 0 || d < Long.MIN_VALUE || d > Long.MAX_VALUE) {
                throw new ParseException(col + " must be an integral long, got " + describe(value));
            }
            return d.longValue();
        }
        if (value instanceof Float f) {
            if (!Float.isFinite(f) || f % 1 != 0 || f < Long.MIN_VALUE || f > Long.MAX_VALUE) {
                throw new ParseException(col + " must be an integral long, got " + describe(value));
            }
            return f.longValue();
        }
        throw new ParseException(col + " must be a number, got " + describe(value));
    }

    private static int readInt(Map<String, Object> row, String col) {
        Object value = row.get(col);
        // P3-121: same strictness for contract_version — Long>int-max or
        // fractional must reject, never wrap into a passing version.
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof Short s) {
            return s;
        }
        if (value instanceof Byte b) {
            return b;
        }
        if (value instanceof Long l) {
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                throw new ParseException(col + " out of int range, got " + describe(value));
            }
            return l.intValue();
        }
        if (value instanceof java.math.BigInteger bi) {
            try {
                return bi.intValueExact();
            } catch (ArithmeticException e) {
                throw new ParseException(col + " must be an integral int, got " + describe(value));
            }
        }
        if (value instanceof java.math.BigDecimal bd) {
            try {
                return bd.intValueExact();
            } catch (ArithmeticException e) {
                throw new ParseException(col + " must be an integral int, got " + describe(value));
            }
        }
        if (value instanceof Double d) {
            if (!Double.isFinite(d) || d % 1 != 0 || d < Integer.MIN_VALUE || d > Integer.MAX_VALUE) {
                throw new ParseException(col + " must be an integral int, got " + describe(value));
            }
            return d.intValue();
        }
        if (value instanceof Float f) {
            if (!Float.isFinite(f) || f % 1 != 0 || f < Integer.MIN_VALUE || f > Integer.MAX_VALUE) {
                throw new ParseException(col + " must be an integral int, got " + describe(value));
            }
            return f.intValue();
        }
        throw new ParseException(col + " must be a number, got " + describe(value));
    }

    private static String describe(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + "('" + value + "')";
    }
}
