package com.trading.common.schema.eod;

/**
 * Outcome of one offload attempt (SCH-23): the content evidence the
 * controller records on the day before advancing it to {@code COMMITTED} —
 * source offset range, row/byte counts, source + target hashes, and the lake
 * snapshot/commit id (the offload-record fields from 01-foundation.md "EOD
 * controller and offload gate"). {@link #failure} carries the reason the day
 * stays {@code FAILED_RETRYABLE}.
 */
public record OffloadResult(
        boolean success,
        long sourceOffsetStart,
        long sourceOffsetEnd,
        long rowCount,
        long byteCount,
        String sourceHash,
        String targetHash,
        String icebergSnapshotId,
        String error) {
    public OffloadResult {
        if (rowCount < 0 || byteCount < 0) {
            throw new IllegalArgumentException("rowCount/byteCount must be >= 0");
        }
        if (sourceOffsetStart != -1 && sourceOffsetEnd != -1 && sourceOffsetStart > sourceOffsetEnd) {
            throw new IllegalArgumentException("sourceOffsetStart > sourceOffsetEnd");
        }
        sourceHash = sourceHash == null ? "" : sourceHash;
        targetHash = targetHash == null ? "" : targetHash;
        icebergSnapshotId = icebergSnapshotId == null ? "" : icebergSnapshotId;
        if (success) {
            if (error != null && !error.isEmpty()) {
                throw new IllegalArgumentException("success result must not carry error");
            }
        } else if (error == null || error.isBlank()) {
            throw new IllegalArgumentException("failure result requires non-blank error");
        }
    }

    public static OffloadResult failure(String error) {
        if (error == null || error.isBlank()) {
            throw new IllegalArgumentException("failure requires non-blank error");
        }
        return new OffloadResult(false, -1, -1, 0, 0, "", "", "", error);
    }
}
