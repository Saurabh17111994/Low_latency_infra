package com.trading.common.safety;

/**
 * One row of the {@code Safety_Halt_Requests} KV table as consumed by the
 * Signal Job's safety-halt consumer. Field names and semantics mirror the
 * ingestion-side {@code SafetyHaltWriter} row contract
 * (contract_version = 2).
 *
 * <p>Validation mirrors the writer: {@code UNSAFE} carries a reason code,
 * {@code RECOVERED} carries {@code ""}. The tracker additionally gates on
 * {@code sourceComponent == "INGESTION"} and on the manifest/hash checks.
 *
 * @param haltRequestId      deterministic SHA-256 dedupe key
 *                           ({@code manifest_fingerprint|slot_id|connection_epoch|state|reason_code})
 * @param sourceComponent    writer component, expected {@code "INGESTION"}
 * @param slotId             subscription slot, {@code "hft-N"}
 * @param connectionEpoch    connection generation (long)
 * @param status             {@link SlotSafetyStatus#UNSAFE} or {@code RECOVERED}
 * @param reasonCode         reason enum name; {@code ""} for RECOVERED
 * @param manifestFingerprint {@code TokenSetHash} of the full manifest
 * @param assignedTokenSetHash {@code TokenSetHash} of the slot's tokens
 * @param detectionTimeMs    epoch milliseconds of the detection
 * @param contractVersion    row contract version (2)
 */
public record SlotSafetyRequest(
        String haltRequestId,
        String sourceComponent,
        String slotId,
        long connectionEpoch,
        SlotSafetyStatus status,
        String reasonCode,
        String manifestFingerprint,
        String assignedTokenSetHash,
        long detectionTimeMs,
        int contractVersion
) {

    /** Expected writer component; rows from any other component are ignored. */
    public static final String SOURCE_COMPONENT_INGESTION = "INGESTION";

    /** Contract version pinned by SafetyHaltWriter. */
    public static final int CONTRACT_VERSION = 2;

    public SlotSafetyRequest {
        require(slotId, "slotId");
        require(haltRequestId, "haltRequestId");
        require(sourceComponent, "sourceComponent");
        require(manifestFingerprint, "manifestFingerprint");
        require(assignedTokenSetHash, "assignedTokenSetHash");
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (connectionEpoch < 0) {
            throw new IllegalArgumentException("connectionEpoch must be >= 0");
        }
        if (detectionTimeMs < 0) {
            throw new IllegalArgumentException("detectionTimeMs must be >= 0");
        }
        if (contractVersion < 1) {
            throw new IllegalArgumentException("contractVersion must be >= 1");
        }
        // P3-126: reasonCode is never null ("" for RECOVERED) and RECOVERED
        // must carry empty — matches SafetyHaltWriter v2 (null→"" on wire).
        if (reasonCode == null) {
            throw new IllegalArgumentException("reasonCode must not be null (use \"\" for RECOVERED)");
        }
        // Mirrors SafetyHaltWriter: UNSAFE requires a reason, RECOVERED does not.
        if (status == SlotSafetyStatus.UNSAFE && reasonCode.isBlank()) {
            throw new IllegalArgumentException("UNSAFE requires a reasonCode");
        }
        if (status == SlotSafetyStatus.RECOVERED && !reasonCode.isEmpty()) {
            throw new IllegalArgumentException("RECOVERED must carry empty reasonCode");
        }
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
