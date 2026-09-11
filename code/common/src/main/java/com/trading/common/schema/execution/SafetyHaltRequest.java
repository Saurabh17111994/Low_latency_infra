package com.trading.common.schema.execution;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/** Offline value of one Safety_Halt_Requests KV row. Deterministic PK = SHA256 hex. */
public record SafetyHaltRequest(
        String haltRequestId,
        String accountScopeId,
        String executionPartitionId,
        String sourceComponent,
        String sourceInstance,
        String reasonCode,
        String reasonDetail,
        long detectionTime,
        long sourceEpoch,
        String evidenceHash,
        String schemaVersion,
        String slotId,
        long connectionEpoch,
        String manifestFingerprint,
        String state) {

    public SafetyHaltRequest {
        // P3-385: the PK must never be null/blank — every persistence path
        // must go through createValidated or check idValid(); a spoofed key
        // stored without the check would corrupt dedup correlation.
        Objects.requireNonNull(haltRequestId, "haltRequestId");
        if (haltRequestId.isBlank()) throw new IllegalArgumentException("haltRequestId must be non-blank");
        Objects.requireNonNull(accountScopeId, "accountScopeId");
        Objects.requireNonNull(sourceComponent, "sourceComponent");
        Objects.requireNonNull(reasonCode, "reasonCode");
        Objects.requireNonNull(evidenceHash, "evidenceHash");
        Objects.requireNonNull(schemaVersion, "schemaVersion");
    }

    /**
     * Validated factory (P3-385): derives the deterministic id so callers
     * cannot construct a row with a mismatched key. Tuple fields must not
     * contain '|' (P3-387 precondition — the canonical form joins on '|').
     */
    public static SafetyHaltRequest createValidated(String accountScopeId, String executionPartitionId,
            String sourceComponent, String sourceInstance, String reasonCode, String reasonDetail,
            long detectionTime, long sourceEpoch, String evidenceHash, String schemaVersion,
            String slotId, long connectionEpoch, String manifestFingerprint, String state) {
        noDelim(accountScopeId, "accountScopeId");
        noDelim(sourceComponent, "sourceComponent");
        noDelim(reasonCode, "reasonCode");
        noDelim(evidenceHash, "evidenceHash");
        noDelim(schemaVersion, "schemaVersion");
        if (executionPartitionId != null) {
            noDelim(executionPartitionId, "executionPartitionId");
        }
        String id = deterministicId(accountScopeId, executionPartitionId, sourceComponent,
                reasonCode, detectionTime, sourceEpoch, evidenceHash, schemaVersion);
        return new SafetyHaltRequest(id, accountScopeId, executionPartitionId, sourceComponent,
                sourceInstance, reasonCode, reasonDetail, detectionTime, sourceEpoch,
                evidenceHash, schemaVersion, slotId, connectionEpoch, manifestFingerprint, state);
    }

    /**
     * P3-387: canonical form joins on '|', so tuple fields must be '|'-free
     * for the PK to be unambiguous. Enforced at the factory (the only path
     * that mints new ids) — the canonical form and existing ids are untouched,
     * so no migration and no audit break.
     */
    private static String noDelim(String v, String name) {
        Objects.requireNonNull(v, name);
        if (v.contains("|")) throw new IllegalArgumentException(name + " must not contain '|'");
        return v;
    }

    /** Canonical §Gate tuple: account|partition|source|reason|detectionTime|sourceEpoch|evidenceHash|schemaVersion */
    public static String deterministicId(String accountScopeId, String executionPartitionId,
            String sourceComponent, String reasonCode, long detectionTime,
            long sourceEpoch, String evidenceHash, String schemaVersion) {
        // P3-386: named fail-closed errors, not a raw String.join NPE.
        // P3-388: null partition = global scope; "" is rejected (it would
        // hash identically to null and collide two distinct rows on upsert).
        Objects.requireNonNull(accountScopeId, "accountScopeId");
        Objects.requireNonNull(sourceComponent, "sourceComponent");
        Objects.requireNonNull(reasonCode, "reasonCode");
        Objects.requireNonNull(evidenceHash, "evidenceHash");
        Objects.requireNonNull(schemaVersion, "schemaVersion");
        if (executionPartitionId != null && executionPartitionId.isEmpty()) {
            throw new IllegalArgumentException("executionPartitionId must not be empty; use null for global scope");
        }
        String canonical = String.join("|",
                accountScopeId, executionPartitionId == null ? "" : executionPartitionId,
                sourceComponent, reasonCode,
                Long.toString(detectionTime), Long.toString(sourceEpoch),
                evidenceHash, schemaVersion);
        try {
            MessageDigest d = MessageDigest.getInstance("SHA-256");
            byte[] h = d.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(h.length*2);
            for (byte b: h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new IllegalStateException(e); }
    }
    public static String deterministicId(SafetyHaltRequest r){
        return deterministicId(r.accountScopeId(), r.executionPartitionId(), r.sourceComponent(),
                r.reasonCode(), r.detectionTime(), r.sourceEpoch(), r.evidenceHash(), r.schemaVersion());
    }
    /** Verifies supplied haltRequestId matches canonical — fail-closed on mismatch. */
    public boolean idValid(){ return haltRequestId != null && haltRequestId.equals(deterministicId(this)); }
}
