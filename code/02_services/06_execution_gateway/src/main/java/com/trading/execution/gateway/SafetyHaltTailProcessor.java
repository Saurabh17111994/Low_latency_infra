package com.trading.execution.gateway;

import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.GateStateStore;
import com.trading.common.schema.execution.SafetyHaltRequest;
import org.apache.fluss.row.InternalRow;
import java.util.HashSet;
import java.util.Set;

/**
 * Offline KV tail: Safety_Halt_Requests \u2192 GateStateStore.halt(). Idempotent, cross-scope reject.
 * Handles both typed {@link SafetyHaltRequest} and live Fluss {@link InternalRow} paths.
 * InternalRow decode follows the 21-column DDL v3 order (18_safety_halt_requests.sql /
 * DdlBootstrap.SAFETY_HALT_SCHEMA) and fail-closes to INVALID_ID on any mismatch.
 */
public final class SafetyHaltTailProcessor {
    public enum ApplyResult { APPLIED, DUPLICATE, CROSS_SCOPE_REJECT, NOT_FOUND, INVALID_ID }
    // DDL v3 column positions for Safety_Halt_Requests (21 cols, DdlBootstrap order)
    private static final int IDX_HALT_REQUEST_ID = 0;
    private static final int IDX_ACCOUNT_SCOPE_ID = 1;
    private static final int IDX_EXECUTION_PARTITION_ID = 3;
    private static final int IDX_SOURCE_COMPONENT = 4;
    private static final int IDX_SOURCE_INSTANCE = 5;
    private static final int IDX_REASON_CODE = 6;
    private static final int IDX_REASON_DETAIL = 7;
    private static final int IDX_DETECTION_TIME = 8;
    private static final int IDX_SOURCE_EPOCH = 9;
    private static final int IDX_EVIDENCE_HASH = 10;
    private static final int IDX_SCHEMA_VERSION = 13;
    private static final int IDX_SLOT_ID = 14;
    private static final int IDX_CONNECTION_EPOCH = 15;
    private static final int IDX_MANIFEST_FINGERPRINT = 16;
    private static final int IDX_STATE = 18;

    private final GateStateStore gates;
    private final Set<String> appliedIds = new HashSet<>();
    public SafetyHaltTailProcessor(GateStateStore gates){ this.gates=gates; }
    public ApplyResult apply(SafetyHaltRequest req, long nowTs){
        // P3-105: fail closed on nulls — NPE or a null-key Fluss lookup hides
        // the true outcome; INVALID_ID / NOT_FOUND name it instead.
        if (req == null || !req.idValid()) return ApplyResult.INVALID_ID;
        // P3-005: check-then-add, never add-then-check — a NOT_FOUND / thrown
        // halt must not poison the set (retry would read DUPLICATE and the
        // halt would be silently dropped).
        if (appliedIds.contains(req.haltRequestId())) return ApplyResult.DUPLICATE;
        if (req.executionPartitionId() == null) return ApplyResult.NOT_FOUND;
        GateRow row = gates.read(req.executionPartitionId());
        if(row==null) return ApplyResult.NOT_FOUND;
        if(req.accountScopeId() == null || !req.accountScopeId().equals(row.accountScopeId())) {
            return ApplyResult.CROSS_SCOPE_REJECT;
        }
        GateRow next = gates.halt(req.executionPartitionId(), row, req.reasonCode()+":"+req.reasonDetail(), req.evidenceHash(), nowTs, req.detectionTime());
        if (next == null) return ApplyResult.NOT_FOUND;
        appliedIds.add(req.haltRequestId());
        return ApplyResult.APPLIED;
    }
    /**
     * Live Fluss path: decode InternalRow (21-col DDL order) → SafetyHaltRequest, validate
     * deterministicId via idValid(), then delegate to typed apply. Any decode failure or
     * id mismatch fail-closes to INVALID_ID.
     */
    public ApplyResult apply(InternalRow row, long nowTs) {
        if (row == null) return ApplyResult.INVALID_ID;
        // P3-106: narrow the try to decode+idValid — the delegated apply does
        // gates.read()/halt(), and a transient store failure misclassified as
        // INVALID_ID would hide outages and break retry/alerting.
        final SafetyHaltRequest req;
        try {
            req = decodeRow(row);
        } catch (Exception e) {
            return ApplyResult.INVALID_ID;
        }
        if (!req.idValid()) return ApplyResult.INVALID_ID;
        return apply(req, nowTs);
    }

    private static SafetyHaltRequest decodeRow(InternalRow r) {
        // P3-327: hardcoded IDX_* positions must stay in sync with
        // 18_safety_halt_requests.sql/DdlBootstrap — a schema add/reorder
        // would otherwise mis-decode scope/evidence/id into wrong fields
        // (wrong scope check, wrong deterministicId). Fail to INVALID_ID
        // instead of failing open.
        if (r.getFieldCount() != 21) throw new IllegalArgumentException("expected 21 cols, got " + r.getFieldCount());
        String haltId = getRequiredString(r, IDX_HALT_REQUEST_ID);
        String acct = getRequiredString(r, IDX_ACCOUNT_SCOPE_ID);
        String partition = getNullableString(r, IDX_EXECUTION_PARTITION_ID);
        String sourceComp = getRequiredString(r, IDX_SOURCE_COMPONENT);
        String sourceInstance = getNullableString(r, IDX_SOURCE_INSTANCE);
        String reasonCode = getRequiredString(r, IDX_REASON_CODE);
        String reasonDetail = getNullableString(r, IDX_REASON_DETAIL);
        long detectionTime = getRequiredLong(r, IDX_DETECTION_TIME);
        long sourceEpoch = getRequiredLong(r, IDX_SOURCE_EPOCH);
        String evidenceHash = getRequiredString(r, IDX_EVIDENCE_HASH);
        String schemaVersion = getRequiredString(r, IDX_SCHEMA_VERSION);
        String slotId = getNullableString(r, IDX_SLOT_ID);
        long connectionEpoch = r.isNullAt(IDX_CONNECTION_EPOCH) ? 0L : r.getLong(IDX_CONNECTION_EPOCH);
        String manifest = getNullableString(r, IDX_MANIFEST_FINGERPRINT);
        String state = getNullableString(r, IDX_STATE);
        return new SafetyHaltRequest(haltId, acct, partition, sourceComp, sourceInstance,
                reasonCode, reasonDetail, detectionTime, sourceEpoch, evidenceHash,
                schemaVersion, slotId, connectionEpoch, manifest, state);
    }

    private static String getRequiredString(InternalRow r, int idx) {
        if (r.isNullAt(idx)) throw new IllegalArgumentException("null at " + idx);
        Object o = r.getString(idx);
        if (o == null) throw new IllegalArgumentException("null string at " + idx);
        String s = o.toString();
        if (s.isEmpty()) throw new IllegalArgumentException("blank at " + idx);
        return s;
    }

    private static String getNullableString(InternalRow r, int idx) {
        if (r.isNullAt(idx)) return null;
        Object o = r.getString(idx);
        return o == null ? null : o.toString();
    }

    private static long getRequiredLong(InternalRow r, int idx) {
        if (r.isNullAt(idx)) throw new IllegalArgumentException("null long at " + idx);
        return r.getLong(idx);
    }

    public void replay(InMemoryControlStateStore store, long nowTs){
        store.replaySafetyHaltsTyped(r -> apply(r, nowTs));
    }
    /** Live Fluss replay: iterates every Safety_Halt_Requests row via the Fluss batch scanner. */
    public void replay(FlussControlStateStore store, long nowTs) {
        store.replaySafetyHalts(row -> apply(row, nowTs));
    }
    /** Generic replay for any ControlStateStore (including InMemory fake for offline tests). */
    public void replay(ControlStateStore store, long nowTs) {
        store.replaySafetyHalts(row -> apply(row, nowTs));
    }
}
