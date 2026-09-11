package com.trading.execution.gateway;

import java.util.*;
import java.util.function.Consumer;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import com.trading.common.schema.execution.SafetyHaltRequest;

/** Offline ControlStateStore — holds SafetyHaltRequests in memory, no Fluss. */
public final class InMemoryControlStateStore implements ControlStateStore {
    private final List<SafetyHaltRequest> halts = new ArrayList<>();
    private final Map<String,SafetyHaltRequest> byId = new LinkedHashMap<>();
    public void add(SafetyHaltRequest r){
        if(!r.idValid()) throw new IllegalArgumentException("halt_request_id != deterministic SHA256");
        // P3-302: KV upsert semantics — duplicate ids are a no-op, matching
        // the PK on halt_request_id (DDL 18_safety_halt_requests.sql).
        if (byId.putIfAbsent(r.haltRequestId(), r) == null) halts.add(r);
    }
    /**
     * Test helper: inject a pre-built InternalRow directly (e.g. tampered id).
     * P3-303: intentionally bypasses the idValid() check so tests can pin the
     * fail-closed INVALID_ID path in SafetyHaltTailProcessor.apply(InternalRow).
     * No production consumer trusts the store — decode always re-validates —
     * so this stays public for same-package tests without risk.
     */
    private final List<InternalRow> rawRows = new ArrayList<>();
    public void addRawRow(InternalRow row){ rawRows.add(java.util.Objects.requireNonNull(row)); }

    @Override public Lookup lookup(String t, List<Object> k){
        // P3-304: serve halt-table point reads from byId so the index is live
        // (written AND read); anything else stays NOT_FOUND offline.
        if ("Safety_Halt_Requests".equals(t) && k != null && k.size() == 1 && k.get(0) != null) {
            SafetyHaltRequest r = byId.get(String.valueOf(k.get(0)));
            if (r != null) return new Lookup(Status.FOUND, toRow(r), "ok");
        }
        return new Lookup(Status.NOT_FOUND,null,"offline");
    }

    @Override public void replaySafetyHalts(Consumer<InternalRow> c){
        // Emit typed halts as 21-col DDL rows (offline Fluss wire), then any raw rows
        for (SafetyHaltRequest r : List.copyOf(halts)) {
            c.accept(toRow(r));
        }
        for (InternalRow r : List.copyOf(rawRows)) c.accept(r);
    }
    public void replaySafetyHaltsTyped(Consumer<SafetyHaltRequest> c){ List.copyOf(halts).forEach(c); }
    @Override public void close(){}

    /**
     * Encode SafetyHaltRequest → 21-col Safety_Halt_Requests DDL row (DdlBootstrap order).
     *
     * <p>P3-305: test-only placeholder — v[17] (assigned_token_set_hash) reuses
     * evidenceHash because SafetyHaltRequest carries no assigned-hash field.
     * Do not feed these rows to slot-identity validation (it requires a real
     * assigned hash); the gateway decoder ignores cols 11,12,17,19,20.
     */
    public static InternalRow toRow(SafetyHaltRequest r) {
        Object[] v = new Object[21];
        v[0] = BinaryString.fromString(r.haltRequestId());
        v[1] = BinaryString.fromString(r.accountScopeId());
        v[2] = null; // portfolio_id
        v[3] = r.executionPartitionId() == null ? null : BinaryString.fromString(r.executionPartitionId());
        v[4] = BinaryString.fromString(r.sourceComponent());
        v[5] = r.sourceInstance() == null ? null : BinaryString.fromString(r.sourceInstance());
        v[6] = BinaryString.fromString(r.reasonCode());
        v[7] = r.reasonDetail() == null ? null : BinaryString.fromString(r.reasonDetail());
        v[8] = r.detectionTime();
        v[9] = r.sourceEpoch();
        v[10] = BinaryString.fromString(r.evidenceHash());
        v[11] = BinaryString.fromString("OPEN"); // application_result
        v[12] = null; // applied_ts
        v[13] = BinaryString.fromString(r.schemaVersion());
        v[14] = r.slotId() == null ? null : BinaryString.fromString(r.slotId());
        v[15] = r.connectionEpoch();
        v[16] = r.manifestFingerprint() == null ? null : BinaryString.fromString(r.manifestFingerprint());
        v[17] = BinaryString.fromString(r.evidenceHash()); // assigned_token_set_hash placeholder (see javadoc)
        v[18] = r.state() == null ? null : BinaryString.fromString(r.state());
        v[19] = null; // evidence_reference
        v[20] = 2; // contract_version
        return GenericRow.of(v);
    }
}
