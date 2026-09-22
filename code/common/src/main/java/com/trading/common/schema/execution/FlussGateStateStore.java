package com.trading.common.schema.execution;

import com.trading.common.schema.fluss.BoundedRetry;

import com.trading.common.model.GateState;
import com.trading.common.schema.fluss.FlussWriteProfiles;
import com.trading.common.schema.ownership.ExecutionGateColumns;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

// Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — 1.0.0 still has no client-visible KV compare-and-swap (its CAS uses are JVM/ZooKeeper-internal: the ZK `setData` guard and the `LogTablet`/`ScannerContext` loops), so single-active-owner still comes from deployment (ASM-EXE-005). Re-check on the next upgrade (DEC-052).
/**
 * Fluss-backed {@link GateStateStore} — production writer for Execution_Gate v3 (CHG-044, T5).
 * Offline protocol is proven by {@link InMemoryGateStateStore}; this writer satisfies the
 * same interface with durable Fluss persistence. Row mapping follows
 * {@link ExecutionGateColumns} (17 cols, schema_version v3).
 * Owns its {@link Connection} and {@link Table}; {@link #close()} releases both.
 *
 * <p>Resource note (P3-365/P3-367/P3-371, D1/D2): the Lookuper and UpsertWriter are created
 * once and reused rather than per call. Both are {@code @NotThreadSafe}, but every method here
 * is {@code synchronized}, so they are confined to this store's monitor — see the field
 * javadoc for why that is sufficient. Writes carry no per-record flush — see
 * {@link FlussWriteProfiles} for that rationale; this store's connection carries only this one
 * table, so a connection-scoped flush would have no sibling to block.
 *
 * <p><b>Fencing (P3-010/P3-012/P3-013).</b> All methods are {@code synchronized}, so a
 * read→decide→write is atomic <i>within one process</i>. The durable table is a
 * <b>VERSIONED merge table on {@code fence_token}</b> (CHG-122), so the tablet itself drops a
 * write whose token is lower than the stored row's: a stale or zombie writer can no longer
 * clobber the live owner. Every mutator is durable-first, re-reads and confirms its own write
 * landed ({@link #verifyPersisted}), and never reports success for a dropped write — Fluss
 * 0.9.1 returns SUCCESS for a version-dropped upsert, so silence here would be a lost halt.
 *
 * <p><b>Residual, unchanged by CHG-122.</b> Equal versions are accepted
 * ({@code compare(old,new) <= 0}), so two <i>hosts</i> that interleave and mint the same next
 * token both win. Cross-host single-active-owner is therefore still owned by the
 * <b>deployment</b> — ASM-EXE-005 ("the deployment provides a fencing/leadership
 * mechanism sufficient for single-active-owner enforcement per
 * execution_partition_id"). This store MUST NOT be run active-active without
 * that mechanism; AC-EXE-006 (two concurrent executors ⇒ one submits) is
 * still NOT_IMPLEMENTED (see docs/02_requirements/09-acceptance-matrix.md).
 */
public final class FlussGateStateStore implements GateStateStore, AutoCloseable {
    private final Connection connection;
    private final Table table;
    private final long timeoutMs;
    private final InMemoryGateStateStore delegate;

    /**
     * P3-365/P3-367/P3-371: the Lookuper and UpsertWriter are created once and reused instead
     * of on every call. Both are {@code @NotThreadSafe}, but every method that touches them is
     * {@code synchronized} on this instance, so they are confined to a single monitor and can
     * never be used concurrently. Neither is {@code Closeable} in Fluss 0.9.1 ({@code Lookuper}
     * has no close; {@code TableWriter} declares only {@code flush()}), so their lifecycle
     * follows the {@link Table}: {@link #close()} drops the references and any later use fails
     * loudly rather than writing through a dead handle.
     */
    private Lookuper lookuper;
    private UpsertWriter upsertWriter;

    private FlussGateStateStore(Connection connection, Table table, long timeoutMs,
            Set<String> authorizedApprovers, boolean anyPrincipalAllowed) {
        this.connection = connection;
        this.table = table;
        this.timeoutMs = timeoutMs;
        this.delegate = new InMemoryGateStateStore(authorizedApprovers, anyPrincipalAllowed);
        this.lookuper = table.newLookup().createLookuper();
        this.upsertWriter = table.newUpsert().createWriter();
    }

    public static FlussGateStateStore open(String bootstrap, String database, String tableName, Duration timeout, Set<String> authorizedApprovers) throws Exception {
        return open(bootstrap, database, tableName, timeout, authorizedApprovers, false);
    }

    private static FlussGateStateStore open(String bootstrap, String database, String tableName, Duration timeout,
            Set<String> authorizedApprovers, boolean anyPrincipalAllowed) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        // D1: money path — 1ms linger instead of the 100ms default, replacing the
        // per-record flush() that used to force each write out (see FlussWriteProfiles).
        FlussWriteProfiles.moneyPath(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            Table table = connection.getTable(TablePath.of(database, tableName));
            return new FlussGateStateStore(connection, table, timeout.toMillis(), authorizedApprovers, anyPrincipalAllowed);
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    /**
     * Test/live-drill overload: single-operator authorization is OFF on purpose (any principal may
     * approve), same as {@link InMemoryGateStateStore#anyApprover()}.
     *
     * <p>P3-142/P3-143: this any-principal behaviour is now requested by choosing <i>this</i>
     * overload rather than by passing an empty set. The 5-arg overload rejects an empty allow-list
     * at construction, so a missing or misconfigured approver set on a production bootstrap (e.g.
     * ExecutionGatewayMain) fails loudly instead of silently accepting any principal.
     */
    public static FlussGateStateStore open(String bootstrap, String database, String tableName, Duration timeout) throws Exception {
        return open(bootstrap, database, tableName, timeout, Set.of(), true);
    }

    @Override public synchronized void close() throws Exception {
        // The cached handles are NOT Closeable — their lifecycle follows the Table. Drop the
        // references BEFORE closing so a use-after-close fails loudly (NPE) instead of
        // silently writing through a handle whose table is gone.
        lookuper = null;
        upsertWriter = null;
        try { table.close(); } finally { connection.close(); }
    }

    /**
     * Test seam (P3-365/P3-367/P3-371): the cached handles must be created once, stay the same
     * instance across calls, and be released by {@link #close()}. Package-private so the
     * invariant is asserted without widening the public surface.
     */
    Lookuper cachedLookuperForTest() { return lookuper; }

    /** Test seam — see {@link #cachedLookuperForTest()}. */
    UpsertWriter cachedUpsertWriterForTest() { return upsertWriter; }

    @Override public synchronized GateRow read(String partitionId) {
        // Read-through (P3-009/P3-011): durable decides, never serve a stale local
        // entry. Lookup failure throws (fail closed) — never a cached yes on error.
        GateRow durable = lookupGateOrThrow(partitionId);
        if (durable != null) {
            // Restart-refresh: rebuild the local authority from the durable row (including its
            // monotonic fence sequence) so a restarted process sees its own prior fence.
            delegate.hydrate(durable);
            return durable;
        }
        return delegate.read(partitionId);
    }

    /**
     * Durable read that distinguishes absent (null) from failed (throws).
     * A lookup failure must never look like "no row" — callers fail closed.
     * Decode errors from {@link #fromRow} propagate, never map to missing.
     */
    private GateRow lookupGateOrThrow(String partitionId) {
        InternalRow r;
        try {
            // P3-365: reuse the monitor-confined Lookuper (see the field javadoc) instead
            // of minting one per lookup on the money path.
            // C5: retry the fence read. A 2s budget sits BELOW the first-write-after-CREATE
            // window (2.2-3.7s measured; see BoundedRetry), so without this a transient
            // surfaces as a hard "gate lookup failed" on the money path. Point read — idempotent.
            r = BoundedRetry.await(() -> lookuper
                    .lookup(GenericRow.of(BinaryString.fromString(partitionId)))
                    .get(timeoutMs, TimeUnit.MILLISECONDS).getSingletonRow());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("gate lookup interrupted for " + partitionId, e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new RuntimeException("gate lookup failed for " + partitionId, e);
        }
        if (r == null) return null;
        return fromRow(r);
    }

    @Override public synchronized GateRow init(GateRow boot) {
        // Restart-refresh: never re-initialize an already-fenced durable gate. If a row already
        // exists in Fluss, hydrate it (with its monotonic fence) and leave it untouched.
        // Lookup failure throws (P3-140) — a timeout must never look like "absent" and
        // overwrite a fenced gate with boot state.
        GateRow durable = lookupGateOrThrow(boot.partitionId());
        if (durable != null) {
            delegate.hydrate(durable);
            return durable;
        }
        // P3-139/P3-145: durable write is the commit and is fail-loud. Persist
        // FIRST, then sync memory — a failed init leaves no memory-only row that
        // a later read would serve once the durable lookup returns null.
        persistOrThrow(boot);
        delegate.hydrate(boot);
        return boot;
    }

    @Override public synchronized FenceResult acquire(String partitionId, String ownerInstanceId, long leaseMs, long nowTs) {
        // Durable-first fencing (P3-010/P3-012): the durable row decides, not local memory.
        // 1. durable read — lookup failure throws (fail closed), absent row returns null.
        GateRow durable = lookupGateOrThrow(partitionId);
        if (durable != null) {
            // Restart-refresh: sync memory + fence sequence from durable before deciding,
            // so a restarted process never re-issues an already-persisted token.
            delegate.hydrate(durable);
        }
        // 2. decide on the delegate (now synced from durable). synchronized ⇒ the
        // whole read→decide→write is one critical section within this process.
        FenceResult res = delegate.acquire(partitionId, ownerInstanceId, leaseMs, nowTs);
        if (res.conflict()) return res;
        // 3. persist is the commit: delegate.acquire already mutated memory, so on durable
        // failure roll memory back to the pre-acquire durable row (sequence stays high —
        // monotonic, never reused) and fail loud. Never return acquired unsaved.
        try {
            persistOrThrow(res.row());
            verifyFence(partitionId, ownerInstanceId, res.token());
        } catch (RuntimeException e) {
            if (durable != null) delegate.hydrate(durable);
            throw e;
        }
        return res;
    }

    @Override public synchronized FenceResult renew(String partitionId, String ownerInstanceId, long fenceToken, long leaseMs, long nowTs) {
        // Durable-first like acquire (P3-013): a stale holder must not renew over
        // the true durable holder. Lookup failure throws — never renew on error.
        GateRow durable = lookupGateOrThrow(partitionId);
        if (durable != null) {
            delegate.hydrate(durable);
        }
        FenceResult res = delegate.renew(partitionId, ownerInstanceId, fenceToken, leaseMs, nowTs);
        if (res.conflict()) return res;
        try {
            persistOrThrow(res.row());
            verifyFence(partitionId, ownerInstanceId, fenceToken);
        } catch (RuntimeException e) {
            if (durable != null) delegate.hydrate(durable);
            throw e;
        }
        return res;
    }

    /**
     * P3-010/P3-012/P3-013: the write is not a CAS (Fluss 0.9.1 has none), so
     * re-read and confirm the durable row still carries {@code owner+token}.
     * A concurrent writer that clobbered between our write and this read makes
     * the fence ours no longer: the caller rolls memory back and fails closed
     * rather than return "acquired" for a fence we lost and then issue a
     * money-moving call.
     */
    private void verifyFence(String partitionId, String owner, long token) {
        GateRow verify = lookupGateOrThrow(partitionId);
        boolean ours = verify != null && verify.fenceToken() == token
                && java.util.Objects.equals(owner, verify.ownerInstanceId());
        if (!ours) {
            throw new IllegalStateException("fence write lost for " + partitionId
                    + ": durable holds " + (verify == null ? "no row"
                    : verify.ownerInstanceId() + "/" + verify.fenceToken())
                    + ", not " + owner + "/" + token
                    + " — no durable CAS in Fluss 0.9.1 or 1.0.0, single-active-owner must come from deployment (ASM-EXE-005)");
        }
    }

    /**
     * P3-010/P3-012/P3-013: the durable gate row is ordered by {@code fence_token} (VERSIONED
     * merge engine), so the tablet DROPS a write whose token is lower than the stored row's —
     * and still reports SUCCESS, because Fluss 0.9.1 carries no "ignored" result on
     * {@code UpsertResult}. A dropped write is therefore invisible unless we re-read it.
     * Every mutator confirms its own write landed.
     *
     * @param allowSuperseded a fence-CLEARING write (revoke) that lost to a strictly newer
     *                        token is legitimately moot — a newer owner took the fence — so
     *                        that is not a lost write. Every other mismatch fails loud.
     */
    private void verifyPersisted(String partitionId, GateRow written, boolean allowSuperseded) {
        checkPersisted(partitionId, lookupGateOrThrow(partitionId), written, allowSuperseded);
    }

    /**
     * The comparison behind {@link #verifyPersisted}, split out so the write-ordering rule is
     * testable without a live cluster. The DROP itself happens tablet-side, so end-to-end proof
     * still needs a real VERSIONED table (the live drill); this pins the decision made on the
     * re-read.
     */
    static void checkPersisted(String partitionId, GateRow verify, GateRow written, boolean allowSuperseded) {
        if (verify == null) {
            throw new IllegalStateException("gate write lost for " + partitionId
                    + ": durable holds no row after writing token " + written.fenceToken());
        }
        if (verify.fenceToken() > written.fenceToken()) {
            if (allowSuperseded) return;
            throw new IllegalStateException("gate write lost for " + partitionId
                    + ": durable token " + verify.fenceToken() + " is newer than the written "
                    + written.fenceToken() + " — the write was version-dropped by the"
                    + " VERSIONED merge engine on fence_token");
        }
        if (verify.fenceToken() != written.fenceToken()
                || verify.epoch() != written.epoch()
                || verify.state() != written.state()) {
            throw new IllegalStateException("gate write lost for " + partitionId
                    + ": durable holds token/epoch/state " + verify.fenceToken() + "/"
                    + verify.epoch() + "/" + verify.state() + ", not " + written.fenceToken()
                    + "/" + written.epoch() + "/" + written.state());
        }
    }

    @Override public synchronized RevokeResult revoke(String partitionId, String ownerInstanceId, long nowTs) {
        // Durable-first like acquire/renew (P3-013): the durable row decides, so the revoke
        // carries the current ordering version and a stale process cannot clear a fence it
        // does not hold.
        GateRow durable = lookupGateOrThrow(partitionId);
        if (durable != null) delegate.hydrate(durable);
        GateRow before = delegate.read(partitionId);
        RevokeResult res = delegate.revoke(partitionId, ownerInstanceId, nowTs);
        // P3-150: persist on the OUTCOME, not on reference identity. ALREADY_CLEAR and NOT_OWNER
        // both hand back an unchanged row, and only REVOKED mutated durable state.
        if (res.revoked()) {
            persistOrRollback(res.row(), before);
            // A newer token means another owner took the fence: the revoke is moot, not lost.
            verifyPersisted(partitionId, res.row(), true);
        }
        return res;
    }

    @Override public synchronized ApprovalResult approve(String partitionId, String principal, long epoch, String evidenceHash, long nowTs) {
        // Durable-first: the approval must be decided against durable truth, and its write
        // must carry the current ordering version or it would be version-dropped.
        GateRow durable = lookupGateOrThrow(partitionId);
        if (durable != null) delegate.hydrate(durable);
        GateRow before = delegate.read(partitionId);
        ApprovalResult res = delegate.approve(partitionId, principal, epoch, evidenceHash, nowTs);
        if (res.outcome() == ApprovalOutcome.APPLIED) {
            persistOrRollback(res.row(), before);
            verifyPersisted(partitionId, res.row(), false);
        }
        return res;
    }

    /**
     * P3-075 durable leg: the delegate promotes APPROVAL_PENDING+complete to
     * ENABLED atomically in memory (synchronized); persist the FINAL row the
     * delegate returned so Fluss and memory agree on the promoted state.
     */
    @Override public synchronized ApprovalResult approveAndEnableIfComplete(String partitionId, String principal,
            long epoch, String evidenceHash, long nowTs) {
        GateRow durable = lookupGateOrThrow(partitionId);
        if (durable != null) delegate.hydrate(durable);
        GateRow before = delegate.read(partitionId);
        ApprovalResult res = delegate.approveAndEnableIfComplete(partitionId, principal, epoch,
                evidenceHash, nowTs);
        if (res.outcome() == ApprovalOutcome.APPLIED) {
            persistOrRollback(res.row(), before);
            verifyPersisted(partitionId, res.row(), false);
        }
        return res;
    }

    @Override public synchronized GateRow halt(String partitionId, GateRow expected, String reason, String evidenceHash, long nowTs, Long detectedTs) {
        // Durable-first: the halt mints strictly ABOVE the durable token (see the delegate),
        // which requires the local sequence to be synced from durable first — otherwise the
        // safety halt would be version-dropped and silently lost.
        GateRow durable = lookupGateOrThrow(partitionId);
        if (durable != null) delegate.hydrate(durable);
        GateRow before = delegate.read(partitionId);
        GateRow r = delegate.halt(partitionId, expected, reason, evidenceHash, nowTs, detectedTs);
        if (r != null && r != before) {
            persistOrRollback(r, before);
            // Never allowSuperseded: a halt that did not land is a safety failure.
            verifyPersisted(partitionId, r, false);
        }
        return r;
    }

    @Override public synchronized void audit(AuditRecord record) { delegate.audit(record); }
    @Override public synchronized List<AuditRecord> auditLog() { return delegate.auditLog(); }

    /**
     * P3-139/P3-145: the durable write is the commit and never swallows. A
     * failure means memory and Fluss would diverge, so roll the in-memory row
     * back to {@code before} and rethrow — the caller must not see in-memory
     * success for a write that did not land.
     */
    private void persistOrRollback(GateRow row, GateRow before) {
        try {
            persistOrThrow(row);
        } catch (RuntimeException e) {
            if (before != null) delegate.hydrate(before);
            throw e;
        }
    }

    /**
     * Durable upsert that fails loud (P3-139/P3-145): lookup/write failure must
     * never surface as in-memory success with Fluss diverged. Interrupt status
     * is restored.
     *
     * <p>D1: no per-record {@code flush()} — see {@link FlussWriteProfiles}. A flush
     * would escape this call's bounded budget, so {@code .get()} returns only on a
     * durable ack ({@code client.writer.acks=all}) with no durability loss.
     */
    private void persistOrThrow(GateRow r) {
        try {
            // P3-367/P3-371: reuse the monitor-confined UpsertWriter (see the field javadoc).
            // Every caller is synchronized, so one writer is never shared concurrently.
            // C5/P3-268: retry the fence write. KV upsert keyed by execution_partition_id,
            // so idempotent by construction and safe to repeat. The cached handle is reused
            // deliberately (P3-367/P3-371); a genuine handle fault is non-transient and
            // still fails fast rather than retrying.
            BoundedRetry.await(() -> {
                upsertWriter.upsert(GenericRow.of(encode(r))).get(timeoutMs, TimeUnit.MILLISECONDS);
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("gate persist interrupted for " + r.partitionId(), e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new RuntimeException("gate persist failed for " + r.partitionId(), e);
        }
    }

    private static Object[] encode(GateRow r) {
        Object[] v = new Object[ExecutionGateColumns.FIELD_COUNT];
        v[ExecutionGateColumns.EXECUTION_PARTITION_ID] = BinaryString.fromString(r.partitionId());
        v[ExecutionGateColumns.ACCOUNT_SCOPE_ID] = BinaryString.fromString(r.accountScopeId());
        v[ExecutionGateColumns.STATE] = BinaryString.fromString(r.state().name());
        v[ExecutionGateColumns.EPOCH] = r.epoch();
        v[ExecutionGateColumns.REASON] = r.reason() == null ? null : BinaryString.fromString(r.reason());
        // P3-364/P3-369: the real detection time, when the halt was raised from safety-path
        // evidence (SafetyHaltRequest.detectionTime). Null when the transition recorded no
        // detection — the column is nullable, so null is honest; it is never re-interpreted
        // from fenceAcquiredTs or from the transition time.
        v[ExecutionGateColumns.DETECTION_TIME] = r.detectionTs();
        v[ExecutionGateColumns.EVIDENCE_HASH] = r.evidenceHash() == null ? null : BinaryString.fromString(r.evidenceHash());
        v[ExecutionGateColumns.APPROVAL_1] = r.approval1() == null ? null : BinaryString.fromString(r.approval1());
        v[ExecutionGateColumns.APPROVAL_2] = r.approval2() == null ? null : BinaryString.fromString(r.approval2());
        // P3-370: a real transition time, stamped by the mutating event itself (see the
        // GateRow.with* copies). It is no longer re-interpreted from fenceAcquiredTs, so
        // "transition time unknown" is no longer inferred by comparing against an unrelated
        // column — only a synthetic row (not built through with*) writes 0.
        v[ExecutionGateColumns.TRANSITION_TS] = r.transitionTs();
        v[ExecutionGateColumns.OWNER_INSTANCE_ID] = r.ownerInstanceId() == null ? null : BinaryString.fromString(r.ownerInstanceId());
        v[ExecutionGateColumns.FENCE_TOKEN] = r.fenceToken();
        v[ExecutionGateColumns.FENCE_ACQUIRED_TS] = r.fenceAcquiredTs();
        v[ExecutionGateColumns.LEASE_EXPIRES_TS] = r.leaseExpiresTs();
        v[ExecutionGateColumns.FENCE_LOST_TS] = r.fenceLostTs();
        v[ExecutionGateColumns.APPROVED_EVIDENCE_HASH] = r.approvedEvidenceHash() == null ? null : BinaryString.fromString(r.approvedEvidenceHash());
        v[ExecutionGateColumns.SCHEMA_VERSION] = BinaryString.fromString(ExecutionGateColumns.SCHEMA_VERSION_V3);
        return v;
    }

    private static GateRow fromRow(InternalRow r) {
        // P3-366/P3-372: fail loud on schema mismatch — key/state columns are NOT
        // NULL in DDL, so a null or unknown value is corruption, never a default.
        // FENCE_TOKEN is nullable in DDL but 0 means "never fenced" in GateRow
        // (NautilusIntentClient defers on null/zero) — a null decodes to 0, never
        // throws, so legacy/unfenced rows stay readable and fail closed downstream.
        if (r.isNullAt(ExecutionGateColumns.EXECUTION_PARTITION_ID)
                || r.isNullAt(ExecutionGateColumns.ACCOUNT_SCOPE_ID)
                || r.isNullAt(ExecutionGateColumns.STATE)) {
            throw new IllegalStateException("corrupt Execution_Gate row: null key/state column");
        }
        String pid = r.getString(ExecutionGateColumns.EXECUTION_PARTITION_ID).toString();
        String acct = r.getString(ExecutionGateColumns.ACCOUNT_SCOPE_ID).toString();
        GateState st;
        try {
            st = GateState.valueOf(r.getString(ExecutionGateColumns.STATE).toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("corrupt Execution_Gate row: unknown state", e);
        }
        // SCHEMA_VERSION is NOT NULL v3 — a mismatch is a writer/DDL drift, fail loud.
        if (r.isNullAt(ExecutionGateColumns.SCHEMA_VERSION)
                || !ExecutionGateColumns.SCHEMA_VERSION_V3.equals(
                        r.getString(ExecutionGateColumns.SCHEMA_VERSION).toString())) {
            throw new IllegalStateException("Execution_Gate schema mismatch: expected v"
                    + ExecutionGateColumns.SCHEMA_VERSION_V3);
        }
        long epoch = r.getLong(ExecutionGateColumns.EPOCH);
        String reason = r.isNullAt(ExecutionGateColumns.REASON) ? null : r.getString(ExecutionGateColumns.REASON).toString();
        String ev = r.isNullAt(ExecutionGateColumns.EVIDENCE_HASH) ? null : r.getString(ExecutionGateColumns.EVIDENCE_HASH).toString();
        String a1 = r.isNullAt(ExecutionGateColumns.APPROVAL_1) ? null : r.getString(ExecutionGateColumns.APPROVAL_1).toString();
        String a2 = r.isNullAt(ExecutionGateColumns.APPROVAL_2) ? null : r.getString(ExecutionGateColumns.APPROVAL_2).toString();
        String approvedEv = r.isNullAt(ExecutionGateColumns.APPROVED_EVIDENCE_HASH) ? null : r.getString(ExecutionGateColumns.APPROVED_EVIDENCE_HASH).toString();
        String owner = r.isNullAt(ExecutionGateColumns.OWNER_INSTANCE_ID) ? null : r.getString(ExecutionGateColumns.OWNER_INSTANCE_ID).toString();
        long fenceToken = r.isNullAt(ExecutionGateColumns.FENCE_TOKEN) ? 0L : r.getLong(ExecutionGateColumns.FENCE_TOKEN);
        Long acq = r.isNullAt(ExecutionGateColumns.FENCE_ACQUIRED_TS) ? null : r.getLong(ExecutionGateColumns.FENCE_ACQUIRED_TS);
        Long lease = r.isNullAt(ExecutionGateColumns.LEASE_EXPIRES_TS) ? null : r.getLong(ExecutionGateColumns.LEASE_EXPIRES_TS);
        Long lost = r.isNullAt(ExecutionGateColumns.FENCE_LOST_TS) ? null : r.getLong(ExecutionGateColumns.FENCE_LOST_TS);
        // TRANSITION_TS is NOT NULL in the DDL, so it reads back as a real value.
        long transitionTs = r.getLong(ExecutionGateColumns.TRANSITION_TS);
        // DETECTION_TIME is nullable: null means no detection was recorded for this row.
        Long detectionTs = r.isNullAt(ExecutionGateColumns.DETECTION_TIME)
                ? null
                : r.getLong(ExecutionGateColumns.DETECTION_TIME);
        return new GateRow(pid, acct, st, epoch, reason, ev, a1, a2, approvedEv, owner, fenceToken, acq, lease, lost,
                transitionTs, detectionTs);
    }
}
