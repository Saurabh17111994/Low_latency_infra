package com.trading.common.schema.eod;

import com.trading.common.schema.EodControllerState;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * EOD controller orchestration (SCH-23): the tested control mechanism behind
 * the storage contract — "unverified or retryable state extends retention
 * through a tested control mechanism; a fixed DDL TTL comment is
 * insufficient" (docs/04_contracts/02-storage.md "EOD controller",
 * 01-foundation.md "EOD controller and offload gate").
 *
 * <p><b>T8 G1/G4 7d hardening (2026-08-22)</b>: live DDL TTL is now 7d (was 2d)
 * plus a <b>block-delete-unverified guard</b>: Fluss TTL delete is BLOCKED
 * until the iceberg manifest for that trading day is VERIFIED; otherwise the
 * controller extends retention (shadow-rewrite with extended TTL) and fires a
 * critical alert. The EOD fail Fri → Fri-Sun must survive 7d even when the
 * S3/Iceberg offload is delayed. See {@link EodPlanner} protected bound and
 * {@link EodRetentionPolicy#requiresExtension}.
 *
 * <p>The controller owns planning (per table: earliest unverified day,
 * protected source-expiry bound, margin, extension decision — via
 * {@link EodPlanner}), the offload drive (PENDING → WRITING → COMMITTED →
 * VERIFYING → VERIFIED with the two failure exits, every transition validated
 * by {@link EodOffloadRecord#transition}), retry/backoff, and manual
 * reconciliation. All state round-trips go through {@link EodStateStore}, so
 * a crashed run resumes idempotently: re-running a day re-reads the durable
 * state and continues from wherever the machine stopped (in-flight
 * WRITING/COMMITTED/VERIFYING days are re-driven, never lost or regressed).
 *
 * <p>Pure logic (planning, due-day selection, per-outcome transitions) is
 * separated from the store/executor I/O so the unit tests drive the exact
 * machine with an in-memory store.
 */
public final class EodController {

    private EodController() {}

    /**
     * Block-guard predicate: true when source data for the protected bound
     * must NOT be allowed to expire — the earliest unverified day is still
     * pending and its iceberg manifest is not VERIFIED. Callers must block
     * any Fluss TTL-driven delete and instead extend retention. This is the
     * G4 storage-guard half of T8; the 7d TTL half is the DDL change.
     * Placeholder is pure logic — the live enforcement is the
     * {@link EodPlanner.Plan#requiresExtension()} check plus the shadow
     * rewrite in {@link EodControllerTool#extend}.
     */
    public static boolean isDeleteBlocked(EodPlanner.Plan plan) {
        // P4-113: a noDays table plans null — nothing on file, nothing to
        // protect. Dereferencing blindly NPEs and defeats the guard.
        if (plan == null) {
            return false;
        }
        return plan.requiresExtension();
    }

    /** Per-table status plan: the planner output plus day-state tallies. */
    public record TablePlan(String table, EodPlanner.Plan plan, List<EodOffloadRecord> days,
                            int verifiedDays, int unverifiedDays, boolean noDays) {}

    /** One day's advance outcome (the durable state it landed in). */
    public record RunOutcome(String tradingDate, String table, EodControllerState from,
                             EodControllerState to, boolean verified, String note) {}

    /** Overall status for automation: healthy, extension overdue, or work pending. */
    public enum Status {
        /** Every day on file VERIFIED and the margin above the floor. */
        OK,
        /** At least one table's protected bound needs retention extension. */
        EXTENSION_REQUIRED,
        /** Unverified/retryable days exist but the margin is still safe. */
        PENDING_WORK
    }

    // ── planning ──────────────────────────────────────────────────────────

    /**
     * Plan every table in scope. A table with no days on file is reported as
     * {@code noDays} (nothing to protect — no extension, no work); otherwise
     * the {@link EodPlanner} computes the protected bound. Fail-closed: every
     * scoped table must have a live TTL in {@code liveTtls} — the controller
     * plans against the table's ACTUAL create-time TTL, never an assumed one.
     */
    public static List<TablePlan> planTables(List<EodOffloadRecord> days, List<String> tables,
            Map<String, Duration> liveTtls, ZoneId zone, Duration safetyFloor, Instant now) {
        Map<String, List<EodOffloadRecord>> byTable = days.stream()
                .collect(Collectors.groupingBy(EodOffloadRecord::tableName));
        List<TablePlan> plans = new ArrayList<>();
        for (String table : tables) {
            Duration liveTtl = liveTtls.get(table);
            if (liveTtl == null) {
                throw new IllegalArgumentException("no live TTL known for table " + table
                        + " — the EOD controller plans against the actual create-time TTL");
            }
            List<EodOffloadRecord> tableDays = byTable.getOrDefault(table, List.of());
            if (tableDays.isEmpty()) {
                plans.add(new TablePlan(table, null, List.of(), 0, 0, true));
                continue;
            }
            EodPlanner.Plan plan = EodPlanner.plan(tableDays, zone, liveTtl, safetyFloor, now);
            int verified = (int) tableDays.stream()
                    .filter(EodOffloadRecord::permitsSourceExpiry).count();
            plans.add(new TablePlan(table, plan, tableDays, verified,
                    tableDays.size() - verified, false));
        }
        return plans;
    }

    /** Overall status from the per-table plans.
     *  P4-339: safetyFloor is intentionally unused — the per-table margin vs
     *  floor decision already happened inside EodPlanner.plan (each plan
     *  carries requiresExtension); re-checking a single floor here would
     *  contradict per-table live TTLs. The parameter is kept for API symmetry
     *  with planTables so callers pass one floor consistently. */
    public static Status statusOf(List<TablePlan> plans, Duration safetyFloor) {
        for (TablePlan p : plans) {
            if (!p.noDays() && p.plan().requiresExtension()) {
                return Status.EXTENSION_REQUIRED;
            }
        }
        for (TablePlan p : plans) {
            if (p.unverifiedDays() > 0) {
                return Status.PENDING_WORK;
            }
        }
        return Status.OK;
    }

    // ── due-day selection (pure) ──────────────────────────────────────────

    /**
     * Days a run must advance: PENDING days up to and including the run date,
     * in-flight days from a crashed run (WRITING/COMMITTED/VERIFYING — re-driven,
     * never lost), and FAILED_RETRYABLE days whose backoff has elapsed. VERIFIED
     * days are terminal; FAILED_MANUAL days need explicit {@link #resetManual}.
     */
    public static List<EodOffloadRecord> dueDays(List<EodOffloadRecord> days, LocalDate runDate,
            long nowMs) {
        return days.stream().filter(d -> {
            LocalDate date = d.tradingDateAsLocalDate();
            // P4-271: gate every non-terminal state by runDate — a
            // future-dated in-flight or retryable day must wait for its EOD
            // boundary, not advance early.
            if (date.isAfter(runDate)) {
                return false;
            }
            return switch (d.state()) {
                case PENDING -> true;
                case WRITING, COMMITTED, VERIFYING -> true;
                case FAILED_RETRYABLE -> d.nextRetryAtMs() <= nowMs;
                case FAILED_MANUAL, VERIFIED -> false;
            };
        }).toList();
    }

    // ── run (offload drive) ───────────────────────────────────────────────

    /**
     * Run one EOD pass: ensure a PENDING record exists for the run date on
     * every scoped table, then advance every due day through the state machine
     * with the executor. Returns the per-day outcomes. Fail-closed: with the
     * {@link NotConfiguredEodOffloadExecutor} (the shipped default) every day
     * lands FAILED_RETRYABLE and extends retention — never a silent VERIFIED.
     */
    public static List<RunOutcome> runOnce(EodStateStore store, EodOffloadExecutor executor,
            LocalDate runDate, List<String> tables, String schemaVersion, Instant now)
            throws Exception {
        List<EodOffloadRecord> days = store.readAll();
        long nowMs = now.toEpochMilli();
        for (String table : tables) {
            boolean hasDay = days.stream().anyMatch(d -> d.tableName().equals(table)
                    && d.tradingDateAsLocalDate().equals(runDate));
            if (!hasDay) {
                store.upsert(EodOffloadRecord.initial(runDate, table, schemaVersion, nowMs));
            }
        }
        days = store.readAll(); // re-read after creating the run-date records
        // P4-272: scope the drive like reconcile() does — a store holding
        // other tables' days must not have them mutated by an unrelated run.
        Set<String> scope = Set.copyOf(tables);
        List<RunOutcome> outcomes = new ArrayList<>();
        for (EodOffloadRecord day : dueDays(days, runDate, nowMs)) {
            if (!scope.contains(day.tableName())) {
                continue;
            }
            outcomes.add(advance(store, executor, day, nowMs));
        }
        return outcomes;
    }

    /**
     * Advance one day through the machine. Legal from every non-terminal
     * state; VERIFIED/FAILED_MANUAL days are refused (they need
     * {@link #reconcile}/{@link #resetManual}, never a blind re-run).
     */
    public static RunOutcome advance(EodStateStore store, EodOffloadExecutor executor,
            EodOffloadRecord day, long nowMs) throws Exception {
        EodControllerState from = day.state();
        return switch (from) {
            case PENDING, FAILED_RETRYABLE -> {
                EodOffloadRecord writing = day.transition(EodControllerState.WRITING, nowMs);
                store.upsert(writing);
                yield afterOffload(store, executor, writing, from, nowMs);
            }
            case WRITING -> afterOffload(store, executor, day, from, nowMs);
            case COMMITTED, VERIFYING -> verifyPhase(store, executor, day, nowMs);
            case VERIFIED, FAILED_MANUAL -> throw new IllegalStateException(
                    "run must not advance a " + from + " day for " + day.tableName()
                            + " " + day.tradingDate() + " — reconcile/reset only");
        };
    }

    private static RunOutcome afterOffload(EodStateStore store, EodOffloadExecutor executor,
            EodOffloadRecord writing, EodControllerState from, long nowMs) throws Exception {
        OffloadResult result;
        try {
            result = executor.offload(writing);
        } catch (Exception e) {
            // Throw == returned failure (executor contract): FAILED_RETRYABLE
            // with backoff, never VERIFIED — and the loop continues with the
            // remaining due days.
            result = OffloadResult.failure("offload threw: "
                    + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
        if (result == null) {
            result = OffloadResult.failure("offload returned null");
        }
        if (result.success()) {
            EodOffloadRecord content = withContent(writing, result);
            EodOffloadRecord committed = content.transition(EodControllerState.COMMITTED, nowMs);
            store.upsert(committed);
            return verifyPhase(store, executor, committed, nowMs);
        }
        EodOffloadRecord failed = writing.transition(EodControllerState.FAILED_RETRYABLE, nowMs);
        store.upsert(failed);
        return new RunOutcome(writing.tradingDate(), writing.tableName(), from,
                EodControllerState.FAILED_RETRYABLE, false, result.error());
    }

    /** Verify a committed/in-verifying copy — the only path to VERIFIED. */
    private static RunOutcome verifyPhase(EodStateStore store, EodOffloadExecutor executor,
            EodOffloadRecord committedOrVerifying, long nowMs) throws Exception {
        boolean alreadyVerifying =
                committedOrVerifying.state() == EodControllerState.VERIFYING;
        EodOffloadRecord verifying = alreadyVerifying ? committedOrVerifying
                : committedOrVerifying.transition(EodControllerState.VERIFYING, nowMs);
        if (!alreadyVerifying) {
            store.upsert(verifying);
        }
        boolean ok;
        // P4-273: keep the root cause — a bare ok=false makes retryable vs
        // manual failures indistinguishable in alerts.
        String verifyError = "verify failed (copy did not reconcile)";
        try {
            ok = executor.verify(verifying);
        } catch (Exception e) {
            ok = false;
            verifyError = "verify threw: "
                    + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
        if (ok) {
            EodOffloadRecord verified = verifying.transition(EodControllerState.VERIFIED, nowMs);
            store.upsert(verified);
            return new RunOutcome(verified.tradingDate(), verified.tableName(),
                    committedOrVerifying.state(), EodControllerState.VERIFIED, true, "");
        }
        EodOffloadRecord failed = verifying.transition(EodControllerState.FAILED_RETRYABLE, nowMs);
        store.upsert(failed);
        return new RunOutcome(failed.tradingDate(), failed.tableName(),
                committedOrVerifying.state(), EodControllerState.FAILED_RETRYABLE, false,
                verifyError);
    }

    // ── reconcile / reset ─────────────────────────────────────────────────

    /**
     * Reconciliation pass: re-verify every COMMITTED/VERIFYING day in scope
     * against the offload target (the crash-resume and manual-reconcile path).
     */
    public static List<RunOutcome> reconcile(EodStateStore store, EodOffloadExecutor executor,
            List<String> tables, Instant now) throws Exception {
        List<RunOutcome> outcomes = new ArrayList<>();
        Set<String> scope = Set.copyOf(tables);
        for (EodOffloadRecord day : store.readAll()) {
            if (!scope.contains(day.tableName())) {
                continue;
            }
            if (day.state() == EodControllerState.COMMITTED
                    || day.state() == EodControllerState.VERIFYING) {
                outcomes.add(verifyPhase(store, executor, day, now.toEpochMilli()));
            }
        }
        return outcomes;
    }

    /**
     * Manual reconciliation: reset FAILED_MANUAL days to PENDING (the only
     * legal exit from FAILED_MANUAL). Optional {@code tradingDate} /
     * {@code tableName} scoping; returns the number of days reset. The caller
     * (the CLI {@code reset --approve}) is the destructive-approval gate.
     */
    public static int resetManual(EodStateStore store, Instant now, String tradingDate,
            String tableName) throws Exception {
        int reset = 0;
        long nowMs = now.toEpochMilli();
        for (EodOffloadRecord day : store.readAll()) {
            if (day.state() != EodControllerState.FAILED_MANUAL) {
                continue;
            }
            if (tradingDate != null && !tradingDate.equals(day.tradingDate())) {
                continue;
            }
            if (tableName != null && !tableName.equals(day.tableName())) {
                continue;
            }
            store.upsert(day.transition(EodControllerState.PENDING, nowMs));
            reset++;
        }
        return reset;
    }

    // ── record content (pure) ─────────────────────────────────────────────

    /** The day record with the offload evidence attached (still in WRITING). */
    static EodOffloadRecord withContent(EodOffloadRecord r, OffloadResult res) {
        return new EodOffloadRecord(r.tradingDate(), r.tableName(), r.schemaVersion(),
                res.sourceOffsetStart(), res.sourceOffsetEnd(), res.rowCount(), res.byteCount(),
                res.sourceHash(), res.targetHash(), res.icebergSnapshotId(), r.state(),
                r.retryCount(), r.nextRetryAtMs(), r.earliestAllowedSourceExpiryMs(),
                r.updatedAtMs());
    }
}
