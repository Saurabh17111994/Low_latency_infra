package com.trading.common.schema.eod;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

// Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
/**
 * EOD controller CLI (SCH-23): the thin runner over {@link EodPlanner} /
 * {@link EodController} against live Fluss tables, mirroring the ddl-apply
 * tool pattern (one-shot subcommands, {@code FLUSS_BOOTSTRAP} via env,
 * machine-readable {@code eod-controller: RESULT=... EXIT=...} sentinel).
 *
 * <pre>{@code
 * eod-controller status    read-only per-table plan + day-state summary
 * eod-controller run       advance due days through the offload state machine
 *                          (creates the run-date PENDING record per table)
 * eod-controller extend    retention-extension recipe; --apply applies the
 *                          table.log.ttl ALTER
 * eod-controller reconcile re-verify COMMITTED/VERIFYING days (crash-resume)
 * eod-controller reset     FAILED_MANUAL -> PENDING (requires --approve)
 * }</pre>
 *
 * <p>Exit codes: 0 OK (or extension applied / days verified), 1 failure,
 * 2 extension required (or retryable days), 3 pending work (status),
 * 4 usage/approval-required, 5 lease held by another controller.
 *
 * <p>Retention extension is one ALTER: Fluss 1.0.0 accepts
 * {@code table.log.ttl} and enforces it on existing sealed segments (A1 probe
 * GREEN 2026-09-25 — accepted + in force in ZK; A2 probe GREEN 2026-09-25 —
 * enforced expiry follows the ALTER in both directions). {@code extend
 * --apply} therefore runs {@code Admin.alterTable} only — no shadow table, no
 * row copy, no swap. The 0.9.1 create-only boundary (verified 2026-08-13) is
 * retired.
 */
public final class EodControllerTool {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * EOD-eligible live tables — now 7d TTL (T8 G1/G4 hardened 2026-08-22 — was
     * ten 2d-TTL tables 2026-08-13). The documented default scope remains ten
     * tables for backward compat, but the storage contract is 7 calendar days
     * + block-delete-unverified guard: Fluss TTL delete is BLOCKED until the
     * iceberg manifest is VERIFIED; otherwise the controller extends retention
     * via one {@code table.log.ttl} ALTER and fires a critical alert.
     *
     * <p>{@code candle_features} (DDL 35, Wave C W-C5a 2026-09-30) succeeded
     * the retired {@code candle_closed} (DDL 33) in the default scope — the
     * merged candle+feature history table carries the archived candle chain
     * (DEC-059); its DDL ships a 3d log TTL, with the controller's TTL option
     * as the retention lever.
     */
    static final List<String> DEFAULT_TABLES = List.of(
            "raw_table_1", "candle_features", "ingestion_quarantine",
            "Order_Lifecycle", "suspected_discontinuities", "Postback_Quarantine",
            "Trade_Decisions", "Ranking_Results", "Portfolio_Reservations",
            "Postback_Projection_Ledger");

    private EodControllerTool() {}

    public static void main(String[] args) {
        try {
            System.exit(run(args));
        } catch (Throwable t) {
            System.err.println("eod-controller: FATAL — " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    static int run(String[] args) throws Exception {
        for (String a : args) {
            if (a.equals("--help") || a.equals("-h")) {
                return usage();
            }
        }
        // P4-276/280: parse/validation errors are usage errors (exit 4), not
        // FATAL exit 1 — the documented exit=4 contract covers bad flags,
        // bad values, and missing subcommand.
        Options opts;
        try {
            opts = Options.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("eod-controller: " + e.getMessage());
            return usage();
        }
        ZoneId zone = ZoneId.of(opts.zone);
        Instant now = Instant.now();
        LocalDate runDate = opts.runDate != null ? LocalDate.parse(opts.runDate)
                : LocalDate.now(zone);
        long nowMs = now.toEpochMilli();

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", opts.bootstrap);
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            // Live TTLs — resolved lazily per subcommand that needs them
            // (status/extend, P4-275/281): run/reconcile/reset never use
            // liveTtls, so resolving eagerly here paid TABLES*admin-RPC cost
            // (up to 30s each) plus spurious fallback warnings on write paths.
            Map<String, Duration> liveTtls = new LinkedHashMap<>();
            java.util.function.Function<String, Map<String, Duration>> liveTtlsFor =
                    sub -> {
                        if (!liveTtls.isEmpty()) {
                            return liveTtls;
                        }
                        if (!sub.equals("status") && !sub.equals("extend")) {
                            return liveTtls; // write paths plan without live TTLs
                        }
                        for (String table : opts.tables) {
                            liveTtls.put(table,
                                    liveTtl(admin, opts.database, table, opts.ttlDefault));
                        }
                        return liveTtls;
                    };

            return switch (opts.subcommand) {
                case "status" -> status(opts, liveTtlsFor.apply("status"), zone, now);
                case "run" -> run(opts, liveTtlsFor.apply("run"), zone, runDate, now, nowMs);
                case "extend" -> extend(opts, admin, liveTtlsFor.apply("extend"), zone, now);
                case "reconcile" -> reconcile(opts, liveTtlsFor.apply("reconcile"), now);
                case "reset" -> reset(opts, now);
                case "tiering" -> tiering(opts, admin);
                default -> usage();
            };
        }
    }

    // ── subcommands ───────────────────────────────────────────────────────

    private static int status(Options opts, Map<String, Duration> liveTtls, ZoneId zone,
            Instant now) throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            List<EodOffloadRecord> days = store.readAll();
            List<EodController.TablePlan> plans = EodController.planTables(
                    days, opts.tables, liveTtls, zone, opts.safetyFloor, now);
            for (EodController.TablePlan p : plans) {
                if (p.noDays()) {
                    System.out.println("eod-controller: status " + p.table()
                            + " — no days on file");
                    continue;
                }
                EodPlanner.Plan plan = p.plan();
                System.out.println("eod-controller: status " + p.table()
                        + " earliestUnverified=" + (plan.earliestUnverifiedDate() == null
                                ? "-" : plan.earliestUnverifiedDate())
                        + " protectedBound=" + (plan.protectedExpiryBound() == null
                                ? "-" : plan.protectedExpiryBound())
                        + " margin=" + (plan.allVerified() ? "-" : plan.marginMs() + "ms")
                        + " requiresExtension=" + plan.requiresExtension()
                        + " verified=" + p.verifiedDays() + " unverified=" + p.unverifiedDays());
                for (EodOffloadRecord d : p.days()) {
                    System.out.println("eod-controller:   day " + d.tradingDate()
                            + " state=" + d.state() + " retry=" + d.retryCount()
                            + " nextRetry=" + (d.nextRetryAtMs() == 0 ? "-"
                            : Instant.ofEpochMilli(d.nextRetryAtMs())));
                }
            }
            EodController.Status s = EodController.statusOf(plans, opts.safetyFloor);
            String result = switch (s) {
                case OK -> "OK";
                case EXTENSION_REQUIRED -> "EXTENSION_REQUIRED";
                case PENDING_WORK -> "PENDING_WORK";
            };
            int exit = switch (s) {
                case OK -> 0;
                case EXTENSION_REQUIRED -> 2;
                case PENDING_WORK -> 3;
            };
            // G4 block-guard alert: when any table needs extension, its
            // earliestUnverified day's sourceExpiryBound is the protected bound —
            // Fluss delete is blocked until that day's iceberg manifest is
            // VERIFIED. Emit a critical alert so the weekend EOD-fail scenario
            // (Fri offload unverified → Fri-Sun must survive past the 3d TTL)
            // is observable. The EOD controller's extend path then holds the data.
            if (s == EodController.Status.EXTENSION_REQUIRED) {
                for (EodController.TablePlan p : plans) {
                    if (!p.noDays() && p.plan().requiresExtension()) {
                        String when = p.plan().earliestUnverifiedDate().toString();
                        System.err.println("eod-controller: ALERT CRITICAL — retention extension required for "
                                + p.table() + " earliestUnverified=" + when
                                + " protectedBound=" + p.plan().protectedExpiryBound()
                                + " margin=" + p.plan().marginMs() + "ms"
                                + " — Fluss TTL delete BLOCKED until iceberg manifest VERIFIED");
                    }
                }
            }
            System.out.println("eod-controller: RESULT=" + result + " EXIT=" + exit
                    + " TABLES=" + opts.tables.size() + " DAYS=" + days.size());
            return exit;
        }
    }

    private static int run(Options opts, Map<String, Duration> liveTtls, ZoneId zone,
            LocalDate runDate, Instant now, long nowMs) throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            if (opts.dryRun) {
                List<EodOffloadRecord> days = store.readAll();
                List<EodOffloadRecord> due = EodController.dueDays(days, runDate, nowMs);
                System.out.println("eod-controller: dry-run — run-date " + runDate
                        + " dueDays=" + due.size() + " executor=" + opts.offloadMode);
                for (EodOffloadRecord d : due) {
                    System.out.println("eod-controller:   would advance " + d.tableName()
                            + " " + d.tradingDate() + " (" + d.state() + ")");
                }
                System.out.println("eod-controller: RESULT=DRY_RUN EXIT=0 TABLES="
                        + opts.tables.size() + " DAYS=" + due.size());
                return 0;
            }
            String myToken = token();
            Lease lease = store.acquireLease(myToken, nowMs, opts.leaseTtl.toMillis());
            if (!lease.isHeldBy(myToken, nowMs)) {
                System.err.println("eod-controller: lease held by " + lease.token()
                        + " until " + Instant.ofEpochMilli(lease.expiryMs())
                        + " — refusing to run (single-writer fencing)");
                System.out.println("eod-controller: RESULT=LEASED EXIT=5 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 5;
            }
            EodOffloadExecutor executor = buildOffloadExecutor(opts.offloadMode);
            List<EodController.RunOutcome> outcomes = EodController.runOnce(store, executor,
                    runDate, opts.tables, opts.schemaVersion, now);
            return reportRunOutcomes(outcomes, "run");
        }
    }

    private static int extend(Options opts, Admin admin,
            Map<String, Duration> liveTtls, ZoneId zone, Instant now) throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            // P4-121/122: extend --apply mutates state (retention ALTER) — it
            // must hold the single-writer lease like run/reconcile.
            if (opts.apply && !opts.dryRun) {
                String myToken = token();
                Lease lease = store.acquireLease(myToken, now.toEpochMilli(),
                        opts.leaseTtl.toMillis());
                if (!lease.isHeldBy(myToken, now.toEpochMilli())) {
                    System.err.println("eod-controller: lease held by " + lease.token()
                            + " — refusing extend --apply");
                    System.out.println("eod-controller: RESULT=LEASED EXIT=5 TABLES="
                            + opts.tables.size() + " DAYS=0");
                    return 5;
                }
            }
            List<EodOffloadRecord> days = store.readAll();
            List<EodController.TablePlan> plans = EodController.planTables(
                    days, opts.tables, liveTtls, zone, opts.safetyFloor, now);
            boolean required = false;
            boolean appliedAll = true;
            for (EodController.TablePlan p : plans) {
                if (p.noDays() || !p.plan().requiresExtension()) {
                    continue;
                }
                required = true;
                Duration live = liveTtls.get(p.table());
                Duration newTtl = EodRetentionPolicy.extendedTtl(live, opts.extension);
                // Block-guard: this table's delete is currently BLOCKED — the
                // protected bound is unverified, so the ALTER (extended 7d +
                // extra) must succeed before the old data is allowed to
                // expire. The alert below makes the hold observable.
                System.err.println("eod-controller: ALERT CRITICAL — block-guard extend for "
                        + p.table() + " liveTtl=" + live + " -> newTtl=" + newTtl
                        + " — Fluss delete BLOCKED until VERIFIED (iceberg manifest)");
                System.out.println("eod-controller: extend " + p.table()
                        + " liveTtl=" + live + " newTtl=" + newTtl
                        + " margin=" + p.plan().marginMs() + "ms");
                if (opts.apply && !opts.dryRun) {
                    if (!alterTtl(admin, opts.database, p.table(), newTtl, TIMEOUT.toMillis())) {
                        appliedAll = false;
                    }
                }
            }
            if (!required) {
                System.out.println("eod-controller: RESULT=OK EXIT=0 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 0;
            }
            if (opts.dryRun) {
                // recipe only — the drill is not executed
                System.out.println("eod-controller: RESULT=EXTENSION_REQUIRED EXIT=2 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 2;
            }
            if (opts.apply) {
                if (appliedAll) {
                    System.out.println("eod-controller: RESULT=EXTENDED EXIT=0 TABLES="
                            + opts.tables.size() + " DAYS=0");
                    return 0;
                }
                System.err.println("eod-controller: RESULT=EXTEND_FAILED EXIT=1 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 1;
            }
            System.out.println("eod-controller: RESULT=EXTENSION_REQUIRED EXIT=2 TABLES="
                    + opts.tables.size() + " DAYS=0");
            return 2;
        }
    }

    private static int reconcile(Options opts, Map<String, Duration> liveTtls, Instant now)
            throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            String myToken = token();
            Lease lease = store.acquireLease(myToken, now.toEpochMilli(),
                    opts.leaseTtl.toMillis());
            if (!lease.isHeldBy(myToken, now.toEpochMilli())) {
                System.err.println("eod-controller: lease held by " + lease.token()
                        + " — refusing to reconcile");
                System.out.println("eod-controller: RESULT=LEASED EXIT=5 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 5;
            }
            EodOffloadExecutor executor = buildOffloadExecutor(opts.offloadMode);
            List<EodController.RunOutcome> outcomes = EodController.reconcile(
                    store, executor, opts.tables, now);
            return reportRunOutcomes(outcomes, "reconcile");
        }
    }

    private static int reset(Options opts, Instant now) throws Exception {
        if (!opts.approve) {
            System.err.println("eod-controller: reset is destructive — pass --approve "
                    + "(FAILED_MANUAL -> PENDING)");
            System.out.println("eod-controller: RESULT=APPROVAL_REQUIRED EXIT=4 TABLES="
                    + opts.tables.size() + " DAYS=0");
            return 4;
        }
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            // P4-121/122: reset mutates state (FAILED_MANUAL -> PENDING) —
            // same lease rule as extend --apply above.
            String myToken = token();
            Lease lease = store.acquireLease(myToken, now.toEpochMilli(),
                    opts.leaseTtl.toMillis());
            if (!lease.isHeldBy(myToken, now.toEpochMilli())) {
                System.err.println("eod-controller: lease held by " + lease.token()
                        + " — refusing reset");
                System.out.println("eod-controller: RESULT=LEASED EXIT=5 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 5;
            }
            int reset = EodController.resetManual(store, now, opts.runDate, opts.singleTable);
            System.out.println("eod-controller: reset " + reset
                    + " FAILED_MANUAL day(s) -> PENDING");
            System.out.println("eod-controller: RESULT=RESET EXIT=0 TABLES="
                    + opts.tables.size() + " DAYS=" + reset);
            return 0;
        }
    }

    private static int reportRunOutcomes(List<EodController.RunOutcome> outcomes, String phase) {
        int verified = 0, retryable = 0;
        Set<String> tables = outcomes.stream()
                .map(EodController.RunOutcome::table).collect(Collectors.toSet());
        for (EodController.RunOutcome o : outcomes) {
            System.out.println("eod-controller: " + phase + " " + o.table() + " "
                    + o.tradingDate() + " " + o.from() + " -> " + o.to()
                    + (o.verified() ? " VERIFIED" : "")
                    + (o.note().isEmpty() ? "" : " (" + o.note() + ")"));
            if (o.verified()) {
                verified++;
            } else {
                retryable++;
            }
        }
        int exit = retryable > 0 ? 2 : 0;
        String result = retryable > 0 ? "RETRYABLE" : "VERIFIED";
        System.out.println("eod-controller: RESULT=" + result + " EXIT=" + exit
                + " TABLES=" + tables.size() + " DAYS=" + outcomes.size());
        return exit;
    }

    // ── extend: retention ALTER (live) ───────────────────────────────────────

    /**
     * Apply the extended retention with one {@code Admin.alterTable} —
     * {@code table.log.ttl} is alterable and enforced in Fluss 1.0.0 (A1 probe
     * GREEN 2026-09-25: accepted + in force in ZK; A2 probe GREEN 2026-09-25:
     * enforced expiry follows the ALTER for existing sealed segments, both
     * directions). No shadow table, no row copy, no swap. Returns true when the
     * ALTER was accepted.
     */
    static boolean alterTtl(Admin admin, String database, String table, Duration newTtl,
            long timeoutMs) {
        String ttl = ttlOption(newTtl);
        try {
            List<TableChange> changes = new java.util.ArrayList<>();
            changes.add(TableChange.set("table.log.ttl", ttl));
            // 2026-09-30 (A2): a partitioned table has TWO expiry mechanisms —
            // the log TTL and auto-partition GC (table.auto-partition.num-retention
            // days). Extending only the TTL would leave the partition to drop at
            // its original count, so the guard raises both on partitioned tables.
            TableInfo info = admin.getTableInfo(TablePath.of(database, table))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            boolean partitioned = info.getPartitionKeys() != null
                    && !info.getPartitionKeys().isEmpty();
            if (partitioned) {
                long dayMs = Duration.ofDays(1).toMillis();
                long days = Math.max(1, (newTtl.toMillis() + dayMs - 1) / dayMs);
                changes.add(TableChange.set("table.auto-partition.num-retention",
                        Long.toString(days)));
            }
            admin.alterTable(TablePath.of(database, table), changes, false)
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            System.out.println("eod-controller: ALTER " + table + " table.log.ttl=" + ttl
                    + (partitioned ? " table.auto-partition.num-retention="
                            + (newTtl.toMillis() + Duration.ofDays(1).toMillis() - 1)
                                    / Duration.ofDays(1).toMillis() : ""));
            return true;
        } catch (Exception e) {
            System.err.println("eod-controller: ALTER table.log.ttl=" + ttl
                    + " failed for " + table + ": " + e);
            return false;
        }
    }

    // ── R2 archive selection (DEC-060, 2026-09-30) ───────────────────────

    /**
     * {@code tiering [--set on|off] [--tables <name>]} — the cluster half of
     * the R2 archive selection (DEC-060): the default ({@code --list}) prints
     * every live table's {@code table.datalake.enabled} flag as
     * {@code tiering <name>=enabled|disabled|unreadable}; {@code --set} flips
     * it on exactly one named table. The selection <b>rules</b> live in
     * {@code r2_archive_selection.py} (one list, nothing outside it) — this
     * tool is deliberately dumb so the sync and the gate guard share one rule
     * source. Exit 1 when any flag is unreadable (fail-closed for the guard).
     */
    private static int tiering(Options opts, Admin admin) throws Exception {
        if (opts.tieringSet() == null) {
            List<String> tables = admin.listTables(opts.database())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            int enabled = 0;
            int unreadable = 0;
            for (String table : tables) {
                Boolean on = datalakeEnabled(admin, opts.database(), table);
                if (on == null) {
                    unreadable++;
                    System.out.println("tiering " + table + "=unreadable");
                } else {
                    if (on) {
                        enabled++;
                    }
                    System.out.println("tiering " + table + "="
                            + (on ? "enabled" : "disabled"));
                }
            }
            System.out.println("eod-controller: RESULT="
                    + (unreadable == 0 ? "OK" : "UNREADABLE")
                    + " EXIT=" + (unreadable == 0 ? 0 : 1)
                    + " TABLES=" + tables.size() + " ENABLED=" + enabled);
            return unreadable == 0 ? 0 : 1;
        }
        if (opts.tables().size() != 1) {
            System.err.println("eod-controller: --set needs exactly one table"
                    + " (--tables <name>)");
            return 2;
        }
        String table = opts.tables().get(0);
        boolean on = "on".equals(opts.tieringSet());
        try {
            admin.alterTable(TablePath.of(opts.database(), table),
                    List.of(TableChange.set("table.datalake.enabled", on ? "true" : "false")),
                    false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            System.out.println("eod-controller: ALTER " + table
                    + " table.datalake.enabled=" + (on ? "true" : "false"));
            return 0;
        } catch (Exception e) {
            System.err.println("eod-controller: ALTER table.datalake.enabled="
                    + (on ? "true" : "false") + " failed for " + table + ": " + e);
            if (String.valueOf(e).contains("InvalidAlterTableException")) {
                System.err.println("eod-controller: this looks like a legacy table"
                        + " (created before the cluster gained datalake.format) — a"
                        + " one-time recreate is required before it can be archived");
            }
            return 1;
        }
    }

    /** The live {@code table.datalake.enabled} flag; {@code null} on read failure. */
    static Boolean datalakeEnabled(Admin admin, String database, String table) {
        try {
            TableInfo info = admin.getTableInfo(TablePath.of(database, table))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return "true".equalsIgnoreCase(
                    info.getProperties().toMap().get("table.datalake.enabled"));
        } catch (Exception e) {
            System.err.println("eod-controller: cannot read table.datalake.enabled for "
                    + table + ": " + e);
            return null;
        }
    }

    /** Render a Duration as a Fluss TTL option value (2d / 1h / 30m / 5000ms). */
    static String ttlOption(Duration d) {
        long dayMs = Duration.ofDays(1).toMillis();
        long hourMs = Duration.ofHours(1).toMillis();
        long minuteMs = Duration.ofMinutes(1).toMillis();
        if (d.toMillis() % dayMs == 0) {
            return d.toDays() + "d";
        }
        if (d.toMillis() % hourMs == 0) {
            return d.toHours() + "h";
        }
        if (d.toMillis() % minuteMs == 0) {
            return d.toMinutes() + "m";
        }
        return d.toMillis() + "ms";
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static Duration liveTtl(Admin admin, String database, String table,
            Duration fallback) {
        try {
            TableInfo info = admin.getTableInfo(TablePath.of(database, table))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            String ttl = info.getProperties().toMap().get("table.log.ttl");
            if (ttl != null && !ttl.isBlank()) {
                return EodRetentionPolicy.parseTtl(ttl);
            }
        } catch (Exception e) {
            // metadata unavailable — fall back below
        }
        System.err.println("eod-controller: no live table.log.ttl for " + table
                + " — using fallback " + fallback);
        return fallback;
    }

    private static String token() {
        String host = System.getenv().getOrDefault("HOSTNAME", "unknown");
        return host + "-" + System.nanoTime();
    }

    private static int usage() {
        System.err.println("""
                usage: eod-controller <status|run|extend|reconcile|reset|tiering> [options]
                  --bootstrap <addr>   (env FLUSS_BOOTSTRAP, default localhost:9123)
                  --database <db>      (env FLUSS_DATABASE, default default)
                  --state-table <name> (env EOD_STATE_TABLE, default eod_offload_state)
                  --tables <t1,t2>     (env EOD_TABLES — EOD-eligible tables)
                  --ttl <ttl>          (env EOD_TTL, default 3d — live-TTL fallback)
                  --safety-floor <ttl> (env EOD_SAFETY_FLOOR, default 1d — extension runway for unverified days)
                  --extension <ttl>    (env EOD_EXTENSION, default 30d)
                  --lease-ttl <ttl>    (env EOD_LEASE_TTL, default 30m)
                  --zone <zone>        (env EOD_ZONE, default Asia/Kolkata)
                  --run-date <date>    (run: trading date, default today in --zone)
                  --schema-version <v> (env EOD_SCHEMA_VERSION, default 1)
                  --offload none|mock|lake  (env EOD_OFFLOAD, default none — fail-closed)
                  --table <name>       (reset: single table scope)
                  --list               (tiering: list every table's archive flag — the default)
                  --set on|off         (tiering: flip table.datalake.enabled on exactly one --tables entry)
                  --apply              (extend: apply the table.log.ttl ALTER)
                  --dry-run            (run/extend: print, don't write)
                  --approve            (reset: destructive approval)
                exit: 0 ok, 1 failure, 2 extension/retryable, 3 pending work,
                      4 usage/approval, 5 lease held""");
        return 4;
    }

    // ── options ───────────────────────────────────────────────────────────

    /** Executor selection (2026-08-31): none=fail-closed, mock=drills,
     *  lake=R2 iceberg evidence via r2-list.sh (tiering job does the copy). */
    private static EodOffloadExecutor buildOffloadExecutor(String mode) {
        switch (mode.toLowerCase()) {
            case "mock":
                return new MockEodOffloadExecutor(true, true);
            case "lake":
                String listSh = System.getenv().getOrDefault("R2_LIST_SCRIPT", "");
                if (listSh.isBlank()) {
                    throw new IllegalArgumentException(
                            "EOD_OFFLOAD=lake requires R2_LIST_SCRIPT (absolute path to r2-list.sh)");
                }
                return new R2LakeTieringEodOffloadExecutor(
                        java.nio.file.Path.of(listSh),
                        System.getenv().getOrDefault("R2_LAKE_PREFIX", "lake"));
            default:
                return NotConfiguredEodOffloadExecutor.INSTANCE;
        }
    }

    record Options(String subcommand, String bootstrap, String database, String stateTable,
                   List<String> tables, Duration ttlDefault, Duration safetyFloor,
                   Duration extension, Duration leaseTtl, String zone, String runDate,
                   String schemaVersion, String offloadMode, String singleTable,
                   String tieringSet, boolean apply, boolean dryRun, boolean approve) {

        static Options parse(String[] args) {
            if (args.length == 0) {
                throw new IllegalArgumentException("subcommand required "
                        + "(status|run|extend|reconcile|reset|tiering)");
            }
            String subcommand = args[0];
            String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
            String database = System.getenv().getOrDefault("FLUSS_DATABASE", "default");
            String stateTable = System.getenv().getOrDefault("EOD_STATE_TABLE",
                    "eod_offload_state");
            String tablesRaw = System.getenv().getOrDefault("EOD_TABLES", null);
            Duration ttlDefault = parseEnvTtl("EOD_TTL", Duration.ofDays(3));
            Duration safetyFloor = parseEnvTtl("EOD_SAFETY_FLOOR", Duration.ofDays(1));
            Duration extension = parseEnvTtl("EOD_EXTENSION", Duration.ofDays(30));
            Duration leaseTtl = parseEnvTtl("EOD_LEASE_TTL", Duration.ofMinutes(30));
            String zone = System.getenv().getOrDefault("EOD_ZONE", "Asia/Kolkata");
            String runDate = null;
            String schemaVersion = System.getenv().getOrDefault("EOD_SCHEMA_VERSION", "1");
            String offloadMode = System.getenv().getOrDefault("EOD_OFFLOAD", "none");
            String singleTable = null;
            String tieringSet = null;
            boolean tieringList = false;
            boolean apply = false;
            boolean dryRun = false;
            boolean approve = false;

            List<String> tableArgs = new ArrayList<>();
            for (int i = 1; i < args.length; i++) {
                switch (args[i]) {
                    case "--bootstrap" -> bootstrap = nextArg(args, ++i, "--bootstrap");
                    case "--database" -> database = nextArg(args, ++i, "--database");
                    case "--state-table" -> stateTable = nextArg(args, ++i, "--state-table");
                    case "--tables" -> {
                        for (String t : nextArg(args, ++i, "--tables").split(",")) {
                            String trimmed = t.trim();
                            if (!trimmed.isEmpty()) {
                                tableArgs.add(trimmed);
                            }
                        }
                    }
                    case "--ttl" -> ttlDefault = EodRetentionPolicy.parseTtl(
                            nextArg(args, ++i, "--ttl"));
                    case "--safety-floor" -> safetyFloor = EodRetentionPolicy.parseTtl(
                            nextArg(args, ++i, "--safety-floor"));
                    case "--extension" -> extension = EodRetentionPolicy.parseTtl(
                            nextArg(args, ++i, "--extension"));
                    case "--lease-ttl" -> leaseTtl = EodRetentionPolicy.parseTtl(
                            nextArg(args, ++i, "--lease-ttl"));
                    case "--zone" -> zone = nextArg(args, ++i, "--zone");
                    case "--run-date" -> runDate = nextArg(args, ++i, "--run-date");
                    case "--schema-version" -> schemaVersion =
                            nextArg(args, ++i, "--schema-version");
                    case "--offload" -> offloadMode = nextArg(args, ++i, "--offload");
                    case "--table" -> singleTable = nextArg(args, ++i, "--table");
                    case "--list" -> tieringList = true;
                    case "--set" -> {
                        String value = nextArg(args, ++i, "--set");
                        if (!value.equals("on") && !value.equals("off")) {
                            throw new IllegalArgumentException("--set must be on or off, got "
                                    + value);
                        }
                        tieringSet = value;
                    }
                    case "--apply" -> apply = true;
                    case "--dry-run" -> dryRun = true;
                    case "--approve" -> approve = true;
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            if (!offloadMode.equalsIgnoreCase("none") && !offloadMode.equalsIgnoreCase("mock")
                    && !offloadMode.equalsIgnoreCase("lake")) {
                throw new IllegalArgumentException("--offload must be none, mock or lake, got "
                        + offloadMode);
            }
            if (tieringList && tieringSet != null) {
                throw new IllegalArgumentException("--list and --set are mutually exclusive");
            }
            if (runDate != null && !runDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
                throw new IllegalArgumentException("--run-date must be yyyy-MM-dd, got " + runDate);
            }
            List<String> tables;
            if (!tableArgs.isEmpty()) {
                tables = List.copyOf(tableArgs);
            } else if (tablesRaw != null) {
                // P4-277/284: normalize the env path exactly like the flag
                // path — "a, b" yielded table " b" (lookup miss) and empty env
                // yielded [""] instead of DEFAULT_TABLES.
                tables = java.util.Arrays.stream(tablesRaw.split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).toList();
                if (tables.isEmpty()) {
                    tables = DEFAULT_TABLES;
                }
            } else {
                tables = DEFAULT_TABLES;
            }
            return new Options(subcommand, bootstrap, database, stateTable, tables,
                    ttlDefault, safetyFloor, extension, leaseTtl, zone, runDate, schemaVersion,
                    offloadMode, singleTable, tieringSet, apply, dryRun, approve);
        }

        // P4-274/283: a trailing flag (e.g. `run --bootstrap`) threw
        // ArrayIndexOutOfBoundsException → FATAL exit 1. Fail as a usage
        // error (exit 4) with the flag named.
        private static String nextArg(String[] args, int i, String flag) {
            if (i >= args.length) {
                throw new IllegalArgumentException(flag + " requires a value");
            }
            return args[i];
        }

        private static Duration parseEnvTtl(String key, Duration fallback) {
            String raw = System.getenv(key);
            if (raw == null || raw.isBlank()) {
                return fallback;
            }
            return EodRetentionPolicy.parseTtl(raw);
        }
    }
}
