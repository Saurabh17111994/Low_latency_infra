package com.trading.common.schema.eod;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.CloseableIterator;

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
 * eod-controller extend    retention-extension recipe; --apply performs the
 *                          shadow-table rewrite drill
 * eod-controller reconcile re-verify COMMITTED/VERIFYING days (crash-resume)
 * eod-controller reset     FAILED_MANUAL -> PENDING (requires --approve)
 * }</pre>
 *
 * <p>Exit codes: 0 OK (or extension applied / days verified), 1 failure,
 * 2 extension required (or retryable days), 3 pending work (status),
 * 4 usage/approval-required, 5 lease held by another controller.
 *
 * <p>The retention-extension mechanism honors the Fluss 0.9.1 boundary:
 * {@code table.log.ttl} is create-time only (verified 2026-08-13), so an
 * extension is a controlled rewrite — {@code extend --apply} creates a shadow
 * table with the extended create-time TTL ({@code name__eod_ext_<date>}),
 * copies the current rows, and verifies count parity. The swap (pointing
 * consumers at the shadow, or drop+recreate under the same name) stays an
 * operator-approved step — the drill measures the copy before production
 * assumptions harden.
 */
public final class EodControllerTool {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    /** Parallel copy workers (CHG-099): capped at the bucket count. */
    static final int COPY_THREADS = 16;
    /** Async upserts in flight per worker before awaiting acks. */
    static final int COPY_WRITE_BATCH = 500;

    /**
     * EOD-eligible live tables — now 7d TTL (T8 G1/G4 hardened 2026-08-22 — was
     * ten 2d-TTL tables 2026-08-13). The documented default scope remains ten
     * tables for backward compat, but the storage contract is 7 calendar days
     * + block-delete-unverified guard: Fluss TTL delete is BLOCKED until the
     * iceberg manifest is VERIFIED; otherwise the controller extends retention
     * via the shadow-rewrite drill and fires a critical alert.
     *
     * <p>{@code candle_closed} (DDL 33) replaced the retired
     * {@code feature_candles_15s} in the default scope (multi-timeframe
     * cutover 2026-09-05) — same 7d TTL and same EOD Iceberg offload contract.
     */
    static final List<String> DEFAULT_TABLES = List.of(
            "raw_table_1", "candle_closed", "ingestion_quarantine",
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
                case "extend" -> extend(opts, connection, admin, liveTtlsFor.apply("extend"),
                        zone, now);
                case "reconcile" -> reconcile(opts, liveTtlsFor.apply("reconcile"), now);
                case "reset" -> reset(opts, now);
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
                System.out.println("eod-controller: status " + p.table()
                        + " earliestUnverified=" + p.plan().earliestUnverifiedDate()
                        + " protectedBound=" + p.plan().protectedExpiryBound()
                        + " margin=" + p.plan().marginMs() + "ms"
                        + " requiresExtension=" + p.plan().requiresExtension()
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
            // (Fri offload unverified → Fri-Sun must survive past 2d, now 7d)
            // is observable. The EOD controller's extend path then holds the data.
            if (s == EodController.Status.EXTENSION_REQUIRED) {
                for (EodController.TablePlan p : plans) {
                    if (!p.noDays() && p.plan().requiresExtension()) {
                        String when = p.plan().earliestUnverifiedDate() != null
                                ? p.plan().earliestUnverifiedDate().toString() : "floor";
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

    private static int extend(Options opts, Connection connection, Admin admin,
            Map<String, Duration> liveTtls, ZoneId zone, Instant now) throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            // P4-121/122: extend --apply mutates state (shadow create + bulk
            // copy) — it must hold the single-writer lease like run/reconcile,
            // or a concurrent run races the shadow check-then-create.
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
                String shadow = p.table() + "__eod_ext_"
                        + now.atZone(zone).toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE);
                // Block-guard: this table's delete is currently BLOCKED — the
                // protected bound is unverified, so the shadow rewrite (extended
                // 7d + extra) must succeed before the old table is allowed to
                // expire. The alert below makes the hold observable.
                System.err.println("eod-controller: ALERT CRITICAL — block-guard extend for "
                        + p.table() + " liveTtl=" + live + " -> newTtl=" + newTtl
                        + " — Fluss delete BLOCKED until VERIFIED (iceberg manifest)");
                System.out.println("eod-controller: extend " + p.table()
                        + " liveTtl=" + live + " newTtl=" + newTtl
                        + " shadow=" + shadow + " margin=" + p.plan().marginMs() + "ms");
                if (opts.apply && !opts.dryRun) {
                    if (!performRewrite(connection, admin, opts.database, p.table(),
                            shadow, newTtl, TIMEOUT.toMillis())) {
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

    // ── extend rewrite drill (live) ───────────────────────────────────────

    /**
     * The controlled-rewrite drill: create the shadow table with the extended
     * create-time TTL (same schema/PK/distribution, lake disabled like the
     * live dev tables), copy every current row via the raw client, and verify
     * count parity. The swap stays an operator step — 0.9.1 has no rename.
     * Returns true when the shadow was created and the copy reconciles.
     */
    static boolean performRewrite(Connection connection, Admin admin, String database,
            String table, String shadow, Duration newTtl, long timeoutMs) throws Exception {
        TablePath livePath = TablePath.of(database, table);
        TablePath shadowPath = TablePath.of(database, shadow);
        TableInfo live = admin.getTableInfo(livePath)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        try {
            admin.getTableInfo(shadowPath).get(timeoutMs, TimeUnit.MILLISECONDS);
            System.err.println("eod-controller: shadow " + shadow + " already exists — "
                    + "drop it (or rename) before re-running the drill");
            return false;
        } catch (ExecutionException e) {
            // P4-118/123: proceed ONLY on not-exists. A bare catch(Exception)
            // mistook timeouts/auth failures for "absent" and built the copy
            // on a lie.
            if (!(e.getCause() instanceof org.apache.fluss.exception.TableNotExistException)) {
                throw new RuntimeException("eod-controller: shadow existence check failed for "
                        + shadow, e);
            }
            // shadow absent — proceed
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }

        Schema schema = live.getSchema();
        TableDescriptor.Builder tb = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(live.getNumBuckets(),
                        live.getBucketKeys().toArray(String[]::new));
        // Keep the create-time options that matter for a rewrite: the extended
        // TTL is the point; kv.format-version rides along; lake stays disabled
        // (create-only — same dev deviation as the live tables).
        String kvFormat = live.getProperties().toMap().get("table.kv.format-version");
        if (kvFormat != null) {
            tb.property("table.kv.format-version", kvFormat);
        }
        tb.property("table.log.ttl", ttlOption(newTtl));
        admin.createTable(shadowPath, tb.build(), false)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        System.out.println("eod-controller: created shadow " + shadow + " ttl=" + newTtl);

        Table liveTable = connection.getTable(livePath);
        Table shadowTable = connection.getTable(shadowPath);
        long copied = copyBucketsParallel(liveTable, shadowTable, live, schema, timeoutMs);
        long liveCount = scanCount(liveTable, live, timeoutMs);
        long shadowCount = scanCount(shadowTable,
                admin.getTableInfo(shadowPath).get(timeoutMs, TimeUnit.MILLISECONDS), timeoutMs);
        System.out.println("eod-controller: shadow copy rows=" + copied
                + " liveCount=" + liveCount + " shadowCount=" + shadowCount);
        if (liveCount != shadowCount || copied != liveCount) {
            System.err.println("eod-controller: shadow copy does not reconcile "
                    + "(live=" + liveCount + " shadow=" + shadowCount + " copied=" + copied + ")");
            return false;
        }
        return true;
    }

    /**
     * Parallel bucket copy (CHG-099): one worker per bucket (capped at
     * {@link #COPY_THREADS}), each with its own {@code UpsertWriter} and
     * batched async acks ({@link #COPY_WRITE_BATCH} futures per await). The
     * old single-threaded path awaited every row's ack before the next —
     * one RTT per row, ~10 rows/s (measured 2026-08-24 on a 385k-row table).
     * Batching + 16-way parallelism targets thousands of rows/s. Correctness
     * contract unchanged: per-bucket full scan + PK upserts (idempotent) +
     * final flush, then the caller's count parity. (Note: a Flink bounded
     * source is NOT viable on lake-disabled tables in fluss 0.9.1-incubating —
     * the enumerator throws "Batch only supports when table option
     * 'table.datalake.enabled' is set to true" — so the copy stays on the
     * raw client, where the batch scanner is supported.)
     */
    static long copyBucketsParallel(Table liveTable, Table shadowTable, TableInfo info,
            Schema schema, long timeoutMs) throws Exception {
        int buckets = info.getNumBuckets();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(buckets, COPY_THREADS));
        List<Future<Long>> futures = new ArrayList<>();
        for (int b = 0; b < buckets; b++) {
            final int bucketId = b;
            futures.add(pool.submit(
                    () -> copyBucket(liveTable, shadowTable, info, schema, bucketId, timeoutMs)));
        }
        long copied = 0;
        // P4-119/124: one shared deadline across buckets (not timeout*buckets
        // stacked), cancel on failure, shutdownNow + await — the old
        // sequential f.get(timeout*4) could block buckets*timeout (hours) and
        // the bare shutdown() leaked workers on failure.
        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMs * 4);
        try {
            for (Future<Long> f : futures) {
                long remaining = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                copied += f.get(Math.max(1, remaining), TimeUnit.MILLISECONDS);
            }
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            for (Future<Long> f : futures) {
                f.cancel(true);
            }
            Throwable cause = (e instanceof ExecutionException && e.getCause() != null)
                    ? e.getCause() : e;
            throw new RuntimeException("eod-controller: bucket copy failed: " + cause, cause);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        }
        return copied;
    }

    private static long copyBucket(Table liveTable, Table shadowTable, TableInfo info,
            Schema schema, int bucketId, long timeoutMs) throws Exception {
        TableBucket bucket = new TableBucket(info.getTableId(), bucketId);
        // P4-279/282 note: the writer is flush()-only in Fluss 0.9.1 (no AutoCloseable
        // — verified by decompile in Group F), so there is nothing to close.
        // 2026-09-11: the flush-in-finally that used to be here is removed. Its old
        // justification ("cannot mask the copy exception because awaitBatch already
        // propagated any ack failure") held only on the success path — and there the
        // flush was a no-op, since awaitBatch had already awaited every future. On the
        // timeout path it was the opposite of harmless: the futures are still in the
        // accumulator, so flush() awaited them with no timeout and the TimeoutException
        // never propagated (see FlussWriteProfiles for why that wait is unbounded).
        // Written-through is unchanged: the last awaitBatch below still runs, and it is
        // what makes every row durable before this method returns.
        UpsertWriter writer = shadowTable.newUpsert().createWriter();
        long copied = 0;
        List<CompletableFuture<?>> batch = new ArrayList<>();
        try (BatchScanner scanner = liveTable.newScan()
                     .limit(Integer.MAX_VALUE)
                     .createBatchScanner(bucket)) {
            // pollBatch returns one time-bounded batch; repeat until empty
            // (a null/empty batch = scan of this bucket exhausted) —
            // measured 2026-08-24: a single pollBatch under 16-way
            // write contention yielded only ~6.8% of the rows (uniform
            // sample), while the drain loop reads the full bucket.
            while (true) {
                boolean any = false;
                try (CloseableIterator<InternalRow> it =
                        scanner.pollBatch(Duration.ofMillis(250))) {
                    while (it != null && it.hasNext()) {
                        InternalRow row = it.next();
                        batch.add(writer.upsert(GenericRow.of(toValues(row, schema))));
                        if (batch.size() == COPY_WRITE_BATCH) {
                            awaitBatch(batch, timeoutMs);
                        }
                        copied++;
                        any = true;
                    }
                }
                if (!any) {
                    break;
                }
            }
        }
        awaitBatch(batch, timeoutMs);
        System.out.println("eod-controller: copy bucket=" + bucketId + " rows=" + copied);
        return copied;
    }

    private static void awaitBatch(List<CompletableFuture<?>> batch, long timeoutMs)
            throws Exception {
        // P4-120/125: one timeout for the whole batch (allOf), not
        // batch-size × timeout sequential — a slow ack stalled everything.
        try {
            CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            batch.clear();
        }
    }

    /** Row → Object[] in schema order (raw-client upsert values). */
    private static Object[] toValues(InternalRow row, Schema schema) {
        List<Schema.Column> columns = schema.getColumns();
        Object[] out = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            if (row.isNullAt(i)) {
                out[i] = null;
                continue;
            }
            DataType type = columns.get(i).getDataType();
                // P4-117/126: map the full Fluss TypeRoot set — the old 7-type
            // switch threw mid-copy AFTER the shadow was created (partial
            // shadow + failed drill) on any TIMESTAMP/DATE/DECIMAL table.
            // Complex nests (ARRAY/MAP/ROW) ride a FieldGetter passthrough.
            org.apache.fluss.row.InternalRow.FieldGetter nested =
                    org.apache.fluss.row.InternalRow.createFieldGetter(type, i);
            out[i] = switch (type.getTypeRoot()) {
                case CHAR, STRING -> row.getString(i);
                case BOOLEAN -> row.getBoolean(i);
                case BINARY, BYTES -> row.getBytes(i);
                case DECIMAL -> row.getDecimal(i,
                        ((org.apache.fluss.types.DecimalType) type).getPrecision(),
                        ((org.apache.fluss.types.DecimalType) type).getScale());
                case TINYINT -> row.getByte(i);
                case SMALLINT -> row.getShort(i);
                case INTEGER -> row.getInt(i);
                case BIGINT -> row.getLong(i);
                case FLOAT -> row.getFloat(i);
                case DOUBLE -> row.getDouble(i);
                case DATE -> row.getInt(i);
                case TIME_WITHOUT_TIME_ZONE -> row.getInt(i);
                case TIMESTAMP_WITHOUT_TIME_ZONE -> row.getTimestampNtz(i, 6);
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> row.getTimestampLtz(i, 6);
                default -> nested.getFieldOrNull(row);
            };
        }
        return out;
    }

    private static long scanCount(Table table, TableInfo info, long timeoutMs) throws Exception {
        // P4-116/127: drain like copyBucket — one pollBatch is one batch, not
        // the bucket. The old single-poll undercounted (false EXTEND_FAILED
        // parity failure) and NPEd on it.hasNext() when pollBatch returned
        // null for an empty/exhausted bucket.
        // P4-278 note: copy-then-count has no snapshot isolation — live writes
        // between copy and count flake exact parity on hot tables. The drill
        // requires a quiesced source (hold the lease / pause writers first).
        long count = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket bucket = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(bucket)) {
                while (true) {
                    boolean any = false;
                    try (CloseableIterator<InternalRow> it =
                            scanner.pollBatch(Duration.ofMillis(250))) {
                        while (it != null && it.hasNext()) {
                            it.next();
                            count++;
                            any = true;
                        }
                    }
                    if (!any) {
                        break;
                    }
                }
            }
        }
        return count;
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
                usage: eod-controller <status|run|extend|reconcile|reset> [options]
                  --bootstrap <addr>   (env FLUSS_BOOTSTRAP, default localhost:9123)
                  --database <db>      (env FLUSS_DATABASE, default default)
                  --state-table <name> (env EOD_STATE_TABLE, default eod_offload_state)
                  --tables <t1,t2>     (env EOD_TABLES — EOD-eligible tables)
                  --ttl <ttl>          (env EOD_TTL, default 7d — live-TTL fallback; T8 hardened was 2d)
                  --safety-floor <ttl> (env EOD_SAFETY_FLOOR, default 7d)
                  --extension <ttl>    (env EOD_EXTENSION, default 30d)
                  --lease-ttl <ttl>    (env EOD_LEASE_TTL, default 30m)
                  --zone <zone>        (env EOD_ZONE, default Asia/Kolkata)
                  --run-date <date>    (run: trading date, default today in --zone)
                  --schema-version <v> (env EOD_SCHEMA_VERSION, default 1)
                  --offload none|mock|lake  (env EOD_OFFLOAD, default none — fail-closed)
                  --table <name>       (reset: single table scope)
                  --apply              (extend: perform the shadow rewrite drill)
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
                   boolean apply, boolean dryRun, boolean approve) {

        static Options parse(String[] args) {
            if (args.length == 0) {
                throw new IllegalArgumentException("subcommand required "
                        + "(status|run|extend|reconcile|reset)");
            }
            String subcommand = args[0];
            String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
            String database = System.getenv().getOrDefault("FLUSS_DATABASE", "default");
            String stateTable = System.getenv().getOrDefault("EOD_STATE_TABLE",
                    "eod_offload_state");
            String tablesRaw = System.getenv().getOrDefault("EOD_TABLES", null);
            Duration ttlDefault = parseEnvTtl("EOD_TTL", Duration.ofDays(7));
            Duration safetyFloor = parseEnvTtl("EOD_SAFETY_FLOOR", Duration.ofDays(7));
            Duration extension = parseEnvTtl("EOD_EXTENSION", Duration.ofDays(30));
            Duration leaseTtl = parseEnvTtl("EOD_LEASE_TTL", Duration.ofMinutes(30));
            String zone = System.getenv().getOrDefault("EOD_ZONE", "Asia/Kolkata");
            String runDate = null;
            String schemaVersion = System.getenv().getOrDefault("EOD_SCHEMA_VERSION", "1");
            String offloadMode = System.getenv().getOrDefault("EOD_OFFLOAD", "none");
            String singleTable = null;
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
                    offloadMode, singleTable, apply, dryRun, approve);
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
