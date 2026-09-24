package com.trading.common.schema.fluss;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

/**
 * Waits until the cluster can place a fresh LOG replica — the fixture-readiness gate for LOG tables.
 *
 * <p>WHY THIS EXISTS (measured 2026-09-22 on the 1.0.0 live drill). {@code createTable()} returns in
 * 11-95 ms, but that reports <i>metadata written</i>, not <i>writable</i>: the table's first write
 * then waits for its bucket leader to be elected and discovered by the client. That wait is
 * ~100-200 ms on a settled cluster but 1-13 s while other tables are being torn down — measured on
 * a freshly created 1-bucket LOG table: 13048 ms, 12603 ms, 5169 ms. A test that budgets 20 s for
 * its first append therefore fails whenever a spike exceeds it, which is exactly how
 * {@code compatFluss002BytesRoundTrip} and {@code compatFluss003LogKvChangelog} died (20.02 s
 * TimeoutException with ZERO server-side log lines — a batch with no known leader is refreshed and
 * slept on rather than sent, so nothing reaches the server to be logged).
 *
 * <p>The KV half of this problem was already solved by {@code awaitWritable} in
 * {@code CompatFlussIntegrationTest} (CHG-212, CHG-213), which probes with a lookup of a key that
 * cannot exist. A LOG table has no primary key to look up, so that helper explicitly SKIPS it — and
 * LOG fixtures were the entire failure surface. This class supplies the missing LOG half.
 *
 * <p>The probe must be a <b>write</b>: a read-only log scan cannot detect the condition, because
 * {@code LogFetcher} skips a bucket whose leader is missing and logs at debug
 * ({@code LogFetcher.java:585-590}), so {@code poll} returns empty without failing. It must also not
 * touch the table under test, whose tests assert on row counts — so it writes to a throwaway
 * 1-bucket canary, the same primitive {@code DdlApplyTool.drainScratchTeardown} already relies on.
 *
 * <p>It lives in {@code src/main} rather than {@code src/test} on purpose (2026-09-25): the drill
 * classes that need it run in three modules (common, execution-gateway, compute) and no module
 * declares common's test-jar, so a test-scope helper was invisible to the gateway drills that
 * failed to compile - the same reason {@link WriteAwait} sits here. It imports nothing
 * test-scoped, so shipping it costs the jar nothing but the class file.
 */
public final class FlussPlacementAwait {

    private FlussPlacementAwait() {}

    /** One bucket, no primary key: the cheapest table that still needs a replica placement. */
    private static final TableDescriptor CANARY =
            TableDescriptor.builder()
                    .schema(Schema.newBuilder().column("probe", DataTypes.STRING()).build())
                    .distributedBy(1, "probe")
                    .build();

    private static final long PROBE_INTERVAL_MS = 1000L;

    /** Per-attempt bound on the probe append. Below the caller's own budget so it can retry. */
    private static final long PROBE_APPEND_TIMEOUT_S = 15L;

    /** Per-call bound on the readiness lookup, so one stalled RPC cannot swallow the budget. */
    private static final long SERVING_PROBE_TIMEOUT_S = 10L;

    private static final long SERVING_PROBE_INTERVAL_MS = 250L;

    /**
     * Blocks until a fresh 1-bucket LOG table can be written, or fails loudly.
     *
     * @param connection live connection, used to write the canary
     * @param admin live admin, used to create the canary
     * @param what the fixture this wait protects (log line and failure message)
     * @param budget wall-clock bound; exceeding it throws here, naming the real cause, rather than
     *     letting the caller fail later with an unrelated write timeout
     */
    public static void awaitPlacement(
            Connection connection, Admin admin, String what, Duration budget) throws Exception {
        dropAbandoned(admin);
        long deadline = System.currentTimeMillis() + budget.toMillis();
        Exception last = null;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            // A fresh canary per attempt: a canary whose own write timed out may still hold a
            // pending batch, and dropping a table under a pending batch makes the client Sender
            // busy-loop on metadata for the whole cluster (see WriteAwait and DdlSmokeTwinSweepTest).
            TablePath path =
                    TablePath.of(
                            "default", "await_canary_" + Long.toHexString(System.nanoTime()));
            boolean created = false;
            try {
                admin.createTable(path, CANARY, false).get(30, TimeUnit.SECONDS);
                created = true;
                AppendWriter writer = connection.getTable(path).newAppend().createWriter();
                writer.append(GenericRow.of(BinaryString.fromString("probe")))
                        .get(PROBE_APPEND_TIMEOUT_S, TimeUnit.SECONDS);
                // The append resolved, so this canary holds no pending batch and is safe to drop.
                dropQuietly(admin, path);
                // Placement has resumed, so earlier attempts' canaries now have leaders and their
                // batches have resolved — this is the moment they can be dropped without the
                // Sender busy-loop.
                dropAbandoned(admin);
                System.out.printf("[await] %s placement ready after %d attempt(s)%n", what, attempt);
                return;
            } catch (Exception e) {
                last = e;
                if (created) {
                    // Deliberately NOT dropped here: see ABANDONED. Dropping now would make the
                    // client Sender busy-loop on metadata for the whole cluster.
                    ABANDONED.add(path);
                }
                Thread.sleep(PROBE_INTERVAL_MS);
            }
        }
        throw new IllegalStateException(
                what
                        + " placement not ready within "
                        + budget.toMillis()
                        + "ms ("
                        + attempt
                        + " attempt(s)); last failure: "
                        + last,
                last);
    }

    /**
     * Blocks until every bucket of {@code path} is served by a leader, or fails loudly naming it.
     *
     * <p>The Admin-only sibling of {@link #awaitPlacement}: the same condition, but proved on the
     * table under test instead of on a throwaway canary, with no extra table and no Connection. That
     * matters twice over. It fits a fixture whose helper holds only an {@code Admin}; and every table
     * a drill creates and leaves behind becomes one more stale leader-registration retry loop on the
     * coordinator (measured 2026-09-25: 183 such loops, ~1.2k WARN lines/min, all of them for tables
     * that had already been dropped), so a gate that adds tables feeds the next stall.
     *
     * <p>WHY IT IS NEEDED (gate step 9, 2026-09-24). {@code createTable()} returns in 11-95 ms -
     * metadata written, not writable - and the first use of that table then waits for its bucket
     * leaders: ~100-200 ms when the cluster is settled, but 1-13 s while other tables are being
     * placed or torn down. {@code GateMergeEngineDrillIntegrationTest} budgeted 20 s for that first
     * upsert and died at 20.05 s with zero server-side lines, because a batch with no known leader is
     * refreshed and slept on rather than sent.
     *
     * <p>{@code listOffsets} here is deliberate: unlike {@code Admin#getTableStats}, its {@code
     * LeaderNotAvailableException} surfaces through the async response handler, so it stays the
     * retriable exception Fluss declares it to be instead of being re-wrapped as fatal.
     *
     * @param admin live admin, used to read the table's bucket count and then its offsets
     * @param path the table whose every bucket must be served
     * @param what the fixture this wait protects (log line and failure message)
     * @param budget wall-clock bound; exceeding it throws here, naming the real cause, rather than
     *     letting the caller fail later with an unrelated write timeout
     */
    public static void awaitServing(Admin admin, TablePath path, String what, Duration budget)
            throws Exception {
        int bucketCount =
                admin.getTableInfo(path)
                        .get(SERVING_PROBE_TIMEOUT_S, TimeUnit.SECONDS)
                        .getNumBuckets();
        List<Integer> buckets = new ArrayList<>(bucketCount);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            buckets.add(bucket);
        }
        long startMillis = System.currentTimeMillis();
        long deadline = startMillis + budget.toMillis();
        Throwable lastFailure = null;
        while (true) {
            try {
                admin.listOffsets(path, buckets, new LatestSpec())
                        .all()
                        .get(
                                Math.max(1L, deadline - System.currentTimeMillis()),
                                TimeUnit.MILLISECONDS);
                // A healthy table is served in ~0 s, so a real wait is the placement backlog this
                // gate exists for. System.out, like awaitWritable: no console appender in this JVM.
                if (System.currentTimeMillis() - startMillis > 1_000) {
                    System.out.printf(
                            "[await] %s served after %ds (placement backlog)%n",
                            what, (System.currentTimeMillis() - startMillis) / 1000);
                }
                return;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                // Transient while the cluster places replicas: record it and probe again.
                lastFailure =
                        e instanceof ExecutionException && e.getCause() != null
                                ? e.getCause()
                                : e;
            }
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(
                        "no leader serves every bucket of "
                                + what
                                + " ("
                                + bucketCount
                                + " bucket(s)) within "
                                + budget.toMillis()
                                + "ms: the cluster is not placing replicas, so the fixture cannot be"
                                + " used",
                        lastFailure);
            }
            Thread.sleep(SERVING_PROBE_INTERVAL_MS);
        }
    }

    /**
     * Canaries whose probe write failed. Such a canary may still hold a pending client batch, and
     * dropping a table under a pending batch makes the client Sender busy-loop on metadata, so it
     * is dropped later instead — once the cluster is demonstrably placing replicas again. Without
     * this list every retry leaked one table into the catalog (measured 2026-09-22: 4 LOG fixtures
     * needed 2 attempts each and left exactly 4 {@code await_canary_*} tables, 31/27 vs the
     * manifest).
     */
    private static final List<TablePath> ABANDONED =
            Collections.synchronizedList(new ArrayList<TablePath>());

    /**
     * Drops canaries abandoned by earlier attempts. Safe only once placement has resumed, because
     * that is when their leaders exist and their pending batches have resolved. Called on entry
     * (to clear stragglers from a previous gate) and after a successful probe.
     */
    private static void dropAbandoned(Admin admin) {
        List<TablePath> pending;
        synchronized (ABANDONED) {
            if (ABANDONED.isEmpty()) {
                return;
            }
            pending = new ArrayList<TablePath>(ABANDONED);
            ABANDONED.clear();
        }
        int dropped = 0;
        for (TablePath path : pending) {
            try {
                admin.dropTable(path, false).get(30, TimeUnit.SECONDS);
                dropped++;
            } catch (Exception ignored) {
                // Best effort; a canary left behind is drift, not a failure of the test.
            }
        }
        System.out.printf(
                "[await] dropped %d/%d abandoned canary table(s)%n", dropped, pending.size());
    }

    private static void dropQuietly(Admin admin, TablePath path) {
        try {
            admin.dropTable(path, false).get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // Best effort only; the canary is a scratch table and never asserted on.
        }
    }
}
