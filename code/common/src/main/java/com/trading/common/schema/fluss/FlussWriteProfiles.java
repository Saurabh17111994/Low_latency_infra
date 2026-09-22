package com.trading.common.schema.fluss;

import org.apache.fluss.config.Configuration;

// Version note (2026-09-23): the 0.9.1 references in this file record the pre-1.0.0 baseline this code was written against, not a constraint of the running Fluss 1.0.0 — re-check them (DEC-052).
/**
 * D1 — role-split write linger for Fluss connections.
 *
 * <p>{@code TableWriter.flush()} is <b>not</b> a per-writer flush and cannot bound the latency of the
 * record being written: it delegates to {@code WriterClient.flush()} &rarr;
 * {@code accumulator.beginFlush()} (a counter bump, RecordAccumulator.java:358-360) +
 * {@code awaitFlushCompletion()} (RecordAccumulator.java:363-373), which awaits only requests already
 * <i>sent</i> ({@code incomplete.requestResults()}). The pre-D1 call sites prove the point — they
 * flushed in a {@code finally} <i>after</i> {@code upsert(row).get()} had already returned the ack, so
 * the flush could not have shortened that write by construction. It also cannot wake the sender: the
 * append path logs "Waking up the sender ..." and then does nothing —
 * {@code // TODO add the wakeup logic refer to Kafka} (WriterClient.java:208-214).
 *
 * <p>What actually bounds per-write latency is this linger, because nothing else wakes the Sender:
 * {@code Sender.sendWriteData()} sleeps {@code nextReadyCheckDelayMs} whenever no bucket is ready
 * (Sender.java:232-238), and that delay is derived from the batch timeout
 * ({@code RecordAccumulator.ready()} / {@code batchReady()}). A record therefore waits up to the
 * linger before the sender even looks at it. Pre-D1 these stores configured no linger at all, so each
 * write waited on Fluss's 100ms default ({@code CLIENT_WRITER_BATCH_TIMEOUT}, ConfigOptions.java:971-974)
 * — that default, not the flush, was the latency. Setting it to 1ms is the D1 win (~99ms per write).
 *
 * <p><b>Do not mistake these values for a batching lever on these paths.</b> A batch is sent early
 * only when it is full or has waited out the timeout (RecordAccumulator.java:539-540, with
 * {@code full = dequeSize > 1 || batch.isClosed()} at :498), and every production writer into these
 * stores is serialized <i>and awaits each ack</i>, so a batch never fills and the linger is
 * latency-additive. That serialization is not incidental: the gate/attempt stores are
 * {@code synchronized} (FlussGateStateStore.java:155+), intent-dedup commits come from the single
 * {@code execution-intent-reader} thread (ExecutionGatewayMain.java:58-65), and projection/ledger
 * writes run on the HTTP handler, which uses serial dispatch in production ({@code httpExecutor ==
 * null}, GatewayHttpServer.java:47-58) — a pool is passed only by the flood soak test. So the linger is
 * not a batching lever on these paths; it is a latency-vs-idle-CPU trade (see below). Where a producer
 * really does submit concurrently (the Flink sink adapter, or the soak test), the linger does batch and
 * the trade inverts.
 *
 * <p>Why not 0: with a zero timeout the not-ready sleep degrades to {@code Thread.sleep(0)} and the
 * sender's own TODO documents the busy-loop hazard there ("The method sendWriteData is in a busy loop.
 * ... cause the CPU to be occupied", Sender.java:234-238). A non-zero floor is what keeps an idle
 * connection from spinning; the cost is roughly 1/linger wakeups per second per connection. Both roles
 * now sit at 1ms (2026-09-11): the bulk path's former 5ms rested on that idle-CPU cost, and the
 * measurement above removed the batching half of its rationale, leaving a split that no longer paid for
 * ~4ms per write. The idle-CPU cost of 1ms on the bulk connections is real but NOT measured here — it
 * only bites when a connection has nothing to send, which the postback/positions paths rarely are while
 * trading.
 *
 * <p>Measured basis, corrected: THR-PROBE-002 (performance_audit.md findings #1/#6) used a single
 * <b>non-blocking</b> {@code AppendWriter} — batched submissions, 58k-357k rows/s across lingers. Those
 * throughput numbers do NOT transfer to these blocking stores, which never fill a batch; the probe is
 * evidence that the client/tablet path has headroom and that 1ms is not a throughput cliff, not the
 * derivation of these values.
 *
 * <p><b>Measured 2026-09-11 (live cluster, {@code BlockingUpsertLingerProbe}, 200 serialized awaits
 * per point, 0 failures throughout) — per-write latency is the linger plus ~2.5ms of fixed overhead:</b>
 * p50 2.29ms @ linger 1, 6.47ms @ 5, 22.37ms @ 20, 102.67ms @ 100. That is the direct confirmation of
 * the argument above on the awaited path: the linger is purely latency-additive here, so the 1ms money
 * path saves ~98ms per write versus Fluss's 100ms default, and the same measurement removed the
 * rationale for the former 5ms bulk split — it cost ~4ms per write and bought no batching at all for
 * these serialized callers, so both roles now use 1ms. (The same probe found 0 is fine for
 * single-write latency — p50 0.78ms — but the non-blocking sweep showed 0 is markedly worse for
 * throughput, 49k vs 148k rows/s at 1ms, consistent with the sender busy-loop noted above; keep a
 * non-zero floor.)
 *
 * <p><b>Why no per-record {@code flush()}</b> — the canonical rationale the stores point at: a pre-ack
 * flush is measurably faster because it marks the batch sendable without waiting out the linger
 * ({@code RecordAccumulator.batchReady()}, :540), but it is an <i>unbounded</i> wait.
 * {@code awaitFlushCompletion()} blocks on every incomplete batch on the connection and
 * {@code WriteBatch.RequestFuture.await()} is a bare {@code latch.await()} with no timeout
 * (WriteBatch.java:309-311). A batch enters that collection at <i>append</i> time
 * (RecordAccumulator.java:596), so a flush right after {@code upsert()} awaits the caller's own write.
 * There is no bound on how long that takes: Fluss 0.9.1 has <b>no delivery timeout</b>
 * (RecordAccumulator.java:114-116, "TODO add deliveryTimeoutMs to report success or failure on record
 * delivery") and {@code client.writer.retries} defaults to {@code Integer.MAX_VALUE}
 * (ConfigOptions.java:1038-1041), while {@code client.request-timeout} bounds only a request already on
 * the wire (Sender.java:395,402 — the timeout is unused for a batch with no known leader, which the
 * sender answers by refreshing metadata and sleeping, Sender.java:214-238). A batch that never becomes
 * sendable therefore hangs the await <i>indefinitely</i>, not for 30s. Every store here awaits a
 * <i>bounded</i> {@code get(timeout)} to fail closed fast, and a flush placed before that get would move
 * the worst case outside that budget; a flush in a {@code finally} would additionally swallow the
 * timeout instead of propagating it. The flush is also connection-scoped — {@code flushesInProgress}
 * is an accumulator counter (RecordAccumulator.java:358-360, :561) read by every bucket's readiness
 * check — so on a shared connection it forces and awaits sibling tables' batches too. It is a sync
 * point, not a per-record primitive.
 *
 * <p>The probe also <b>disproves</b> the P4-134 note in {@code FlussEodStateStore} that "get() before
 * flush() risks hanging to timeout": 1000 no-flush awaited writes across every linger, including 100ms,
 * produced zero timeouts. A pre-ack {@code flush()} is a latency <i>optimization</i> (~1-2ms, because
 * {@code flushInProgress()} makes the batch sendable without waiting for the next poll), never a
 * correctness requirement — so removing the per-record flushes did not trade durability for latency,
 * contrary to what the pre-D1 {@code finally { flush(); }} implied. That probe result is why
 * {@code FlussEodStateStore} no longer flushes either: its flush was not protecting correctness, it was
 * working around a bare {@code Configuration} that set no linger, and it was replaced by a write profile
 * (2026-09-11) so that both the latency and the failure mode match every other store. None of the stores
 * documented here flushes on its write path; the bounded {@code get(timeout)} is the contract. (Some
 * one-shot tooling elsewhere — {@code DdlApplyTool}, {@code CompositeKeyMatrixVerifier},
 * {@code InstrumentManifestWriter} — still flushes, including in {@code finally} slots after a bounded
 * get, which is the same masking shape corrected in {@code FlussEodStateStore}. Those are not on a
 * request path, so they are left as-is rather than silently changed here.)
 *
 * <p><b>Why per-call writers (D2)</b> — the other half of the rationale the stores point at. The
 * {@code UpsertWriter}/{@code AppendWriter}/{@code Lookuper} instances the stores create per call are
 * intentional, not a leak: in Fluss 0.9.1 none of them is {@code Closeable} (verified against the client
 * sources — {@code Lookuper} exposes only {@code lookup()}, {@code UpsertWriter} only
 * {@code upsert()}/{@code delete()}), so there is nothing to close and nothing accumulates. Sharing one
 * across calls would instead be unsafe: Fluss documents {@code Table} as <i>not</i> thread-safe and says
 * caching or pooling of {@code Table} instances is not recommended, so a shared writer would need a lock
 * on the very path it exists to keep fast — serializing every order to save a microsecond-scale
 * constructor. Only {@code Table} and {@code Connection} are {@code AutoCloseable}, and the store that
 * owns the connection closes both.
 *
 * <p>Durability is unaffected: writers await {@code upsert(row).get(timeout)} and Fluss defaults
 * {@code client.writer.acks=all} (ConfigOptions.java:1009-1012), so the future completes only on a
 * durable acknowledgement. Linger bounds <i>when the batch is sent</i>; it never weakens the ack.
 */
public final class FlussWriteProfiles {

    /**
     * Money path — Execution_Gate fence reads/writes, Execution_Attempts lifecycle,
     * safety halts and intent-dedup commits. These gate every order, so the wait
     * before send must be at the measured optimum.
     */
    public static final String MONEY_PATH_LINGER = "1ms";

    /**
     * Bulk path — postback projection, ledger, quarantine, positions, correlation
     * and lifecycle writes. Previously 5ms, on the assumption that a longer
     * linger bought batching on these higher-volume paths. The 2026-09-11
     * serialized probe disproved that: every bulk caller awaits its own ack
     * (FlussProjectionLedgerStore.java:69, FlussProjectionWriter.java:147/151,
     * PostbackQuarantineStore.java:107, FlussPositionsStateStore.java:102,
     * FlussPostbackQuarantineStore.java:93), so a batch never fills and the
     * extra ~4ms was pure added latency per write. Held equal to the money-path
     * value; the name stays separate because the role intent differs, not the
     * number.
     */
    public static final String BULK_PATH_LINGER = "1ms";

    static final String BATCH_TIMEOUT_KEY = "client.writer.batch-timeout";

    private FlussWriteProfiles() {}

    /** Applies the money-path linger to an already-configured connection config. */
    public static Configuration moneyPath(Configuration c) {
        c.setString(BATCH_TIMEOUT_KEY, MONEY_PATH_LINGER);
        return c;
    }

    /** Applies the bulk-path linger to an already-configured connection config. */
    public static Configuration bulkPath(Configuration c) {
        c.setString(BATCH_TIMEOUT_KEY, BULK_PATH_LINGER);
        return c;
    }
}
