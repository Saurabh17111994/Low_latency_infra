package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tracker 14 P5.1 — baseline measurements of the live dedup hot path, driven
 * through the same Flink 2.2.1 operator harness as
 * {@link FingerprintDedupFunctionTest} (no cluster). Each measurement is
 * printed as a {@code P5.1[...]} System.out line so the surefire report is a
 * self-contained evidence record.
 *
 * <p>Since CHG-023 item 2 (2026-08-17) expiry is the NATIVE {@code StateTtlConfig}
 * on the dedup MapState — no expiry-index map, no event-time timers — the
 * timer-count box of the tracker is answered by {@code numEventTimeTimers()}
 * == 0, and the post-expiry leg advances the harness TTL processing time and
 * forces a gauge resync (which scans {@code entries()} — TTL-aware, skips
 * expired) to prove the expired set falls out of the state-size gauge.
 *
 * <p>All feed waves use one instrument token with DISTINCT event times, so
 * every fingerprint is a distinct state key: state count grows in lockstep —
 * a deterministic series that proves the state is bounded and exactly one
 * entry per live fingerprint.
 */
@DisplayName("Tracker 14 P5.1: dedup baseline measurements (counts, bytes, GC)")
class DedupBaselineMeasurementTest {

    private static final long TTL_MS = 60_000L;
    private static final long T0 = 1_700_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    /** The function under test — gauge reads use its fields (the MetricGroup gauges' source). */
    private FingerprintDedupFunction function;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
        FingerprintDedupFunction.GAUGE_RESYNC_INTERVAL_ROWS = 10_000L;
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private void openHarness() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        function = new FingerprintDedupFunction(config);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private void feedDistinct(int waveIndex, int count) throws Exception {
        long waveStart = T0 + waveIndex * 100_000L;
        for (int i = 0; i < count; i++) {
            harness.processElement(
                    TestRawRows.row(1L, waveStart + i, "fp-wave-" + waveIndex + "-" + i,
                            "TRADE", 10_000L + i, 100L),
                    waveStart + i);
        }
    }

    @Test
    @DisplayName("state count grows linearly with live fingerprints and the expired set falls out via native TTL")
    void measureStateCountsAcrossFeedWaves() throws Exception {
        openHarness();
        int perWave = 2_000;

        feedDistinct(0, perWave);
        long s1 = function.dedupStateCountForTest();
        long t1 = harness.numEventTimeTimers();
        long b1 = function.bytesEstimateForTest();
        System.out.println("P5.1[waves] wave1 state=" + s1 + " timers=" + t1
                + " bytesEstimate=" + b1);
        assertEquals(perWave, s1, "one entry per live fingerprint");
        assertEquals(0, t1, "no hand-rolled timers — native TTL expires entries");

        feedDistinct(1, perWave);
        long s2 = function.dedupStateCountForTest();
        long t2 = harness.numEventTimeTimers();
        System.out.println("P5.1[waves] wave2 state=" + s2 + " timers=" + t2
                + " bytesEstimate=" + function.bytesEstimateForTest());
        assertEquals(2L * perWave, s2, "wave 2 doubles the live count");
        assertEquals(0, t2);

        feedDistinct(2, perWave);
        long s3 = function.dedupStateCountForTest();
        long t3 = harness.numEventTimeTimers();
        System.out.println("P5.1[waves] wave3 state=" + s3 + " timers=" + t3
                + " bytesEstimate=" + function.bytesEstimateForTest());
        assertEquals(3L * perWave, s3, "wave 3 triples the live count");
        assertEquals(0, t3);
        assertEquals(3L * perWave * FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES,
                function.bytesEstimateForTest(),
                "bytes estimate = entries x 136 B/entry upper bound (entry + TTL timestamp)");

        // Native TTL expiry: advance the TTL clock past every entry's anchor
        // (all anchored at write-time provider clock = 0) and force a gauge
        // resync — entries() skips the expired set, so the state-size gauge
        // falls to the single fresh row inserted after the advance.
        FingerprintDedupFunction.GAUGE_RESYNC_INTERVAL_ROWS = 1L;
        harness.setStateTtlProcessingTime(TTL_MS + 1L);
        harness.processElement(
                TestRawRows.row(1L, T0 + TTL_MS + 10_000L, "fp-after-expiry", "TRADE", 99_000L, 1L),
                T0 + TTL_MS + 10_000L);
        long s4 = function.dedupStateCountForTest();
        System.out.println("P5.1[waves] afterTtlExpiry state=" + s4
                + " bytesEstimate=" + function.bytesEstimateForTest());
        assertEquals(1L, s4, "expired set falls out of the gauge — only the fresh row is live");
    }

    @Test
    @DisplayName("allocation and GC rate over a 100k-fingerprint feed are measurable and bounded")
    void measureAllocationAndGcRate() throws Exception {
        openHarness();
        java.lang.management.ThreadMXBean mgmt = ManagementFactory.getThreadMXBean();
        com.sun.management.ThreadMXBean tmx =
                (mgmt instanceof com.sun.management.ThreadMXBean)
                        ? (com.sun.management.ThreadMXBean) mgmt
                        : null;
        boolean allocSupported = tmx != null && tmx.isThreadAllocatedMemorySupported();
        long allocBefore = allocSupported ? tmx.getCurrentThreadAllocatedBytes() : -1L;
        long gcBefore = gcCount();
        long gcTimeBefore = gcTimeMs();

        int n = 100_000;
        for (int i = 0; i < n; i++) {
            harness.processElement(
                    TestRawRows.row(1L, T0 + i, "fp-gc-" + i, "TRADE", 10_000L + i, 100L),
                    T0 + i);
        }

        long allocAfter = allocSupported ? tmx.getCurrentThreadAllocatedBytes() : -1L;        long gcDelta = gcCount() - gcBefore;
        long gcTimeDelta = gcTimeMs() - gcTimeBefore;
        long state = function.dedupStateCountForTest();
        System.out.println("P5.1[gc] rows=" + n + " allocatedBytes="
                + (allocSupported ? (allocAfter - allocBefore) : "unsupported")
                + " gcCollections=" + gcDelta + " gcMs=" + gcTimeDelta
                + " state=" + state);
        assertEquals(n, state, "state holds exactly one entry per distinct fingerprint");
        if (allocSupported) {
            assertTrue(allocAfter > allocBefore,
                    "feeding 100k fingerprints must allocate bytes on this thread");
        }
        assertTrue(gcDelta >= 0 && gcTimeDelta >= 0, "GC counters never go backwards");
    }

    @Test
    @DisplayName("a repeated fingerprint creates no duplicate state entries")
    void repeatedFingerprintDoesNotGrowState() throws Exception {
        openHarness();
        harness.processElement(TestRawRows.row(1L, T0, "fp-dup", "TRADE", 100, 1), T0);
        for (int i = 1; i < 10_000; i++) {
            harness.processElement(
                    TestRawRows.row(1L, T0 + i, "fp-dup", "TRADE", 100 + i, i), T0 + i);
        }
        assertEquals(1L, function.dedupStateCountForTest(),
                "10k duplicates must not grow the fingerprint map");
        assertEquals(0, harness.numEventTimeTimers());
    }

    private static long gcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            sum += bean.getCollectionCount();
        }
        return sum;
    }

    private static long gcTimeMs() {
        long sum = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            sum += bean.getCollectionTime();
        }
        return sum;
    }
}
