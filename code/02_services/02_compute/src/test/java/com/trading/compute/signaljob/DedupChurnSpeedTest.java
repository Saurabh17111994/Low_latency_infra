package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * G-DEDUP-5 guardrail (2026-09-03 redesign): the production heap-window
 * operator holds p99 &lt; 50 µs/record under churn, and behaves identically
 * to the Step-0 {@link HeapWindowDedupPrototype} on the same feed.
 *
 * <p>History: as Step 0 this test split the old RocksDB/TTL operator against
 * the prototype (66x p99, PROCEED decision). The old operator is gone; the
 * production implementation IS the heap window now, so the guardrail targets
 * it directly and the prototype serves as the behavioral oracle.
 *
 * <p>Churn here is volume (waves of distinct fingerprints + full resends per
 * wave). There is no TTL machinery left to expire and no resync scans to
 * force — the processing-time advance between waves is a documented no-op
 * for both implementations.
 */
@DisplayName("G-DEDUP-5: production heap-window p99 guardrail + prototype equivalence")
class DedupChurnSpeedTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final int TOKENS = 3;
    private static final int PER_TOKEN_WAVE = 1_000;
    private static final int WAVES = 3;
    /** Unmeasured JIT warmup on token 1 (distinct fps — counts as firsts). */
    private static final int WARMUP_N = 1_000;
    /** Production bound at the dev max: measures the hot path, not trimming. */
    private static final int IMPL_BOUND = 100_000;
    /** Prototype bound far above the test volume: measures hot path, not trim. */
    private static final int PROTO_BOUND = 1_000_000;
    /** G-DEDUP-5 guardrail: heap hot-path p99 budget per record. */
    private static final long P99_BUDGET_NS = 50_000L;

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", Integer.toString(IMPL_BOUND));
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private record Run(long p50ns, long p99ns, double recPerSec, long passed) {}

    private static long emitted(KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream()
                .filter(o -> o instanceof org.apache.flink.streaming.runtime.streamrecord.StreamRecord)
                .count();
    }

    /** Feeds waves of distinct fingerprints per token; resends each wave (dups). */
    private Run drive(Object fn) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        (org.apache.flink.streaming.api.functions.KeyedProcessFunction<Long, RowData, RowData>) fn,
                        row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG);
        h.open();
        try {
            h.setProcessingTime(T0);
            // Warmup (JIT), unmeasured (but its distinct fps count as firsts).
            for (int i = 0; i < WARMUP_N; i++) {
                h.processElement(
                        TestRawRows.row(1L, T0 + i, "warm-" + i, "TRADE", 100L, 1L), T0 + i);
            }
            h.getOutput().clear();

            int total = WAVES * TOKENS * PER_TOKEN_WAVE * 2; // new + resend per wave
            long[] lat = new long[total];
            int li = 0;
            long passed = 0;
            long tStart = System.nanoTime();
            for (int w = 0; w < WAVES; w++) {
                for (int t = 1; t <= TOKENS; t++) {
                    long base = T0 + w * 1_000_000L;
                    for (int i = 0; i < PER_TOKEN_WAVE; i++) {
                        String fp = "spd-t" + t + "-w" + w + "-" + i;
                        long ts = base + i;
                        long s = System.nanoTime();
                        h.processElement(TestRawRows.row(t, ts, fp, "TRADE", 100L, 1L), ts);
                        lat[li++] = System.nanoTime() - s;
                    }
                    // Resend the same wave: all repeats (within window for both).
                    for (int i = 0; i < PER_TOKEN_WAVE; i++) {
                        String fp = "spd-t" + t + "-w" + w + "-" + i;
                        long ts = base + PER_TOKEN_WAVE + i;
                        long s = System.nanoTime();
                        h.processElement(TestRawRows.row(t, ts, fp, "TRADE", 100L, 1L), ts);
                        lat[li++] = System.nanoTime() - s;
                    }
                }
                // No TTL machinery remains — the advance only spaces waves.
                h.setProcessingTime(T0 + (w + 1) * 1_000_000L);
                passed = emitted(h);
            }
            double secs = (System.nanoTime() - tStart) / 1_000_000_000.0;
            Arrays.sort(lat, 0, li);
            long p50 = lat[li / 2];
            long p99 = lat[(int) (li * 0.99)];
            return new Run(p50, p99, li / secs, passed);
        } finally {
            h.close();
        }
    }

    @Test
    @DisplayName("production impl meets p99 guardrail; identical pass/drop to the prototype oracle")
    void implGuardrailAndPrototypeEquivalence() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        Run impl = drive(new FingerprintDedupFunction(config));
        HeapWindowDedupPrototype proto = new HeapWindowDedupPrototype(PROTO_BOUND);
        Run heap = drive(proto);

        long perWave = (long) TOKENS * PER_TOKEN_WAVE;
        System.out.println("CHURN[impl]  p50ns=" + impl.p50ns() + " p99ns=" + impl.p99ns()
                + " recPerSec=" + (long) impl.recPerSec() + " passed=" + impl.passed());
        System.out.println("CHURN[proto] p50ns=" + heap.p50ns() + " p99ns=" + heap.p99ns()
                + " recPerSec=" + (long) heap.recPerSec() + " passed=" + heap.passed()
                + " firsts=" + proto.firsts + " dups=" + proto.duplicates);

        // Contract on both: every new passes, every resend drops. New waves =
        // WAVES*TOKENS*PER_TOKEN_WAVE passed; resends add nothing.
        assertEquals(perWave * WAVES, impl.passed(), "impl: all new pass, all resends drop");
        assertEquals(perWave * WAVES, heap.passed(), "prototype: all new pass, all resends drop");
        assertEquals(WARMUP_N + perWave * WAVES, proto.firsts, "prototype first counter");
        assertEquals(perWave * WAVES, proto.duplicates, "prototype duplicate counter");

        // G-DEDUP-5 guardrail (asserted): production heap hot path stays in budget.
        assertTrue(impl.p99ns() < P99_BUDGET_NS,
                "impl p99 " + impl.p99ns() + "ns exceeds " + P99_BUDGET_NS + "ns budget");
    }
}
