package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link FingerprintDedupFunction} driven through the Flink 2.2.1 operator
 * harness (KeyedOneInputStreamOperatorTestHarness — no cluster). Covers the
 * dedup half of SIG-UNIT-008/009 (state content/compactness + expiry) under
 * design B (state-authoritative, no external store, no eviction) with NATIVE
 * TTL expiry (CHG-023 item 2, 2026-08-17): first occurrence passes,
 * duplicate within TTL is dropped, state holds exactly the compact
 * (fingerprint → first_seen/nominal_expiry) map, and expiry is the native
 * {@code StateTtlConfig} on the map — advance the harness TTL processing time
 * past {@code first_seen + TTL} (wall clock, {@code NeverReturnExpired}) and a
 * re-arriving fingerprint is eligible again, never double-accepted.
 */
class FingerprintDedupFunctionTest {

    private static final long TTL_MS = 60_000L;
    private static final long T0 = 1_700_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    /**
     * The function under test — gauge assertions read its fields (the exact
     * values the {@code MetricGroup} gauges export; CHG-023 item 1 removed
     * the client-side ComputeOtlpEmitter mirror). Fresh per {@link #openHarness}.
     */
    private FingerprintDedupFunction function;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
        // Test seam reset: expiry/gauge legs lower the resync interval to
        // force an immediate gauge resync; restore the production cadence.
        FingerprintDedupFunction.GAUGE_RESYNC_INTERVAL_ROWS = 10_000L;
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

    /** Same 5-key baseline as SignalJobConfigTest; tuning keys take defaults. */
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

    private static long emittedCount(KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream().filter(o -> o instanceof StreamRecord).count();
    }

    private void process(GenericRowData row) throws Exception {
        harness.processElement(row, row.getLong(RawTableColumns.EVENT_TIME));
    }

    @Test
    void firstOccurrencePassesDuplicateWithinTtlDropped() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));

        assertEquals(1, emittedCount(harness), "duplicate within TTL must be dropped");
        // State layout contract (SIG-UNIT-008): per active key exactly ONE
        // state row — the fingerprint-dedup map (the expiry index is gone,
        // CHG-023 item 2). Fingerprint count adds map ENTRIES, never new
        // rows: no raw payload, decoded field, or event object is stored per
        // fingerprint.
        assertEquals(1, harness.numKeyedStateEntries(), "one dedup row per active key");
        assertEquals(0, harness.numEventTimeTimers(), "no hand-rolled timers — native TTL expires entries");
    }

    @Test
    void expiredFingerprintReAdmittedAfterTtlElapses() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));
        assertEquals(1, emittedCount(harness));

        // Native TTL (processing time): advance the harness TTL clock past
        // first-seen + TTL. The entry was anchored at the write-time clock
        // (provider starts at 0), so TTL + 1 expires it.
        harness.setStateTtlProcessingTime(TTL_MS + 1L);

        // Re-arriving fingerprint after expiry is eligible again (SIG-UNIT-009).
        process(TestRawRows.row(1L, T0 + TTL_MS + 10_000L, "fp-1", "TRADE", 200, 5));
        assertEquals(2, emittedCount(harness));
        assertEquals(1, harness.numKeyedStateEntries(),
                "the re-admitted entry is the only state — the expired entry was read as absent and replaced");
    }

    @Test
    void duplicateStillDroppedBeforeTtlElapses() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));

        // TTL still far ahead: entry must exist and still dedupe.
        process(TestRawRows.row(1L, T0 + TTL_MS - 1_000L, "fp-1", "TRADE", 101, 2));
        assertEquals(1, emittedCount(harness), "duplicate still dropped while unexpired");
        assertEquals(1, harness.numKeyedStateEntries(), "never deleted early");
    }

    @Test
    void stateKeyScopedByInstrumentToken() throws Exception {
        openHarness();
        // Same fingerprint on a different instrument is a different state key
        // (version | scope | fingerprint) — both must pass.
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(2L, T0, "fp-1", "TRADE", 100, 1));

        assertEquals(2, emittedCount(harness));
        // Two active keys → 2 rows (one dedup map entry each).
        assertEquals(2, harness.numKeyedStateEntries(), "one dedup row per active key");
    }

    @Test
    void distinctFingerprintsSameInstrumentAllPass() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-2", "TRADE", 101, 1));

        assertEquals(2, emittedCount(harness));
        // One active key: two fingerprints add map entries, NOT state rows —
        // still exactly 1 row (compactness: no per-fingerprint storage).
        assertEquals(1, harness.numKeyedStateEntries(), "fingerprint count never grows state rows");
        assertEquals(0, harness.numEventTimeTimers());
    }

    // ── tracker 14 P5.3 hot-path boundary tests ───────────────────────────

    @Test
    void differentFingerprintVersionDoesNotCollide() throws Exception {
        openHarness();
        GenericRowData v1 = TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1);
        v1.setField(RawTableColumns.FINGERPRINT_VERSION, StringData.fromString("1"));
        process(v1);
        GenericRowData v2 = TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 1);
        v2.setField(RawTableColumns.FINGERPRINT_VERSION, StringData.fromString("2"));
        process(v2); // same fingerprint, different version → distinct state key

        assertEquals(2, emittedCount(harness), "version is part of the state key — v2 must pass");
    }

    @Test
    void stateDoesNotGrowWhenAllRowsAreDuplicates() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        for (int i = 0; i < 10_000; i++) {
            process(TestRawRows.row(1L, T0 + i, "fp-1", "TRADE", 100 + i, i));
        }
        assertEquals(1, function.dedupStateCountForTest(),
                "10k duplicates of one fingerprint must not grow state");
        assertEquals(0, harness.numEventTimeTimers());
    }

    @Test
    void idleSourceDoesNotAccumulateState() throws Exception {
        openHarness();
        // Nothing processed: the operator must add no state and no timers of
        // its own — an idle source cannot grow the dedup map.
        assertEquals(0, harness.numKeyedStateEntries());
        assertEquals(0, harness.numEventTimeTimers());
    }

    @Test
    void highCardinalityFingerprintsAllPass() throws Exception {
        openHarness();
        int n = 5_000;
        for (int i = 0; i < n; i++) {
            process(TestRawRows.row(1L, T0 + i, "fp-hc-" + i, "TRADE", 100 + i, i));
        }
        assertEquals(n, emittedCount(harness), "every distinct fingerprint passes");
        assertEquals(n, function.dedupStateCountForTest(),
                "state holds exactly one entry per live fingerprint");
    }

    @Test
    void malformedEmptyFingerprintIsScopedNotCrashed() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "", "TRADE", 101, 2));

        assertEquals(1, emittedCount(harness),
                "empty fingerprint is a valid (if malformed) state key — first passes, duplicate drops");
        assertEquals(1, function.dedupStateCountForTest());
    }

    @Test
    void eventTimeNearMaxValueAcceptedAndDeduped() throws Exception {
        openHarness();
        long nearMax = Long.MAX_VALUE - 1_000L;
        GenericRowData row = TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1);
        row.setField(RawTableColumns.EVENT_TIME, nearMax);
        process(row); // nominal expiry would wrap negative — must clamp, not crash

        assertEquals(1, emittedCount(harness));
        assertEquals(1, function.dedupStateCountForTest());
        // Duplicate of the same near-max row: still deduped (no overflow
        // crash; the native TTL is unaffected by the event-time value).
        GenericRowData dup = TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2);
        dup.setField(RawTableColumns.EVENT_TIME, nearMax + 1_000L);
        process(dup);
        assertEquals(1, emittedCount(harness), "near-max event time still dedupes within TTL");
        assertEquals(1, function.dedupStateCountForTest());
    }

    // ── design B: no eviction — the state IS the authoritative full set ────

    @Test
    void noEvictionStateHoldsFullLiveSet() throws Exception {
        openHarness();
        // 100 distinct fingerprints — far beyond the retired DEC-038 cache cap
        // (250k entries / 32 MB min). Design B keeps EVERY live fingerprint in
        // keyed state: no eviction, no external store to re-decide against.
        for (int i = 0; i < 100; i++) {
            process(TestRawRows.row(1L, T0 + i * 1_000L, "fp-full-" + i, "TRADE", 100 + i, i));
        }
        assertEquals(100, function.dedupStateCountForTest(),
                "state holds the full live set — eviction must not occur");

        // An early in-TTL fingerprint re-arrives: still deduped directly from
        // state — no rehydration, no store round trip.
        long emittedBefore = emittedCount(harness);
        process(TestRawRows.row(1L, T0, "fp-full-0", "TRADE", 200, 1000));
        assertEquals(emittedBefore, emittedCount(harness),
                "re-delivery of an in-TTL fingerprint must still be dropped from state alone");
        assertEquals(100, function.dedupStateCountForTest(),
                "the re-delivery adds no state");

        // TTL expiry clears the full set (native): advance the TTL clock past
        // every entry's anchor and force a gauge resync — entries() skips the
        // expired set, so the count falls to the single fresh row.
        FingerprintDedupFunction.GAUGE_RESYNC_INTERVAL_ROWS = 1L;
        harness.setStateTtlProcessingTime(TTL_MS + 1L);
        process(TestRawRows.row(1L, T0 + TTL_MS + 10_000L, "fp-after-expiry", "TRADE", 300, 2000));
        assertEquals(1, function.dedupStateCountForTest(),
                "the 100 expired entries are gone from the gauge — only the fresh row is live");
    }

    // ── tracker 14 P5.1 gauge sources (the MetricGroup gauges' fields) ────

    @Test
    void dedupGaugesTrackInsertAndExpiry() throws Exception {
        openHarness(); // gauge fields start at zero; the MetricGroup gauges read them
        FingerprintDedupFunction.GAUGE_RESYNC_INTERVAL_ROWS = 1L; // immediate resync
        assertEquals(0, function.dedupStateCountForTest());
        assertEquals(0, function.bytesEstimateForTest());

        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-2", "TRADE", 101, 2));
        process(TestRawRows.row(1L, T0 + 2_000L, "fp-1", "TRADE", 102, 3)); // duplicate

        assertEquals(2, function.dedupStateCountForTest(),
                "two live fingerprints, duplicate adds nothing");
        assertEquals(2 * FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES,
                function.bytesEstimateForTest());

        // Native TTL expiry: advance the TTL clock past both anchors. The
        // gauge still shows 2 (insertion-side) until the next resync folds the
        // actual live set in.
        harness.setStateTtlProcessingTime(TTL_MS + 1L);
        assertEquals(2, function.dedupStateCountForTest(),
                "gauge is an insertion ledger until the next resync");

        process(TestRawRows.row(1L, T0 + TTL_MS + 10_000L, "fp-3", "TRADE", 200, 4));
        assertEquals(1, function.dedupStateCountForTest(),
                "resync folds the actual live set — both expired entries dropped, fp-3 live");
        assertEquals(1 * FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES,
                function.bytesEstimateForTest());
    }

    @Test
    void dedupGaugesFoldRestoredStateExactly() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        assertEquals(1, function.dedupStateCountForTest());

        // Checkpoint the harness, then restore it into a NEW operator — the
        // restored MapState holds fp-1 but the fresh gauge fields start at zero.
        OperatorSubtaskState state = harness.snapshot(0L, 0L);
        harness.close();

        // forKeyedProcessFunction already initializes the harness (empty) —
        // a RESTORE needs a manually constructed harness: initializeState
        // must run before open. The fresh function starts its gauge fields at
        // zero; the first row per token folds the restored size in exactly.
        FingerprintDedupFunction restoredFunction =
                new FingerprintDedupFunction(SignalJobConfig.from(env()));
        function = restoredFunction; // subsequent gauge asserts read the restored instance
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> restored =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(restoredFunction),
                        row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG);
        restored.initializeState(state);
        restored.open();
        try {
            // First row for token 1: replace-tracked(0) with actual-restored(1).
            processOn(restored, TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));
            assertEquals(1, function.dedupStateCountForTest(),
                    "restored fp-1 folded in exactly (replace, not add)");

            // A brand-new fingerprint adds one.
            processOn(restored, TestRawRows.row(1L, T0 + 2_000L, "fp-2", "TRADE", 102, 3));
            assertEquals(2, function.dedupStateCountForTest());

            // Restored fp-1 is still deduplicated.
            processOn(restored, TestRawRows.row(1L, T0 + 3_000L, "fp-1", "TRADE", 103, 4));
            assertEquals(2, function.dedupStateCountForTest());
        } finally {
            restored.close();
        }
    }

    private static void processOn(
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h, GenericRowData row)
            throws Exception {
        h.processElement(row, row.getLong(RawTableColumns.EVENT_TIME));
    }

    // ── SIG-HARNESS-004: identical-looking events vs broker duplicates ─────

    @Test
    void identicalLookingEventsCollapseAndEmitLimitationEvidence() throws Exception {
        openHarness();
        // The documented fingerprint limitation (dossier §Dedup state): an
        // identical legitimate event and a broker duplicate are INDISTINGUISHABLE
        // by fingerprint — both carry the same content hash. The dedup applies
        // the limitation consistently: the first occurrence passes, every
        // identical-looking re-arrival inside the TTL is collapsed to a
        // duplicate, directly from keyed state.
        process(TestRawRows.row(1L, T0, "fp-identical", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-identical", "TRADE", 100, 1));

        assertEquals(1, emittedCount(harness),
                "the identical legitimate event is collapsed, never double-accepted");
        assertEquals(1, function.dedupStateCountForTest(),
                "one accepted fingerprint in state, not two");
    }
}
