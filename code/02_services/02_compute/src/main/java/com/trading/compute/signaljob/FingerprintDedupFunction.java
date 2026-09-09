package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Fingerprint deduplication (Signal dossier operator 2, REQ-FC-003):
 * operator-local count-windowed repeat filter.
 *
 * <p>Design (2026-09-03 dedup redesign — replaced the RocksDB MapState +
 * native-TTL build, which cost ~1.7 ms/record/thread and capped the pipeline
 * at ~5k/s): per key (instrument token) an insertion-ordered set of the last
 * {@code dedupWindowEntries} fingerprint strings. Present = repeat (drop);
 * absent = new (add, auto-trim, pass). Forgetting is structural
 * ({@code removeEldestEntry}) — no TTL, no timers, no background deletes, no
 * scans. A repeat arriving after its fingerprint aged out is re-admitted; for
 * stock ticks (retries arrive back-to-back) the window covers ~100+ s per
 * instrument at target rates — longer than the 60 s TTL it replaces — and a
 * lapsed repeat costs one double-counted tick in one candle, never money
 * movement.
 *
 * <p><b>Intentional amnesia:</b> the windows are plain fields, NOT Flink
 * managed state, so checkpoints do not carry them and a restore starts empty.
 * This is safe by construction: restores resume sources from checkpointed
 * offsets (no replay of already-emitted ticks), and downstream candle/window
 * state is itself restored — replayed in-flight ticks re-apply onto restored
 * state and converge correctly. There is no legacy state to migrate: this
 * operator requests no managed state at all (run under uid
 * {@code fingerprint-dedup-v2}; see G-DEDUP-3).
 *
 * <p><b>Fail-fast guards (G-DEDUP-1/2/4):</b> per-token bound enforced after
 * every insert (trim breakage fails loud, never OOMs later); a global
 * entries cap fails closed instead of silently evicting (eviction would
 * silently change repeat semantics); the window size is required,
 * range-validated config, never a silent default.
 *
 * <p><b>Cheap-dedup (A):</b> the per-tick waste is removed — the window bound
 * is read once at {@code open()}, the window lookup uses get+put (no
 * per-tick lambda allocation), and the guards are plain {@code if}+throw
 * (messages built only on failure). Accept/drop behavior is unchanged.
 */
public class FingerprintDedupFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 2L;

    /**
     * Headroom over the instrument universe for the global entries cap
     * (G-DEDUP-2): 1024 instruments are expected; the cap tolerates 16× that
     * before failing closed. Growth beyond it is a leak, not legitimate
     * listings growth.
     */
    static final long GLOBAL_CAP_HEADROOM_TOKENS = 16_384L;

    /** Fixed-capacity insertion-ordered set; trims itself on every insert. */
    static final class Window extends LinkedHashMap<String, Boolean> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int max;

        Window(int max) {
            super(Math.min(max * 2, 1 << 20), 0.75f, false);
            if (max < 1) {
                throw new IllegalArgumentException("window max=" + max);
            }
            this.max = max;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > max;
        }
    }

    private final SignalJobConfig config;

    /** Per-token recent-fingerprint windows. Plain fields: intentionally NOT checkpointed. */
    private final Map<Long, Window> windows = new HashMap<>();
    /** Guard counter (G-DEDUP-2): live entries across all windows. Resets with windows. */
    private long totalEntries;
    /**
     * Cheap-dedup (A): window bound cached at {@code open()} — the config
     * value is fixed for the job, so re-reading it per tick is pure waste.
     */
    private int windowMax;

    private transient Counter firstEvents;
    private transient Counter duplicates;

    public FingerprintDedupFunction(SignalJobConfig config) {
        if (config == null) {
            throw new NullPointerException("config is required");
        }
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        windows.clear();
        totalEntries = 0;
        windowMax = config.dedupWindowEntries();
        firstEvents = getRuntimeContext().getMetricGroup().counter("compute.dedup.first");
        duplicates = getRuntimeContext().getMetricGroup().counter("compute.dedup.duplicates");
    }

    @Override
    public void processElement(RowData row, Context ctx, Collector<RowData> out) throws Exception {
        // Identity contract (unchanged): version + fingerprint, scoped by the
        // keyBy key (instrument token) via the per-key window. Upstream
        // RawValidationFunction rejects blank version/fingerprint (root cause);
        // fail loud here so a bypassed gate cannot collapse keys silently.
        if (row.isNullAt(RawTableColumns.FINGERPRINT_VERSION)
                || row.isNullAt(RawTableColumns.EVENT_FINGERPRINT)) {
            throw new IllegalStateException(
                    "dedup identity column null: fingerprint_version/event_fingerprint");
        }
        String fp = row.getString(RawTableColumns.FINGERPRINT_VERSION).toString()
                + "|" + row.getString(RawTableColumns.EVENT_FINGERPRINT).toString();
        // Cheap-dedup (A): get+put instead of computeIfAbsent — same logic,
        // no per-tick lambda allocation on the hot path.
        int max = windowMax;
        Window w = windows.get(ctx.getCurrentKey());
        if (w == null) {
            w = new Window(max);
            windows.put(ctx.getCurrentKey(), w);
        }
        int before = w.size();
        if (w.put(fp, Boolean.TRUE) != null) {
            duplicates.inc();
            return;
        }
        // G-DEDUP-1: the bound is structural — an overgrown window means
        // trimming broke. Fail loud here, never grow into an OOM later.
        // Cheap-dedup (A): plain if+throw — the message is built only on
        // failure, not on every tick. Regression canary: removeEldestEntry
        // trims on every put so this should never trip; it exists to shout
        // if LinkedHashMap behaviour ever changes.
        if (w.size() > max) {
            throw new IllegalStateException(
                    "dedup window overgrew bound: size=" + w.size() + " max=" + max);
        }
        totalEntries++;
        if (w.size() == before) {
            // Insert displaced the eldest (self-trim): net entries unchanged.
            totalEntries--;
        }
        // G-DEDUP-2: global cap fails closed instead of silently evicting.
        // Eviction would silently change repeat semantics; a leak must shout.
        // Cheap-dedup (A): plain if+throw — same fail-closed, no per-tick
        // varargs array + boxing on the hot path.
        long cap = (long) max * GLOBAL_CAP_HEADROOM_TOKENS;
        if (totalEntries > cap) {
            throw new IllegalStateException("dedup total entries " + totalEntries
                    + " exceeded cap " + cap + " — refusing silent eviction");
        }
        firstEvents.inc();
        out.collect(row);
    }

    /** Test seams: live window size for a token; total live entries. */
    long windowSizeForTest(long token) {
        Window w = windows.get(token);
        return w == null ? 0 : w.size();
    }

    long totalEntriesForTest() {
        return totalEntries;
    }
}
